"""Performance test: throughput, scaling, memory and SLA for the full pipeline.

Runs Bronze -> Silver -> Gold in memory on synthetic data at 1x, 5x and 10x the
size of the workshop dataset (1x = 2,000 employees and ~30,000 learning events).
Run with:  python -m pytest -m performance -s
"""
import json
import resource
import time

import numpy as np
import pandas as pd
import pytest
from conftest import FakeS3
from test_pipeline_e2e import FakePostgres, run_all

from lib.contracts import EVENT_TYPES
from lib.storage import DataLake

pytestmark = pytest.mark.performance

BASE_EMPLOYEES = 2000
EVENTS_PER_EMPLOYEE = 15
SCALES = [1, 5, 10]
SLA_SECONDS = 120          # end-to-end budget for the 10x run


def synthetic_sources(n_emp: int, seed: int = 7) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    n_teams = max(n_emp // 20, 1)
    ids = np.arange(1, n_emp + 1)
    n_proj = n_teams // 3 + 1
    return {
        "teams": pd.DataFrame({"team_id": np.arange(1, n_teams + 1),
                               "name": [f"Team {i}" for i in range(1, n_teams + 1)],
                               "department": rng.choice(["Sales", "Engineering", "Design"], n_teams)}),
        "competencies": pd.DataFrame({"competency_id": np.arange(1, 41),
                                      "name": [f"Skill {i}" for i in range(1, 41)],
                                      "category": rng.choice(["Technical", "Leadership", "Domain"], 40)}),
        "employees": pd.DataFrame({"employee_id": ids, "first_name": "First",
                                   "last_name": [f"L{i}" for i in ids],
                                   "email": [f"user{i}@acme.example" for i in ids],
                                   "team_id": rng.integers(1, n_teams + 1, n_emp),
                                   "job_level": rng.choice(["IC1", "IC2", "Manager"], n_emp),
                                   "hire_date": "2020-01-01"}),
        "projects": pd.DataFrame({"project_id": np.arange(1, n_proj + 1),
                                  "name": [f"Project {i}" for i in range(1, n_proj + 1)],
                                  "team_id": rng.integers(1, n_teams + 1, n_proj),
                                  "outcome_status": rng.choice(["on-time", "delayed"], n_proj),
                                  "outcome_score": rng.uniform(40, 95, n_proj).round(2)}),
        "performance_reviews": pd.DataFrame({"review_id": np.arange(1, 2 * n_emp + 1),
                                             "employee_id": np.repeat(ids, 2),
                                             "review_period": np.tile(["2024-H2", "2025-H1"], n_emp),
                                             "rating": rng.integers(1, 6, 2 * n_emp),
                                             "manager_comment": "Met expectations",
                                             "submitted_date": np.tile(["2025-08-20", "2026-02-20"], n_emp)}),
        "competency_assessments": pd.DataFrame({
            "assessment_id": np.arange(1, 2 * n_emp + 1),
            "employee_id": np.repeat(ids, 2),
            "competency_id": np.column_stack([rng.integers(1, 21, n_emp),
                                              rng.integers(21, 41, n_emp)]).ravel(),
            "proficiency_level": rng.integers(1, 6, 2 * n_emp),
            "required_level": rng.integers(2, 5, 2 * n_emp),
            "assessed_date": "2025-10-01"}),
        "talent_review": pd.DataFrame({"employee_id": ids, "review_cycle": "2025-H1",
                                       "potential_rating": rng.choice(["Low", "Medium", "High"], n_emp),
                                       "promotion_readiness": rng.choice(["Ready Now", "1-2 Years", "Not Ready"], n_emp),
                                       "attrition_risk": rng.choice(["Low", "Medium", "High"], n_emp)}),
    }


def synthetic_landing(n_emp: int, seed: int = 7) -> dict[str, bytes]:
    """JSONL event files spread across 30 daily partitions."""
    rng = np.random.default_rng(seed)
    n = n_emp * EVENTS_PER_EMPLOYEE
    employees = rng.integers(1, n_emp + 1, n).tolist()
    days = rng.integers(1, 31, n).tolist()
    types = rng.choice(sorted(EVENT_TYPES), n).tolist()
    scores = rng.integers(0, 101, n).tolist()
    by_day: dict[int, list[str]] = {}
    for i in range(n):
        record = {"event_id": f"EV{i:09d}", "employee_id": str(employees[i]),
                  "event_date": f"2026-03-{days[i]:02d}T09:00:00", "event_type": types[i],
                  "course_name": "SQL Basics", "text": "activity",
                  "score": scores[i] if types[i].startswith("quiz") else None}
        by_day.setdefault(days[i], []).append(json.dumps(record))
    return {f"landing/learning_activity/year=2026/month=03/day={d:02d}/events-000.jsonl":
            "\n".join(lines).encode() for d, lines in by_day.items()}


def run_at_scale(cfg, scale: int) -> dict:
    n_emp = BASE_EMPLOYEES * scale
    lake = DataLake(cfg, client=FakeS3())
    for key, body in synthetic_landing(n_emp).items():
        lake.write_bytes(key, body)
    pg = FakePostgres(synthetic_sources(n_emp))

    started = time.perf_counter()
    audit = run_all(cfg, lake, pg)
    seconds = time.perf_counter() - started

    rows = sum(s["rows_read"] for s in audit.steps if s["layer"] == "bronze")
    return {
        "scale": f"{scale}x",
        "employees": n_emp,
        "source_rows": rows,
        "seconds": round(seconds, 2),
        "rows_per_minute": int(rows / seconds * 60),
        "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
        "status": audit.status,
    }


def test_scaling_throughput_memory_and_sla(cfg):
    results = [run_at_scale(cfg, s) for s in SCALES]
    print("\n" + pd.DataFrame(results).to_string(index=False))

    assert all(r["status"] == "SUCCESS" for r in results)
    per_row = [r["seconds"] / r["source_rows"] for r in results]
    # Near-linear scaling: cost per row at 10x stays within 2x of the cost at 1x
    assert per_row[-1] <= 2 * per_row[0], f"nonlinear slowdown: {per_row}"
    assert results[-1]["seconds"] <= SLA_SECONDS, f"SLA breached: {results[-1]['seconds']}s"