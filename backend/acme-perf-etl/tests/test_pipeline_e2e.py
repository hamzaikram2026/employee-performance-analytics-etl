"""Functional end-to-end tests: Bronze -> Silver -> Gold on a small dataset with known answers.

Runs the real pipeline code against an in-memory S3 and a fake PostgreSQL source.
Covers source ingestion, transformation, aggregation, data-quality quarantine,
reconciliation across layers, and idempotent reruns.
"""
import json
import re

import pandas as pd
import pytest

from lib import bronze, gold, silver
from lib.contracts import CONTRACTS
from lib.logging_utils import RunAudit


class FakePostgres:
    """Returns source tables as strings, exactly like PostgresSource.read_table."""

    def __init__(self, tables):
        self.tables = tables

    def read_table(self, table):
        return self.tables[table].astype("string")


SOURCE = {
    "teams": pd.DataFrame(
        [[1, "Team A", "Engineering"], [2, "Team B", "Operations"]],
        columns=["team_id", "name", "department"]),
    "competencies": pd.DataFrame(
        [[1, "Python", "Technical"], [2, "Coaching", "Leadership"]],
        columns=["competency_id", "name", "category"]),
    "employees": pd.DataFrame(
        [[1, "Asha", "Rao", "asha@acme.example", 1, "IC1", "2020-01-01"],
         [2, "Ben", "Lee", "ben@acme.example", 1, "IC2", "2020-01-01"],
         [3, "Cara", "Diaz", "cara@acme.example", 2, "IC1", "2026-01-01"]],   # hired late
        columns=["employee_id", "first_name", "last_name", "email", "team_id", "job_level", "hire_date"]),
    "projects": pd.DataFrame(
        [[1, "Launch", 1, "on-time", 80.0], [2, "Rollout", 2, "delayed", 50.0]],
        columns=["project_id", "name", "team_id", "outcome_status", "outcome_score"]),
    "performance_reviews": pd.DataFrame(
        [[1, 1, "2024-H2", 3, "Met expectations", "2025-08-20"],
         [2, 1, "2025-H1", 4, "Consistently exceeded goals", "2026-02-20"],
         [3, 2, "2024-H2", 4, "Solid contributor", "2025-08-20"],
         [4, 2, "2025-H1", 2, "Missed several key deliverables", "2026-02-20"],
         [5, 3, "2024-H2", 3, "Met expectations", "2025-08-20"],    # before hire -> quarantine
         [6, 1, "2025-H1", 7, "Typo rating", "2026-02-20"]],        # rating 7 -> quarantine
        columns=["review_id", "employee_id", "review_period", "rating", "manager_comment", "submitted_date"]),
    "competency_assessments": pd.DataFrame(
        [[1, 1, 1, 2, 4, "2025-10-01"],      # gap of 2 -> critical
         [2, 2, 2, 4, 3, "2025-10-01"],
         [3, 3, 1, 3, 3, "2025-10-01"]],     # before hire -> quarantine
        columns=["assessment_id", "employee_id", "competency_id", "proficiency_level",
                 "required_level", "assessed_date"]),
    "talent_review": pd.DataFrame(
        [[1, "2025-H1", "High", "Ready Now", "Low"],
         [2, "2025-H1", "Low", "Ready Now", "High"],
         [3, "2025-H1", "Medium", "Not Ready", "Low"]],
        columns=["employee_id", "review_cycle", "potential_rating", "promotion_readiness", "attrition_risk"]),
}


def event(event_id, employee_id, when, event_type, **extra):
    record = {"event_id": event_id, "employee_id": str(employee_id), "event_date": when,
              "event_type": event_type, "course_name": "SQL Basics", "text": "activity", "score": None}
    record.update(extra)
    return json.dumps(record)


DRIFTED = json.dumps({"event_id": "E3", "employee_id": "2", "event_date": "2026-03-01T11:00:00",
                      "event_type": "quiz_passed", "course_name": "SQL Basics",
                      "notes": "drifted note", "score": 85})

LANDING_FILES = {
    "landing/learning_activity/year=2026/month=03/day=01/events-000.jsonl": [
        event("E1", 1, "2026-03-01T09:00:00", "enrollment"),
        event("E2", 1, "2026-03-01T10:00:00", "course_completed"),
        DRIFTED,                                                       # notes instead of text
        event("E1", 1, "2026-03-01T09:00:00", "enrollment"),           # duplicate -> quarantine
        "{broken json",                                                # malformed -> quarantine
        "",                                                            # blank line, ignored
        event("E5", 3, "2025-06-01T09:00:00", "enrollment"),           # before hire -> quarantine
        event("E6", 1, "yesterday", "module_started"),                 # bad date -> quarantine
    ],
    "landing/learning_activity/year=2026/month=03/day=02/events-000.jsonl": [
        event("E7", 2, "2026-03-02T09:00:00", "module_started"),
    ],
}
SOURCE_EVENT_LINES = 8  # non-empty lines across both files


def run_all(cfg, lake, pg, full_refresh=False):
    audit = RunAudit(cfg.run_id, cfg.run_ts)
    bronze.run(cfg, pg, lake, audit, full_refresh=full_refresh)
    silver.run(cfg, lake, audit)
    gold.run(cfg, lake, audit)
    return audit


def steps(audit, layer):
    return {s["dataset"]: s for s in audit.steps if s["layer"] == layer}


def dataset_counts(lake):
    counts = {}
    for layer in ("bronze", "silver", "quarantine", "gold"):
        names = {k.split("/")[1] for k in lake.list_keys(f"{layer}/") if k.endswith(".parquet")}
        for name in names:
            counts[f"{layer}/{name}"] = len(lake.read_parquet_prefix(f"{layer}/{name}/"))
    return counts


@pytest.fixture
def pg():
    return FakePostgres(SOURCE)


@pytest.fixture
def result(cfg, lake, pg):
    for key, lines in LANDING_FILES.items():
        lake.write_bytes(key, "\n".join(lines).encode())
    return run_all(cfg, lake, pg)


# ----------------------------------------------------------------- whole run
def test_every_step_succeeds(result):
    assert result.status == "SUCCESS"
    assert len(steps(result, "bronze")) == 8
    assert len(steps(result, "silver")) == 8
    assert len(steps(result, "gold")) == 9


# -------------------------------------------------------------------- Bronze
def test_bronze_ingests_every_source_row(result):
    b = steps(result, "bronze")
    for table, df in SOURCE.items():
        assert b[table]["rows_written"] == len(df), table
    assert b["learning_activity"]["rows_written"] == SOURCE_EVENT_LINES


def test_bronze_partition_layout(result, lake):
    pattern = re.compile(r"^bronze/[a-z_]+/year=\d{4}/month=\d{2}/day=\d{2}/data\.parquet$")
    keys = [k for k in lake.list_keys("bronze/")]
    assert keys and all(pattern.match(k) for k in keys)


def test_bronze_manifest_matches_source_file_sizes(result, lake, cfg):
    manifest = json.loads(lake.read_text(f"_audit/manifests/bronze_learning_activity_{cfg.run_id}.json"))
    assert len(manifest["files"]) == len(LANDING_FILES)
    for entry in manifest["files"]:
        assert entry["bytes"] == len(lake.read_bytes(entry["source_key"]))


# -------------------------------------------------------------------- Silver
def test_silver_quarantines_bad_rows_with_reasons(result, lake, cfg):
    def reasons(name):
        q = lake.read_parquet_prefix(f"quarantine/{name}/{cfg.run_partition}/")
        return set(q["_reject_reason"])

    assert reasons("performance_reviews") == {"rating: above 5", "review submitted before employee hire date"}
    assert reasons("competency_assessments") == {"assessment dated before employee hire date"}
    assert reasons("learning_activity") == {
        "duplicate primary key",
        "malformed JSON record",
        "event_date: not a valid datetime",
        "event dated before employee hire date",
    }


def test_silver_reconciles_read_written_quarantined(result):
    for name, s in steps(result, "silver").items():
        assert s["rows_read"] == s["rows_written"] + s["rows_quarantined"], name


def test_silver_keys_unique_and_not_null(result, lake):
    for name, contract in CONTRACTS.items():
        df = lake.read_parquet_prefix(f"silver/{name}/")
        keys = list(contract.primary_key)
        assert df[keys].notna().all().all(), name
        assert not df.duplicated(keys).any(), name


def test_silver_types_and_schema_drift(result, lake):
    emp = lake.read_parquet_prefix("silver/employees/")
    assert pd.api.types.is_datetime64_any_dtype(emp["hire_date"])
    ev = lake.read_parquet_prefix("silver/learning_activity/").set_index("event_id")
    assert pd.api.types.is_datetime64_any_dtype(ev["event_date"])
    assert ev.loc["E3", "text"] == "drifted note" and ev.loc["E3", "_text_field"] == "notes"
    assert sorted(ev.index) == ["E1", "E2", "E3", "E7"]


# ---------------------------------------------------------------------- Gold
def test_gold_row_counts_reconcile_with_silver(result, lake):
    silver_count = lambda n: len(lake.read_parquet_prefix(f"silver/{n}/"))
    gold_count = lambda n: len(lake.read_parquet(f"gold/{n}/data.parquet"))
    n_emp = silver_count("employees")
    for table in ["gold_employee_performance", "gold_training_summary", "gold_attrition_risk",
                  "gold_promotion_candidates", "gold_employee_profile"]:
        assert gold_count(table) == n_emp, table
    assert gold_count("gold_review_history") == silver_count("performance_reviews")
    assert gold_count("gold_skill_gaps") == silver_count("competency_assessments")
    assert gold_count("gold_team_performance") == silver_count("teams")


def test_gold_metrics_match_known_values(result, lake):
    p = lake.read_parquet("gold/gold_employee_profile/data.parquet").set_index("employee_id")
    # Employee 1: 3 -> 4, high potential, but a critical skill gap blocks promotion
    assert p.loc[1, "performance_trend"] == "improving"
    assert p.loc[1, "critical_gap_count"] == 1
    assert p.loc[1, "promotion_status"] == "High potential - develop"
    # Employee 2: 4 -> 2 (+3), latest 2 (+2), negative comment (+1), recent training (+0) = 6
    assert p.loc[2, "performance_trend"] == "declining"
    assert p.loc[2, "attrition_risk_score"] == 6 and p.loc[2, "derived_attrition_risk"] == "High"
    # Employee 3: only review was before hire, so it was quarantined
    assert p.loc[3, "performance_trend"] == "no reviews"

    t = lake.read_parquet("gold/gold_training_summary/data.parquet").set_index("employee_id")
    assert t.loc[1, "enrollments"] == 1 and t.loc[1, "courses_completed"] == 1
    assert t.loc[2, "quizzes_passed"] == 1 and t.loc[2, "avg_quiz_score"] == 85.0

    team = lake.read_parquet("gold/gold_team_performance/data.parquet").set_index("team_id")
    assert team.loc[1, "headcount"] == 2 and team.loc[1, "avg_latest_rating"] == 3.0
    assert team.loc[1, "avg_outcome_score"] == 80.0 and team.loc[1, "pct_projects_on_time"] == 1.0


# --------------------------------------------------------------- Idempotency
def test_rerun_is_idempotent_and_incremental(result, cfg, lake, pg):
    before = dataset_counts(lake)
    second = run_all(cfg, lake, pg)
    assert second.status == "SUCCESS"
    assert dataset_counts(lake) == before
    events = steps(second, "bronze")["learning_activity"]
    assert events["partitions_skipped"] == 2 and events["rows_written"] == 0


def test_full_refresh_reprocesses_without_duplicates(result, cfg, lake, pg):
    before = dataset_counts(lake)
    refreshed = run_all(cfg, lake, pg, full_refresh=True)
    assert steps(refreshed, "bronze")["learning_activity"]["rows_written"] == SOURCE_EVENT_LINES
    assert dataset_counts(lake) == before