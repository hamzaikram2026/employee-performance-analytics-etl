"""Read-only dashboard API over the Gold layer: summary figures, search filters, input validation."""
import json

import pandas as pd
import pytest

import function as service
from lib.dashboard_api import MAX_LIMIT, dashboard_summary, search_employees


def profile():
    return pd.DataFrame({
        "employee_id": [1, 2, 3, 4],
        "employee_name": ["Asha Rao", "Ben Lee", "Cara Diaz", "Dev Shah"],
        "department": ["Sales", "Sales", "Engineering", "Engineering"],
        "team_name": ["T1", "T1", "T2", "T2"],
        "job_level": ["IC1", "IC2", "IC1", "Manager"],
        "latest_rating": pd.array([5, 2, 4, None], dtype="Int64"),
        "avg_rating": [4.5, 3.0, 4.0, None],
        "performance_trend": ["improving", "declining", "stable", "no reviews"],
        "critical_gap_count": [0, 1, 0, 2],
        "courses_completed": [3, 0, 1, 0],
        "certifications_earned": [1, 0, 0, 0],
        "derived_attrition_risk": ["Low", "High", "Low", "Medium"],
        "attrition_risk_score": [0, 7, 0, 2],
        "promotion_status": ["Ready now - high potential", "Not yet", "Ready now", "High potential - develop"],
        "potential_rating": ["High", "Low", "Medium", "High"],
        "readiness_label_agrees": [True, False, True, True],
        "attrition_label_agrees": [True, True, False, True],
        "has_recent_training": [True, False, True, False],
        "_gold_built_at": ["2026-10-15T12:00:00+00:00"] * 4,
        "_run_id": ["abc123"] * 4,
    })


@pytest.fixture
def gold_lake(lake):
    lake.write_parquet(profile(), "gold/gold_employee_profile/data.parquet")
    lake.write_parquet(pd.DataFrame({
        "team_name": ["T1", "T2"], "department": ["Sales", "Engineering"], "headcount": [2, 2],
        "has_projects": [True, False], "avg_latest_rating": [3.5, 4.0], "avg_outcome_score": [80.0, None],
    }), "gold/gold_team_performance/data.parquet")
    lake.write_parquet(pd.DataFrame({
        "employee_id": [2, 4, 4, 1], "department": ["Sales", "Engineering", "Engineering", "Sales"],
        "competency_name": ["Python", "SQL", "Python", "SQL"], "is_critical_gap": [True, True, True, False],
    }), "gold/gold_skill_gaps/data.parquet")
    return lake


def names(result):
    return [row["employee_name"] for row in result["rows"]]


# ----------------------------------------------------------------- summary
def test_summary_kpis_and_charts(cfg, gold_lake):
    s = dashboard_summary(cfg, gold_lake)
    assert s["kpis"] == {"employees": 4, "improving": 1, "declining": 1, "critical_gap_employees": 2,
                         "ready_now_high_potential": 1, "high_attrition_risk": 1, "no_recent_learning": 2}
    assert s["charts"]["attrition_risk"] == {"Low": 2, "Medium": 1, "High": 1}
    assert s["charts"]["top_critical_competencies"] == {"Python": 2, "SQL": 1}
    assert s["charts"]["label_agreement_pct"] == {"promotion": 75.0, "attrition": 75.0}
    assert s["charts"]["rating_outcome_correlation"] is None  # only one team has projects
    sales = next(d for d in s["charts"]["departments"] if d["department"] == "Sales")
    assert sales == {"department": "Sales", "employees": 2, "avg_rating": 3.5,
                     "pct_critical_gap": 50.0, "pct_high_risk": 50.0}
    assert s["filters"]["department"] == ["Engineering", "Sales"]
    assert s["generated_from"]["run_id"] == "abc123"
    json.dumps(s)  # fully JSON-serialisable


# ------------------------------------------------------------------ search
def test_search_combines_filters_case_insensitively(cfg, gold_lake):
    assert names(search_employees(cfg, gold_lake, {"department": "sales", "min_rating": "4"})) == ["Asha Rao"]


def test_search_by_name_and_risk(cfg, gold_lake):
    assert names(search_employees(cfg, gold_lake, {"name": "diaz"})) == ["Cara Diaz"]
    assert names(search_employees(cfg, gold_lake, {"attrition_risk": "high"})) == ["Ben Lee"]


def test_search_critical_gap_sorted_by_risk(cfg, gold_lake):
    result = search_employees(cfg, gold_lake, {"has_critical_gap": "true", "sort": "risk"})
    assert names(result) == ["Ben Lee", "Dev Shah"]


def test_search_missing_rating_is_null_and_excluded_by_rating_filter(cfg, gold_lake):
    rows = {r["employee_name"]: r for r in search_employees(cfg, gold_lake, {})["rows"]}
    assert rows["Dev Shah"]["latest_rating"] is None
    assert "Dev Shah" not in names(search_employees(cfg, gold_lake, {"max_rating": "5"}))


def test_search_pagination(cfg, gold_lake):
    result = search_employees(cfg, gold_lake, {"limit": "1", "offset": "1"})
    assert result["total"] == 4 and names(result) == ["Ben Lee"]


@pytest.mark.parametrize("params", [
    {"min_rating": "9"}, {"min_rating": "abc"}, {"limit": str(MAX_LIMIT + 1)},
    {"sort": "salary"}, {"has_critical_gap": "maybe"},
])
def test_search_rejects_invalid_input(cfg, gold_lake, params):
    with pytest.raises(ValueError):
        search_employees(cfg, gold_lake, params)


# ----------------------------------------------------------------- handler
@pytest.fixture
def routed(monkeypatch, cfg, gold_lake):
    monkeypatch.setattr(service.Config, "from_env", lambda: cfg)
    monkeypatch.setattr(service, "DataLake", lambda c: gold_lake)


def url_event(**query):
    return {"requestContext": {"http": {"method": "GET"}}, "queryStringParameters": query}


def test_handler_serves_dashboard_over_public_url(routed):
    resp = service.handler(url_event(action="dashboard"), None)
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["kpis"]["employees"] == 4


def test_handler_search_returns_400_on_bad_input(routed):
    resp = service.handler(url_event(action="search", min_rating="9"), None)
    assert resp["statusCode"] == 400


def test_handler_search_ok(routed):
    resp = service.handler(url_event(action="search", department="Engineering"), None)
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["total"] == 2


# ------------------------------------------------------------ table viewer
from lib.dashboard_api import PREVIEW_MAX, preview_table  # noqa: E402


class FakeAurora:
    def read_table(self, table):
        return pd.DataFrame({"employee_id": ["1", "2", "3"], "first_name": ["A", "B", "C"],
                             "email": ["a@x", "b@x", "c@x"]}).astype("string")


def test_table_from_aurora_hides_email_and_limits_rows(cfg, gold_lake):
    result = preview_table(cfg, gold_lake, {"source": "aurora", "table": "employees", "limit": "2"}, pg=FakeAurora())
    assert result["source"] == "aurora" and result["total_rows"] == 3
    assert "email" not in result["columns"] and len(result["rows"]) == 2


def test_table_from_gold(cfg, gold_lake):
    result = preview_table(cfg, gold_lake, {"source": "gold", "table": "gold_employee_profile"})
    assert result["total_rows"] == 4 and result["rows"][0]["employee_name"] == "Asha Rao"


def test_table_bronze_reads_latest_snapshot_only(cfg, gold_lake):
    old = pd.DataFrame({"team_id": ["1"]})
    new = pd.DataFrame({"team_id": ["1", "2"]})
    gold_lake.write_parquet(old, "bronze/teams/year=2026/month=10/day=14/data.parquet")
    gold_lake.write_parquet(new, "bronze/teams/year=2026/month=10/day=15/data.parquet")
    assert preview_table(cfg, gold_lake, {"source": "bronze", "table": "teams"})["total_rows"] == 2


@pytest.mark.parametrize("params", [
    {"source": "aurora", "table": "pg_shadow"},           # not whitelisted
    {"source": "gold", "table": "employees"},             # wrong layer
    {"source": "secret", "table": "teams"},
    {"source": "gold", "table": "gold_employee_profile", "limit": str(PREVIEW_MAX + 1)},
])
def test_table_rejects_anything_not_whitelisted(cfg, gold_lake, params):
    with pytest.raises(ValueError):
        preview_table(cfg, gold_lake, params, pg=FakeAurora())


def test_handler_table_over_public_url(routed):
    resp = service.handler(url_event(action="table", source="gold", table="gold_employee_profile", limit="1"), None)
    assert resp["statusCode"] == 200 and len(json.loads(resp["body"])["rows"]) == 1
    assert service.handler(url_event(action="table", source="aurora", table="pg_user"), None)["statusCode"] == 400