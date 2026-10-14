"""Unit tests for Bronze, Silver and Gold logic using small hand-built data with known answers."""
import pandas as pd
import pytest

from lib import gold
from lib.bronze import add_lineage, partition_of
from lib.logging_utils import RunAudit
from lib.silver import apply_business_rules, parse_events
from lib.validation import REJECT_REASON


def ts(value):
    return pd.Timestamp(value)


def ints(*values):
    return pd.array(list(values), dtype="Int64")


def reasons_of(rejected):
    return pd.concat(rejected)[REJECT_REASON].tolist() if rejected else []


# ------------------------------------------------------------------ Bronze
def test_partition_of_extracts_hive_partition():
    key = "landing/learning_activity/year=2025/month=06/day=02/events-000.jsonl"
    assert partition_of(key) == "year=2025/month=06/day=02"


def test_add_lineage_adds_metadata_without_mutating_input(cfg):
    df = pd.DataFrame({"a": [1]})
    out = add_lineage(df, cfg, "postgres", "public.teams")
    assert {"_source_system", "_source_object", "_ingested_at", "_run_id"} <= set(out.columns)
    assert out["_run_id"].iloc[0] == "testrun"
    assert list(df.columns) == ["a"]


# ------------------------------------------------------------------ Silver
def test_parse_events_separates_malformed_and_tracks_drift():
    bronze = pd.DataFrame({
        "raw_record": ['{"event_id": "E1", "text": "hi"}', '{"event_id": "E2", "notes": "drift"}',
                       '{broken', '[1, 2]'],
        "_source_file": ["f.jsonl"] * 4,
    })
    parsed, malformed = parse_events(bronze)
    assert parsed["event_id"].tolist() == ["E1", "E2"]
    assert parsed["_text_field"].tolist() == ["text", "notes"]
    assert len(malformed) == 2
    assert set(malformed[REJECT_REASON]) == {"malformed JSON record"}


@pytest.fixture
def ref():
    return {
        "employees": pd.DataFrame({"employee_id": ints(1, 2),
                                   "hire_date": [ts("2024-01-01"), ts("2025-06-01")]}),
        "teams": pd.DataFrame({"team_id": ints(10)}),
        "competencies": pd.DataFrame({"competency_id": ints(100)}),
    }


def test_employee_rules(cfg, ref):
    df = pd.DataFrame({"employee_id": ints(1, 2, 3), "team_id": ints(10, 10, 99),
                       "email": ["a@b.com", "not-an-email", "c@d.com"]})
    valid, rejected = apply_business_rules("employees", df, ref, cfg)
    assert valid["employee_id"].tolist() == [1]
    assert set(reasons_of(rejected)) == {"team_id not found in teams", "invalid email format"}


def test_review_rules(cfg, ref):
    df = pd.DataFrame({
        "review_id": ints(1, 2, 3, 4, 5),
        "employee_id": ints(1, 1, 2, 9, 1),
        "review_period": ["2024-H2", "2024-H2", "2024-H2", "2024-H2", "2025 first half"],
        "submitted_date": [ts("2025-03-01"), ts("2025-03-02"), ts("2025-02-01"),
                           ts("2025-03-01"), ts("2025-08-01")],
    })
    valid, rejected = apply_business_rules("performance_reviews", df, ref, cfg)
    assert valid["review_id"].tolist() == [1]
    assert set(reasons_of(rejected)) == {
        "employee_id not found in employees",
        "review_period not in YYYY-H1/H2 format",
        "review submitted before employee hire date",
        "duplicate review for employee and period",
    }


def test_assessment_rules(cfg, ref):
    df = pd.DataFrame({
        "assessment_id": ints(1, 2, 3, 4),
        "employee_id": ints(1, 1, 1, 2),
        "competency_id": ints(100, 100, 999, 100),
        "assessed_date": [ts("2025-01-01"), ts("2025-02-01"), ts("2025-01-01"), ts("2025-01-01")],
    })
    valid, rejected = apply_business_rules("competency_assessments", df, ref, cfg)
    assert valid["assessment_id"].tolist() == [1]
    assert set(reasons_of(rejected)) == {
        "competency_id not found in competencies",
        "assessment dated before employee hire date",
        "duplicate assessment for employee and competency",
    }


def test_talent_review_flags_but_keeps_contradictions(cfg, ref):
    df = pd.DataFrame({"employee_id": ints(1, 2), "potential_rating": ["Low", "High"],
                       "promotion_readiness": ["Ready Now", "Ready Now"]})
    valid, rejected = apply_business_rules("talent_review", df, ref, cfg)
    assert len(valid) == 2 and rejected == []
    assert valid["is_label_inconsistent"].tolist() == [True, False]


def test_event_rules(cfg, ref):
    df = pd.DataFrame({
        "event_id": ["E1", "E2", "E3", "E4"],
        "employee_id": ints(1, 1, 2, 7),
        "event_date": [ts("2025-01-01"), ts("2027-01-01"), ts("2025-01-01"), ts("2025-01-01")],
    })
    valid, rejected = apply_business_rules("learning_activity", df, ref, cfg)
    assert valid["event_id"].tolist() == ["E1"]
    assert set(reasons_of(rejected)) == {
        "employee_id not found in employees",
        "event dated before employee hire date",
        "event dated in the future",
    }


# -------------------------------------------------------------------- Gold
@pytest.fixture
def dim():
    return pd.DataFrame({
        "employee_id": ints(1, 2, 3, 4),
        "employee_name": ["A", "B", "C", "D"],
        "job_level": ["IC1"] * 4,
        "hire_date": [ts("2020-01-01")] * 4,
        "team_id": ints(10, 10, 20, 20),
        "team_name": ["T1", "T1", "T2", "T2"],
        "department": ["Eng", "Eng", "Ops", "Ops"],
    })


def reviews():
    return pd.DataFrame({
        "review_id": ints(1, 2, 3, 4, 5),
        "employee_id": ints(1, 1, 2, 2, 3),
        "review_period": ["2024-H2", "2025-H1", "2024-H2", "2025-H1", "2025-H1"],
        "rating": ints(3, 4, 4, 2, 5),
        "manager_comment": ["Met expectations", "Consistently exceeded goals", "Solid",
                            "Missed key deliverables", "Fine"],
        "submitted_date": [ts("2025-08-01")] * 5,
    })


def test_comment_sentiment():
    s = pd.Series(["Consistently exceeded goals", "Missed several deliverables", "Met expectations", None])
    assert gold.comment_sentiment(s).tolist() == ["positive", "negative", "neutral", "neutral"]


def test_performance_trends_and_rating_change(dim):
    history = gold.build_review_history({"performance_reviews": reviews()}, dim)
    perf = gold.build_performance(history, dim).set_index("employee_id")
    assert perf.loc[1, "performance_trend"] == "improving" and perf.loc[1, "rating_change"] == 1
    assert perf.loc[1, "avg_rating"] == 3.5
    assert perf.loc[2, "performance_trend"] == "declining" and perf.loc[2, "latest_rating"] == 2
    assert perf.loc[3, "performance_trend"] == "single review"
    assert perf.loc[4, "performance_trend"] == "no reviews" and perf.loc[4, "review_count"] == 0
    assert history.loc[history["review_id"] == 2, "previous_rating"].iloc[0] == 3


def test_skill_gaps_and_distribution(dim):
    s = {
        "competencies": pd.DataFrame({"competency_id": ints(100), "name": ["Python"], "category": ["Technical"]}),
        "competency_assessments": pd.DataFrame({
            "assessment_id": ints(1, 2, 3), "employee_id": ints(1, 2, 3), "competency_id": ints(100, 100, 100),
            "proficiency_level": ints(1, 3, 5), "required_level": ints(4, 4, 4),
            "assessed_date": [ts("2025-01-01")] * 3,
        }),
    }
    gaps = gold.build_skill_gaps(s, dim)
    assert gaps["gap"].tolist() == [3, 1, -1]
    assert gaps["gap_severity"].tolist() == ["critical", "minor", "meets or exceeds"]
    assert gaps["is_critical_gap"].tolist() == [True, False, False]
    assert gold.build_skill_distribution(gaps)["employee_count"].sum() == 3


def test_training_summary_counts_and_rates(dim):
    ev = pd.DataFrame({
        "employee_id": ints(1, 1, 1, 1, 2),
        "event_type": ["enrollment", "course_completed", "quiz_attempted", "quiz_passed", "enrollment"],
        "course_name": ["C1"] * 5,
        "score": [None, None, 60.0, 80.0, None],
        "event_date": [ts("2026-01-01")] * 4 + [ts("2024-01-01")],
    })
    t = gold.build_training({"learning_activity": ev}, dim).set_index("employee_id")
    assert t.loc[1, "enrollments"] == 1 and t.loc[1, "courses_completed"] == 1
    assert t.loc[1, "avg_quiz_score"] == 70.0
    assert t.loc[1, "quiz_pass_rate"] == 1.0 and t.loc[1, "course_completion_rate"] == 1.0
    assert t.loc[2, "events_last_180d"] == 0 and not t.loc[2, "has_recent_training"]
    assert t.loc[3, "enrollments"] == 0


def test_profile_attrition_score_and_promotion(dim):
    perf = gold.build_performance(gold.build_review_history({"performance_reviews": reviews()}, dim), dim)
    gaps = pd.DataFrame({"employee_id": ints(3), "gap": ints(2), "is_critical_gap": [True]})
    ev = pd.DataFrame({"employee_id": ints(1), "event_type": ["enrollment"], "course_name": ["C"],
                       "score": [None], "event_date": [ts("2026-01-01")]})
    training = gold.build_training({"learning_activity": ev}, dim)
    talent = pd.DataFrame({
        "employee_id": ints(1, 2, 3, 4),
        "potential_rating": ["High", "Low", "Medium", "Low"],
        "promotion_readiness": ["Ready Now", "Ready Now", "Not Ready", "Not Ready"],
        "attrition_risk": ["Low", "High", "Low", "Low"],
        "is_label_inconsistent": [False, True, False, False],
    })
    p = gold.build_profile(perf, gaps, training, talent).set_index("employee_id")

    # Employee 2: drop of 2 (+3), latest rating 2 (+2), negative comment (+1), no training (+1)
    assert p.loc[2, "attrition_risk_score"] == 7 and p.loc[2, "derived_attrition_risk"] == "High"
    assert p.loc[1, "attrition_risk_score"] == 0 and p.loc[1, "derived_attrition_risk"] == "Low"
    assert p.loc[1, "promotion_status"] == "Ready now - high potential"
    assert p.loc[3, "critical_gap_count"] == 1 and not p.loc[3, "derived_promotion_ready"]
    assert not p.loc[2, "readiness_label_agrees"]


def test_team_performance_joins_projects():
    profile = pd.DataFrame({
        "employee_id": ints(1, 2, 3, 4), "team_id": ints(10, 10, 20, 20),
        "latest_rating": ints(4, 2, 5, None),
        "performance_trend": ["improving", "declining", "single review", "no reviews"],
        "derived_attrition_risk": ["Low", "High", "Low", "Low"],
        "avg_skill_gap": [0.0, 1.0, 2.0, None],
    })
    s = {
        "teams": pd.DataFrame({"team_id": ints(10, 20), "name": ["T1", "T2"], "department": ["Eng", "Ops"]}),
        "projects": pd.DataFrame({"project_id": ints(1, 2), "team_id": ints(10, 10),
                                  "outcome_status": ["on-time", "delayed"], "outcome_score": [80.0, 60.0]}),
    }
    t = gold.build_team_performance(s, profile).set_index("team_id")
    assert t.loc[10, "headcount"] == 2 and t.loc[10, "avg_latest_rating"] == 3.0
    assert t.loc[10, "pct_high_performers"] == 0.5 and t.loc[10, "pct_projects_on_time"] == 0.5
    assert t.loc[10, "avg_outcome_score"] == 70.0
    assert t.loc[20, "project_count"] == 0 and not t.loc[20, "has_projects"]


def test_publish_writes_table_with_lineage(cfg, lake):
    audit = RunAudit("r", "t")
    gold.publish(cfg, lake, audit, "gold_x", pd.DataFrame({"employee_id": [1, 2]}), ["employee_id"], 2, 2)
    out = lake.read_parquet("gold/gold_x/data.parquet")
    assert len(out) == 2 and out["_run_id"].iloc[0] == "testrun"
    assert audit.steps[-1]["status"] == "SUCCESS"


def test_publish_rejects_grain_violation(cfg, lake):
    audit = RunAudit("r", "t")
    with pytest.raises(ValueError, match="violate grain"):
        gold.publish(cfg, lake, audit, "gold_x", pd.DataFrame({"employee_id": [1, 1]}), ["employee_id"], 2, 2)
    assert audit.steps[-1]["status"] == "FAILED"