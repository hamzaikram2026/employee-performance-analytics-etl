"""Gold layer: business-ready analytics tables, one per business question.

Every table declares its grain (the columns that identify one row); the grain
is enforced on every run and row counts are reconciled against Silver.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .config import EVENT_DATASET, SOURCE_TABLES, Config
from .logging_utils import RunAudit, get_logger, log_event, track_step
from .storage import DataLake

logger = get_logger(__name__)

# Business rule thresholds (documented assumptions)
CRITICAL_GAP = 2            # required level minus proficiency of 2+ levels is critical
RECENT_DAYS = 180           # training activity window, relative to the latest event
NEGATIVE_WORDS = ("trended down", "missed", "struggled", "needs support")
POSITIVE_WORDS = ("exceeded", "upward trend", "stretch")
EVENT_COLUMNS = {
    "enrollment": "enrollments",
    "module_started": "modules_started",
    "module_completed": "modules_completed",
    "quiz_attempted": "quizzes_attempted",
    "quiz_passed": "quizzes_passed",
    "course_completed": "courses_completed",
    "certification_earned": "certifications_earned",
}


def as_bool(mask: pd.Series) -> np.ndarray:
    """Nullable boolean -> plain numpy bool, treating missing as False."""
    return pd.Series(mask).fillna(False).to_numpy(dtype=bool)


# ------------------------------------------------------------------ inputs
def load_silver(cfg: Config, lake: DataLake) -> dict[str, pd.DataFrame]:
    data = {}
    for name in (*SOURCE_TABLES, EVENT_DATASET):
        df = lake.read_parquet_prefix(f"{cfg.silver_prefix}/{name}/")
        if df.empty:
            raise FileNotFoundError(f"Silver dataset '{name}' is missing or empty; run Silver first")
        data[name] = df.drop(columns=[c for c in df.columns if c.startswith("_")])
    return data


def employee_dim(s: dict[str, pd.DataFrame]) -> pd.DataFrame:
    teams = s["teams"].rename(columns={"name": "team_name"})
    e = s["employees"].merge(teams, on="team_id", how="left")
    e["employee_name"] = e["first_name"] + " " + e["last_name"]
    return e[["employee_id", "employee_name", "job_level", "hire_date", "team_id", "team_name", "department"]]


def comment_sentiment(comments: pd.Series) -> pd.Series:
    """Keyword-based sentiment of manager comments: positive / neutral / negative."""
    text = comments.fillna("").str.lower()
    out = pd.Series("neutral", index=comments.index)
    out[as_bool(text.str.contains("|".join(POSITIVE_WORDS), na=False))] = "positive"
    out[as_bool(text.str.contains("|".join(NEGATIVE_WORDS), na=False))] = "negative"
    return out


# ---------------------------------------------------------------- builders
def build_review_history(s: dict, dim: pd.DataFrame) -> pd.DataFrame:
    r = s["performance_reviews"].sort_values(["employee_id", "review_period"]).copy()
    r["comment_sentiment"] = comment_sentiment(r["manager_comment"])
    r["previous_rating"] = r.groupby("employee_id")["rating"].shift(1)
    r["rating_change"] = r["rating"] - r["previous_rating"]
    r = r.merge(dim, on="employee_id", how="left")
    return r[["review_id", "employee_id", "employee_name", "team_name", "department", "job_level",
              "review_period", "rating", "previous_rating", "rating_change",
              "comment_sentiment", "manager_comment", "submitted_date"]]


def build_performance(history: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    g = history.sort_values(["employee_id", "review_period"]).groupby("employee_id")
    perf = g.agg(
        review_count=("rating", "size"),
        avg_rating=("rating", "mean"),
        first_rating=("rating", "first"),
        latest_rating=("rating", "last"),
        latest_period=("review_period", "last"),
        latest_sentiment=("comment_sentiment", "last"),
    ).reset_index()
    perf["rating_change"] = (perf["latest_rating"] - perf["first_rating"]).where(perf["review_count"] > 1)
    change = perf["rating_change"].fillna(0)
    perf["performance_trend"] = np.select(
        [as_bool(perf["review_count"] < 2), as_bool(change > 0), as_bool(change < 0)],
        ["single review", "improving", "declining"],
        default="stable",
    )
    perf["avg_rating"] = perf["avg_rating"].astype(float).round(2)
    out = dim.merge(perf.drop(columns="first_rating"), on="employee_id", how="left")
    out["review_count"] = out["review_count"].fillna(0).astype(int)
    out["performance_trend"] = out["performance_trend"].fillna("no reviews")
    return out


def build_skill_gaps(s: dict, dim: pd.DataFrame) -> pd.DataFrame:
    comps = s["competencies"].rename(columns={"name": "competency_name", "category": "competency_category"})
    a = s["competency_assessments"].merge(comps, on="competency_id", how="left")
    a["gap"] = a["required_level"] - a["proficiency_level"]
    a["gap_severity"] = np.select(
        [as_bool(a["gap"] >= CRITICAL_GAP), as_bool(a["gap"] >= 1)],
        ["critical", "minor"],
        default="meets or exceeds",
    )
    a["is_critical_gap"] = as_bool(a["gap"] >= CRITICAL_GAP)
    a = a.merge(dim, on="employee_id", how="left")
    return a[["employee_id", "employee_name", "department", "team_name", "job_level",
              "competency_id", "competency_name", "competency_category",
              "proficiency_level", "required_level", "gap", "gap_severity", "is_critical_gap", "assessed_date"]]


def build_skill_distribution(gaps: pd.DataFrame) -> pd.DataFrame:
    keys = ["department", "competency_category", "competency_name", "proficiency_level"]
    out = gaps.groupby(keys, dropna=False).agg(
        employee_count=("employee_id", "nunique"),
        avg_gap=("gap", "mean"),
    ).reset_index()
    out["avg_gap"] = out["avg_gap"].astype(float).round(2)
    return out


def build_training(s: dict, dim: pd.DataFrame) -> pd.DataFrame:
    ev = s[EVENT_DATASET]
    counts = (
        pd.crosstab(ev["employee_id"], ev["event_type"])
        .reindex(columns=list(EVENT_COLUMNS), fill_value=0)
        .rename(columns=EVENT_COLUMNS)
    )
    quiz = ev[ev["event_type"].isin(["quiz_attempted", "quiz_passed"])]
    avg_quiz = quiz.groupby("employee_id")["score"].mean().rename("avg_quiz_score")
    last = ev.groupby("employee_id")["event_date"].max().rename("last_activity_date")
    distinct = (
        ev[ev["event_type"] == "course_completed"]
        .groupby("employee_id")["course_name"].nunique().rename("distinct_courses_completed")
    )
    reference = ev["event_date"].max()
    recent = (
        ev[ev["event_date"] >= reference - pd.Timedelta(days=RECENT_DAYS)]
        .groupby("employee_id").size().rename("events_last_180d")
    )

    t = dim.set_index("employee_id").join([counts, avg_quiz, last, distinct, recent])
    int_cols = [*EVENT_COLUMNS.values(), "distinct_courses_completed", "events_last_180d"]
    t[int_cols] = t[int_cols].fillna(0).astype(int)
    t["quiz_pass_rate"] = (t["quizzes_passed"] / t["quizzes_attempted"].replace(0, np.nan)).round(3)
    t["course_completion_rate"] = (t["courses_completed"] / t["enrollments"].replace(0, np.nan)).round(3)
    t["avg_quiz_score"] = t["avg_quiz_score"].astype(float).round(1)
    t["has_recent_training"] = t["events_last_180d"] > 0
    t["activity_reference_date"] = reference
    return t.reset_index()


def build_profile(perf: pd.DataFrame, gaps: pd.DataFrame, training: pd.DataFrame,
                  talent: pd.DataFrame) -> pd.DataFrame:
    """One row per employee with derived attrition risk and promotion readiness."""
    crit = gaps.groupby("employee_id").agg(
        critical_gap_count=("is_critical_gap", "sum"), avg_skill_gap=("gap", "mean")
    ).reset_index()
    tal = talent[["employee_id", "potential_rating", "promotion_readiness", "attrition_risk",
                  "is_label_inconsistent"]].rename(columns={
        "promotion_readiness": "talent_promotion_readiness",
        "attrition_risk": "talent_attrition_risk",
    })
    train_cols = ["employee_id", "courses_completed", "certifications_earned",
                  "avg_quiz_score", "events_last_180d", "has_recent_training"]
    p = (perf.merge(crit, on="employee_id", how="left")
             .merge(training[train_cols], on="employee_id", how="left")
             .merge(tal, on="employee_id", how="left"))
    p["critical_gap_count"] = p["critical_gap_count"].fillna(0).astype(int)
    p["avg_skill_gap"] = p["avg_skill_gap"].astype(float).round(2)

    # Attrition risk score (explainable points system)
    change = p["rating_change"].fillna(0)
    latest = p["latest_rating"].fillna(3)
    score = (
        3 * as_bool(change <= -2)                          # sharp rating drop
        + 2 * as_bool(change == -1)                        # rating drop
        + 2 * as_bool(latest <= 2)                         # currently low performer
        + 1 * as_bool(p["latest_sentiment"] == "negative") # negative manager comment
        + 1 * as_bool(p["events_last_180d"] == 0)          # disengaged from learning
    )
    p["attrition_risk_score"] = score
    p["derived_attrition_risk"] = np.select([score >= 4, score >= 2], ["High", "Medium"], default="Low")
    p["attrition_label_agrees"] = as_bool(p["talent_attrition_risk"] == p["derived_attrition_risk"])

    # Promotion readiness (data-driven) vs the talent-review label
    ready = (
        as_bool(latest >= 4)
        & as_bool(p["avg_rating"] >= 3.5)
        & (p["critical_gap_count"].to_numpy() == 0)
        & as_bool(p["performance_trend"] != "declining")
    )
    hipo = as_bool(p["potential_rating"] == "High")
    p["is_high_potential"] = hipo
    p["derived_promotion_ready"] = ready
    p["promotion_status"] = np.select(
        [ready & hipo, ready, hipo],
        ["Ready now - high potential", "Ready now", "High potential - develop"],
        default="Not yet",
    )
    p["readiness_label_agrees"] = as_bool(p["talent_promotion_readiness"] == "Ready Now") == ready
    return p


def build_team_performance(s: dict, profile: pd.DataFrame) -> pd.DataFrame:
    teams = s["teams"].rename(columns={"name": "team_name"})[["team_id", "team_name", "department"]]
    people = profile.assign(
        high_performer=as_bool(profile["latest_rating"] >= 4),
        declining=as_bool(profile["performance_trend"] == "declining"),
        high_attrition=as_bool(profile["derived_attrition_risk"] == "High"),
    )
    emp = people.groupby("team_id").agg(
        headcount=("employee_id", "count"),
        avg_latest_rating=("latest_rating", "mean"),
        pct_high_performers=("high_performer", "mean"),
        pct_declining=("declining", "mean"),
        pct_high_attrition_risk=("high_attrition", "mean"),
        avg_skill_gap=("avg_skill_gap", "mean"),
    ).reset_index()
    proj = s["projects"].assign(on_time=lambda d: as_bool(d["outcome_status"] == "on-time"))
    proj = proj.groupby("team_id").agg(
        project_count=("project_id", "count"),
        avg_outcome_score=("outcome_score", "mean"),
        pct_projects_on_time=("on_time", "mean"),
    ).reset_index()
    t = teams.merge(emp, on="team_id", how="left").merge(proj, on="team_id", how="left")
    t["project_count"] = t["project_count"].fillna(0).astype(int)
    t["has_projects"] = t["project_count"] > 0
    for col in ["avg_latest_rating", "pct_high_performers", "pct_declining",
                "pct_high_attrition_risk", "avg_skill_gap", "avg_outcome_score", "pct_projects_on_time"]:
        t[col] = t[col].astype(float).round(3)
    return t


# ----------------------------------------------------------------- publish
def publish(cfg: Config, lake: DataLake, audit: RunAudit, name: str, df: pd.DataFrame,
            grain: list[str], source_rows: int, expected_rows: int | None = None) -> None:
    """Enforce the grain, reconcile counts, and overwrite the Gold table."""
    with track_step(logger, audit, "gold", name) as m:
        m["rows_read"] = source_rows
        m["grain"] = grain
        duplicates = int(df.duplicated(grain).sum())
        if duplicates:
            raise ValueError(f"{name}: {duplicates} rows violate grain {grain}")
        if expected_rows is not None and len(df) != expected_rows:
            log_event(logger, "grain_count_mismatch", logging.WARNING,
                      dataset=name, expected=expected_rows, actual=len(df))
        out = df.copy()
        out["_gold_built_at"] = cfg.run_ts
        out["_run_id"] = cfg.run_id
        m["rows_written"] = lake.overwrite_dataset(out, f"{cfg.gold_prefix}/{name}")


def run(cfg: Config, lake: DataLake, audit: RunAudit) -> None:
    s = load_silver(cfg, lake)
    log_event(logger, "silver_loaded", tables={k: len(v) for k, v in s.items()})
    n_emp, n_teams = len(s["employees"]), len(s["teams"])

    dim = employee_dim(s)
    history = build_review_history(s, dim)
    perf = build_performance(history, dim)
    gaps = build_skill_gaps(s, dim)
    distribution = build_skill_distribution(gaps)
    training = build_training(s, dim)
    profile = build_profile(perf, gaps, training, s["talent_review"])
    team = build_team_performance(s, profile)

    attrition = profile[[
        "employee_id", "employee_name", "team_name", "department", "job_level",
        "latest_rating", "rating_change", "performance_trend", "latest_sentiment",
        "events_last_180d", "critical_gap_count", "attrition_risk_score",
        "derived_attrition_risk", "talent_attrition_risk", "attrition_label_agrees",
    ]].sort_values("attrition_risk_score", ascending=False)
    promotion = profile[[
        "employee_id", "employee_name", "team_name", "department", "job_level",
        "latest_rating", "avg_rating", "performance_trend", "critical_gap_count",
        "certifications_earned", "potential_rating", "is_high_potential",
        "derived_promotion_ready", "promotion_status", "talent_promotion_readiness",
        "is_label_inconsistent", "readiness_label_agrees",
    ]]

    n_reviews, n_assess = len(s["performance_reviews"]), len(s["competency_assessments"])
    tables = [
        ("gold_review_history", history, ["employee_id", "review_period"], n_reviews, n_reviews),
        ("gold_employee_performance", perf, ["employee_id"], n_reviews, n_emp),
        ("gold_skill_gaps", gaps, ["employee_id", "competency_id"], n_assess, n_assess),
        ("gold_skill_distribution", distribution,
         ["department", "competency_category", "competency_name", "proficiency_level"], len(gaps), None),
        ("gold_training_summary", training, ["employee_id"], len(s[EVENT_DATASET]), n_emp),
        ("gold_attrition_risk", attrition, ["employee_id"], n_emp, n_emp),
        ("gold_promotion_candidates", promotion, ["employee_id"], n_emp, n_emp),
        ("gold_team_performance", team, ["team_id"], n_teams, n_teams),
        ("gold_employee_profile", profile, ["employee_id"], n_emp, n_emp),
    ]
    for name, df, grain, source_rows, expected in tables:
        try:
            publish(cfg, lake, audit, name, df, grain, source_rows, expected)
        except Exception:  # details already logged by track_step
            log_event(logger, "dataset_skipped", logging.WARNING, layer="gold", dataset=name)