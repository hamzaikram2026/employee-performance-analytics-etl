"""Read-only API for the public dashboard: Gold analytics, employee search, table viewer.

Only aggregated figures, whitelisted filters and whitelisted tables are exposed: nothing is
written, no free-form query or code is executed, and personal contact data (email) is never
returned. Every request reads current data, so the dashboard reflects the latest pipeline run.
Aurora is read from inside the VPC with the credentials Terraform injects into the Lambda.
"""
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from .bronze import partition_of
from .config import EVENT_DATASET, SOURCE_TABLES, Config
from .storage import DataLake, PostgresSource

TREND_ORDER = ["improving", "stable", "declining", "single review", "no reviews"]
RISK_ORDER = ["Low", "Medium", "High"]
PROMOTION_ORDER = ["Ready now - high potential", "Ready now", "High potential - develop", "Not yet"]

SEARCH_COLUMNS = [
    "employee_id", "employee_name", "department", "team_name", "job_level",
    "latest_rating", "avg_rating", "performance_trend", "critical_gap_count",
    "courses_completed", "certifications_earned", "derived_attrition_risk",
    "attrition_risk_score", "promotion_status", "potential_rating",
]
# Exact-match filters: request parameter -> Gold column
EXACT_FILTERS = {
    "department": "department",
    "job_level": "job_level",
    "performance_trend": "performance_trend",
    "attrition_risk": "derived_attrition_risk",
    "promotion_status": "promotion_status",
}
SORTS = {
    "name": ("employee_name", True),
    "rating": ("latest_rating", False),
    "risk": ("attrition_risk_score", False),
}
DEFAULT_LIMIT, MAX_LIMIT = 50, 200


# ----------------------------------------------------------------- helpers
def _gold(cfg: Config, lake: DataLake, name: str) -> pd.DataFrame:
    return lake.read_parquet(f"{cfg.gold_prefix}/{name}/data.parquet")


def _counts(series: pd.Series, order: list[str]) -> dict[str, int]:
    counts = series.value_counts()
    return {key: int(counts.get(key, 0)) for key in order}


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """JSON-safe rows: NaN/NA become null, numpy types become plain numbers."""
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _text(params: dict, key: str) -> str | None:
    value = params.get(key)
    if value is None or str(value).strip() == "":
        return None
    return str(value).strip()


def _number(params: dict, key: str, low: float, high: float) -> float | None:
    raw = _text(params, key)
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"'{key}' must be a number") from None
    if not low <= value <= high:
        raise ValueError(f"'{key}' must be between {low:g} and {high:g}")
    return value


# --------------------------------------------------------------- endpoints
def dashboard_summary(cfg: Config, lake: DataLake) -> dict[str, Any]:
    """KPIs, chart series and filter options, computed from the current Gold tables."""
    p = _gold(cfg, lake, "gold_employee_profile")
    teams = _gold(cfg, lake, "gold_team_performance")
    gaps = _gold(cfg, lake, "gold_skill_gaps")

    has_gap = p["critical_gap_count"].fillna(0) > 0
    by_dept = (
        p.assign(has_gap=has_gap, high_risk=p["derived_attrition_risk"] == "High")
        .groupby("department")
        .agg(employees=("employee_id", "count"), avg_rating=("latest_rating", "mean"),
             pct_critical_gap=("has_gap", "mean"), pct_high_risk=("high_risk", "mean"))
        .reset_index()
    )
    by_dept["avg_rating"] = by_dept["avg_rating"].astype(float).round(2)
    for col in ("pct_critical_gap", "pct_high_risk"):
        by_dept[col] = (by_dept[col].astype(float) * 100).round(1)

    with_projects = teams[teams["has_projects"].astype(bool)][
        ["team_name", "department", "headcount", "avg_latest_rating", "avg_outcome_score"]
    ]
    corr = (
        with_projects["avg_latest_rating"].astype(float).corr(with_projects["avg_outcome_score"].astype(float))
        if len(with_projects) >= 3 else float("nan")  # a correlation needs at least 3 teams
    )
    critical = gaps[gaps["is_critical_gap"].astype(bool)]

    return {
        "generated_from": {
            "gold_built_at": str(p["_gold_built_at"].iloc[0]) if "_gold_built_at" in p and len(p) else None,
            "run_id": str(p["_run_id"].iloc[0]) if "_run_id" in p and len(p) else None,
            "bucket": cfg.bucket,
        },
        "kpis": {
            "employees": int(len(p)),
            "improving": int((p["performance_trend"] == "improving").sum()),
            "declining": int((p["performance_trend"] == "declining").sum()),
            "critical_gap_employees": int(has_gap.sum()),
            "ready_now_high_potential": int((p["promotion_status"] == "Ready now - high potential").sum()),
            "high_attrition_risk": int((p["derived_attrition_risk"] == "High").sum()),
            "no_recent_learning": int((~p["has_recent_training"].fillna(False).astype(bool)).sum()),
        },
        "charts": {
            "performance_trend": _counts(p["performance_trend"], TREND_ORDER),
            "attrition_risk": _counts(p["derived_attrition_risk"], RISK_ORDER),
            "promotion_status": _counts(p["promotion_status"], PROMOTION_ORDER),
            "top_critical_competencies": {
                str(k): int(v) for k, v in critical["competency_name"].value_counts().head(10).items()
            },
            "departments": _records(by_dept),
            "teams_with_projects": _records(with_projects),
            "rating_outcome_correlation": None if pd.isna(corr) else round(float(corr), 2),
            "label_agreement_pct": {
                "promotion": round(100 * float(p["readiness_label_agrees"].astype(bool).mean()), 1),
                "attrition": round(100 * float(p["attrition_label_agrees"].astype(bool).mean()), 1),
            },
        },
        "filters": {
            "department": sorted(p["department"].dropna().astype(str).unique().tolist()),
            "job_level": sorted(p["job_level"].dropna().astype(str).unique().tolist()),
            "performance_trend": TREND_ORDER,
            "attrition_risk": RISK_ORDER,
            "promotion_status": PROMOTION_ORDER,
        },
    }


def search_employees(cfg: Config, lake: DataLake, params: dict) -> dict[str, Any]:
    """Filter employees with whitelisted parameters only. Raises ValueError on bad input."""
    p = _gold(cfg, lake, "gold_employee_profile")
    mask = pd.Series(True, index=p.index)

    for key, column in EXACT_FILTERS.items():
        value = _text(params, key)
        if value is not None:
            match = p[column].astype("string").str.lower() == value.lower()
            mask &= match.fillna(False).astype(bool)

    rating = p["latest_rating"].astype("Float64")
    low = _number(params, "min_rating", 1, 5)
    high = _number(params, "max_rating", 1, 5)
    if low is not None:
        mask &= (rating >= low).fillna(False).astype(bool)
    if high is not None:
        mask &= (rating <= high).fillna(False).astype(bool)

    gap = _text(params, "has_critical_gap")
    if gap is not None:
        if gap.lower() not in ("true", "false"):
            raise ValueError("'has_critical_gap' must be true or false")
        has_gap = (p["critical_gap_count"].fillna(0) > 0).astype(bool)
        mask &= has_gap if gap.lower() == "true" else ~has_gap

    name = _text(params, "name")
    if name is not None:
        mask &= p["employee_name"].astype("string").str.contains(name, case=False, regex=False).fillna(False).astype(bool)

    sort = (_text(params, "sort") or "name").lower()
    if sort not in SORTS:
        raise ValueError(f"'sort' must be one of {sorted(SORTS)}")
    limit = int(_number(params, "limit", 1, MAX_LIMIT) or DEFAULT_LIMIT)
    offset = int(_number(params, "offset", 0, 1_000_000) or 0)

    column, ascending = SORTS[sort]
    result = p.loc[mask, SEARCH_COLUMNS].sort_values(
        [column, "employee_id"], ascending=[ascending, True], na_position="last"
    )
    page = result.iloc[offset: offset + limit]
    return {"total": int(len(result)), "limit": limit, "offset": offset, "sort": sort, "rows": _records(page)}


# ------------------------------------------------------------ table viewer
GOLD_TABLES = (
    "gold_review_history", "gold_employee_performance", "gold_skill_gaps", "gold_skill_distribution",
    "gold_training_summary", "gold_attrition_risk", "gold_promotion_candidates",
    "gold_team_performance", "gold_employee_profile",
)
VIEWABLE = {
    "aurora": SOURCE_TABLES,                       # live source database (read inside the VPC)
    "bronze": (*SOURCE_TABLES, EVENT_DATASET),
    "silver": (*SOURCE_TABLES, EVENT_DATASET),
    "gold": GOLD_TABLES,
}
HIDDEN_COLUMNS = {"email"}                         # personal contact data is never served
PREVIEW_DEFAULT, PREVIEW_MAX = 20, 100


def _latest_partition(lake: DataLake, prefix: str) -> pd.DataFrame:
    keys = [k for k in lake.list_keys(prefix) if k.endswith(".parquet")]
    if not keys:
        return pd.DataFrame()
    latest = max(partition_of(k) for k in keys)
    return pd.concat([lake.read_parquet(k) for k in keys if partition_of(k) == latest], ignore_index=True)


def preview_table(cfg: Config, lake: DataLake, params: dict, pg: PostgresSource | None = None) -> dict[str, Any]:
    """Read-only preview of one whitelisted table from Aurora or a lake layer."""
    source = (_text(params, "source") or "gold").lower()
    if source == "postgres":
        source = "aurora"
    if source not in VIEWABLE:
        raise ValueError(f"'source' must be one of {sorted(VIEWABLE)}")
    table = _text(params, "table")
    if table not in VIEWABLE[source]:
        raise ValueError(f"'table' for source '{source}' must be one of {list(VIEWABLE[source])}")
    limit = int(_number(params, "limit", 1, PREVIEW_MAX) or PREVIEW_DEFAULT)

    if source == "aurora":
        df = (pg or PostgresSource(cfg)).read_table(table)
    elif source == "bronze":
        df = _latest_partition(lake, f"{cfg.bronze_prefix}/{table}/")      # latest partition only
    elif source == "gold":
        df = _gold(cfg, lake, table)
    else:
        df = lake.read_parquet_prefix(f"{getattr(cfg, source + '_prefix')}/{table}/")

    df = df.drop(columns=[c for c in df.columns if c in HIDDEN_COLUMNS])
    return {"source": source, "table": table, "total_rows": int(len(df)),
            "columns": list(df.columns), "rows": _records(df.head(limit))}