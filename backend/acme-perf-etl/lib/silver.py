"""Silver layer: validate, clean and conform Bronze data; quarantine bad records."""
from __future__ import annotations

import json
import logging

import pandas as pd

from .bronze import partition_of
from .config import EVENT_DATASET, SOURCE_TABLES, Config
from .contracts import CONTRACTS
from .logging_utils import RunAudit, get_logger, log_event, track_step
from .storage import DataLake
from .validation import REJECT_REASON, quarantine_where, validate

logger = get_logger(__name__)

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PERIOD_PATTERN = r"^\d{4}-H[12]$"


# ---------------------------------------------------------------- reading
def read_bronze(cfg: Config, lake: DataLake, dataset: str) -> pd.DataFrame:
    """Tables: this run's snapshot (or the latest available). Events: all partitions."""
    root = f"{cfg.bronze_prefix}/{dataset}/"
    if dataset == EVENT_DATASET:
        df = lake.read_parquet_prefix(root)
    else:
        partitions = sorted({partition_of(k) for k in lake.list_keys(root) if k.endswith(".parquet")})
        if not partitions:
            raise FileNotFoundError(f"No Bronze data found for '{dataset}'")
        chosen = cfg.run_partition if cfg.run_partition in partitions else partitions[-1]
        df = lake.read_parquet_prefix(f"{root}{chosen}/")
    if df.empty:
        raise FileNotFoundError(f"Bronze dataset '{dataset}' is empty")
    return df


def parse_events(bronze_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse raw JSON lines. Returns (parsed rows as strings, malformed rows)."""
    lineage = [c for c in bronze_df.columns if c.startswith("_")]
    good_idx, objects, bad_idx = [], [], []
    for idx, raw in bronze_df["raw_record"].items():
        try:
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                raise ValueError("not a JSON object")
            good_idx.append(idx)
            objects.append(obj)
        except (TypeError, ValueError):
            bad_idx.append(idx)

    parsed = pd.DataFrame(objects, index=good_idx).astype("string")
    parsed = parsed.join(bronze_df.loc[good_idx, lineage])
    # Lineage for schema drift: record which field the free text came from
    parsed["_text_field"] = "text"
    if "notes" in parsed.columns:
        parsed.loc[parsed["notes"].notna(), "_text_field"] = "notes"

    malformed = bronze_df.loc[bad_idx].copy()
    malformed[REJECT_REASON] = "malformed JSON record"
    return parsed.reset_index(drop=True), malformed.reset_index(drop=True)


# ---------------------------------------------------------- business rules
def apply_business_rules(
    name: str, df: pd.DataFrame, ref: dict[str, pd.DataFrame], cfg: Config
) -> tuple[pd.DataFrame, list[pd.DataFrame]]:
    """Cross-row and cross-table rules applied after the schema contract passes."""
    rejected: list[pd.DataFrame] = []

    def rule(mask: pd.Series, reason: str) -> None:
        nonlocal df
        df, bad = quarantine_where(df, mask, reason)
        if len(bad):
            rejected.append(bad)

    def hire_dates() -> pd.Series:
        return df["employee_id"].map(ref["employees"].set_index("employee_id")["hire_date"])

    if name == "employees":
        rule(~df["team_id"].isin(ref["teams"]["team_id"]), "team_id not found in teams")
        rule(~df["email"].str.match(EMAIL_PATTERN, na=False), "invalid email format")

    elif name == "projects":
        rule(~df["team_id"].isin(ref["teams"]["team_id"]), "team_id not found in teams")

    elif name == "performance_reviews":
        rule(~df["employee_id"].isin(ref["employees"]["employee_id"]), "employee_id not found in employees")
        rule(~df["review_period"].str.match(PERIOD_PATTERN, na=False), "review_period not in YYYY-H1/H2 format")
        rule(df["submitted_date"] < hire_dates(), "review submitted before employee hire date")
        rule(df.duplicated(["employee_id", "review_period"], keep="first"),
             "duplicate review for employee and period")

    elif name == "competency_assessments":
        rule(~df["employee_id"].isin(ref["employees"]["employee_id"]), "employee_id not found in employees")
        rule(~df["competency_id"].isin(ref["competencies"]["competency_id"]),
             "competency_id not found in competencies")
        rule(df["assessed_date"] < hire_dates(), "assessment dated before employee hire date")
        rule(df.duplicated(["employee_id", "competency_id"], keep="first"),
             "duplicate assessment for employee and competency")

    elif name == "talent_review":
        rule(~df["employee_id"].isin(ref["employees"]["employee_id"]), "employee_id not found in employees")
        # Contradictory labels are kept but flagged for analysts, not dropped
        df["is_label_inconsistent"] = (df["potential_rating"] == "Low") & (
            df["promotion_readiness"] == "Ready Now"
        )

    elif name == EVENT_DATASET:
        run_time = pd.Timestamp(cfg.run_ts).tz_convert(None)
        rule(~df["employee_id"].isin(ref["employees"]["employee_id"]), "employee_id not found in employees")
        rule(df["event_date"] < hire_dates(), "event dated before employee hire date")
        rule(df["event_date"] > run_time, "event dated in the future")

    return df, rejected


# ----------------------------------------------------------------- writing
def write_silver(cfg: Config, lake: DataLake, name: str, df: pd.DataFrame) -> int:
    """Overwrite the Silver dataset (idempotent). Events are partitioned by year."""
    df = df.copy()
    df["_silver_processed_at"] = cfg.run_ts
    prefix = f"{cfg.silver_prefix}/{name}"
    if name != EVENT_DATASET:
        return lake.overwrite_dataset(df, prefix)
    lake.delete_prefix(prefix)
    written = 0
    for year, part in df.groupby(df["event_date"].dt.year):
        written += lake.write_parquet(part, f"{prefix}/event_year={int(year)}/data.parquet")
    return written


def write_quarantine(cfg: Config, lake: DataLake, name: str, frames: list[pd.DataFrame]) -> int:
    """Write rejected rows (all columns as text) with reasons for troubleshooting."""
    prefix = f"{cfg.quarantine_prefix}/{name}/{cfg.run_partition}"
    frames = [f for f in frames if len(f)]
    if not frames:
        lake.delete_prefix(prefix)
        return 0
    q = pd.concat(frames, ignore_index=True).astype("string")
    q["_quarantined_at"] = cfg.run_ts
    q["_run_id"] = cfg.run_id
    return lake.overwrite_dataset(q, prefix)


# --------------------------------------------------------------------- run
def process_dataset(
    cfg: Config, lake: DataLake, audit: RunAudit, name: str, ref: dict[str, pd.DataFrame]
) -> None:
    with track_step(logger, audit, "silver", name) as m:
        bronze_df = read_bronze(cfg, lake, name)
        m["rows_read"] = len(bronze_df)
        rejected: list[pd.DataFrame] = []

        if name == EVENT_DATASET:
            df, malformed = parse_events(bronze_df)
            rejected.append(malformed)
            m["schema_drift_rows"] = int((df["_text_field"] == "notes").sum())
        else:
            df = bronze_df

        valid, contract_rejects = validate(df, CONTRACTS[name])
        rejected.append(contract_rejects)
        valid, rule_rejects = apply_business_rules(name, valid, ref, cfg)
        rejected.extend(rule_rejects)

        m["rows_quarantined"] = write_quarantine(cfg, lake, name, rejected)
        m["rows_written"] = write_silver(cfg, lake, name, valid)
        reasons = pd.concat([r[REJECT_REASON] for r in rejected if len(r)] or [pd.Series(dtype="string")])
        m["reject_reasons"] = {str(k): int(v) for k, v in reasons.value_counts().items()}

        if m["rows_read"] != m["rows_written"] + m["rows_quarantined"]:
            log_event(logger, "reconciliation_mismatch", logging.WARNING, dataset=name,
                      rows_read=m["rows_read"], rows_written=m["rows_written"],
                      rows_quarantined=m["rows_quarantined"])
        ref[name] = valid


def run(cfg: Config, lake: DataLake, audit: RunAudit) -> None:
    """Process parents before children so referential checks use validated data."""
    ref: dict[str, pd.DataFrame] = {}
    for name in (*SOURCE_TABLES, EVENT_DATASET):
        try:
            process_dataset(cfg, lake, audit, name, ref)
        except Exception:  # details already logged by track_step
            log_event(logger, "dataset_skipped", logging.WARNING, layer="silver", dataset=name)