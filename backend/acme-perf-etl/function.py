"""ACME Performance & Talent ETL service: Bronze -> Silver -> Gold, using Pandas.

Built from backend/_examples/python-service. Runs two ways:
  * Lambda (LocalStack or AWS): handler(event, context), deployed by bin/deploy-backend.sh
  * Terminal: python function.py [all|bronze|silver|gold]

Lambda requests choose stages with ?stage=bronze (query string) or {"stage": "bronze"}
(JSON body). Optional: "full_refresh": true, "bucket": "<data-lake-bucket>".
"""
from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

from lib import bronze, gold, silver
from lib.config import Config
from lib.logging_utils import RunAudit, get_logger, log_event
from lib.storage import DataLake, PostgresSource

logger = get_logger("acme-perf-etl")

DEFAULT_BUCKET = "acme-perf-datalake-local"
ALL_STAGES = ("bronze", "silver", "gold")


def ingest_into_bronze(cfg: Config, lake: DataLake, audit: RunAudit, full_refresh: bool) -> None:
    """Ingest raw data from PostgreSQL and the S3 landing zone into the bronze layer."""
    bronze.run(cfg, PostgresSource(cfg), lake, audit, full_refresh=full_refresh)


def transform_to_silver(cfg: Config, lake: DataLake, audit: RunAudit) -> None:
    """Validate, clean, and conform bronze data into the silver layer."""
    silver.run(cfg, lake, audit)


def aggregate_to_gold(cfg: Config, lake: DataLake, audit: RunAudit) -> None:
    """Build business-ready analytics tables in the gold layer."""
    gold.run(cfg, lake, audit)


def run_pipeline(stages: list[str], full_refresh: bool = False) -> dict[str, Any]:
    """Run the requested stages in order; always persist the run audit record."""
    cfg = Config.from_env()
    audit = RunAudit(cfg.run_id, cfg.run_ts)
    lake = DataLake(cfg)
    log_event(logger, "job_started", stages=stages, full_refresh=full_refresh, **cfg.describe())
    try:
        for stage in stages:
            if stage == "bronze":
                ingest_into_bronze(cfg, lake, audit, full_refresh)
            elif stage == "silver":
                transform_to_silver(cfg, lake, audit)
            elif stage == "gold":
                aggregate_to_gold(cfg, lake, audit)
    except Exception as exc:
        log_event(logger, "job_crashed", logging.ERROR, error=f"{type(exc).__name__}: {exc}")
        audit.steps.append({"layer": "job", "dataset": "-", "status": "FAILED", "error": str(exc)})
    finally:
        lake.write_json(audit.to_dict(), f"{cfg.audit_prefix}/runs/{cfg.run_date}/{cfg.run_id}.json")
        log_event(logger, "job_finished", run_id=cfg.run_id, status=audit.status, steps=len(audit.steps))
    return audit.to_dict()


def parse_request(event: dict | None) -> tuple[list[str], bool]:
    """Read stage, full_refresh and bucket from a Function URL, API, or direct invocation."""
    event = event or {}
    params: dict[str, Any] = dict(event.get("queryStringParameters") or {})
    body = event.get("body")
    if body:
        try:
            params.update(json.loads(body))
        except (TypeError, ValueError):
            pass
    for key in ("stage", "full_refresh", "bucket"):
        if key in event:
            params.setdefault(key, event[key])

    stage = str(params.get("stage", "all")).lower()
    stages = list(ALL_STAGES) if stage == "all" else [s.strip() for s in stage.split(",") if s.strip()]
    invalid = [s for s in stages if s not in ALL_STAGES]
    if invalid:
        raise ValueError(f"Unknown stage(s) {invalid}; use one of {list(ALL_STAGES)} or 'all'")

    if params.get("bucket"):
        os.environ["DATA_LAKE_BUCKET"] = str(params["bucket"])
    full_refresh = str(params.get("full_refresh", "false")).lower() == "true"
    return stages, full_refresh


def response(status: int, payload: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def handler(event: dict | None, context: Any) -> dict:
    """Lambda entry point."""
    os.environ.setdefault("DATA_LAKE_BUCKET", DEFAULT_BUCKET)
    try:
        stages, full_refresh = parse_request(event)
    except ValueError as exc:
        return response(400, {"error": str(exc)})
    try:
        result = run_pipeline(stages, full_refresh)
    except EnvironmentError as exc:
        return response(500, {"error": f"configuration error: {exc}"})
    return response(200 if result["status"] == "SUCCESS" else 500, result)


if __name__ == "__main__":
    stage_arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    result = handler({"stage": stage_arg, "full_refresh": os.getenv("FULL_REFRESH", "false")}, None)
    print(json.dumps(json.loads(result["body"]), indent=2))
    sys.exit(0 if result["statusCode"] == 200 else 1)