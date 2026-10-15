"""ACME Performance & Talent ETL service: Bronze -> Silver -> Gold, using Pandas.

Built from backend/_examples/python-service. Runs two ways:
  * Lambda (LocalStack or AWS): handler(event, context), deployed by bin/deploy-backend.sh
  * Terminal: python function.py [all|bronze|silver|gold|load_sources]

Actions (query string ?action=..., JSON body, or direct-invocation field):
  run (default)  run the pipeline; ?stage=all|bronze|silver|gold, &full_refresh=true
  dashboard      KPIs and chart data from the Gold layer (read-only, public)
  search         employee search with whitelisted filters (read-only, public)
  table          preview of a whitelisted table from Aurora or a lake layer (read-only, public)
  sql            one read-only SQL query on the source database (IAM-signed invoke only)
  load_sources   load the HR source tables into Aurora (IAM-signed invoke only)
"""
from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

# LocalStack hot-reload mounts this folder as-is, without the pip-installed packages from the zip.
# Dependencies are vendored into the hidden .deps/ folder for that case only; the AWS zip excludes
# dot-folders and installs requirements.txt itself, so this path is never used there.
_DEPS = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".deps")
if os.getenv("AWS_LAMBDA_FUNCTION_NAME") and os.path.isdir(_DEPS):
    sys.path.insert(0, _DEPS)

from lib import bronze, gold, silver  # noqa: E402
from lib.admin_sql import run_readonly_sql  # noqa: E402
from lib.config import Config  # noqa: E402
from lib.dashboard_api import dashboard_summary, preview_table, search_employees  # noqa: E402
from lib.logging_utils import RunAudit, get_logger, log_event  # noqa: E402
from lib.source_loader import load_sources  # noqa: E402
from lib.storage import DataLake, PostgresSource  # noqa: E402

logger = get_logger("acme-perf-etl")

ALL_STAGES = ("bronze", "silver", "gold")
URL_EVENT_KEYS = {"version", "routeKey", "rawPath", "rawQueryString", "headers", "cookies", "requestContext",
                  "body", "isBase64Encoded", "queryStringParameters", "pathParameters", "stageVariables"}


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
    """Run the requested stages in order; always try to persist the run audit record."""
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
        try:
            lake.write_json(audit.to_dict(), f"{cfg.audit_prefix}/runs/{cfg.run_date}/{cfg.run_id}.json")
        except Exception as exc:  # e.g. bucket missing: report it, don't crash the response
            log_event(logger, "audit_write_failed", logging.ERROR, error=f"{type(exc).__name__}: {exc}")
        log_event(logger, "job_finished", run_id=cfg.run_id, status=audit.status, steps=len(audit.steps))
    return audit.to_dict()


def _request_params(event: dict | None) -> dict[str, Any]:
    """Merge query string, JSON body and direct-invocation fields into one dict."""
    event = event or {}
    params: dict[str, Any] = dict(event.get("queryStringParameters") or {})
    body = event.get("body")
    if body:
        try:
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                params.update(parsed)
        except (TypeError, ValueError):
            pass
    # Direct invocations pass parameters as top-level fields (Function URL events use the query string)
    for key, value in event.items():
        if key not in URL_EVENT_KEYS and isinstance(value, (str, int, float, bool)):
            params.setdefault(key, value)
    return params


def parse_request(event: dict | None) -> tuple[list[str], bool]:
    """Read stage, full_refresh and bucket from a Function URL, API, or direct invocation."""
    params = _request_params(event)
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
    """Lambda entry point. Always answers with JSON, even on unexpected errors."""
    try:
        params = _request_params(event)
        action = str(params.get("action", "run")).lower()
        via_public_url = "requestContext" in (event or {})

        if action in ("load_sources", "sql") and via_public_url:
            return response(403, {"error": f"{action} must be run with an IAM-signed 'aws lambda invoke'"})

        if action == "load_sources":
            if params.get("bucket"):
                os.environ["DATA_LAKE_BUCKET"] = str(params["bucket"])
            cfg = Config.from_env()
            return response(200, load_sources(cfg, DataLake(cfg)))

        if action == "sql":  # operator-only, read-only SQL on the source database
            try:
                return response(200, run_readonly_sql(Config.from_env(), params.get("query")))
            except ValueError as exc:
                return response(400, {"error": str(exc)})

        if action in ("dashboard", "search", "table"):  # read-only views for the public dashboard
            cfg = Config.from_env()
            lake = DataLake(cfg)
            readers = {"dashboard": lambda: dashboard_summary(cfg, lake),
                       "search": lambda: search_employees(cfg, lake, params),
                       "table": lambda: preview_table(cfg, lake, params)}
            try:
                payload = readers[action]()
            except ValueError as exc:
                return response(400, {"error": str(exc)})
            return response(200, payload)

        if action != "run":
            return response(400, {"error": f"Unknown action '{action}'; use 'run', 'dashboard', 'search', "
                                           "'table', 'sql' or 'load_sources'"})
        try:
            stages, full_refresh = parse_request(event)
        except ValueError as exc:
            return response(400, {"error": str(exc)})
        result = run_pipeline(stages, full_refresh)
        return response(200 if result["status"] == "SUCCESS" else 500, result)
    except Exception as exc:  # last resort: never a bare "Internal Server Error"
        log_event(logger, "handler_crashed", logging.ERROR, error=f"{type(exc).__name__}: {exc}")
        return response(500, {"error": f"{type(exc).__name__}: {exc}"})


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    event = ({"action": "load_sources"} if arg == "load_sources"
             else {"stage": arg, "full_refresh": os.getenv("FULL_REFRESH", "false")})
    result = handler(event, None)
    print(json.dumps(json.loads(result["body"]), indent=2))
    sys.exit(0 if result["statusCode"] == 200 else 1)