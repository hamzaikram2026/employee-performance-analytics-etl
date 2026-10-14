"""Structured logging and step-level lineage metrics."""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    """Return a logger; configures the root logger once (level from LOG_LEVEL)."""
    global _CONFIGURED
    if not _CONFIGURED:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
            stream=sys.stdout,
        )
        for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        _CONFIGURED = True
    return logging.getLogger(name)


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    """Emit one structured, machine-parseable log line."""
    logger.log(level, json.dumps({"event": event, **fields}, default=str))


class RunAudit:
    """Collects metrics for every step of one run; persisted as lineage evidence."""

    def __init__(self, run_id: str, run_ts: str) -> None:
        self.run_id = run_id
        self.run_ts = run_ts
        self.steps: list[dict[str, Any]] = []

    @property
    def status(self) -> str:
        return "FAILED" if any(s["status"] == "FAILED" for s in self.steps) else "SUCCESS"

    def to_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "run_ts": self.run_ts, "status": self.status, "steps": self.steps}


@contextmanager
def track_step(
    logger: logging.Logger, audit: RunAudit, layer: str, dataset: str
) -> Iterator[dict[str, Any]]:
    """Time a pipeline step and record its row counts and outcome.

    Usage:
        with track_step(logger, audit, "silver", "employees") as m:
            m["rows_read"] = len(df)
            ...
    """
    metrics: dict[str, Any] = {"rows_read": 0, "rows_written": 0, "rows_quarantined": 0}
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    timer = time.perf_counter()
    status = "SUCCESS"
    log_event(logger, "step_started", layer=layer, dataset=dataset)
    try:
        yield metrics
    except Exception as exc:
        status = "FAILED"
        metrics["error"] = f"{type(exc).__name__}: {exc}"
        log_event(logger, "step_failed", logging.ERROR, layer=layer, dataset=dataset, error=metrics["error"])
        raise
    finally:
        record = {
            "layer": layer,
            "dataset": dataset,
            "status": status,
            "started_at": started_at,
            "duration_s": round(time.perf_counter() - timer, 3),
            **metrics,
        }
        audit.steps.append(record)
        if status == "SUCCESS" and metrics.get("rows_written", 0) == 0:
            log_event(logger, "zero_rows_written", logging.WARNING, layer=layer, dataset=dataset)
        log_event(logger, "step_finished", **record)