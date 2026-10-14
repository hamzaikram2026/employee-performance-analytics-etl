"""Pipeline configuration, read entirely from environment variables.

The same code runs against local PostgreSQL + LocalStack (IS_LOCAL=true)
or AWS Aurora + S3 (IS_LOCAL=false) without code changes.
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

SOURCE_TABLES: tuple[str, ...] = (
    "teams",
    "competencies",
    "employees",
    "projects",
    "performance_reviews",
    "competency_assessments",
    "talent_review",
)
EVENT_DATASET = "learning_activity"


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Required environment variable '{name}' is not set")
    return value


@dataclass(frozen=True)
class Config:
    """Immutable runtime configuration for one pipeline run."""

    is_local: bool
    pg_host: str
    pg_port: int
    pg_name: str
    pg_user: str
    pg_pass: str
    bucket: str
    aws_region: str
    s3_endpoint: str | None
    run_date: date
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    run_ts: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )

    # Medallion zone prefixes inside the data lake bucket
    landing_prefix: str = "landing"
    bronze_prefix: str = "bronze"
    silver_prefix: str = "silver"
    gold_prefix: str = "gold"
    quarantine_prefix: str = "quarantine"
    audit_prefix: str = "_audit"

    @classmethod
    def from_env(cls) -> "Config":
        """Build config from environment variables; fail fast if any are missing."""
        is_local = os.getenv("IS_LOCAL", "false").lower() == "true"
        run_date_env = os.getenv("RUN_DATE")  # optional, e.g. 2026-10-15 for backfills
        run_date = (
            date.fromisoformat(run_date_env)
            if run_date_env
            else datetime.now(timezone.utc).date()
        )
        return cls(
            is_local=is_local,
            pg_host=os.getenv("POSTGRES_HOST", "localhost"),
            pg_port=int(os.getenv("POSTGRES_PORT", "5432")),
            pg_name=_require("POSTGRES_NAME"),
            pg_user=_require("POSTGRES_USER"),
            pg_pass=_require("POSTGRES_PASS"),
            bucket=_require("DATA_LAKE_BUCKET"),
            aws_region=os.getenv("AWS_REGION", "ap-south-1"),
            s3_endpoint=os.getenv("AWS_ENDPOINT_URL") if is_local else None,
            run_date=run_date,
        )

    @property
    def pg_connect_kwargs(self) -> dict:
        """psycopg2 connection arguments; Aurora in the cloud requires SSL."""
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_name,
            "user": self.pg_user,
            "password": self.pg_pass,
            "sslmode": "prefer" if self.is_local else "require",
            "connect_timeout": 10,
        }

    @property
    def run_partition(self) -> str:
        """Hive-style partition path for this run, e.g. year=2026/month=10/day=15."""
        d = self.run_date
        return f"year={d:%Y}/month={d:%m}/day={d:%d}"

    def describe(self) -> dict:
        """Loggable summary of the config (never includes secrets)."""
        return {
            "mode": "local" if self.is_local else "cloud",
            "pg_host": self.pg_host,
            "pg_name": self.pg_name,
            "bucket": self.bucket,
            "s3_endpoint": self.s3_endpoint or "aws-default",
            "run_id": self.run_id,
            "run_date": self.run_date.isoformat(),
        }