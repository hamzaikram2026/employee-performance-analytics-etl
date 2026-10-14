"""I/O adapters for the PostgreSQL source and the S3 data lake, with retries."""
from __future__ import annotations

import functools
import io
import json
import logging
import time
from typing import Any, Callable

import boto3
import pandas as pd
import psycopg2
from botocore.config import Config as BotoConfig
from botocore.exceptions import ConnectionClosedError, EndpointConnectionError, ReadTimeoutError
from psycopg2 import sql

from .config import Config
from .logging_utils import get_logger, log_event

logger = get_logger(__name__)

TRANSIENT_S3_ERRORS = (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError)


def retry(max_attempts: int = 4, base_delay: float = 0.5, exceptions: tuple = (Exception,)) -> Callable:
    """Retry a call on transient errors with exponential backoff (0.5s, 1s, 2s, ...)."""

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        log_event(logger, "retry_exhausted", logging.ERROR,
                                  func=fn.__name__, attempts=attempt, error=str(exc))
                        raise
                    delay = base_delay * 2 ** (attempt - 1)
                    log_event(logger, "retrying", logging.WARNING,
                              func=fn.__name__, attempt=attempt, delay_s=delay, error=str(exc))
                    time.sleep(delay)
            raise RuntimeError("unreachable")

        return wrapper

    return decorator


class PostgresSource:
    """Reads source tables from PostgreSQL (local) or Aurora (cloud)."""

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    @retry(exceptions=(psycopg2.OperationalError,))
    def _connect(self):
        return psycopg2.connect(**self._cfg.pg_connect_kwargs)

    def read_table(self, table: str) -> pd.DataFrame:
        """Read a full table ordered by its first column.

        Values are returned as strings: Bronze stores data raw (schema-on-read)
        and all type casting happens, and is validated, in Silver.
        """
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT * FROM {} ORDER BY 1").format(sql.Identifier(table)))
                columns = [c.name for c in cur.description]
                rows = cur.fetchall()
        finally:
            conn.close()
        df = pd.DataFrame(rows, columns=columns, dtype=object)
        return df.map(lambda v: None if v is None else str(v)).astype("string")


class DataLake:
    """Thin wrapper around one S3 bucket that holds every medallion zone."""

    def __init__(self, cfg: Config, client: Any = None) -> None:
        self.bucket = cfg.bucket
        boto_cfg = BotoConfig(
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "path"} if cfg.is_local else {},
        )
        self._s3 = client or boto3.client(
            "s3", endpoint_url=cfg.s3_endpoint, region_name=cfg.aws_region, config=boto_cfg
        )

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self._s3.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    @retry(exceptions=TRANSIENT_S3_ERRORS)
    def read_bytes(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    @retry(exceptions=TRANSIENT_S3_ERRORS)
    def write_bytes(self, key: str, data: bytes) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def read_text(self, key: str) -> str:
        return self.read_bytes(key).decode("utf-8")

    def write_json(self, obj: Any, key: str) -> None:
        self.write_bytes(key, json.dumps(obj, indent=2, default=str).encode("utf-8"))

    def write_parquet(self, df: pd.DataFrame, key: str) -> int:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="snappy")
        self.write_bytes(key, buf.getvalue())
        return len(df)

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self.read_bytes(key)))

    def read_parquet_prefix(self, prefix: str) -> pd.DataFrame:
        """Read and concatenate every Parquet file under a prefix."""
        keys = [k for k in self.list_keys(prefix) if k.endswith(".parquet")]
        if not keys:
            return pd.DataFrame()
        return pd.concat([self.read_parquet(k) for k in keys], ignore_index=True)

    def delete_prefix(self, prefix: str) -> int:
        """Delete every object under a folder-style prefix (trailing slash enforced)."""
        keys = self.list_keys(prefix.rstrip("/") + "/")
        for i in range(0, len(keys), 1000):
            self._s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]},
            )
        return len(keys)

    def overwrite_dataset(self, df: pd.DataFrame, prefix: str, filename: str = "data.parquet") -> int:
        """Replace a dataset folder so reruns never duplicate data."""
        self.delete_prefix(prefix)
        return self.write_parquet(df, f"{prefix.rstrip('/')}/{filename}")