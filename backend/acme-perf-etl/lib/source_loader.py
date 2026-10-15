"""Source-system setup: load the HR tables into PostgreSQL/Aurora from files in S3.

Aurora in AWS only accepts connections from inside the workshop VPC, so the WorkSpace
cannot run data/source/load.sh against it. The Lambda runs inside the VPC and performs
the same steps: apply schema.sql, then COPY each CSV. This is a one-off setup of the
source system, not part of the ETL, and only runs on an explicit (IAM-signed) request.
"""
from __future__ import annotations

import io
import time

from .config import SOURCE_TABLES, Config
from .logging_utils import get_logger, log_event
from .storage import DataLake, PostgresSource

logger = get_logger(__name__)

SOURCE_PREFIX = "landing/hr_source"  # schema.sql + <table>.csv uploaded here


def load_sources(cfg: Config, lake: DataLake, pg: PostgresSource | None = None) -> dict:
    """(Re)create the source tables and load every CSV in a single transaction."""
    pg = pg or PostgresSource(cfg)
    # Read every file first, so a missing file fails before the database is touched
    schema_sql = lake.read_text(f"{SOURCE_PREFIX}/schema.sql")
    csvs = {table: lake.read_bytes(f"{SOURCE_PREFIX}/{table}.csv") for table in SOURCE_TABLES}

    started = time.perf_counter()
    counts: dict[str, int] = {}
    conn = pg._connect()  # reuses the retry/backoff connection (Aurora may be resuming)
    try:
        with conn:  # commit if everything succeeds, roll back otherwise
            with conn.cursor() as cur:
                # Table names come from the fixed SOURCE_TABLES constant, never from user input
                cur.execute("DROP TABLE IF EXISTS " + ", ".join(reversed(SOURCE_TABLES)) + " CASCADE")
                cur.execute(schema_sql)
                for table in SOURCE_TABLES:
                    cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)",
                                    io.BytesIO(csvs[table]))
                    cur.execute(f"SELECT count(*) FROM {table}")
                    counts[table] = cur.fetchone()[0]
    finally:
        conn.close()

    result = {"status": "SUCCESS", "action": "load_sources", "tables": counts,
              "duration_s": round(time.perf_counter() - started, 2)}
    log_event(logger, "sources_loaded", **result)
    return result