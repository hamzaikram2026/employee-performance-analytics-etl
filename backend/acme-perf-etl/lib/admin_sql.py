"""Read-only SQL against the source database, for operators only.

Aurora is private to the VPC, so operators cannot connect psql to it from outside.
The Lambda (inside the VPC) runs one read-only query on their behalf. Safeguards:
  * only reachable through an IAM-signed `aws lambda invoke` (refused over the public URL)
  * one statement only, starting with SELECT / WITH / SHOW / EXPLAIN
  * the database session is READ ONLY, so any write is rejected by PostgreSQL itself
  * 20 s statement timeout and at most MAX_ROWS rows returned
"""
from __future__ import annotations

from typing import Any

from .config import Config
from .storage import PostgresSource

MAX_ROWS = 200
ALLOWED_PREFIXES = ("select", "with", "show", "explain")


def check_query(query: str | None) -> str:
    """Return the cleaned query, or raise ValueError if it is not a single read-only statement."""
    text = (query or "").strip().rstrip(";").strip()
    if not text:
        raise ValueError("'query' is required")
    if ";" in text:
        raise ValueError("only one SQL statement is allowed")
    if not text.lower().startswith(ALLOWED_PREFIXES):
        raise ValueError(f"query must start with one of {list(ALLOWED_PREFIXES)}")
    return text


def run_readonly_sql(cfg: Config, query: str | None, pg: PostgresSource | None = None) -> dict[str, Any]:
    text = check_query(query)
    conn = (pg or PostgresSource(cfg))._connect()
    try:
        conn.set_session(readonly=True)               # every transaction starts READ ONLY
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 20000")
            cur.execute(text)
            columns = [c.name for c in cur.description] if cur.description else []
            rows = cur.fetchmany(MAX_ROWS + 1) if columns else []
        conn.rollback()                               # nothing is ever committed
    finally:
        conn.close()
    truncated = len(rows) > MAX_ROWS
    rows = rows[:MAX_ROWS]
    return {"columns": columns,
            "rows": [[None if v is None else str(v) for v in row] for row in rows],
            "row_count": len(rows), "truncated": truncated}