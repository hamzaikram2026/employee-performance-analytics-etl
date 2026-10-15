"""Operator-only read-only SQL: statement checks, read-only session, row cap, access control."""
import json

import psycopg2
import pytest

import function as service
from lib import admin_sql
from lib.admin_sql import MAX_ROWS, check_query, run_readonly_sql


class Column:
    def __init__(self, name):
        self.name = name


class FakeCursor:
    def __init__(self, conn):
        self.conn, self.description = conn, None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.conn.executed.append(sql)
        if sql.lower().startswith("select"):
            self.description = [Column("id"), Column("name")]

    def fetchmany(self, n):
        return [(i, None if i == 0 else f"n{i}") for i in range(min(n, self.conn.total))]


class FakeConnection:
    def __init__(self, total=3):
        self.total, self.executed = total, []
        self.readonly = self.rolled_back = self.closed = False

    def set_session(self, readonly):
        self.readonly = readonly

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class FakePostgres:
    def __init__(self, total=3):
        self.conn = FakeConnection(total)

    def _connect(self):
        return self.conn


@pytest.mark.parametrize("query", ["SELECT 1", "  select * from teams;  ", "WITH x AS (SELECT 1) SELECT * FROM x",
                                   "SHOW server_version", "EXPLAIN SELECT 1"])
def test_read_queries_are_accepted(query):
    assert check_query(query)


@pytest.mark.parametrize("query", ["", None, "DELETE FROM teams", "DROP TABLE teams", "UPDATE teams SET name='x'",
                                   "SELECT 1; DELETE FROM teams", "COMMIT; INSERT INTO teams VALUES (1)"])
def test_writes_and_multiple_statements_are_rejected(query):
    with pytest.raises(ValueError):
        check_query(query)


def test_query_runs_in_read_only_session_and_is_rolled_back(cfg):
    pg = FakePostgres()
    result = run_readonly_sql(cfg, "SELECT id, name FROM employees", pg)
    assert pg.conn.readonly and pg.conn.rolled_back and pg.conn.closed
    assert pg.conn.executed[0] == "SET statement_timeout = 20000"
    assert result["columns"] == ["id", "name"] and result["rows"][0] == ["0", None]
    assert result["row_count"] == 3 and not result["truncated"]


def test_row_cap(cfg):
    result = run_readonly_sql(cfg, "SELECT * FROM performance_reviews", FakePostgres(total=MAX_ROWS + 50))
    assert result["row_count"] == MAX_ROWS and result["truncated"]


def test_sql_refused_over_public_url():
    event = {"requestContext": {"http": {}}, "queryStringParameters": {"action": "sql", "query": "SELECT 1"}}
    assert service.handler(event, None)["statusCode"] == 403


def test_sql_direct_invoke_rejects_write(monkeypatch, cfg):
    monkeypatch.setattr(service.Config, "from_env", lambda: cfg)
    resp = service.handler({"action": "sql", "query": "DELETE FROM employees"}, None)
    assert resp["statusCode"] == 400 and "must start with" in json.loads(resp["body"])["error"]