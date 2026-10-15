"""Cloud-mode behaviour: bucket resolution, Aurora source loading, and handler safety."""
import json

import pytest

import function as service
from lib.config import SOURCE_TABLES, Config
from lib.source_loader import SOURCE_PREFIX, load_sources


@pytest.fixture
def env(monkeypatch):
    for name, value in {"POSTGRES_NAME": "db", "POSTGRES_USER": "user", "POSTGRES_PASS": "pw",
                        "RUN_DATE": "2026-01-05"}.items():
        monkeypatch.setenv(name, value)
    for name in ("DATA_LAKE_BUCKET", "APP_ID", "APP_NAME"):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


# ------------------------------------------------------------ bucket name
def test_explicit_bucket_wins(env):
    env.setenv("IS_LOCAL", "false")
    env.setenv("DATA_LAKE_BUCKET", "my-lake")
    assert Config.from_env().bucket == "my-lake"


def test_cloud_bucket_is_derived_from_workshop_variables(env):
    env.setenv("IS_LOCAL", "false")
    env.setenv("APP_ID", "74cbf3a2")
    env.setenv("APP_NAME", "coding-workshop-74cbf3a2")
    assert Config.from_env().bucket == "coding-workshop-datalake-74cbf3a2"


def test_localstack_uses_local_bucket(env):
    env.setenv("IS_LOCAL", "true")
    assert Config.from_env().bucket == "acme-perf-datalake-local"


def test_cloud_without_bucket_or_app_variables_fails_fast(env):
    env.setenv("IS_LOCAL", "false")
    with pytest.raises(EnvironmentError, match="DATA_LAKE_BUCKET"):
        Config.from_env()


# ---------------------------------------------------------- source loader
class FakeCursor:
    def __init__(self, log):
        self.log, self.rows = log, 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.log.append(sql)

    def copy_expert(self, sql, fh):
        self.log.append(sql)
        self.rows = len(fh.read().decode().strip().splitlines()) - 1  # minus header

    def fetchone(self):
        return (self.rows,)


class FakeConnection:
    def __init__(self):
        self.log, self.committed, self.closed = [], False, False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *exc):
        self.committed = exc_type is None
        return False

    def cursor(self):
        return FakeCursor(self.log)

    def close(self):
        self.closed = True


class FakePostgres:
    def __init__(self):
        self.conn = FakeConnection()

    def _connect(self):
        return self.conn


def seed_sources(lake):
    lake.write_bytes(f"{SOURCE_PREFIX}/schema.sql", b"CREATE TABLE teams (team_id INTEGER);")
    for i, table in enumerate(SOURCE_TABLES):
        rows = "\n".join(str(n) for n in range(i + 1))
        lake.write_bytes(f"{SOURCE_PREFIX}/{table}.csv", f"id\n{rows}".encode())


def test_load_sources_applies_schema_and_copies_every_table(cfg, lake):
    seed_sources(lake)
    pg = FakePostgres()
    result = load_sources(cfg, lake, pg)
    assert result["status"] == "SUCCESS"
    assert result["tables"] == {table: i + 1 for i, table in enumerate(SOURCE_TABLES)}
    assert pg.conn.log[0].startswith("DROP TABLE IF EXISTS")
    assert pg.conn.log[1].startswith("CREATE TABLE")
    assert pg.conn.committed and pg.conn.closed


def test_load_sources_missing_file_fails_before_touching_database(cfg, lake):
    pg = FakePostgres()
    with pytest.raises(KeyError):
        load_sources(cfg, lake, pg)
    assert pg.conn.log == []


# ---------------------------------------------------------------- handler
def test_load_sources_is_refused_over_public_url():
    event = {"requestContext": {"http": {"method": "GET"}},
             "queryStringParameters": {"action": "load_sources"}}
    resp = service.handler(event, None)
    assert resp["statusCode"] == 403


def test_unknown_action_is_rejected():
    resp = service.handler({"action": "drop_everything"}, None)
    assert resp["statusCode"] == 400


def test_handler_returns_json_on_unexpected_error(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(service, "run_pipeline", boom)
    resp = service.handler({"stage": "gold"}, None)
    assert resp["statusCode"] == 500
    assert "boom" in json.loads(resp["body"])["error"]