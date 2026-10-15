"""Error handling and resilience: retry/backoff, config validation, step tracking, request parsing."""
import json
import logging

import psycopg2
import pytest
from botocore.exceptions import EndpointConnectionError

import function as service
from lib import storage
from lib.config import Config
from lib.logging_utils import RunAudit, get_logger, track_step
from lib.storage import retry


@pytest.fixture
def delays(monkeypatch):
    """Record backoff delays instead of actually sleeping."""
    recorded = []
    monkeypatch.setattr(storage.time, "sleep", lambda seconds: recorded.append(seconds))
    return recorded


# ------------------------------------------------------------ retry/backoff
def test_retry_recovers_from_transient_failures(delays):
    calls = {"n": 0}

    @retry(max_attempts=4, base_delay=0.5, exceptions=(ConnectionError,))
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("temporary")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3
    assert delays == [0.5, 1.0]  # exponential backoff


def test_retry_gives_up_after_max_attempts(delays):
    @retry(max_attempts=3, base_delay=0.5, exceptions=(ConnectionError,))
    def always_down():
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        always_down()
    assert delays == [0.5, 1.0]


def test_retry_does_not_retry_programming_errors(delays):
    calls = {"n": 0}

    @retry(exceptions=(ConnectionError,))
    def buggy():
        calls["n"] += 1
        raise ValueError("bug")

    with pytest.raises(ValueError):
        buggy()
    assert calls["n"] == 1 and delays == []


def test_postgres_connection_timeout_is_retried(cfg, monkeypatch, delays):
    attempts = {"n": 0}

    def fake_connect(**kwargs):
        attempts["n"] += 1
        raise psycopg2.OperationalError("connection timed out")

    monkeypatch.setattr(storage.psycopg2, "connect", fake_connect)
    with pytest.raises(psycopg2.OperationalError):
        storage.PostgresSource(cfg).read_table("teams")
    assert attempts["n"] == 4 and delays == [0.5, 1.0, 2.0]


def test_s3_transient_error_is_retried(cfg, delays):
    class FlakyS3:
        def __init__(self):
            self.calls = 0

        def put_object(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise EndpointConnectionError(endpoint_url="http://s3")

    client = FlakyS3()
    storage.DataLake(cfg, client=client).write_bytes("key", b"data")
    assert client.calls == 2 and delays == [0.5]


# ------------------------------------------------------------------ config
@pytest.fixture
def base_env(monkeypatch):
    for name, value in {"POSTGRES_NAME": "db", "POSTGRES_USER": "user", "POSTGRES_PASS": "x-secret",
                        "DATA_LAKE_BUCKET": "bucket", "RUN_DATE": "2026-01-05",
                        "AWS_ENDPOINT_URL": "http://localstack:4566"}.items():
        monkeypatch.setenv(name, value)
    return monkeypatch



def test_config_switches_between_local_and_cloud(base_env):
    base_env.setenv("IS_LOCAL", "true")
    local = Config.from_env()
    assert local.pg_connect_kwargs["sslmode"] == "prefer"
    assert local.s3_endpoint == "http://localstack:4566"
    assert local.run_partition == "year=2026/month=01/day=05"

    base_env.setenv("IS_LOCAL", "false")
    cloud = Config.from_env()
    assert cloud.pg_connect_kwargs["sslmode"] == "require"   # Aurora requires SSL
    assert cloud.s3_endpoint is None                         # real AWS endpoint
    assert "x-secret" not in json.dumps(cloud.describe())    # secrets never logged


# --------------------------------------------------------- step tracking
def test_failed_step_is_recorded_and_reraised():
    audit = RunAudit("r", "t")
    with pytest.raises(RuntimeError):
        with track_step(get_logger("test"), audit, "silver", "demo") as m:
            m["rows_read"] = 5
            raise RuntimeError("boom")
    assert audit.status == "FAILED"
    assert audit.steps[0]["error"] == "RuntimeError: boom"
    assert audit.steps[0]["rows_read"] == 5


def test_zero_rows_written_logs_warning(caplog):
    audit = RunAudit("r", "t")
    with caplog.at_level(logging.WARNING):
        with track_step(get_logger("test"), audit, "gold", "demo"):
            pass
    assert "zero_rows_written" in caplog.text


# --------------------------------------------------------- Lambda handler
def test_parse_request_reads_query_string_and_body():
    stages, full_refresh = service.parse_request({
        "queryStringParameters": {"stage": "silver"},
        "body": json.dumps({"full_refresh": True}),
    })
    assert stages == ["silver"] and full_refresh is True


def test_parse_request_defaults_to_all_stages():
    assert service.parse_request({}) == (["bronze", "silver", "gold"], False)


def test_handler_rejects_unknown_stage():
    response = service.handler({"stage": "platinum"}, None)
    assert response["statusCode"] == 400
    assert "Unknown stage" in json.loads(response["body"])["error"]