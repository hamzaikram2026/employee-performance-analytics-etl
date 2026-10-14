"""Integration tests against the real local PostgreSQL and LocalStack S3.

Skipped automatically when the environment variables or services are unavailable.
Run only these with:  python -m pytest -m integration -v
"""
import uuid

import pandas as pd
import pytest

from lib.bronze import partition_of
from lib.config import Config
from lib.storage import DataLake, PostgresSource

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def live_cfg():
    try:
        return Config.from_env()
    except EnvironmentError as exc:
        pytest.skip(f"environment not configured: {exc}")


@pytest.fixture(scope="module")
def live_lake(live_cfg):
    lake = DataLake(live_cfg)
    try:
        lake.list_keys("landing/")
    except Exception as exc:
        pytest.skip(f"S3 not reachable: {exc}")
    return lake


def test_postgres_reads_source_tables(live_cfg):
    pg = PostgresSource(live_cfg)
    try:
        teams = pg.read_table("teams")
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    assert len(teams) == 100
    assert list(teams.columns) == ["team_id", "name", "department"]
    assert len(pg.read_table("employees")) == 2000


def test_s3_write_overwrite_read_and_cleanup(live_lake):
    prefix = f"_integration_test/{uuid.uuid4().hex[:8]}"
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    try:
        assert live_lake.overwrite_dataset(df, prefix) == 3
        assert live_lake.overwrite_dataset(df.head(2), prefix) == 2   # replaces, never appends
        assert live_lake.read_parquet_prefix(prefix + "/")["id"].tolist() == [1, 2]
    finally:
        live_lake.delete_prefix(prefix)
    assert live_lake.list_keys(prefix + "/") == []


def test_landing_zone_has_every_source_file(live_lake):
    keys = live_lake.list_keys("landing/learning_activity/")
    assert len([k for k in keys if k.endswith(".jsonl")]) == 783


def test_lake_reconciles_source_silver_quarantine_gold(live_lake):
    reviews = live_lake.read_parquet_prefix("silver/performance_reviews/")
    if reviews.empty:
        pytest.skip("pipeline has not been run yet")
    partitions = sorted({partition_of(k) for k in live_lake.list_keys("quarantine/performance_reviews/")})
    quarantined = live_lake.read_parquet_prefix(f"quarantine/performance_reviews/{partitions[-1]}/")
    assert len(reviews) + len(quarantined) == 4000            # every source review accounted for
    assert len(live_lake.read_parquet("gold/gold_employee_profile/data.parquet")) == 2000