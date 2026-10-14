"""Shared test setup: import path, a test Config, and an in-memory S3 for DataLake."""
import io
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.config import Config  # noqa: E402
from lib.storage import DataLake  # noqa: E402


class FakeS3:
    """Minimal in-memory stand-in for the boto3 S3 client methods DataLake uses."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def delete_objects(self, Bucket, Delete):
        for obj in Delete["Objects"]:
            self.objects.pop(obj["Key"], None)

    def get_paginator(self, name):
        objects = self.objects

        class Paginator:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}

        return Paginator()


@pytest.fixture
def cfg():
    return Config(
        is_local=True, pg_host="localhost", pg_port=5432, pg_name="test", pg_user="test",
        pg_pass="test", bucket="test-bucket", aws_region="ap-south-1", s3_endpoint=None,
        run_date=date(2026, 10, 14), run_id="testrun", run_ts="2026-10-14T12:00:00+00:00",
    )


@pytest.fixture
def lake(cfg):
    return DataLake(cfg, client=FakeS3())