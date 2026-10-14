"""Bronze layer: land raw source data in the lake, unchanged, with lineage metadata."""
from __future__ import annotations

import logging

import pandas as pd

from .config import EVENT_DATASET, SOURCE_TABLES, Config
from .logging_utils import RunAudit, get_logger, log_event, track_step
from .storage import DataLake, PostgresSource

logger = get_logger(__name__)


def add_lineage(df: pd.DataFrame, cfg: Config, source_system: str, source_object: str) -> pd.DataFrame:
    """Attach lineage columns that travel with every row through the lake."""
    df = df.copy()
    df["_source_system"] = source_system
    df["_source_object"] = source_object
    df["_ingested_at"] = cfg.run_ts
    df["_run_id"] = cfg.run_id
    return df


def partition_of(key: str) -> str:
    """Extract 'year=YYYY/month=MM/day=DD' from an object key."""
    return "/".join(p for p in key.split("/") if p.startswith(("year=", "month=", "day=")))


def ingest_table(cfg: Config, pg: PostgresSource, lake: DataLake, audit: RunAudit, table: str) -> None:
    """Snapshot one PostgreSQL table into its Bronze partition for this run date."""
    with track_step(logger, audit, "bronze", table) as m:
        df = pg.read_table(table)
        m["rows_read"] = len(df)
        df = add_lineage(df, cfg, "postgres", f"public.{table}")
        prefix = f"{cfg.bronze_prefix}/{table}/{cfg.run_partition}"
        m["rows_written"] = lake.overwrite_dataset(df, prefix)
        m["target"] = prefix


def ingest_events(cfg: Config, lake: DataLake, audit: RunAudit, full_refresh: bool = False) -> None:
    """Copy learning-activity JSONL lines into Bronze, one partition per source day.

    Lines are stored as raw text (even malformed ones); parsing happens in Silver.
    Partitions already present in Bronze are skipped unless full_refresh is True.
    """
    with track_step(logger, audit, "bronze", EVENT_DATASET) as m:
        landing = f"{cfg.landing_prefix}/{EVENT_DATASET}/"
        bronze_root = f"{cfg.bronze_prefix}/{EVENT_DATASET}"

        by_partition: dict[str, list[str]] = {}
        for key in lake.list_keys(landing):
            if key.endswith(".jsonl"):
                by_partition.setdefault(partition_of(key), []).append(key)

        existing = {partition_of(k) for k in lake.list_keys(bronze_root + "/") if k.endswith(".parquet")}
        manifest: list[dict] = []
        skipped = 0

        for partition, keys in sorted(by_partition.items()):
            if partition in existing and not full_refresh:
                skipped += 1
                continue
            frames = []
            for key in keys:
                raw = lake.read_bytes(key)
                lines = raw.decode("utf-8", errors="replace").splitlines()
                rows = [(text, n) for n, text in enumerate(lines, start=1) if text.strip()]
                frames.append(pd.DataFrame({
                    "raw_record": pd.array([r[0] for r in rows], dtype="string"),
                    "_source_file": key,
                    "_line_number": [r[1] for r in rows],
                }))
                manifest.append({"source_key": key, "bytes": len(raw), "lines": len(rows)})
            df = add_lineage(pd.concat(frames, ignore_index=True), cfg, "s3", landing)
            m["rows_read"] += len(df)
            m["rows_written"] += lake.overwrite_dataset(df, f"{bronze_root}/{partition}")

        m["partitions_total"] = len(by_partition)
        m["partitions_skipped"] = skipped
        if skipped == len(by_partition):
            log_event(logger, "no_new_partitions", dataset=EVENT_DATASET, partitions=skipped)
        lake.write_json(
            {"run_id": cfg.run_id, "dataset": EVENT_DATASET, "files": manifest},
            f"{cfg.audit_prefix}/manifests/bronze_{EVENT_DATASET}_{cfg.run_id}.json",
        )


def run(cfg: Config, pg: PostgresSource, lake: DataLake, audit: RunAudit, full_refresh: bool = False) -> None:
    """Ingest every source. A failure in one dataset is logged and isolated."""
    for table in SOURCE_TABLES:
        try:
            ingest_table(cfg, pg, lake, audit, table)
        except Exception:  # already logged with details by track_step
            log_event(logger, "dataset_skipped", logging.WARNING, layer="bronze", dataset=table)
    try:
        ingest_events(cfg, lake, audit, full_refresh)
    except Exception:
        log_event(logger, "dataset_skipped", logging.WARNING, layer="bronze", dataset=EVENT_DATASET)