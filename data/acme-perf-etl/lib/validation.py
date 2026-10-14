"""Schema contracts and data-quality rules.

Every failed rule is recorded per row with a readable reason, so invalid
records are quarantined instead of failing the whole pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

REJECT_REASON = "_reject_reason"


class SchemaContractError(Exception):
    """A dataset is structurally unusable, e.g. a required column is missing."""


@dataclass(frozen=True)
class ColumnSpec:
    """Contract for one column."""

    name: str
    dtype: str  # one of: int, float, str, date, datetime
    nullable: bool = False
    allowed: frozenset[str] | None = None
    min_value: float | None = None
    max_value: float | None = None


@dataclass(frozen=True)
class TableContract:
    """Contract for one dataset: columns, primary key, and known schema drift."""

    name: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    aliases: dict[str, str] = field(default_factory=dict)  # drifted name -> canonical name

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def cast_series(series: pd.Series, dtype: str) -> pd.Series:
    """Cast raw values to the target type; values that cannot be cast become NA."""
    s = series.astype("string").str.strip()
    s = s.mask(s == "")
    if dtype == "int":
        num = pd.to_numeric(s, errors="coerce").astype("Float64")
        num = num.where(num.isna() | (num == num.round()))  # 3.5 is not a valid int
        return num.astype("Int64")
    if dtype == "float":
        return pd.to_numeric(s, errors="coerce").astype("Float64")
    if dtype == "date":
        return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    if dtype == "datetime":
        parsed = pd.to_datetime(s, format="ISO8601", errors="coerce", utc=True)
        return parsed.dt.tz_localize(None)
    if dtype == "str":
        return s
    raise ValueError(f"Unsupported dtype '{dtype}'")


def validate(df: pd.DataFrame, contract: TableContract) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply a contract. Returns (valid typed rows, rejected raw rows with reasons).

    Columns starting with '_' (lineage metadata) are passed through untouched.
    """
    df = df.copy()

    # 1. Resolve known schema drift (e.g. 'notes' used instead of 'text')
    for drifted, canonical in contract.aliases.items():
        if drifted in df.columns:
            df[canonical] = df[canonical].fillna(df[drifted]) if canonical in df.columns else df[drifted]

    # 2. Structural check: every contract column must exist
    missing = [c for c in contract.column_names if c not in df.columns]
    if missing:
        raise SchemaContractError(f"{contract.name}: missing required columns {missing}")

    reasons = pd.Series("", index=df.index, dtype="string")

    def flag(mask: pd.Series, message: str) -> None:
        nonlocal reasons
        mask = mask.fillna(False).astype(bool)
        reasons = reasons.mask(mask, reasons + message + "; ")

    # 3. Column-level rules: type, nullability, allowed values, ranges
    typed = pd.DataFrame(index=df.index)
    for col in contract.columns:
        raw = df[col.name].astype("string")
        present = raw.notna() & (raw.str.strip() != "")
        cast = cast_series(raw, col.dtype)
        flag(present & cast.isna(), f"{col.name}: not a valid {col.dtype}")
        if not col.nullable:
            flag(~present, f"{col.name}: required value missing")
        if col.allowed is not None:
            flag(cast.notna() & ~cast.isin(col.allowed), f"{col.name}: value not allowed")
        if col.min_value is not None:
            flag(cast.notna() & (cast < col.min_value), f"{col.name}: below {col.min_value}")
        if col.max_value is not None:
            flag(cast.notna() & (cast > col.max_value), f"{col.name}: above {col.max_value}")
        typed[col.name] = cast

    # 4. Primary-key uniqueness among otherwise-valid rows (keep first occurrence)
    still_valid = reasons == ""
    dup = typed.loc[still_valid].duplicated(subset=list(contract.primary_key), keep="first")
    flag(dup.reindex(df.index, fill_value=False), "duplicate primary key")

    # 5. Carry lineage columns through
    for col in [c for c in df.columns if c.startswith("_")]:
        typed[col] = df[col]

    is_valid = reasons == ""
    valid = typed.loc[is_valid].reset_index(drop=True)
    rejected = df.loc[~is_valid].copy()
    rejected[REJECT_REASON] = reasons.loc[~is_valid].str.rstrip("; ")
    return valid, rejected.reset_index(drop=True)


def quarantine_where(df: pd.DataFrame, mask: pd.Series, reason: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply a business rule after contract validation (e.g. review dated before hire).

    Returns (rows that pass, rows that fail with the reason attached).
    """
    mask = mask.fillna(False).astype(bool)
    bad = df.loc[mask].copy()
    bad[REJECT_REASON] = reason
    return df.loc[~mask].reset_index(drop=True), bad.reset_index(drop=True)