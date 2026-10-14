"""Unit tests for schema contracts and data-quality rules (lib/validation.py)."""
import pandas as pd
import pytest

from lib.validation import (
    REJECT_REASON,
    ColumnSpec,
    SchemaContractError,
    TableContract,
    cast_series,
    quarantine_where,
    validate,
)

CONTRACT = TableContract(
    name="demo",
    columns=(
        ColumnSpec("id", "int"),
        ColumnSpec("rating", "int", min_value=1, max_value=5),
        ColumnSpec("status", "str", allowed=frozenset({"on-time", "delayed"})),
        ColumnSpec("day", "date"),
        ColumnSpec("note", "str", nullable=True),
    ),
    primary_key=("id",),
)


def make(**overrides):
    """One valid raw row (all strings, as Bronze stores them), with optional overrides."""
    row = {"id": "1", "rating": "3", "status": "on-time", "day": "2025-01-01", "note": None}
    row.update(overrides)
    return row


def run(rows):
    return validate(pd.DataFrame(rows), CONTRACT)


class TestCastSeries:
    def test_int_accepts_whole_numbers_and_trims_spaces(self):
        assert cast_series(pd.Series(["1", " 2 ", "3.0"]), "int").tolist() == [1, 2, 3]

    def test_int_rejects_fractions_text_and_blanks(self):
        assert cast_series(pd.Series(["3.5", "abc", ""]), "int").isna().all()

    def test_date_requires_iso_format(self):
        out = cast_series(pd.Series(["2025-01-31", "31/01/2025", "yesterday"]), "date")
        assert out.notna().tolist() == [True, False, False]

    def test_datetime_parses_iso_and_rejects_free_text(self):
        out = cast_series(pd.Series(["2025-06-02T17:16:00", "N/A"]), "datetime")
        assert out.iloc[0] == pd.Timestamp("2025-06-02 17:16:00")
        assert pd.isna(out.iloc[1])

    def test_unknown_dtype_raises(self):
        with pytest.raises(ValueError):
            cast_series(pd.Series(["x"]), "uuid")


class TestValidate:
    def test_valid_row_passes_and_is_typed(self):
        valid, rejected = run([make()])
        assert len(valid) == 1 and rejected.empty
        assert valid["id"].dtype == "Int64"
        assert pd.api.types.is_datetime64_any_dtype(valid["day"])

    def test_required_value_missing_is_rejected(self):
        _, rejected = run([make(status=None)])
        assert "status: required value missing" in rejected[REJECT_REASON].iloc[0]

    def test_nullable_column_allows_missing(self):
        valid, _ = run([make(note=None)])
        assert len(valid) == 1

    def test_out_of_range_is_rejected(self):
        _, rejected = run([make(rating="9")])
        assert "rating: above 5" in rejected[REJECT_REASON].iloc[0]

    def test_value_not_in_allowed_set_is_rejected(self):
        _, rejected = run([make(status="lost")])
        assert "status: value not allowed" in rejected[REJECT_REASON].iloc[0]

    def test_invalid_type_is_rejected(self):
        _, rejected = run([make(day="bad-date")])
        assert "day: not a valid date" in rejected[REJECT_REASON].iloc[0]

    def test_all_problems_in_a_row_are_reported(self):
        _, rejected = run([make(id="x", day="bad-date")])
        reason = rejected[REJECT_REASON].iloc[0]
        assert "id: not a valid int" in reason and "day: not a valid date" in reason

    def test_duplicate_primary_key_keeps_first_occurrence(self):
        valid, rejected = run([make(id="1", rating="2"), make(id="1", rating="4")])
        assert valid["rating"].tolist() == [2]
        assert rejected[REJECT_REASON].tolist() == ["duplicate primary key"]

    def test_missing_required_column_raises(self):
        df = pd.DataFrame([make()]).drop(columns="status")
        with pytest.raises(SchemaContractError):
            validate(df, CONTRACT)

    def test_schema_drift_alias_is_mapped(self):
        contract = TableContract(
            "events",
            (ColumnSpec("id", "int"), ColumnSpec("text", "str")),
            primary_key=("id",),
            aliases={"notes": "text"},
        )
        df = pd.DataFrame({"id": ["1", "2"], "text": ["a", None], "notes": [None, "b"]})
        valid, rejected = validate(df, contract)
        assert valid["text"].tolist() == ["a", "b"] and rejected.empty

    def test_lineage_columns_pass_through(self):
        df = pd.DataFrame([make()])
        df["_source_file"] = "events-000.jsonl"
        valid, _ = validate(df, CONTRACT)
        assert valid["_source_file"].iloc[0] == "events-000.jsonl"

    def test_rows_are_conserved(self):
        rows = [make(id=str(i)) for i in range(5)] + [make(id="x"), make(id="1")]
        valid, rejected = run(rows)
        assert len(valid) + len(rejected) == len(rows)


class TestQuarantineWhere:
    def test_splits_rows_and_attaches_reason(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        good, bad = quarantine_where(df, df["x"] > 1, "too big")
        assert good["x"].tolist() == [1]
        assert bad[REJECT_REASON].unique().tolist() == ["too big"]

    def test_missing_mask_values_count_as_pass(self):
        df = pd.DataFrame({"x": [1, 2]})
        good, bad = quarantine_where(df, pd.Series([pd.NA, True], dtype="boolean"), "r")
        assert len(good) == 1 and len(bad) == 1