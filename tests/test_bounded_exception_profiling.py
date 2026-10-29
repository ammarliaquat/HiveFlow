"""Focused tests for generic bounded duplicate discovery evidence."""

from types import SimpleNamespace

from src.discovery.type_mapper import (
    _build_bounded_exception_complement_query,
    build_bounded_duplicate_query,
    detect_business_key_profiled,
)


class _Row:
    def __init__(self, values):
        self.values = values

    def asDict(self):
        return self.values

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values.values())[key]
        return self.values[key]


class _Frame:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return [_Row(row) for row in self.rows]


class _Reader:
    def __init__(self, frames):
        self.frames = iter(frames)
        self.options = []

    def option(self, name, value):
        self.options.append((name, value))
        return self

    def load(self):
        return _Frame(next(self.frames))


class _Spark:
    def __init__(self, frames):
        self.reader = _Reader(frames)
        self.read = self

    def format(self, _name):
        return self.reader


def test_bounded_duplicate_query_is_dialect_correct():
    postgres = build_bounded_duplicate_query("public.events", "event_id", "postgres", 6)
    assert "LIMIT 6" in build_bounded_duplicate_query(
        "public.events", "event_id", "postgres", limit=6
    )
    sqlserver = build_bounded_duplicate_query("dbo.events", "event_id", "mssql", 6)
    oracle = build_bounded_duplicate_query("APP.EVENTS", "event_id", "oracle", 6)

    assert "LIMIT 6" in postgres
    assert "TOP (6)" in sqlserver
    assert "LIMIT" not in sqlserver
    assert "ROWNUM <= 6" in oracle

    complement = _build_bounded_exception_complement_query(
        "public.events", "event_id", "postgres", [0, 7]
    )
    assert "NOT IN (0, 7)" in complement
    assert "COUNT(DISTINCT \"event_id\")" in complement


def test_profiled_key_returns_bounded_exception_evidence_without_activation():
    # The discovery result is evidence.  Promotion remains the authority that
    # decides whether BOUNDED_EXCEPTION_KEY becomes executable.
    spark = _Spark(
        [
            [{"total_rows": 3_500_000, "non_null_rows": 3_500_000, "distinct_rows": 3_499_982}],
            [{"duplicate_value": 0, "occurrences": 19}],
            [{"non_exception_rows": 3_499_981, "non_exception_distinct": 3_499_981}],
        ]
    )
    result = detect_business_key_profiled(
        ["reg_id"],
        spark_session=spark,
        conn_config=SimpleNamespace(db_type="oracle", url=""),
        table_fqn="OPFSERVICES.TBLOP_DEC24",
        conditional_key_policy={
            "max_exception_values": 5,
            "max_exception_rows": 100,
            "max_exception_fraction": 0.005,
        },
    )

    assert result["identity_mode"] == "BOUNDED_EXCEPTION_KEY"
    assert result["key_source"] == "PROFILED_CONDITIONAL"
    assert result["exception_values"] == [0]
    assert result["exception_row_count"] == 19
    assert result["key_evidence_type"] == "PROFILED_CONDITIONAL_KEY"
    assert result["key_evidence"]["thresholds"]["max_exception_values"] == 5


def test_null_key_is_measured_independently_and_never_qualifies():
    spark = _Spark(
        [[{"total_rows": 100, "non_null_rows": 99, "distinct_rows": 99}]]
    )
    result = detect_business_key_profiled(
        ["event_id"],
        spark_session=spark,
        conn_config=SimpleNamespace(db_type="postgres", url=""),
        table_fqn="public.events",
        conditional_key_policy={"max_exception_values": 5},
    )

    assert result["identity_mode"] == "KEYLESS"
    assert result["key_source"] == "NONE"
    assert result["key_candidate_nullable"] is True


def test_conditional_exception_values_do_not_coerce_text_or_float_keys():
    # Typed conditional identity must not collapse textual ``"001"`` and
    # numeric ``1`` (or a floating-point representation) into one value.
    spark = _Spark(
        [
            [{"total_rows": 100, "non_null_rows": 100, "distinct_rows": 99}],
            [{"duplicate_value": "001", "occurrences": 2}],
        ]
    )
    result = detect_business_key_profiled(
        ["event_id"],
        spark_session=spark,
        conn_config=SimpleNamespace(db_type="postgres", url=""),
        table_fqn="public.events",
    )
    assert result["identity_mode"] == "KEYLESS"


def test_non_exception_duplicate_in_complement_disqualifies_bounded_identity():
    spark = _Spark(
        [
            [{"total_rows": 100, "non_null_rows": 100, "distinct_rows": 98}],
            [{"duplicate_value": 0, "occurrences": 2}],
            # The bounded exception list did not account for another duplicate
            # group; complement proof must reject the candidate.
            [{"non_exception_rows": 98, "non_exception_distinct": 97}],
        ]
    )
    result = detect_business_key_profiled(
        ["event_id"],
        spark_session=spark,
        conn_config=SimpleNamespace(db_type="postgres", url=""),
        table_fqn="public.events",
    )
    assert result["identity_mode"] == "KEYLESS"
