# tests/test_rca_identifier_drift_fixes.py
import pytest
from src.utils.db_utils import _validate_identifier, quote_column_name, quote_table_name
from src.validation.schema_classifier import SchemaClassifier
from src.types.canonical import types_compatible


def test_validate_identifier_allows_digit_prefixes():
    """Verify numeric table names like date-prefixed tables are accepted safely."""
    assert _validate_identifier("28042026mapplicants") == "28042026mapplicants"
    assert _validate_identifier("28042026memberships") == "28042026memberships"
    assert _validate_identifier("28042026msubservices") == "28042026msubservices"
    assert _validate_identifier("table_123") == "table_123"
    assert _validate_identifier("_private") == "_private"


def test_validate_identifier_blocks_sql_injection():
    """Verify unsafe SQL injection characters are strictly rejected."""
    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        _validate_identifier("drop table users;--")

    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        _validate_identifier("table' OR '1'='1")

    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        _validate_identifier("table;select")

    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        _validate_identifier("table with spaces")


def test_schema_classifier_int_boolean_bidirectional_info():
    """Verify INT <-> BOOLEAN drift is classified as INFO (non-critical widening/drift)."""
    assert SchemaClassifier.classify_drift("TYPE_CHANGED", "INT", "BOOLEAN") == "INFO"
    assert SchemaClassifier.classify_drift("TYPE_CHANGED", "BOOLEAN", "INT") == "INFO"


def test_schema_classifier_float_decimal_bidirectional_info():
    """Verify FLOAT <-> DECIMAL drift is classified as INFO (safe numeric widening/representation)."""
    assert SchemaClassifier.classify_drift("TYPE_CHANGED", "FLOAT", "DECIMAL") == "INFO"
    assert SchemaClassifier.classify_drift("TYPE_CHANGED", "DECIMAL", "FLOAT") == "INFO"



def test_types_compatible_short_and_smallint():
    """Verify short and smallint compatibility in canonical types."""
    assert types_compatible("short", "smallint") is True
    assert types_compatible("short", "integer") is True
    assert types_compatible("short", "bigint") is True
    assert types_compatible("smallint", "integer") is True
    assert types_compatible("smallint", "int2") is True
