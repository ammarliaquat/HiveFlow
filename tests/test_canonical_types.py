# tests/test_canonical_types.py
"""
Unit tests for vendor-neutral canonical type system.
"""
from src.types.canonical import (
    CanonicalType,
    resolve_canonical,
    types_compatible,
    CANONICAL_TO_SPARK,
    CANONICAL_TO_POSTGRES,
)


def test_resolve_canonical_oracle():
    """Verify Oracle native types map correctly to canonical types."""
    assert resolve_canonical("CLOB", "oracle") == CanonicalType.STRING
    assert resolve_canonical("NCLOB", "oracle") == CanonicalType.STRING
    assert resolve_canonical("VARCHAR2(255)", "oracle") == CanonicalType.STRING
    assert resolve_canonical("NUMBER", "oracle") == CanonicalType.DECIMAL
    assert resolve_canonical("NUMBER(5)", "oracle") == CanonicalType.INTEGER
    assert resolve_canonical("NUMBER(10)", "oracle") == CanonicalType.LONG
    assert resolve_canonical("NUMBER(18,2)", "oracle") == CanonicalType.DECIMAL
    assert resolve_canonical("DATE", "oracle") == CanonicalType.DATE
    assert resolve_canonical("BLOB", "oracle") == CanonicalType.BINARY


def test_resolve_canonical_mysql():
    """Verify MySQL native types map correctly to canonical types."""
    assert resolve_canonical("LONGTEXT", "mysql") == CanonicalType.STRING
    assert resolve_canonical("INT", "mysql") == CanonicalType.INTEGER
    assert resolve_canonical("DATETIME", "mysql") == CanonicalType.TIMESTAMP


def test_types_compatible():
    """Verify type compatibility matrix eliminates spurious warnings."""
    # Bigint -> int/bigint/numeric
    assert types_compatible("bigint", "integer") is True
    assert types_compatible("bigint", "bigint") is True
    assert types_compatible("bigint", "numeric") is True
    assert types_compatible("bigint", "double precision") is True

    # Decimal -> numeric / double precision
    assert types_compatible("decimal(38,2)", "numeric") is True
    assert types_compatible("decimal(38,2)", "double precision") is True

    # String -> text / varchar
    assert types_compatible("string", "text") is True
    assert types_compatible("string", "character varying") is True

    # Incompatible types
    assert types_compatible("boolean", "integer") is False


def test_db_syntax_dialect_helpers():
    """Verify quote_column_name and get_table_alias_keyword dialect helpers."""
    from src.utils.db_utils import quote_column_name, get_table_alias_keyword, quote_table_name

    # MySQL quoting and alias
    assert quote_column_name("user_id", "mysql") == "`user_id`"
    assert get_table_alias_keyword("mysql") == " AS "

    # Oracle quoting and alias
    assert quote_column_name("user_id", "oracle") == "user_id"
    assert get_table_alias_keyword("oracle") == " "

    # Postgres quoting and alias
    assert quote_column_name("user_id", "postgres") == '"user_id"'
    assert get_table_alias_keyword("postgres") == " AS "

