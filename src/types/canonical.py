# src/types/canonical.py
"""
Vendor-neutral canonical type system for the ETL framework.

All connectors map their native types into canonical types.
Spark derives its type from canonical. Target derives its DDL from canonical.
No Oracle/Postgres-specific logic belongs in Transformer or SchemaValidator.
"""
import re
from enum import Enum
from typing import Optional, Dict, Set


class CanonicalType(str, Enum):
    STRING    = "STRING"
    TEXT      = "STRING"   # Logical alias: STRING is the unified representation for all character/text data
    INTEGER   = "INTEGER"
    LONG      = "LONG"
    DECIMAL   = "DECIMAL"
    DOUBLE    = "DOUBLE"
    BOOLEAN   = "BOOLEAN"
    DATE      = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BINARY    = "BINARY"
    UNKNOWN   = "UNKNOWN"


# ── Source Adapters (native type → canonical) ─────────────────────────────
ORACLE_TO_CANONICAL: Dict[str, CanonicalType] = {
    "clob":            CanonicalType.STRING,
    "nclob":           CanonicalType.STRING,
    "blob":            CanonicalType.BINARY,
    "raw":             CanonicalType.BINARY,
    "long raw":        CanonicalType.BINARY,
    "varchar2":        CanonicalType.STRING,
    "nvarchar2":       CanonicalType.STRING,
    "char":            CanonicalType.STRING,
    "nchar":           CanonicalType.STRING,
    "long":            CanonicalType.STRING,
    "number":          CanonicalType.DECIMAL,   # Default for unconstrained NUMBER
    "integer":         CanonicalType.INTEGER,
    "binary_float":    CanonicalType.DOUBLE,
    "binary_double":   CanonicalType.DOUBLE,
    "date":            CanonicalType.DATE,
    "datetime":        CanonicalType.TIMESTAMP,
    "timestamp":       CanonicalType.TIMESTAMP,
    "timestamp(6)":    CanonicalType.TIMESTAMP,
    "xmltype":         CanonicalType.STRING,
}

MYSQL_TO_CANONICAL: Dict[str, CanonicalType] = {
    "text":       CanonicalType.STRING,
    "mediumtext": CanonicalType.STRING,
    "longtext":   CanonicalType.STRING,
    "varchar":    CanonicalType.STRING,
    "char":       CanonicalType.STRING,
    "int":        CanonicalType.INTEGER,
    "bigint":     CanonicalType.LONG,
    "tinyint":    CanonicalType.INTEGER,
    "smallint":   CanonicalType.INTEGER,
    "decimal":    CanonicalType.DECIMAL,
    "numeric":    CanonicalType.DECIMAL,
    "float":      CanonicalType.DOUBLE,
    "double":     CanonicalType.DOUBLE,
    "date":       CanonicalType.DATE,
    "datetime":   CanonicalType.TIMESTAMP,
    "timestamp":  CanonicalType.TIMESTAMP,
    "boolean":    CanonicalType.BOOLEAN,
    "tinyint(1)": CanonicalType.BOOLEAN,
    "blob":       CanonicalType.BINARY,
}

# ── Canonical → Spark cast string ────────────────────────────────────────
CANONICAL_TO_SPARK: Dict[CanonicalType, str] = {
    CanonicalType.STRING:    "string",
    CanonicalType.INTEGER:   "integer",
    CanonicalType.LONG:      "long",
    CanonicalType.DECIMAL:   "decimal(38,10)",
    CanonicalType.DOUBLE:    "double",
    CanonicalType.BOOLEAN:   "boolean",
    CanonicalType.DATE:      "date",
    CanonicalType.TIMESTAMP: "timestamp",
    CanonicalType.BINARY:    "binary",
}

# ── Canonical → Target DDL (PostgreSQL) ──────────────────────────────────
CANONICAL_TO_POSTGRES: Dict[CanonicalType, str] = {
    CanonicalType.STRING:    "TEXT",
    CanonicalType.INTEGER:   "INTEGER",
    CanonicalType.LONG:      "BIGINT",
    CanonicalType.DECIMAL:   "NUMERIC",
    CanonicalType.DOUBLE:    "DOUBLE PRECISION",
    CanonicalType.BOOLEAN:   "BOOLEAN",
    CanonicalType.DATE:      "DATE",
    CanonicalType.TIMESTAMP: "TIMESTAMP",
    CanonicalType.BINARY:    "BYTEA",
}

# ── Compatibility Matrix ──────────────────────────────────────────────────
# spark_simple_string → set of compatible target DDL type tokens (lowercased)
# "Compatible" means Spark can write this value without data loss or JDBC error.
#
# Design rules:
#  1. Keys are Spark simpleString() values.  The base key (no precision) acts as
#     a wildcard fallback for any decimal(p,s) variant not listed explicitly.
#  2. The token set includes BOTH database DDL surface names (e.g. "double precision")
#     AND Spark simpleString() aliases (e.g. "double", "float") so that the check
#     works regardless of which side provided the type description.
#  3. Floating-point family: decimal/numeric <-> double/float/real are all
#     losslessly widening for PERMISSIVE policy; the validator enforces the cast.

# Shared token sets — kept as module-level constants so future contributors can
# see the full equivalence class in one place rather than duplicating tokens.
_FLOAT_FAMILY: Set[str] = {
    "double precision", "float8",   # PostgreSQL DDL names
    "double",                       # Spark / MySQL simpleString
    "float4", "real",               # PostgreSQL float4 / ANSI REAL
    "float",                        # MySQL / Spark simpleString
}
_NUMERIC_FAMILY: Set[str] = {
    "numeric", "decimal",
}
_INT_FAMILY: Set[str] = {
    "integer", "int", "int4", "bigint", "int8", "long",
}
_SMALL_INT_FAMILY: Set[str] = {
    "smallint", "int2", "short",
}

COMPATIBILITY: Dict[str, Set[str]] = {
    "string":    {"text", "varchar", "character varying", "bpchar", "char", "string"},
    "bigint":    _INT_FAMILY | _NUMERIC_FAMILY | _FLOAT_FAMILY,
    "int":       _INT_FAMILY | _SMALL_INT_FAMILY,
    "integer":   _INT_FAMILY | _SMALL_INT_FAMILY,
    "long":      _INT_FAMILY | _SMALL_INT_FAMILY,
    # Wildcard base key — resolves any decimal(p,s) that has no explicit entry.
    # Decimal is compatible with any floating-point and numeric target column.
    "decimal":   _NUMERIC_FAMILY | _FLOAT_FAMILY,
    # Explicit common precisions (kept for clarity; the base key handles the rest).
    "decimal(38,2)":  _NUMERIC_FAMILY | _FLOAT_FAMILY,
    "decimal(38,10)": _NUMERIC_FAMILY | _FLOAT_FAMILY,
    "double":    _FLOAT_FAMILY | _NUMERIC_FAMILY,
    "float":     _FLOAT_FAMILY | _NUMERIC_FAMILY,
    "timestamp": {"timestamp without time zone", "timestamp", "timestamptz"},
    "date":      {"date", "timestamp without time zone", "timestamp"},
    "boolean":   {"boolean", "bool"},
    "binary":    {"bytea", "binary"},
    "short":     _SMALL_INT_FAMILY | _INT_FAMILY,
    "smallint":  _SMALL_INT_FAMILY | _INT_FAMILY,
}

# ── Connector Capabilities ────────────────────────────────────────────────
# Used by Extractor to auto-decide strategy (no hardcoding in pipeline logic).
CONNECTOR_CAPABILITIES: Dict[str, Dict[str, bool]] = {
    "oracle": {
        "supports_partitioning": True,
        "supports_clob":         True,
        "supports_array":        False,
        "supports_merge":        True,
        "requires_clob_cast":    True,   # CLOBs must be cast to STRING before Spark read
    },
    "postgres": {
        "supports_partitioning": False,
        "supports_text":         True,
        "supports_json":         True,
        "supports_array":        True,
        "requires_clob_cast":    False,
    },
    "mysql": {
        "supports_partitioning": True,
        "supports_clob":         False,
        "supports_array":        False,
        "supports_merge":        False,
        "requires_clob_cast":    False,
        "requires_date_string":  True,   # MySQL year-0000 DATE crash workaround
    },
}


def resolve_canonical(native_type: str, db_type: str) -> CanonicalType:
    """
    Resolve a native database type string to a CanonicalType.
    Supports precision and scale parsing for overloaded numeric types like NUMBER(p, s).
    """
    if not native_type:
        return CanonicalType.UNKNOWN
    
    clean_native = str(native_type).lower().strip()
    db = db_type.lower()
    
    # Precision/scale parsing for Oracle NUMBER
    if ("oracle" in db or "ora" in db) and clean_native.startswith("number"):
        match = re.match(r"number\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?", clean_native)
        if match:
            p_str, s_str = match.group(1), match.group(2)
            if p_str is not None:
                p = int(p_str)
                s = int(s_str) if s_str is not None else 0
                if s == 0:
                    if p <= 9:
                        return CanonicalType.INTEGER
                    else:
                        return CanonicalType.LONG
                else:
                    return CanonicalType.DECIMAL
            return CanonicalType.DECIMAL
            
    adapter = {
        "oracle": ORACLE_TO_CANONICAL,
        "ora":    ORACLE_TO_CANONICAL,
        "mysql":  MYSQL_TO_CANONICAL,
    }.get(db, {})
    
    base_native = clean_native.split("(")[0].strip()
    return adapter.get(base_native, CanonicalType.UNKNOWN)


def types_compatible(spark_simple: str, pg_ddl_type: str) -> bool:
    """Return True when spark_simple type is safely writable into pg_ddl_type column.

    Resolution order (first match wins):
      1. Exact string equality.
      2. Base-type equality (strips precision, e.g. decimal(38,10) -> decimal).
      3. Full precision key lookup in COMPATIBILITY.
      4. Base-type key lookup in COMPATIBILITY (wildcard fallback for any
         decimal(p,s) variant, any varchar(n) variant, etc.).

    This means adding a new precision like decimal(20,5) does NOT require a
    new COMPATIBILITY entry — the "decimal" base key handles it automatically.
    """
    if not spark_simple or not pg_ddl_type:
        return True
    s, t = spark_simple.lower().strip(), pg_ddl_type.lower().strip()
    if s == t:
        return True
    # Strip precision specifiers, e.g. decimal(38,10) -> decimal, varchar(255) -> varchar
    base_s = s.split("(")[0].strip()
    base_t = t.split("(")[0].strip()
    if base_s == base_t:
        return True
    # Lookup: prefer the full precision key, fall back to the base-type wildcard.
    compat_set = COMPATIBILITY.get(s) or COMPATIBILITY.get(base_s, set())
    return t in compat_set or base_t in compat_set


def get_spark_column_expr(actual_src: str, canonical: Optional[CanonicalType], dtype: Optional[str] = None):
    """
    Centralized Spark column expression builder.
    Maps canonical types to Spark cast expressions with date/timestamp sanitization.
    """
    from pyspark.sql import functions as F
    col_expr = F.col(actual_src)
    spark_cast = CANONICAL_TO_SPARK.get(canonical) if canonical else None
    if not spark_cast and dtype:
        spark_cast = str(dtype).replace("Type", "").lower()

    if spark_cast == "timestamp":
        casted = col_expr.cast("timestamp")
        return F.when(casted.isNull() | (F.year(casted) < 1900), F.lit(None).cast("timestamp")).otherwise(casted)
    elif spark_cast == "date":
        casted = col_expr.cast("date")
        return F.when(casted.isNull() | (F.year(casted) < 1900), F.lit(None).cast("date")).otherwise(casted)
    elif spark_cast:
        return col_expr.cast(spark_cast)
    else:
        return col_expr
