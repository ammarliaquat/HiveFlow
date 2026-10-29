# src/discovery/type_mapper.py
"""
Source → Spark type mapping.
Used by the Weekly Crawler and ColumnMapper for consistency.
"""

def map_source_to_spark_type(source_data_type: str, precision: int = None, scale: int = None) -> str:
    """
    Map a source database datatype to a Spark type string.
    
    Args:
        source_data_type: Raw datatype from the source (case-insensitive).
        precision: Numeric precision (if applicable).
        scale: Numeric scale (if applicable).
    
    Returns:
        Spark type string (e.g., 'StringType', 'IntegerType', 'DecimalType(18,2)').
    """
    dtype = source_data_type.upper().strip()
    
    # String types
    if dtype in ('VARCHAR', 'VARCHAR2', 'CHAR', 'TEXT', 'STRING', 'CLOB', 'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT', 'NVARCHAR'):
        return 'StringType'
    
    # Integer types
    if dtype in ('INT', 'INTEGER', 'SMALLINT', 'BIGINT', 'TINYINT', 'SERIAL', 'BIGSERIAL', 'INT2', 'INT4', 'INT8'):
        return 'IntegerType'
    
    # Decimal / Numeric types
    if dtype in ('NUMBER', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'DOUBLE PRECISION', 'REAL'):
        if precision is None or precision == 0:
            return 'DecimalType(18,2)'
        else:
            scale = scale if scale is not None else 0
            return f'DecimalType({precision},{scale})'
    
    # Temporal types
    if dtype in ('DATE', 'TIMESTAMP', 'DATETIME', 'DATETIME2', 'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'):
        return 'TimestampType'
    
    # Boolean
    if dtype in ('BOOLEAN', 'BIT', 'BOOL'):
        return 'BooleanType'
    
    # Binary
    if dtype in ('BLOB', 'BYTEA', 'BINARY', 'VARBINARY', 'IMAGE', 'OID'):
        return 'BinaryType'
    
    # JSON / XML / UUID
    if dtype in ('JSON', 'JSONB', 'XML', 'UUID'):
        return 'StringType'
    
    # Default fallback
    return 'StringType'


UPDATE_HINTS = [
    "updated_at", "last_updated", "last_modified", "modified_at",
    "update_date", "change_date", "dml_timestamp", "modifieddate", "updatetime"
]

INSERT_HINTS = [
    "created_at", "created_date", "registration_date", "insert_date", "createddate"
]

VERSION_HINTS = [
    "version_no", "row_version", "revision"
]

# A business key is an identity, not a best-effort deduplication hint.  It is
# therefore strictly unique; discovery may instead prove a composite key.
CDC_SEQUENCE_CURSOR_MIN_DISTINCT_RATIO = 0.95

# Conditional-key profiling is deliberately small and bounded.  These values
# are only the compatibility defaults for callers that have not yet supplied
# the validated policy object; promoted execution must freeze the actual
# policy thresholds in its evidence.
CONDITIONAL_KEY_DEFAULT_MAX_EXCEPTION_VALUES = 5
CONDITIONAL_KEY_DEFAULT_MAX_EXCEPTION_ROWS = 10_000
CONDITIONAL_KEY_DEFAULT_MAX_EXCEPTION_FRACTION = 0.005


def build_bounded_duplicate_query(
    table_fqn: str,
    column_name: str,
    db_type: str,
    max_groups: int = None,
    *,
    limit: int = None,
) -> str:
    """Build a dialect-safe query returning at most *max_groups* duplicates.

    The query is used only for bounded discovery evidence.  Identifiers are
    validated/quoted by the shared db_utils helpers; the bound is validated as
    an integer before it is rendered.  Values are returned as ``duplicate_value``
    together with their occurrence count, ordered deterministically so a
    ``max_groups + 1`` probe can prove the configured bound was exceeded.

    No database-specific syntax leaks into the profiling algorithm: LIMIT,
    TOP, and ROWNUM are selected here at the query boundary.
    """
    if max_groups is not None and limit is not None:
        raise ValueError("provide either max_groups or limit, not both")
    max_groups = limit if limit is not None else max_groups
    try:
        limit = int(max_groups)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_groups must be a positive integer") from exc
    if isinstance(max_groups, bool) or limit <= 0 or limit != max_groups:
        raise ValueError("max_groups must be a positive integer")

    from src.utils.db_utils import (
        get_table_alias_keyword,
        quote_column_name,
        quote_table_name,
    )

    normalized_db_type = str(db_type or "").strip().lower()
    q_column = quote_column_name(column_name, normalized_db_type)
    q_table = quote_table_name(table_fqn, normalized_db_type)
    alias_keyword = get_table_alias_keyword(normalized_db_type)

    # SQL Server applies TOP in the SELECT list; LIMIT is not valid there.
    if normalized_db_type in ("sqlserver", "mssql"):
        return (
            f"SELECT TOP ({limit}) {q_column} AS duplicate_value, "
            f"COUNT(1) AS occurrences FROM {q_table} "
            f"WHERE {q_column} IS NOT NULL GROUP BY {q_column} "
            f"HAVING COUNT(1) > 1 "
            f"ORDER BY COUNT(1) DESC, {q_column}"
        )

    duplicate_query = (
        f"SELECT {q_column} AS duplicate_value, COUNT(1) AS occurrences "
        f"FROM {q_table} WHERE {q_column} IS NOT NULL GROUP BY {q_column} "
        f"HAVING COUNT(1) > 1 ORDER BY COUNT(1) DESC, {q_column}"
    )
    if normalized_db_type in ("oracle", "ora"):
        # Oracle 11g has no FETCH FIRST syntax.  Keep the ordering in the
        # inner query and apply ROWNUM in the outer query.
        return (
            f"SELECT duplicate_value, occurrences FROM ({duplicate_query})"
            f"{alias_keyword}duplicate_groups WHERE ROWNUM <= {limit}"
        )
    if normalized_db_type in ("postgres", "postgresql", "postgressql", "mysql"):
        return f"{duplicate_query} LIMIT {limit}"
    raise ValueError(f"Unsupported database type for bounded duplicate query: {db_type!r}")


# Private spelling retained for callers/tests that use the project's existing
# helper naming convention.
_build_bounded_duplicate_query = build_bounded_duplicate_query


def _build_bounded_exception_complement_query(
    table_fqn: str,
    column_name: str,
    db_type: str,
    exception_values,
) -> str:
    """Build the aggregate proving the non-exception key set is unique.

    Nullability is checked by the full-table profile before this query.  The
    complement therefore uses a typed integer ``NOT IN`` list and only returns
    aggregate counts; it never collects source rows on the driver.
    """
    from src.utils.db_utils import quote_column_name, quote_table_name

    values = tuple(exception_values or ())
    if not values:
        raise ValueError("exception_values must contain at least one value")
    from numbers import Integral

    if any(isinstance(value, bool) or not isinstance(value, Integral) for value in values):
        raise ValueError("exception_values must contain typed integer values")
    literals = ", ".join(str(int(value)) for value in values)
    q_column = quote_column_name(column_name, str(db_type or "").strip().lower())
    q_table = quote_table_name(table_fqn, str(db_type or "").strip().lower())
    return (
        f"SELECT COUNT(1) AS non_exception_rows, "
        f"COUNT(DISTINCT {q_column}) AS non_exception_distinct "
        f"FROM {q_table} WHERE {q_column} NOT IN ({literals})"
    )


def _conditional_key_limits(policy=None):
    """Resolve and validate bounded-key limits from a mapping/object.

    ``None`` uses compatibility defaults so legacy callers retain behaviour;
    production callers should pass the validated pipeline policy object and
    persist these resolved values with the candidate evidence.
    """
    def _value(name, default):
        if policy is None:
            return default
        if isinstance(policy, dict):
            return policy.get(name, default)
        return getattr(policy, name, default)

    raw_values = (
        _value("max_exception_values", CONDITIONAL_KEY_DEFAULT_MAX_EXCEPTION_VALUES),
        _value("max_exception_rows", CONDITIONAL_KEY_DEFAULT_MAX_EXCEPTION_ROWS),
        _value("max_exception_fraction", CONDITIONAL_KEY_DEFAULT_MAX_EXCEPTION_FRACTION),
    )
    max_values, max_rows, max_fraction = raw_values

    def _integer_bound(value):
        if isinstance(value, bool):
            raise ValueError("conditional-key bounds must use integer values")
        try:
            integer = int(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("conditional-key bounds must use integer values") from exc
        try:
            if float(value) != float(integer):
                raise ValueError("conditional-key bounds must use integer values")
        except (TypeError, ValueError, OverflowError):
            raise ValueError("conditional-key bounds must use integer values")
        return integer

    try:
        max_values = _integer_bound(max_values)
        max_rows = _integer_bound(max_rows)
        max_fraction = float(max_fraction)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid conditional-key profiling bounds") from exc
    if max_values <= 0 or max_rows <= 0 or not 0 < max_fraction <= 1:
        raise ValueError("conditional-key profiling bounds must be positive")
    return max_values, max_rows, max_fraction


def _coerce_int64_key(value):
    """Return a typed integral value as Python ``int`` or ``None`` when unsafe.

    JDBC numeric values arrive as Python ``int``/``Decimal`` (and sometimes a
    NumPy integral).  Do not coerce strings or floats: two distinct textual or
    floating-point keys could otherwise collapse to the same exception value
    in the typed conditional-key contract.
    """
    from decimal import Decimal
    from numbers import Integral

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Integral):
        candidate = int(value)
    elif isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        candidate = int(value)
    else:
        return None
    if not -(2**63) <= candidate <= (2**63 - 1):
        return None
    return candidate


def _duplicate_group_values(rows):
    """Extract bounded duplicate groups from Spark rows without guessing types."""
    values = []
    for row in rows or []:
        item = _extract_row_dict(row)
        value = item.get("duplicate_value")
        occurrences = item.get("occurrences")
        if value is None:
            try:
                value = row[0]
            except Exception:
                value = None
        if occurrences is None:
            try:
                occurrences = row[1]
            except Exception:
                occurrences = None
        normalized = _coerce_int64_key(value)
        try:
            occurrences = int(occurrences)
        except (TypeError, ValueError):
            occurrences = 0
        if normalized is not None and occurrences > 1:
            values.append((normalized, occurrences))
    return values


def _extract_row_dict(row) -> dict:
    if hasattr(row, "asDict") and callable(getattr(row, "asDict")):
        try:
            d = row.asDict()
            if isinstance(d, dict):
                return {str(k).lower(): v for k, v in d.items()}
        except Exception:
            pass
    if isinstance(row, dict):
        return {str(k).lower(): v for k, v in row.items()}
    res = {}
    candidate_keys = [
        "total", "pop", "dist_cnt", "min_v", "max_v",
        "total_rows", "non_null_rows", "distinct_rows",
        "null_cnt", "distinct_cnt", "max_val", "min_val",
        "duplicate_value", "occurrences", "duplicate_count",
    ]
    for k in candidate_keys:
        try:
            res[k] = row[k]
        except Exception:
            pass
    return res


def detect_cdc_columns(columns: list) -> list:
    """
    Auto-detect CDC (Change Data Capture) columns from a list of column names.
    Legacy entrypoint wrapper returning list of detected columns.
    """
    res = detect_cdc_columns_profiled(columns)
    if res["cdc_columns"]:
        return [c.strip() for c in res["cdc_columns"].split(",") if c.strip()]
    return []


def detect_cdc_columns_profiled(
    columns: list,
    configured_cdc: str = None,
    configured_source: str = None,
    spark_session=None,
    conn_config=None,
    table_fqn: str = None,
    business_key: str = None,
    key_confidence: int = 100,
    key_source: str = None,
    last_profile_date=None,
    force_reprofile: bool = False
) -> dict:
    """
    Production-Grade CDC Discovery & Profiling Engine with Computed Scoring.

    1. Metadata Governance Precedence (Strict: Manual Override NEVER overwritten)
    2. Native Provider Detection (TIMESTAMP, ORA_ROWSCN, FLASHBACK, POSTGRES_WAL, SQLSERVER_CDC, MYSQL_BINLOG)
    3. Categorized Candidate Discovery (Tier 1 UPDATE vs Tier 2 INSERT vs Tier 3 VERSION)
    4. Profile Caching (Skips expensive queries if profiled within 7 days unless forced)
    5. Data Profiling & Monotonicity Validation (Datatype, NULL %, Distinct Ratio, Freshness)
    6. Unique Key Validation (Refuses active CDC if business key missing/unconfident -> PENDING_APPROVAL)
    7. Dynamic Computed Confidence Score (Datatype 30 + Nulls 20 + Variation 20 + Freshness/sequence 20 + Physical key 10)
    8. Rich Multi-Check Health Diagnostic Payload (JSON)
    """
    import json
    from datetime import datetime, timedelta

    column_defs = {
        str(c.get("column_name", "")).strip().lower(): c
        for c in (columns or [])
        if isinstance(c, dict) and c.get("column_name")
    }

    def _is_sequence_cursor_type(col_name: str) -> bool:
        """Return whether a source column has an exact integer sequence representation.

        The runtime persists narrow values as INT64 and wider integer NUMBER
        values as exact DECIMAL cursors.  Discovery must recognise both;
        otherwise a valid Oracle NUMBER(38) business key is incorrectly
        displaced by a date-name hint and the table falls back to a full scan.
        """
        column = column_defs.get(str(col_name).strip().lower(), {})
        data_type = str(column.get("data_type") or "").upper().strip()
        if data_type in {
            "INT", "INT2", "INT4", "INT8", "INTEGER", "SMALLINT", "TINYINT",
            "MEDIUMINT", "BIGINT", "BYTEINT", "SHORT", "LONG",
        }:
            return True
        if data_type not in {"NUMBER", "DECIMAL", "NUMERIC", "DEC"}:
            return False
        try:
            precision = int(column.get("data_precision")) if column.get("data_precision") is not None else None
        except (TypeError, ValueError):
            precision = None
        try:
            scale = int(column.get("data_scale")) if column.get("data_scale") is not None else None
        except (TypeError, ValueError):
            scale = None
        return precision is not None and 0 < precision <= 38 and (scale is None or scale == 0)

    def _is_business_key_cursor(col_name: str) -> bool:
        if not col_name or not business_key or (key_confidence or 0) < 50:
            return False
        key_parts = [part.strip().lower() for part in str(business_key).split(",") if part.strip()]
        # A sequence high-watermark is itself a single-column identity.  A
        # composite key may still be an exact merge identity, but no one
        # component can safely stand in for the composite cursor.
        return len(key_parts) == 1 and str(col_name).strip().lower() in key_parts

    def _has_valid_single_business_key() -> bool:
        key_parts = [part.strip() for part in str(business_key or "").split(",") if part.strip()]
        return (
            len(key_parts) == 1
            and (key_confidence or 0) >= 80
            and str(key_source or "").strip().upper() in ("PRIMARY_KEY", "UNIQUE_CONSTRAINT", "UNIQUE", "PROFILED", "IDENTITY")
        )

    def _has_physical_single_business_key() -> bool:
        key_parts = [part.strip() for part in str(business_key or "").split(",") if part.strip()]
        return (
            len(key_parts) == 1
            and str(key_source or "").strip().upper() in ("PRIMARY_KEY", "UNIQUE_CONSTRAINT", "UNIQUE", "IDENTITY")
            and (key_confidence or 0) >= 100
        )

    def _is_physical_sequence_cursor(col_name: str) -> bool:
        return (
            _is_business_key_cursor(col_name)
            and _has_physical_single_business_key()
            and _is_sequence_cursor_type(col_name)
        )

    def _is_qualified_sequence_cursor(col_name: str) -> bool:
        key_parts = [part.strip() for part in str(business_key or "").split(",") if part.strip()]
        source = str(key_source or "").strip().upper()
        # A composite identity may use one numeric component as its bounded
        # insertion cursor.  This is safe only when the composite identity was
        # itself fully profiled; a name-based or unverified composite must not
        # silently activate sequence CDC.
        if len(key_parts) > 1 and source != "PROFILED_COMPOSITE":
            return False
        return (
            bool(key_parts)
            and str(col_name).strip().lower() in {part.lower() for part in key_parts}
            and (key_confidence or 0) >= 90
            and source in {
                "PRIMARY_KEY", "UNIQUE_CONSTRAINT", "UNIQUE", "IDENTITY", "PROFILED", "PROFILED_COMPOSITE"
            }
            and _is_sequence_cursor_type(col_name)
        )

    def _has_qualified_single_business_key_for_scoring() -> bool:
        key_parts = [part.strip() for part in str(business_key or "").split(",") if part.strip()]
        return any(_is_qualified_sequence_cursor(column) for column in key_parts)

    # Helper to resolve strategy for specific column name
    def _resolve_strategy_for_col(col_name: str) -> tuple:
        if not col_name:
            return "FULL_LOAD", "NONE"
        if _is_business_key_cursor(col_name) and _is_sequence_cursor_type(col_name):
            return "SEQUENCE_HIGH_WATERMARK", "SEQUENCE_HIGH_WATERMARK"
        c_lower = str(col_name).lower().strip()
        if c_lower in INSERT_HINTS or any(h in c_lower for h in ("create", "register", "insert")):
            return "INSERT_TIMESTAMP", "INSERT_TIMESTAMP"
        return "TIMESTAMP_HIGH_WATERMARK", "TIMESTAMP_HIGH_WATERMARK"

    # ── 1. Metadata Governance Precedence ──────────────────────────────────────
    if configured_source == "METADATA" and configured_cdc and str(configured_cdc).strip():
        strat, prov = _resolve_strategy_for_col(configured_cdc)
        active_cdc = strat == "SEQUENCE_HIGH_WATERMARK"
        health_details = {
            "datatype_ok": True,
            "null_pct": 0.0,
            "distinct_ratio_pct": 100.0,
            "recent_activity": True,
            "unique_key_valid": True,
            "confidence_breakdown": {"manual_override": 100}
        }
        return {
            "cdc_columns": configured_cdc,
            "is_cdc": "y" if active_cdc else "n",
            "cdc_strategy": strat,
            "cdc_provider": prov,
            "cdc_source": "METADATA",
            "cdc_confidence": 100,
            "cdc_health": "OK" if active_cdc else "WARNING",
            "cdc_null_rows": 0,
            "cdc_health_details": json.dumps(health_details)
        }

    # ── 2. Profile Caching Check ──────────────────────────────────────────────
    if last_profile_date and not force_reprofile:
        if isinstance(last_profile_date, str):
            try:
                last_profile_date = datetime.strptime(last_profile_date.split('.')[0], "%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        if isinstance(last_profile_date, datetime):
            if datetime.now() - last_profile_date < timedelta(days=7):
                # Return cached status without executing profiling query
                strat, prov = _resolve_strategy_for_col(configured_cdc) if configured_cdc else ("FULL_LOAD", "NONE")
                active_cdc = configured_cdc and strat == "SEQUENCE_HIGH_WATERMARK"
                return {
                    "cdc_columns": configured_cdc,
                    "is_cdc": "y" if active_cdc else "n",
                    "cdc_strategy": strat,
                    "cdc_provider": prov,
                    "cdc_source": "PROFILED_CACHED",
                    "cdc_confidence": 95 if configured_cdc else 0,
                    "cdc_health": "OK" if active_cdc else "WARNING",
                    "cdc_null_rows": 0,
                    "cdc_health_details": json.dumps({"cached": True})
                }


    # ── 3. Candidate Categorization ──────────────────────────────────────────
    col_names = [
        c["column_name"] if isinstance(c, dict) and "column_name" in c else str(c)
        for c in (columns or [])
    ]
    cols_map = {c.lower().strip(): c for c in col_names}
    tier1_matches = [cols_map[h] for h in UPDATE_HINTS if h in cols_map]
    tier2_matches = [cols_map[h] for h in INSERT_HINTS if h in cols_map]
    tier3_matches = [cols_map[h] for h in VERSION_HINTS if h in cols_map]

    if not tier1_matches:
        for c in col_names:
            if any(h in c.lower() for h in ("update", "modified", "change")):
                tier1_matches.append(c)

    if not tier2_matches:
        for c in col_names:
            if any(h in c.lower() for h in ("create", "register", "insert")):
                tier2_matches.append(c)

    candidates = []
    if tier1_matches:
        candidates.append((tier1_matches[0], "TIMESTAMP_HIGH_WATERMARK", "TIMESTAMP_HIGH_WATERMARK", "UPDATE"))
    else:
        # INSERT_TIMESTAMP is not an executable governed strategy.  Prefer a
        # fully profiled numeric business key for append-only CDC when the
        # source has no update cursor; otherwise retain the insert hint as a
        # diagnostic recommendation.
        key_parts = [part.strip() for part in str(business_key or "").split(",") if part.strip()]
        sequence_keys = [part for part in key_parts if _is_qualified_sequence_cursor(part)]
        if sequence_keys:
            candidates.append((sequence_keys[0], "SEQUENCE_HIGH_WATERMARK", "SEQUENCE_HIGH_WATERMARK", "SEQUENCE"))
        elif tier2_matches:
            candidates.append((tier2_matches[0], "INSERT_TIMESTAMP", "INSERT_TIMESTAMP", "INSERT"))
    if tier3_matches:
        candidates.append((tier3_matches[0], "TIMESTAMP_HIGH_WATERMARK", "TIMESTAMP_HIGH_WATERMARK", "VERSION"))

    # Prefer a profiled update cursor when one exists: it captures mutations
    # that a monotonically increasing primary key cannot see.  Use the primary
    # key as a sequence cursor only when there is no change timestamp.
    if not candidates:
        physical_key_columns = [
            col for col in col_names if _is_physical_sequence_cursor(col)
        ]
        if physical_key_columns:
            candidates.append((
                physical_key_columns[0],
                "SEQUENCE_HIGH_WATERMARK",
                "SEQUENCE_HIGH_WATERMARK",
                "SEQUENCE",
            ))

    if not candidates:
        health_details = {
            "datatype_ok": False,
            "null_pct": 100.0,
            "distinct_ratio_pct": 0.0,
            "recent_activity": False,
            "unique_key_valid": bool(business_key),
            "confidence_breakdown": {}
        }
        return {
            "cdc_columns": None,
            "is_cdc": "n",
            "cdc_strategy": "FULL_LOAD",
            "cdc_provider": "NONE",
            "cdc_source": "NONE",
            "cdc_confidence": 0,
            "cdc_health": "OK",
            "cdc_null_rows": 0,
            "cdc_health_details": json.dumps(health_details)
        }

    # ── 4. Data Profiling & Computed Confidence Scoring ──────────────────────
    profile_error = None
    if spark_session and conn_config and table_fqn:
        try:
            from src.utils.db_utils import apply_jdbc_options, quote_column_name, quote_table_name
            db_type = getattr(conn_config, "db_type", "postgres").lower()
            alias_kw = " " if db_type == "oracle" else " AS "

            for cand_col, strategy, provider, tier_type in candidates:
                # Use the shared dialect helper.  Quoting a lower-case
                # Oracle catalog name (e.g. "emigrant_id") makes it a
                # case-sensitive identifier and fails against normal,
                # unquoted Oracle columns (EMIGRANT_ID).
                q_cand = quote_column_name(cand_col, db_type)
                prof_sql = (
                    f"SELECT COUNT(1) AS total, COUNT({q_cand}) AS pop, "
                    f"COUNT(DISTINCT {q_cand}) AS dist_cnt, "
                    f"MIN({q_cand}) AS min_v, MAX({q_cand}) AS max_v FROM {quote_table_name(table_fqn, db_type)}"
                )
                try:
                    df = (
                        apply_jdbc_options(spark_session.read.format("jdbc"), conn_config)
                        .option("dbtable", f"({prof_sql}){alias_kw}prof")
                        .load()
                    )
                    row = df.collect()[0]
                    row_dict_lower = _extract_row_dict(row)

                    dist_cnt = int(row_dict_lower.get("dist_cnt", row_dict_lower.get("distinct_cnt", row_dict_lower.get("distinct_rows", 0))) or 0)
                    total = int(row_dict_lower.get("total", row_dict_lower.get("total_rows", dist_cnt or 100)) or 100)
                    null_cnt = int(row_dict_lower.get("null_cnt", 0) if "null_cnt" in row_dict_lower else max(0, total - int(row_dict_lower.get("pop", row_dict_lower.get("non_null_rows", total)) or total)))
                    populated = int(row_dict_lower.get("pop", row_dict_lower.get("non_null_rows", total - null_cnt)) or (total - null_cnt))
                    max_val = row_dict_lower.get("max_v", row_dict_lower.get("max_val"))
                    min_val = row_dict_lower.get("min_v", row_dict_lower.get("min_val", 1 if dist_cnt > 0 else None))

                    null_pct = (null_cnt / float(total)) * 100.0 if total > 0 else 0.0
                    distinct_ratio = (dist_cnt / float(populated)) if populated > 0 else 0.0
                    duplicate_count = max(0, total - dist_cnt)

                    # ── Reject Conditions ──
                    if null_pct > 95.0:
                        continue   # Reject candidate if >95% NULL
                    if total > 5 and dist_cnt <= 1:
                        continue   # Reject candidate if MIN == MAX / 1 distinct value

                    # ── Computed Confidence Score Components ──
                    # 1. Datatype Score (Max 30)
                    datatype_score = 30
                    # 2. NULL Score (Max 20)
                    null_score = int(round(20.0 * (1.0 - (null_pct / 100.0))))
                    # 3. Variation / Distinct Score (Max 20)
                    variation_score = int(round(20.0 * min(1.0, distinct_ratio * 10.0)))
                    # 4. Freshness / sequence-shape Score (Max 20)
                    # A timestamp can be a valid CDC cursor even when the
                    # table has not changed recently (for example a static
                    # reference table).  Freshness is useful operator
                    # telemetry, but it is not proof of cursor correctness
                    # and must not disable an otherwise complete profile.
                    freshness_score = 10
                    recent_activity = False
                    sequence_shape_valid = False
                    if strategy == "SEQUENCE_HIGH_WATERMARK":
                        # A sequence cursor may be one component of a composite
                        # business key.  The identity is strictly profiled
                        # separately; the cursor itself must be numeric,
                        # non-null, and high-cardinality enough to bound an
                        # append-only window.
                        from numbers import Number
                        key_parts = [part.strip() for part in str(business_key or "").split(",") if part.strip()]
                        sequence_shape_valid = (
                            _is_sequence_cursor_type(cand_col)
                            and
                            isinstance(min_val, Number)
                            and not isinstance(min_val, bool)
                            and isinstance(max_val, Number)
                            and not isinstance(max_val, bool)
                            and total > 0
                            and null_cnt == 0
                            and distinct_ratio >= CDC_SEQUENCE_CURSOR_MIN_DISTINCT_RATIO
                            and (len(key_parts) > 1 or duplicate_count == 0)
                            and max_val >= min_val
                        )
                        freshness_score = 20 if sequence_shape_valid else 0
                    elif max_val is not None:
                        try:
                            if isinstance(max_val, str):
                                max_dt = datetime.strptime(max_val.split('.')[0], "%Y-%m-%d %H:%M:%S")
                            elif isinstance(max_val, (datetime, datetime)):
                                max_dt = max_val
                            else:
                                max_dt = None
                            if max_dt:
                                freshness_score = 20
                                recent_activity = (
                                    datetime.now() - max_dt.replace(tzinfo=None)
                                ) < timedelta(days=60)
                        except Exception:
                            freshness_score = 10

                    # 5. Unique Key Alignment Check (Rule 7).  The existing
                    # formula documented a 10-point index/key component but
                    # omitted it, making a confidence above 90 impossible.
                    unique_key_valid = bool(business_key and str(business_key).strip() and (key_confidence or 0) >= 50)
                    key_penalty = 0 if unique_key_valid else -25
                    physical_key_score = 10 if _has_physical_single_business_key() else 0
                    qualified_key_score = 10 if _has_qualified_single_business_key_for_scoring() else 0

                    computed_confidence = max(
                        0,
                        min(
                            100,
                            datatype_score + null_score + variation_score
                            + freshness_score + max(physical_key_score, qualified_key_score) + key_penalty,
                        ),
                    )

                    # ── Health Diagnostic Payload ──
                    health_details = {
                        "datatype_ok": True,
                        "null_pct": round(null_pct, 2),
                        "distinct_ratio_pct": round(distinct_ratio * 100.0, 2),
                        "recent_activity": recent_activity,
                        "unique_key_valid": unique_key_valid,
                        "physical_sequence_key": _is_physical_sequence_cursor(cand_col),
                        "physical_business_key": _has_physical_single_business_key(),
                        "sequence_shape_valid": sequence_shape_valid,
                        "cursor_type_valid": _is_sequence_cursor_type(cand_col),
                        "cursor_duplicate_count": duplicate_count,
                        "cursor_distinct_ratio_pct": round(distinct_ratio * 100.0, 4),
                        "confidence_breakdown": {
                            "datatype": datatype_score,
                            "nulls": null_score,
                            "variation": variation_score,
                            "freshness": freshness_score,
                            "physical_key": physical_key_score,
                            "qualified_key": qualified_key_score,
                            "unique_key_penalty": key_penalty
                        }
                    }

                    # Determine Strategy & Health Status (Rule 7 & 9)
                    if not unique_key_valid:
                        effective_strategy = "PENDING_APPROVAL"
                        health_status = "WARNING"
                    else:
                        effective_strategy = strategy
                        health_status = "WARNING" if (null_cnt > 0 or computed_confidence < 70) else "OK"

                    auto_sequence_cdc = (
                        effective_strategy == "SEQUENCE_HIGH_WATERMARK"
                        and _is_qualified_sequence_cursor(cand_col)
                        and sequence_shape_valid
                        and computed_confidence >= 90
                    )
                    auto_timestamp_cdc = (
                        effective_strategy == "TIMESTAMP_HIGH_WATERMARK"
                        and _has_valid_single_business_key()
                        and null_cnt == 0
                        and computed_confidence >= 90
                    )
                    auto_cdc = auto_sequence_cdc or auto_timestamp_cdc
                    if not auto_cdc:
                        health_status = "WARNING"

                    if strategy == "SEQUENCE_HIGH_WATERMARK" and not auto_sequence_cdc:
                        # A failed sequence qualification must not fall through
                        # to the non-executable INSERT_TIMESTAMP hint.
                        return {
                            "cdc_columns": None,
                            "is_cdc": "n",
                            "cdc_strategy": "FULL_SNAPSHOT",
                            "cdc_provider": "NONE",
                            "cdc_source": "PROFILED",
                            "cdc_confidence": computed_confidence,
                            "cdc_health": "WARNING",
                            "cdc_null_rows": null_cnt,
                            "cdc_health_details": json.dumps(health_details),
                        }

                    return {
                        "cdc_columns": (
                            f"{cand_col},{business_key}"
                            if auto_timestamp_cdc else cand_col
                        ),
                        "is_cdc": "y" if auto_cdc else "n",
                        "cdc_strategy": effective_strategy,
                        "cdc_provider": provider,
                        "cdc_source": "PROFILED",
                        "cdc_confidence": computed_confidence,
                        "cdc_health": health_status,
                        "cdc_null_rows": null_cnt,
                        "cdc_health_details": json.dumps(health_details)
                    }
                except Exception as exc:
                    # Keep the fallback fail-closed, but retain a safe,
                    # non-secret diagnostic so discovery does not mislabel a
                    # JDBC/query failure as a data-quality rejection.
                    profile_error = {
                        "candidate": str(cand_col),
                        "exception_type": type(exc).__name__,
                    }
        except Exception as exc:
            profile_error = {"exception_type": type(exc).__name__}

    # Unprofiled candidate fallback.  A name-based candidate is evidence only;
    # it must never be persisted as an approved CDC detection method.
    cand_col, strategy, provider, tier_type = candidates[0]
    if strategy == "SEQUENCE_HIGH_WATERMARK" and spark_session and conn_config and table_fqn:
        return {
            "cdc_columns": None,
            "is_cdc": "n",
            "cdc_strategy": "FULL_SNAPSHOT",
            "cdc_provider": "NONE",
            "cdc_source": "PROFILE_ERROR" if profile_error else "PROFILE_REQUIRED",
            "cdc_confidence": 0,
            "cdc_health": "WARNING",
            "cdc_null_rows": 0,
            "cdc_health_details": json.dumps({
                "reason": "Sequence profile query failed" if profile_error else "Sequence profile failed qualification",
                "business_key_requires_zero_duplicates": True,
                **(profile_error or {}),
            }),
        }
    unique_key_valid = bool(business_key and str(business_key).strip() and (key_confidence or 0) >= 50)
    effective_strategy = strategy if unique_key_valid else "PENDING_APPROVAL"
    confidence = 80 if (tier_type == "UPDATE" and unique_key_valid) else 40
    health_details = {
        "datatype_ok": True,
        "null_pct": 0.0,
        "distinct_ratio_pct": 50.0,
        "recent_activity": True,
        "unique_key_valid": unique_key_valid,
        "confidence_breakdown": {"profiling_required": confidence}
    }
    return {
        "cdc_columns": cand_col,
        "is_cdc": "n",
        "cdc_strategy": effective_strategy,
        "cdc_provider": provider,
        "cdc_source": "PROFILE_REQUIRED",
        "cdc_confidence": confidence,
        "cdc_health": "WARNING",
        "cdc_null_rows": 0,
        "cdc_health_details": json.dumps(health_details)
    }


def detect_business_key(columns: list, primary_keys: list = None):
    """
    Return only physical primary-key evidence as an approved legacy key.

    Profiled and heuristic results remain available through
    detect_business_key_profiled(), but are never promoted to execution
    authority by this compatibility helper.
    """
    res = detect_business_key_profiled(columns, primary_keys)
    if res["key_source"] == "PRIMARY_KEY":
        return res["columns_unique_key"]
    return None


def _safe_tag(conn_config) -> str:
    if not conn_config:
        return ""
    tag = getattr(conn_config, "tag", "")
    if hasattr(tag, "_mock_return_value") or "Mock" in type(tag).__name__:
        return ""
    return str(tag or "")


def detect_business_key_profiled(
    columns: list,
    primary_keys: list = None,
    spark_session=None,
    conn_config=None,
    table_fqn: str = None,
    conditional_key_policy=None,
) -> dict:
    """
    Auto-detect business key (natural key) using data profiling against source database.

    Steps:
      1. Physical Primary Key (Confidence: 100%)
      2. Single Candidate Data Profiling (Confidence: 95%)
      3. Composite Pair Data Profiling (Confidence: 90%)
      4. Lowest Duplicate Candidate Profiling (Confidence: 80%)
      5. No candidate when physical or profiled evidence is unavailable.

    Returns dict:
      {
        'columns_unique_key': str or None,
        'identity_mode': 'EXACT_KEY' | 'BOUNDED_EXCEPTION_KEY' | 'KEYLESS',
        'exception_values': list (typed integral values for bounded identity),
        'key_source': 'PRIMARY_KEY' | 'PROFILED' | 'PROFILED_COMPOSITE' | 'PROFILED_CONDITIONAL' | 'PROFILED_LOWEST_DUP' | 'NONE',
        'key_confidence': int (0-100),
        'duplicate_count': int,
        'skip_dedup': 'y' | 'n'
      }
    """
    # ── Step 1: Physical Primary Key (Highest Priority) ────────────────────────
    from src.metadata.policy_models import compute_deterministic_fingerprint
    max_exception_values, max_exception_rows, max_exception_fraction = (
        _conditional_key_limits(conditional_key_policy)
    )

    if primary_keys and len(primary_keys) > 0:
        clean_pks = [p.strip() for p in primary_keys if p and p.strip()]
        fingerprint = compute_deterministic_fingerprint({
            "source_tag": _safe_tag(conn_config),
            "table_fqn": table_fqn or "",
            "key_columns": clean_pks,
            "evidence_type": "PRIMARY_KEY",
        })
        return {
            "columns_unique_key": ",".join(clean_pks),
            "identity_mode": "EXACT_KEY",
            "exception_values": [],
            "exception_row_count": 0,
            "exception_group_count": 0,
            "key_source": "PRIMARY_KEY",
            "key_confidence": 100,
            "duplicate_count": 0,
            "null_count": 0,
            "skip_dedup": "n",
            "key_candidate_columns": clean_pks,
            "key_evidence_type": "PRIMARY_KEY",
            "key_candidate_nullable": False,
            "key_candidate_compatible": True,
            "key_evidence_fingerprint": fingerprint,
        }

    # Generate Candidate Columns (filtering out generic non-key fields)
    excluded_keywords = {
        "created_at", "created_date", "updated_at", "updated_date", "modified_at",
        "modified_date", "last_modified", "dml_timestamp", "dml_status", "status",
        "name", "address", "date", "description", "notes", "comments", "row_hash"
    }
    # Timestamps and row versions can support CDC windows, but they are not
    # stable business identities by themselves.  They are considered only as
    # a profiled partner for a repeated candidate key below.
    excluded_keywords.update(UPDATE_HINTS + INSERT_HINTS + VERSION_HINTS)

    candidates = []
    for c in columns:
        c_lower = c.lower().strip()
        if c_lower in excluded_keywords:
            continue
        # Prioritize key-like columns
        if any(token in c_lower for token in ("_id", "_no", "_code", "_num", "id", "uuid", "guid", "cnic", "passport", "reg", "emigrant", "member")):
            candidates.append(c)

    # Append remaining non-excluded columns as lower priority candidates
    for c in columns:
        if c not in candidates and c.lower().strip() not in excluded_keywords:
            candidates.append(c)

    # ── Step 2 & 3: Single & Composite Data Profiling ─────────────────────────
    lowest_dup_col = None
    lowest_dup_count = float("inf")
    lowest_total_rows = 0
    lowest_null_count = 0

    if spark_session and conn_config and table_fqn and candidates:
        try:
            from src.utils.db_utils import (
                apply_jdbc_options,
                get_table_alias_keyword,
                quote_column_name,
                quote_table_name,
            )
            db_type = getattr(conn_config, "db_type", "postgres").lower()

            def quote_col(c):
                return quote_column_name(c, db_type)

            alias_kw = get_table_alias_keyword(db_type)

            for cand in candidates[:10]:  # Profile top 10 candidates
                q_cand = quote_col(cand)
                profile_sql = (
                    f"SELECT "
                    f"COUNT(1) AS total_rows, "
                    f"COUNT({q_cand}) AS non_null_rows, "
                    f"COUNT(DISTINCT {q_cand}) AS distinct_rows "
                    f"FROM {quote_table_name(table_fqn, db_type)}"
                )
                try:
                    dup_df = (
                        apply_jdbc_options(spark_session.read.format("jdbc"), conn_config)
                        .option("dbtable", f"({profile_sql}){alias_kw}prof")
                        .load()
                    )
                    row = dup_df.collect()[0]
                    row_dict = _extract_row_dict(row)
                    total_rows = int(row_dict.get("total_rows", 0) or 0)
                    non_null_rows = int(row_dict.get("non_null_rows", 0) or 0)
                    distinct_rows = int(row_dict.get("distinct_rows", 0) or 0)
                    null_cnt = max(0, total_rows - non_null_rows)
                    dup_cnt = max(0, non_null_rows - distinct_rows)

                    if total_rows > 0 and null_cnt == 0 and dup_cnt == 0:
                        # Only a truly unique source identity can become a
                        # business key.  If it fails, profile composite pairs
                        # below rather than silently discarding source rows.
                        fingerprint = compute_deterministic_fingerprint({
                            "source_tag": _safe_tag(conn_config),
                            "table_fqn": table_fqn,
                            "key_columns": [cand],
                            "evidence_type": "PROFILED_UNIQUE",
                            "total_rows": total_rows,
                            "null_count": 0,
                            "duplicate_count": 0,
                        })
                        return {
                            "columns_unique_key": cand,
                            "identity_mode": "EXACT_KEY",
                            "exception_values": [],
                            "exception_row_count": 0,
                            "exception_group_count": 0,
                            "key_source": "PROFILED",
                            "key_confidence": 100,
                            "duplicate_count": 0,
                            "null_count": 0,
                            "skip_dedup": "n",
                            "key_candidate_columns": [cand],
                            "key_evidence_type": "PROFILED_UNIQUE",
                            "key_candidate_nullable": False,
                            "key_candidate_compatible": True,
                            "key_evidence_fingerprint": fingerprint,
                        }
                    if dup_cnt < lowest_dup_count and null_cnt == 0:
                        lowest_dup_count = dup_cnt
                        lowest_dup_col = cand
                        lowest_total_rows = total_rows
                        lowest_null_count = null_cnt
                except Exception:
                    pass

            # Step 4: Test Composite Candidates.  A nearly unique numeric
            # cursor is never accepted as an identity on its own; profile it
            # with likely temporal discriminators before falling back.
            pair_candidates = []
            if lowest_dup_col:
                temporal_partners = [
                    column for column in columns
                    if column != lowest_dup_col and (
                        column.lower().strip() in UPDATE_HINTS + INSERT_HINTS
                        or any(token in column.lower() for token in ("date", "time", "timestamp"))
                    )
                ]
                fallback_partners = [
                    column for column in candidates[:10]
                    if column != lowest_dup_col and column not in temporal_partners
                ]
                pair_candidates.extend(
                    (lowest_dup_col, partner)
                    for partner in (temporal_partners + fallback_partners)[:12]
                )
            else:
                pair_candidates.extend(
                    (candidates[i], candidates[j])
                    for i in range(min(4, len(candidates)))
                    for j in range(i + 1, min(5, len(candidates)))
                )

            for c1, c2 in pair_candidates:
                q1 = quote_col(c1)
                q2 = quote_col(c2)
                comp_sql = (
                    f"SELECT "
                    f"COUNT(1) AS total_rows, "
                    f"COUNT(CASE WHEN {q1} IS NOT NULL AND {q2} IS NOT NULL THEN 1 END) AS non_null_rows "
                    f"FROM {quote_table_name(table_fqn, db_type)}"
                )
                dist_sql = (
                    f"SELECT COUNT(1) AS dist_cnt FROM ("
                    f"SELECT DISTINCT {q1}, {q2} FROM {quote_table_name(table_fqn, db_type)} "
                    f"WHERE {q1} IS NOT NULL AND {q2} IS NOT NULL"
                    f"){alias_kw}comp"
                )
                try:
                    df1 = apply_jdbc_options(spark_session.read.format("jdbc"), conn_config).option("dbtable", f"({comp_sql}){alias_kw}c1").load()
                    row1 = df1.collect()[0]
                    d1 = _extract_row_dict(row1)
                    total_rows = int(d1.get("total_rows", 0) or 0)
                    non_null_rows = int(d1.get("non_null_rows", 0) or 0)

                    df2 = apply_jdbc_options(spark_session.read.format("jdbc"), conn_config).option("dbtable", f"({dist_sql}){alias_kw}c2").load()
                    row2 = df2.collect()[0]
                    d2 = _extract_row_dict(row2)
                    distinct_rows = int(d2.get("dist_cnt", 0) or d2.get("distinct_rows", 0) or 0)

                    null_cnt = max(0, total_rows - non_null_rows)
                    dup_cnt = max(0, non_null_rows - distinct_rows)

                    if total_rows > 0 and null_cnt == 0 and dup_cnt == 0:
                        fingerprint = compute_deterministic_fingerprint({
                            "source_tag": _safe_tag(conn_config),
                            "table_fqn": table_fqn,
                            "key_columns": [c1, c2],
                            "evidence_type": "PROFILED_COMPOSITE",
                            "total_rows": total_rows,
                            "null_count": 0,
                            "duplicate_count": 0,
                        })
                        return {
                            "columns_unique_key": f"{c1},{c2}",
                            "identity_mode": "EXACT_KEY",
                            "exception_values": [],
                            "exception_row_count": 0,
                            "exception_group_count": 0,
                            "key_source": "PROFILED_COMPOSITE",
                            "key_confidence": 90,
                            "duplicate_count": 0,
                            "null_count": 0,
                            "skip_dedup": "n",
                            "key_candidate_columns": [c1, c2],
                            "key_evidence_type": "PROFILED_COMPOSITE",
                            "key_candidate_nullable": False,
                            "key_candidate_compatible": True,
                            "key_evidence_fingerprint": fingerprint,
                        }
                except Exception:
                    pass

            # Step 5a: If no exact composite identity was proven, inspect a
            # bounded set of duplicate groups.  This is evidence only; the
            # promotion layer decides whether to activate the conditional-key
            # execution policy.  Request one extra group so the bound can be
            # proven exceeded without collecting an unbounded result.
            if (
                lowest_dup_col
                and lowest_dup_count != float("inf")
                and lowest_null_count == 0
                and lowest_total_rows > 0
            ):
                duplicate_query = build_bounded_duplicate_query(
                    table_fqn,
                    lowest_dup_col,
                    db_type,
                    max_exception_values + 1,
                )
                duplicate_df = (
                    apply_jdbc_options(spark_session.read.format("jdbc"), conn_config)
                    .option("dbtable", f"({duplicate_query}){alias_kw}duplicate_groups")
                    .load()
                )
                duplicate_groups = _duplicate_group_values(duplicate_df.collect())
                exception_values = [value for value, _ in duplicate_groups]
                exception_rows = sum(count for _, count in duplicate_groups)
                bounded = (
                    len(duplicate_groups) <= max_exception_values
                    and exception_rows <= max_exception_rows
                    and (exception_rows / float(lowest_total_rows)) <= max_exception_fraction
                )
                if bounded and exception_values:
                    # The bounded group query identifies every duplicate group
                    # only when the configured limit was not exceeded.  Verify
                    # its complement independently so a source mutation or an
                    # incomplete bounded result cannot be mistaken for a safe
                    # conditional identity.
                    complement_query = _build_bounded_exception_complement_query(
                        table_fqn,
                        lowest_dup_col,
                        db_type,
                        exception_values,
                    )
                    complement_df = (
                        apply_jdbc_options(spark_session.read.format("jdbc"), conn_config)
                        .option("dbtable", f"({complement_query}){alias_kw}complement")
                        .load()
                    )
                    complement_row = _extract_row_dict(complement_df.collect()[0])
                    non_exception_rows = int(
                        complement_row.get("non_exception_rows", 0) or 0
                    )
                    non_exception_distinct = int(
                        complement_row.get("non_exception_distinct", 0) or 0
                    )
                    if (
                        non_exception_rows <= 0
                        or non_exception_rows != non_exception_distinct
                    ):
                        bounded = False

                if bounded and exception_values:
                    from src.contracts.conditional_key import Int64ExceptionCodec

                    exception_values_payload = Int64ExceptionCodec.encode(exception_values)
                    exception_values_fingerprint = Int64ExceptionCodec.fingerprint(
                        exception_values_payload
                    )
                    evidence = {
                        "source_tag": _safe_tag(conn_config),
                        "table_fqn": table_fqn,
                        "key_columns": [lowest_dup_col],
                        "evidence_type": "PROFILED_CONDITIONAL_KEY",
                        "exception_values": exception_values,
                        "exception_values_payload": exception_values_payload,
                        "exception_values_fingerprint": exception_values_fingerprint,
                        "exception_evidence_fingerprint": exception_values_fingerprint,
                        "exception_occurrences": [count for _, count in duplicate_groups],
                        "total_rows": lowest_total_rows,
                        "exception_row_count": exception_rows,
                        "duplicate_count": int(lowest_dup_count),
                        "thresholds": {
                            "max_exception_values": max_exception_values,
                            "max_exception_rows": max_exception_rows,
                            "max_exception_fraction": max_exception_fraction,
                        },
                    }
                    fingerprint = compute_deterministic_fingerprint(evidence)
                    return {
                        "columns_unique_key": lowest_dup_col,
                        "identity_mode": "BOUNDED_EXCEPTION_KEY",
                        "exception_values": exception_values,
                        "exception_key_values": exception_values,
                        "exception_values_payload": exception_values_payload,
                        "exception_values_fingerprint": exception_values_fingerprint,
                        "exception_evidence_fingerprint": exception_values_fingerprint,
                        "exception_occurrences": [count for _, count in duplicate_groups],
                        "exception_row_count": exception_rows,
                        "exception_group_count": len(exception_values),
                        "key_source": "PROFILED_CONDITIONAL",
                        "key_confidence": 95,
                        "duplicate_count": int(lowest_dup_count),
                        "null_count": 0,
                        "skip_dedup": "n",
                        "key_candidate_columns": [lowest_dup_col],
                        "key_evidence_type": "PROFILED_CONDITIONAL_KEY",
                        "key_candidate_nullable": False,
                        "key_candidate_compatible": True,
                        "key_evidence": evidence,
                        "key_evidence_fingerprint": fingerprint,
                    }
        except Exception:
            pass

    # ── Step 5: Lowest-Duplicate Candidate or Heuristic Fallback ──────────────
    if lowest_dup_col and lowest_dup_count != float("inf"):
        return {
            "columns_unique_key": lowest_dup_col,
            "identity_mode": "KEYLESS",
            "exception_values": [],
            "exception_row_count": 0,
            "exception_group_count": 0,
            "key_source": "PROFILED_LOWEST_DUP",
            "key_confidence": 80,
            "duplicate_count": int(lowest_dup_count),
            "null_count": 0,
            "skip_dedup": "n",
            "key_candidate_columns": [lowest_dup_col],
            "key_evidence_type": "PROFILED_LOWEST_DUP",
            "key_candidate_nullable": True,  # Diagnostic-only
            "key_candidate_compatible": False,
            "key_evidence_fingerprint": "",
        }

    # Naming patterns are not source-identity evidence.  Keep the catalog
    # unkeyed until a physical constraint or a completed profile proves it.
    return {
        "columns_unique_key": None,
        "identity_mode": "KEYLESS",
        "exception_values": [],
        "exception_row_count": 0,
        "exception_group_count": 0,
        "key_source": "NONE",
        "key_confidence": 0,
        "duplicate_count": 0,
        "null_count": 0,
        "skip_dedup": "y",
        "key_candidate_columns": [],
        "key_evidence_type": "NONE",
        "key_candidate_nullable": True,
        "key_candidate_compatible": False,
        "key_evidence_fingerprint": "",
    }
