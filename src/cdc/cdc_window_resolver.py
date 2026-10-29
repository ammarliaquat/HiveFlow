# src/cdc/cdc_window_resolver.py
"""
CDC-01 — Bounded Source-Cursor Change Extraction & Window Resolver.

Governs:
- Explicit promoted CDC policy validation (SEQUENCE_HIGH_WATERMARK, APPEND_ONLY, PREFIX_FINAL).
- Non-mutating committed lower boundary loading (PK: table_id, policy_version_id).
- Source-side upper boundary capture (H = source MAX(cursor)).
- Immutable attempt-scoped window persistence (metadata.cdc_attempt_window).
- Dialect-safe extraction predicate generation.
- Stage-0 freeze: zero table_cdc_state mutations; zero Gold keyed writes.
"""

from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from decimal import Decimal, InvalidOperation
import logging
import json
import uuid

from pyspark.sql import SparkSession

from src.connections.connection_manager import ConnectionManager
from src.metadata.key_contract import ResolvedTableContract
from src.utils.db_utils import (
    quote_table_name,
    quote_column_name,
    get_table_alias_keyword,
    apply_jdbc_options,
)
from src.metadata.policy_models import (
    CDCStrategy,
    SourceMutationMode,
    CursorFinalityMode,
    CursorLogicalType,
    ResolvedCDCPolicy,
    CDCCursor,
    ResolvedCDCWindow,
    compute_deterministic_fingerprint,
)
from src.exceptions import (
    CDCPolicyNotPromotedError,
    CDCStrategyNotImplementedError,
    CDCMutationModeUnsupportedError,
    CDCCursorFinalityUncertifiedError,
    CDCCursorTypeInvalidError,
    CDCSourceCursorRegressionError,
    CDCUpperBoundCaptureFailedError,
    CDCWindowInvalidError,
)

logger = logging.getLogger(__name__)


class CDCWindowResolver:
    """
    Resolver for capturing, freezing, and building bounded extraction windows
    from certified source change streams.
    """

    def __init__(self, conn_manager: ConnectionManager, spark: Optional[SparkSession] = None):
        self._conn_manager = conn_manager
        self._spark = spark

    def resolve_and_persist_attempt_window(
        self,
        run_id: str,
        table_id: int,
        attempt_number: int,
        contract: ResolvedTableContract,
        source_tag: Optional[str] = None,
        source_schema: Optional[str] = None,
        source_table: Optional[str] = None,
        overlap_days: int = 0,
    ) -> ResolvedCDCWindow:
        """
        Resolves the bounded extraction window (L, H] for an attempt and durably persists it.
        If an attempt window is already persisted for this (run_id, table_id, attempt_number),
        it returns the frozen window without re-capturing a moving source upper boundary.
        """
        # 1. Validate promoted CDC policy presence
        cdc_policy = contract.cdc_policy
        if not cdc_policy or not isinstance(cdc_policy, ResolvedCDCPolicy):
            raise CDCPolicyNotPromotedError(
                f"Table ID {table_id} (version {contract.version_number}) has no promoted CDC policy. "
                "CDC extraction requires explicit promoted CDC governance."
            )

        # 2. Validate strategy and mutation semantics
        if cdc_policy.strategy == CDCStrategy.LOG_BASED:
            raise CDCStrategyNotImplementedError(
                "LOG_BASED CDC strategy is not implemented."
            )
        elif cdc_policy.strategy not in (
            CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        ):
            raise CDCStrategyNotImplementedError(
                f"CDC strategy '{cdc_policy.strategy.value}' is not implemented in CDC-01."
            )

        if cdc_policy.strategy == CDCStrategy.SEQUENCE_HIGH_WATERMARK:
            if cdc_policy.mutation_mode != SourceMutationMode.APPEND_ONLY:
                raise CDCMutationModeUnsupportedError(
                    f"Sequence high-watermark CDC requires APPEND_ONLY source mutation mode, got '{cdc_policy.mutation_mode.value}'."
                )
            if cdc_policy.cursor_finality_mode != CursorFinalityMode.PREFIX_FINAL:
                raise CDCCursorFinalityUncertifiedError(
                    f"Sequence high-watermark CDC requires certified PREFIX_FINAL cursor finality, got '{cdc_policy.mutation_mode.value}'."
                )
            if (
                len(cdc_policy.cursor_types) != 1
                or cdc_policy.cursor_types[0] not in (CursorLogicalType.INT64, CursorLogicalType.DECIMAL)
            ):
                raise CDCCursorTypeInvalidError(
                    "Sequence high-watermark CDC requires exactly one INT64 or DECIMAL cursor type, "
                    f"got '{cdc_policy.cursor_types}'."
                )
        else:
            if cdc_policy.mutation_mode != SourceMutationMode.MUTABLE:
                raise CDCMutationModeUnsupportedError(
                    "Timestamp high-watermark CDC requires MUTABLE source mutation mode."
                )
            if cdc_policy.cursor_finality_mode != CursorFinalityMode.UNSAFE_OR_UNKNOWN:
                raise CDCCursorFinalityUncertifiedError(
                    "Timestamp high-watermark CDC requires overlap-reconciled cursor finality."
                )
            if (
                len(cdc_policy.cursor_columns) != 2
                or len(cdc_policy.cursor_types) != 2
                or cdc_policy.cursor_types[0] != CursorLogicalType.TIMESTAMP
                or tuple(cdc_policy.tie_breaker_columns) != (cdc_policy.cursor_columns[1],)
            ):
                raise CDCCursorTypeInvalidError(
                    "Timestamp high-watermark CDC requires (timestamp, business-key) cursor columns and types."
                )
            if int(overlap_days or 0) <= 0:
                raise CDCWindowInvalidError(
                    "Timestamp high-watermark CDC requires a positive frozen cdc_p_days overlap."
                )

        # 3. Check if attempt window was already durably captured (Idempotency & Frozen Snapshot)
        existing_window = self._get_persisted_attempt_window(run_id, table_id, attempt_number)
        if existing_window:
            logger.info(
                "Loaded existing frozen CDC window %s for run %s, table %s, attempt %s",
                existing_window.window_id, run_id, table_id, attempt_number
            )
            return existing_window

        # 4. Resolve source metadata if not passed
        if not source_tag or not source_table:
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT source_tag, source_table, source_schema
                        FROM metadata.db_tables_etl
                        WHERE table_id = %s
                    """, [table_id])
                    srow = cur.fetchone()
                    if srow:
                        source_tag = source_tag or srow[0]
                        source_table = source_table or srow[1]
                        if len(srow) > 2 and srow[2]:
                            source_schema = source_schema or srow[2]

        # 5. Load committed lower boundary L from metadata.table_cdc_state (PK: table_id, policy_version_id)
        lower_cursor = self._load_committed_cursor(table_id, contract.policy_version_id)

        # 6. Capture source upper boundary H directly from the source system
        upper_cursor = self._capture_source_upper_boundary(
            source_tag=source_tag or "mysql",
            source_table=source_table or "",
            cdc_policy=cdc_policy,
            source_schema=source_schema,
        )

        # 7. Check for source cursor regression (H < L)
        is_empty = False
        if lower_cursor is not None:
            if self._cursor_values_for_comparison(upper_cursor) < self._cursor_values_for_comparison(lower_cursor):
                raise CDCSourceCursorRegressionError(
                    f"Source cursor regression detected for table_id {table_id}: source upper boundary "
                    f"({upper_cursor.values}) is less than committed lower boundary ({lower_cursor.values})."
                )
            elif (
                cdc_policy.strategy == CDCStrategy.SEQUENCE_HIGH_WATERMARK
                and upper_cursor.values == lower_cursor.values
            ):
                is_empty = True

        # 8. Compute deterministic window fingerprint
        window_payload = {
            "table_id": table_id,
            "policy_version_id": contract.policy_version_id,
            "cdc_policy_fingerprint": cdc_policy.cdc_policy_fingerprint,
            "lower_cursor": lower_cursor.to_dict() if lower_cursor else None,
            "upper_cursor": upper_cursor.to_dict(),
            "overlap_days": int(overlap_days or 0),
        }
        window_fingerprint = compute_deterministic_fingerprint(window_payload)
        window_id = str(uuid.uuid4())
        captured_at = datetime.utcnow()

        # 9. Durably persist attempt window to metadata.cdc_attempt_window
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metadata.cdc_attempt_window (
                        window_id, run_id, table_id, attempt_number,
                        policy_version_id, cdc_policy_fingerprint, window_fingerprint,
                        lower_cursor, upper_cursor, captured_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s
                    )
                    ON CONFLICT (run_id, table_id, attempt_number) DO UPDATE
                    SET window_fingerprint = EXCLUDED.window_fingerprint
                    RETURNING window_id, captured_at
                """, [
                    window_id,
                    run_id,
                    table_id,
                    attempt_number,
                    contract.policy_version_id,
                    cdc_policy.cdc_policy_fingerprint,
                    window_fingerprint,
                    json.dumps(lower_cursor.to_dict()) if lower_cursor else None,
                    json.dumps(upper_cursor.to_dict()),
                    captured_at,
                ])
                row = cur.fetchone()
                if row:
                    window_id = str(row[0])
                    captured_at = row[1]
            conn.commit()

        return ResolvedCDCWindow(
            window_id=window_id,
            run_id=run_id,
            table_id=table_id,
            attempt_number=attempt_number,
            policy_version_id=contract.policy_version_id,
            cdc_policy_fingerprint=cdc_policy.cdc_policy_fingerprint,
            strategy=cdc_policy.strategy,
            lower_cursor=lower_cursor,
            upper_cursor=upper_cursor,
            window_fingerprint=window_fingerprint,
            captured_at=captured_at,
            is_empty=is_empty,
        )

    def _load_committed_cursor(self, table_id: int, policy_version_id: str) -> Optional[CDCCursor]:
        """Loads committed cursor for (table_id, policy_version_id) from metadata.table_cdc_state."""
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT committed_cursor
                    FROM metadata.table_cdc_state
                    WHERE table_id = %s AND policy_version_id = %s
                """, [table_id, policy_version_id])
                row = cur.fetchone()
                if row and row[0]:
                    cursor_dict = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                    return CDCCursor.from_dict(cursor_dict)
        return None

    def load_committed_state_version(self, table_id: int, policy_version_id: str) -> Optional[int]:
        """Return the frozen CDC state version used by an attempt, or ``None`` on bootstrap."""
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT state_version
                    FROM metadata.table_cdc_state
                    WHERE table_id = %s AND policy_version_id = %s
                    """ ,
                    [table_id, policy_version_id],
                )
                row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def _get_persisted_attempt_window(
        self, run_id: str, table_id: int, attempt_number: int
    ) -> Optional[ResolvedCDCWindow]:
        """Loads previously persisted attempt window if present."""
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        window_id, run_id, table_id, attempt_number,
                        policy_version_id, cdc_policy_fingerprint, window_fingerprint,
                        lower_cursor, upper_cursor, captured_at
                    FROM metadata.cdc_attempt_window
                    WHERE run_id = %s AND table_id = %s AND attempt_number = %s
                """, [run_id, table_id, attempt_number])
                row = cur.fetchone()
                if row:
                    lower_dict = row[7] if isinstance(row[7], dict) else (json.loads(row[7]) if row[7] else None)
                    upper_dict = row[8] if isinstance(row[8], dict) else json.loads(row[8])

                    lower_cursor = CDCCursor.from_dict(lower_dict) if lower_dict else None
                    upper_cursor = CDCCursor.from_dict(upper_dict)

                    is_empty = False
                    if (
                        upper_cursor.strategy == CDCStrategy.SEQUENCE_HIGH_WATERMARK
                        and lower_cursor
                        and lower_cursor.values == upper_cursor.values
                    ):
                        is_empty = True

                    return ResolvedCDCWindow(
                        window_id=str(row[0]),
                        run_id=str(row[1]),
                        table_id=row[2],
                        attempt_number=row[3],
                        policy_version_id=str(row[4]),
                        cdc_policy_fingerprint=row[5],
                        strategy=upper_cursor.strategy,
                        lower_cursor=lower_cursor,
                        upper_cursor=upper_cursor,
                        window_fingerprint=row[6],
                        captured_at=row[9],
                        is_empty=is_empty,
                    )
        return None

    def _capture_source_upper_boundary(
        self,
        source_tag: str,
        source_table: str,
        cdc_policy: ResolvedCDCPolicy,
        source_schema: Optional[str] = None,
    ) -> CDCCursor:
        """Captures upper boundary H directly from source system without querying target state."""
        cursor_col = cdc_policy.cursor_columns[0]
        
        try:
            conn_cfg = self._conn_manager.get(source_tag, expected_role="source")
            db_type = getattr(conn_cfg, "db_type", "mysql").lower()
        except Exception:
            conn_cfg = None
            db_type = "mysql"

        full_source_table = f"{source_schema}.{source_table}" if source_schema else source_table
        quoted_table = quote_table_name(full_source_table, db_type)
        quoted_col = quote_column_name(cursor_col, db_type)
        
        if cdc_policy.strategy == CDCStrategy.TIMESTAMP_HIGH_WATERMARK:
            tie_col = cdc_policy.cursor_columns[1]
            quoted_tie_col = quote_column_name(tie_col, db_type)
            if db_type == "mysql":
                select_list = f"DATE_FORMAT({quoted_col}, '%Y-%m-%dT%H:%i:%s') AS {quoted_col}, {quoted_tie_col}"
            else:
                select_list = f"{quoted_col}, {quoted_tie_col}"
            ordering = f"ORDER BY {quoted_col} DESC, {quoted_tie_col} DESC"
            predicate = f"WHERE {quoted_col} IS NOT NULL"
            if db_type in ("mssql", "sqlserver"):
                query = f"SELECT TOP 1 {select_list} FROM {quoted_table} {predicate} {ordering}"
            elif db_type in ("oracle", "ora"):
                query = (
                    f"SELECT {select_list} FROM ("
                    f"SELECT {select_list} FROM {quoted_table} {predicate} {ordering}"
                    f") WHERE ROWNUM = 1"
                )
            else:
                # MySQL, MariaDB, PostgreSQL and PostgreSQL-compatible drivers.
                query = f"SELECT {select_list} FROM {quoted_table} {predicate} {ordering} LIMIT 1"
        else:
            query = f"SELECT MAX({quoted_col}) AS max_val FROM {quoted_table}"

        row = None
        try:
            if self._spark is not None and conn_cfg is not None:
                alias_kw = get_table_alias_keyword(db_type)
                dbtable = f"({query}){alias_kw}cdc_bound"
                reader = self._spark.read.format("jdbc")
                reader = apply_jdbc_options(reader, conn_cfg)
                df = reader.option("dbtable", dbtable).load()
                row = df.first()
            else:
                with self._conn_manager.get_db_connection(source_tag, expected_role="source") as conn:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        row = cur.fetchone()
            max_val = row[0] if row and row[0] is not None else 0
        except Exception as e:
            raise CDCUpperBoundCaptureFailedError(
                f"Failed to capture source upper boundary on {source_tag}.{full_source_table}: {e}"
            ) from e

        if cdc_policy.strategy == CDCStrategy.TIMESTAMP_HIGH_WATERMARK:
            if not row or row[0] is None or len(row) < 2 or row[1] is None:
                raise CDCUpperBoundCaptureFailedError(
                    f"Timestamp CDC upper boundary is incomplete for {source_tag}.{full_source_table}."
                )
            if hasattr(row[0], "isoformat"):
                timestamp_value = row[0].isoformat()
            else:
                timestamp_value = str(row[0]).strip().replace(" ", "T")
            values = (timestamp_value, row[1])
        else:
            values = (self._sequence_cursor_value(max_val, cdc_policy.cursor_types[0]),)

        return CDCCursor(
            schema_version=1,
            strategy=cdc_policy.strategy,
            columns=cdc_policy.cursor_columns,
            types=cdc_policy.cursor_types,
            values=values,
        )

    def build_extraction_filter_sql(
        self,
        window: ResolvedCDCWindow,
        dialect: str = "mysql",
        overlap_days: int = 0,
        exception_values: Optional[Tuple[Any, ...]] = None,
    ) -> str:
        """
        Builds dialect-safe SQL extraction filter predicate for the bounded window.
        Returns:
            - "1=0" if empty window.
            - "`col` <= H" for bootstrap runs.
            - "`col` > L AND `col` <= H" for incremental runs.
        """
        # A bounded-exception identity has a small, frozen set of key values
        # that must be reread on every run.  Those rows are intentionally
        # outside the normal high-watermark lane (for example a sentinel key
        # of 0), so include them explicitly even when the cursor window is
        # empty.  The values arrive from the frozen contract, not mutable
        # catalog state.
        if exception_values:
            from src.contracts.conditional_key import Int64ExceptionCodec

            try:
                normalized_exceptions = Int64ExceptionCodec.normalize(exception_values)
            except Exception as exc:
                raise CDCWindowInvalidError(
                    "Bounded-exception extraction values are not valid INT64 values."
                ) from exc
        else:
            normalized_exceptions = ()

        def _exception_predicate(quoted_column: str, cursor_type: CursorLogicalType) -> str:
            if not normalized_exceptions:
                return ""
            literals = ", ".join(
                self._sequence_sql_literal(value, cursor_type)
                for value in normalized_exceptions
            )
            return f"{quoted_column} IN ({literals})"

        if window.is_empty:
            exception_predicate = _exception_predicate(
                quote_column_name(window.upper_cursor.columns[0], dialect),
                window.upper_cursor.types[0],
            )
            return exception_predicate or "1=0"

        cursor_col = window.upper_cursor.columns[0]
        quoted_col = quote_column_name(cursor_col, dialect)
        upper_val = window.upper_cursor.values[0]

        if window.strategy == CDCStrategy.TIMESTAMP_HIGH_WATERMARK:
            if int(overlap_days or 0) <= 0 or len(window.upper_cursor.values) != 2:
                raise CDCWindowInvalidError(
                    "Timestamp CDC requires a positive frozen overlap and a composite upper cursor."
            )
            tie_col = window.upper_cursor.columns[1]
            quoted_tie_col = quote_column_name(tie_col, dialect)
            upper_timestamp = str(upper_val).replace("'", "''")
            upper_tie = self._sql_literal(window.upper_cursor.values[1])
            upper_predicate = (
                f"({quoted_col} < '{upper_timestamp}' OR "
                f"({quoted_col} = '{upper_timestamp}' AND {quoted_tie_col} <= {upper_tie}))"
            )
            if window.lower_cursor is None:
                return upper_predicate

            lower_timestamp = str(window.lower_cursor.values[0]).replace("'", "''")
            return (
                f"{quoted_col} >= {self._timestamp_overlap_start(lower_timestamp, int(overlap_days), dialect)} "
                f"AND {upper_predicate}"
            )

        if window.strategy == CDCStrategy.SEQUENCE_HIGH_WATERMARK:
            upper_literal = self._sequence_sql_literal(upper_val, window.upper_cursor.types[0])
            if window.lower_cursor is None:
                window_predicate = f"{quoted_col} <= {upper_literal}"
            else:
                lower_literal = self._sequence_sql_literal(
                    window.lower_cursor.values[0], window.lower_cursor.types[0]
                )
                window_predicate = f"{quoted_col} > {lower_literal} AND {quoted_col} <= {upper_literal}"
            exception_predicate = _exception_predicate(quoted_col, window.upper_cursor.types[0])
            return (
                f"(({window_predicate}) OR {exception_predicate})"
                if exception_predicate
                else window_predicate
            )

        if window.lower_cursor is None:
            # Bootstrap run
            return f"{quoted_col} <= {upper_val}"
        else:
            # Incremental run
            lower_val = window.lower_cursor.values[0]
            return f"{quoted_col} > {lower_val} AND {quoted_col} <= {upper_val}"

    @staticmethod
    def _sql_literal(value: Any) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _sequence_cursor_value(value: Any, cursor_type: CursorLogicalType) -> Any:
        if cursor_type == CursorLogicalType.INT64:
            return int(value)
        try:
            return format(Decimal(str(value)), "f")
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise CDCCursorTypeInvalidError(
                f"Sequence high-watermark CDC requires an exact numeric upper boundary, got '{value}'."
            ) from exc

    @staticmethod
    def _sequence_sql_literal(value: Any, cursor_type: CursorLogicalType) -> str:
        if cursor_type == CursorLogicalType.INT64:
            return str(int(value))
        try:
            return format(Decimal(str(value)), "f")
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise CDCWindowInvalidError(
                f"Invalid DECIMAL sequence cursor value '{value}'."
            ) from exc

    @staticmethod
    def _cursor_values_for_comparison(cursor: CDCCursor) -> Tuple[Any, ...]:
        return tuple(
            Decimal(str(value)) if cursor_type == CursorLogicalType.DECIMAL else value
            for value, cursor_type in zip(cursor.values, cursor.types)
        )

    @staticmethod
    def _timestamp_overlap_start(timestamp: str, overlap_days: int, dialect: str) -> str:
        normalized_dialect = dialect.lower()
        if normalized_dialect in ("mysql", "mariadb"):
            return f"DATE_SUB('{timestamp}', INTERVAL {overlap_days} DAY)"
        if normalized_dialect in ("postgres", "postgresql"):
            return f"('{timestamp}'::timestamp - INTERVAL '{overlap_days} days')"
        if normalized_dialect in ("mssql", "sqlserver"):
            return f"DATEADD(day, -{overlap_days}, CAST('{timestamp}' AS datetime2))"
        if normalized_dialect == "oracle":
            return f"('{timestamp}' - NUMTODSINTERVAL({overlap_days}, 'DAY'))"
        raise CDCWindowInvalidError(f"Unsupported timestamp CDC dialect '{dialect}'.")
