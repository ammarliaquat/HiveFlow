from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from src.constants import RunStatus

class Reconciler:

    @staticmethod
    def count_and_cache(df: DataFrame) -> int:
        """
        Cache the DataFrame and return its count.
        Use this BEFORE writing to avoid re-scanning the source.
        """
        df.cache()
        return df.count()

    @staticmethod
    def count_parquet(spark: SparkSession, path: str, run_id: str = None) -> int:
        """Read back Parquet files from disk and count rows (optionally filtered by run_id)."""
        try:
            df = spark.read.parquet(path)
            if run_id and "pipeline_run_id" in df.columns:
                df = df.filter(df.pipeline_run_id == run_id)
            return df.count()
        except Exception as e:
            print(f"[DEBUG] count_parquet exception for {path}: {e}")
            msg = str(e).lower()
            if "unable_to_infer_schema" in msg or "path does not exist" in msg or "empty" in msg:
                return 0
            raise

    @staticmethod
    def count_delta(spark: SparkSession, path: str, run_id: str = None) -> int:
        """Read back Delta table and count rows (optionally filtered by run_id)."""
        try:
            df = spark.read.format("delta").load(path)
            if run_id and "pipeline_run_id" in df.columns:
                df = df.filter(df.pipeline_run_id == run_id)
            return df.count()
        except Exception as e:
            print(f"[DEBUG] count_delta exception for {path}: {e}")
            msg = str(e).lower()
            if "unable_to_infer_schema" in msg or "path does not exist" in msg or "empty" in msg:
                return 0
            raise

    @staticmethod
    def reconcile(
        source_count: int,
        target_count: int,
        layer: str,           # "bronze", "silver", "gold"
        meta: object,         # MetadataRow
        expected_loss: int = 0,
    ) -> str:
        """
        Validate row counts against the configured threshold.
        expected_loss: certified expected reduction (e.g. duplicates removed during Silver dedup).
        Returns one of:
            - RunStatus.SUCCESS
            - RunStatus.RECONCILIATION_WARNING
            - RunStatus.RECONCILIATION_FAILED (raises RuntimeError if action='FAIL')
        """
        expected_target_count = max(0, source_count - expected_loss)

        # Unconditional overflow guard: target > expected_target_count is never valid.
        # This means duplicates were written, a merge key collision occurred, or
        # a runaway append happened. Applies regardless of is_reconcile flag.
        if expected_target_count > 0 and target_count > expected_target_count:
            msg = (
                f"Row count overflow for {meta.source_table} ({layer}): "
                f"target={target_count} > source={expected_target_count}. "
                f"Possible duplicate write or merge key collision."
            )
            print(f"[RECONCILIATION FAILED] {msg}")
            raise RuntimeError(f"Reconciliation failed for {meta.source_table} ({layer}): target > source")

        # Skip further reconciliation if not enabled for this table
        if getattr(meta, "is_reconcile", "n") != 'y':
            return RunStatus.SUCCESS

        # Calculate discrepancy percentage against expected target count
        if expected_target_count == 0:
            discrepancy = 0.0 if target_count == 0 else 100.0
        else:
            discrepancy = abs(target_count - expected_target_count) / expected_target_count * 100

        threshold = float(getattr(meta, "reconcile_threshold", 0.0) or 0.0)

        # Within threshold → SUCCESS
        if discrepancy <= threshold:
            return RunStatus.SUCCESS

        # Exceeded threshold → determine action
        msg = (
            f"Reconciliation {layer} mismatch for {meta.source_table}: "
            f"source={expected_target_count}, target={target_count}, "
            f"disc={discrepancy:.2f}% > threshold={threshold:.2f}%"
        )

        action = (getattr(meta, "reconcile_action", "FAIL") or 'FAIL').upper()

        if action == 'WARN':
            print(f"[WARN] {msg}")
            return RunStatus.RECONCILIATION_WARNING
        else:
            # 'FAIL' → raise exception, table will be marked FAILED
            print(f"[RECONCILIATION FAILED] {msg}")
            raise RuntimeError(f"Reconciliation failed for {meta.source_table} ({layer})")

    @staticmethod
    def certify_execution(
        input_count: int,
        output_count: int,
        actual_loss: int,
        expected_loss: int,
        layer: str,
        table_name: str,
        distinct_natural_key_count: Optional[int] = None,
    ) -> None:
        """
        Execution Certification rule:
        Verifies actual_loss against expected_loss across Medallion transitions.
        Also asserts that if actual_loss > 0, distinct_natural_key_count != input_count.
        Fails hard with RuntimeError if actual_loss != expected_loss.
        """
        if actual_loss != expected_loss:
            msg = (
                f"[Execution Certification FAILED] Table '{table_name}' ({layer}): "
                f"actual row loss={actual_loss} does not match expected loss={expected_loss} "
                f"(input={input_count}, output={output_count}). Aborting pipeline execution immediately."
            )
            print(f"CRITICAL ERROR: {msg}")
            raise RuntimeError(msg)

        if actual_loss > 0 and distinct_natural_key_count is not None and distinct_natural_key_count == input_count:
            msg = (
                f"[Execution Certification FAILED] Table '{table_name}' ({layer}): "
                f"deduplicate() removed {actual_loss} rows (input={input_count}, output={output_count}), "
                f"BUT natural key distinct count equals total input count ({distinct_natural_key_count}). "
                f"Deduplication destroyed unique records! Aborting pipeline execution immediately."
            )
            print(f"CRITICAL ERROR: {msg}")
            raise RuntimeError(msg)

    @staticmethod
    def certify_medallion_transition(
        input_count: int,
        output_count: int,
        duplicates_removed: int,
        distinct_natural_key_count: int,
        layer: str,
        table_name: str,
    ) -> None:
        """Legacy alias pointing to certify_execution."""
        expected_loss = 0 if distinct_natural_key_count == input_count else duplicates_removed
        Reconciler.certify_execution(
            input_count=input_count,
            output_count=output_count,
            actual_loss=duplicates_removed,
            expected_loss=expected_loss,
            layer=layer,
            table_name=table_name,
            distinct_natural_key_count=distinct_natural_key_count,
        )

    @staticmethod
    def print_dq_summary_report(
        table_name: str,
        oracle_rows: Optional[int],
        bronze_rows: Optional[int],
        silver_rows: Optional[int],
        gold_rows: Optional[int],
        natural_key: str,
        duplicate_keys: int = 0,
        null_keys: int = 0,
        duplicates_removed: int = 0,
        certification_status: str = "PASS",
    ) -> None:
        """Format and print operational Data Quality & Certification Summary Report."""
        sep = "=" * 50
        print("\n" + sep)
        print(f"Table : {table_name.upper()}")
        print(sep)
        print(f"Oracle Rows          : {oracle_rows:,}" if oracle_rows is not None else "Oracle Rows          : N/A")
        print(f"Bronze Rows          : {bronze_rows:,}" if bronze_rows is not None else "Bronze Rows          : N/A")
        print(f"Silver Rows          : {silver_rows:,}" if silver_rows is not None else "Silver Rows          : N/A")
        print(f"Gold Rows            : {gold_rows:,}" if gold_rows is not None else "Gold Rows            : N/A")
        print(f"Natural Key          : {natural_key}")
        print(f"Duplicate Keys       : {duplicate_keys:,}")
        print(f"Null Keys            : {null_keys:,}")
        print(f"Duplicates Removed   : {duplicates_removed:,}")
        print(f"Certification        : {certification_status}")
        print(sep + "\n")
