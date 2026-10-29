# src/validation/health_checker.py
"""
Modular Health Checker — End-to-end infrastructure & metadata diagnostic checks.
================================================================================
Provides independent, modular health check methods that can be invoked individually
or composed into an operator-friendly terminal dashboard (--action healthcheck).
"""
import logging
from typing import List, Optional, Tuple
from src.connections.connection_manager import ConnectionManager
from src.validation.metadata_validator import MetadataValidator
from src.utils.db_utils import test_jdbc_connection
from src.constants import MetadataConstants

log = logging.getLogger("HealthChecker")


class HealthChecker:
    """Modular health diagnostic checks for the ETL pipeline."""

    def __init__(self, conn_manager: ConnectionManager, spark=None):
        self.conn_manager = conn_manager
        self.spark = spark

    def check_repository_db(self) -> Tuple[bool, str]:
        """Ping test the metadata repository database connection."""
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    row = cur.fetchone()
            return (row and row[0] == 1), "OK"
        except Exception as e:
            return False, str(e)

    def check_schema_version(self, required_version: Optional[str] = None) -> Tuple[bool, str]:
        """Check if metadata.schema_version matches the configured required version."""
        if not required_version:
            required_version = self.conn_manager.pipeline_config.get(
                "required_schema_version", "1.0.0"
            )
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version FROM metadata.schema_version ORDER BY applied_at DESC LIMIT 1")
                    row = cur.fetchone()
            if not row:
                return False, "No version found in metadata.schema_version"
            current_ver = str(row[0])
            if current_ver == str(required_version):
                return True, current_ver
            return False, f"Mismatch: found {current_ver}, expected {required_version}"
        except Exception as e:
            return False, str(e)

    def check_source_connections(
        self, source_tag: Optional[str] = None
    ) -> Tuple[int, int, List[str]]:
        """Test configured source connections. Returns (succeeded, total, error_list)."""
        tags = [source_tag] if source_tag else self.conn_manager.available_connection_tags(role="source")
        if not tags:
            return 0, 0, []
        succeeded = 0
        errors = []
        for tag in tags:
            try:
                cfg = self.conn_manager.get(tag)
                ok = test_jdbc_connection(cfg, self.spark)
                if ok:
                    succeeded += 1
                else:
                    errors.append(f"{tag}: connection test failed (SELECT 1 returned no result)")
            except Exception as e:
                errors.append(f"{tag}: {e}")
        return succeeded, len(tags), errors

    def check_target_connections(self) -> Tuple[int, int, List[str]]:
        """Test configured target connections. Returns (succeeded, total, error_list)."""
        tags = self.conn_manager.available_connection_tags(role="target")
        if not tags:
            return 0, 0, []
        succeeded = 0
        errors = []
        for tag in tags:
            try:
                cfg = self.conn_manager.get(tag)
                ok = test_jdbc_connection(cfg, self.spark)
                if ok:
                    succeeded += 1
                else:
                    errors.append(f"{tag}: connection test failed (SELECT 1 returned no result)")
            except Exception as e:
                errors.append(f"{tag}: {e}")
        return succeeded, len(tags), errors

    def check_metadata_validation(
        self, source_tag: Optional[str] = None
    ) -> Tuple[bool, int, int, int]:
        """
        Run fast metadata validation rules.
        Returns (is_valid, dup_tables_count, cdc_errors_count, datatype_errors_count).
        """
        validator = MetadataValidator(self.conn_manager)
        is_valid, errors, _ = validator.validate(source_tag=source_tag, quiet=True)
        dup_count = sum(1 for e in errors if "Duplicate active table" in e)
        cdc_count = sum(1 for e in errors if "CDC enabled on table" in e)
        dt_count = sum(1 for e in errors if "Malformed or empty column_type" in e)
        return is_valid, dup_count, cdc_count, dt_count

    def check_spark_connect(self) -> Tuple[bool, str]:
        """Check if SparkSession is reachable."""
        if not self.spark:
            return True, "SKIPPED (no spark instance provided)"
        try:
            res = self.spark.sql("SELECT 1").collect()
            return (res and res[0][0] == 1), "OK"
        except Exception as e:
            return False, str(e)

    def run_all_checks(self, source_tag: Optional[str] = None, quiet: bool = False) -> bool:
        """Execute all modular health checks and display formatted terminal report."""
        rep_ok, rep_msg = self.check_repository_db()
        ver_ok, ver_msg = self.check_schema_version()
        src_ok_cnt, src_tot, _ = self.check_source_connections(source_tag)
        tgt_ok_cnt, tgt_tot, _ = self.check_target_connections()
        meta_ok, dup_cnt, cdc_cnt, dt_cnt = self.check_metadata_validation(source_tag)
        spk_ok, _ = self.check_spark_connect()

        all_ok = rep_ok and ver_ok and meta_ok and spk_ok and (src_ok_cnt == src_tot) and (tgt_ok_cnt == tgt_tot)

        if not quiet:
            sep = "=" * 36
            print()
            print(sep)
            print("          Pipeline Health           ")
            print(sep)
            print(f"Repository DB        [ {'OK' if rep_ok else 'FAIL'} ]")
            print(f"Schema Version       [ {'OK' if ver_ok else 'FAIL'} ] ({ver_msg})")
            print(f"Source Connections   {src_ok_cnt}/{src_tot}")
            print(f"Target Connections   {tgt_ok_cnt}/{tgt_tot}")
            print(f"Metadata Validation  [ {'OK' if meta_ok else 'FAIL'} ]")
            print(f"  Duplicate Tables   {dup_cnt}")
            print(f"  CDC Errors         {cdc_cnt}")
            print(f"  Datatype Errors    {dt_cnt}")
            print(f"Spark Connect        [ {'OK' if spk_ok else 'FAIL'} ]")
            print(sep)
            print()

        return all_ok
