import os
import re
import hashlib
from typing import List, Tuple
from src.connections.connection_manager import ConnectionManager
from src.exceptions import MetadataSchemaIncompatibleError

CANONICAL_MIGRATIONS: List[str] = [
    "001_master_subscriber_schema.sql",
    "002_reconciliation_idempotency.sql",
    "003_resilience_batch_summary.sql",
    "004_restructure_db_tables_etl.sql",
    "005_alter_last_success_date_to_timestamp.sql",
    "006_metadata_schema_version.sql",
    "007_batch_log_target_tag.sql",
    "008_batch_summary_metrics.sql",
    "009_dynamic_connections.sql",
    "010_notifications_schema.sql",
    "010_system_settings.sql",
    "011_add_source_tag_to_db_columns_etl.sql",
    "011_recommendation_engine.sql",
    "012_add_connection_limits.sql",
    "012_governance_engine.sql",
    "013_add_data_governance.sql",
    "013_governance_separation_v3.sql",
    "014_add_data_quality.sql",
    "015_schema_lifecycle.sql",
    "016_add_rbac.sql",
    "017_data_lineage.sql",
    "018_canonical_types_and_strategies.sql",
    "019_adaptive_partitioning.sql",
    "020_execution_state_model.sql",
    "021_add_stage_metrics_column.sql",
    "021_batch_stage_log_schema.sql",
    "022_benchmark_history_schema.sql",
    "022_live_progress_monitoring.sql",
    "023_metadata_platform_v3.sql",
    "024_create_pipeline_jobs.sql",
    "025_data_profiled_key_discovery.sql",
    "026_cdc_strategy_and_profiling.sql",
    "027_enhanced_cdc_governance_and_health.sql",
    "028_rearrange_metadata_tables_schema.sql",
    "029_source_business_key_governance.sql",
    "030_map01_authoritative_column_mapping.sql",
    "031_schema_certification.sql",
    "032_job01_authoritative_execution_lifecycle.sql",
    "033_meta01_policy_version_governance.sql",
    "034_cdc01_bounded_cursor_state.sql",
    "035_recon01_attempt_reconciliation.sql",
    "036_obs01_atomic_cursor_commit.sql",
    "037_gold01_atomic_publication.sql",
    "038_snapshot01_consistent_extraction.sql",
    "039_execution_authority_mode.sql",
    "040_execution_authority_mode_blocked.sql",
    "041_gold_publication_recovery.sql",
    "042_job_scope_claim_fencing.sql",
    "043_strict_claim_fencing.sql",
    "044_claim_fencing_provenance.sql",
    "045_recovery_and_frozen_policy.sql",
    "046_partial_success_job_lifecycle.sql",
    "047_layer_artifact_manifest.sql",
    "048_multi_target_tier2_subscriptions.sql",
]


class SchemaCertifier:
    """
    SCHEMA-CERT-01 Implementation
    Validates that the metadata database schema matches the exact migration ledger expected by the application.
    Runs prior to ETL processing to enforce fail-closed metadata consistency.
    """

    def __init__(self, conn_manager: ConnectionManager, migrations_dir: str = "migrations"):
        self._conn_manager = conn_manager
        self._migrations_dir = migrations_dir

    def _get_local_migrations(self) -> List[Tuple[str, str, str]]:
        """Returns sorted list of (version, filename, checksum) from the local filesystem."""
        migrations = []
        if not os.path.exists(self._migrations_dir):
            raise MetadataSchemaIncompatibleError(f"Migrations directory not found: {self._migrations_dir}")
            
        for filename in CANONICAL_MIGRATIONS:
            version = filename.split('_')[0]
            filepath = os.path.join(self._migrations_dir, filename)
            if not os.path.exists(filepath):
                raise MetadataSchemaIncompatibleError(f"Canonical migration file missing: {filename}")
            with open(filepath, 'rb') as f:
                content = f.read().replace(b'\r\n', b'\n')
                checksum = hashlib.sha256(content).hexdigest()
            migrations.append((version, filename, checksum))
        
        if not migrations:
            raise MetadataSchemaIncompatibleError("No local migrations found to certify.")
            
        return migrations

    def _get_expected_ledger_migrations(self) -> List[Tuple[str, str, str]]:
        """
        Returns the migration rows expected in metadata.schema_migrations.

        The historical ledger uses version as its primary key. Several legacy
        migration files intentionally share a numeric version, and the canonical
        runner applies those files but records only the first ledger row for that
        version. Certification must therefore compare the immutable ledger rows,
        not every executable migration file one-for-one.
        """
        ledger_migrations = []
        seen_versions = set()
        for version, filename, checksum in self._get_local_migrations():
            if version in seen_versions:
                continue
            ledger_migrations.append((version, filename, checksum))
            seen_versions.add(version)
        return ledger_migrations

    @staticmethod
    def _strip_outer_transaction(sql_text: str) -> str:
        """
        Remove standalone BEGIN/COMMIT statements so the certifier can execute
        the migration and ledger insert inside one caller-owned transaction.
        PL/pgSQL function bodies are left untouched.
        """
        filtered_lines = []
        for line in sql_text.splitlines():
            if re.match(r"^\s*(BEGIN|COMMIT)\s*;\s*$", line, flags=re.IGNORECASE):
                continue
            filtered_lines.append(line)
        return "\n".join(filtered_lines)

    def apply_migration(self, migration_name: str) -> None:
        """
        Apply exactly one canonical migration and record its real checksum.

        Safety rules:
        - the migration must be the next missing canonical migration;
        - every existing ledger row before it must match the local artifact;
        - SQL execution and ledger insertion happen in one transaction;
        - no synthetic/manual checksum is accepted.
        """
        local_migrations = self._get_local_migrations()
        expected_ledger_migrations = self._get_expected_ledger_migrations()
        local_by_name = {name: (version, name, checksum) for version, name, checksum in local_migrations}
        if migration_name not in local_by_name:
            raise MetadataSchemaIncompatibleError(
                f"Migration is not canonical and cannot be applied: {migration_name}"
            )

        target_version, target_name, target_checksum = local_by_name[migration_name]
        conn = None
        original_autocommit = None

        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                original_autocommit = getattr(conn, "autocommit", None)
                if original_autocommit is not None:
                    conn.autocommit = False

                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT version, migration_name, checksum
                        FROM metadata.schema_migrations
                        ORDER BY version ASC
                    """)
                    db_migrations = cur.fetchall()

                    for i, db_row in enumerate(db_migrations):
                        db_ver, db_name, db_checksum = db_row
                        if db_ver == target_version:
                            if db_name == target_name and db_checksum == target_checksum:
                                conn.rollback()
                                return
                            raise MetadataSchemaIncompatibleError(
                                f"Checksum mismatch for already-applied migration {target_version}."
                            )
                        if i >= len(expected_ledger_migrations):
                            raise MetadataSchemaIncompatibleError(
                                f"Database has unexpected extra migration at index {i}: {db_ver}"
                            )
                        local_ver, local_name, local_checksum = expected_ledger_migrations[i]
                        if (db_ver, db_name, db_checksum) != (local_ver, local_name, local_checksum):
                            raise MetadataSchemaIncompatibleError(
                                f"Migration chain mismatch before {target_version}: "
                                f"expected {(local_ver, local_name)}, found {(db_ver, db_name)}"
                            )

                    if len(db_migrations) >= len(expected_ledger_migrations):
                        raise MetadataSchemaIncompatibleError(
                            "Database migration ledger is already at or beyond the local canonical chain."
                        )

                    next_version, next_name, _ = expected_ledger_migrations[len(db_migrations)]
                    if (next_version, next_name) != (target_version, target_name):
                        raise MetadataSchemaIncompatibleError(
                            f"Cannot apply {target_version}; next missing migration is {next_version}."
                        )

                    migration_path = os.path.join(self._migrations_dir, target_name)
                    with open(migration_path, "rb") as f:
                        raw_sql = f.read().replace(b"\r\n", b"\n")
                    sql_text = raw_sql.decode("utf-8")
                    transactional_sql = self._strip_outer_transaction(sql_text)

                    cur.execute(transactional_sql)
                    cur.execute(
                        """
                        INSERT INTO metadata.schema_migrations
                            (version, migration_name, checksum, applied_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        """,
                        (target_version, target_name, target_checksum),
                    )
                conn.commit()
        except MetadataSchemaIncompatibleError:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        except Exception as e:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise MetadataSchemaIncompatibleError(
                f"Failed to apply migration {target_version} ({target_name}): {e}"
            ) from e
        finally:
            if conn is not None:
                try:
                    if original_autocommit is not None:
                        conn.autocommit = original_autocommit
                except Exception:
                    pass

    def certify(self) -> None:
        """
        Validates the DB ledger against local migrations.
        Raises MetadataSchemaIncompatibleError if:
        - Ledger table is missing.
        - Migration chain is broken/missing.
        - Checksums mismatch.
        """
        expected_ledger_migrations = self._get_expected_ledger_migrations()
        expected_final_version = expected_ledger_migrations[-1][0]

        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT version, migration_name, checksum 
                        FROM metadata.schema_migrations 
                        ORDER BY version ASC
                    """)
                    db_migrations = cur.fetchall()
        except Exception as e:
            raise MetadataSchemaIncompatibleError(f"Failed to read schema_migrations ledger: {e}")

        if not db_migrations:
            raise MetadataSchemaIncompatibleError("Database schema_migrations ledger is empty.")

        if len(db_migrations) < len(expected_ledger_migrations):
            missing = expected_ledger_migrations[len(db_migrations)][0]
            raise MetadataSchemaIncompatibleError(f"Database is missing required migration: {missing}")

        for i, (local_ver, local_name, local_checksum) in enumerate(expected_ledger_migrations):
            db_ver, db_name, db_checksum = db_migrations[i]
            
            if local_ver != db_ver:
                raise MetadataSchemaIncompatibleError(f"Migration chain mismatch at index {i}: Expected {local_ver}, found {db_ver}")
                
            if local_checksum != db_checksum:
                raise MetadataSchemaIncompatibleError(
                    f"Checksum mismatch for migration {local_ver} ({local_name}). "
                    f"Local: {local_checksum}, DB: {db_checksum}. "
                    "The database schema has been mutated or the migration file was altered after application."
                )

        print(f"Schema Certified: Validated {len(expected_ledger_migrations)} migrations up through {expected_final_version}.")
