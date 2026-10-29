# src/validation/schema_drift_detector.py
import json
import uuid
import logging
from typing import Dict, List, Any
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType,
    StringType, DateType, TimestampType,
    BooleanType, BinaryType
)

from src.connections.connection_manager import ConnectionManager
from src.metadata.metadata_models import MetadataRow
from src.validation.schema_classifier import SchemaClassifier
from src.services.notification_service import NotificationService
from src.utils.db_utils import apply_jdbc_options

logger = logging.getLogger("SchemaDriftDetector")

class SchemaDriftException(Exception):
    pass

class SchemaDriftDetector:
    def __init__(self, spark: SparkSession, conn_manager: ConnectionManager, run_id: str):
        self.spark = spark
        self.conn_manager = conn_manager
        self.run_id = run_id

    def detect(self, meta: MetadataRow):
        """
        Detects schema drift by comparing expected metadata against a fast JDBC infer.
        Raises SchemaDriftException if a blocking change is detected.
        """
        expected_schema = self._get_expected_schema(meta.id)
        if not expected_schema:
            return  # No metadata to compare against

        actual_schema = self._infer_actual_schema(meta)
        if not actual_schema:
            return

        policy = self._get_promotion_policy(meta.id)
        
        drifts = self._compare_schemas(expected_schema, actual_schema)
        if not drifts:
            return

        deployment_id = f"deploy_{datetime.now().strftime('%Y%m%d_%H')}_{meta.source_tag}"
        
        # Log all drifts
        critical_count = 0
        warning_count = 0
        
        with self.conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                for drift in drifts:
                    severity = SchemaClassifier.classify_drift(
                        drift["drift_type"], drift["old_canonical_type"], drift["new_canonical_type"]
                    )
                    
                    status = "PENDING"
                    if policy == "AUTO" and severity != "CRITICAL":
                        status = "AUTO_PROMOTED"
                        # Note: Auto-promotion logic (updating db_columns_etl) would go here
                        
                    if severity == "CRITICAL":
                        critical_count += 1
                    elif severity == "WARNING":
                        warning_count += 1
                        
                    impact_summary = self._generate_impact_summary(conn, meta.id, drift["column_name"])
                    
                    query = """
                        INSERT INTO metadata.schema_drift_alerts (
                            run_id, table_id, deployment_correlation_id, column_name, 
                            drift_type, old_canonical_type, new_canonical_type, 
                            severity, status, impact_summary
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cur.execute(query, (
                        self.run_id, meta.id, deployment_id, drift["column_name"],
                        drift["drift_type"], drift["old_canonical_type"], drift["new_canonical_type"],
                        severity, status, json.dumps(impact_summary)
                    ))
            conn.commit()

        # Grouped Notification
        if drifts:
            total = len(drifts)
            sev = "CRITICAL" if critical_count > 0 else "WARNING"
            msg = f"Detected {total} schema changes ({critical_count} Critical, {warning_count} Warning) for table {meta.source_table}."
            
            NotificationService.create_or_update_notification(
                notification_type="SCHEMA_DRIFT",
                event_code="SCHEMA_DRIFT_DETECTED",
                created_by_component="DRIFT_DETECTOR",
                severity=sev,
                title=f"Schema Drift: {meta.source_table}",
                message=msg,
                correlation_key=f"schema_drift_deploy:{deployment_id}",
                run_id=self.run_id,
                source_tag=meta.source_tag,
                source_table=meta.source_table
            )

        # Enforce Policy
        if policy == "BLOCK" or (policy == "MANUAL" and critical_count > 0):
            logger.error(f"Schema drift blocked pipeline execution for {meta.source_table}")
            raise SchemaDriftException(f"Schema drift detected. Found {critical_count} critical changes.")


    def _get_expected_schema(self, table_id: int) -> Dict[str, str]:
        schema = {}
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    # Only active columns
                    cur.execute("""
                        SELECT COALESCE(target_column, source_column) AS col_name, 
                               COALESCE(canonical_type, source_column_datatype) AS data_type 
                        FROM metadata.db_columns_etl 
                        WHERE table_id = %s AND retired_at IS NULL
                    """, (table_id,))
                    for row in cur.fetchall():
                        canonical = SchemaClassifier.map_to_canonical("generic", row[1] or "STRING")
                        schema[row[0].lower()] = canonical
        except Exception as e:
            logger.error(f"Failed to load expected schema: {e}")
        return schema

    def _infer_actual_schema(self, meta: MetadataRow) -> Dict[str, str]:
        schema = {}
        try:
            conn_cfg = self.conn_manager.get(meta.source_tag, expected_role="source")
            from src.utils.db_utils import quote_table_name
            alias_kw = " " if conn_cfg.db_type == "oracle" else " AS "
            dbtable = f"(SELECT * FROM {quote_table_name(meta.full_source_table, conn_cfg.db_type)} WHERE 1=0){alias_kw}schema_infer"
            
            infer_df = (
                apply_jdbc_options(self.spark.read.format("jdbc"), conn_cfg)
                .option("dbtable", dbtable)
                .load()
            )
            
            for field in infer_df.schema.fields:
                canonical = self._pyspark_to_canonical(field.dataType)
                schema[field.name.lower()] = canonical
        except Exception as e:
            logger.error(f"Failed to infer actual schema: {e}")
        return schema

    def _pyspark_to_canonical(self, dtype) -> str:
        if isinstance(dtype, (IntegerType, ShortType, ByteType)): return "INT"
        if isinstance(dtype, LongType): return "BIGINT"
        if isinstance(dtype, (FloatType, DoubleType)): return "FLOAT"
        if isinstance(dtype, DecimalType): return "DECIMAL"
        if isinstance(dtype, DateType): return "DATE"
        if isinstance(dtype, TimestampType): return "TIMESTAMP"
        if isinstance(dtype, BooleanType): return "BOOLEAN"
        if isinstance(dtype, BinaryType): return "BINARY"
        return "STRING"

    def _get_promotion_policy(self, table_id: int) -> str:
        policy = "MANUAL"
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT policy FROM metadata.promotion_policies WHERE table_id = %s", (table_id,))
                    row = cur.fetchone()
                    if row:
                        policy = row[0]
        except: pass
        return policy

    def _compare_schemas(self, expected: Dict[str, str], actual: Dict[str, str]) -> List[Dict]:
        drifts = []
        for col, expected_type in expected.items():
            if col not in actual:
                drifts.append({
                    "column_name": col,
                    "drift_type": "DROPPED_COLUMN",
                    "old_canonical_type": expected_type,
                    "new_canonical_type": None
                })
            elif expected_type != actual[col]:
                drifts.append({
                    "column_name": col,
                    "drift_type": "TYPE_CHANGED",
                    "old_canonical_type": expected_type,
                    "new_canonical_type": actual[col]
                })

        for col, actual_type in actual.items():
            if col not in expected:
                drifts.append({
                    "column_name": col,
                    "drift_type": "NEW_COLUMN",
                    "old_canonical_type": None,
                    "new_canonical_type": actual_type
                })
                
        # Basic Rename Detection heuristic
        dropped = [d for d in drifts if d["drift_type"] == "DROPPED_COLUMN"]
        added = [d for d in drifts if d["drift_type"] == "NEW_COLUMN"]
        
        if len(dropped) == 1 and len(added) == 1:
            if dropped[0]["old_canonical_type"] == added[0]["new_canonical_type"]:
                drifts.remove(dropped[0])
                drifts.remove(added[0])
                drifts.append({
                    "column_name": added[0]["column_name"],
                    "drift_type": "RENAME_DETECTED",
                    "old_canonical_type": dropped[0]["old_canonical_type"],
                    "new_canonical_type": added[0]["new_canonical_type"]
                })
                
        return drifts

    def _generate_impact_summary(self, conn, table_id: int, column_name: str) -> Dict[str, Any]:
        """Looks up downstream impacts like DQ rules or models."""
        impact = {"affected_dq_rules": 0}
        try:
            with conn.cursor() as cur:
                # Check for affected DQ rules
                cur.execute("""
                    SELECT count(*) FROM metadata.data_quality_rules 
                    WHERE table_id = %s AND column_name ILIKE %s AND enabled = 'Y'
                """, (table_id, column_name))
                row = cur.fetchone()
                if row:
                    impact["affected_dq_rules"] = row[0]
        except Exception: pass
        return impact

