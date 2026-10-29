# src/quality/dq_engine.py
import time
import json
import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from src.connections.connection_manager import ConnectionManager
from src.quality.dq_planner import DQRule, DQPlanner
from src.quality.dq_logger import DQLogger

logger = logging.getLogger("DQEngine")

class DQEngine:
    def __init__(self, conn_manager: ConnectionManager, run_id: str):
        self.conn_manager = conn_manager
        self.run_id = run_id
        self.dq_logger = DQLogger(conn_manager, run_id)

    def _load_rules(self, table_id: int, layer: str) -> List[DQRule]:
        rules = []
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT rule_id, column_name, rule_type, rule_expression, expected_value, 
                               tolerance, failure_action, severity, weight, depends_on_rule_id
                        FROM metadata.data_quality_rules
                        WHERE table_id = %s AND execution_layer = %s AND enabled = 'Y'
                        ORDER BY execution_order ASC
                    """
                    cur.execute(query, (table_id, layer))
                    for row in cur.fetchall():
                        rules.append(DQRule(
                            rule_id=row[0], column_name=row[1], rule_type=row[2],
                            rule_expression=row[3], expected_value=row[4], tolerance=row[5],
                            failure_action=row[6] or 'CONTINUE', severity=row[7] or 'WARNING', 
                            weight=row[8] or 10, depends_on_rule_id=row[9]
                        ))
        except Exception as e:
            logger.error(f"Failed to load DQ rules for table {table_id}: {e}")
        return rules

    def evaluate(self, df: DataFrame, table_id: int, table_name: str, source_tag: str, layer: str) -> str:
        """
        Evaluates rules, logs results, scores the table, and returns the highest severity failure action.
        Returns one of: 'CONTINUE', 'WARN', 'SKIP_TABLE', 'FAIL_SOURCE', 'FAIL_PIPELINE'
        """
        rules = self._load_rules(table_id, layer)
        if not rules:
            return "CONTINUE"
            
        logger.info(f"Evaluating {len(rules)} DQ rules for {table_name} at {layer} layer.")
        planner = DQPlanner(rules)
        plan = planner.plan()
        
        results = []
        total_weight = 0
        total_deduction = 0.0
        
        # Determine highest severity action to return
        action_precedence = {"CONTINUE": 0, "WARN": 1, "SKIP_TABLE": 2, "FAIL_SOURCE": 3, "FAIL_PIPELINE": 4}
        highest_action = "CONTINUE"
        
        # 1. Execute AGGREGATION Group
        agg_rules = plan["AGGREGATION"]
        if agg_rules:
            exprs = DQPlanner.build_aggregation_exprs(agg_rules)
            start_ms = int(time.time() * 1000)
            
            # We must also compute total count to calculate percentages
            exprs.append(F.count(F.lit(1)).alias("total_count"))
            agg_row = df.agg(*exprs).collect()[0]
            
            total_rows = agg_row["total_count"] or 0
            exec_time = int(time.time() * 1000) - start_ms
            
            for r in agg_rules:
                failed_rows = agg_row[f"rule_{r.rule_id}"] or 0
                if failed_rows < 0:
                    status = "ERROR" # parse error
                    failed_rows = 0
                else:
                    status = "PASS" if failed_rows == 0 else "FAIL"
                    
                failure_pct = (failed_rows / total_rows) if total_rows > 0 else 0.0
                
                # Check Tolerance (simplistic exact expected_value check for now)
                # If expected_value is 0, and actual > 0, it fails unless tolerance allows.
                
                results.append({
                    "rule_id": r.rule_id,
                    "status": status,
                    "expected_value": r.expected_value or "0",
                    "actual_value": str(failed_rows),
                    "failed_rows": failed_rows,
                    "failure_pct": float(failure_pct),
                    "execution_ms": exec_time,
                    "severity": r.severity,
                    "rule_type": r.rule_type
                })
                
                # Score deduction
                total_weight += r.weight
                sev_mult = 1.0 if r.severity == "CRITICAL" else 0.5 if r.severity == "WARNING" else 0.0
                if status == "FAIL":
                    total_deduction += (r.weight * failure_pct * sev_mult)
                    if action_precedence[r.failure_action] > action_precedence[highest_action]:
                        highest_action = r.failure_action
                
                # Quarantine if failed
                if status == "FAIL" and failed_rows > 0:
                    self._quarantine_agg_rule(df, r)
                    
        # Capture total_rows from AGGREGATION result to avoid a separate df.count() in DISTINCT loop
        # If no AGGREGATION rules ran, total_rows remains None and is computed on-demand (once).
        total_rows: Optional[int] = None
        if agg_rules:
            total_rows = agg_row["total_count"] or 0

        # 2. Execute DISTINCT Group
        distinct_rules = plan["DISTINCT"]
        for r in distinct_rules:
            total_weight += r.weight
            start_ms = int(time.time() * 1000)
            col = r.column_name
            # Find rows with duplicates
            dup_df = df.groupBy(col).count().filter(F.col("count") > 1)
            failed_distinct = dup_df.count()
            
            # To get total failed rows, we could sum the counts, but for now we'll record distinct failing keys
            failed_rows = failed_distinct
            exec_time = int(time.time() * 1000) - start_ms
            
            status = "PASS" if failed_rows == 0 else "FAIL"
            failure_pct = 0.0
            if failed_rows > 0:
                if total_rows is None:
                    total_rows = df.count()  # computed at most once across all DISTINCT rules
                failure_pct = failed_rows / total_rows if total_rows > 0 else 0.0

            results.append({
                "rule_id": r.rule_id,
                "status": status,
                "expected_value": "0",
                "actual_value": str(failed_rows),
                "failed_rows": failed_rows,
                "failure_pct": float(failure_pct),
                "execution_ms": exec_time,
                "severity": r.severity,
                "rule_type": r.rule_type
            })
            
            sev_mult = 1.0 if r.severity == "CRITICAL" else 0.5 if r.severity == "WARNING" else 0.0
            if status == "FAIL":
                total_deduction += (r.weight * failure_pct * sev_mult)
                if action_precedence[r.failure_action] > action_precedence[highest_action]:
                    highest_action = r.failure_action
                # Quarantine
                self._quarantine_distinct_rule(df, r, dup_df)

        # Log results
        self.dq_logger.log_results(table_id, table_name, source_tag, results)
        
        # Calculate Score
        quality_score = 100.0
        if total_weight > 0:
            quality_score = max(0.0, 100.0 - total_deduction)
            
        overall_status = "PASS" if quality_score >= 90.0 else "FAIL"
        self.dq_logger.log_table_score(table_id, layer, quality_score, overall_status)
        
        return highest_action

    def _quarantine_agg_rule(self, df: DataFrame, rule: DQRule):
        """Constructs a filter to grab failing rows and quarantines them."""
        col = F.col(rule.column_name) if rule.column_name else None
        rt = rule.rule_type.upper()
        
        filter_expr = None
        if rt == "NOT_NULL":
            filter_expr = col.isNull()
        elif rt == "ACCEPTED_VALUES" and rule.rule_expression:
            try:
                allowed = json.loads(rule.rule_expression)
                filter_expr = ~col.isin(allowed)
            except Exception:
                pass
        elif rt == "REGEX" and rule.rule_expression:
            filter_expr = col.isNotNull() & ~col.rlike(rule.rule_expression)
        elif rt == "SQL_EXPRESSION" and rule.rule_expression:
            filter_expr = ~F.expr(rule.rule_expression)
            
        if filter_expr is not None:
            failed_df = df.filter(filter_expr)
            self.dq_logger.quarantine_failures(rule.rule_id, failed_df)

    def _quarantine_distinct_rule(self, df: DataFrame, rule: DQRule, dup_keys_df: DataFrame):
        """Joins back to original df to get failing rows, or just limits the duplicate keys."""
        col = rule.column_name
        # Simple quarantine: just record the duplicate keys themselves for now
        self.dq_logger.quarantine_failures(rule.rule_id, dup_keys_df)

