# src/quality/dq_planner.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import json

@dataclass
class DQRule:
    rule_id: int
    column_name: str
    rule_type: str
    rule_expression: str
    expected_value: str
    tolerance: str
    failure_action: str
    severity: str
    weight: int
    depends_on_rule_id: int

class DQPlanner:
    """
    Analyzes a list of DQ rules for a table and groups them into optimal Spark execution passes.
    """

    def __init__(self, rules: List[DQRule]):
        self.rules = rules

    def plan(self) -> Dict[str, List[DQRule]]:
        """
        Returns a dictionary grouping rules into execution strategies.
        """
        plan = {
            "AGGREGATION": [],
            "DISTINCT": [],
            "RECONCILIATION": []
        }

        for rule in self.rules:
            rt = rule.rule_type.upper()
            if rt in ("NOT_NULL", "ROW_COUNT", "REGEX", "ACCEPTED_VALUES", "SQL_EXPRESSION"):
                plan["AGGREGATION"].append(rule)
            elif rt == "UNIQUE":
                plan["DISTINCT"].append(rule)
            elif rt in ("RECON_COUNT", "RECON_SUM", "RECON_HASH"):
                plan["RECONCILIATION"].append(rule)
            else:
                # Fallback to single pass or ignore
                plan["AGGREGATION"].append(rule)
                
        return plan

    @staticmethod
    def build_aggregation_exprs(rules: List[DQRule]) -> List[F.Column]:
        """
        Translates a list of AGGREGATION rules into a list of PySpark aggregation expressions.
        Each expression calculates the number of *failed* rows.
        """
        exprs = []
        for r in rules:
            rt = r.rule_type.upper()
            col = F.col(r.column_name) if r.column_name else None
            
            # The goal is to return the number of violating rows.
            # So if NOT_NULL, we sum where col is NULL.
            if rt == "NOT_NULL":
                expr = F.sum(col.isNull().cast("int")).alias(f"rule_{r.rule_id}")
                exprs.append(expr)
            elif rt == "ROW_COUNT":
                # Returns total row count (failure calculation is done post-eval)
                expr = F.count(F.lit(1)).alias(f"rule_{r.rule_id}")
                exprs.append(expr)
            elif rt == "ACCEPTED_VALUES":
                if r.rule_expression:
                    try:
                        # Expecting a JSON list like '["M", "F"]'
                        allowed = json.loads(r.rule_expression)
                        expr = F.sum((~col.isin(allowed)).cast("int")).alias(f"rule_{r.rule_id}")
                        exprs.append(expr)
                    except Exception:
                        expr = F.lit(-1).alias(f"rule_{r.rule_id}") # parse error
                        exprs.append(expr)
            elif rt == "REGEX":
                if r.rule_expression:
                    # RLike returns true if it matches. Violations are where it doesn't match AND isn't null.
                    expr = F.sum((col.isNotNull() & ~col.rlike(r.rule_expression)).cast("int")).alias(f"rule_{r.rule_id}")
                    exprs.append(expr)
            elif rt == "SQL_EXPRESSION":
                if r.rule_expression:
                    # SQL expression should return true for valid rows. Violations are where expression is false.
                    expr = F.sum((~F.expr(r.rule_expression)).cast("int")).alias(f"rule_{r.rule_id}")
                    exprs.append(expr)
                    
        return exprs

