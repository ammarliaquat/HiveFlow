# src/security/data_governance.py
"""
DataGovernanceService enforces data protection policies across observability boundaries.
It does NOT modify business data in the ETL pipelines. Instead, it masks sensitive data
in logs, error messages, and GUI displays according to db_columns_etl metadata.
"""

import re
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("DataGovernanceService")

class DataGovernanceService:
    _instance = None
    _classification_cache = {}
    _is_cached = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DataGovernanceService, cls).__new__(cls)
        return cls._instance

    def __init__(self, conn_manager=None):
        if not self._is_cached and conn_manager:
            self._load_cache(conn_manager)

    def _load_cache(self, conn_manager):
        """Load classification rules once per ETL run."""
        try:
            with conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    # Query all columns that have masking enabled
                    cur.execute("""
                        SELECT 
                            LOWER(t.source_table),
                            LOWER(c.column_name),
                            c.classification_level,
                            c.log_masking,
                            c.display_masking,
                            c.export_masking
                        FROM metadata.db_columns_etl c
                        JOIN metadata.db_tables_etl t ON c.table_id = t.table_id
                        WHERE c.log_masking = 'Y' 
                           OR c.display_masking = 'Y' 
                           OR c.export_masking = 'Y'
                    """)
                    rows = cur.fetchall()
                    for row in rows:
                        table, col, level, log_mask, display_mask, export_mask = row
                        key = (table, col)
                        self._classification_cache[key] = {
                            "level": level,
                            "log_masking": log_mask == 'Y',
                            "display_masking": display_mask == 'Y',
                            "export_masking": export_mask == 'Y'
                        }
            self._is_cached = True
            logger.info(f"DataGovernanceService cached {len(self._classification_cache)} masking rules.")
        except Exception as e:
            logger.error(f"Failed to load classification cache: {e}")

    @classmethod
    def get_column_rule(cls, table_name: str, column_name: str) -> Optional[Dict[str, Any]]:
        if not table_name or not column_name:
            return None
        return cls._classification_cache.get((table_name.lower(), column_name.lower()))

    @classmethod
    def sanitize_exception(cls, error_message: str, table_name: Optional[str] = None) -> str:
        """
        Scrub potential PII and restricted values from error stack traces before logging.
        For example: Passport=AB123456 -> Passport=<REDACTED>
        """
        if not error_message:
            return error_message
            
        sanitized = error_message

        # 1. Generic regex masking for highly sensitive global patterns (e.g. Email, SSN, Credit Cards)
        # Note: In a production system, these regexes would be comprehensive.
        # Example: rudimentary email mask
        sanitized = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '<EMAIL_REDACTED>', sanitized)
        
        # 2. Heuristic masking: if we see "ColumnName=Value" format, and ColumnName is restricted
        # We can dynamically look up if table_name is provided, or just redact known sensitive keywords
        sensitive_keywords = ['passport', 'cnic', 'password', 'token', 'secret', 'ssn', 'salary']
        
        for keyword in sensitive_keywords:
            # Match formats like: Passport=AB123456, passport: 'secret', etc.
            pattern = re.compile(rf'({keyword}\s*[=:]\s*)[\'"]?([^\s\'",]+)[\'"]?', re.IGNORECASE)
            sanitized = pattern.sub(r'\1<REDACTED>', sanitized)

        return sanitized

    @classmethod
    def mask_for_display(cls, row: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Returns a safely masked row dictionary for GUI display based on db_columns_etl metadata.
        """
        if not row or not table_name:
            return row
        masked_row = dict(row)
        for col_name, value in row.items():
            rule = cls.get_column_rule(table_name, col_name)
            if rule and rule.get("display_masking") and value is not None:
                str_val = str(value)
                if len(str_val) <= 4:
                    masked_row[col_name] = "****"
                else:
                    masked_row[col_name] = str_val[:2] + "****" + str_val[-2:]
        return masked_row


class GovernedLogFormatter(logging.Formatter):
    """
    Custom logging Formatter that automatically sanitizes output text
    via DataGovernanceService.sanitize_exception before stream emission.
    """
    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)
        return DataGovernanceService.sanitize_exception(formatted)


def apply_governed_logging(target_logger: Optional[logging.Logger] = None) -> None:
    """
    Applies GovernedLogFormatter to all handlers of the specified logger (or root logger).
    """
    logger_obj = target_logger or logging.getLogger()
    for handler in logger_obj.handlers:
        if not isinstance(handler.formatter, GovernedLogFormatter):
            fmt_str = getattr(handler.formatter, "_fmt", "%(asctime)s [%(levelname)s] %(name)s: %(message)s") if handler.formatter else "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            handler.setFormatter(GovernedLogFormatter(fmt_str))

