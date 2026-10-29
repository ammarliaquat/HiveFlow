# src/validation/schema_classifier.py

import logging

logger = logging.getLogger("SchemaClassifier")

class SchemaClassifier:
    
    @staticmethod
    def map_to_canonical(db_type: str, raw_data_type: str) -> str:
        """
        Maps a database-specific data type to a canonical type:
        INT, BIGINT, FLOAT, DECIMAL, STRING, DATE, TIMESTAMP, BOOLEAN, BINARY
        """
        if not raw_data_type:
            return "UNKNOWN"
            
        rt = raw_data_type.lower()
        
        # Numeric / Integer
        if any(x in rt for x in ["smallint", "tinyint", "mediumint"]):
            return "INT"
        if "bigint" in rt:
            return "BIGINT"
        if "int" in rt or "serial" in rt:
            return "INT"
            
        # Exact Numeric
        if any(x in rt for x in ["decimal", "numeric", "number"]):
            # For Oracle, NUMBER without precision is usually just FLOAT or BIGINT
            # For simplicity, treat as DECIMAL
            return "DECIMAL"
            
        # Approximate Numeric
        if any(x in rt for x in ["float", "real", "double"]):
            return "FLOAT"
            
        # Date/Time
        if "timestamp" in rt or "datetime" in rt:
            return "TIMESTAMP"
        if "date" in rt:
            return "DATE"
            
        # Boolean
        if any(x in rt for x in ["boolean", "bool", "bit"]):
            return "BOOLEAN"
            
        # Binary
        if any(x in rt for x in ["blob", "binary", "bytea"]):
            return "BINARY"
            
        # Default to String for char, varchar, text, clob, json, uuid
        return "STRING"

    @staticmethod
    def classify_drift(drift_type: str, old_type: str, new_type: str) -> str:
        """
        Returns INFO, WARNING, or CRITICAL based on the drift.
        """
        if drift_type == "NEW_COLUMN":
            return "WARNING"
            
        if drift_type == "DROPPED_COLUMN":
            return "CRITICAL"
            
        if drift_type == "RENAME_DETECTED":
            return "WARNING"
            
        if drift_type == "TYPE_CHANGED":
            if not old_type or not new_type:
                return "CRITICAL"
                
            old_t = old_type.upper()
            new_t = new_type.upper()
            
            # Safe Widening Check
            widening_paths = {
                "INT": ["BIGINT", "DECIMAL", "FLOAT", "STRING", "BOOLEAN"],
                "BIGINT": ["DECIMAL", "FLOAT", "STRING"],
                "FLOAT": ["DECIMAL", "STRING"],
                "DECIMAL": ["FLOAT", "STRING"],
                "DATE": ["TIMESTAMP", "STRING"],
                "TIMESTAMP": ["STRING"],
                "BOOLEAN": ["STRING", "INT"]
            }
            
            if new_t in widening_paths.get(old_t, []):
                return "INFO"
                
            return "CRITICAL"

        return "WARNING"
