# src/extract/extractor.py
# -----------------------------------------------------------------------------
# STEP 1 IN OUR DATA PIPELINE: THE EXTRACTOR (BRONZE / LANDING INGESTION)
# -----------------------------------------------------------------------------
# Think of this file like a super-fast librarian robot!
# Its only job is to go to different school libraries (source databases like Oracle or MySQL),
# ask politely for specific books or lists of toys (tables of data),
# and bring them back safely into our supercomputer's hands (Apache Spark)
# without dropping any pages or crashing the computer.
# -----------------------------------------------------------------------------
"""
Reads from source database via JDBC using Spark's parallel read capability.
Replaces the old DataIngestion.pyd + TableReadInThreadNew.pyd modules.

Key pattern: JDBC parallel read using partition_column + numPartitions.
This is the PySpark equivalent of Python threading (10 threads).
"""

# [Imports - grabbing our toolkits from our backpack]
# Here we import helper labels: Optional means a lunchbox can have a sandwich or be empty; Any means any toy fits.
from typing import Optional, Any
# SparkSession is our supercomputer engine; DataFrame is a giant Excel sheet held in computer memory.
from pyspark.sql import SparkSession, DataFrame
# F gives us lots of handy math and text functions for our giant spreadsheet.
from pyspark.sql import functions as F
# logging lets our robot write diary entries so human operators know what it is doing.
import logging

# MetadataRow holds a recipe card with instructions about what table to read and where it lives.
from src.metadata.metadata_models import MetadataRow
# ConnectionManager is our secret phonebook containing addresses and keys for all databases.
from src.connections.connection_manager import ConnectionManager, ConnectionConfig
# DefaultValues holds standard fallback numbers (like: if we don't know how many helpers to hire, hire 4).
from src.constants import DefaultValues
# db_utils has little tools to put quotes around weird table names and attach connection options.
from src.utils.db_utils import quote_table_name, apply_jdbc_options

# RuntimeOptimizer is our smart planner that calculates the best shopping cart size (fetch size) for our data.
from src.runtime.runtime_optimizer import RuntimeOptimizer, OptimizationPlan

# Create our diary writer specifically with the name "extractor"
log = logging.getLogger("extractor")


# -----------------------------------------------------------------------------
# The Extractor Class: Our Librarian Robot
# -----------------------------------------------------------------------------
class Extractor:

    # The setup function: this runs when we first turn our robot on
    def __init__(self, spark: SparkSession, conn_manager: ConnectionManager):
        # Store our big Spark supercomputer engine inside the robot
        self.spark = spark
        # Store our address book so the robot knows where to connect
        self.conn_manager = conn_manager
        # Wake up our smart planner helper to optimize how we read
        self.optimizer = RuntimeOptimizer(conn_manager)

    # -------------------------------------------------------------------------
    # Main Function: Read a table from a database into a Spark spreadsheet
    # -------------------------------------------------------------------------
    def read(
        self,
        meta: MetadataRow,                    # The instruction card telling us which table to read
        cdc_filter: Optional[str] = None,     # An optional filter, like "only give me today's homework"
        batch_logger: Optional[Any] = None,   # A diary recorder for this specific run
        log_id: Optional[int] = None,         # The ID number of the diary entry
    ) -> DataFrame:
        """
        Read a source table into a Spark DataFrame.

        cdc_filter: optional SQL WHERE clause fragment for CDC
                    e.g. "updated_at >= '2024-01-01'"
        """
        # Look up the connection details (host, port, username) for this source database
        conn: ConnectionConfig = self.conn_manager.get(meta.source_tag, expected_role="source")
        # Check what kind of database it is (like "mysql", "oracle", or "postgresql")
        db_type = conn.db_type.lower() if conn.db_type else "unknown"

        # 1. To bypass the MySQL JDBC driver date-decoding year-0000 crash, we dynamically
        # detect all DATE/DATETIME/TIMESTAMP columns from the source table and map them
        # to STRING at read time using Spark's customSchema option.
        # Create an empty dictionary to hold special rules for data types if needed
        custom_schema_opts = {}
        # Oracle uses a space before subquery aliases, while MySQL and Postgres use ' AS '
        alias_kw = " " if db_type == "oracle" else " AS "
        # Flag to remember if this table has giant text or picture files (LOBs)
        has_lob = False
        # By default, ask to select all columns (the asterisk '*' means everything)
        select_expr = "*"

        # Special safety check for MySQL databases
        if db_type == "mysql":
            try:
                # Do a quick "fake query" with WHERE 1=0 (gets 0 rows) just to see what the column names and types are
                infer_df = (
                    apply_jdbc_options(self.spark.read.format("jdbc"), conn)
                    .option("dbtable", f"(SELECT * FROM {quote_table_name(meta.full_source_table, conn.db_type)} WHERE 1=0){alias_kw}schema_infer")
                    .load()
                )
                # Grab date and timestamp type definitions from Spark
                from pyspark.sql.types import TimestampType, DateType
                # Find all columns that hold calendar dates or timestamps
                date_cols = [
                    f.name for f in infer_df.schema.fields
                    if isinstance(f.dataType, (TimestampType, DateType))
                ]
                # If there are date columns, tell Spark to read them as plain text (STRING) so year 0000 doesn't crash us!
                if date_cols:
                    custom_schema_opts["customSchema"] = ", ".join(f"{col} STRING" for col in date_cols)
            except Exception:
                # If this quick check fails, don't panic, just move on gently
                pass

        # Reset our column selector to all columns before checking Oracle
        select_expr = "*"

        # Special safety check for Oracle databases: handling giant text columns (CLOBs and BLOBs)
        if db_type in ("oracle", "ora"):
            try:
                # Connect to our local metadata database to look up column types
                with self.conn_manager.get_metadata_db_connection() as _meta_conn:
                    # Open a cursor (like picking up a pen to write a question to the database)
                    with _meta_conn.cursor() as _cur:
                        # Ask our catalog if this table has any huge text/binary columns (clob, nclob, blob, bfile)
                        _cur.execute("""
                            SELECT UPPER(column_name), LOWER(data_type)
                            FROM metadata.db_columns
                            WHERE LOWER(database_tag) = LOWER(%s)
                              AND table_name = %s
                            ORDER BY column_seq
                        """, [meta.source_tag, meta.source_table])
                        meta_cols_raw = _cur.fetchall()
                        lob_col_types = {
                            r[0]: r[1] for r in meta_cols_raw
                            if r[1] in ('clob', 'nclob', 'blob', 'bfile')
                        }

                # If we found any giant text/binary columns, we need to handle them carefully!
                if lob_col_types:
                    # Mark that this table has giant text/binary fields
                    has_lob = True
                    # If any column is 'bfile', live probe with SELECT * fails because Spark JDBC cannot map type -13.
                    # In that case or as fallback, use the column sequence directly from metadata.db_columns.
                    live_cols = [r[0] for r in meta_cols_raw]
                    if not any(t == 'bfile' for t in lob_col_types.values()):
                        try:
                            probe_df = (
                                apply_jdbc_options(self.spark.read.format("jdbc"), conn)
                                .option("dbtable", f"(SELECT * FROM {quote_table_name(meta.full_source_table, conn.db_type)} WHERE 1=0){alias_kw}schema_probe")
                                .load()
                            )
                            live_cols = [f.name for f in probe_df.schema.fields]
                        except Exception as _probe_err:
                            log.warning(f"[Oracle LOB] Live schema probe skipped ({_probe_err}), using metadata columns.")

                    # List for building our custom SQL column list
                    proj_list = []
                    # List to track which giant columns we actually matched
                    actual_lob_cols = []

                    # Look at each live column one by one
                    for c_name in live_cols:
                        c_upper = c_name.upper()
                        if c_upper in lob_col_types:
                            dtype = lob_col_types[c_upper]
                            if dtype == "blob":
                                # Oracle SQL RAW buffer limit is 2000 bytes (amount=4000 triggers ORA-06502)
                                proj_list.append(f'DBMS_LOB.SUBSTR("{c_name}", 2000, 1) AS "{c_name}"')
                            elif dtype == "bfile":
                                # Oracle BFILE cannot be read by Spark JDBC (type -13). Project safely as NULL string.
                                proj_list.append(f'CAST(NULL AS VARCHAR2(500)) AS "{c_name}"')
                            else:
                                # CLOB / NCLOB: Oracle SQL VARCHAR2 limit is 4000 characters
                                proj_list.append(f'DBMS_LOB.SUBSTR("{c_name}", 4000, 1) AS "{c_name}"')
                            actual_lob_cols.append(c_name)
                        else:
                            # Normal columns get wrapped in safe quotation marks
                            proj_list.append(f'"{c_name}"')

                    # Glue all the columns together with commas to make our final SELECT statement
                    select_expr = ", ".join(proj_list)
                    # Keep track of the giant columns we protected
                    lob_cols = actual_lob_cols
                    # Write a friendly note in the log describing what we protected
                    log.info(
                        f"[Oracle LOB Engine Cast] Safely projected {len(lob_cols)} LOB/binary column(s) for "
                        f"'{meta.source_table}': {lob_cols}"
                    )
                    # Tell Spark to treat these trimmed giant columns as plain text (STRING)
                    custom_schema_opts["customSchema"] = ", ".join(f"{col} STRING" for col in lob_cols)
            except Exception as _e:
                # If something went wrong checking giant columns, write a gentle warning and keep going
                log.warning(
                    f"[Oracle CLOB] Could not construct LOB projection for '{meta.source_table}': {_e}. "
                    f"Proceeding with SELECT *."
                )

        # Count how many giant columns we found (or 0 if none)
        lob_cnt = len(lob_cols) if "lob_cols" in locals() and lob_cols else 0
        # True if we customized our column list instead of using '*'
        to_char_proj = bool(select_expr != "*")
        # Ask our smart optimizer helper: "How many rows should we fetch in each shopping trip?"
        plan: ExecutionPlan = self.optimizer.optimize_table_execution(
            meta=meta, has_lob=has_lob, lob_count=lob_cnt, to_char_projected=to_char_proj
        )

        # If a run logger is active, write our optimization plan into the diary
        if batch_logger and log_id:
            try:
                # dataclasses lets us turn a Python object into a neat dictionary
                import dataclasses
                # Save the execution plan into our stage metrics table
                batch_logger.update_stage_metrics(log_id, {"execution_plan": dataclasses.asdict(plan)})
            except Exception as _e:
                # Log a small warning if saving metrics had an issue
                log.warning(f"[Extractor] Could not record execution_plan in stage_metrics: {_e}")

        # Build our filter rule: if we have a special CDC filter, use it; otherwise '1=1' means "match everything!"
        where_clause = cdc_filter if cdc_filter else "1=1"
        # Build the exact SQL query string that Spark will send across the network to the database
        dbtable = f"(SELECT {select_expr} FROM {quote_table_name(meta.full_source_table, conn.db_type)} WHERE {where_clause}){alias_kw}src"

        # Prepare our Spark reader with the database address, credentials, query, and shopping cart fetch size
        reader = (
            apply_jdbc_options(self.spark.read.format("jdbc"), conn)
            .option("dbtable", dbtable)
            .option("fetchsize", str(plan.fetch_size))
        )

        # If we created special data type rules (like for MySQL dates or Oracle CLOBs), tell Spark to use them
        if "customSchema" in custom_schema_opts:
            reader = reader.option("customSchema", custom_schema_opts["customSchema"])

        # Parallel read check: Can we hire multiple helper workers to read different parts of the table at once?
        # If we have a partition column (like an ID number 1 to 1000), we can split the work!
        if meta.partition_column:
            reader = self._add_parallel_read_options(reader, meta, conn)
        else:
            # If no partition column, just tell Spark how many partitions to expect
            reader = reader.option("numPartitions", meta.num_partitions or DefaultValues.NUM_PARTITIONS)

        # Tell Spark to execute the read and give us back our big DataFrame spreadsheet!
        df = reader.load()

        # Let's check our work and write a nice diagnostics report in the log
        try:
            # Ask Spark how many helper workers (partitions) are actually assigned to read this data
            actual_partitions = df.rdd.getNumPartitions()  # zero-cost local metadata — no Spark action
            # The number of partitions configured on our instruction card
            configured_parts = meta.num_partitions or DefaultValues.NUM_PARTITIONS
            # The chunk size we configured
            fetch_sz = meta.chunk_size or DefaultValues.CHUNK_SIZE
            # The column name we split work on (or 'N/A' if none)
            part_col = meta.partition_column or "N/A"
            # The lowest ID number we split on
            has_lower = getattr(meta, "partition_lower_bound", "N/A")
            # The highest ID number we split on
            has_upper = getattr(meta, "partition_upper_bound", "N/A")

            # Write a clear summary for the human engineers to read
            log.info(
                f"\n[Oracle Extraction Diagnostics]\n"
                f"  Table Name                 : {meta.source_table}\n"
                f"  Partition Column           : {part_col}\n"
                f"  Configured JDBC Partitions : {configured_parts}\n"
                f"  Actual Spark Partitions    : {actual_partitions}\n"
                f"  Fetch Size                 : {fetch_sz}\n"
                f"  Lower Bound                : {has_lower}\n"
                f"  Upper Bound                : {has_upper}"
            )
        except Exception as _e:
            # If checking partition count fails, just note it without stopping the pipeline
            log.warning(f"[Oracle Diagnostics Error] Could not determine actual partitions: {_e}")

        # Return the loaded DataFrame spreadsheet so the next step (Transformer) can work with it!
        return df

    # -------------------------------------------------------------------------
    # Helper: Read already-landed files from disk instead of calling database again
    # -------------------------------------------------------------------------
    def read_landing(
        self,
        meta: MetadataRow,                    # The instruction card for the table
        date_partition: Optional[str] = None, # Optional folder date like "dt=2026-09-09"
    ) -> DataFrame:
        """
        Read landed Parquet data from Bronze layer instead of re-extracting from source database.
        Ensures source database extraction happens only once per pipeline run.
        """
        # Build the folder path where the Bronze/Landing files live on our hard drive
        base_path = (
            f"{self.conn_manager.paths['landing_base']}"
            f"/{meta.source_tag}"
            f"/{meta.landing_table or meta.source_table}"
        )
        # If a specific date folder was requested, point to it; otherwise use the base folder
        path = f"{base_path}/dt={date_partition}" if date_partition else base_path
        # Read the Parquet file straight off the local disk!
        return self.spark.read.parquet(path)

    def read_history(self, meta: MetadataRow) -> DataFrame:
        """Read the persisted Silver artifact for one table without querying its source."""
        path = (
            f"{self.conn_manager.paths['history_base']}"
            f"/{meta.source_tag}"
            f"/{meta.history_table or meta.source_table}"
        )
        try:
            return self.spark.read.format("delta").load(path)
        except Exception:
            return self.spark.read.parquet(path)

    def read_artifact(self, artifact: Any) -> DataFrame:
        """Read the exact immutable artifact bound into a frozen contract."""
        uri = getattr(artifact, "storage_uri", None)
        fmt = getattr(artifact, "storage_format", None) or "parquet"
        if not uri:
            raise ValueError("Frozen artifact reference has no storage_uri")
        return self.spark.read.format(str(fmt).lower()).load(str(uri))

    # -------------------------------------------------------------------------
    # Helper: Configure multiple helper workers to read in parallel
    # -------------------------------------------------------------------------
    def _add_parallel_read_options(self, reader, meta: MetadataRow, conn: ConnectionConfig):
        """
        Enable parallel JDBC read by computing bounds on the partition_column.
        Spark splits the column range into num_partitions buckets, each read by one task.
        """
        # Determine our chunk/fetch size from metadata or standard defaults
        fetch_sz = meta.chunk_size or DefaultValues.CHUNK_SIZE
        # Set the fetch size on our reader
        reader = reader.option("fetchsize", str(fetch_sz))

        # Get the partition column name (like "order_id")
        part_col = meta.partition_column
        # If multiple columns were given (like "order_id, user_id"), take only the first one
        primary_col = part_col.split(",")[0].strip() if part_col else part_col
        # Check if the instruction card already knows the lowest ID number (lowerBound)
        has_lower = getattr(meta, "partition_lower_bound", None)
        # Check if the instruction card already knows the highest ID number (upperBound)
        has_upper = getattr(meta, "partition_upper_bound", None)
        # How many helper workers we want to split the work between
        num_parts = meta.num_partitions or DefaultValues.NUM_PARTITIONS

        def _format_bound(val):
            if val is None:
                return val
            s = str(val).strip()
            try:
                f = float(s)
                if f.is_integer():
                    return str(int(f))
            except (ValueError, TypeError):
                pass
            return s

        # If both the lowest and highest ID numbers were already provided on the card:
        if has_lower is not None and has_upper is not None:
            # Tell Spark to divide the range between lowerBound and upperBound across numPartitions
            return (
                reader
                .option("partitionColumn", primary_col)
                .option("lowerBound", _format_bound(has_lower))
                .option("upperBound", _format_bound(has_upper))
                .option("numPartitions", str(num_parts))
            )

        # If bounds weren't on the card, we must ask the database directly for the MIN and MAX values
        db_type = conn.db_type.lower() if conn.db_type else "unknown"
        # Call our helper function below to find the minimum and maximum values in the column
        bounds = self._get_column_bounds(
            conn, meta.full_source_table, part_col, db_type
        )
        # If we couldn't find bounds, or if the column type isn't supported, or min == max (only 1 value exists)
        if bounds["min"] is None or not bounds["supported"] or str(bounds["min"]) == str(bounds["max"]):
            # If the column type was something weird like text words instead of numbers
            if not bounds["supported"] and bounds["min"] is not None:
                # Warn the human operator that we can't split text words into math buckets
                log.warning(
                    f"Partition column '{part_col}' on table '{meta.source_table}' "
                    f"is of type {bounds['type']}. Parallel read is only supported for numeric, date, or timestamp. "
                    f"Falling back to single partition read."
                )
            # Fall back to 1 helper worker so it still reads safely without errors
            return reader.option("numPartitions", "1")

        # Log our exciting discovery of the live column bounds
        log.info(
            f"[Extractor] Live column bounds discovered for '{meta.source_table}': "
            f"min={bounds['min']}, max={bounds['max']}, numPartitions={meta.num_partitions or DefaultValues.NUM_PARTITIONS}"
        )
        # Configure Spark with the live MIN (lowerBound) and MAX (upperBound) values
        return (
            reader
            .option("partitionColumn", primary_col)
            .option("lowerBound", _format_bound(bounds["min"]))
            .option("upperBound", _format_bound(bounds["max"]))
            .option("numPartitions", str(meta.num_partitions or DefaultValues.NUM_PARTITIONS))
        )

    # -------------------------------------------------------------------------
    # Helper: Ask the database for the minimum and maximum values of a column
    # -------------------------------------------------------------------------
    def _get_column_bounds(self, conn: ConnectionConfig, table: str, column: str, db_type: str) -> dict:
        """Get min/max of the partition column for parallel read setup."""
        # Grab only the first column name if a comma list was provided
        primary_col = column.split(",")[0].strip() if column else column
        # Convert the database type to lowercase letters
        db_type_lower = str(db_type).lower() if db_type else ""
        # Put appropriate quote characters around the column name based on database dialect
        if db_type_lower == "mysql":
            col_quoted = f"`{primary_col}`"        # MySQL uses backticks
        elif db_type_lower in ("oracle", "ora"):
            col_quoted = primary_col               # Oracle prefers plain unquoted or uppercase
        else:
            col_quoted = f'"{primary_col}"'        # PostgreSQL / others use double quotes

        # Oracle needs a space; MySQL/Postgres need ' AS '
        alias_kw = " " if db_type == "oracle" else " AS "
        # Query the database to find MIN() and MAX() of this column
        bounds_df = (
            apply_jdbc_options(self.spark.read.format("jdbc"), conn)
            .option("dbtable", f"(SELECT MIN({col_quoted}) AS mn, MAX({col_quoted}) AS mx FROM {table}){alias_kw}bounds")
            .load()
        )
        # Find the column names for the minimum and maximum results
        if isinstance(getattr(bounds_df, "columns", None), list) and len(bounds_df.columns) >= 2:
            col_mn = bounds_df.columns[0]
            col_mx = bounds_df.columns[1]
        elif hasattr(bounds_df, "schema") and hasattr(bounds_df.schema, "fields") and len(bounds_df.schema.fields) >= 2:
            col_mn = bounds_df.schema.fields[0].name
            col_mx = bounds_df.schema.fields[1].name
        else:
            col_mn, col_mx = "mn", "mx"

        # Pull the single result row from the database into local memory
        row = bounds_df.collect()[0]
        # Extract the minimum value safely
        mn_val = row[col_mn] if isinstance(row, dict) else getattr(row, col_mn, row[0] if hasattr(row, '__getitem__') else None)
        # Extract the maximum value safely
        mx_val = row[col_mx] if isinstance(row, dict) else getattr(row, col_mx, row[1] if hasattr(row, '__getitem__') else None)

        # Import Spark types to check if this column can be mathematically partitioned
        from pyspark.sql.types import NumericType, DateType, TimestampType
        # Look at the data type of the minimum value column
        mn_type = bounds_df.schema[col_mn].dataType
        # We only support parallel slicing on numbers, dates, or timestamps
        supported = isinstance(mn_type, (NumericType, DateType, TimestampType))

        # If it is a supported type and we got real values back:
        if supported and mn_val is not None and mx_val is not None:
            # If it's a number (like an ID 1, 2, 3), convert it to clean whole numbers (integers)
            if isinstance(mn_type, NumericType):
                try:
                    mn_val = int(float(mn_val))
                    mx_val = int(float(mx_val))
                except (ValueError, TypeError):
                    pass
            # If it's a calendar date or clock timestamp, format it nicely as "YYYY-MM-DD HH:MM:SS"
            elif isinstance(mn_type, (DateType, TimestampType)):
                if hasattr(mn_val, "strftime"):
                    mn_val = mn_val.strftime("%Y-%m-%d %H:%M:%S")
                if hasattr(mx_val, "strftime"):
                    mx_val = mx_val.strftime("%Y-%m-%d %H:%M:%S")

        # Return a neat dictionary with our min, max, whether it's supported, and its type
        return {
            "min": mn_val,
            "max": mx_val,
            "supported": supported,
            "type": str(mn_type)
        }

    # -------------------------------------------------------------------------
    # Helper: Count total rows in the source table
    # -------------------------------------------------------------------------
    def count_source(self, meta: MetadataRow) -> int:
        """Get source row count for reconciliation."""
        # Look up connection details for the source database
        conn = self.conn_manager.get(meta.source_tag, expected_role="source")
        # Check what dialect of database it is
        db_type = conn.db_type.lower() if conn.db_type else "unknown"
        # Oracle uses a space; MySQL/Postgres need ' AS '
        alias_kw = " " if db_type == "oracle" else " AS "
        # Run a COUNT(*) query on the source table
        df = (
            apply_jdbc_options(self.spark.read.format("jdbc"), conn)
            .option("dbtable", f"(SELECT COUNT(*) AS cnt FROM {quote_table_name(meta.full_source_table, conn.db_type)}){alias_kw}cnt_q")
            .load()
        )
        # Pull the count number from the first row and return it as an integer
        return df.collect()[0][df.columns[0]]
