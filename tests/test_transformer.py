# tests/test_transformer.py
import pytest
from src.transform.transformer import Transformer
from src.metadata.metadata_models import MetadataRow
from src.constants import AuditColumns, DmlStatus
import datetime

pytestmark = pytest.mark.live

def test_add_audit_columns(spark):
    transformer = Transformer(run_id="test-run-123")
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="id", source_key_prefix="usr-", natural_key_column="id",
        key_approval_state="APPROVED", approved_source_business_key=["id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )
    
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    res_df = transformer.add_audit_columns(df, meta)
    
    assert AuditColumns.DML_TIMESTAMP in res_df.columns
    assert AuditColumns.DML_STATUS in res_df.columns
    assert AuditColumns.SOURCE_TAG in res_df.columns
    assert AuditColumns.PIPELINE_RUN_ID in res_df.columns
    assert AuditColumns.LANDING_INGESTION_TIMESTAMP in res_df.columns
    
    rows = res_df.collect()
    assert rows[0][AuditColumns.SOURCE_TAG] == "test_src"
    assert rows[0][AuditColumns.PIPELINE_RUN_ID] == "test-run-123"


def test_add_source_identification_key(spark):
    transformer = Transformer(run_id="test-run-123")
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="id", source_key_prefix="usr-", natural_key_column="id",
        key_approval_state="APPROVED", approved_source_business_key=["id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )
    
    data = [(123, "Alice")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    res_df = transformer.add_source_identification_key(df, meta)
    row = res_df.collect()[0]
    assert row[AuditColumns.SOURCE_IDENTIFICATION_KEY] == "usr-123"

def test_deduplicate(spark):
    transformer = Transformer(run_id="test-run-123")
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="id", source_key_prefix="usr-", natural_key_column="id",
        key_approval_state="APPROVED", approved_source_business_key=["id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )
    
    t1 = datetime.datetime(2026, 1, 1, 10, 0, 0)
    t2 = datetime.datetime(2026, 1, 1, 11, 0, 0)
    
    data = [
        (1, "Alice", t1),
        (1, "Alice Modified", t2),
        (2, "Bob", t1)
    ]
    df = spark.createDataFrame(data, ["id", "name", "updated_at"])
    
    res_df = transformer.deduplicate(df, meta, order_col="updated_at")
    
    rows = sorted(res_df.collect(), key=lambda r: r["id"])
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice Modified"
    assert rows[1]["name"] == "Bob"

def test_cast_all_to_string(spark):
    from pyspark.sql.types import StringType
    
    transformer = Transformer(run_id="test-run-123")
    
    # Create DataFrame with various types
    data = [(1, 12345.6789, True, datetime.datetime(2026, 6, 23, 12, 0, 0))]
    schema = ["int_col", "double_col", "bool_col", "ts_col"]
    df = spark.createDataFrame(data, schema)
    
    # Perform string casting
    res_df = transformer.cast_all_to_string(df)
    
    # Verify all columns are StringType
    for field in res_df.schema.fields:
        assert field.dataType == StringType()
        
    # Verify collected values are strings
    row = res_df.collect()[0]
    assert row["int_col"] == "1"
    # Note: Spark's double representation might have minor trailing float representation,
    # but casting to string preserves its literal decimal or standard representation.
    assert "12345.6789" in row["double_col"] or row["double_col"] == "12345.6789"
    assert row["bool_col"] == "true"


def test_cast_columns_date_sanitization(spark):
    transformer = Transformer(run_id="test-run-123")
    
    # 1. Test case: Date/timestamp strings (including malformed ones)
    data = [
        # (id, issueddate, validupto, protectordate, dateofbirth)
        (1, "1999-11-12 00:00:00", "0000-11-30 00:00:00", "2020-00-15", "1990-05-15 12:00:00"),
        (2, None, "2024-12-31 00:00:00", "2020-05-00", "2000-01-01 00:00:00")
    ]
    df = spark.createDataFrame(data, ["id", "issueddate", "validupto", "protectordate", "dateofbirth"])
    
    # Column mapping with mix of target datatypes and fallback source datatypes
    column_map = [
        {"column_id": 1, "column_sequence": 1, "canonical_type": "datetime", "source_column": "issueddate", "target_column": "issueddate", "data_type": "TimestampType", "source_column_datatype": "datetime"},
        {"column_id": 2, "column_sequence": 2, "canonical_type": "date", "source_column": "validupto", "target_column": "validupto", "data_type": "DateType", "source_column_datatype": "date"},
        {"column_id": 3, "column_sequence": 3, "canonical_type": "datetime", "source_column": "protectordate", "target_column": "protectordate", "data_type": None, "source_column_datatype": "datetime"},
        {"column_id": 4, "column_sequence": 4, "canonical_type": "date", "source_column": "dateofbirth", "target_column": "dateofbirth", "data_type": None, "source_column_datatype": "date"}
    ]
    
    from src.metadata.metadata_models import ResolvedTableMappings, MappingPolicy
    resolved = ResolvedTableMappings.build(1, column_map, MappingPolicy.EXPLICIT)
    meta = type("MockMeta", (), {"resolved_mappings": resolved})()
    
    res_df = transformer.cast_columns(df, meta)
    
    # Cast to string for safe local collect (Python's datetime library doesn't support year 0)
    res_str_df = res_df.select(
        "id",
        res_df.issueddate.cast("string").alias("issueddate"),
        res_df.validupto.cast("string").alias("validupto"),
        res_df.protectordate.cast("string").alias("protectordate"),
        res_df.dateofbirth.cast("string").alias("dateofbirth")
    )
    rows = sorted(res_str_df.collect(), key=lambda r: r["id"])
    
    # Row 1 check
    # - "1999-11-12 00:00:00" -> valid timestamp -> kept
    assert rows[0]["issueddate"] == "1999-11-12 00:00:00"
    # - "0000-11-30 00:00:00" -> year 0 -> converted to NULL
    assert rows[0]["validupto"] is None
    # - "2020-00-15" -> invalid zero-month -> converted to NULL
    assert rows[0]["protectordate"] is None
    # - "1990-05-15 12:00:00" -> fallback source datatype is 'date' -> cast to Date
    assert rows[0]["dateofbirth"] == "1990-05-15"
    
    # Row 2 check
    assert rows[1]["issueddate"] is None
    assert rows[1]["validupto"] == "2024-12-31"
    # - "2020-05-00" -> invalid zero-day -> converted to NULL
    assert rows[1]["protectordate"] is None
    assert rows[1]["dateofbirth"] == "2000-01-01"


def test_composite_keys(spark):
    transformer = Transformer(run_id="test-run-123")
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="members",
        source_db_type="postgres", landing_table="l_members", history_table="h_members",
        target_table="t_members", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column=None, source_key_prefix="usr-", natural_key_column="key_part1, key_part2",
        key_approval_state="APPROVED", approved_source_business_key=["key_part1", "key_part2"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )

    # 1. Test composite key lineage surrogate formatting
    data = [("VAL1", "VAL2", "Alice")]
    df = spark.createDataFrame(data, ["key_part1", "key_part2", "name"])

    res_df = transformer.add_source_identification_key(df, meta)
    row = res_df.collect()[0]
    assert row[AuditColumns.SOURCE_IDENTIFICATION_KEY] == "usr-VAL1-VAL2"

    # 2. Test composite key deduplication
    t1 = datetime.datetime(2026, 1, 1, 10, 0, 0)
    t2 = datetime.datetime(2026, 1, 1, 11, 0, 0)

    data_dedup = [
        ("VAL1", "VAL2", "Alice First", t1),
        ("VAL1", "VAL2", "Alice Latest", t2),
        ("VAL1", "VAL3", "Bob Unique", t1)
    ]
    df_dedup = spark.createDataFrame(data_dedup, ["key_part1", "key_part2", "name", "updated_at"])
    res_dedup = transformer.deduplicate(df_dedup, meta, order_col="updated_at")

    rows = sorted(res_dedup.collect(), key=lambda r: (r["key_part1"], r["key_part2"]))
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice Latest"
    assert rows[1]["name"] == "Bob Unique"


def test_natural_key_case_insensitive_resolution(spark):
    """Verify metadata 'emigrant_id' resolves to DataFrame 'EMIGRANT_ID' without fallback to 'ID'."""
    from src.transform.transformer import NaturalKeyResolutionException
    transformer = Transformer(run_id="test-run-case-123")
    meta = MetadataRow(
        id=1, source_tag="oracle_opf", source_schema="public", source_table="tblop_jandec25100626",
        source_db_type="oracle", landing_table="land.tblop_jandec25100626", history_table="hist.tblop_jandec25100626",
        target_table="target.tblop_jandec25100626", target_schema="dw", is_active="y", priority=2,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="emigrant_id", source_key_prefix="oracle_opf-", natural_key_column="emigrant_id",
        key_approval_state="APPROVED", approved_source_business_key=["emigrant_id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )

    data = [(100, 1, "Alice"), (200, 1, "Bob")]  # EMIGRANT_ID=100 & 200 (unique), ID=1 (duplicate)
    df = spark.createDataFrame(data, ["EMIGRANT_ID", "ID", "NAME"])

    # Must resolve to 'EMIGRANT_ID' (actual DataFrame casing), NOT 'ID'
    keys = transformer._parse_natural_keys(df, meta)
    assert keys == ["EMIGRANT_ID"]

    # Deduplicate must keep BOTH rows because EMIGRANT_ID is unique
    df_dedup = transformer.deduplicate(df, meta)
    assert df_dedup.count() == 2


def test_natural_key_missing_key_raises_exception(spark):
    """Verify missing configured natural key raises NaturalKeyResolutionException and DOES NOT silently fall back."""
    import pytest
    from src.transform.transformer import NaturalKeyResolutionException
    transformer = Transformer(run_id="test-run-missing-123")
    meta = MetadataRow(
        id=1, source_tag="oracle_opf", source_schema="public", source_table="tblop_jandec25100626",
        source_db_type="oracle", landing_table="land.tblop_jandec25100626", history_table="hist.tblop_jandec25100626",
        target_table="target.tblop_jandec25100626", target_schema="dw", is_active="y", priority=2,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="non_existent_key", source_key_prefix="oracle_opf-", natural_key_column="non_existent_key",
        key_approval_state="APPROVED", approved_source_business_key=["non_existent_key"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )

    df = spark.createDataFrame([(1, 100, "Alice")], ["ID", "EMIGRANT_ID", "NAME"])

    # Must raise NaturalKeyResolutionException instead of falling back to 'ID'
    with pytest.raises(NaturalKeyResolutionException) as exc_info:
        transformer._parse_natural_keys(df, meta)

    assert "non_existent_key" in str(exc_info.value)
    assert "not found in DataFrame columns" in str(exc_info.value)


def test_add_business_hash(spark):
    """Verify add_business_hash computes deterministic SHA-256 hash across sorted payload columns."""
    transformer = Transformer(run_id="test-run-hash-123")
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="id", source_key_prefix="usr-", natural_key_column="id",
        key_approval_state="APPROVED", approved_source_business_key=["id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )
    
    # 1. Identical rows produce identical 64-char SHA-256 hashes
    data = [(1, "Alice", 5000), (1, "Alice", 5000)]
    df = spark.createDataFrame(data, ["id", "name", "salary"])
    
    res_df = transformer.add_business_hash(df, meta)
    assert "row_hash" in res_df.columns
    
    rows = res_df.collect()
    assert rows[0]["row_hash"] == rows[1]["row_hash"]
    assert len(rows[0]["row_hash"]) == 64

    # 2. Re-ordered columns produce identical hashes due to sorted() business column ordering
    df_reordered = spark.createDataFrame([(5000, "Alice", 1)], ["salary", "name", "id"])
    res_reordered = transformer.add_business_hash(df_reordered, meta)
    assert res_reordered.collect()[0]["row_hash"] == rows[0]["row_hash"]

def test_composite_keys_untrimmed_whitespace(spark):
    """Verify natural_key_column with untrimmed whitespace produces clean normalized identification keys and columns."""
    transformer = Transformer(run_id="test-run-trim-123")
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="members",
        source_db_type="postgres", landing_table="l_members", history_table="h_members",
        target_table="t_members", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column=None, source_key_prefix="usr-", natural_key_column=" key_part1 , key_part2 ",
        key_approval_state="APPROVED", approved_source_business_key=["key_part1", "key_part2"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )

    assert meta.canonical_business_key_columns == ("key_part1", "key_part2")

    data = [("VAL1", "VAL2", "Alice")]
    df = spark.createDataFrame(data, ["key_part1", "key_part2", "name"])

    # 1. Test AuditColumns.SOURCE_IDENTIFICATION_TABLE formatting (no spaces)
    audit_df = transformer.add_audit_columns(df, meta)
    row_audit = audit_df.collect()[0]
    assert row_audit[AuditColumns.SOURCE_IDENTIFICATION_TABLE] == "test_src-members-key_part1-key_part2"

    # 2. Test natural key resolution
    keys = transformer._parse_natural_keys(df, meta)
    assert keys == ["key_part1", "key_part2"]


def test_add_source_identification_key_empty_prefix_fallback(spark):
    """Verify source_identification_key falls back to source_tag-source_table prefix when source_key_prefix is empty/None."""
    transformer = Transformer(run_id="test-run-prefix-fallback")
    
    # 1. source_key_prefix="" should fallback to source_tag + "-" + source_table + "-"
    meta = MetadataRow(
        id=1, source_tag="opf_oracle", source_schema="public", source_table="tblop_jandec25260126",
        source_db_type="oracle", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns=None, cdc_p_days=7, delta_days=1,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="emigrant_id", source_key_prefix="", natural_key_column="emigrant_id",
        key_approval_state="APPROVED", approved_source_business_key=["emigrant_id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="test",
        current_key_evidence_fingerprint="test", key_approved_by="test",
        key_approved_at=datetime.datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )
    df = spark.createDataFrame([(20012, "SAQIB ALI")], ["emigrant_id", "full_name"])
    res_df = transformer.add_source_identification_key(df, meta)
    row = res_df.collect()[0]
    assert row[AuditColumns.SOURCE_IDENTIFICATION_KEY] == "opf_oracle_tblop_jandec25260126_20012"

    # 2. source_key_prefix=None should also fallback to canonical prefix
    meta.source_key_prefix = None
    res_df2 = transformer.add_source_identification_key(df, meta)
    row2 = res_df2.collect()[0]
    assert row2[AuditColumns.SOURCE_IDENTIFICATION_KEY] == "opf_oracle_tblop_jandec25260126_20012"

    # 3. keyless table (no natural keys) should produce synthetic identity
    meta_keyless = MetadataRow(
        id=2, source_tag="opf_oracle", source_schema="public", source_table="keyless_tbl",
        source_db_type="oracle", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="", natural_key_column=None,
    )
    df_keyless = spark.createDataFrame([(1, "data")], ["col1", "col2"])
    res_keyless = transformer.add_source_identification_key(df_keyless, meta_keyless)
    row_keyless = res_keyless.collect()[0]
    assert row_keyless[AuditColumns.SOURCE_IDENTIFICATION_KEY].startswith("opf_oracle_keyless_tbl_synthetic_")



