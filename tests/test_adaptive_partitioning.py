# tests/test_adaptive_partitioning.py
"""
Unit tests for Industry-Standard Adaptive Partitioning Strategy.
"""
from src.metadata.metadata_models import MetadataRow


def test_adaptive_partitioning_threshold_under_minimum():
    """Verify tables under minimum_partition_threshold return numPartitions = 1."""
    meta = MetadataRow(
        id=1, source_tag="src", source_schema="s", source_table="t", source_db_type="oracle",
        landing_table="t", history_table="t", target_table="t", target_schema="public",
        is_active="y", priority=1, load_type="gold", is_cdc="n", cdc_columns=None,
        cdc_p_days=0, delta_days=0, last_success_timestamp=None, is_reconcile="n",
        reconcile_column=None, reconcile_measure="count", chunk_size=100000,
        num_partitions=1, partition_column="id", source_key_prefix="src-", natural_key_column="id",
        target_rows_per_partition=250000,
        minimum_partition_threshold=50000,
        maximum_partitions=32
    )
    # 30,000 rows < 50,000 threshold -> 1 partition
    assert meta.calculate_adaptive_partitions(row_count=30000) == 1
    assert meta.calculate_adaptive_partitions(row_count=49999) == 1


def test_adaptive_partitioning_dynamic_calculation():
    """Verify dynamic ceil(source_count / target_rows_per_partition) calculation."""
    meta = MetadataRow(
        id=1, source_tag="src", source_schema="s", source_table="t", source_db_type="oracle",
        landing_table="t", history_table="t", target_table="t", target_schema="public",
        is_active="y", priority=1, load_type="gold", is_cdc="n", cdc_columns=None,
        cdc_p_days=0, delta_days=0, last_success_timestamp=None, is_reconcile="n",
        reconcile_column=None, reconcile_measure="count", chunk_size=100000,
        num_partitions=1, partition_column="id", source_key_prefix="src-", natural_key_column="id",
        target_rows_per_partition=250000,
        minimum_partition_threshold=50000,
        maximum_partitions=32
    )
    # 50,000 rows -> ceil(50000 / 250000) = 1
    assert meta.calculate_adaptive_partitions(row_count=50000) == 1
    
    # 414,264 rows -> ceil(414264 / 250000) = 2
    assert meta.calculate_adaptive_partitions(row_count=414264) == 2
    
    # 1,000,000 rows -> ceil(1000000 / 250000) = 4
    assert meta.calculate_adaptive_partitions(row_count=1000000) == 4


def test_adaptive_partitioning_maximum_cap():
    """Verify max partitions cap is respected for very large datasets."""
    meta = MetadataRow(
        id=1, source_tag="src", source_schema="s", source_table="t", source_db_type="oracle",
        landing_table="t", history_table="t", target_table="t", target_schema="public",
        is_active="y", priority=1, load_type="gold", is_cdc="n", cdc_columns=None,
        cdc_p_days=0, delta_days=0, last_success_timestamp=None, is_reconcile="n",
        reconcile_column=None, reconcile_measure="count", chunk_size=100000,
        num_partitions=1, partition_column="id", source_key_prefix="src-", natural_key_column="id",
        target_rows_per_partition=250000,
        minimum_partition_threshold=50000,
        maximum_partitions=32
    )
    # 50,000,000 rows -> ceil(50000000 / 250000) = 200, capped at max=32
    assert meta.calculate_adaptive_partitions(row_count=50000000) == 32


def test_adaptive_partitioning_manual_override():
    """Verify manual partition override takes precedence over dynamic calculation."""
    meta = MetadataRow(
        id=1, source_tag="src", source_schema="s", source_table="t", source_db_type="oracle",
        landing_table="t", history_table="t", target_table="t", target_schema="public",
        is_active="y", priority=1, load_type="gold", is_cdc="n", cdc_columns=None,
        cdc_p_days=0, delta_days=0, last_success_timestamp=None, is_reconcile="n",
        reconcile_column=None, reconcile_measure="count", chunk_size=100000,
        num_partitions=1, partition_column="id", source_key_prefix="src-", natural_key_column="id",
        target_rows_per_partition=250000,
        minimum_partition_threshold=50000,
        maximum_partitions=32
    )
    # Explicit override = 20 takes precedence
    assert meta.calculate_adaptive_partitions(row_count=414264, override=20) == 20
