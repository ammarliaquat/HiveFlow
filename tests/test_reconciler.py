# tests/test_reconciler.py
import pytest
from unittest.mock import MagicMock
from src.validation.reconciler import Reconciler
from src.constants import RunStatus

def test_count_and_cache(spark):
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    count = Reconciler.count_and_cache(df)
    assert count == 2
    assert df.is_cached is True

def test_reconcile_disabled():
    meta = MagicMock(is_reconcile="n")
    status = Reconciler.reconcile(100, 95, "bronze", meta)
    assert status == RunStatus.SUCCESS

def test_reconcile_success_exact():
    meta = MagicMock(is_reconcile="y", reconcile_threshold=0.0, reconcile_action="FAIL", source_table="test")
    status = Reconciler.reconcile(100, 100, "bronze", meta)
    assert status == RunStatus.SUCCESS

def test_reconcile_within_threshold():
    # 0.5% threshold
    # 1000 source, 996 target -> discrepancy is 4/1000 * 100 = 0.4%
    meta = MagicMock(is_reconcile="y", reconcile_threshold=0.5, reconcile_action="FAIL", source_table="test")
    status = Reconciler.reconcile(1000, 996, "bronze", meta)
    assert status == RunStatus.SUCCESS

def test_reconcile_exceeds_threshold_fail():
    # 0.5% threshold
    # 1000 source, 994 target -> discrepancy is 6/1000 * 100 = 0.6%
    meta = MagicMock(is_reconcile="y", reconcile_threshold=0.5, reconcile_action="FAIL", source_table="test")
    with pytest.raises(RuntimeError) as exc_info:
        Reconciler.reconcile(1000, 994, "bronze", meta)
    assert "Reconciliation failed" in str(exc_info.value)

def test_reconcile_exceeds_threshold_warn():
    meta = MagicMock(is_reconcile="y", reconcile_threshold=0.5, reconcile_action="WARN", source_table="test")
    status = Reconciler.reconcile(1000, 994, "bronze", meta)
    assert status == RunStatus.RECONCILIATION_WARNING


def test_certify_medallion_transition_fails_on_destroyed_rows():
    """Verify certify_medallion_transition raises RuntimeError if deduplication destroyed unique rows."""
    # duplicates_removed = 300, input_count = 1000, distinct_natural_key_count = 1000 (all unique!)
    with pytest.raises(RuntimeError) as exc_info:
        Reconciler.certify_medallion_transition(
            input_count=1000,
            output_count=700,
            duplicates_removed=300,
            distinct_natural_key_count=1000,
            layer="SILVER",
            table_name="tblop_jandec25100626"
        )
    assert "Execution Certification FAILED" in str(exc_info.value)
    assert "actual row loss=300 does not match expected loss=0" in str(exc_info.value)


def test_reconcile_with_expected_loss_success():
    """Verify Silver reconciliation succeeds when target equals source minus certified expected duplicates."""
    meta = MagicMock(is_reconcile="y", reconcile_threshold=0.0, reconcile_action="FAIL", source_table="tblop_dec24")
    # 3,548,812 read, 18 duplicates removed, 3,548,794 written
    status = Reconciler.reconcile(
        source_count=3548812,
        target_count=3548794,
        layer="silver",
        meta=meta,
        expected_loss=18,
    )
    assert status == RunStatus.SUCCESS


def test_reconcile_with_expected_loss_exceeds_threshold():
    """Verify Silver reconciliation fails when write loss exceeds certified expected loss."""
    meta = MagicMock(is_reconcile="y", reconcile_threshold=0.0, reconcile_action="FAIL", source_table="tblop_dec24")
    # 3,548,812 read, 18 expected lost, but only 3,548,700 written (additional 94 rows lost)
    with pytest.raises(RuntimeError) as exc_info:
        Reconciler.reconcile(
            source_count=3548812,
            target_count=3548700,
            layer="silver",
            meta=meta,
            expected_loss=18,
        )
    assert "Reconciliation failed for tblop_dec24 (silver)" in str(exc_info.value)


def test_reconcile_with_expected_loss_overflow_fails():
    """Verify Silver reconciliation flags overflow when target count exceeds expected deduplicated count."""
    meta = MagicMock(is_reconcile="y", reconcile_threshold=0.0, reconcile_action="FAIL", source_table="tblop_dec24")
    # Expected target is 3,548,794, but target is 3,548,800 (runaway append/duplicate write)
    with pytest.raises(RuntimeError) as exc_info:
        Reconciler.reconcile(
            source_count=3548812,
            target_count=3548800,
            layer="silver",
            meta=meta,
            expected_loss=18,
        )
    assert "target > source" in str(exc_info.value)
