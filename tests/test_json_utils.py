import json
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest


def test_json_safe_dumps_normalizes_nested_numpy_scalars():
    np = pytest.importorskip("numpy")
    from src.utils.json_utils import json_safe_dumps

    payload = {
        "target_reconciliation": {
            "staged_rows": np.int64(633967),
            "ratio": np.float64(100.0),
            "target_observed": np.bool_(True),
        }
    }

    encoded = json_safe_dumps(payload, sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded == {
        "target_reconciliation": {
            "ratio": 100.0,
            "staged_rows": 633967,
            "target_observed": True,
        }
    }
    assert type(decoded["target_reconciliation"]["staged_rows"]) is int
    assert type(decoded["target_reconciliation"]["target_observed"]) is bool


def test_json_safe_dumps_handles_supported_control_plane_types():
    from src.utils.json_utils import json_safe_dumps

    payload = {
        "amount": Decimal("1141.57"),
        "business_date": date(2026, 9, 17),
        "captured_at": datetime(2026, 9, 17, 12, 17, 9, tzinfo=timezone.utc),
        "keys": ("emigrant_id",),
    }

    assert json.loads(json_safe_dumps(payload)) == {
        "amount": "1141.57",
        "business_date": "2026-09-17",
        "captured_at": "2026-09-17T12:17:09+00:00",
        "keys": ["emigrant_id"],
    }


def test_json_safe_dumps_rejects_unknown_objects():
    from src.utils.json_utils import json_safe_dumps

    with pytest.raises(TypeError, match="not JSON serializable"):
        json_safe_dumps({"unsafe": object()})


def test_governance_fingerprint_accepts_numpy_counts():
    np = pytest.importorskip("numpy")
    from src.metadata.policy_models import compute_deterministic_fingerprint

    fingerprint = compute_deterministic_fingerprint({"expected_row_count": np.int64(633967)})

    assert len(fingerprint) == 64


def test_persistence_manifest_stores_python_integer_counts():
    np = pytest.importorskip("numpy")
    from uuid import uuid4

    from src.metadata.policy_models import AttemptPersistenceManifest

    manifest = AttemptPersistenceManifest.build(
        run_id=str(uuid4()),
        table_id=np.int64(179),
        attempt_number=np.int64(1),
        policy_version_id=str(uuid4()),
        window_id=uuid4(),
        storage_layer="target",
        storage_object_identity="landing.tblop_jandec25100626",
        expected_row_count=np.int64(633967),
        expected_key_fingerprint="evidence-fingerprint",
        schema_fingerprint="schema-fingerprint",
    )

    assert type(manifest.table_id) is int
    assert type(manifest.attempt_number) is int
    assert type(manifest.expected_row_count) is int


def test_batch_logger_persists_numpy_reconciliation_evidence_as_json():
    np = pytest.importorskip("numpy")
    from src.logging_.batch_logger import BatchLogger

    logger = object.__new__(BatchLogger)
    logger._attempt_numbers = {17: 1}
    logger._conn_manager = MagicMock()
    connection = logger._conn_manager.get_metadata_db_connection.return_value.__enter__.return_value
    cursor = connection.cursor.return_value.__enter__.return_value

    logger.update_stage_metrics(
        17,
        {"target_reconciliation": {"matched_rows": np.int64(633967)}},
    )

    cursor.execute.assert_called_once()
    encoded_payload = cursor.execute.call_args.args[1][0]
    assert json.loads(encoded_payload) == {
        "target_reconciliation": {"matched_rows": 633967}
    }
