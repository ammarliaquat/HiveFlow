"""Verified run-scoped Bronze/Silver artifact persistence helpers.

The project already owns ``metadata.attempt_persistence_manifest`` as the
immutable persistence ledger.  This module extends its use for layer artifacts
without introducing a second metadata table.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Optional

from src.constants import compute_artifact_fingerprint
from src.contracts.claim_models import FrozenArtifactReference
from src.metadata.policy_models import compute_deterministic_fingerprint


def _canonicalize_schema_type(value: Any) -> Any:
    """Retain semantic Spark types while discarding Parquet-unstable details."""
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, list):
        return [_canonicalize_schema_type(item) for item in value]
    if not isinstance(value, dict):
        return value

    schema_type = value.get("type")
    if schema_type == "struct":
        return {
            "type": "struct",
            "fields": sorted(
                (
                    {
                        "name": str(field["name"]),
                        "type": _canonicalize_schema_type(field["type"]),
                    }
                    for field in value.get("fields", [])
                ),
                key=lambda field: field["name"].casefold(),
            ),
        }
    if schema_type == "array":
        return {"type": "array", "elementType": _canonicalize_schema_type(value.get("elementType"))}
    if schema_type == "map":
        return {
            "type": "map",
            "keyType": _canonicalize_schema_type(value.get("keyType")),
            "valueType": _canonicalize_schema_type(value.get("valueType")),
        }
    return {
        key: _canonicalize_schema_type(item)
        for key, item in sorted(value.items())
        if key not in {"metadata", "nullable", "containsNull", "valueContainsNull"}
    }


def dataframe_schema_fingerprint(df: Any) -> str:
    """Hash a storage-stable Spark schema without scanning the DataFrame."""
    schema = getattr(df, "schema", None)
    schema_json = schema.json() if schema is not None and hasattr(schema, "json") else str(schema)
    try:
        schema_value = json.loads(schema_json)
    except (TypeError, json.JSONDecodeError):
        schema_value = schema_json
    return compute_deterministic_fingerprint(
        {"artifact_schema_version": 1, "schema": _canonicalize_schema_type(schema_value)}
    )


def _cursor_json(cursor: Any) -> Optional[Dict[str, Any]]:
    if cursor is None:
        return None
    if isinstance(cursor, dict):
        return cursor
    if hasattr(cursor, "to_dict"):
        return cursor.to_dict()
    return json.loads(cursor) if isinstance(cursor, str) else None


def _row_value(row: Any, index: int, name: str) -> Any:
    """Read a DB-API tuple or mapping row without requiring one cursor style."""
    if isinstance(row, dict):
        return row.get(name)
    return row[index]


def publish_verified_artifact(
    conn_manager: Any,
    *,
    run_id: str,
    table_id: int,
    attempt_number: int,
    source_tag: str,
    target_tag: str,
    source_table: str,
    target_table: str,
    storage_layer: str,
    storage_uri: str,
    storage_format: str,
    row_count: int,
    policy_version_id: str,
    policy_fingerprint: str,
    mapping_fingerprint: str,
    source_key_fingerprint: str,
    schema_fingerprint: str,
    cdc_window_id: Optional[str] = None,
    cdc_lower_cursor: Any = None,
    cdc_upper_cursor: Any = None,
    cdc_state_version: Optional[int] = None,
    batch_log_id: Optional[int] = None,
    artifact_id: Optional[str] = None,
) -> FrozenArtifactReference:
    """Insert one verified artifact identity after read-back validation.

    No mutable metadata is resolved here.  The supplied contract values are
    persisted verbatim and the unique ledger key makes publication idempotent.
    """
    artifact_uuid = uuid.UUID(str(artifact_id)) if artifact_id else uuid.uuid4()
    layer = str(storage_layer).upper()
    fmt = str(storage_format).lower()
    lower = _cursor_json(cdc_lower_cursor)
    upper = _cursor_json(cdc_upper_cursor)
    artifact_fp = compute_artifact_fingerprint(
        storage_layer=layer,
        storage_uri=storage_uri,
        storage_format=fmt,
        row_count=row_count,
        schema_fingerprint=schema_fingerprint,
        policy_fingerprint=policy_fingerprint,
        mapping_fingerprint=mapping_fingerprint,
        source_key_fingerprint=source_key_fingerprint,
        cdc_window_id=cdc_window_id,
    )
    # Keep old reconciliation fields populated for existing readers.
    with conn_manager.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.attempt_persistence_manifest (
                    manifest_id, run_id, table_id, attempt_number,
                    policy_version_id, window_id, storage_layer,
                    storage_object_identity, expected_row_count,
                    expected_key_fingerprint, expected_content_fingerprint,
                    schema_fingerprint, manifest_fingerprint,
                    source_tag, target_tag, source_table, target_table,
                    storage_uri, storage_format, artifact_status,
                    mapping_fingerprint, source_key_fingerprint,
                    policy_fingerprint, artifact_fingerprint,
                    cdc_lower_cursor, cdc_upper_cursor, cdc_state_version,
                    batch_log_id, created_at, verified_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, 'WRITING', %s, %s, %s, %s,
                    %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (run_id, table_id, attempt_number, storage_layer)
                DO NOTHING
                RETURNING manifest_id
                """,
                [
                    str(artifact_uuid), str(run_id), int(table_id), int(attempt_number),
                    str(policy_version_id), str(cdc_window_id) if cdc_window_id else None,
                    layer, str(storage_uri), int(row_count), str(source_key_fingerprint),
                    artifact_fp, str(schema_fingerprint), artifact_fp,
                    str(source_tag), str(target_tag), str(source_table), str(target_table),
                    str(storage_uri), fmt, str(mapping_fingerprint),
                    str(source_key_fingerprint), str(policy_fingerprint), artifact_fp,
                    json.dumps(lower) if lower is not None else None,
                    json.dumps(upper) if upper is not None else None,
                    cdc_state_version, batch_log_id,
                ],
            )
            inserted = cur.fetchone()
            if inserted:
                persisted_id = _row_value(inserted, 0, "manifest_id")
                # The storage object has already passed Spark read-back
                # verification when this helper is called.  Persist the
                # lifecycle transition explicitly so the database trigger
                # enforces the same immutable WRITING -> VERIFIED protocol
                # without introducing another metadata table.
                cur.execute(
                    """
                    UPDATE metadata.attempt_persistence_manifest
                    SET artifact_status = 'VERIFIED', verified_at = CURRENT_TIMESTAMP
                    WHERE manifest_id = %s
                    RETURNING manifest_id
                    """,
                    [str(persisted_id)],
                )
                verified = cur.fetchone()
                if verified:
                    persisted_id = _row_value(verified, 0, "manifest_id")
            else:
                cur.execute(
                    """
                    SELECT manifest_id, storage_uri, storage_format,
                           expected_row_count, policy_version_id,
                           policy_fingerprint, mapping_fingerprint,
                           source_key_fingerprint, schema_fingerprint,
                           artifact_fingerprint, window_id,
                           cdc_lower_cursor, cdc_upper_cursor,
                           cdc_state_version, batch_log_id, artifact_status
                    FROM metadata.attempt_persistence_manifest
                    WHERE run_id = %s AND table_id = %s
                      AND attempt_number = %s AND storage_layer = %s
                    """,
                    [str(run_id), int(table_id), int(attempt_number), layer],
                )
                existing = cur.fetchone()
                if not existing:
                    raise RuntimeError("Artifact manifest conflict could not be resolved")
                existing_values = {
                    "storage_uri": _row_value(existing, 1, "storage_uri"),
                    "storage_format": _row_value(existing, 2, "storage_format"),
                    "row_count": _row_value(existing, 3, "expected_row_count"),
                    "policy_version_id": _row_value(existing, 4, "policy_version_id"),
                    "policy_fingerprint": _row_value(existing, 5, "policy_fingerprint"),
                    "mapping_fingerprint": _row_value(existing, 6, "mapping_fingerprint"),
                    "source_key_fingerprint": _row_value(existing, 7, "source_key_fingerprint"),
                    "schema_fingerprint": _row_value(existing, 8, "schema_fingerprint"),
                    "artifact_fingerprint": _row_value(existing, 9, "artifact_fingerprint"),
                    "cdc_window_id": _row_value(existing, 10, "window_id"),
                    "cdc_lower_cursor": _cursor_json(_row_value(existing, 11, "cdc_lower_cursor")),
                    "cdc_upper_cursor": _cursor_json(_row_value(existing, 12, "cdc_upper_cursor")),
                    "cdc_state_version": _row_value(existing, 13, "cdc_state_version"),
                    "batch_log_id": _row_value(existing, 14, "batch_log_id"),
                    "artifact_status": _row_value(existing, 15, "artifact_status"),
                }
                expected_values = {
                    "storage_uri": str(storage_uri),
                    "storage_format": fmt,
                    "row_count": int(row_count),
                    "policy_version_id": str(policy_version_id),
                    "policy_fingerprint": str(policy_fingerprint),
                    "mapping_fingerprint": str(mapping_fingerprint),
                    "source_key_fingerprint": str(source_key_fingerprint),
                    "schema_fingerprint": str(schema_fingerprint),
                    "artifact_fingerprint": artifact_fp,
                    "cdc_window_id": str(cdc_window_id) if cdc_window_id else None,
                    "cdc_lower_cursor": lower,
                    "cdc_upper_cursor": upper,
                    "cdc_state_version": cdc_state_version,
                    "batch_log_id": batch_log_id,
                    "artifact_status": "VERIFIED",
                }
                mismatches = [
                    name for name, expected in expected_values.items()
                    if (existing_values[name] != expected)
                ]
                if mismatches:
                    raise RuntimeError(
                        "Artifact manifest conflict has different immutable fields: "
                        + ", ".join(mismatches)
                    )
                persisted_id = _row_value(existing, 0, "manifest_id")
        conn.commit()
    return FrozenArtifactReference(
        artifact_id=str(persisted_id),
        producer_run_id=str(run_id),
        producer_attempt_number=int(attempt_number),
        storage_layer=layer,
        storage_uri=str(storage_uri),
        storage_format=fmt,
        row_count=int(row_count),
        policy_version_id=str(policy_version_id),
        policy_fingerprint=str(policy_fingerprint),
        mapping_fingerprint=str(mapping_fingerprint),
        source_key_fingerprint=str(source_key_fingerprint),
        schema_fingerprint=str(schema_fingerprint),
        artifact_fingerprint=artifact_fp,
        cdc_window_id=str(cdc_window_id) if cdc_window_id else None,
        cdc_lower_cursor=lower,
        cdc_upper_cursor=upper,
        cdc_state_version=cdc_state_version,
        batch_log_id=batch_log_id,
    )


def load_verified_artifact(
    conn_manager: Any,
    *,
    artifact_id: Optional[str] = None,
    run_id: Optional[str] = None,
    table_id: Optional[int] = None,
    storage_layer: Optional[str] = None,
) -> FrozenArtifactReference:
    """Load exactly one verified artifact and confirm its producer succeeded."""
    predicates = [
        "m.artifact_status = 'VERIFIED'",
        "m.batch_log_id IS NOT NULL",
        "b.status IN ('SUCCESS', 'RECONCILIATION_WARNING')",
    ]
    params = []
    if artifact_id:
        predicates.append("m.manifest_id = %s")
        params.append(str(artifact_id))
    if run_id:
        predicates.append("m.run_id = %s")
        params.append(str(run_id))
    if table_id is not None:
        predicates.append("m.table_id = %s")
        params.append(int(table_id))
    if storage_layer:
        predicates.append("UPPER(m.storage_layer) = %s")
        params.append(str(storage_layer).upper())
    sql = f"""
        SELECT m.manifest_id, m.run_id, m.attempt_number, m.storage_layer,
               m.storage_uri, m.storage_format, m.expected_row_count,
               m.policy_version_id, m.policy_fingerprint, m.mapping_fingerprint,
               m.source_key_fingerprint, m.schema_fingerprint,
               m.artifact_fingerprint, m.window_id, m.cdc_lower_cursor,
               m.cdc_upper_cursor, m.cdc_state_version, m.batch_log_id,
               b.status
        FROM metadata.attempt_persistence_manifest m
        JOIN metadata.batch_log b
          ON b.id = m.batch_log_id
         AND b.run_id = m.run_id
         AND LOWER(b.load_type) = LOWER(m.storage_layer)
         AND LOWER(b.source_tag) = LOWER(m.source_tag)
         AND LOWER(b.target_tag) = LOWER(m.target_tag)
         AND LOWER(b.source_table) = LOWER(m.source_table)
         AND LOWER(b.target_table) = LOWER(m.target_table)
        WHERE {' AND '.join(predicates)}
        ORDER BY m.created_at DESC
        LIMIT 1
    """
    with conn_manager.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    if not row:
        raise ValueError("Verified layer artifact not found for the frozen reference")
    producer_status = _row_value(row, 18, "status")
    if producer_status not in ("SUCCESS", "RECONCILIATION_WARNING"):
        raise ValueError(f"Layer artifact producer is not successful: {producer_status}")
    return FrozenArtifactReference(
        artifact_id=str(_row_value(row, 0, "manifest_id")),
        producer_run_id=str(_row_value(row, 1, "run_id")),
        producer_attempt_number=int(_row_value(row, 2, "attempt_number")),
        storage_layer=str(_row_value(row, 3, "storage_layer")).upper(),
        storage_uri=str(_row_value(row, 4, "storage_uri")),
        storage_format=str(_row_value(row, 5, "storage_format") or "parquet").lower(),
        row_count=int(_row_value(row, 6, "expected_row_count")),
        policy_version_id=str(_row_value(row, 7, "policy_version_id")),
        policy_fingerprint=str(_row_value(row, 8, "policy_fingerprint") or ""),
        mapping_fingerprint=str(_row_value(row, 9, "mapping_fingerprint") or ""),
        source_key_fingerprint=str(_row_value(row, 10, "source_key_fingerprint") or ""),
        schema_fingerprint=str(_row_value(row, 11, "schema_fingerprint") or ""),
        artifact_fingerprint=str(_row_value(row, 12, "artifact_fingerprint") or ""),
        cdc_window_id=str(_row_value(row, 13, "window_id")) if _row_value(row, 13, "window_id") else None,
        cdc_lower_cursor=(
            _row_value(row, 14, "cdc_lower_cursor")
            if isinstance(_row_value(row, 14, "cdc_lower_cursor"), dict)
            else (_cursor_json(_row_value(row, 14, "cdc_lower_cursor")) if _row_value(row, 14, "cdc_lower_cursor") else None)
        ),
        cdc_upper_cursor=(
            _row_value(row, 15, "cdc_upper_cursor")
            if isinstance(_row_value(row, 15, "cdc_upper_cursor"), dict)
            else (_cursor_json(_row_value(row, 15, "cdc_upper_cursor")) if _row_value(row, 15, "cdc_upper_cursor") else None)
        ),
        cdc_state_version=(int(_row_value(row, 16, "cdc_state_version")) if _row_value(row, 16, "cdc_state_version") is not None else None),
        batch_log_id=(int(_row_value(row, 17, "batch_log_id")) if _row_value(row, 17, "batch_log_id") is not None else None),
    )
