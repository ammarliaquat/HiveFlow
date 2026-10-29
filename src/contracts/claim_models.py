# src/contracts/claim_models.py
"""
Authoritative claim and frozen-contract data models.

These are plain dataclasses with no external dependencies — safe to import
anywhere in the execution chain without circular-import risk.

Security note:
    ExecutionFence.raw_token is held in process memory only.
    It is excluded from __repr__, __str__, to_dict(), and all JSON
    serialization. Only fencing_token_digest (SHA-256 of raw_token)
    is persisted to the database or written to logs.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec

FROZEN_JOB_CONTRACT_VERSION = "frozen_job_execution.v2"
LEGACY_FROZEN_JOB_CONTRACT_VERSIONS = frozenset({"frozen_job_execution.v1"})


@dataclass(frozen=True)
class ExecutionFence:
    """
    Proof-of-lease token returned by ScopeClaimManager after a successful
    RESERVED → ACTIVE or RECOVERY_REQUIRED → ACTIVE CAS transition.

    All fields are sourced directly from the database RETURNING clause;
    none are caller-supplied. The fence is immutable once issued.

    Security: raw_token is intentionally excluded from __repr__ and
    any serialization path to prevent accidental exposure in logs,
    telemetry, or persisted parameters.
    """
    run_id: str
    table_id: int
    claim_id: int
    scope_action: str           # immutable intent column (post-045: scope_action)
    lease_revision: int
    fencing_epoch: int
    fencing_token_digest: str   # 64-char lowercase hex SHA-256 of raw_token; persisted
    raw_token: str              # 64-char hex; held in process memory only; never persisted
    owner_instance_id: str
    worker_attempt_number: int
    lease_expires_at: datetime

    def __repr__(self) -> str:
        # Explicitly omit raw_token from all string representations
        return (
            f"ExecutionFence(run_id={self.run_id!r}, claim_id={self.claim_id}, "
            f"epoch={self.fencing_epoch}, revision={self.lease_revision}, "
            f"owner={self.owner_instance_id!r})"
        )

    def to_dict(self) -> dict:
        """Serializable snapshot — raw_token excluded."""
        return {
            "run_id": self.run_id,
            "table_id": self.table_id,
            "claim_id": self.claim_id,
            "scope_action": self.scope_action,
            "lease_revision": self.lease_revision,
            "fencing_epoch": self.fencing_epoch,
            "fencing_token_digest": self.fencing_token_digest,
            # raw_token intentionally omitted
            "owner_instance_id": self.owner_instance_id,
            "worker_attempt_number": self.worker_attempt_number,
        }


@dataclass(frozen=True)
class FrozenArtifactReference:
    """Immutable reference to a verified upstream Bronze/Silver artifact."""

    artifact_id: str
    producer_run_id: str
    producer_attempt_number: int
    storage_layer: str
    storage_uri: str
    storage_format: str
    row_count: int
    policy_version_id: str
    policy_fingerprint: str
    mapping_fingerprint: str
    source_key_fingerprint: str
    schema_fingerprint: str
    artifact_fingerprint: str
    cdc_window_id: Optional[str] = None
    cdc_lower_cursor: Optional[Dict[str, Any]] = None
    cdc_upper_cursor: Optional[Dict[str, Any]] = None
    cdc_state_version: Optional[int] = None
    batch_log_id: Optional[int] = None

    def __post_init__(self) -> None:
        if not str(self.artifact_id).strip() or not str(self.storage_uri).strip():
            raise ValueError("FrozenArtifactReference requires artifact_id and storage_uri")
        if not str(self.producer_run_id).strip():
            raise ValueError("FrozenArtifactReference requires producer_run_id")
        if str(self.storage_layer).upper() not in {"BRONZE", "SILVER"}:
            raise ValueError("FrozenArtifactReference storage_layer must be BRONZE or SILVER")
        if int(self.row_count) < 0:
            raise ValueError("FrozenArtifactReference row_count must be non-negative")

    def to_dict(self) -> dict:
        return {
            "artifact_id": str(self.artifact_id),
            "producer_run_id": str(self.producer_run_id),
            "producer_attempt_number": int(self.producer_attempt_number),
            "storage_layer": str(self.storage_layer).upper(),
            "storage_uri": str(self.storage_uri),
            "storage_format": str(self.storage_format).lower(),
            "row_count": int(self.row_count),
            "policy_version_id": str(self.policy_version_id),
            "policy_fingerprint": str(self.policy_fingerprint),
            "mapping_fingerprint": str(self.mapping_fingerprint),
            "source_key_fingerprint": str(self.source_key_fingerprint),
            "schema_fingerprint": str(self.schema_fingerprint),
            "artifact_fingerprint": str(self.artifact_fingerprint),
            "cdc_window_id": self.cdc_window_id,
            "cdc_lower_cursor": self.cdc_lower_cursor,
            "cdc_upper_cursor": self.cdc_upper_cursor,
            "cdc_state_version": self.cdc_state_version,
            "batch_log_id": self.batch_log_id,
        }

    @classmethod
    def from_dict(cls, value: dict) -> "FrozenArtifactReference":
        required = {
            "artifact_id", "producer_run_id", "producer_attempt_number",
            "storage_layer", "storage_uri", "storage_format", "row_count",
            "policy_version_id", "policy_fingerprint", "mapping_fingerprint",
            "source_key_fingerprint", "schema_fingerprint", "artifact_fingerprint",
        }
        missing = sorted(required - set(value or {}))
        if missing:
            raise ValueError(f"FrozenArtifactReference missing required fields: {missing}")
        return cls(
            artifact_id=str(value["artifact_id"]),
            producer_run_id=str(value["producer_run_id"]),
            producer_attempt_number=int(value["producer_attempt_number"]),
            storage_layer=str(value["storage_layer"]).upper(),
            storage_uri=str(value["storage_uri"]),
            storage_format=str(value["storage_format"]).lower(),
            row_count=int(value["row_count"]),
            policy_version_id=str(value["policy_version_id"]),
            policy_fingerprint=str(value["policy_fingerprint"]),
            mapping_fingerprint=str(value["mapping_fingerprint"]),
            source_key_fingerprint=str(value["source_key_fingerprint"]),
            schema_fingerprint=str(value["schema_fingerprint"]),
            artifact_fingerprint=str(value["artifact_fingerprint"]),
            cdc_window_id=value.get("cdc_window_id"),
            cdc_lower_cursor=value.get("cdc_lower_cursor"),
            cdc_upper_cursor=value.get("cdc_upper_cursor"),
            cdc_state_version=(int(value["cdc_state_version"]) if value.get("cdc_state_version") is not None else None),
            batch_log_id=(int(value["batch_log_id"]) if value.get("batch_log_id") is not None else None),
        )


@dataclass
class FrozenTableEntry:
    """
    Snapshot of a single table's immutable execution parameters as recorded
    in gui_job_registry.parameters at job submission time.

    All fields are resolved at submission time from authoritative metadata
    and frozen. Workers use this to reconstruct execution context without
    any mutable metadata re-resolution.

    Mapping/business-key payloads are referenced by their immutable version IDs.
    Workers load the payload by version_id — version records are append-only
    and immutable once written, making this a safe non-mutable read.
    """
    # Core identity
    table_id: int
    source_tag: str
    source_table: str
    target_tag: str
    target_schema: Optional[str]
    target_table: str

    # Execution control
    priority: int
    authority_mode: str          # 'GOVERNED' | 'LEGACY' | 'BLOCKED'

    # Policy version (immutable at submission time)
    policy_version_id: str       # references metadata.policy_version
    policy_fingerprint: str      # SHA-256 of the versioned policy record

    # Column mapping (immutable version reference)
    column_mapping_version_id: str
    column_mapping_fingerprint: str   # SHA-256; worker validates on load

    # Business key (immutable version reference + frozen columns)
    business_key_version_id: str
    business_key_fingerprint: str     # SHA-256
    business_key_columns: List[str]   # actual columns — small, safe to freeze

    # Deployment profile
    deployment_profile_fingerprint: str

    # Frozen execution policy
    source_schema: Optional[str] = None
    source_db_type: str = ""
    landing_table: Optional[str] = None
    history_table: Optional[str] = None
    load_type: str = "full"
    is_cdc: str = "n"
    cdc_columns: Optional[str] = None
    cdc_strategy: Optional[str] = None
    cdc_p_days: int = 0
    delta_days: int = 0
    last_success_timestamp: Optional[datetime] = None
    last_success_date: Optional[datetime] = None
    rerun_flag: str = "n"
    partition_lower_bound: Optional[int] = None
    partition_upper_bound: Optional[int] = None
    target_rows_per_partition: int = 250000
    minimum_partition_threshold: int = 50000
    maximum_partitions: int = 32
    is_reconcile: str = "n"
    reconcile_threshold: float = 0.0
    reconcile_action: str = "FAIL"
    contract_payload: Optional[Dict[str, Any]] = None
    upstream_artifact: Optional[FrozenArtifactReference] = None
    # Conditional identity fields.  ``None`` means legacy inference: a
    # non-empty business key is EXACT_KEY, otherwise KEYLESS.
    identity_mode: Optional[str] = None
    conditional_exception_values_payload: Optional[Dict[str, Any]] = None
    conditional_exception_fingerprint: Optional[str] = None
    frozen_capability_evidence: Optional[Dict[str, Any]] = None
    row_hash_codec_version: Optional[str] = None
    landing_mode: str = "METADATA"

    def __post_init__(self) -> None:
        columns = tuple(str(column).strip() for column in (self.business_key_columns or ()))
        if any(not column for column in columns):
            raise ValueError("FrozenTableEntry business key columns cannot be empty")
        if len({column.lower() for column in columns}) != len(columns):
            raise ValueError("FrozenTableEntry business key columns cannot contain duplicates")
        object.__setattr__(self, "business_key_columns", list(columns))
        raw_mode = self.identity_mode
        if raw_mode is None or not str(raw_mode).strip():
            mode = IdentityMode.EXACT_KEY if columns else IdentityMode.KEYLESS
        else:
            try:
                mode = raw_mode if isinstance(raw_mode, IdentityMode) else IdentityMode(str(raw_mode).strip().upper())
            except ValueError as exc:
                raise ValueError(f"Unsupported frozen identity mode: {raw_mode!r}") from exc
        if mode is IdentityMode.EXACT_KEY and not columns:
            raise ValueError("EXACT_KEY frozen identity requires business key columns")
        payload = self.conditional_exception_values_payload
        if isinstance(payload, str) and payload.strip():
            try:
                payload = json.loads(payload)
            except (TypeError, ValueError) as exc:
                raise ValueError("Invalid frozen conditional exception payload") from exc
        values = Int64ExceptionCodec.decode(payload) if payload is not None else ()
        if mode is IdentityMode.BOUNDED_EXCEPTION_KEY:
            if not columns:
                raise ValueError("BOUNDED_EXCEPTION_KEY requires business key columns")
            if len(columns) != 1:
                raise ValueError("BOUNDED_EXCEPTION_KEY requires one business key column")
            if not values:
                raise ValueError("BOUNDED_EXCEPTION_KEY requires exception values")
        elif values:
            raise ValueError(
                "Conditional exception values require BOUNDED_EXCEPTION_KEY identity"
            )
        exception_fingerprint = Int64ExceptionCodec.fingerprint(values) if values else None
        if self.conditional_exception_fingerprint and self.conditional_exception_fingerprint != exception_fingerprint:
            raise ValueError("Frozen conditional exception fingerprint does not match values")
        object.__setattr__(self, "identity_mode", mode.value)
        object.__setattr__(
            self,
            "conditional_exception_values_payload",
            Int64ExceptionCodec.encode(values) if values else None,
        )
        object.__setattr__(self, "conditional_exception_fingerprint", exception_fingerprint)

    @staticmethod
    def _serialize_dt(value):
        return value.isoformat() if isinstance(value, datetime) else value

    @staticmethod
    def _parse_dt(value):
        if value is None or isinstance(value, datetime):
            return value
        if isinstance(value, str) and value.strip():
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return None

    def to_dict(self, *, include_conditional_fields: bool = True) -> dict:
        payload = {
            "table_id": self.table_id,
            "source_tag": self.source_tag,
            "source_table": self.source_table,
            "target_tag": self.target_tag,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "priority": self.priority,
            "authority_mode": self.authority_mode,
            "policy_version_id": self.policy_version_id,
            "policy_fingerprint": self.policy_fingerprint,
            "column_mapping_version_id": self.column_mapping_version_id,
            "column_mapping_fingerprint": self.column_mapping_fingerprint,
            "business_key_version_id": self.business_key_version_id,
            "business_key_fingerprint": self.business_key_fingerprint,
            "business_key_columns": self.business_key_columns,
            "deployment_profile_fingerprint": self.deployment_profile_fingerprint,
            "source_schema": self.source_schema,
            "source_db_type": self.source_db_type,
            "landing_table": self.landing_table,
            "history_table": self.history_table,
            "load_type": self.load_type,
            "is_cdc": self.is_cdc,
            "cdc_columns": self.cdc_columns,
            "cdc_strategy": self.cdc_strategy,
            "cdc_p_days": self.cdc_p_days,
            "delta_days": self.delta_days,
            "last_success_timestamp": self._serialize_dt(self.last_success_timestamp),
            "last_success_date": self._serialize_dt(self.last_success_date),
            "rerun_flag": self.rerun_flag,
            "partition_lower_bound": self.partition_lower_bound,
            "partition_upper_bound": self.partition_upper_bound,
            "target_rows_per_partition": self.target_rows_per_partition,
            "minimum_partition_threshold": self.minimum_partition_threshold,
            "maximum_partitions": self.maximum_partitions,
            "is_reconcile": self.is_reconcile,
            "reconcile_threshold": self.reconcile_threshold,
            "reconcile_action": self.reconcile_action,
            "contract_payload": self.contract_payload or {},
            "upstream_artifact": self.upstream_artifact.to_dict() if self.upstream_artifact else None,
            "landing_mode": getattr(self, "landing_mode", "METADATA") or "METADATA",
        }
        if include_conditional_fields:
            payload.update({
                "identity_mode": self.identity_mode,
                "conditional_exception_values_payload": self.conditional_exception_values_payload,
                "conditional_exception_fingerprint": self.conditional_exception_fingerprint,
                "frozen_capability_evidence": self.frozen_capability_evidence,
                "row_hash_codec_version": self.row_hash_codec_version,
            })
        return payload

    @classmethod
    def from_dict(cls, d: dict) -> "FrozenTableEntry":
        required = [
            "table_id", "source_tag", "source_table", "target_tag", "target_table",
            "priority", "authority_mode",
            "policy_version_id", "policy_fingerprint",
            "column_mapping_version_id", "column_mapping_fingerprint",
            "business_key_version_id", "business_key_fingerprint", "business_key_columns",
            "deployment_profile_fingerprint",
        ]
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(
                f"FrozenTableEntry.from_dict: missing required fields: {missing}"
            )
        invalid_policy_fields = [
            field_name
            for field_name in ("policy_version_id", "policy_fingerprint")
            if d.get(field_name) is None
            or not str(d.get(field_name)).strip()
            or str(d.get(field_name)).strip().lower() in {"none", "null"}
        ]
        if invalid_policy_fields:
            raise ValueError(
                "FrozenTableEntry.from_dict: governed policy binding is missing: "
                f"{invalid_policy_fields}"
            )
        conditional_exception_payload = (
            d.get("conditional_exception_values_payload")
            if d.get("conditional_exception_values_payload") is not None
            else d.get("exception_values_payload")
        )
        if conditional_exception_payload is None:
            raw_exception_values = (
                d.get("exception_key_values")
                if d.get("exception_key_values") is not None
                else d.get("exception_values")
            )
            if raw_exception_values is None:
                raw_exception_values = d.get("conditional_exception_values")
            if raw_exception_values is not None:
                conditional_exception_payload = Int64ExceptionCodec.encode(raw_exception_values)
        return cls(
            table_id=int(d["table_id"]),
            source_tag=str(d["source_tag"]),
            source_table=str(d["source_table"]),
            target_tag=str(d["target_tag"]),
            target_schema=d.get("target_schema"),
            target_table=str(d["target_table"]),
            priority=int(d["priority"]),
            authority_mode=str(d["authority_mode"]),
            policy_version_id=str(d["policy_version_id"]),
            policy_fingerprint=str(d["policy_fingerprint"]),
            column_mapping_version_id=str(d["column_mapping_version_id"]),
            column_mapping_fingerprint=str(d["column_mapping_fingerprint"]),
            business_key_version_id=str(d["business_key_version_id"]),
            business_key_fingerprint=str(d["business_key_fingerprint"]),
            business_key_columns=[str(c) for c in d["business_key_columns"]],
            deployment_profile_fingerprint=str(d["deployment_profile_fingerprint"]),
            source_schema=d.get("source_schema"),
            source_db_type=str(d.get("source_db_type") or ""),
            landing_table=d.get("landing_table"),
            history_table=d.get("history_table"),
            load_type=str(d.get("load_type") or "full"),
            is_cdc=str(d.get("is_cdc") or "n").lower(),
            cdc_columns=d.get("cdc_columns"),
            cdc_strategy=d.get("cdc_strategy"),
            cdc_p_days=int(d.get("cdc_p_days") or 0),
            delta_days=int(d.get("delta_days") or 0),
            last_success_timestamp=cls._parse_dt(d.get("last_success_timestamp")),
            last_success_date=cls._parse_dt(d.get("last_success_date")),
            rerun_flag=str(d.get("rerun_flag") or "n").lower(),
            partition_lower_bound=d.get("partition_lower_bound"),
            partition_upper_bound=d.get("partition_upper_bound"),
            target_rows_per_partition=int(d.get("target_rows_per_partition") or 250000),
            minimum_partition_threshold=int(d.get("minimum_partition_threshold") or 50000),
            maximum_partitions=int(d.get("maximum_partitions") or 32),
            is_reconcile=str(d.get("is_reconcile") or "n").lower(),
            reconcile_threshold=float(d.get("reconcile_threshold") or 0.0),
            reconcile_action=str(d.get("reconcile_action") or "FAIL").upper(),
            contract_payload=d.get("contract_payload") or {},
            upstream_artifact=(
                FrozenArtifactReference.from_dict(d["upstream_artifact"])
                if d.get("upstream_artifact") else None
            ),
            identity_mode=d.get("identity_mode"),
            conditional_exception_values_payload=conditional_exception_payload,
            conditional_exception_fingerprint=(
                d.get("conditional_exception_fingerprint")
                if d.get("conditional_exception_fingerprint") is not None
                else (
                    d.get("exception_evidence_fingerprint")
                    if d.get("exception_evidence_fingerprint") is not None
                    else d.get("exception_values_fingerprint")
                )
            ),
            frozen_capability_evidence=d.get("frozen_capability_evidence"),
            row_hash_codec_version=d.get("row_hash_codec_version"),
            landing_mode=str(d.get("landing_mode") or "METADATA"),
        )


@dataclass
class FrozenJobExecution:
    """
    Deserialized snapshot of the parameters JSON stored in
    metadata.gui_job_registry.parameters at job-submission time.

    Workers use this to reconstruct execution context without re-reading
    any mutable metadata rows mid-run.

    parent_contract_fingerprint is the SHA-256 of the frozen contract JSON
    (excluding the fingerprint field itself). Workers validate this on load.
    """
    run_id: str
    action: str
    parent_contract_fingerprint: str    # SHA-256 of contract JSON sans this field
    deployment_profile_fingerprint: str
    tables: List[FrozenTableEntry] = field(default_factory=list)
    contract_version: str = FROZEN_JOB_CONTRACT_VERSION
    retry_parent_run_id: Optional[str] = None
    execution_scope: str = "FULL"   # FULL | BRONZE | SILVER | GOLD
    _legacy_execution_scope_present: Optional[bool] = field(
        default=None, repr=False, compare=False
    )

    def to_dict(self) -> dict:
        is_legacy = self.contract_version in LEGACY_FROZEN_JOB_CONTRACT_VERSIONS
        payload = {
            "contract_version": self.contract_version,
            "run_id": self.run_id,
            "action": self.action,
            "parent_contract_fingerprint": self.parent_contract_fingerprint,
            "deployment_profile_fingerprint": self.deployment_profile_fingerprint,
            "tables": [
                t.to_dict(include_conditional_fields=not is_legacy)
                for t in self.tables
            ],
        }
        if not is_legacy or self._legacy_execution_scope_present is not False:
            payload["execution_scope"] = self.execution_scope
        if self.retry_parent_run_id:
            payload["retry_parent_run_id"] = self.retry_parent_run_id
        return payload

    @staticmethod
    def compute_fingerprint(contract_dict: dict) -> str:
        payload = dict(contract_dict)
        payload.pop("parent_contract_fingerprint", None)
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        action: str,
        deployment_profile_fingerprint: str,
        tables: List[FrozenTableEntry],
        retry_parent_run_id: Optional[str] = None,
        execution_scope: str = "FULL",
    ) -> "FrozenJobExecution":
        from src.constants import normalize_execution_scope
        scope = normalize_execution_scope(execution_scope)
        contract = cls(
            contract_version=FROZEN_JOB_CONTRACT_VERSION,
            run_id=str(run_id),
            action=str(action),
            parent_contract_fingerprint="",
            deployment_profile_fingerprint=str(deployment_profile_fingerprint),
            tables=list(tables),
            retry_parent_run_id=str(retry_parent_run_id) if retry_parent_run_id else None,
            execution_scope=scope,
        )
        contract.parent_contract_fingerprint = cls.compute_fingerprint(contract.to_dict())
        return contract

    @classmethod
    def from_dict(cls, d: dict) -> "FrozenJobExecution":
        """
        Deserializes from the JSON dict stored in gui_job_registry.parameters.
        Raises ValueError on any missing required field — no silent defaults.
        """
        from src.constants import normalize_execution_scope
        required_top = ["contract_version", "run_id", "action", "parent_contract_fingerprint",
                        "deployment_profile_fingerprint", "tables"]
        missing = [k for k in required_top if k not in d]
        if missing:
            raise ValueError(
                f"FrozenJobExecution.from_dict: missing required fields: {missing}"
            )
        if d["contract_version"] not in (
            FROZEN_JOB_CONTRACT_VERSION,
            *LEGACY_FROZEN_JOB_CONTRACT_VERSIONS,
        ):
            raise ValueError(
                "FrozenJobExecution.from_dict: unsupported contract_version "
                f"{d['contract_version']!r}"
            )
        if d["action"] != "run_governed":
            raise ValueError(
                f"FrozenJobExecution.from_dict: action must be run_governed, got {d['action']!r}"
            )
        # Contracts created before execution_scope existed remain FULL.  An
        # explicit blank value is rejected instead of silently becoming FULL.
        scope = normalize_execution_scope("FULL" if "execution_scope" not in d else d["execution_scope"])
        actual_fingerprint = cls.compute_fingerprint(d)
        if actual_fingerprint != d["parent_contract_fingerprint"]:
            raise ValueError(
                "FrozenJobExecution.from_dict: parent_contract_fingerprint mismatch"
            )
        tables = [FrozenTableEntry.from_dict(t) for t in d["tables"]]
        retry_parent_run_id = d.get("retry_parent_run_id")
        if retry_parent_run_id is not None and not isinstance(retry_parent_run_id, str):
            raise ValueError("FrozenJobExecution.from_dict: retry_parent_run_id must be a string")
        if not tables:
            raise ValueError("FrozenJobExecution.from_dict: tables must not be empty")
        seen = set()
        for table in tables:
            if table.table_id in seen:
                raise ValueError(
                    f"FrozenJobExecution.from_dict: duplicate table_id {table.table_id}"
                )
            seen.add(table.table_id)
            if table.authority_mode != "GOVERNED":
                raise ValueError(
                    "FrozenJobExecution.from_dict: every frozen table must be GOVERNED"
                )
        return cls(
            contract_version=str(d["contract_version"]),
            run_id=str(d["run_id"]),
            action=str(d["action"]),
            parent_contract_fingerprint=str(d["parent_contract_fingerprint"]),
            deployment_profile_fingerprint=str(d["deployment_profile_fingerprint"]),
            tables=tables,
            retry_parent_run_id=retry_parent_run_id,
            execution_scope=scope,
            _legacy_execution_scope_present=("execution_scope" in d),
        )
