# tests/test_source_erp_db_target_tag_consistency.py
import uuid
import pytest
from gui.services.database import get_psycopg2_conn
from gui.services.execution_service import _materialize_governed_contract, ExecutionService
from gui.models.submission import SubmissionRequest
from gui.models.project_context import ProjectContext
from src.contracts.claim_models import FrozenJobExecution, FrozenTableEntry


@pytest.mark.live
def test_source_erp_db_active_tables_share_single_target_tag():
    """Verify that all active tables in source project source_erp_db target 'eland' uniformly."""
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_id, source_table, target_tag, target_schema
                FROM metadata.db_tables_etl
                WHERE LOWER(source_tag) = 'source_erp_db'
                  AND is_active = 'y'
                ORDER BY table_id
            """)
            rows = cur.fetchall()
            assert len(rows) > 0, "source_erp_db must have registered active tables."
            
            target_tags = {r[2] for r in rows}
            assert target_tags == {"eland"}, (
                f"All active tables in source_erp_db must target 'eland', found: {target_tags}"
            )
            
            # Specifically check ptblbank (ID: 1305)
            ptblbank_row = [r for r in rows if r[1].lower() == "ptblbank"]
            assert len(ptblbank_row) == 1, "ptblbank must be present and active in source_erp_db."
            assert ptblbank_row[0][2] == "eland", f"ptblbank target_tag must be 'eland', got: {ptblbank_row[0][2]}"
            assert ptblbank_row[0][3] == "eland", f"ptblbank target_schema must be 'eland', got: {ptblbank_row[0][3]}"


@pytest.mark.live
def test_source_erp_db_contract_materialization_single_target_tag():
    """Verify that governed contract materialization for source_erp_db yields single target_tag."""
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_id
                FROM metadata.db_tables_etl
                WHERE LOWER(source_tag) = 'source_erp_db'
                  AND is_active = 'y'
                ORDER BY table_id
            """)
            table_ids = [r[0] for r in cur.fetchall()]
            
            contract = _materialize_governed_contract(
                cur,
                run_id="00000000-0000-0000-0000-000000000000",
                source_tag="source_erp_db",
                table_ids=table_ids,
                action="run_governed",
                execution_scope="FULL",
            )
            
            target_tags = {t.target_tag for t in contract.tables}
            assert len(target_tags) == 1, (
                f"run_governed contract for source_erp_db must have exactly 1 target tag, got: {target_tags}"
            )
            assert target_tags == {"eland"}


def test_submit_run_rejects_heterogeneous_target_tags(monkeypatch):
    """ExecutionService.submit_run must fail fast if a contract has multiple target_tags."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_governed",
        source_tag="test_src",
        source_table="all_datasets",
    )

    fake_contract = FrozenJobExecution.build(
        run_id=str(uuid.uuid4()),
        action="run_governed",
        deployment_profile_fingerprint="abc",
        tables=[
            FrozenTableEntry(
                table_id=1,
                source_tag="test_src",
                source_table="tbl1",
                target_tag="target_a",
                target_schema="schema_a",
                target_table="tbl1",
                priority=1,
                authority_mode="GOVERNED",
                policy_version_id="policy-1",
                policy_fingerprint="p" * 64,
                column_mapping_version_id="mapping-1",
                column_mapping_fingerprint="m" * 64,
                business_key_version_id="key-1",
                business_key_fingerprint="k" * 64,
                business_key_columns=["id"],
                deployment_profile_fingerprint="d" * 64,
            ),
            FrozenTableEntry(
                table_id=2,
                source_tag="test_src",
                source_table="tbl2",
                target_tag="target_b",
                target_schema="schema_b",
                target_table="tbl2",
                priority=1,
                authority_mode="GOVERNED",
                policy_version_id="policy-2",
                policy_fingerprint="p" * 64,
                column_mapping_version_id="mapping-2",
                column_mapping_fingerprint="m" * 64,
                business_key_version_id="key-2",
                business_key_fingerprint="k" * 64,
                business_key_columns=["id"],
                deployment_profile_fingerprint="d" * 64,
            ),
        ],
    )

    class MockConn:
        def __init__(self):
            self._last_sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def cursor(self):
            return self

        def execute(self, sql, params=None):
            self._last_sql = str(sql).lower()

        def fetchall(self):
            if "job_scope_claim" in self._last_sql and "column_name" in self._last_sql:
                return [("scope_action",), ("worker_action",)]
            return [(1,), (2,)]

        def fetchone(self):
            return None

        def rollback(self):
            pass

        def commit(self):
            pass

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: MockConn())
    monkeypatch.setattr("gui.services.execution_service._materialize_governed_contract", lambda *args, **kwargs: fake_contract)

    with pytest.raises(RuntimeError, match="run_governed V1 requires all frozen tables to share one target_tag"):
        ExecutionService.submit_run(req, context=ProjectContext("test_src", "PROD"))


def test_submit_run_resolves_tables_scoped_to_target_tag(monkeypatch):
    """ExecutionService.submit_run must filter by target_tag when resolving tables for multi-target sources."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_governed",
        source_tag="opf_oracle_housing",
        target_tag="land_housing_prod",
        source_table="all_datasets",
    )

    executed_sqls = []

    class MockConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def cursor(self):
            return self

        def execute(self, sql, params=None):
            executed_sqls.append((str(sql), params))

        def fetchall(self):
            # If checking claim columns
            last_sql = executed_sqls[-1][0].lower() if executed_sqls else ""
            if "job_scope_claim" in last_sql and "column_name" in last_sql:
                return [("scope_action",), ("worker_action",)]
            return [(101,), (102,)]

        def fetchone(self):
            last_sql = executed_sqls[-1][0].lower() if executed_sqls else ""
            if "returning submitted_at" in last_sql:
                return ("2026-10-26 12:00:00",)
            return None

        def rollback(self):
            pass

        def commit(self):
            pass

    fake_contract = FrozenJobExecution.build(
        run_id=str(uuid.uuid4()),
        action="run_governed",
        deployment_profile_fingerprint="abc",
        tables=[
            FrozenTableEntry(
                table_id=101,
                source_tag="opf_oracle_housing",
                source_table="tbl1",
                target_tag="land_housing_prod",
                target_schema="land_housing_prod",
                target_table="tbl1",
                priority=1,
                authority_mode="GOVERNED",
                policy_version_id="policy-1",
                policy_fingerprint="p" * 64,
                column_mapping_version_id="mapping-1",
                column_mapping_fingerprint="m" * 64,
                business_key_version_id="key-1",
                business_key_fingerprint="k" * 64,
                business_key_columns=["id"],
                deployment_profile_fingerprint="d" * 64,
            ),
        ],
    )

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: MockConn())
    monkeypatch.setattr("gui.services.execution_service._materialize_governed_contract", lambda *args, **kwargs: fake_contract)
    monkeypatch.setattr("gui.services.execution_service.ScopeClaimManager.reserve_scope_claims_cursor", lambda *args, **kwargs: None)
    monkeypatch.setattr("gui.services.execution_service.JobManager.submit_governed_job", lambda *args, **kwargs: None)

    res = ExecutionService.submit_run(req, context=ProjectContext("opf_oracle_housing", "PROD"))
    assert res.accepted is True

    # Find the table_id resolution query
    table_id_queries = [
        (sql, params) for sql, params in executed_sqls
        if "select table_id" in sql.lower() and "db_tables_etl" in sql.lower()
    ]
    assert len(table_id_queries) == 1, "Expected exactly one table_id resolution query."
    sql, params = table_id_queries[0]
    assert "lower(target_tag) = lower(%s)" in sql.lower(), "Table ID query must filter by target_tag."
    assert params == ("opf_oracle_housing", "land_housing_prod")

