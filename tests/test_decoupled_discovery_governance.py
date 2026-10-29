# tests/test_decoupled_discovery_governance.py
"""
Comprehensive Behavioral Test Suite for Decoupled Schema Discovery & Governed Onboarding.

Acceptance Tests Required:
1. Discovery Decoupling:
   - crawl() persists only to Tier 1 catalog (metadata.db_tables, metadata.db_columns)
   - crawl() has zero invocations or imports of promote_table / _promote_one_table
   - DiscoveryCrawler.promote_to_tier2() raises RuntimeError directing callers to Step 3
2. Step 4 Server-Owned Authorization:
   - Unauthenticated or forged Step-4 policy promotion requests are rejected with PolicyActorUnauthorizedError
   - Principal actor_id mismatch is rejected as forgery
   - Non-admin / non-system roles are rejected
3. Checked Atomic Authority Transition:
   - promote_policy updates current_policy_version_id and execution_authority_mode = 'GOVERNED' atomically
   - is_active remains the operator-controlled scope switch
   - Transitions only from 'BLOCKED' (for initial promotion)
   - Validates cur.rowcount == 1, raising PolicyTransitionInvalidError if zero rows affected
4. Step 3 PromotionService RBAC & Validation:
   - Enforces ADMIN role on caller
   - Rejects cross-tenant source_tag outside active ProjectContext
   - Rejects target connection environment mismatch against active context environment
5. Fake Default Elimination:
   - Missing business keys or unobserved CDC recommendations never synthesize 'id', 'PRIMARY_KEY', or 100 confidence
6. Unapproved Key Invariant:
   - Tables without verified physical PK evidence are onboarded with key_approval_state = 'UNAPPROVED',
     execution_authority_mode = 'BLOCKED', is_active = 'n'
7. Verified Physical-Key Provenance:
   - Tables with verified Tier-1 physical PK evidence are onboarded with key_approval_state = 'APPROVED',
     preserving approved key evidence so they can successfully reach Step 4 GOVERNED status
8. Safe Transaction Boundaries (Savepoint Injection Immunity):
   - Hostile/unusual table names (e.g. "orders'; DROP TABLE metadata.db_tables_etl; --")
     use server-generated numeric savepoints (sp_onboard_{idx}), never interpolating table names into SQL
9. Partial Batch Failure Isolation & Transactional Audit:
   - Partial batch failures commit only fully successful tables, columns, and SUCCESS audits
   - Failing tables roll back to savepoint and commit ERROR audit without contaminating Tier 2 tables
"""

import pytest
import inspect
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

from src.models.principal import AuthenticatedPrincipal
from gui.models.project_context import ProjectContext
from gui.services.execution_service import AuthorizationError
from gui.services.promotion_service import PromotionService
from src.metadata.policy_governance import PolicyGovernanceService
from src.metadata.policy_models import (
    CandidateTableContract,
    PromotionRequest,
    PromotionResult,
    MappingPolicy,
)
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
)
from src.metadata.metadata_models import ResolvedTableMappings
from src.exceptions import (
    PolicyActorUnauthorizedError,
    PolicyTransitionInvalidError,
    BusinessKeyRequiredError,
)
from jobs.discover_and_promote import DiscoveryCrawler
import jobs.discover_and_promote as dap


def _approved_policy_row(
    current_version_id=None,
    cdc_strategy="FULL_SNAPSHOT",
    cdc_source="CONFIG",
    cdc_confidence=100,
    cdc_health="HEALTHY",
    cdc_null_rows=0,
):
    return (
        101, "EXPLICIT", "source_tag", "orders", "land_test", "landing", "orders",
        current_version_id, ["id"], "PRIMARY_KEY", "src_fp", "operator_alice", datetime.now(timezone.utc),
        1, False, True, "src_fp",
        cdc_strategy, cdc_source, cdc_confidence, cdc_health, cdc_null_rows,
    )


# ─── 1. DISCOVERY DECOUPLING ──────────────────────────────────────────────────

def test_discovery_crawler_has_zero_promotion_imports_and_calls():
    """Verify DiscoveryCrawler does not import or invoke _promote_one_table or promote_to_tier2."""
    source_code = inspect.getsource(dap)
    assert "from jobs.promote_table import _promote_one_table" not in source_code
    assert "import _promote_one_table" not in source_code

    crawler = DiscoveryCrawler(
        spark=MagicMock(),
        conn_manager=MagicMock(),
        target_tag="land_test",
    )
    # promote_to_tier2() method body must fail-closed with RuntimeError
    with pytest.raises(RuntimeError, match="Automatic Tier-2 promotion during discovery has been disabled"):
        crawler.promote_to_tier2(source_tag="oracle_finance")


def test_discovery_crawl_only_writes_tier_1_catalog():
    """Verify crawl() writes only Tier 1 db_tables/db_columns and never calls promote."""
    mock_spark = MagicMock()
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn

    crawler = DiscoveryCrawler(
        spark=mock_spark,
        conn_manager=mock_cm,
        target_tag="land_test",
    )

    with patch.object(crawler, "discover_source", return_value={"tables": [{"table_name": "orders"}], "columns": []}), \
         patch.object(crawler, "sync_tables") as mock_sync_tables, \
         patch.object(crawler, "sync_columns") as mock_sync_cols:

        crawler.crawl(source_tag="oracle_finance")
        assert crawler.stats["tables_discovered"] == 1
        mock_sync_tables.assert_called_once()
        mock_sync_cols.assert_called_once()


# ─── 2. STEP 4 SERVER-OWNED AUTHORIZATION ─────────────────────────────────────

def test_step4_policy_promotion_rejects_unauthenticated_request():
    """Verify PolicyGovernanceService rejects promotion requests lacking an AuthenticatedPrincipal."""
    mock_cm = MagicMock()
    service = PolicyGovernanceService(mock_cm)

    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint="fp123",
        actor_type="USER",
        actor_id="hacker_bob",
        authenticated_principal=None,
    )
    with pytest.raises(PolicyActorUnauthorizedError, match="Step 4 policy promotion requires an authenticated"):
        service.promote_policy(req)


def test_step4_policy_promotion_rejects_forged_actor_id():
    """Verify PolicyGovernanceService rejects forged actor IDs where request.actor_id != principal.actor_id."""
    mock_cm = MagicMock()
    service = PolicyGovernanceService(mock_cm)

    principal = AuthenticatedPrincipal(actor_id="actual_bob", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint="fp123",
        actor_type="USER",
        actor_id="impersonated_alice",
        authenticated_principal=principal,
    )
    with pytest.raises(PolicyActorUnauthorizedError, match="Forged policy promotion request"):
        service.promote_policy(req)


def test_step4_policy_promotion_rejects_unauthorized_role():
    """Verify PolicyGovernanceService rejects non-admin users (e.g. VIEWER/OPERATOR without ADMIN)."""
    mock_cm = MagicMock()
    service = PolicyGovernanceService(mock_cm)

    principal = AuthenticatedPrincipal(actor_id="viewer_bob", actor_type="USER", roles={"VIEWER"})
    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint="fp123",
        actor_type="USER",
        actor_id="viewer_bob",
        authenticated_principal=principal,
    )
    with pytest.raises(PolicyActorUnauthorizedError, match="not authorized for policy promotion"):
        service.promote_policy(req)


# ─── 3. CHECKED ATOMIC AUTHORITY TRANSITION ───────────────────────────────────

def test_atomic_authority_transition_sets_pointer_and_authority_without_overriding_scope():
    """Verify promotion does not overwrite the operator's active/inactive scope choice."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.side_effect = [
        # candidate contract lookup:
        _approved_policy_row(None),
        None,  # candidate cdc lookup
        # promote_policy lookups:
        _approved_policy_row(None),
        None,  # promote candidate cdc lookup
        (datetime.now(timezone.utc),),  # INSERT RETURNING promoted_at
    ]
    mock_cur.rowcount = 1

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()
    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=101,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=101,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    candidate = service.build_candidate_contract(101)

    principal = AuthenticatedPrincipal(actor_id="operator_alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="operator_alice",
        reason="Governed onboarding",
        authenticated_principal=principal,
    )
    result = service.promote_policy(req)

    assert result.table_id == 101
    assert result.new_version_number == 1

    # Verify atomic update statement
    update_calls = [
        c for c in mock_cur.execute.call_args_list
        if "SET current_policy_version_id = %s" in str(c.args[0])
    ]
    assert len(update_calls) == 1
    sql = update_calls[0].args[0]
    assert "execution_authority_mode = 'GOVERNED'" in sql
    assert "is_active = 'y'" not in sql
    assert "execution_authority_mode = 'BLOCKED'" in sql


def test_atomic_authority_transition_fails_if_rowcount_is_zero():
    """Verify promote_policy raises PolicyTransitionInvalidError if table was not in BLOCKED mode."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.side_effect = [
        # candidate contract lookup:
        _approved_policy_row(None),
        None,  # candidate cdc lookup
        # promote_policy lookups:
        _approved_policy_row(None),
        None,  # promote candidate cdc lookup
        (datetime.now(timezone.utc),),  # INSERT RETURNING promoted_at
    ]
    # Simulate zero rows updated (e.g. table is not in BLOCKED mode or concurrency race)
    mock_cur.rowcount = 0

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()
    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=101,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=101,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    candidate = service.build_candidate_contract(101)

    principal = AuthenticatedPrincipal(actor_id="operator_alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="operator_alice",
        authenticated_principal=principal,
    )
    with pytest.raises(PolicyTransitionInvalidError, match="authority transition failed"):
        service.promote_policy(req)


# ─── 4. STEP 3 PROMOTIONSERVICE RBAC & VALIDATION ────────────────────────────

def test_promotion_service_enforces_admin_role():
    """Verify PromotionService rejects non-admin operators during Step 3 dataset onboarding."""
    non_admin_principal = AuthenticatedPrincipal(actor_id="dev_dave", actor_type="USER", roles={"DEVELOPER"})
    with pytest.raises(AuthorizationError, match="requires ADMIN role"):
        PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            principal=non_admin_principal,
        )


def test_promotion_service_rejects_cross_tenant_scope():
    """Verify PromotionService rejects source tag outside active ProjectContext scope."""
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})
    ctx_hospital = ProjectContext(project_id="mysql_hospital", environment="PROD")

    with pytest.raises(AuthorizationError, match="RBAC Authorization Violation"):
        PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            context=ctx_hospital,
            principal=admin_principal,
        )


def test_promotion_service_requires_explicit_target_tag():
    """Verify PromotionService raises ValueError if target_tag is empty or whitespace."""
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})
    with pytest.raises(ValueError, match="Explicit target_tag is required"):
        PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="",
            principal=admin_principal,
        )


def test_full_dataset_refreshes_existing_policies_when_no_tables_are_new():
    """Full-dataset Step 3 remains actionable after every table is onboarded."""
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})
    mock_cm = MagicMock()
    mock_cm.get.return_value = MagicMock(env="PROD", supports_target=True)
    reconciliation = {
        "updated": ["orders"],
        "unchanged": ["customers"],
        "skipped": [],
        "failed": [],
    }

    with patch(
        "gui.services.promotion_service.reconcile_catalog_cdc_policies",
        return_value=reconciliation,
    ) as reconcile:
        result = PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=[],
            target_tag="land_test",
            principal=admin_principal,
            cm=mock_cm,
            refresh_existing=True,
        )

    assert result == {
        "successful": [],
        "failed": [],
        "total": 0,
        "policy_refresh": reconciliation,
    }
    reconcile.assert_called_once_with(
        mock_cm,
        "oracle_finance",
        target_tag="land_test",
        actor_id="admin_alice",
    )


def test_full_dataset_refresh_updates_priority_for_existing_subscriptions():
    """Verify refresh_existing applies selected priority to existing subscriptions."""
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})
    mock_cm = MagicMock()
    mock_cm.get.return_value = MagicMock(env="PROD", supports_target=True)
    
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.rowcount = 42
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn

    reconciliation = {"updated": [], "unchanged": ["t1"], "skipped": [], "failed": []}

    with patch(
        "gui.services.promotion_service.reconcile_catalog_cdc_policies",
        return_value=reconciliation,
    ):
        result = PromotionService.promote_catalog_datasets(
            source_tag="msopforg_bureau",
            table_names=[],
            target_tag="land_n",
            priority=3,
            principal=admin_principal,
            cm=mock_cm,
            refresh_existing=True,
        )

    assert result["priority_updated"] == 42
    assert result["priority"] == 3
    assert result["policy_refresh"] == reconciliation


def test_promotion_service_rejects_target_environment_mismatch():
    """Verify PromotionService rejects target connections whose environment does not match context."""
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})
    ctx_prod = ProjectContext(project_id="oracle_finance", environment="PROD")

    mock_cm = MagicMock()
    mock_cm.get.side_effect = ValueError("Environment mismatch: 'land_test' is env='DEV', expected 'PROD'.")

    with pytest.raises(AuthorizationError, match="Environment mismatch"):
        PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            context=ctx_prod,
            principal=admin_principal,
            cm=mock_cm,
        )
    mock_cm.get.assert_called_once_with("land_test", expected_role="target", expected_env="PROD")


def test_promotion_service_rejects_re_onboarding_governed_table():
    """Verify PromotionService rejects Step 3 re-onboarding for an already GOVERNED table."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get.return_value = MagicMock(env="PROD", supports_target=True)

    # 4A query finds existing GOVERNED row
    mock_cur.fetchone.return_value = ("GOVERNED", "y", "pol_v1")
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})

    with patch("gui.services.promotion_service._promote_one_table") as mock_promote:
        result = PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            principal=admin_principal,
            cm=mock_cm,
        )
        assert len(result["successful"]) == 0
        assert len(result["failed"]) == 1
        assert "already GOVERNED" in result["failed"][0]["error"]
        mock_promote.assert_not_called()


def test_promotion_service_allows_controlled_refresh_of_blocked_table():
    """Verify PromotionService allows force refresh for an existing BLOCKED table."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get.return_value = MagicMock(env="PROD", supports_target=True)

    # 4A query finds existing BLOCKED row, 4B finds Tier-1 evidence
    mock_cur.fetchone.side_effect = [
        ("BLOCKED", "n", None),  # existing Tier 2 row
        ("PRIMARY_KEY", 100, False, True, "fp1"),  # Tier 1 evidence
    ]
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})

    with patch("gui.services.promotion_service._promote_one_table", return_value=True) as mock_promote:
        result = PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            principal=admin_principal,
            cm=mock_cm,
        )
        assert len(result["successful"]) == 1
        mock_promote.assert_called_once()
        # Verify force=True was passed for controlled refresh of BLOCKED table
        assert mock_promote.call_args.kwargs["force"] is True


def test_promotion_service_activates_cdc_after_onboarding_commit():
    """CDC activation must use a new connection after Tier-2 onboarding commits."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get.return_value = MagicMock(env="PROD", supports_target=True)
    mock_cur.fetchone.side_effect = [
        None,  # no existing Tier-2 row
        ("PROFILED_UNIQUE", 100, False, True, "fp1"),  # Tier-1 evidence
    ]
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})

    def onboard_and_publish_id(*args, **kwargs):
        kwargs["on_committed_table"](901)
        return True

    with patch("gui.services.promotion_service._promote_one_table", side_effect=onboard_and_publish_id), \
         patch("gui.services.promotion_service.evaluate_and_promote_table_cdc", return_value=True) as promote_cdc:
        result = PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            principal=admin_principal,
            cm=mock_cm,
        )

    assert result["successful"] == ["orders"]
    promote_cdc.assert_called_once_with(
        mock_cm,
        901,
        requested_is_cdc="y",
        requested_strategy="AUTO",
        actor_id="admin_alice",
    )
    assert mock_conn.commit.called


def test_promotion_service_reports_post_commit_policy_failure_truthfully():
    """A committed Tier-2 row is not reported GOVERNED when policy materialization fails."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get.return_value = MagicMock(env="PROD", supports_target=True)
    mock_cur.fetchone.side_effect = [
        None,  # no existing Tier-2 row
        ("PROFILED_UNIQUE", 100, False, True, "fp1"),  # Tier-1 evidence
    ]
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})

    def onboard_and_publish_id(*args, **kwargs):
        kwargs["on_committed_table"](901)
        return True

    with patch("gui.services.promotion_service._promote_one_table", side_effect=onboard_and_publish_id), \
         patch("gui.services.promotion_service.evaluate_and_promote_table_cdc", return_value=False):
        result = PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            principal=admin_principal,
            cm=mock_cm,
        )

    assert result["successful"] == []
    assert result["failed"] == [{
        "table_name": "orders",
        "error": "Tier-2 onboarding committed, but governed policy materialization failed.",
    }]
    assert mock_conn.commit.called


def test_promotion_service_audit_records_client_ip_without_inventing_address():
    """Verify PromotionService writes actual client_ip (or None) to gui_audit_log, never hardcoding 127.0.0.1."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get.return_value = MagicMock(env="PROD", supports_target=True)

    mock_cur.fetchone.side_effect = [
        None,  # existing row check
        ("PRIMARY_KEY", 100, False, True, "fp1"),  # Tier 1 evidence
    ]
    principal_no_ip = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"}, client_ip=None)

    with patch("gui.services.promotion_service._promote_one_table", return_value=True):
        PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["orders"],
            target_tag="land_test",
            principal=principal_no_ip,
            cm=mock_cm,
        )

    audit_calls = [
        c for c in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.gui_audit_log" in str(c.args[0])
    ]
    assert len(audit_calls) >= 1
    client_ip_param = audit_calls[0].args[1][8]
    assert client_ip_param is None


# ─── 5 & 6. FAKE DEFAULTS ELIMINATED & UNAPPROVED KEYS REMAIN BLOCKED ─────────

def test_unapproved_table_without_evidence_stays_blocked():
    """Verify tables without verified catalog PK evidence are onboarded as BLOCKED and UNAPPROVED."""
    from jobs.promote_table import _promote_one_table

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Master catalog has no PK (NULL business_key, confidence 0, unobserved)
    master_row = {
        "table_master_key": "oracle_finance_unobserved_tbl",
        "database_tag": "oracle_finance",
        "table_name": "unobserved_tbl",
        "schema_name": "dbo",
        "database_type": "oracle",
        "business_key": None,
        "key_source": None,
        "key_confidence": 0,
        "key_evidence_type": None,
        "key_candidate_nullable": True,
        "key_candidate_compatible": False,
        "key_evidence_fingerprint": None,
        "cdc_columns": None,
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_provider": "FULL_SNAPSHOT",
        "cdc_source": "UNOBSERVED",
        "cdc_confidence": 0,
        "cdc_health": "UNKNOWN",
        "cdc_null_rows": 0,
        "cdc_health_details": None,
        "match_count": 1,
    }
    mock_cur.fetchone.side_effect = [
        master_row,
        None,  # Check for existing row in db_tables_etl
        {"table_id": 101},  # INSERT RETURNING
    ]
    mock_cur.fetchall.return_value = []

    mock_target_cfg = MagicMock(database="land_test_db", schema="landing", db_type="postgres")
    mock_cm_pt = MagicMock()
    mock_cm_pt.get.return_value = mock_target_cfg

    with patch("jobs.promote_table.ConnectionManager", return_value=mock_cm_pt), \
         patch("jobs.promote_table.RecommendationService.get_effective_recommendation", return_value=None):
        ok = _promote_one_table(
            mock_conn,
            source_tag="oracle_finance",
            source_database="oracle_finance",
            table_name="unobserved_tbl",
            target_tag="land_test",
            priority=1,
            force=True,
            is_active="n",
            approve_physical_primary_key=False,
            allow_unapproved=True,
        )
        assert ok is True

    # Inspect db_tables_etl INSERT parameters
    insert_calls = [
        c for c in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.db_tables_etl" in str(c.args[0])
    ]
    assert len(insert_calls) == 1
    insert_params = insert_calls[0].args[1]

    # Verify key_approval_state is UNAPPROVED, execution_authority_mode is BLOCKED, is_active is 'n'
    assert insert_params[7] is None  # business_key is None (NOT fake 'id')
    assert insert_params[18] == "n"  # is_active is 'n'
    assert insert_params[26] == "UNAPPROVED"  # key_approval_state
    assert insert_params[27] is None  # approved_source_business_key is None


# ─── 7. VERIFIED PHYSICAL PK REACHES STEP 4 GOVERNED ──────────────────────────

def test_verified_physical_pk_onboarding_can_reach_governed():
    """Verify tables with verified physical PK evidence are approved and can transition to GOVERNED."""
    from jobs.promote_table import _promote_one_table

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    orders_row = {
        "table_master_key": "oracle_finance_orders",
        "database_tag": "oracle_finance",
        "table_name": "orders",
        "schema_name": "dbo",
        "database_type": "oracle",
        "business_key": "order_id",
        "key_source": "PRIMARY_KEY",
        "key_confidence": 100,
        "key_evidence_type": "PRIMARY_KEY",
        "key_candidate_nullable": False,
        "key_candidate_compatible": True,
        "key_evidence_fingerprint": "fp_orders_pk_100",
        "cdc_columns": None,
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_provider": "FULL_SNAPSHOT",
        "cdc_source": "PRIMARY_KEY",
        "cdc_confidence": 100,
        "cdc_health": "HEALTHY",
        "cdc_null_rows": 0,
        "cdc_health_details": None,
        "match_count": 1,
    }
    mock_cur.fetchone.side_effect = [
        orders_row,
        None,  # Check for existing row in db_tables_etl
        {"table_id": 102},  # INSERT RETURNING
    ]
    mock_cur.fetchall.return_value = []

    mock_target_cfg = MagicMock(database="land_test_db", schema="landing", db_type="postgres")
    mock_cm_pt = MagicMock()
    mock_cm_pt.get.return_value = mock_target_cfg

    with patch("jobs.promote_table.ConnectionManager", return_value=mock_cm_pt), \
         patch("jobs.promote_table.RecommendationService.get_effective_recommendation", return_value=None):
        ok = _promote_one_table(
            mock_conn,
            source_tag="oracle_finance",
            source_database="oracle_finance",
            table_name="orders",
            target_tag="land_test",
            priority=1,
            force=True,
            is_active="n",
            approve_physical_primary_key=True,
            approved_by="admin_alice",
            allow_unapproved=True,
        )
        assert ok is True

    insert_calls = [
        c for c in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.db_tables_etl" in str(c.args[0])
    ]
    assert len(insert_calls) == 1
    insert_params = insert_calls[0].args[1]
    assert insert_params[7] == "order_id"  # Valid business key preserved
    assert insert_params[26] == "APPROVED"  # key_approval_state is APPROVED
    assert '["order_id"]' in insert_params[27]  # approved_source_business_key json


# ─── 8. SAVEPOINT INJECTION RESISTANCE (NUMERIC SAVEPOINTS) ───────────────────

def test_hostile_table_names_do_not_inject_into_savepoints():
    """Verify hostile table names use numeric savepoints (sp_onboard_{idx}) preventing SQL injection."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get.return_value = MagicMock(env="TEST", supports_target=True)

    mock_cur.fetchone.side_effect = [
        None,  # table 1 existing row
        ("PRIMARY_KEY", 100, False, True, "fp1"),  # table 1 evidence
        None,  # table 2 existing row
        ("PRIMARY_KEY", 100, False, True, "fp2"),  # table 2 evidence
    ]

    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})
    hostile_names = [
        "orders'; DROP TABLE metadata.db_tables_etl; --",
        "users\" OR 1=1 --",
    ]

    with patch("gui.services.promotion_service._promote_one_table", return_value=True):
        result = PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=hostile_names,
            target_tag="land_test",
            principal=admin_principal,
            cm=mock_cm,
        )
        assert len(result["successful"]) == 2

    # Verify savepoint commands executed on cursor
    savepoint_calls = [
        c.args[0] for c in mock_cur.execute.call_args_list
        if "SAVEPOINT" in str(c.args[0])
    ]
    # Check numeric format
    assert "SAVEPOINT sp_onboard_1" in savepoint_calls
    assert "RELEASE SAVEPOINT sp_onboard_1" in savepoint_calls
    assert "SAVEPOINT sp_onboard_2" in savepoint_calls
    assert "RELEASE SAVEPOINT sp_onboard_2" in savepoint_calls
    # Assert hostile names never appear in SAVEPOINT commands
    for sp_call in savepoint_calls:
        assert "DROP TABLE" not in sp_call
        assert "OR 1=1" not in sp_call


# ─── 9. PARTIAL BATCH FAILURE ISOLATION & TRANSACTIONAL AUDIT ─────────────────

def test_partial_batch_failure_commits_successful_and_rolls_back_failing():
    """Verify partial batch failures preserve only successful tables/audits, rolling back failures."""
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get.return_value = MagicMock(env="TEST", supports_target=True)

    mock_cur.fetchone.side_effect = [
        None,  # table 1 existing row check
        ("PRIMARY_KEY", 100, False, True, "fp1"),  # table 1 evidence
        None,  # table 2 existing row check
        ("PRIMARY_KEY", 100, False, True, "fp2"),  # table 2 evidence
        None,  # table 3 existing row check
        ("PRIMARY_KEY", 100, False, True, "fp3"),  # table 3 evidence
    ]
    admin_principal = AuthenticatedPrincipal(actor_id="admin_alice", actor_type="USER", roles={"ADMIN"})

    def mock_promote_side_effect(conn, source_tag, source_database, table_name, **kwargs):
        if table_name == "failing_tbl":
            raise ValueError("Intentional failure on table 2")
        return True

    with patch("gui.services.promotion_service._promote_one_table", side_effect=mock_promote_side_effect):
        result = PromotionService.promote_catalog_datasets(
            source_tag="oracle_finance",
            table_names=["success_tbl_1", "failing_tbl", "success_tbl_2"],
            target_tag="land_test",
            principal=admin_principal,
            cm=mock_cm,
        )

    assert result["successful"] == ["success_tbl_1", "success_tbl_2"]
    assert len(result["failed"]) == 1
    assert result["failed"][0]["table_name"] == "failing_tbl"

    all_sql = [c.args[0] for c in mock_cur.execute.call_args_list]

    # Check table 1 succeeded
    assert "SAVEPOINT sp_onboard_1" in all_sql
    assert "RELEASE SAVEPOINT sp_onboard_1" in all_sql

    # Check table 2 rolled back to savepoint
    assert "SAVEPOINT sp_onboard_2" in all_sql
    assert "ROLLBACK TO SAVEPOINT sp_onboard_2" in all_sql

    # Check table 3 succeeded
    assert "SAVEPOINT sp_onboard_3" in all_sql
    assert "RELEASE SAVEPOINT sp_onboard_3" in all_sql

    # Check transactional audit logs written: 2 SUCCESS and 1 ERROR
    audit_inserts = [
        c for c in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.gui_audit_log" in str(c.args[0])
    ]
    assert len(audit_inserts) == 3
    statuses = [c.args[1][7] for c in audit_inserts]
    assert statuses == ["SUCCESS", "ERROR", "SUCCESS"]


# ─── 10. STEP 4 CDC TUPLE-BASED VALIDATION ────────────────────────────────────

def test_step4_cdc_validation_rejects_critical_health_tuple():
    """Verify promote_policy rejects database tuple when CDC health is CRITICAL."""
    from src.metadata.policy_models import (
        CDCStrategy,
        ResolvedCDCPolicy,
        SourceMutationMode,
        CursorFinalityMode,
        CursorLogicalType,
    )
    from src.exceptions import PolicyDependencyInvalidError

    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Tuple row with cdc_health="CRITICAL" at index 20
    critical_row = _approved_policy_row(
        current_version_id=None,
        cdc_strategy="TIMESTAMP_HIGH_WATERMARK",
        cdc_source="PROFILED",
        cdc_confidence=100,
        cdc_health="CRITICAL",
        cdc_null_rows=0,
    )
    mock_cur.fetchone.side_effect = [
        critical_row,  # build_candidate_contract lookup
        critical_row,  # promote_policy row lookup
    ]

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()
    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=101,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=101,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )

    candidate_cdc = ResolvedCDCPolicy.build(
        table_id=101,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("updated_at",),
        cursor_types=(CursorLogicalType.TIMESTAMP,),
    )

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    candidate = service.build_candidate_contract(101, cdc_policy=candidate_cdc)

    principal = AuthenticatedPrincipal(actor_id="operator_alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="operator_alice",
        authenticated_principal=principal,
    )

    with patch.object(service, "_load_candidate_cdc_policy", return_value=candidate_cdc):
        with pytest.raises(PolicyDependencyInvalidError, match="CDC health is 'CRITICAL'"):
            service.promote_policy(req)


def test_step4_cdc_validation_rejects_low_profiled_confidence_tuple():
    """Verify promote_policy rejects database tuple when profiled CDC confidence is below 50%."""
    from src.metadata.policy_models import (
        CDCStrategy,
        ResolvedCDCPolicy,
        SourceMutationMode,
        CursorFinalityMode,
        CursorLogicalType,
    )
    from src.exceptions import PolicyDependencyInvalidError

    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Tuple row with cdc_confidence=30 at index 19 and cdc_source="PROFILED" at index 18
    low_conf_row = _approved_policy_row(
        current_version_id=None,
        cdc_strategy="TIMESTAMP_HIGH_WATERMARK",
        cdc_source="PROFILED",
        cdc_confidence=30,
        cdc_health="OK",
        cdc_null_rows=0,
    )
    mock_cur.fetchone.side_effect = [
        low_conf_row,
        low_conf_row,
    ]

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()
    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=101,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=101,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )

    candidate_cdc = ResolvedCDCPolicy.build(
        table_id=101,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("updated_at",),
        cursor_types=(CursorLogicalType.TIMESTAMP,),
    )

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    candidate = service.build_candidate_contract(101, cdc_policy=candidate_cdc)

    principal = AuthenticatedPrincipal(actor_id="operator_alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="operator_alice",
        authenticated_principal=principal,
    )

    with patch.object(service, "_load_candidate_cdc_policy", return_value=candidate_cdc):
        with pytest.raises(PolicyDependencyInvalidError, match="below 50% threshold"):
            service.promote_policy(req)


def test_step4_cdc_validation_rejects_null_cursor_rows_tuple():
    """Verify promote_policy rejects database tuple when CDC cursor contains null rows."""
    from src.metadata.policy_models import (
        CDCStrategy,
        ResolvedCDCPolicy,
        SourceMutationMode,
        CursorFinalityMode,
        CursorLogicalType,
    )
    from src.exceptions import PolicyDependencyInvalidError

    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Tuple row with cdc_null_rows=5 at index 21
    null_rows_row = _approved_policy_row(
        current_version_id=None,
        cdc_strategy="TIMESTAMP_HIGH_WATERMARK",
        cdc_source="PROFILED",
        cdc_confidence=95,
        cdc_health="OK",
        cdc_null_rows=5,
    )
    mock_cur.fetchone.side_effect = [
        null_rows_row,
        null_rows_row,
    ]

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()
    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=101,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=101,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )

    candidate_cdc = ResolvedCDCPolicy.build(
        table_id=101,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("updated_at",),
        cursor_types=(CursorLogicalType.TIMESTAMP,),
    )

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    candidate = service.build_candidate_contract(101, cdc_policy=candidate_cdc)

    principal = AuthenticatedPrincipal(actor_id="operator_alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=101,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="operator_alice",
        authenticated_principal=principal,
    )

    with patch.object(service, "_load_candidate_cdc_policy", return_value=candidate_cdc):
        with pytest.raises(PolicyDependencyInvalidError, match="contains 5 null rows"):
            service.promote_policy(req)
