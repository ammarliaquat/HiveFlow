# tests/test_rbac_enforcement.py
import pytest
from src.connections.connection_manager import ConnectionManager, ConnectionConfig

def _connection_manager_with_configs():
    cm = ConnectionManager.__new__(ConnectionManager)
    cm._configs = {
        "msopforg_opforg": ConnectionConfig(
            source_tag="msopforg_opforg",
            db_type="mysql",
            jdbc_url="jdbc:mysql://source.example.internal:3306/src",
            username="user",
            credential_reference="pwd",
            driver="com.mysql.cj.jdbc.Driver",
            supports_source=True,
            supports_target=False,
        ),
        "land_n": ConnectionConfig(
            source_tag="land_n",
            db_type="postgresql",
            jdbc_url="jdbc:postgresql://target.example.internal:5432/tgt",
            username="user",
            credential_reference="pwd",
            driver="org.postgresql.Driver",
            supports_source=False,
            supports_target=True,
        ),
    }
    return cm


def test_rbac_role_enforcement_in_get():
    cm = _connection_manager_with_configs()
    
    # Check that msopforg_opforg is configured as role='source'
    src_cfg = cm.get("msopforg_opforg", expected_role="source")
    assert src_cfg.role == "source"
    
    # Attempting to request a 'target' role for a source connection MUST fail
    with pytest.raises(ValueError) as exc_info:
        cm.get("msopforg_opforg", expected_role="target")
    assert "Security Exception" in str(exc_info.value)
    assert "is configured for role 'source', but requested for operation role 'target'" in str(exc_info.value)

    # Check that land_n is configured as role='target'
    tgt_cfg = cm.get("land_n", expected_role="target")
    assert tgt_cfg.role == "target"

    # Attempting to request a 'source' role for a target connection MUST fail
    with pytest.raises(ValueError) as exc_info:
        cm.get("land_n", expected_role="source")
    assert "Security Exception" in str(exc_info.value)
    assert "is configured for role 'target', but requested for operation role 'source'" in str(exc_info.value)

    # Check that a connection configured with role='both' accepts either role
    cm._configs["test_both"] = ConnectionConfig(
        source_tag="test_both",
        db_type="postgresql",
        jdbc_url="jdbc:postgresql://localhost:5432/test",
        username="user",
        credential_reference="pwd",
        driver="org.postgresql.Driver",
        supports_source=True,
        supports_target=True
    )
    both_cfg_src = cm.get("test_both", expected_role="source")
    both_cfg_tgt = cm.get("test_both", expected_role="target")
    assert both_cfg_src.role == "both"
    assert both_cfg_tgt.role == "both"

def test_rbac_role_enforcement_in_get_db_connection():
    cm = _connection_manager_with_configs()
    
    # Attempting to get a db connection for a source connection with expected_role='target' MUST fail before opening connection
    with pytest.raises(ValueError) as exc_info:
        with cm.get_db_connection("msopforg_opforg", expected_role="target"):
            pass
    assert "Security Exception" in str(exc_info.value)


def test_env_enforcement_in_get():
    cm = _connection_manager_with_configs()
    cm._configs["prod_db"] = ConnectionConfig(
        source_tag="prod_db",
        db_type="postgresql",
        jdbc_url="jdbc:postgresql://localhost:5432/prod",
        username="user",
        credential_reference="pwd",
        driver="org.postgresql.Driver",
        supports_target=True,
        env="prod"
    )
    cm._configs["dev_db"] = ConnectionConfig(
        source_tag="dev_db",
        db_type="postgresql",
        jdbc_url="jdbc:postgresql://localhost:5432/dev",
        username="user",
        credential_reference="pwd",
        driver="org.postgresql.Driver",
        supports_target=True,
        env="dev"
    )

    # Matching env should succeed
    assert cm.get("prod_db", expected_env="prod").env == "prod"
    assert cm.get("dev_db", expected_env="dev").env == "dev"

    # Mismatched env MUST raise ValueError
    with pytest.raises(ValueError) as exc_info:
        cm.get("dev_db", expected_env="prod")
    assert "Environment mismatch" in str(exc_info.value)

    # has_connection should honor expected_env
    assert cm.has_connection("prod_db", expected_env="prod") is True
    assert cm.has_connection("prod_db", expected_env="dev") is False

    # available_connection_tags should honor env filter
    tags_prod = cm.available_connection_tags(env="prod")
    assert "prod_db" in tags_prod
    assert "dev_db" not in tags_prod
