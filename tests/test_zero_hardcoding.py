# tests/test_zero_hardcoding.py
"""
Automated Policy Enforcement Test.

Scans all Python files in src/ and jobs/ to ensure zero hardcoded credentials,
IP addresses, Spark Connect URLs, and direct database driver connections outside
of ConnectionManager.
"""
import os
import re
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCAN_DIRS = ["src", "jobs"]
EXCLUDED_FILES = [
    "src/connections/connection_manager.py",
    "src/services/connection_service.py",
    "src/utils/db_utils.py",
    "src/load/target_schema_inspector.py",
    "jobs/check_architecture_rules.py"
]

# Allowed fallback default port occurrences (e.g. 5432 or 3306 default in .get("port", 5432))
ALLOWED_PORT_FILES = [
    "src/connections/connection_manager.py",
    "src/services/connection_service.py",
    "src/utils/db_utils.py",
    "jobs/discover_schema.py"
]


def get_python_files():
    py_files = []
    for d in SCAN_DIRS:
        dir_path = os.path.join(PROJECT_ROOT, d)
        if not os.path.exists(dir_path):
            continue
        for root, _, files in os.walk(dir_path):
            for f in files:
                if f.endswith(".py"):
                    full_path = os.path.join(root, f)
                    rel_path = os.path.relpath(full_path, PROJECT_ROOT).replace("\\", "/")
                    py_files.append((rel_path, full_path))
    return py_files


@pytest.mark.parametrize("rel_path, full_path", get_python_files())
def test_no_forbidden_connection_calls(rel_path, full_path):
    """Ensure no direct psycopg2/pymysql/cx_Oracle/mysql.connector connect calls outside ConnectionManager."""
    if rel_path in EXCLUDED_FILES:
        return
    
    with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
        
    forbidden_calls = [
        "psycopg2.connect",
        "pymysql.connect",
        "cx_Oracle.connect",
        "mysql.connector.connect"
    ]
    
    for call in forbidden_calls:
        assert call not in content, f"Forbidden direct database connection '{call}' found in {rel_path}. Use ConnectionManager instead."


@pytest.mark.parametrize("rel_path, full_path", get_python_files())
def test_no_hardcoded_ips_and_urls(rel_path, full_path):
    """Ensure no hardcoded server IP addresses or Spark Connect URLs."""
    if rel_path in EXCLUDED_FILES:
        return

    with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
        
    # Check for Spark connect URLs
    assert "sc://" not in content, f"Hardcoded Spark Connect URL ('sc://') found in {rel_path}."
    
    # Check for hardcoded IPs (e.g., 10.99.x.x, 58.65.x.x, 192.168.x.x, 172.17.x.x) excluding 127.0.0.1 loopback
    ip_pattern = re.compile(r'\b(?:10|58|192|172)\.\d+\.\d+\.\d+\b')
    matches = [ip for ip in ip_pattern.findall(content) if ip != "127.0.0.1"]
    assert not matches, f"Hardcoded IP addresses {matches} found in {rel_path}. All endpoints must be configured in databases.ini or environment variables."


@pytest.mark.parametrize("rel_path, full_path", get_python_files())
def test_no_hardcoded_ports(rel_path, full_path):
    """Ensure no hardcoded port literals like 15002 (Spark connect) or database ports outside allowed fallbacks."""
    with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
        
    # 15002 should never be hardcoded anywhere in src/ or jobs/
    assert "15002" not in content, f"Hardcoded Spark Connect port 15002 found in {rel_path}."
    
    # Database ports 3306, 5432, 1521 check outside allowed fallback default files
    if rel_path not in ALLOWED_PORT_FILES:
        port_pattern = re.compile(r'\b(?:3306|5432|1521)\b')
        matches = port_pattern.findall(content)
        assert not matches, f"Hardcoded database port literals {matches} found in {rel_path}. Ports must come from databases.ini."


def test_databases_ini_passwords_have_no_fallbacks_or_literals():
    """Ensure no password field in databases.ini contains literal fallback values (:-default) or plaintext secrets."""
    ini_path = os.path.join(PROJECT_ROOT, "config", "databases.ini")
    if not os.path.exists(ini_path):
        pytest.skip(f"databases.ini missing at {ini_path}")
    
    with open(ini_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        
    password_pattern = re.compile(r'^\s*password\s*=\s*\$\{([A-Z0-9_]+)\}\s*$')
    for idx, line in enumerate(lines, 1):
        if line.strip().startswith("password"):
            assert ":-" not in line, (
                f"SECURITY VIOLATION in config/databases.ini line {idx}: Password field contains literal fallback ':-'. "
                f"Password fields must be purely ${{VAR}} with zero defaults.\nLine: {line.strip()}"
            )
            assert password_pattern.match(line.strip()), (
                f"SECURITY VIOLATION in config/databases.ini line {idx}: Password field must be purely ${{ENV_VAR_NAME}} "
                f"with no plaintext or fallback value.\nLine: {line.strip()}"
            )


@pytest.mark.parametrize("rel_path, full_path", get_python_files())
def test_no_hardcoded_hostnames(rel_path, full_path):
    """Ensure no hardcoded infrastructure hostnames like PGDBBI exist anywhere in application source code."""
    with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    assert "PGDBBI" not in content, f"Hardcoded infrastructure hostname 'PGDBBI' found in {rel_path}. Hostnames must be resolved via configuration."


