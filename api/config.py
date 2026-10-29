# api/config.py
import os
from typing import List
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

_DEV_EPHEMERAL_SECRET = None

def get_secret_key() -> str:
    """
    Authoritative secret key getter.
    In production (APP_ENV=production/prod): Fails closed if API_SECRET_KEY is not configured.
    In development (APP_ENV!=production): If API_SECRET_KEY is not configured, generates
    a secure ephemeral key for the current process session (never uses a hardcoded default).
    """
    global _DEV_EPHEMERAL_SECRET
    key = os.getenv("API_SECRET_KEY")
    if key:
        return key

    app_env = os.getenv("APP_ENV", "local").lower()
    if app_env in ("production", "prod"):
        raise RuntimeError(
            "FATAL: 'API_SECRET_KEY' environment variable is not configured in production. "
            "A cryptographically secure key (>=32 chars) is required. Server fails closed."
        )

    if _DEV_EPHEMERAL_SECRET is None:
        import secrets
        import logging
        _DEV_EPHEMERAL_SECRET = secrets.token_hex(32)
        logging.getLogger("api.config").warning(
            "NOTICE: 'API_SECRET_KEY' is not set. Generated secure ephemeral session secret "
            "for local development (APP_ENV=%s). Set API_SECRET_KEY in production.",
            app_env,
        )
    return _DEV_EPHEMERAL_SECRET

# Secret for signing session JWT tokens (empty string if unset so get_secret_key() fails closed)
SECRET_KEY = os.getenv("API_SECRET_KEY", "")
ALGORITHM = "HS256"
SESSION_EXPIRE_MINUTES = int(os.getenv("API_SESSION_EXPIRE_MINUTES", "60"))

# Cookie settings
COOKIE_NAME = "etl_session"
CSRF_COOKIE_NAME = "etl_csrf"
COOKIE_SECURE = os.getenv("API_COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = "lax"

# Dynamic CORS origins from environment (P0 invariant: zero hardcoded IPs)
_default_origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]
_env_origins = [o.strip() for o in os.getenv("API_CORS_ORIGINS", "").split(",") if o.strip()]
CORS_ORIGINS: List[str] = _default_origins + _env_origins

