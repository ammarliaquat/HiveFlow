# api/routers/__init__.py
from api.routers.auth import router as auth_router
from api.routers.dashboard import router as dashboard_router
from api.routers.pipelines import router as pipelines_router
from api.routers.runs import router as runs_router
from api.routers.system import router as system_router
from api.routers.discovery import router as discovery_router
from api.routers.users import router as users_router

__all__ = [
    "auth_router",
    "dashboard_router",
    "pipelines_router",
    "runs_router",
    "system_router",
    "discovery_router",
    "users_router",
]


