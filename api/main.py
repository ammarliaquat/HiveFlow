# api/main.py
import logging
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.config import CORS_ORIGINS
from api.models.envelope import ResponseEnvelope
from api.routers import (
    auth_router,
    dashboard_router,
    pipelines_router,
    runs_router,
    system_router,
    discovery_router,
    users_router,
)


log = logging.getLogger("api.main")

app = FastAPI(
    title="HiveFlow Governed Lakehouse API",
    version="1.0.0",
    description="Unified API boundary for HiveFlow Governed Lakehouse Platform by Hiveworx",
    docs_url="/docs",
    redoc_url="/redoc",
)

# 1. CORS Middleware (Restricted methods and headers per security review)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=[
        "Content-Type",
        "Authorization",
        "X-Project-Context",
        "X-Request-Id",
        "X-CSRF-Token",
        "Accept",
    ],
)

# 2. Global Exception Handlers ensuring 100% Standardized Envelopes
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    req_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    code_map = {
        400: "BAD_REQUEST",
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        409: "CONFLICT",
        422: "UNPROCESSABLE_ENTITY",
    }
    code = code_map.get(exc.status_code, "HTTP_ERROR")
    envelope = ResponseEnvelope.fail(
        code=code,
        message=str(exc.detail),
        correlation_id=req_id,
        request_id=req_id,
    )
    return JSONResponse(status_code=exc.status_code, content=envelope.model_dump())

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    req_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    errors = exc.errors()
    error_msg = "; ".join(f"{'.'.join(str(l) for l in e.get('loc', []))}: {e.get('msg')}" for e in errors)
    envelope = ResponseEnvelope.fail(
        code="VALIDATION_ERROR",
        message=f"Request validation failed: {error_msg}",
        correlation_id=req_id,
        request_id=req_id,
    )
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content=envelope.model_dump())

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    req_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    log.error(f"Unhandled server error [req_id={req_id}]: {exc}", exc_info=True)
    envelope = ResponseEnvelope.fail(
        code="INTERNAL_SERVER_ERROR",
        message="An unexpected internal server error occurred. Please contact the platform operator.",
        correlation_id=req_id,
        request_id=req_id,
    )
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=envelope.model_dump())

# 3. Mount Routers under /api/v1
app.include_router(auth_router, prefix="/api/v1")
app.include_router(dashboard_router, prefix="/api/v1")
app.include_router(pipelines_router, prefix="/api/v1")
app.include_router(runs_router, prefix="/api/v1")
app.include_router(system_router, prefix="/api/v1")
app.include_router(discovery_router, prefix="/api/v1")
app.include_router(users_router, prefix="/api/v1")

# 4. Root Service Meta Endpoint

@app.get("/api/v1", response_model=ResponseEnvelope[dict])
def get_api_root():
    return ResponseEnvelope.ok({
        "service": "HiveFlow Governed Lakehouse Platform API",
        "version": "1.0.0",
        "status": "OPERATIONAL",
        "docs": "/docs",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

# 5. Mount Static Assets and HTML5 Client-Side Routing Fallback
import os
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

frontend_dist = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")
assets_dir = os.path.join(frontend_dist, "assets")

if os.path.exists(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

@app.get("/{full_path:path}", include_in_schema=False)
async def serve_spa_route(request: Request, full_path: str):
    # Exclude API endpoints and Swagger docs from SPA catch-all
    if full_path.startswith("api/") or full_path in ("docs", "redoc", "openapi.json"):
        raise StarletteHTTPException(status_code=404, detail="Endpoint not found")

    # If an exact static file in frontend_dist exists, return it
    requested_file = os.path.join(frontend_dist, full_path)
    if os.path.isfile(requested_file):
        return FileResponse(requested_file)

    # Return SPA index.html for client-side routing (e.g. /dashboard, /pipelines, /runs)
    index_file = os.path.join(frontend_dist, "index.html")
    if os.path.isfile(index_file):
        return FileResponse(index_file)

    raise StarletteHTTPException(status_code=404, detail="Frontend distribution bundle not found")

