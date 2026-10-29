# api/routers/auth.py
import secrets
from datetime import timedelta
from fastapi import APIRouter, Request, Response, HTTPException, status, Depends

from api.config import (
    COOKIE_NAME,
    CSRF_COOKIE_NAME,
    COOKIE_SECURE,
    COOKIE_SAMESITE,
    SESSION_EXPIRE_MINUTES,
)
from api.models.auth import LoginRequest, UserResponse
from api.models.envelope import ResponseEnvelope
from api.security.auth import (
    authenticate_credentials,
    create_session_token,
    get_current_user,
    revoke_session_token,
    verify_csrf_token,
)

router = APIRouter(prefix="/auth", tags=["Authentication"])

@router.post("/login", response_model=ResponseEnvelope[UserResponse])
def login(creds: LoginRequest, response: Response):
    user = authenticate_credentials(creds.username, creds.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password, or account inactive.",
        )

    # Issue session token
    token = create_session_token(
        data={
            "username": user.username,
            "role": user.role,
            "permissions": user.permissions,
        },
        expires_delta=timedelta(minutes=SESSION_EXPIRE_MINUTES),
    )

    # Generate CSRF token for double-submit cookie protection
    csrf_token = secrets.token_hex(24)

    # Set secure HttpOnly session cookie
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=SESSION_EXPIRE_MINUTES * 60,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        path="/",
    )

    # Set non-HttpOnly CSRF cookie so frontend JavaScript can read and include it in mutating requests
    response.set_cookie(
        key=CSRF_COOKIE_NAME,
        value=csrf_token,
        max_age=SESSION_EXPIRE_MINUTES * 60,
        httponly=False,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        path="/",
    )

    return ResponseEnvelope.ok(user)

@router.post("/logout", response_model=ResponseEnvelope[dict])
def logout(request: Request, response: Response, current_user: UserResponse = Depends(get_current_user)):
    # Enforce CSRF protection on logout if using session cookie
    verify_csrf_token(request)

    # Revoke active session token
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:].strip()
    if token:
        revoke_session_token(token)

    # Clear cookies with exact RFC 6265 matching attributes
    response.delete_cookie(
        key=COOKIE_NAME,
        path="/",
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
    )
    response.delete_cookie(
        key=CSRF_COOKIE_NAME,
        path="/",
        httponly=False,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
    )
    return ResponseEnvelope.ok({"logged_out": True, "username": current_user.username})

@router.get("/me", response_model=ResponseEnvelope[UserResponse])
def get_me(current_user: UserResponse = Depends(get_current_user)):
    return ResponseEnvelope.ok(current_user)

