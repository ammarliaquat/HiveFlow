# api/models/auth.py
from typing import List, Optional
from pydantic import BaseModel, Field

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=200)

class UserResponse(BaseModel):
    username: str
    role: str
    permissions: List[str]
    allowed_projects: List[str] = Field(default_factory=list)

class SessionPayload(BaseModel):
    username: str
    role: str
    permissions: List[str]
    exp: int
