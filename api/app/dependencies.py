# app/dependencies.py
import os
import logging
from typing import Annotated, Optional
from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlmodel import Session
from supabase import Client
import jwt
from .classes.supabase_db import SupabaseDatabaseManager
from .classes.logger import AppLogger

# Create a single instance
db_manager = SupabaseDatabaseManager()

def get_db_session():
    """Database session dependency"""
    with Session(db_manager.engine) as session:
        yield session

def get_supabase_client() -> Client:
    return db_manager.client

# reusable type annotations
DBSession = Annotated[Session, Depends(get_db_session)]
SupabaseClientDep = Annotated[Client, Depends(get_supabase_client)]


app_logger = AppLogger()

def get_logger():
    """Logger dependency"""
    return app_logger.get_logger()

AppLoggerDep = Annotated[logging.Logger, Depends(get_logger)]


_bearer = HTTPBearer()

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),
) -> str:
    try:
        payload = jwt.decode(
            credentials.credentials,
            os.environ.get("JWT_SECRET", ""),
            algorithms=["HS256"],
            options={"verify_aud": False},
        )
        user_id: str = payload.get("sub", "")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token: missing sub")
        return user_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")

CurrentUser = Annotated[str, Depends(get_current_user)]


_bearer_optional = HTTPBearer(auto_error=False)

def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer_optional),
) -> Optional[str]:
    """Like get_current_user, but for endpoints that are public and only need
    to know *who's asking* (if anyone) rather than requiring a session."""
    if credentials is None:
        return None
    try:
        payload = jwt.decode(
            credentials.credentials,
            os.environ.get("JWT_SECRET", ""),
            algorithms=["HS256"],
            options={"verify_aud": False},
        )
        return payload.get("sub") or None
    except jwt.InvalidTokenError:
        return None

OptionalCurrentUser = Annotated[Optional[str], Depends(get_current_user_optional)]
