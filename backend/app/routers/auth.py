from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..audit import record_audit
from ..database import get_db
from ..models import AuthSession, User
from ..schemas import LoginRequest, LoginResponse, UserPublic
from ..security import (
    SESSION_COOKIE,
    check_login_rate_limit,
    clear_login_attempts,
    create_session,
    destroy_session,
    get_current_session,
    get_current_user,
    normalize_email,
    require_csrf,
    verify_password,
)


router = APIRouter(prefix="/auth", tags=["authentication"])


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, request: Request, response: Response, db: Annotated[Session, Depends(get_db)]):
    email = normalize_email(payload.email)
    key = f"{request.client.host if request.client else 'unknown'}:{email}"
    check_login_rate_limit(key)
    user = db.scalar(select(User).where(User.email == email))
    if not user or not user.is_active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    clear_login_attempts(key)
    csrf_token = create_session(db, user, response)
    record_audit(db, "auth.login", actor_id=user.id)
    db.commit()
    return LoginResponse(user=UserPublic.model_validate(user), csrf_token=csrf_token)


@router.post("/logout", status_code=204)
def logout(
    response: Response,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(require_csrf)],
    session_token: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
):
    record_audit(db, "auth.logout", actor_id=auth_session.user_id)
    destroy_session(db, session_token, response)
    response.status_code = 204
    return response


@router.get("/me", response_model=LoginResponse)
def me(auth_session: Annotated[AuthSession, Depends(get_current_session)]):
    return LoginResponse(user=UserPublic.model_validate(auth_session.user), csrf_token=auth_session.csrf_token)
