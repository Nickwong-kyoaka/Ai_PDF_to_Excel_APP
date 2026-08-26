from __future__ import annotations

import hashlib
import secrets
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Annotated

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from fastapi import Cookie, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from .config import get_settings
from .database import get_db
from .models import AuthSession, User, utcnow


SESSION_COOKIE = "formsight_session"
password_hasher = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=2)
settings = get_settings()
_login_attempts: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=10))


def normalize_email(email: str) -> str:
    return email.strip().casefold()


def hash_password(password: str) -> str:
    return password_hasher.hash(password)


def verify_password(password: str, encoded: str) -> bool:
    try:
        return password_hasher.verify(encoded, password)
    except (VerifyMismatchError, InvalidHashError):
        return False


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def check_login_rate_limit(key: str) -> None:
    now = time.monotonic()
    attempts = _login_attempts[key]
    while attempts and now - attempts[0] > 300:
        attempts.popleft()
    if len(attempts) >= 5:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    attempts.append(now)


def clear_login_attempts(key: str) -> None:
    _login_attempts.pop(key, None)


def create_session(db: Session, user: User, response: Response) -> str:
    raw_token = secrets.token_urlsafe(48)
    csrf_token = secrets.token_urlsafe(32)
    expires = datetime.now(timezone.utc) + timedelta(hours=settings.session_hours)
    db.add(
        AuthSession(
            user_id=user.id,
            token_hash=hash_token(raw_token),
            csrf_token=csrf_token,
            expires_at=expires,
        )
    )
    db.commit()
    response.set_cookie(
        SESSION_COOKIE,
        raw_token,
        max_age=settings.session_hours * 3600,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="strict",
        path="/",
    )
    return csrf_token


def destroy_session(db: Session, token: str | None, response: Response) -> None:
    if token:
        db.execute(delete(AuthSession).where(AuthSession.token_hash == hash_token(token)))
        db.commit()
    response.delete_cookie(SESSION_COOKIE, path="/")


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def get_current_session(
    db: Annotated[Session, Depends(get_db)],
    session_token: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
) -> AuthSession:
    if not session_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    auth_session = db.scalar(
        select(AuthSession).where(AuthSession.token_hash == hash_token(session_token))
    )
    if not auth_session or _as_utc(auth_session.expires_at) <= datetime.now(timezone.utc):
        if auth_session:
            db.delete(auth_session)
            db.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired")
    if not auth_session.user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account disabled")
    auth_session.last_seen_at = utcnow()
    return auth_session


def get_current_user(auth_session: Annotated[AuthSession, Depends(get_current_session)]) -> User:
    return auth_session.user


def require_csrf(
    request: Request,
    auth_session: Annotated[AuthSession, Depends(get_current_session)],
    csrf_header: Annotated[str | None, Header(alias="X-CSRF-Token")] = None,
) -> AuthSession:
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        if not csrf_header or not secrets.compare_digest(csrf_header, auth_session.csrf_token):
            raise HTTPException(status_code=403, detail="Invalid CSRF token")
    return auth_session


def require_roles(*roles: str):
    def dependency(auth_session: Annotated[AuthSession, Depends(require_csrf)]) -> User:
        user = auth_session.user
        if user.role not in roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user

    return dependency


CurrentUser = Annotated[User, Depends(get_current_user)]
WriteSession = Annotated[AuthSession, Depends(require_csrf)]
