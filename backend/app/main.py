from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .bootstrap import bootstrap_defaults
from .config import get_settings
from .database import SessionLocal, init_database
from .retention import purge_expired
from .routers import admin, auth, jobs, system


settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings.ensure_directories()
    init_database()
    with SessionLocal() as db:
        bootstrap_defaults(db, settings)
        purge_expired(db, settings)
    yield


app = FastAPI(
    title="FormSight API",
    version="0.1.0",
    description="Private bilingual questionnaire scanning with Qwen, YOLO, and human review.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.frontend_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-CSRF-Token"],
)
app.include_router(system.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(jobs.router, prefix="/api")
app.include_router(admin.router, prefix="/api")
