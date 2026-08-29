from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from backend.app.config import Settings
from backend.app.database import Base
from backend.app.models import ModelProfile, User


LOCAL_EMAIL = "desktop@formsight.local"
LOCAL_PROFILE_SLUG = "desktop-auto-detected"


def local_data_root() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
    return base / "FormSight Local"


def install_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def resource_path(name: str) -> Path:
    candidates = [
        install_root() / name,
        Path(getattr(sys, "_MEIPASS", install_root())) / name,
    ]
    return next((path for path in candidates if path.exists()), candidates[0])


@dataclass(slots=True)
class DesktopRuntime:
    root: Path
    settings: Settings
    sessions: sessionmaker[Session]
    weights_path: Path


def create_runtime(base_url: str) -> DesktopRuntime:
    root = local_data_root()
    root.mkdir(parents=True, exist_ok=True)
    work = root / "work"
    models = root / "models"
    work.mkdir(parents=True, exist_ok=True)
    models.mkdir(parents=True, exist_ok=True)
    database_path = root / "formsight-local.db"
    engine = create_engine(
        f"sqlite:///{database_path.as_posix()}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    @event.listens_for(engine, "connect")
    def configure_sqlite(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()

    Base.metadata.create_all(engine)
    # Tiny forward-only migration support for durable batches created by an earlier desktop build.
    with engine.begin() as connection:
        columns = {
            str(row[1])
            for row in connection.exec_driver_sql("PRAGMA table_info(local_batch_items)").fetchall()
        }
        if "started_at" not in columns:
            connection.exec_driver_sql("ALTER TABLE local_batch_items ADD COLUMN started_at DATETIME")
        if "finished_at" not in columns:
            connection.exec_driver_sql("ALTER TABLE local_batch_items ADD COLUMN finished_at DATETIME")
        if "output_path" not in columns:
            connection.exec_driver_sql("ALTER TABLE local_batch_items ADD COLUMN output_path TEXT")
        if "series_label" not in columns:
            connection.exec_driver_sql(
                "ALTER TABLE local_batch_items ADD COLUMN series_label VARCHAR(160) NOT NULL DEFAULT ''"
            )
        legacy_items = connection.exec_driver_sql(
            "SELECT id, original_path, order_index FROM local_batch_items "
            "WHERE series_label IS NULL OR TRIM(series_label) = ''"
        ).fetchall()
        for item_id, original_path, order_index in legacy_items:
            legacy_label = f"{Path(str(original_path)).stem} legacy {int(order_index) + 1}"
            connection.exec_driver_sql(
                "UPDATE local_batch_items SET series_label = ? WHERE id = ?",
                (legacy_label[:160], str(item_id)),
            )
        batch_columns = {
            str(row[1])
            for row in connection.exec_driver_sql("PRAGMA table_info(local_batches)").fetchall()
        }
        if "verifier_model_id" not in batch_columns:
            connection.exec_driver_sql("ALTER TABLE local_batches ADD COLUMN verifier_model_id VARCHAR(255)")
        answer_columns = {
            str(row[1])
            for row in connection.exec_driver_sql("PRAGMA table_info(answers)").fetchall()
        }
        if "verifier_value" not in answer_columns:
            connection.exec_driver_sql("ALTER TABLE answers ADD COLUMN verifier_value JSON")
        if "verifier_model_id" not in answer_columns:
            connection.exec_driver_sql("ALTER TABLE answers ADD COLUMN verifier_model_id VARCHAR(255)")
    sessions = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
    packaged_weights = resource_path("models/questionnaire_marks.onnx")
    user_weights = models / "questionnaire_marks.onnx"
    weights = user_weights if user_weights.exists() else packaged_weights
    settings = Settings(
        environment="desktop",
        data_dir=work,
        database_url=f"sqlite:///{database_path.as_posix()}",
        cookie_secure=False,
        lmstudio_base_url=f"{base_url.rstrip('/')}/v1",
        lmstudio_api_key="",
        lmstudio_api_key_b64="",
        legacy_v14_path=resource_path("universal_questionnaire_lmstudio_extractor_v14_consensus_geometry.py"),
        yolo_weights=weights,
    )
    settings.ensure_directories()
    return DesktopRuntime(root, settings, sessions, weights)


def ensure_local_identity(db: Session, extractor_model_id: str, judge_model_id: str) -> tuple[User, ModelProfile]:
    user = db.scalar(select(User).where(User.email == LOCAL_EMAIL))
    if not user:
        user = User(
            email=LOCAL_EMAIL,
            display_name="Local Desktop User",
            role="admin",
            password_hash="desktop-direct-access-no-login",
        )
        db.add(user)
        db.flush()
    profile = db.scalar(select(ModelProfile).where(ModelProfile.slug == LOCAL_PROFILE_SLUG))
    if not profile:
        profile = ModelProfile(
            slug=LOCAL_PROFILE_SLUG,
            name="Automatically detected local models",
            extractor_model_id=extractor_model_id,
            judge_model_id=judge_model_id,
            quantization="auto",
            context_length=32768,
            max_concurrency=1,
            image_max_side=3000,
            verification_mode="maximum",
            approved=True,
            is_default=True,
        )
        db.add(profile)
    else:
        profile.extractor_model_id = extractor_model_id
        profile.judge_model_id = judge_model_id
    db.commit()
    return user, profile
