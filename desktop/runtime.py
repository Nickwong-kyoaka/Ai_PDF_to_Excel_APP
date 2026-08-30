from __future__ import annotations

import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
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
    version_marker = root / "schema-v0.6.marker"
    upgrade_from_pre_v06 = database_path.exists() and not version_marker.exists()
    if upgrade_from_pre_v06:
        backup_dir = root / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = backup_dir / f"formsight-local-pre-v0.6-{stamp}.db"
        with sqlite3.connect(database_path) as source_db, sqlite3.connect(backup_path) as backup_db:
            source_db.backup(backup_db)
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
        for name, sql_type in (
            ("expected_questionnaires", "INTEGER"),
            ("pages_per_questionnaire", "INTEGER"),
            ("grouping_confidence", "FLOAT NOT NULL DEFAULT 0"),
            ("grouping_reason", "TEXT NOT NULL DEFAULT ''"),
            ("template_variant", "VARCHAR(80)"),
        ):
            if name not in columns:
                connection.exec_driver_sql(
                    f"ALTER TABLE local_batch_items ADD COLUMN {name} {sql_type}"
                )
        batch_columns = {
            str(row[1])
            for row in connection.exec_driver_sql("PRAGMA table_info(local_batches)").fetchall()
        }
        if "verifier_model_id" not in batch_columns:
            connection.exec_driver_sql("ALTER TABLE local_batches ADD COLUMN verifier_model_id VARCHAR(255)")
        if "processing_mode" not in batch_columns:
            connection.exec_driver_sql(
                "ALTER TABLE local_batches ADD COLUMN processing_mode VARCHAR(32) "
                "NOT NULL DEFAULT 'balanced'"
            )
        answer_columns = {
            str(row[1])
            for row in connection.exec_driver_sql("PRAGMA table_info(answers)").fetchall()
        }
        if "verifier_value" not in answer_columns:
            connection.exec_driver_sql("ALTER TABLE answers ADD COLUMN verifier_value JSON")
        if "verifier_model_id" not in answer_columns:
            connection.exec_driver_sql("ALTER TABLE answers ADD COLUMN verifier_model_id VARCHAR(255)")
        for name, sql_type in (
            ("answer_key", "VARCHAR(512)"),
            ("page_ordinal", "INTEGER"),
            ("template_question_id", "VARCHAR(240)"),
            ("geometry_value", "JSON"),
            ("geometry_confidence", "FLOAT NOT NULL DEFAULT 0"),
        ):
            if name not in answer_columns:
                connection.exec_driver_sql(f"ALTER TABLE answers ADD COLUMN {name} {sql_type}")
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_answers_answer_key ON answers (answer_key)"
        )
        if upgrade_from_pre_v06:
            connection.exec_driver_sql(
                "UPDATE local_batches SET status = 'legacy_requires_restart', "
                "stage_message = 'Created by v0.5; restart with v0.6 grouping' "
                "WHERE status NOT IN ('completed', 'failed', 'cancelled', 'legacy_requires_restart')"
            )
    version_marker.write_text("0.6.0\n", encoding="utf-8")
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
            context_length=12288,
            max_concurrency=1,
            image_max_side=2000,
            verification_mode="selective",
            approved=True,
            is_default=True,
        )
        db.add(profile)
    else:
        profile.extractor_model_id = extractor_model_id
        profile.judge_model_id = judge_model_id
        profile.context_length = 12288
        profile.image_max_side = 2000
        profile.verification_mode = "selective"
    db.commit()
    return user, profile
