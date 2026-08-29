from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.database import Base
from app.models import Answer, Job, ModelProfile, QuestionnaireGroup, User
from app.scanner.extractor import QuestionnaireExtractor


def test_extract_job_skips_pages_already_checkpointed(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{(tmp_path / 'resume.db').as_posix()}")
    Base.metadata.create_all(engine)
    settings = Settings(
        data_dir=tmp_path / "data",
        legacy_v14_path=tmp_path / "missing.py",
        yolo_weights=tmp_path / "missing.onnx",
    )
    settings.ensure_directories()

    with Session(engine, expire_on_commit=False) as db:
        user = User(
            email="resume@test.local",
            display_name="Resume Test",
            role="operator",
            password_hash="unused",
        )
        profile = ModelProfile(
            slug="resume",
            name="Resume",
            extractor_model_id="qwen",
            judge_model_id="qwen",
        )
        db.add_all([user, profile])
        db.flush()
        job = Job(
            owner_id=user.id,
            profile_id=profile.id,
            filename="two-pages.pdf",
            stored_path=str(tmp_path / "two-pages.pdf"),
            media_type="application/pdf",
            sha256="0" * 64,
            status="extracting",
            page_count=2,
            groups_confirmed=True,
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
        db.add(job)
        db.flush()
        group = QuestionnaireGroup(
            job_id=job.id,
            group_index=0,
            start_page=1,
            end_page=2,
            confirmed=True,
        )
        db.add(group)
        db.flush()
        db.add(
            Answer(
                job_id=job.id,
                group_id=group.id,
                page_number=1,
                question_id="Q1",
                question_text="Checkpointed answer",
                scanner_value="Yes",
                final_value="Yes",
            )
        )
        db.commit()

        extractor = QuestionnaireExtractor(
            settings,
            {
                "extractor_model_id": "qwen",
                "verifier_model_id": "gemma",
                "judge_model_id": "qwen",
                "image_max_side": 1200,
            },
            manage_models=False,
        )
        calls: list[int] = []

        def fake_extract(source, page_number, total_pages, yolo_available, image_max_side=None):
            calls.append(page_number)
            return [], {"checkpoint_complete": True}

        monkeypatch.setattr(extractor, "extract_one_page", fake_extract)
        extractor.extract_job(db, job)

        assert calls == [2]
        saved = list(db.scalars(select(Answer).where(Answer.job_id == job.id)).all())
        assert [(answer.page_number, answer.question_id) for answer in saved] == [(1, "Q1")]
