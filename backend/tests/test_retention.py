from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select

from app.config import get_settings
from app.database import SessionLocal
from app.models import Answer, Job, ModelProfile, QuestionnaireGroup, User
from app.retention import purge_expired


def test_retention_removes_pii_and_files(client):
    settings = get_settings()
    with SessionLocal() as db:
        user = db.scalar(select(User).limit(1))
        profile = db.scalar(select(ModelProfile).limit(1))
        directory = settings.uploads_dir / "expired-job"
        directory.mkdir(parents=True, exist_ok=True)
        source = directory / "private.pdf"
        source.write_bytes(b"private")
        job = Job(
            id="expired-job",
            owner_id=user.id,
            profile_id=profile.id,
            filename="private.pdf",
            stored_path=str(source),
            media_type="application/pdf",
            sha256="secret-sha",
            status="finalized",
            page_count=1,
            profile_snapshot={"secret": "model"},
            expires_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
        db.add(job); db.flush()
        group = QuestionnaireGroup(job_id=job.id, group_index=1, start_page=1, end_page=1, participant_id="CSA999", reason="PII")
        db.add(group); db.flush()
        db.add(Answer(job_id=job.id, group_id=group.id, page_number=1, question_id="name", question_text="Name", scanner_value="Private Person", final_value="Private Person"))
        db.commit()
        result = purge_expired(db, settings)
        db.refresh(job); db.refresh(group)
        assert result["jobs_purged"] >= 1
        assert job.filename == "purged"
        assert job.sha256 == ""
        assert job.profile_snapshot == {}
        assert group.participant_id is None
        assert not directory.exists()
        assert db.scalar(select(Answer).where(Answer.job_id == job.id)) is None
