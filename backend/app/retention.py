from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from .config import Settings
from .models import Answer, Artifact, AuditEvent, Job, QuestionnaireGroup


def purge_expired(db: Session, settings: Settings) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    jobs = db.scalars(select(Job).where(Job.expires_at < now)).all()
    files_removed = 0
    for job in jobs:
        paths = {Path(job.stored_path).parent, settings.pages_dir / job.id, settings.artifacts_dir / job.id}
        for path in paths:
            if path.exists() and path.resolve() != settings.data_dir.resolve():
                shutil.rmtree(path, ignore_errors=True)
                files_removed += 1
        db.execute(delete(Artifact).where(Artifact.job_id == job.id))
        db.execute(delete(Answer).where(Answer.job_id == job.id))
        groups = db.scalars(select(QuestionnaireGroup).where(QuestionnaireGroup.job_id == job.id)).all()
        for group in groups:
            group.participant_id = None
            group.reason = "Purged after retention period"
        job.filename = "purged"
        job.stored_path = "purged"
        job.sha256 = ""
        job.error = None
        job.stage_message = "Personal data purged after retention period"
        job.status = "purged"
        job.profile_snapshot = {}
        db.add(AuditEvent(job_id=job.id, action="retention.purged", metadata_json={"expired_at": job.expires_at.isoformat()}))
    db.commit()
    return {"jobs_purged": len(jobs), "directories_removed": files_removed}
