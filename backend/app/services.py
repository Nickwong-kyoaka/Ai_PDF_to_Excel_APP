from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import Answer, Job, User


def accessible_job(db: Session, job_id: str, user: User) -> Job:
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if user.role == "operator" and job.owner_id != user.id:
        raise HTTPException(status_code=403, detail="You do not have access to this job")
    return job


def pending_count(db: Session, job_id: str) -> int:
    return int(
        db.scalar(
            select(func.count(Answer.id)).where(Answer.job_id == job_id, Answer.review_status == "pending")
        )
        or 0
    )
