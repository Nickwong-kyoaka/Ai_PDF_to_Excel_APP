from __future__ import annotations

import json
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from ..audit import record_audit
from ..config import get_settings
from ..database import SessionLocal, get_db
from ..documents import inspect_and_render, propose_groups, validate_group_partition
from ..exports import generate_artifacts, result_payload
from ..models import (
    Answer,
    Artifact,
    AuthSession,
    Job,
    ModelProfile,
    QuestionnaireGroup,
    ReviewEvent,
    new_id,
    utcnow,
)
from ..schemas import ConfirmGroupsRequest, ReviewRequest
from ..scanner.grouping import visual_grouping
from ..scanner.lmstudio import LMStudioGateway
from ..security import get_current_session, require_csrf, require_roles
from ..services import accessible_job, pending_count
from ..storage import safe_filename, save_upload


router = APIRouter(prefix="/jobs", tags=["jobs"])
settings = get_settings()


def job_payload(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "filename": job.filename,
        "media_type": job.media_type,
        "status": job.status,
        "page_count": job.page_count,
        "language": job.language,
        "groups_confirmed": job.groups_confirmed,
        "progress": job.progress,
        "stage_message": job.stage_message,
        "error": job.error,
        "draft_artifacts_ready": job.draft_artifacts_ready,
        "profile_snapshot": job.profile_snapshot,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "expires_at": job.expires_at,
        "groups": [
            {
                "id": group.id,
                "group_index": group.group_index,
                "start_page": group.start_page,
                "end_page": group.end_page,
                "participant_id": group.participant_id,
                "confidence": group.confidence,
                "reason": group.reason,
                "confirmed": group.confirmed,
            }
            for group in sorted(job.groups, key=lambda item: item.group_index)
        ],
        "artifacts": [
            {
                "id": artifact.id,
                "kind": artifact.kind,
                "draft": artifact.draft,
                "filename": artifact.filename,
                "created_at": artifact.created_at,
            }
            for artifact in sorted(job.artifacts, key=lambda item: item.created_at)
        ],
    }


@router.get("")
def list_jobs(
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(require_csrf)],
):
    statement = select(Job).order_by(Job.created_at.desc())
    if auth_session.user.role == "operator":
        statement = statement.where(Job.owner_id == auth_session.user_id)
    return [job_payload(job) for job in db.scalars(statement).unique().all()]


@router.post("", status_code=201)
async def create_job(
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(require_csrf)],
    file: UploadFile = File(...),
    profile_id: str | None = Form(default=None),
    language: str = Form(default="auto"),
):
    profile = db.get(ModelProfile, profile_id) if profile_id else db.scalar(
        select(ModelProfile).where(ModelProfile.is_default.is_(True), ModelProfile.approved.is_(True))
    )
    if not profile or not profile.approved:
        raise HTTPException(status_code=400, detail="Choose an approved model profile")
    job_id = new_id()
    filename = safe_filename(file.filename or "questionnaire.pdf")
    upload_dir = settings.uploads_dir / job_id
    destination = upload_dir / filename
    sha256, _size = await save_upload(file, destination, settings.max_upload_mb * 1024 * 1024)
    try:
        info = inspect_and_render(destination, settings.pages_dir / job_id, max_pages=settings.max_pages)
    except ValueError as exc:
        shutil.rmtree(upload_dir, ignore_errors=True)
        shutil.rmtree(settings.pages_dir / job_id, ignore_errors=True)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    snapshot = {
        "profile_id": profile.id,
        "slug": profile.slug,
        "name": profile.name,
        "extractor_model_id": profile.extractor_model_id,
        "judge_model_id": profile.judge_model_id,
        "quantization": profile.quantization,
        "context_length": profile.context_length,
        "max_concurrency": profile.max_concurrency,
        "image_max_side": profile.image_max_side,
        "verification_mode": profile.verification_mode,
    }
    group_proposals = propose_groups(info.embedded_text)
    if info.page_count > 1 and len(group_proposals) == 1 and group_proposals[0].confidence < 0.5:
        try:
            group_proposals = visual_grouping(
                info.page_images,
                LMStudioGateway(settings.lmstudio_base_url, settings.lmstudio_token),
                profile.extractor_model_id,
            )
        except Exception:
            # A safe one-document proposal remains available when the model server is offline.
            pass
    job = Job(
        id=job_id,
        owner_id=auth_session.user_id,
        profile_id=profile.id,
        filename=filename,
        stored_path=str(destination.resolve()),
        media_type=file.content_type or "application/octet-stream",
        sha256=sha256,
        status="awaiting_confirmation",
        page_count=info.page_count,
        language=language,
        profile_snapshot=snapshot,
        stage_message="Confirm the proposed questionnaire page groups",
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.retention_days),
    )
    db.add(job)
    for index, group in enumerate(group_proposals, start=1):
        db.add(
            QuestionnaireGroup(
                job_id=job.id,
                group_index=index,
                start_page=group.start_page,
                end_page=group.end_page,
                participant_id=group.participant_id,
                confidence=group.confidence,
                reason=group.reason,
            )
        )
    record_audit(db, "job.created", actor_id=auth_session.user_id, job_id=job.id, metadata={"sha256": sha256})
    db.commit()
    db.refresh(job)
    return job_payload(job)


@router.get("/{job_id}")
def get_job(
    job_id: str,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(require_csrf)],
):
    return job_payload(accessible_job(db, job_id, auth_session.user))


@router.post("/{job_id}/groups/confirm")
def confirm_groups(
    job_id: str,
    payload: ConfirmGroupsRequest,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(require_csrf)],
):
    job = accessible_job(db, job_id, auth_session.user)
    if job.status not in {"awaiting_confirmation", "failed"}:
        raise HTTPException(status_code=409, detail="Page groups can only be changed before processing")
    try:
        validate_group_partition([(group.start_page, group.end_page) for group in payload.groups], job.page_count)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    db.execute(delete(QuestionnaireGroup).where(QuestionnaireGroup.job_id == job.id))
    db.flush()
    for index, group in enumerate(sorted(payload.groups, key=lambda item: item.start_page), start=1):
        db.add(
            QuestionnaireGroup(
                job_id=job.id,
                group_index=index,
                start_page=group.start_page,
                end_page=group.end_page,
                participant_id=group.participant_id,
                confidence=1.0,
                reason="Confirmed by operator",
                confirmed=True,
            )
        )
    job.groups_confirmed = True
    job.status = "queued"
    job.stage_message = "Waiting for the GPU worker"
    job.error = None
    record_audit(db, "job.groups_confirmed", actor_id=auth_session.user_id, job_id=job.id)
    db.commit()
    db.refresh(job)
    return job_payload(job)


@router.post("/{job_id}/cancel")
def cancel_job(
    job_id: str,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(require_csrf)],
):
    job = accessible_job(db, job_id, auth_session.user)
    if job.status in {"finalized", "purged"}:
        raise HTTPException(status_code=409, detail="This job cannot be cancelled")
    job.cancel_requested = True
    if job.status in {"queued", "awaiting_confirmation"}:
        job.status = "cancelled"
        job.stage_message = "Cancelled by user"
    record_audit(db, "job.cancel_requested", actor_id=auth_session.user_id, job_id=job.id)
    db.commit()
    return job_payload(job)


@router.post("/{job_id}/retry")
def retry_job(
    job_id: str,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(require_csrf)],
):
    job = accessible_job(db, job_id, auth_session.user)
    if job.status not in {"failed", "cancelled"} or not job.groups_confirmed:
        raise HTTPException(status_code=409, detail="Only failed or cancelled confirmed jobs can be retried")
    job.status = "queued"
    job.cancel_requested = False
    job.progress = 0
    job.error = None
    job.stage_message = "Queued for retry"
    record_audit(db, "job.retried", actor_id=auth_session.user_id, job_id=job.id)
    db.commit()
    return job_payload(job)


@router.get("/{job_id}/result")
def get_result(
    job_id: str,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(get_current_session)],
):
    job = accessible_job(db, job_id, auth_session.user)
    return result_payload(db, job)


@router.post("/{job_id}/answers/{answer_id}/review")
def review_answer(
    job_id: str,
    answer_id: str,
    payload: ReviewRequest,
    db: Annotated[Session, Depends(get_db)],
    reviewer=Depends(require_roles("admin", "reviewer")),
):
    job = accessible_job(db, job_id, reviewer)
    answer = db.get(Answer, answer_id)
    if not answer or answer.job_id != job.id:
        raise HTTPException(status_code=404, detail="Answer not found")
    previous = answer.final_value
    if payload.action == "accept_qwen":
        if answer.judge_suggestion is None:
            raise HTTPException(status_code=409, detail="This answer has no Qwen suggestion")
        answer.final_value = answer.judge_suggestion
        answer.final_source = "qwen_judge_accepted"
    elif payload.action == "revert_scanner":
        answer.final_value = answer.scanner_value
        answer.final_source = "scanner_reverted"
    else:
        if payload.value is None:
            raise HTTPException(status_code=422, detail="An edited value is required")
        answer.final_value = payload.value
        answer.final_source = "human"
    answer.review_status = "resolved"
    answer.reviewer_id = reviewer.id
    answer.reviewed_at = utcnow()
    answer.review_comment = payload.comment
    db.add(
        ReviewEvent(
            answer_id=answer.id,
            reviewer_id=reviewer.id,
            action=payload.action,
            previous_value=previous,
            new_value=answer.final_value,
            comment=payload.comment,
        )
    )
    db.flush()
    remaining = pending_count(db, job.id)
    if remaining <= 0:
        job.status = "ready"
        job.stage_message = "All flagged answers have been reviewed"
    record_audit(db, "answer.reviewed", actor_id=reviewer.id, job_id=job.id, metadata={"answer_id": answer.id, "action": payload.action})
    db.commit()
    return {"answer": result_payload(db, job)["answers"], "remaining": max(0, pending_count(db, job.id))}


@router.post("/{job_id}/finalize")
def finalize_job(
    job_id: str,
    db: Annotated[Session, Depends(get_db)],
    reviewer=Depends(require_roles("admin", "reviewer")),
):
    job = accessible_job(db, job_id, reviewer)
    unresolved = pending_count(db, job.id)
    if unresolved:
        raise HTTPException(status_code=409, detail=f"Resolve {unresolved} flagged answer(s) before final export")
    generate_artifacts(db, job, settings, draft=False)
    job.status = "finalized"
    job.stage_message = "Final export approved"
    record_audit(db, "job.finalized", actor_id=reviewer.id, job_id=job.id)
    db.commit()
    db.refresh(job)
    return job_payload(job)


@router.get("/{job_id}/artifacts/{artifact_id}")
def download_artifact(
    job_id: str,
    artifact_id: str,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(get_current_session)],
):
    job = accessible_job(db, job_id, auth_session.user)
    artifact = db.get(Artifact, artifact_id)
    if not artifact or artifact.job_id != job.id:
        raise HTTPException(status_code=404, detail="Artifact not found")
    path = Path(artifact.stored_path)
    if not path.exists():
        raise HTTPException(status_code=410, detail="Artifact has expired")
    return FileResponse(path, filename=artifact.filename)


@router.get("/{job_id}/pages/{page_number}")
def page_preview(
    job_id: str,
    page_number: int,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(get_current_session)],
):
    job = accessible_job(db, job_id, auth_session.user)
    if page_number < 1 or page_number > job.page_count:
        raise HTTPException(status_code=404, detail="Page not found")
    path = settings.pages_dir / job.id / f"page-{page_number:04d}.jpg"
    if not path.exists():
        raise HTTPException(status_code=410, detail="Page preview has expired")
    return FileResponse(path, media_type="image/jpeg")


@router.get("/{job_id}/events")
def job_events(
    job_id: str,
    db: Annotated[Session, Depends(get_db)],
    auth_session: Annotated[AuthSession, Depends(get_current_session)],
):
    accessible_job(db, job_id, auth_session.user)
    user_id, role = auth_session.user_id, auth_session.user.role

    def stream():
        last = ""
        for _ in range(900):
            with SessionLocal() as poll_db:
                job = poll_db.get(Job, job_id)
                if not job or (role == "operator" and job.owner_id != user_id):
                    yield "event: error\ndata: {\"detail\":\"Job unavailable\"}\n\n"
                    return
                data = json.dumps({"status": job.status, "progress": job.progress, "message": job.stage_message})
                if data != last:
                    yield f"data: {data}\n\n"
                    last = data
                if job.status in {"review_needed", "ready", "finalized", "failed", "cancelled", "purged"}:
                    return
            time.sleep(1)

    return StreamingResponse(stream(), media_type="text/event-stream")
