from __future__ import annotations

import json
import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select, update

from .config import get_settings
from .database import SessionLocal, init_database
from .exports import generate_artifacts
from .models import Job, YoloModel
from .scanner.extractor import QuestionnaireExtractor
from .services import pending_count


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("formsight.worker")
settings = get_settings()
running = True


def stop_worker(_signum=None, _frame=None) -> None:  # type: ignore[no-untyped-def]
    global running
    running = False


def heartbeat(status: str, job_id: str | None = None) -> None:
    path = settings.data_dir / "worker-heartbeat.json"
    path.write_text(
        json.dumps({"time": datetime.now(timezone.utc).isoformat(), "status": status, "job_id": job_id}),
        encoding="utf-8",
    )


def recover_interrupted() -> None:
    with SessionLocal() as db:
        db.execute(
            update(Job)
            .where(Job.status.in_(["extracting", "judging"]))
            .values(status="queued", stage_message="Recovered after worker restart", error=None)
        )
        db.commit()


def claim_next_job() -> str | None:
    with SessionLocal() as db:
        job = db.scalar(select(Job).where(Job.status == "queued").order_by(Job.created_at).limit(1))
        if not job:
            return None
        changed = db.execute(
            update(Job)
            .where(Job.id == job.id, Job.status == "queued")
            .values(status="extracting", stage_message="GPU worker started", progress=0.01)
        )
        db.commit()
        return job.id if changed.rowcount == 1 else None


def process_job(job_id: str) -> None:
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return
        active_yolo = db.scalar(select(YoloModel).where(YoloModel.active.is_(True)).limit(1))
        extractor = QuestionnaireExtractor(
            settings,
            dict(job.profile_snapshot),
            Path(active_yolo.weights_path) if active_yolo else settings.yolo_weights,
        )
        try:
            extractor.extract_job(db, job)
            db.refresh(job)
            if job.status == "cancelled":
                return
            extractor.judge_job(db, job)
            generate_artifacts(db, job, settings, draft=True)
            unresolved = pending_count(db, job.id)
            job.status = "review_needed" if unresolved else "ready"
            job.progress = 1.0
            job.stage_message = f"{unresolved} answer(s) need review" if unresolved else "Ready for finalization"
            job.draft_artifacts_ready = True
            db.commit()
            logger.info("Completed job %s with %s unresolved answers", job.id, unresolved)
        except Exception as exc:
            logger.exception("Job %s failed", job.id)
            job.status = "failed"
            job.error = str(exc)[:4000]
            job.stage_message = "Processing failed; the job can be retried"
            db.commit()


def main() -> None:
    settings.ensure_directories()
    init_database()
    recover_interrupted()
    signal.signal(signal.SIGINT, stop_worker)
    signal.signal(signal.SIGTERM, stop_worker)
    logger.info("FormSight worker started")
    while running:
        job_id = claim_next_job()
        if job_id:
            heartbeat("processing", job_id)
            process_job(job_id)
        else:
            heartbeat("idle")
            time.sleep(settings.worker_poll_seconds)
    heartbeat("stopped")


if __name__ == "__main__":
    main()
