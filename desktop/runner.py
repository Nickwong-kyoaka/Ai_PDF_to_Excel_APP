from __future__ import annotations

import hashlib
import mimetypes
import re
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from sqlalchemy import delete, select

from backend.app.documents import ProposedGroup, inspect_and_render, propose_groups, validate_group_partition
from backend.app.models import Answer, Job, LocalBatch, LocalBatchItem, QuestionnaireGroup, new_id, utcnow
from backend.app.scanner.extractor import QuestionnaireExtractor
from backend.app.scanner.grouping import visual_grouping
from backend.app.scanner.lmstudio import LMStudioGateway

from .exporter import write_source_excel
from .model_discovery import DiscoveryResult
from .runtime import DesktopRuntime, ensure_local_identity


ALLOWED_SUFFIXES = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}


@dataclass(slots=True, frozen=True)
class GroupDraft:
    job_id: str
    source_file: str
    page_count: int
    group_index: int
    start_page: int
    end_page: int
    participant_id: str | None
    confidence: float
    reason: str


@dataclass(slots=True, frozen=True)
class RunnerEvent:
    batch_id: str
    stage: str
    progress: float
    message: str
    source_index: int | None = None


ProgressCallback = Callable[[RunnerEvent], None]


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^\w.()\[\] -]+", "_", Path(value).name, flags=re.UNICODE).strip(" .")
    return cleaned[:180] or "questionnaire"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class LocalBatchRunner:
    def __init__(self, runtime: DesktopRuntime, callback: ProgressCallback | None = None):
        self.runtime = runtime
        self.callback = callback
        self.cancel_event = threading.Event()

    def _emit(
        self,
        batch_id: str,
        stage: str,
        progress: float,
        message: str,
        source_index: int | None = None,
    ) -> None:
        if self.callback:
            self.callback(RunnerEvent(batch_id, stage, max(0.0, min(1.0, progress)), message, source_index))

    def request_cancel(self) -> None:
        self.cancel_event.set()

    def create_batch(
        self,
        sources: Iterable[str | Path],
        output_path: str | Path,
        discovery: DiscoveryResult,
        *,
        review_groups: bool,
        extractor_model_id: str | None = None,
        judge_model_id: str | None = None,
    ) -> str:
        source_paths: list[Path] = []
        seen: set[str] = set()
        for source in sources:
            path = Path(source).expanduser().resolve()
            key = str(path).casefold()
            if key not in seen:
                source_paths.append(path)
                seen.add(key)
        if not source_paths:
            raise ValueError("Select at least one PDF or image")
        for source in source_paths:
            if source.suffix.casefold() not in ALLOWED_SUFFIXES:
                raise ValueError(f"Unsupported file type: {source.name}")
            if not source.is_file():
                raise ValueError(f"File not found: {source}")
        if discovery.status != "ready" or not discovery.selected_vision:
            raise ValueError(discovery.message or "LM Studio is not ready")

        vision_id = extractor_model_id or discovery.selected_vision.api_id
        judge_id = judge_model_id or (discovery.selected_judge or discovery.selected_vision).api_id
        output = Path(output_path).expanduser().resolve()
        if output.exists() and not output.is_dir():
            raise ValueError("The output location must be a folder")
        output.mkdir(parents=True, exist_ok=True)
        reserved_outputs = {
            child.name.casefold() for child in output.iterdir() if child.is_file()
        }

        output_paths: list[Path] = []
        for source in source_paths:
            stem = _safe_filename(source.stem)
            candidate = output / f"{stem}_FormSight.xlsx"
            suffix = 2
            while candidate.name.casefold() in reserved_outputs:
                candidate = output / f"{stem}_FormSight_{suffix}.xlsx"
                suffix += 1
            reserved_outputs.add(candidate.name.casefold())
            output_paths.append(candidate)
        batch_id = new_id()
        batch_root = self.runtime.settings.data_dir / "batches" / batch_id
        uploads_root = batch_root / "uploads"
        uploads_root.mkdir(parents=True, exist_ok=True)

        with self.runtime.sessions() as db:
            batch = LocalBatch(
                id=batch_id,
                status="preparing",
                output_path=str(output),
                review_groups=review_groups,
                lmstudio_base_url=discovery.base_url,
                extractor_model_id=vision_id,
                judge_model_id=judge_id,
                stage_message="Preparing files / 正在準備檔案",
            )
            db.add(batch)
            for index, source in enumerate(source_paths):
                item_id = new_id()
                destination = uploads_root / item_id / _safe_filename(source.name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
                db.add(
                    LocalBatchItem(
                        id=item_id,
                        batch_id=batch_id,
                        order_index=index,
                        original_path=str(source),
                        stored_path=str(destination),
                        output_path=str(output_paths[index]),
                        status="pending",
                    )
                )
            db.commit()
        self.prepare_batch(batch_id)
        return batch_id

    def prepare_batch(self, batch_id: str) -> None:
        with self.runtime.sessions() as db:
            batch = db.get(LocalBatch, batch_id)
            if not batch:
                raise ValueError("Local batch not found")
            user, profile = ensure_local_identity(db, batch.extractor_model_id, batch.judge_model_id)
            items = list(
                db.scalars(
                    select(LocalBatchItem)
                    .where(LocalBatchItem.batch_id == batch.id)
                    .order_by(LocalBatchItem.order_index.asc())
                ).all()
            )
            batch.status = "preparing"
            batch.error = None
            db.commit()

            for item_index, item in enumerate(items):
                if self.cancel_event.is_set():
                    batch.status = "paused"
                    batch.stage_message = "Paused / 已暫停"
                    db.commit()
                    return
                if item.job_id:
                    continue
                overall = (item_index / max(1, len(items))) * 0.12
                self._emit(batch.id, "preparing", overall, f"Inspecting {Path(item.original_path).name}", item_index)
                item.status = "preparing"
                item.started_at = utcnow()
                db.commit()
                job_id = new_id()
                try:
                    info = inspect_and_render(
                        Path(item.stored_path),
                        self.runtime.settings.pages_dir / job_id,
                        max_pages=self.runtime.settings.max_pages,
                    )
                    proposals = propose_groups(info.embedded_text)
                    if info.page_count > 1 and len(proposals) == 1 and proposals[0].confidence < 0.5:
                        try:
                            proposals = visual_grouping(
                                info.page_images,
                                LMStudioGateway(batch.lmstudio_base_url, ""),
                                batch.extractor_model_id,
                            )
                        except Exception as exc:
                            proposals[0].reason += f"; visual grouping unavailable: {str(exc)[:120]}"
                    snapshot = {
                        "profile_id": profile.id,
                        "slug": profile.slug,
                        "name": profile.name,
                        "extractor_model_id": batch.extractor_model_id,
                        "judge_model_id": batch.judge_model_id,
                        "quantization": profile.quantization,
                        "context_length": profile.context_length,
                        "max_concurrency": 1,
                        "image_max_side": profile.image_max_side,
                        "verification_mode": "maximum",
                        "local_desktop": True,
                    }
                    job = Job(
                        id=job_id,
                        owner_id=user.id,
                        profile_id=profile.id,
                        filename=Path(item.original_path).name,
                        stored_path=item.stored_path,
                        media_type=mimetypes.guess_type(item.stored_path)[0] or "application/octet-stream",
                        sha256=_sha256(Path(item.stored_path)),
                        status="awaiting_confirmation" if batch.review_groups else "queued",
                        page_count=info.page_count,
                        language="auto",
                        profile_snapshot=snapshot,
                        groups_confirmed=not batch.review_groups,
                        stage_message=(
                            "Confirm questionnaire page groups"
                            if batch.review_groups
                            else "Ready for sequential processing"
                        ),
                        expires_at=datetime.now(timezone.utc) + timedelta(days=self.runtime.settings.retention_days),
                    )
                    db.add(job)
                    db.flush()
                    for group_index, proposal in enumerate(proposals):
                        db.add(
                            QuestionnaireGroup(
                                job_id=job.id,
                                group_index=group_index,
                                start_page=proposal.start_page,
                                end_page=proposal.end_page,
                                participant_id=proposal.participant_id,
                                confidence=proposal.confidence,
                                reason=proposal.reason,
                                confirmed=not batch.review_groups,
                            )
                        )
                    item.job_id = job.id
                    item.status = "awaiting_confirmation" if batch.review_groups else "queued"
                    item.error = None
                except Exception as exc:
                    item.status = "failed"
                    item.error = str(exc)[:2000]
                    item.finished_at = utcnow()
                db.commit()

            batch.status = "awaiting_confirmation" if batch.review_groups else "queued"
            batch.stage_message = (
                "Review questionnaire page groups / 請檢查問卷頁面分組"
                if batch.review_groups
                else "Ready to scan / 準備掃描"
            )
            batch.progress = 0.12
            db.commit()
            self._emit(batch.id, batch.status, 0.12, batch.stage_message)

    def group_drafts(self, batch_id: str) -> list[GroupDraft]:
        with self.runtime.sessions() as db:
            items = list(
                db.scalars(
                    select(LocalBatchItem)
                    .where(LocalBatchItem.batch_id == batch_id, LocalBatchItem.job_id.is_not(None))
                    .order_by(LocalBatchItem.order_index.asc())
                ).all()
            )
            drafts: list[GroupDraft] = []
            for item in items:
                job = db.get(Job, item.job_id)
                if not job:
                    continue
                groups = list(
                    db.scalars(
                        select(QuestionnaireGroup)
                        .where(QuestionnaireGroup.job_id == job.id)
                        .order_by(QuestionnaireGroup.group_index.asc())
                    ).all()
                )
                drafts.extend(
                    GroupDraft(
                        job_id=job.id,
                        source_file=Path(item.original_path).name,
                        page_count=job.page_count,
                        group_index=group.group_index,
                        start_page=group.start_page,
                        end_page=group.end_page,
                        participant_id=group.participant_id,
                        confidence=group.confidence,
                        reason=group.reason,
                    )
                    for group in groups
                )
            return drafts

    def confirm_groups(self, batch_id: str, groups_by_job: dict[str, list[ProposedGroup]]) -> None:
        with self.runtime.sessions() as db:
            batch = db.get(LocalBatch, batch_id)
            if not batch:
                raise ValueError("Local batch not found")
            items = list(
                db.scalars(select(LocalBatchItem).where(LocalBatchItem.batch_id == batch.id)).all()
            )
            for item in items:
                if not item.job_id:
                    continue
                job = db.get(Job, item.job_id)
                proposals = groups_by_job.get(job.id if job else "")
                if not job or proposals is None:
                    raise ValueError(f"Missing group confirmation for {Path(item.original_path).name}")
                validate_group_partition(
                    [(group.start_page, group.end_page) for group in proposals], job.page_count
                )
                db.execute(delete(QuestionnaireGroup).where(QuestionnaireGroup.job_id == job.id))
                db.flush()
                for group_index, proposal in enumerate(sorted(proposals, key=lambda group: group.start_page)):
                    db.add(
                        QuestionnaireGroup(
                            job_id=job.id,
                            group_index=group_index,
                            start_page=proposal.start_page,
                            end_page=proposal.end_page,
                            participant_id=proposal.participant_id,
                            confidence=1.0,
                            reason="Confirmed in FormSight Local",
                            confirmed=True,
                        )
                    )
                job.groups_confirmed = True
                job.status = "queued"
                job.stage_message = "Ready for sequential processing"
                item.status = "queued"
            batch.status = "queued"
            batch.stage_message = "Ready to scan / 準備掃描"
            db.commit()

    def execute_batch(self, batch_id: str) -> dict[str, object] | None:
        self.cancel_event.clear()
        with self.runtime.sessions() as db:
            batch = db.get(LocalBatch, batch_id)
            if not batch:
                raise ValueError("Local batch not found")
            if batch.status == "awaiting_confirmation":
                raise ValueError("Confirm questionnaire page groups before scanning")
            items = list(
                db.scalars(
                    select(LocalBatchItem)
                    .where(LocalBatchItem.batch_id == batch.id)
                    .order_by(LocalBatchItem.order_index.asc())
                ).all()
            )
            profile = {
                "extractor_model_id": batch.extractor_model_id,
                "judge_model_id": batch.judge_model_id,
                "image_max_side": 3000,
                "verification_mode": "maximum",
            }
            batch.status = "running"
            batch.error = None
            batch.stage_message = "Scanning sequentially / 正在依次掃描"
            db.commit()

            results: list[dict[str, object]] = []
            for item_index, item in enumerate(items):
                if self.cancel_event.is_set():
                    self._pause(db, batch, item)
                    return None

                if item.status == "completed":
                    results.append(self._write_item_workbook(db, batch, item))
                    continue
                job = db.get(Job, item.job_id) if item.job_id else None
                if not job:
                    item.status = "failed"
                    item.error = item.error or "Prepared job is missing"
                    item.finished_at = item.finished_at or utcnow()
                    db.commit()
                    results.append(self._write_item_workbook(db, batch, item))
                    continue

                item.status = "running"
                item.started_at = utcnow()
                item.finished_at = None
                item.error = None
                job.cancel_requested = False
                job.status = "extracting"
                job.error = None
                db.commit()

                def page_progress(stage: str, fraction: float, message: str) -> None:
                    overall = 0.12 + ((item_index + fraction) / max(1, len(items))) * 0.80
                    batch.progress = overall
                    batch.stage_message = message
                    db.commit()
                    self._emit(batch.id, stage, overall, message, item_index)

                extractor = QuestionnaireExtractor(
                    self.runtime.settings,
                    profile,
                    self.runtime.weights_path if self.runtime.weights_path.exists() else None,
                    manage_models=False,
                    progress_callback=page_progress,
                    cancel_check=self.cancel_event.is_set,
                )
                try:
                    extractor.extract_job(db, job)
                    if self.cancel_event.is_set() or job.status == "cancelled":
                        self._pause(db, batch, item)
                        return None
                    extractor.judge_job(db, job)
                    if self.cancel_event.is_set() or job.status == "cancelled":
                        self._pause(db, batch, item)
                        return None
                    pending = db.scalar(
                        select(Answer.id).where(Answer.job_id == job.id, Answer.review_status == "pending").limit(1)
                    )
                    job.status = "review_needed" if pending else "ready"
                    job.progress = 1.0
                    job.stage_message = "Completed for its source workbook"
                    item.status = "completed"
                    item.finished_at = utcnow()
                except Exception as exc:
                    job.status = "failed"
                    job.error = str(exc)[:2000]
                    job.stage_message = "Input failed; continuing batch"
                    item.status = "failed"
                    item.error = str(exc)[:2000]
                    item.finished_at = utcnow()
                finally:
                    extractor.yolo.release()
                db.commit()
                export_progress = 0.12 + ((item_index + 0.95) / max(1, len(items))) * 0.86
                batch.status = "exporting"
                batch.progress = export_progress
                batch.stage_message = f"Creating Excel for {Path(item.original_path).name}"
                db.commit()
                self._emit(batch.id, "exporting", export_progress, batch.stage_message, item_index)
                try:
                    results.append(self._write_item_workbook(db, batch, item))
                except Exception as exc:
                    previous = item.error
                    item.status = "export_failed"
                    item.error = f"{previous}; Excel export failed: {exc}" if previous else f"Excel export failed: {exc}"
                    item.error = item.error[:2000]
                    db.commit()

            failed = sum(item.status in {"failed", "export_failed"} for item in items)
            flags = sum(int(result.get("flags", 0)) for result in results)
            status_label = "COMPLETED — FLAGS PRESENT" if failed or flags else "COMPLETED"
            batch.status = "completed"
            batch.progress = 1.0
            batch.stage_message = status_label
            batch.completed_at = utcnow()
            batch.error = None
            db.commit()
            self._emit(batch.id, "completed", 1.0, batch.stage_message)
            paths = [str(result["path"]) for result in results if result.get("path")]
            return {
                "paths": paths,
                "output_directory": batch.output_path,
                "status": status_label,
                "source_files": len(items),
                "workbooks": len(paths),
                "failed": failed,
                "flags": flags,
            }

    def _write_item_workbook(
        self,
        db,
        batch: LocalBatch,
        item: LocalBatchItem,
    ) -> dict[str, object]:
        if not item.output_path:
            output_directory = Path(batch.output_path)
            if output_directory.suffix.casefold() == ".xlsx":
                output_directory = output_directory.parent
            output_directory.mkdir(parents=True, exist_ok=True)
            item.output_path = str(
                output_directory / f"{_safe_filename(Path(item.original_path).stem)}_FormSight.xlsx"
            )
            db.commit()
        return write_source_excel(db, batch, item, item.output_path)

    def _pause(self, db, batch: LocalBatch, current_item: LocalBatchItem) -> None:
        current_item.status = "paused"
        current_item.error = "Paused by user; this input will restart from page 1 when resumed."
        batch.status = "paused"
        batch.stage_message = "Paused — completed inputs are preserved / 已暫停，完成的檔案已保留"
        db.commit()
        self._emit(batch.id, "paused", batch.progress, batch.stage_message, current_item.order_index)

    def resume_batch(self, batch_id: str) -> dict[str, object] | None:
        with self.runtime.sessions() as db:
            batch = db.get(LocalBatch, batch_id)
            if not batch:
                raise ValueError("Local batch not found")
            items = list(db.scalars(select(LocalBatchItem).where(LocalBatchItem.batch_id == batch.id)).all())
            for item in items:
                if item.status == "paused":
                    item.status = "queued"
                    item.error = None
                    if item.job_id:
                        job = db.get(Job, item.job_id)
                        if job:
                            job.status = "queued"
                            job.cancel_requested = False
            batch.status = "queued"
            batch.error = None
            db.commit()
        return self.execute_batch(batch_id)

    def latest_resumable_batch(self) -> str | None:
        with self.runtime.sessions() as db:
            batch = db.scalar(
                select(LocalBatch)
                .where(LocalBatch.status.in_({"paused", "export_failed", "queued", "awaiting_confirmation"}))
                .order_by(LocalBatch.updated_at.desc())
            )
            return batch.id if batch else None

    def batch_status(self, batch_id: str) -> dict[str, object]:
        with self.runtime.sessions() as db:
            batch = db.get(LocalBatch, batch_id)
            if not batch:
                raise ValueError("Local batch not found")
            items = list(
                db.scalars(
                    select(LocalBatchItem)
                    .where(LocalBatchItem.batch_id == batch.id)
                    .order_by(LocalBatchItem.order_index.asc())
                ).all()
            )
            return {
                "id": batch.id,
                "status": batch.status,
                "progress": batch.progress,
                "message": batch.stage_message,
                "error": batch.error,
                "output_directory": batch.output_path,
                "extractor_model_id": batch.extractor_model_id,
                "judge_model_id": batch.judge_model_id,
                "items": [
                    {
                        "index": item.order_index,
                        "source": item.original_path,
                        "output_path": item.output_path,
                        "status": item.status,
                        "error": item.error,
                    }
                    for item in items
                ],
            }

    def purge_expired(self, days: int = 30) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        removed = 0
        with self.runtime.sessions() as db:
            batches = list(
                db.scalars(
                    select(LocalBatch).where(
                        LocalBatch.created_at < cutoff,
                        LocalBatch.status.not_in({"running", "preparing", "exporting"}),
                    )
                ).all()
            )
            for batch in batches:
                job_ids = [item.job_id for item in batch.items if item.job_id]
                root = self.runtime.settings.data_dir / "batches" / batch.id
                db.delete(batch)
                db.flush()
                for job_id in job_ids:
                    job = db.get(Job, job_id)
                    if job:
                        db.delete(job)
                shutil.rmtree(root, ignore_errors=True)
                removed += 1
            db.commit()
        return removed
