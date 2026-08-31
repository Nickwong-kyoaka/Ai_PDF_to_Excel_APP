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

from .exporter import write_series_excel
from .group_series import (
    GroupingInference,
    best_template_group_index,
    build_fixed_size_series,
    expected_questionnaire_count,
    infer_safe_series_groups,
    page_cycle_similarity,
)
from .model_discovery import (
    DiscoveryResult,
    probe_model_capability,
    probe_text_model_capability,
)
from .runtime import DesktopRuntime, ensure_local_identity


ALLOWED_SUFFIXES = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}


PROCESSING_PROFILES: dict[str, dict[str, object]] = {
    "balanced": {
        "image_max_side": 1800,
        "page_retry_max_side": 1800,
        "page_attempts": 1,
        "verification_mode": "selective",
        "verifier_confidence_threshold": 0.86,
        "verifier_audit_interval": 10,
        "verifier_calibration_questionnaires": 2,
        "verifier_tile_count": 0,
        "orientation_mode": "document",
        "orientation_retries": 0,
        "request_timeout": 90,
        "extraction_max_tokens": 2048,
        "extraction_retries": 0,
        "template_mode": True,
        "template_schema_max_tokens": 3072,
        "compact_max_tokens": 1536,
        "adjudication_chunk_size": 24,
        "adjudication_retries": 0,
        "judge_max_tokens": 1536,
        "judge_retries": 0,
    },
    "maximum": {
        "image_max_side": 2200,
        "page_retry_max_side": 2200,
        "page_attempts": 1,
        "verification_mode": "selective",
        "verifier_confidence_threshold": 0.91,
        "verifier_audit_interval": 5,
        "verifier_calibration_questionnaires": 2,
        "verifier_tile_count": 0,
        "tile_retries": 0,
        "orientation_mode": "model",
        "orientation_retries": 0,
        "request_timeout": 90,
        "extraction_max_tokens": 3072,
        "extraction_retries": 0,
        "template_mode": True,
        "template_schema_max_tokens": 4096,
        "compact_max_tokens": 2048,
        "adjudication_chunk_size": 24,
        "adjudication_retries": 0,
        "judge_max_tokens": 2048,
        "judge_retries": 0,
    },
}


def processing_profile(mode: str) -> dict[str, object]:
    try:
        return dict(PROCESSING_PROFILES[mode])
    except KeyError as exc:
        raise ValueError(f"Unknown processing mode: {mode}") from exc


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
    expected_questionnaires: int | None = None
    detected_cycle_pages: int | None = None
    template_variant: str | None = None
    pages_root: str | None = None


@dataclass(slots=True, frozen=True)
class FocusPageDraft:
    template_key: str
    series_label: str
    template_variant: str
    page_ordinal: int
    sample_paths: tuple[str, ...]
    sample_labels: tuple[str, ...]


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


def normalize_series_label(value: str) -> str:
    cleaned = " ".join(str(value).replace("\x00", "").split()).strip(" .")
    if not cleaned:
        raise ValueError("Every input needs a series label")
    return cleaned[:120]


def focus_template_key(series_label: str, template_variant: str | None) -> str:
    return f"{series_label.casefold()}::{(template_variant or 'generic').casefold()}"


def series_workbook_filename(label: str) -> str:
    return f"{_safe_filename(normalize_series_label(label))}_FormSight.xlsx"


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
        review_focus: bool = False,
        extractor_model_id: str | None = None,
        verifier_model_id: str | None = None,
        judge_model_id: str | None = None,
        series_labels: Iterable[str] | None = None,
        processing_mode: str = "balanced",
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
        if discovery.status not in {"ready", "qwen_only"} or not discovery.selected_vision:
            raise ValueError(discovery.message or "LM Studio is not ready")
        performance = processing_profile(processing_mode)

        vision_id = extractor_model_id or discovery.selected_vision.api_id
        verifier_id = (
            discovery.selected_verifier.api_id
            if verifier_model_id is None and discovery.selected_verifier
            else None
        )
        if verifier_model_id is not None:
            verifier_id = verifier_model_id.strip() or None
        if verifier_id and verifier_id == vision_id:
            raise ValueError("Primary and verifier vision models must be different")
        selected_ids = {
            "primary": vision_id,
            **({"verifier": verifier_id} if verifier_id else {}),
        }
        discovered_ids = {
            "primary": discovery.selected_vision.api_id,
            "verifier": discovery.selected_verifier.api_id if discovery.selected_verifier else None,
        }
        for role, model_id in selected_ids.items():
            already_passed = (
                discovered_ids.get(role) == model_id
                and "passed" in discovery.probe_results.get(role, "").casefold()
            )
            if already_passed:
                continue
            passed, detail = probe_model_capability(discovery.base_url, model_id)
            if not passed:
                raise ValueError(
                    f"Selected {role} model failed the image/JSON preflight: {detail}"
                )
        judge_id = judge_model_id or vision_id
        if judge_id != vision_id:
            judge_already_passed = (
                discovery.selected_judge is not None
                and discovery.selected_judge.api_id == judge_id
                and "passed" in discovery.probe_results.get("judge", "").casefold()
            )
            if not judge_already_passed:
                passed, detail = probe_text_model_capability(discovery.base_url, judge_id)
                if not passed:
                    raise ValueError(
                        f"Selected reasonableness model failed the text/JSON preflight: {detail}"
                    )
        output = Path(output_path).expanduser().resolve()
        if output.exists() and not output.is_dir():
            raise ValueError("The output location must be a folder")
        output.mkdir(parents=True, exist_ok=True)
        reserved_outputs = {
            child.name.casefold() for child in output.iterdir() if child.is_file()
        }

        supplied_labels = list(series_labels) if series_labels is not None else None
        if supplied_labels is not None and len(supplied_labels) != len(source_paths):
            raise ValueError("Series labels must match the selected source-file order")

        if supplied_labels is None:
            # Preserve the historic one-input/one-workbook default while assigning stable labels.
            labels: list[str] = []
            label_counts: dict[str, int] = {}
            for source in source_paths:
                base = normalize_series_label(source.stem)
                key = base.casefold()
                label_counts[key] = label_counts.get(key, 0) + 1
                labels.append(base if label_counts[key] == 1 else f"{base}_{label_counts[key]}")
        else:
            labels = [normalize_series_label(value) for value in supplied_labels]

        canonical_labels: dict[str, str] = {}
        labels = [
            canonical_labels.setdefault(label.casefold(), label)
            for label in labels
        ]

        label_outputs: dict[str, Path] = {}
        for label in labels:
            key = label.casefold()
            if key in label_outputs:
                continue
            stem = _safe_filename(label)
            candidate = output / f"{stem}_FormSight.xlsx"
            suffix = 2
            while candidate.name.casefold() in reserved_outputs:
                candidate = output / f"{stem}_FormSight_{suffix}.xlsx"
                suffix += 1
            reserved_outputs.add(candidate.name.casefold())
            label_outputs[key] = candidate
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
                review_focus=review_focus,
                processing_mode=processing_mode,
                lmstudio_base_url=discovery.base_url,
                extractor_model_id=vision_id,
                verifier_model_id=verifier_id,
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
                        series_label=labels[index],
                        output_path=str(label_outputs[labels[index].casefold()]),
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
            performance = processing_profile(batch.processing_mode)
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

            # Within each label, inspect explicit filename ranges first so their
            # confirmed cycle can be safely applied to sibling PDFs even if the user
            # added an unnumbered file before them.
            preparation_order = sorted(
                items,
                key=lambda item: (
                    item.series_label.casefold(),
                    expected_questionnaire_count(Path(item.original_path).name) is None,
                    item.order_index,
                ),
            )
            series_patterns: dict[str, list[tuple[int, list[Path], str]]] = {}
            for preparation_index, item in enumerate(preparation_order):
                if self.cancel_event.is_set():
                    batch.status = "paused"
                    batch.stage_message = "Paused / 已暫停"
                    db.commit()
                    return
                if item.job_id:
                    continue
                overall = (preparation_index / max(1, len(items))) * 0.12
                self._emit(batch.id, "preparing", overall, f"Inspecting {Path(item.original_path).name}", item.order_index)
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
                    inference = infer_safe_series_groups(
                        Path(item.original_path).name, info.page_images
                    )
                    pattern_key = item.series_label.casefold()
                    patterns = series_patterns.get(pattern_key, [])
                    matched_variant: str | None = None
                    if not inference.safe_for_one_take and patterns:
                        for cycle_pages, reference_pages, variant_name in patterns:
                            if info.page_count % cycle_pages != 0:
                                continue
                            similarity = page_cycle_similarity(
                                [*reference_pages[:cycle_pages], *info.page_images], cycle_pages
                            )
                            if similarity >= 0.90:
                                detected_count = info.page_count // cycle_pages
                                reason = (
                                    f"Applied confirmed {cycle_pages}-page series pattern; "
                                    f"cross-file layout similarity {similarity:.1%}"
                                )
                                inference = GroupingInference(
                                    build_fixed_size_series(info.page_count, cycle_pages),
                                    detected_count,
                                    cycle_pages,
                                    min(0.99, 0.75 + similarity * 0.25),
                                    reason,
                                    True,
                                )
                                matched_variant = variant_name
                                break
                    if inference.safe_for_one_take and inference.pages_per_questionnaire:
                        cycle_pages = inference.pages_per_questionnaire
                        if not matched_variant:
                            for known_cycle, reference_pages, variant_name in patterns:
                                if known_cycle != cycle_pages:
                                    continue
                                similarity = page_cycle_similarity(
                                    [*reference_pages[:known_cycle], *info.page_images], known_cycle
                                )
                                if similarity >= 0.90:
                                    matched_variant = variant_name
                                    break
                        if not matched_variant:
                            same_cycle_count = sum(
                                known_cycle == cycle_pages
                                for known_cycle, _pages, _name in patterns
                            )
                            matched_variant = (
                                f"{cycle_pages}p-cycle"
                                if same_cycle_count == 0
                                else f"{cycle_pages}p-cycle-v{same_cycle_count + 1}"
                            )
                            series_patterns.setdefault(pattern_key, []).append(
                                (cycle_pages, list(info.page_images), matched_variant)
                            )
                    item.expected_questionnaires = inference.expected_questionnaires
                    item.pages_per_questionnaire = inference.pages_per_questionnaire
                    item.grouping_confidence = inference.confidence
                    item.grouping_reason = inference.reason
                    item.template_variant = matched_variant
                    proposals = [
                        ProposedGroup(
                            proposal.start_page,
                            proposal.end_page,
                            proposal.participant_id,
                            inference.confidence,
                            inference.reason,
                        )
                        for proposal in inference.groups
                    ]
                    if not inference.safe_for_one_take and not batch.review_groups:
                        item.status = "skipped_grouping"
                        item.error = (
                            "Automatic safe-skip: questionnaire boundaries were uncertain. "
                            + inference.reason
                        )[:2000]
                        item.finished_at = utcnow()
                        db.commit()
                        continue
                    if not inference.safe_for_one_take and batch.review_groups:
                        proposals = propose_groups(info.embedded_text)
                        try:
                            if info.page_count > 1:
                                proposals = visual_grouping(
                                    info.page_images,
                                    LMStudioGateway(
                                        batch.lmstudio_base_url,
                                        "",
                                        timeout=float(performance["request_timeout"]),
                                        cancel_check=self.cancel_event.is_set,
                                    ),
                                    batch.extractor_model_id,
                                    retries=0,
                                )
                        except Exception as exc:
                            proposals[0].reason += f"; visual grouping unavailable: {str(exc)[:120]}"
                    snapshot = {
                        "profile_id": profile.id,
                        "slug": profile.slug,
                        "name": profile.name,
                        "extractor_model_id": batch.extractor_model_id,
                        "verifier_model_id": batch.verifier_model_id,
                        "judge_model_id": batch.judge_model_id,
                        "quantization": profile.quantization,
                        "context_length": 12288 if batch.processing_mode == "balanced" else 16384,
                        "max_concurrency": 1,
                        "processing_mode": batch.processing_mode,
                        "expected_questionnaires": inference.expected_questionnaires,
                        "pages_per_questionnaire": inference.pages_per_questionnaire,
                        "grouping_confidence": inference.confidence,
                        "grouping_reason": inference.reason,
                        "template_variant": item.template_variant,
                        "template_reference_group_index": best_template_group_index(
                            info.page_images, proposals
                        ),
                        **performance,
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
                        status=(
                            "awaiting_confirmation"
                            if batch.review_groups
                            else "awaiting_focus"
                            if batch.review_focus
                            else "queued"
                        ),
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
                    item.status = (
                        "awaiting_confirmation"
                        if batch.review_groups
                        else "awaiting_focus"
                        if batch.review_focus
                        else "queued"
                    )
                    item.error = None
                except Exception as exc:
                    item.status = "failed"
                    item.error = str(exc)[:2000]
                    item.finished_at = utcnow()
                db.commit()

            has_prepared_jobs = any(item.job_id for item in items)
            batch.status = (
                "awaiting_confirmation"
                if batch.review_groups
                else "awaiting_focus"
                if batch.review_focus and has_prepared_jobs
                else "queued"
            )
            batch.stage_message = {
                "awaiting_confirmation": "Review questionnaire page groups / 請檢查問卷頁面分組",
                "awaiting_focus": "Select reusable focus regions / 圈選可重用的重點區域",
                "queued": "Ready to scan / 準備掃描",
            }[batch.status]
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
                        expected_questionnaires=item.expected_questionnaires,
                        detected_cycle_pages=item.pages_per_questionnaire,
                        template_variant=item.template_variant,
                        pages_root=str(self.runtime.settings.pages_dir / job.id),
                    )
                    for group in groups
                )
            return drafts

    def focus_drafts(self, batch_id: str) -> list[FocusPageDraft]:
        """Return each page type with samples from the first two questionnaires."""

        with self.runtime.sessions() as db:
            items = list(
                db.scalars(
                    select(LocalBatchItem)
                    .where(LocalBatchItem.batch_id == batch_id, LocalBatchItem.job_id.is_not(None))
                    .order_by(LocalBatchItem.order_index.asc())
                ).all()
            )
            samples: dict[tuple[str, int], list[tuple[str, str]]] = {}
            metadata: dict[str, tuple[str, str]] = {}
            questionnaire_numbers: dict[str, int] = {}
            for item in items:
                job = db.get(Job, item.job_id) if item.job_id else None
                if not job:
                    continue
                key = focus_template_key(item.series_label, item.template_variant)
                metadata.setdefault(
                    key, (item.series_label, item.template_variant or "generic")
                )
                groups = list(
                    db.scalars(
                        select(QuestionnaireGroup)
                        .where(QuestionnaireGroup.job_id == job.id)
                        .order_by(QuestionnaireGroup.group_index.asc())
                    ).all()
                )
                root = self.runtime.settings.pages_dir / job.id
                for group in groups:
                    sample_number = questionnaire_numbers.get(key, 0) + 1
                    if sample_number > 2:
                        break
                    questionnaire_numbers[key] = sample_number
                    for page_number in range(group.start_page, group.end_page + 1):
                        ordinal = page_number - group.start_page + 1
                        image_path = root / f"page-{page_number:04d}.jpg"
                        if not image_path.exists():
                            continue
                        samples.setdefault((key, ordinal), []).append(
                            (
                                str(image_path),
                                f"Questionnaire {sample_number} · {Path(item.original_path).name} · page {page_number}",
                            )
                        )
            drafts: list[FocusPageDraft] = []
            for (key, ordinal), page_samples in sorted(
                samples.items(),
                key=lambda pair: (pair[0][0], pair[0][1]),
            ):
                series_label, template_variant = metadata[key]
                drafts.append(
                    FocusPageDraft(
                        template_key=key,
                        series_label=series_label,
                        template_variant=template_variant,
                        page_ordinal=ordinal,
                        sample_paths=tuple(path for path, _label in page_samples[:2]),
                        sample_labels=tuple(label for _path, label in page_samples[:2]),
                    )
                )
            return drafts

    def apply_focus_regions(
        self,
        batch_id: str,
        regions_by_template: dict[str, dict[str, list[list[float]]]],
    ) -> None:
        """Validate and persist normalized focus boxes before any model calls begin."""

        cleaned: dict[str, dict[str, list[list[float]]]] = {}
        for template_key, page_map in regions_by_template.items():
            if not isinstance(page_map, dict):
                raise ValueError("Focus regions must be grouped by page type")
            cleaned_pages: dict[str, list[list[float]]] = {}
            for ordinal, regions in page_map.items():
                if not str(ordinal).isdigit() or int(ordinal) < 1:
                    raise ValueError("Focus page ordinals must be positive numbers")
                cleaned_regions: list[list[float]] = []
                for region in regions:
                    if not isinstance(region, (list, tuple)) or len(region) != 4:
                        raise ValueError("Every focus region must contain four coordinates")
                    x1, y1, x2, y2 = (float(value) for value in region)
                    if not (0 <= x1 < x2 <= 1 and 0 <= y1 < y2 <= 1):
                        raise ValueError("Focus coordinates must be normalized inside the page")
                    if (x2 - x1) * (y2 - y1) < 0.0004:
                        raise ValueError("A focus region is too small")
                    cleaned_regions.append([x1, y1, x2, y2])
                if cleaned_regions:
                    cleaned_pages[str(int(ordinal))] = cleaned_regions
            cleaned[str(template_key)] = cleaned_pages

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
                if not job:
                    continue
                key = focus_template_key(item.series_label, item.template_variant)
                page_regions = cleaned.get(key, {})
                snapshot = dict(job.profile_snapshot or {})
                snapshot["focus_regions_v1"] = {
                    "version": 1,
                    "template_key": key,
                    "source": "operator_first_two_questionnaires",
                    "regions_by_page": page_regions,
                    "region_count": sum(len(values) for values in page_regions.values()),
                }
                job.profile_snapshot = snapshot
                job.status = "queued"
                job.stage_message = "Ready with reusable focus regions"
                item.status = "queued"
            batch.status = "queued"
            batch.stage_message = "Focus regions saved — ready to scan / 重點區域已儲存，準備掃描"
            batch.error = None
            db.commit()

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
                lengths = [proposal.end_page - proposal.start_page + 1 for proposal in proposals]
                if lengths and len(set(lengths)) == 1:
                    item.expected_questionnaires = len(proposals)
                    item.pages_per_questionnaire = lengths[0]
                    item.grouping_confidence = 1.0
                    item.grouping_reason = "Confirmed manually in FormSight Local"
                    item.template_variant = f"{lengths[0]}p-manual-{job.id[:8]}"
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
            if batch.status == "awaiting_focus":
                raise ValueError("Select or skip focus regions before scanning")
            items = list(
                db.scalars(
                    select(LocalBatchItem)
                    .where(LocalBatchItem.batch_id == batch.id)
                    .order_by(LocalBatchItem.order_index.asc())
                ).all()
            )
            profile: dict[str, object] = {
                "extractor_model_id": batch.extractor_model_id,
                "verifier_model_id": batch.verifier_model_id,
                "judge_model_id": batch.judge_model_id,
                "processing_mode": batch.processing_mode,
                "local_desktop": True,
                **processing_profile(batch.processing_mode),
            }
            batch.status = "running"
            batch.error = None
            batch.stage_message = "Scanning sequentially / 正在依次掃描"
            db.commit()

            results_by_label: dict[str, dict[str, object]] = {}
            # A confirmed page-cycle may be reused by every source in the same series
            # and layout variant.  The dictionary is deliberately shared with each
            # extractor instance so newly discovered page schemas become available to
            # the next questionnaire without another full-page discovery call.
            templates_by_series: dict[tuple[str, str], dict[str, list[dict[str, object]]]] = {}
            template_questionnaire_counts: dict[tuple[str, str], int] = {}
            for item_index, item in enumerate(items):
                if self.cancel_event.is_set():
                    self._pause(db, batch, item)
                    return None

                if item.status == "skipped_grouping":
                    try:
                        result = self._write_series_workbook(db, batch, item.series_label)
                        results_by_label[item.series_label.casefold()] = result
                    except Exception as exc:
                        self._emit(
                            batch.id,
                            "checkpoint_warning",
                            batch.progress,
                            f"Safe-skip checkpoint delayed: {str(exc)[:160]}",
                            item_index,
                        )
                    continue
                if item.status == "completed":
                    completed_job = db.get(Job, item.job_id) if item.job_id else None
                    completed_snapshot = dict(completed_job.profile_snapshot or {}) if completed_job else {}
                    completed_template = completed_snapshot.get("series_template_v1") or {}
                    completed_pages = completed_template.get("pages")
                    template_key = (
                        item.series_label.casefold(),
                        (item.template_variant or "generic").casefold(),
                    )
                    if isinstance(completed_pages, dict):
                        templates_by_series.setdefault(template_key, {}).update(completed_pages)
                    if completed_job:
                        completed_groups = len(
                            db.scalars(
                                select(QuestionnaireGroup).where(
                                    QuestionnaireGroup.job_id == completed_job.id
                                )
                            ).all()
                        )
                        template_questionnaire_counts[template_key] = (
                            template_questionnaire_counts.get(template_key, 0) + completed_groups
                        )
                    try:
                        result = self._write_series_workbook(db, batch, item.series_label)
                        results_by_label[item.series_label.casefold()] = result
                    except Exception as exc:
                        self._emit(
                            batch.id,
                            "checkpoint_warning",
                            batch.progress,
                            f"Series checkpoint delayed: {str(exc)[:160]}",
                            item_index,
                        )
                    continue
                job = db.get(Job, item.job_id) if item.job_id else None
                if not job:
                    item.status = "failed"
                    item.error = item.error or "Prepared job is missing"
                    item.finished_at = item.finished_at or utcnow()
                    db.commit()
                    try:
                        result = self._write_series_workbook(db, batch, item.series_label)
                        results_by_label[item.series_label.casefold()] = result
                    except Exception as exc:
                        self._emit(
                            batch.id,
                            "checkpoint_warning",
                            batch.progress,
                            f"Series checkpoint delayed: {str(exc)[:160]}",
                            item_index,
                        )
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

                def questionnaire_checkpoint(completed: int, total: int) -> None:
                    try:
                        result = self._write_series_workbook(db, batch, item.series_label)
                        results_by_label[item.series_label.casefold()] = result
                        self._emit(
                            batch.id,
                            "checkpointing",
                            batch.progress,
                            f"Saved questionnaire {completed}/{total} to partial Excel",
                            item_index,
                        )
                    except Exception as exc:
                        self._emit(
                            batch.id,
                            "checkpoint_warning",
                            batch.progress,
                            f"Questionnaire checkpoint delayed: {str(exc)[:160]}",
                            item_index,
                        )

                template_key = (
                    item.series_label.casefold(),
                    (item.template_variant or "generic").casefold(),
                )
                shared_template = templates_by_series.setdefault(template_key, {})
                saved_template = (dict(job.profile_snapshot or {}).get("series_template_v1") or {}).get(
                    "pages"
                )
                if isinstance(saved_template, dict):
                    shared_template.update(saved_template)
                job_profile = {
                    **profile,
                    "template_reference_group_index": dict(job.profile_snapshot or {}).get(
                        "template_reference_group_index", 0
                    ),
                    "verifier_calibration_offset": template_questionnaire_counts.get(
                        template_key, 0
                    ),
                    "focus_regions": (
                        (dict(job.profile_snapshot or {}).get("focus_regions_v1") or {}).get(
                            "regions_by_page", {}
                        )
                    ),
                }
                extractor = QuestionnaireExtractor(
                    self.runtime.settings,
                    job_profile,
                    self.runtime.weights_path if self.runtime.weights_path.exists() else None,
                    manage_models=False,
                    progress_callback=page_progress,
                    cancel_check=self.cancel_event.is_set,
                    template_pages=shared_template,
                    questionnaire_callback=questionnaire_checkpoint,
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
                    template_questionnaire_counts[template_key] = (
                        template_questionnaire_counts.get(template_key, 0)
                        + len(
                            db.scalars(
                                select(QuestionnaireGroup).where(
                                    QuestionnaireGroup.job_id == job.id
                                )
                            ).all()
                        )
                    )
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
                batch.stage_message = f"Checkpointing series: {item.series_label}"
                db.commit()
                self._emit(batch.id, "exporting", export_progress, batch.stage_message, item_index)
                try:
                    result = self._write_series_workbook(db, batch, item.series_label)
                    results_by_label[item.series_label.casefold()] = result
                except Exception as exc:
                    self._emit(
                        batch.id,
                        "checkpoint_warning",
                        export_progress,
                        f"Series checkpoint delayed; final export will retry: {str(exc)[:160]}",
                        item_index,
                    )

            # Rebuild every series once after all of its sources are terminal. Each earlier
            # per-source export is only a crash-safe partial checkpoint.
            final_export_errors: dict[str, str] = {}
            unique_labels = list(dict.fromkeys(item.series_label for item in items))
            for label_index, label in enumerate(unique_labels):
                try:
                    result = self._write_series_workbook(db, batch, label)
                    results_by_label[label.casefold()] = result
                    for series_item in items:
                        if (
                            series_item.series_label.casefold() == label.casefold()
                            and series_item.status == "completed"
                            and series_item.error
                            and "Excel export failed:" in series_item.error
                        ):
                            series_item.error = None
                    db.commit()
                except Exception as exc:
                    final_export_errors[label.casefold()] = str(exc)[:1000]
                    for series_item in items:
                        if series_item.series_label.casefold() == label.casefold():
                            previous = series_item.error
                            series_item.error = (
                                f"{previous}; Excel export failed: {exc}"
                                if previous
                                else f"Excel export failed: {exc}"
                            )[:2000]
                    db.commit()
                final_progress = 0.98 + ((label_index + 1) / max(1, len(unique_labels))) * 0.019
                self._emit(batch.id, "exporting", final_progress, f"Finalizing series: {label}")

            failed = sum(
                item.status in {"failed", "export_failed", "skipped_grouping"}
                for item in items
            )
            results = list(results_by_label.values())
            flags = sum(int(result.get("flags", 0)) for result in results)
            status_label = "COMPLETED — FLAGS PRESENT" if failed or flags else "COMPLETED"
            if final_export_errors:
                status_label = "EXPORT FAILED — RESUME AVAILABLE"
            batch.status = "export_failed" if final_export_errors else "completed"
            batch.progress = 1.0
            batch.stage_message = status_label
            batch.completed_at = None if final_export_errors else utcnow()
            batch.error = (
                "; ".join(f"{label}: {error}" for label, error in final_export_errors.items())[:2000]
                if final_export_errors
                else None
            )
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

    def _write_series_workbook(
        self,
        db,
        batch: LocalBatch,
        series_label: str,
    ) -> dict[str, object]:
        items = list(
            db.scalars(
                select(LocalBatchItem)
                .where(
                    LocalBatchItem.batch_id == batch.id,
                    LocalBatchItem.series_label == series_label,
                )
                .order_by(LocalBatchItem.order_index.asc())
            ).all()
        )
        if not items:
            raise ValueError(f"Series not found: {series_label}")
        output_path = next((item.output_path for item in items if item.output_path), None)
        if not output_path:
            output_directory = Path(batch.output_path)
            if output_directory.suffix.casefold() == ".xlsx":
                output_directory = output_directory.parent
            output_directory.mkdir(parents=True, exist_ok=True)
            output_path = str(output_directory / series_workbook_filename(series_label))
        for item in items:
            item.output_path = output_path
        db.commit()
        return write_series_excel(db, batch, items, series_label, output_path)

    def _pause(self, db, batch: LocalBatch, current_item: LocalBatchItem) -> None:
        current_item.status = "paused"
        current_item.error = "Paused by user; completed pages are checkpointed and will be skipped on resume."
        batch.status = "paused"
        batch.stage_message = "Paused — completed inputs are preserved / 已暫停，完成的檔案已保留"
        db.commit()
        self._emit(batch.id, "paused", batch.progress, batch.stage_message, current_item.order_index)

    def resume_batch(self, batch_id: str) -> dict[str, object] | tuple[str, str] | None:
        with self.runtime.sessions() as db:
            batch = db.get(LocalBatch, batch_id)
            if not batch:
                raise ValueError("Local batch not found")
            if batch.status == "awaiting_confirmation":
                return "prepared", batch_id
            if batch.status == "awaiting_focus":
                return "focus", batch_id
            resume_preparation = batch.status == "preparing"
            items = list(db.scalars(select(LocalBatchItem).where(LocalBatchItem.batch_id == batch.id)).all())
            for item in items:
                if resume_preparation and not item.job_id:
                    item.status = "pending"
                    item.error = None
                elif item.status in {"paused", "running", "export_failed"}:
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
        if resume_preparation:
            self.prepare_batch(batch_id)
            with self.runtime.sessions() as db:
                batch = db.get(LocalBatch, batch_id)
                if batch and batch.status == "awaiting_confirmation":
                    return "prepared", batch_id
                if batch and batch.status == "awaiting_focus":
                    return "focus", batch_id
        return self.execute_batch(batch_id)

    def latest_resumable_batch(self) -> str | None:
        with self.runtime.sessions() as db:
            batch = db.scalar(
                select(LocalBatch)
                .where(
                    LocalBatch.status.in_(
                        {
                            "preparing",
                            "running",
                            "exporting",
                            "paused",
                            "export_failed",
                            "queued",
                            "awaiting_confirmation",
                            "awaiting_focus",
                        }
                    )
                )
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
                "verifier_model_id": batch.verifier_model_id,
                "judge_model_id": batch.judge_model_id,
                "processing_mode": batch.processing_mode,
                "review_focus": batch.review_focus,
                "items": [
                    {
                        "index": item.order_index,
                        "source": item.original_path,
                        "series_label": item.series_label,
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
