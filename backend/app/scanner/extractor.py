from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pymupdf as fitz
from PIL import Image
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from ..config import Settings
from ..models import Answer, Job, QuestionnaireGroup, Rule
from .fusion import FusedAnswer, clean_text, fuse_page, item_key, valid_bbox
from .legacy import V14Compatibility
from .lmstudio import LMStudioGateway
from .prompts import conflict_prompt, extraction_prompt, judge_prompt, orientation_prompt
from .rules import evaluate_rule, generic_findings
from .yolo import YoloMarkDetector


def render_page(source: Path, page_number: int, max_side: int, dpi: int = 240) -> Image.Image:
    if source.suffix.casefold() == ".pdf":
        document = fitz.open(source)
        try:
            page = document.load_page(page_number - 1)
            scale = dpi / 72
            pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
            image = Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
        finally:
            document.close()
    else:
        image = Image.open(source).convert("RGB")
    if max(image.size) > max_side:
        ratio = max_side / max(image.size)
        image = image.resize(
            (max(1, int(image.width * ratio)), max(1, int(image.height * ratio))),
            Image.Resampling.LANCZOS,
        )
    return image


def crop_bbox(image: Image.Image, bbox: list[float] | None, padding: float = 0.035) -> Image.Image:
    if not bbox:
        return image
    x1 = max(0, int((bbox[0] - padding) * image.width))
    y1 = max(0, int((bbox[1] - padding) * image.height))
    x2 = min(image.width, int((bbox[2] + padding) * image.width))
    y2 = min(image.height, int((bbox[3] + padding) * image.height))
    return image.crop((x1, y1, x2, y2)) if x2 > x1 and y2 > y1 else image


def sanitize_item(item: dict[str, Any], fallback_id: str) -> dict[str, Any]:
    allowed_options = item.get("allowed_options") if isinstance(item.get("allowed_options"), list) else []
    selected_options = item.get("selected_options") if isinstance(item.get("selected_options"), list) else []
    return {
        "question_id": clean_text(item.get("question_id")) or fallback_id,
        "question_text": clean_text(item.get("question_text")) or "Unlabelled field",
        "section": clean_text(item.get("section")),
        "answer_type": clean_text(item.get("answer_type")) or "other",
        "allowed_options": allowed_options,
        "selected_options": selected_options,
        "value": item.get("value"),
        "question_bbox": valid_bbox(item.get("question_bbox")),
        "answer_bbox": valid_bbox(item.get("answer_bbox")),
        "blank": bool(item.get("blank")),
        "confidence": max(0.0, min(1.0, float(item.get("confidence") or 0))),
        "reason": clean_text(item.get("reason")),
    }


class QuestionnaireExtractor:
    def __init__(self, settings: Settings, profile: dict[str, Any], yolo_weights: Path | None = None):
        self.settings = settings
        self.profile = profile
        self.gateway = LMStudioGateway(settings.lmstudio_base_url, settings.lmstudio_token)
        self.yolo = YoloMarkDetector(yolo_weights or settings.yolo_weights)
        self.legacy = V14Compatibility(settings.legacy_v14_path)

    def orient(self, image: Image.Image) -> Image.Image:
        try:
            result = self.gateway.chat_json(
                model=self.profile["extractor_model_id"],
                prompt=orientation_prompt(),
                images=[image.copy().resize((min(image.width, 1200), min(image.height, 1200)))],
                max_tokens=100,
                retries=1,
            )
            rotation = int(result.get("rotation_degrees") or 0)
            if rotation in {90, 180, 270}:
                return image.rotate(-rotation, expand=True)
        except Exception:
            pass
        return image

    def extract_pass(
        self,
        image: Image.Image,
        page_number: int,
        total_pages: int,
        pass_name: str,
        include_tiles: bool,
    ) -> list[dict[str, Any]]:
        evidence_images = [image]
        if include_tiles:
            evidence_images.extend(self.legacy.zoom_tiles(image, max_tiles=4))
        response = self.gateway.chat_json(
            model=self.profile["extractor_model_id"],
            prompt=extraction_prompt(page_number, total_pages, pass_name),
            images=evidence_images,
            max_tokens=8192,
            retries=2,
        )
        items = response.get("items")
        if not isinstance(items, list):
            raise ValueError("Extractor JSON omitted the items array")
        return [
            sanitize_item(item, f"P{page_number}-R{index + 1}")
            for index, item in enumerate(items)
            if isinstance(item, dict)
        ]

    def tiebreak(self, image: Image.Image, fused: FusedAnswer) -> FusedAnswer:
        bbox = valid_bbox(fused.item.get("answer_bbox")) or valid_bbox(fused.item.get("question_bbox"))
        try:
            result = self.gateway.chat_json(
                model=self.profile["extractor_model_id"],
                prompt=conflict_prompt(fused.item, [fused.qwen_value, fused.yolo_value]),
                images=[crop_bbox(image, bbox)],
                max_tokens=700,
                retries=1,
            )
            if result.get("resolved") and float(result.get("confidence") or 0) >= 0.82:
                resolved = result.get("value")
                fused.scanner_value = resolved
                fused.confidence = float(result.get("confidence"))
                fused.reason += f"; cropped Qwen tiebreak: {clean_text(result.get('reason'))}"
                fused.needs_review = normalized_mismatch(resolved, fused.qwen_value, fused.yolo_value)
                fused.evidence.append(
                    {
                        "source": "qwen_tiebreak",
                        "label": "cropped verification",
                        "bbox": bbox or [0, 0, 1, 1],
                        "confidence": fused.confidence,
                    }
                )
        except Exception as exc:
            fused.reason += f"; cropped tiebreak unavailable: {str(exc)[:100]}"
            fused.needs_review = True
        return fused

    def extract_one_page(
        self,
        source: Path,
        page_number: int,
        total_pages: int,
        yolo_available: bool,
    ) -> tuple[list[FusedAnswer], dict[str, Any]]:
        image = render_page(source, page_number, int(self.profile.get("image_max_side", 3000)))
        image = self.orient(self.legacy.enhance(image))
        first = self.extract_pass(image, page_number, total_pages, "first pass", False)
        second = self.extract_pass(image, page_number, total_pages, "independent verification pass", True)
        detections = self.yolo.detect(image)
        fused_answers = fuse_page(first, second, detections, yolo_available)
        for fused in fused_answers:
            if fused.needs_tiebreak:
                self.tiebreak(image, fused)
        return fused_answers, {
            "first": first,
            "second": second,
            "yolo": [detection.as_dict() for detection in detections],
        }

    def extract_job(self, db: Session, job: Job) -> None:
        self.gateway.manage_model("load", self.profile["extractor_model_id"])
        db.execute(delete(Answer).where(Answer.job_id == job.id))
        db.commit()
        source = Path(job.stored_path)
        groups = db.scalars(
            select(QuestionnaireGroup)
            .where(QuestionnaireGroup.job_id == job.id)
            .order_by(QuestionnaireGroup.group_index)
        ).all()
        total_work_pages = sum(group.end_page - group.start_page + 1 for group in groups)
        processed = 0
        yolo_available = self.yolo.health()["status"] == "online"
        debug_dir = self.settings.artifacts_dir / job.id / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        page_failures: list[dict[str, Any]] = []

        for group in groups:
            for page_number in range(group.start_page, group.end_page + 1):
                db.refresh(job)
                if job.cancel_requested:
                    job.status = "cancelled"
                    job.stage_message = "Cancelled by user"
                    db.commit()
                    return
                job.stage_message = f"Extracting page {page_number} of {job.page_count}"
                job.progress = (processed / max(1, total_work_pages)) * 0.72
                db.commit()

                try:
                    fused_answers, debug_payload = self.extract_one_page(
                        source, page_number, job.page_count, yolo_available
                    )
                    for fused in fused_answers:
                        selected = fused.item.get("selected_options") or []
                        db.add(
                            Answer(
                                job_id=job.id,
                                group_id=group.id,
                                page_number=page_number,
                                question_id=item_key(fused.item),
                                question_text=fused.item["question_text"],
                                section=fused.item.get("section", ""),
                                answer_type=fused.item.get("answer_type", "other"),
                                allowed_options=fused.item.get("allowed_options", []),
                                selected_options=selected,
                                qwen_value=fused.qwen_value,
                                yolo_value=fused.yolo_value,
                                scanner_value=fused.scanner_value,
                                scanner_confidence=fused.confidence,
                                fusion_reason=fused.reason,
                                evidence=[{"page_number": page_number, **item} for item in fused.evidence],
                                final_value=fused.scanner_value,
                                final_source="scanner",
                                review_status="pending" if fused.needs_review else "not_required",
                            )
                        )
                except Exception as exc:
                    debug_payload = {"page_error": str(exc)}
                    page_failures.append({"page_number": page_number, "error": str(exc)})
                    db.add(
                        Answer(
                            job_id=job.id,
                            group_id=group.id,
                            page_number=page_number,
                            question_id=f"PAGE-{page_number}-EXTRACTION-ERROR",
                            question_text="Page extraction failed",
                            answer_type="other",
                            scanner_value=None,
                            scanner_confidence=0,
                            fusion_reason=str(exc)[:1000],
                            final_value=None,
                            final_source="scanner",
                            reasonableness_status="review_required",
                            judge_reason="Partial questionnaire preserved; retry or manually review this page.",
                            review_status="pending",
                        )
                    )
                (debug_dir / f"page-{page_number:04d}.json").write_text(
                    json.dumps(debug_payload, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                processed += 1
                db.commit()
        if page_failures:
            job.error = f"{len(page_failures)} page(s) failed; partial results were preserved"
            db.commit()

    def judge_job(self, db: Session, job: Job) -> None:
        self.yolo.release()
        if self.profile["judge_model_id"] != self.profile["extractor_model_id"]:
            self.gateway.manage_model("unload", self.profile["extractor_model_id"])
            self.gateway.manage_model("load", self.profile["judge_model_id"])
        job.status = "judging"
        job.stage_message = "Checking whether answers are reasonable"
        job.progress = 0.76
        db.commit()
        rules = db.scalars(select(Rule).where(Rule.enabled.is_(True))).all()
        groups = db.scalars(
            select(QuestionnaireGroup).where(QuestionnaireGroup.job_id == job.id)
        ).all()
        for group_index, group in enumerate(groups):
            answers = db.scalars(
                select(Answer).where(Answer.group_id == group.id).order_by(Answer.page_number, Answer.question_id)
            ).all()
            context = {answer.question_id: answer.scanner_value for answer in answers}
            payload: list[dict[str, Any]] = []
            findings: list[dict[str, Any]] = []
            for answer in answers:
                record = {
                    "question_id": answer.question_id,
                    "question_text": answer.question_text,
                    "answer_type": answer.answer_type,
                    "allowed_options": answer.allowed_options,
                    "scanner_value": answer.scanner_value,
                }
                payload.append(record)
                findings.extend(generic_findings(record))
                for rule in rules:
                    definition = dict(rule.definition)
                    definition["rule_id"] = rule.id
                    finding = evaluate_rule(answer.question_id, answer.scanner_value, definition, context)
                    if finding:
                        findings.append(finding)
            try:
                response = self.gateway.chat_json(
                    model=self.profile["judge_model_id"],
                    prompt=judge_prompt(payload, findings),
                    max_tokens=4096,
                    retries=2,
                )
                results = {
                    str(item.get("question_id")): item
                    for item in response.get("results", [])
                    if isinstance(item, dict) and item.get("question_id")
                }
            except Exception as exc:
                results = {}
                for answer in answers:
                    answer.judge_reason = f"Reasonableness model unavailable: {str(exc)[:180]}"

            findings_by_question: dict[str, list[dict[str, Any]]] = {}
            for finding in findings:
                findings_by_question.setdefault(str(finding.get("question_id")), []).append(finding)
            for answer in answers:
                result = results.get(answer.question_id)
                related = findings_by_question.get(answer.question_id, [])
                if not result:
                    if related:
                        answer.reasonableness_status = "review_required"
                        answer.judge_reason = "; ".join(str(item["message"]) for item in related)
                        answer.rule_refs = [str(item["rule_id"]) for item in related]
                        answer.review_status = "pending"
                    else:
                        answer.reasonableness_status = "not_checked"
                    continue
                status = str(result.get("status") or "review_required")
                confidence = max(0.0, min(1.0, float(result.get("confidence") or 0)))
                suggestion = result.get("suggestion")
                basis = str(result.get("evidence_basis") or "none")
                answer.reasonableness_status = status
                answer.judge_suggestion = suggestion
                answer.judge_reason = clean_text(result.get("reason"))
                answer.judge_confidence = confidence
                answer.rule_refs = [str(item["rule_id"]) for item in related]
                can_correct = (
                    status == "corrected"
                    and suggestion is not None
                    and confidence >= self.settings.judge_correction_threshold
                    and basis in {"deterministic_rule", "printed_option", "cross_field"}
                    and answer.answer_type not in {"long_text", "signature"}
                )
                if can_correct:
                    answer.final_value = suggestion
                    answer.final_source = "qwen_judge"
                    answer.review_status = "pending"
                    answer.judge_reason = f"Qwen corrected — pending human review. {answer.judge_reason}"
                elif status in {"corrected", "review_required"}:
                    answer.review_status = "pending"
                    answer.reasonableness_status = "review_required"
            job.progress = 0.76 + ((group_index + 1) / max(1, len(groups))) * 0.14
            db.commit()


def normalized_mismatch(resolved: Any, qwen: Any, yolo: Any) -> bool:
    def norm(value: Any) -> str:
        if isinstance(value, list):
            return json.dumps(sorted(str(item).casefold().strip() for item in value), ensure_ascii=False)
        return str(value).casefold().strip() if value is not None else ""

    candidates = {norm(value) for value in (qwen, yolo) if value is not None}
    return bool(candidates) and norm(resolved) not in candidates
