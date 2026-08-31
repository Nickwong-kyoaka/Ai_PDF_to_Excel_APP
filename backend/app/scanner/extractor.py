from __future__ import annotations

import json
import time
from pathlib import Path
from collections.abc import Callable
from typing import Any

import pymupdf as fitz
from PIL import Image, ImageDraw
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from ..config import Settings
from ..models import Answer, Job, QuestionnaireGroup, Rule
from .fusion import (
    FusedAnswer,
    clean_text,
    fuse_page,
    fuse_primary_only,
    fuse_qwen_passes,
    fuse_vision_models,
    item_key,
    item_value,
    valid_bbox,
)
from .legacy import V14Compatibility
from .lmstudio import LMStudioGateway
from .prompts import (
    compact_extraction_prompt,
    conflict_prompt,
    extraction_prompt,
    judge_prompt,
    orientation_prompt,
    page_conflicts_prompt,
    template_schema_prompt,
)
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


def _ink_density(image: Image.Image, bbox: list[float]) -> float:
    crop = crop_bbox(image, bbox, padding=0.0).convert("L")
    histogram = crop.histogram()
    dark = sum(histogram[:150])
    return dark / max(1, crop.width * crop.height)


def geometry_mark_evidence(image: Image.Image, item: dict[str, Any]) -> tuple[Any, float]:
    """Return lightweight, training-free mark evidence for printed option regions."""

    options = item.get("allowed_options") or []
    scored: list[tuple[str, float]] = []
    for option in options:
        if not isinstance(option, dict):
            continue
        bbox = valid_bbox(option.get("bbox"))
        label = clean_text(option.get("label"))
        if bbox and label:
            scored.append((label, _ink_density(image, bbox)))
    if len(scored) < 2:
        return None, 0.0
    ranked = sorted(scored, key=lambda pair: pair[1], reverse=True)
    gap = ranked[0][1] - ranked[1][1]
    if gap < 0.018:
        return None, max(0.0, min(0.49, gap * 20))
    return ranked[0][0], max(0.5, min(0.92, 0.5 + gap * 8.0))


def labeled_crop_sheet(
    image: Image.Image, records: list[dict[str, Any]]
) -> Image.Image:
    """Place only disputed answer regions on one bounded, labelled verifier image."""

    columns = 3
    cell_width, cell_height = 400, 190
    rows = max(1, (len(records) + columns - 1) // columns)
    sheet = Image.new("RGB", (columns * cell_width, rows * cell_height), "white")
    draw = ImageDraw.Draw(sheet)
    for index, record in enumerate(records):
        column, row = index % columns, index // columns
        left, top = column * cell_width, row * cell_height
        bbox = valid_bbox(record.get("answer_bbox"))
        crop = crop_bbox(image, bbox, padding=0.12) if bbox else image.copy()
        crop.thumbnail((cell_width - 20, cell_height - 38), Image.Resampling.LANCZOS)
        sheet.paste(crop, (left + 10, top + 28))
        draw.rectangle(
            (left + 3, top + 3, left + cell_width - 3, top + cell_height - 3),
            outline="#667085",
            width=2,
        )
        draw.text((left + 10, top + 8), str(record.get("question_id") or index + 1), fill="black")
    return sheet


def build_focus_crop_sheet(
    image: Image.Image,
    regions: list[list[float]],
    *,
    max_side: int = 2200,
) -> Image.Image:
    """Build one compact page containing only operator-selected normalized regions."""

    boxes = [bbox for bbox in (valid_bbox(region) for region in regions) if bbox]
    if not boxes:
        return image
    crops = [crop_bbox(image, bbox, padding=0.012).convert("RGB") for bbox in boxes]
    if len(crops) == 1:
        focused = crops[0]
    else:
        columns = 2 if len(crops) > 1 else 1
        cell_width = min(900, max(320, max(crop.width for crop in crops) + 24))
        prepared: list[Image.Image] = []
        for crop in crops:
            candidate = crop.copy()
            candidate.thumbnail((cell_width - 24, 920), Image.Resampling.LANCZOS)
            prepared.append(candidate)
        row_count = (len(prepared) + columns - 1) // columns
        row_heights = [
            max(
                (prepared[index].height for index in range(row * columns, min(len(prepared), (row + 1) * columns))),
                default=1,
            )
            + 40
            for row in range(row_count)
        ]
        focused = Image.new("RGB", (columns * cell_width, sum(row_heights)), "white")
        draw = ImageDraw.Draw(focused)
        y = 0
        for row, row_height in enumerate(row_heights):
            for column in range(columns):
                index = row * columns + column
                if index >= len(prepared):
                    break
                crop = prepared[index]
                x = column * cell_width
                draw.rectangle(
                    (x + 3, y + 3, x + cell_width - 3, y + row_height - 3),
                    outline="#52756f",
                    width=2,
                )
                draw.text((x + 12, y + 10), f"Focus {index + 1}", fill="black")
                focused.paste(crop, (x + 12, y + 34))
            y += row_height
    if max(focused.size) > max_side:
        ratio = max_side / max(focused.size)
        focused = focused.resize(
            (max(1, round(focused.width * ratio)), max(1, round(focused.height * ratio))),
            Image.Resampling.LANCZOS,
        )
    return focused


def sanitize_item(item: dict[str, Any], fallback_id: str) -> dict[str, Any]:
    allowed_options = item.get("allowed_options") if isinstance(item.get("allowed_options"), list) else []
    selected_options = item.get("selected_options") if isinstance(item.get("selected_options"), list) else []
    return {
        "template_question_id": clean_text(item.get("template_question_id")),
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
        "mark_type": clean_text(item.get("mark_type")),
        "missing_model_output": bool(item.get("missing_model_output")),
        "reason": clean_text(item.get("reason")),
    }


def sanitize_template_item(item: dict[str, Any], fallback_id: str) -> dict[str, Any]:
    allowed_options = item.get("allowed_options") if isinstance(item.get("allowed_options"), list) else []
    return {
        "template_question_id": clean_text(item.get("template_question_id")) or fallback_id,
        "question_id": clean_text(item.get("template_question_id")) or fallback_id,
        "question_text": clean_text(item.get("question_text")) or "Unlabelled field",
        "section": clean_text(item.get("section")),
        "answer_type": clean_text(item.get("answer_type")) or "other",
        "allowed_options": allowed_options,
        "question_bbox": valid_bbox(item.get("question_bbox")),
        "answer_bbox": valid_bbox(item.get("answer_bbox")),
    }


def compact_answers_to_items(
    template_items: list[dict[str, Any]], response: dict[str, Any]
) -> list[dict[str, Any]]:
    raw_answers = response.get("answers")
    if not isinstance(raw_answers, list):
        raise ValueError("Compact extractor JSON omitted the answers array")
    by_id = {
        clean_text(answer.get("template_question_id")): answer
        for answer in raw_answers
        if isinstance(answer, dict) and clean_text(answer.get("template_question_id"))
    }
    items: list[dict[str, Any]] = []
    for index, template in enumerate(template_items, start=1):
        template_id = clean_text(template.get("template_question_id")) or f"R{index}"
        answer = by_id.get(template_id, {})
        missing = template_id not in by_id
        value = answer.get("value")
        selected = answer.get("selected_options")
        if not isinstance(selected, list):
            selected = [] if value in (None, "") else ([*value] if isinstance(value, list) else [value])
        items.append(
            sanitize_item(
                {
                    **template,
                    "question_id": template_id,
                    "selected_options": selected,
                    "value": value,
                    "blank": bool(answer.get("blank", value in (None, "", []))),
                    "confidence": answer.get("confidence") or 0,
                    "reason": (
                        clean_text(answer.get("reason"))
                        or ("Missing from model output" if missing else "")
                    ),
                    "mark_type": clean_text(answer.get("mark_type")),
                    "missing_model_output": missing,
                },
                template_id,
            )
        )
    return items


def chunk_judge_records(
    records: list[dict[str, Any]], *, max_items: int = 24, max_json_chars: int = 16000
) -> list[list[dict[str, Any]]]:
    """Keep reasonableness prompts below small local-model context limits."""
    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_chars = 0
    for record in records:
        size = len(json.dumps(record, ensure_ascii=False, default=str))
        if current and (len(current) >= max_items or current_chars + size > max_json_chars):
            chunks.append(current)
            current = []
            current_chars = 0
        current.append(record)
        current_chars += size
    if current:
        chunks.append(current)
    return chunks


class QuestionnaireExtractor:
    def __init__(
        self,
        settings: Settings,
        profile: dict[str, Any],
        yolo_weights: Path | None = None,
        *,
        manage_models: bool = True,
        progress_callback: Callable[[str, float, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        template_pages: dict[str, list[dict[str, Any]]] | None = None,
        questionnaire_callback: Callable[[int, int], None] | None = None,
    ):
        self.settings = settings
        self.profile = profile
        self.gateway = LMStudioGateway(
            settings.lmstudio_base_url,
            settings.lmstudio_token,
            timeout=float(profile.get("request_timeout", 600)),
            cancel_check=cancel_check,
        )
        self.yolo = YoloMarkDetector(yolo_weights or settings.yolo_weights)
        self.legacy = V14Compatibility(settings.legacy_v14_path)
        self.manage_models = manage_models
        self.progress_callback = progress_callback
        self.cancel_check = cancel_check
        self.template_pages = template_pages if template_pages is not None else {}
        self.questionnaire_callback = questionnaire_callback
        self.model_calls = 0
        self.started_monotonic = time.monotonic()
        self.template_schema_failures: set[str] = set()

    def notify(self, stage: str, progress: float, message: str) -> None:
        if self.progress_callback:
            bounded = max(0.0, min(1.0, progress))
            elapsed = max(0.0, time.monotonic() - self.started_monotonic)
            eta = elapsed * (1 - bounded) / bounded if bounded >= 0.02 else None
            timing = f"calls {self.model_calls} · elapsed {elapsed / 60:.1f}m"
            if eta is not None:
                timing += f" · ETA {eta / 60:.1f}m"
            self.progress_callback(stage, bounded, f"{message} · {timing}")

    def apply_focus_regions(
        self, image: Image.Image, page_ordinal: int
    ) -> tuple[Image.Image, list[list[float]]]:
        page_regions = self.profile.get("focus_regions") or {}
        raw_regions = page_regions.get(str(page_ordinal), []) if isinstance(page_regions, dict) else []
        regions = [bbox for bbox in (valid_bbox(value) for value in raw_regions) if bbox]
        if not regions:
            return image, []
        return (
            build_focus_crop_sheet(
                image,
                regions,
                max_side=int(self.profile.get("image_max_side", 2200)),
            ),
            regions,
        )

    def orient(self, image: Image.Image) -> Image.Image:
        if self.profile.get("orientation_mode", "model") != "model":
            return image
        try:
            self.model_calls += 1
            result = self.gateway.chat_json(
                model=self.profile["extractor_model_id"],
                prompt=orientation_prompt(),
                images=[image.copy().resize((min(image.width, 1200), min(image.height, 1200)))],
                max_tokens=100,
                retries=int(self.profile.get("orientation_retries", 1)),
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
        model_id: str | None = None,
    ) -> list[dict[str, Any]]:
        selected_model = model_id or self.profile["extractor_model_id"]
        prompt = extraction_prompt(page_number, total_pages, pass_name)
        response: dict[str, Any] = {}
        full_page_error: Exception | None = None
        try:
            # One image per request keeps visual tokens bounded on 8k/16k local contexts.
            self.model_calls += 1
            response = self.gateway.chat_json(
                model=selected_model,
                prompt=prompt,
                images=[image],
                max_tokens=int(self.profile.get("extraction_max_tokens", 4096)),
                retries=int(self.profile.get("extraction_retries", 2)),
            )
        except Exception as exc:
            full_page_error = exc

        items = response.get("items")
        tile_count = int(self.profile.get("verifier_tile_count", 4))
        if include_tiles and tile_count > 0 and (full_page_error or not isinstance(items, list) or not items):
            items = []
            tile_errors: list[str] = []
            for tile_number, tile in enumerate(
                self.legacy.zoom_tiles(image, max_tiles=tile_count), start=1
            ):
                try:
                    self.model_calls += 1
                    tile_response = self.gateway.chat_json(
                        model=selected_model,
                        prompt=f"{prompt}\nThis is zoom region {tile_number}; return only questions visible in this region.",
                        images=[tile],
                        max_tokens=int(self.profile.get("tile_max_tokens", 3072)),
                        retries=int(self.profile.get("tile_retries", 1)),
                    )
                    tile_items = tile_response.get("items")
                    if isinstance(tile_items, list):
                        items.extend(tile_items)
                except Exception as exc:
                    tile_errors.append(str(exc)[:160])
            if not items and full_page_error:
                raise RuntimeError(
                    f"Full-page request failed ({full_page_error}); zoom fallback failed ({tile_errors})"
                ) from full_page_error
        if not isinstance(items, list):
            raise ValueError("Extractor JSON omitted the items array")
        return [
            sanitize_item(item, f"P{page_number}-R{index + 1}")
            for index, item in enumerate(items)
            if isinstance(item, dict)
        ]

    def discover_template_schema(
        self, image: Image.Image, page_ordinal: int
    ) -> list[dict[str, Any]]:
        self.model_calls += 1
        response = self.gateway.chat_json(
            model=self.profile["extractor_model_id"],
            prompt=template_schema_prompt(page_ordinal),
            images=[image],
            max_tokens=int(self.profile.get("template_schema_max_tokens", 3072)),
            retries=0,
        )
        items = response.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("Template schema JSON omitted the items array")
        sanitized = [
            sanitize_template_item(item, f"P{page_ordinal}-R{index + 1}")
            for index, item in enumerate(items)
            if isinstance(item, dict)
        ]
        if not sanitized:
            raise ValueError("Template schema did not contain answerable items")
        identifiers = [item["template_question_id"] for item in sanitized]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("Template schema returned duplicate template_question_id values")
        self.template_pages[str(page_ordinal)] = sanitized
        return sanitized

    def extract_compact_pass(
        self,
        image: Image.Image,
        page_ordinal: int,
        template_items: list[dict[str, Any]],
        pass_name: str,
        model_id: str,
    ) -> list[dict[str, Any]]:
        self.model_calls += 1
        expected = len(template_items)
        token_budget = min(
            int(self.profile.get("compact_max_tokens", 1536)),
            max(384, 160 + expected * 58),
        )
        response = self.gateway.chat_json(
            model=model_id,
            prompt=compact_extraction_prompt(page_ordinal, template_items, pass_name),
            images=[image],
            max_tokens=token_budget,
            retries=0,
        )
        return compact_answers_to_items(template_items, response)

    def should_verify_page(self, items: list[dict[str, Any]], page_number: int) -> bool:
        """Select pages needing an independent vision pass in balanced mode."""

        if self.profile.get("verification_mode", "maximum") != "selective":
            return True
        if not items:
            return True
        audit_interval = max(0, int(self.profile.get("verifier_audit_interval", 10)))
        if audit_interval and page_number % audit_interval == 0:
            return True
        threshold = float(self.profile.get("verifier_confidence_threshold", 0.86))
        correction_words = {
            "ambiguous",
            "unclear",
            "corrected",
            "correction",
            "overwrite",
            "overwritten",
            "strikeout",
            "crossed out",
            "更正",
            "塗改",
            "劃掉",
            "划掉",
            "模糊",
        }
        for item in items:
            if item.get("missing_model_output"):
                return True
            value = item_value(item)
            meaningful = not bool(item.get("blank")) or value not in (None, "", [])
            if meaningful and float(item.get("confidence") or 0) < threshold:
                return True
            reason = clean_text(item.get("reason")).casefold()
            if any(word in reason for word in correction_words):
                return True
        return False

    def repair_missing_crops(
        self,
        image: Image.Image,
        page_ordinal: int,
        template_items: list[dict[str, Any]],
        extracted_items: list[dict[str, Any]],
        model_id: str,
    ) -> list[dict[str, Any]]:
        """Use one bounded request containing only answer-region crops omitted by a pass."""

        missing_ids = {
            clean_text(item.get("template_question_id"))
            for item in extracted_items
            if item.get("missing_model_output")
        }
        missing_templates = [
            item
            for item in template_items
            if clean_text(item.get("template_question_id")) in missing_ids
            and valid_bbox(item.get("answer_bbox"))
        ]
        if not missing_templates:
            return extracted_items
        crops = [crop_bbox(image, valid_bbox(item.get("answer_bbox")), padding=0.08) for item in missing_templates]
        self.model_calls += 1
        response = self.gateway.chat_json(
            model=model_id,
            prompt=(
                compact_extraction_prompt(
                    page_ordinal, missing_templates, "targeted repair of omitted answer crops"
                )
                + "\nThe images are answer-region crops in the same order as the template records."
            ),
            images=crops,
            max_tokens=min(1024, max(256, 120 + len(missing_templates) * 60)),
            retries=0,
        )
        repaired = compact_answers_to_items(missing_templates, response)
        repaired_by_id = {
            clean_text(item.get("template_question_id")): item
            for item in repaired
            if not item.get("missing_model_output")
        }
        return [
            repaired_by_id.get(clean_text(item.get("template_question_id")), item)
            for item in extracted_items
        ]

    def tiebreak(self, image: Image.Image, fused: FusedAnswer) -> FusedAnswer:
        bbox = valid_bbox(fused.item.get("answer_bbox")) or valid_bbox(fused.item.get("question_bbox"))
        independent_value = fused.verifier_value if fused.verifier_model_id else fused.yolo_value
        try:
            self.model_calls += 1
            result = self.gateway.chat_json(
                model=self.profile["extractor_model_id"],
                prompt=conflict_prompt(fused.item, [fused.qwen_value, independent_value]),
                images=[crop_bbox(image, bbox)],
                max_tokens=700,
                retries=1,
            )
            if result.get("resolved") and float(result.get("confidence") or 0) >= 0.82:
                resolved = result.get("value")
                fused.scanner_value = resolved
                fused.confidence = float(result.get("confidence"))
                fused.reason += f"; cropped primary-model adjudication: {clean_text(result.get('reason'))}"
                fused.needs_review = normalized_mismatch(resolved, fused.qwen_value, independent_value)
                fused.evidence.append(
                    {
                        "source": "primary_adjudicator",
                        "model_id": self.profile["extractor_model_id"],
                        "label": "cropped conflict adjudication",
                        "bbox": bbox or [0, 0, 1, 1],
                        "confidence": fused.confidence,
                    }
                )
        except Exception as exc:
            fused.reason += f"; cropped tiebreak unavailable: {str(exc)[:100]}"
            fused.needs_review = True
        return fused

    def tiebreak_page(self, image: Image.Image, answers: list[FusedAnswer]) -> None:
        """Adjudicate page conflicts in bounded batches instead of one call per answer."""

        conflicts = [answer for answer in answers if answer.needs_tiebreak]
        if not conflicts:
            return
        chunk_size = max(1, int(self.profile.get("adjudication_chunk_size", 24)))
        for offset in range(0, len(conflicts), chunk_size):
            chunk = conflicts[offset : offset + chunk_size]
            records: list[dict[str, Any]] = []
            by_id: dict[str, FusedAnswer] = {}
            for index, fused in enumerate(chunk, start=1):
                conflict_id = item_key(fused.item, offset + index - 1)
                independent_value = (
                    fused.verifier_value if fused.verifier_model_id else fused.yolo_value
                )
                records.append(
                    {
                        "question_id": conflict_id,
                        "question_text": clean_text(fused.item.get("question_text"))[:500],
                        "answer_type": clean_text(fused.item.get("answer_type")),
                        "answer_bbox": valid_bbox(fused.item.get("answer_bbox"))
                        or valid_bbox(fused.item.get("question_bbox")),
                        "candidates": [fused.qwen_value, independent_value],
                    }
                )
                by_id[conflict_id] = fused
            try:
                self.model_calls += 1
                result = self.gateway.chat_json(
                    model=self.profile["extractor_model_id"],
                    prompt=(
                        page_conflicts_prompt(records)
                        + "\nThe supplied image is a crop sheet. Each crop is labelled with its question_id."
                    ),
                    images=[labeled_crop_sheet(image, records)],
                    max_tokens=min(1800, max(900, 300 + len(records) * 90)),
                    retries=int(self.profile.get("adjudication_retries", 1)),
                )
                resolved_ids: set[str] = set()
                for item in result.get("results", []):
                    if not isinstance(item, dict):
                        continue
                    conflict_id = str(item.get("question_id") or "")
                    fused = by_id.get(conflict_id)
                    if not fused:
                        continue
                    resolved_ids.add(conflict_id)
                    confidence = max(0.0, min(1.0, float(item.get("confidence") or 0)))
                    if item.get("resolved") and confidence >= 0.82:
                        resolved = item.get("value")
                        independent_value = (
                            fused.verifier_value if fused.verifier_model_id else fused.yolo_value
                        )
                        fused.scanner_value = resolved
                        fused.confidence = confidence
                        fused.reason += (
                            "; batched page adjudication: " + clean_text(item.get("reason"))
                        )
                        fused.needs_review = normalized_mismatch(
                            resolved, fused.qwen_value, independent_value
                        )
                        fused.needs_tiebreak = False
                        fused.evidence.append(
                            {
                                "source": "primary_adjudicator",
                                "model_id": self.profile["extractor_model_id"],
                                "label": "batched page conflict adjudication",
                                "bbox": valid_bbox(fused.item.get("answer_bbox"))
                                or valid_bbox(fused.item.get("question_bbox"))
                                or [0, 0, 1, 1],
                                "confidence": confidence,
                            }
                        )
                    else:
                        fused.needs_review = True
                for conflict_id, fused in by_id.items():
                    if conflict_id not in resolved_ids:
                        fused.reason += "; batched adjudicator omitted this conflict"
                        fused.needs_review = True
            except Exception as exc:
                for fused in chunk:
                    fused.reason += f"; batched tiebreak unavailable: {str(exc)[:100]}"
                    fused.needs_review = True

    def extract_one_page(
        self,
        source: Path,
        page_number: int,
        total_pages: int,
        yolo_available: bool,
        image_max_side: int | None = None,
        page_ordinal: int | None = None,
        force_verifier: bool = False,
    ) -> tuple[list[FusedAnswer], dict[str, Any]]:
        ordinal = page_ordinal or page_number
        image = render_page(
            source,
            page_number,
            image_max_side or int(self.profile.get("image_max_side", 3000)),
        )
        original_size = image.size
        image = self.legacy.enhance(image)
        image, focus_regions = self.apply_focus_regions(image, ordinal)
        image = self.orient(image)
        primary_model_id = self.profile["extractor_model_id"]
        verifier_model_id = self.profile.get("verifier_model_id")
        model_errors: dict[str, str] = {}
        verifier_skipped = False
        repair_used = False
        template_items: list[dict[str, Any]] | None = None
        if self.profile.get("template_mode"):
            template_items = self.template_pages.get(str(ordinal))
            if not template_items and str(ordinal) not in self.template_schema_failures:
                try:
                    template_items = self.discover_template_schema(image, ordinal)
                except Exception as exc:
                    model_errors["template"] = str(exc)
                    self.template_schema_failures.add(str(ordinal))
        if verifier_model_id:
            try:
                first = (
                    self.extract_compact_pass(
                        image,
                        ordinal,
                        template_items,
                        "primary vision model pass",
                        primary_model_id,
                    )
                    if template_items
                    else self.extract_pass(
                        image,
                        page_number,
                        total_pages,
                        "primary vision model pass",
                        False,
                        primary_model_id,
                    )
                )
                if template_items and any(
                    item.get("missing_model_output") for item in first
                ):
                    try:
                        repair_used = True
                        first = self.repair_missing_crops(
                            image, ordinal, template_items, first, primary_model_id
                        )
                    except Exception as repair_exc:
                        model_errors["targeted_repair"] = str(repair_exc)
            except Exception as exc:
                first = []
                model_errors["primary"] = str(exc)
            run_verifier = force_verifier or self.should_verify_page(first, page_number)
            if run_verifier:
                try:
                    second = (
                        self.extract_compact_pass(
                            image,
                            ordinal,
                            template_items,
                            "independent verifier vision model pass",
                            str(verifier_model_id),
                        )
                        if template_items
                        else self.extract_pass(
                            image,
                            page_number,
                            total_pages,
                            "independent verifier vision model pass",
                            False,
                            verifier_model_id,
                        )
                    )
                except Exception as exc:
                    second = []
                    model_errors["verifier"] = str(exc)
            else:
                second = []
                verifier_skipped = True
            if not first and not second:
                raise RuntimeError(f"Both vision model passes failed: {model_errors}")
            detections = []
            if verifier_skipped:
                fused_answers = fuse_primary_only(first, primary_model_id)
            elif first and not second and "verifier" in model_errors:
                fused_answers = fuse_primary_only(first, primary_model_id)
                for fused in fused_answers:
                    fused.reason += "; verifier unavailable for this page"
            else:
                fused_answers = fuse_vision_models(
                    first, second, primary_model_id, str(verifier_model_id)
                )
        else:
            if template_items:
                first = self.extract_compact_pass(
                    image, ordinal, template_items, "primary vision model pass", primary_model_id
                )
                if any(item.get("missing_model_output") for item in first):
                    try:
                        repair_used = True
                        first = self.repair_missing_crops(
                            image, ordinal, template_items, first, primary_model_id
                        )
                    except Exception as repair_exc:
                        model_errors["targeted_repair"] = str(repair_exc)
                second = []
                detections = []
                fused_answers = fuse_primary_only(first, primary_model_id)
            else:
                first = self.extract_pass(
                    image, page_number, total_pages, "primary vision model pass", False, primary_model_id
                )
                second = self.extract_pass(
                    image, page_number, total_pages, "independent verification pass", True
                )
                if self.profile.get("local_desktop"):
                    detections = []
                    fused_answers = fuse_qwen_passes(first, second, primary_model_id)
                else:
                    detections = self.yolo.detect(image)
                    fused_answers = fuse_page(first, second, detections, yolo_available)
        geometry_debug: list[dict[str, Any]] = []
        for fused in fused_answers:
            geometry_value, geometry_confidence = geometry_mark_evidence(image, fused.item)
            fused.item["geometry_value"] = geometry_value
            fused.item["geometry_confidence"] = geometry_confidence
            if geometry_value is not None:
                geometry_debug.append(
                    {
                        "template_question_id": fused.item.get("template_question_id"),
                        "value": geometry_value,
                        "confidence": geometry_confidence,
                    }
                )
                fused.evidence.append(
                    {
                        "source": "geometry",
                        "model_id": None,
                        "label": "deterministic option-region ink signal",
                        "bbox": valid_bbox(fused.item.get("answer_bbox")) or [0, 0, 1, 1],
                        "confidence": geometry_confidence,
                    }
                )
                if geometry_confidence >= 0.75:
                    if normalized_mismatch(fused.scanner_value, geometry_value, None):
                        fused.needs_review = True
                        fused.reason += "; deterministic geometry disagrees"
                    else:
                        fused.reason += "; deterministic geometry agrees"
                        fused.confidence = max(fused.confidence, geometry_confidence)
        if not repair_used:
            self.tiebreak_page(image, fused_answers)
            repair_used = any(answer.needs_tiebreak for answer in fused_answers)
        return fused_answers, {
            "primary_model_id": primary_model_id,
            "primary": first,
            "verifier_model_id": verifier_model_id,
            "verifier": second,
            "verifier_skipped": verifier_skipped,
            "verification_mode": self.profile.get("verification_mode", "maximum"),
            "template_mode": bool(template_items),
            "page_ordinal": ordinal,
            "focus_regions_applied": focus_regions,
            "focus_original_size": list(original_size),
            "focus_model_image_size": list(image.size),
            "template_items": template_items or [],
            "geometry": geometry_debug,
            "model_calls": self.model_calls,
            "targeted_repair_used": repair_used,
            "model_errors": model_errors,
            "yolo": [detection.as_dict() for detection in detections],
        }

    def extract_job(self, db: Session, job: Job) -> None:
        if self.manage_models:
            self.gateway.manage_model("load", self.profile["extractor_model_id"])
        source = Path(job.stored_path)
        groups = db.scalars(
            select(QuestionnaireGroup)
            .where(QuestionnaireGroup.job_id == job.id)
            .order_by(QuestionnaireGroup.group_index)
        ).all()
        total_work_pages = sum(group.end_page - group.start_page + 1 for group in groups)
        processed = 0
        dual_vision = bool(self.profile.get("verifier_model_id"))
        yolo_available = False if dual_vision else self.yolo.health()["status"] == "online"
        debug_dir = self.settings.artifacts_dir / job.id / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        page_failures: list[dict[str, Any]] = []
        if self.profile.get("template_mode") and groups:
            reference_index = max(
                0,
                min(
                    len(groups) - 1,
                    int(self.profile.get("template_reference_group_index", 0)),
                ),
            )
            reference_group = groups[reference_index]
            for page_number in range(reference_group.start_page, reference_group.end_page + 1):
                page_ordinal = page_number - reference_group.start_page + 1
                if str(page_ordinal) in self.template_pages:
                    continue
                if job.cancel_requested or (self.cancel_check and self.cancel_check()):
                    job.status = "cancelled"
                    job.stage_message = "Cancelled by user"
                    db.commit()
                    return
                try:
                    self.notify(
                        "template",
                        0.01,
                        f"Discovering template page {page_ordinal} from questionnaire {reference_index + 1}",
                    )
                    template_image = render_page(
                        source,
                        page_number,
                        int(self.profile.get("image_max_side", 2200)),
                    )
                    template_image = self.legacy.enhance(template_image)
                    template_image, _focus_regions = self.apply_focus_regions(
                        template_image, page_ordinal
                    )
                    template_image = self.orient(template_image)
                    self.discover_template_schema(template_image, page_ordinal)
                    snapshot = dict(job.profile_snapshot or {})
                    snapshot["series_template_v1"] = {
                        "version": 1,
                        "pages_per_questionnaire": (
                            reference_group.end_page - reference_group.start_page + 1
                        ),
                        "reference_group_index": reference_index,
                        "pages": self.template_pages,
                    }
                    snapshot["model_calls"] = self.model_calls
                    job.profile_snapshot = snapshot
                    db.commit()
                except Exception as exc:
                    self.template_schema_failures.add(str(page_ordinal))
                    snapshot = dict(job.profile_snapshot or {})
                    warnings = list(snapshot.get("template_schema_warnings") or [])
                    warnings.append(
                        {"page_ordinal": page_ordinal, "error": str(exc)[:500]}
                    )
                    snapshot["template_schema_warnings"] = warnings
                    job.profile_snapshot = snapshot
                    db.commit()
        existing_answers = list(db.scalars(select(Answer).where(Answer.job_id == job.id)).all())
        existing_by_page: dict[int, list[Answer]] = {}
        for answer in existing_answers:
            existing_by_page.setdefault(answer.page_number, []).append(answer)
        completed_pages = {
            page_number
            for page_number, answers in existing_by_page.items()
            if answers
            and not any(answer.question_id.startswith("PAGE-") and answer.question_id.endswith("-EXTRACTION-ERROR") for answer in answers)
        }

        for group in groups:
            for page_number in range(group.start_page, group.end_page + 1):
                db.refresh(job)
                if job.cancel_requested or (self.cancel_check and self.cancel_check()):
                    job.status = "cancelled"
                    job.stage_message = "Cancelled by user"
                    db.commit()
                    return
                if page_number in completed_pages:
                    processed += 1
                    job.stage_message = f"Resuming: page {page_number} already checkpointed"
                    job.progress = (processed / max(1, total_work_pages)) * 0.72
                    self.notify("extracting", job.progress, job.stage_message)
                    db.commit()
                    continue
                job.stage_message = f"Extracting page {page_number} of {job.page_count}"
                job.progress = (processed / max(1, total_work_pages)) * 0.72
                self.notify("extracting", job.progress, job.stage_message)
                db.commit()

                db.execute(
                    delete(Answer).where(Answer.job_id == job.id, Answer.page_number == page_number)
                )
                db.commit()
                fused_answers: list[FusedAnswer] | None = None
                debug_payload: dict[str, Any] = {}
                final_error: Exception | None = None
                max_side = int(self.profile.get("image_max_side", 3000))
                page_attempts = max(1, int(self.profile.get("page_attempts", 2)))
                retry_side = min(max_side, int(self.profile.get("page_retry_max_side", 2200)))
                attempt_sides = [max_side]
                if page_attempts > 1 and retry_side != max_side:
                    attempt_sides.append(retry_side)
                for page_attempt, attempt_side in enumerate(attempt_sides, start=1):
                    try:
                        page_ordinal = page_number - group.start_page + 1
                        calibration_count = int(
                            self.profile.get("verifier_calibration_questionnaires", 2)
                        )
                        calibration_offset = int(
                            self.profile.get("verifier_calibration_offset", 0)
                        )
                        fused_answers, debug_payload = self.extract_one_page(
                            source,
                            page_number,
                            job.page_count,
                            yolo_available,
                            image_max_side=attempt_side,
                            page_ordinal=page_ordinal,
                            force_verifier=(
                                calibration_offset + group.group_index < calibration_count
                            ),
                        )
                        debug_payload["page_attempt"] = page_attempt
                        debug_payload["checkpoint_complete"] = True
                        final_error = None
                        break
                    except Exception as exc:
                        final_error = exc
                        debug_payload = {
                            "page_error": str(exc),
                            "page_attempt": page_attempt,
                            "retry_image_max_side": attempt_side,
                        }
                        if page_attempt < len(attempt_sides):
                            self.notify(
                                "extracting",
                                job.progress,
                                f"Retrying page {page_number} with a smaller image",
                            )

                if fused_answers is not None:
                    for fused in fused_answers:
                        page_ordinal = page_number - group.start_page + 1
                        template_question_id = (
                            clean_text(fused.item.get("template_question_id"))
                            or f"P{page_ordinal}:{item_key(fused.item)}"
                        )
                        answer_key = (
                            f"{job.id}:G{group.group_index + 1}:P{page_ordinal}:"
                            f"{template_question_id}"
                        )[:512]
                        selected = fused.item.get("selected_options") or []
                        if fused.verifier_model_id and fused.item.get("answer_type") in {
                            "single_choice",
                            "multi_choice",
                            "yes_no",
                            "consent",
                            "scale",
                            "matrix",
                        }:
                            if isinstance(fused.scanner_value, list):
                                selected = fused.scanner_value
                            elif fused.scanner_value is not None and fused.scanner_value != "":
                                selected = [fused.scanner_value]
                        db.add(
                            Answer(
                                answer_key=answer_key,
                                job_id=job.id,
                                group_id=group.id,
                                page_number=page_number,
                                page_ordinal=page_ordinal,
                                question_id=item_key(fused.item),
                                template_question_id=template_question_id,
                                question_text=fused.item["question_text"],
                                section=fused.item.get("section", ""),
                                answer_type=fused.item.get("answer_type", "other"),
                                allowed_options=fused.item.get("allowed_options", []),
                                selected_options=selected,
                                qwen_value=fused.qwen_value,
                                yolo_value=fused.yolo_value,
                                verifier_value=fused.verifier_value,
                                verifier_model_id=fused.verifier_model_id,
                                geometry_value=fused.item.get("geometry_value"),
                                geometry_confidence=float(
                                    fused.item.get("geometry_confidence") or 0.0
                                ),
                                scanner_value=fused.scanner_value,
                                scanner_confidence=fused.confidence,
                                fusion_reason=fused.reason,
                                evidence=[{"page_number": page_number, **item} for item in fused.evidence],
                                final_value=fused.scanner_value,
                                final_source="scanner",
                                review_status="pending" if fused.needs_review else "not_required",
                            )
                        )
                else:
                    error_text = str(final_error or "Unknown page extraction failure")
                    page_failures.append({"page_number": page_number, "error": error_text})
                    db.add(
                        Answer(
                            answer_key=f"{job.id}:G{group.group_index + 1}:P{page_number - group.start_page + 1}:EXTRACTION-ERROR",
                            job_id=job.id,
                            group_id=group.id,
                            page_number=page_number,
                            page_ordinal=page_number - group.start_page + 1,
                            question_id=f"PAGE-{page_number}-EXTRACTION-ERROR",
                            template_question_id=f"P{page_number - group.start_page + 1}:EXTRACTION-ERROR",
                            question_text="Page extraction failed",
                            answer_type="other",
                            scanner_value=None,
                            scanner_confidence=0,
                            fusion_reason=error_text[:1000],
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
                if self.template_pages:
                    snapshot = dict(job.profile_snapshot or {})
                    snapshot["series_template_v1"] = {
                        "version": 1,
                        "pages_per_questionnaire": group.end_page - group.start_page + 1,
                        "pages": self.template_pages,
                    }
                    snapshot["model_calls"] = self.model_calls
                    job.profile_snapshot = snapshot
                db.commit()
            if self.questionnaire_callback:
                self.questionnaire_callback(group.group_index + 1, len(groups))
        if page_failures:
            job.error = f"{len(page_failures)} page(s) failed; partial results were preserved"
            db.commit()

    def judge_job(self, db: Session, job: Job) -> None:
        self.yolo.release()
        if self.manage_models and self.profile["judge_model_id"] != self.profile["extractor_model_id"]:
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
            if job.cancel_requested or (self.cancel_check and self.cancel_check()):
                job.status = "cancelled"
                job.stage_message = "Cancelled by user"
                db.commit()
                return
            self.notify("judging", job.progress, f"Checking questionnaire {group_index + 1} of {len(groups)}")
            answers = db.scalars(
                select(Answer).where(Answer.group_id == group.id).order_by(Answer.page_number, Answer.question_id)
            ).all()
            context = {answer.question_id: answer.scanner_value for answer in answers}
            context.update(
                {
                    answer.answer_key: answer.scanner_value
                    for answer in answers
                    if answer.answer_key
                }
            )
            payload: list[dict[str, Any]] = []
            findings: list[dict[str, Any]] = []
            for answer in answers:
                answer.final_value = answer.scanner_value
                answer.final_source = "scanner"
                if answer.question_id.startswith("PAGE-") and answer.question_id.endswith(
                    "-EXTRACTION-ERROR"
                ):
                    answer.reasonableness_status = "not_applicable"
                    answer.judge_reason = "Extraction failure is excluded from reasonableness judging."
                    answer.review_status = "pending"
                    continue
                answer_key = answer.answer_key or answer.id
                record = {
                    "answer_key": answer_key,
                    "question_id": answer.question_id,
                    "question_text": answer.question_text[:1200],
                    "answer_type": answer.answer_type,
                    "allowed_options": (answer.allowed_options or [])[:80],
                    "scanner_value": answer.scanner_value,
                }
                payload.append(record)
                for finding in generic_findings(record):
                    findings.append({**finding, "answer_key": answer_key})
                for rule in rules:
                    definition = dict(rule.definition)
                    definition["rule_id"] = rule.id
                    finding = evaluate_rule(answer.question_id, answer.scanner_value, definition, context)
                    if finding:
                        findings.append({**finding, "answer_key": answer_key})
            results: dict[str, dict[str, Any]] = {}
            for chunk_index, chunk in enumerate(chunk_judge_records(payload), start=1):
                if job.cancel_requested or (self.cancel_check and self.cancel_check()):
                    job.status = "cancelled"
                    job.stage_message = "Cancelled by user"
                    db.commit()
                    return
                chunk_ids = {str(record["answer_key"]) for record in chunk}
                chunk_findings = [
                    finding
                    for finding in findings
                    if str(finding.get("answer_key")) in chunk_ids
                ]
                try:
                    self.model_calls += 1
                    self.notify(
                        "judging",
                        job.progress,
                        f"Checking questionnaire {group_index + 1}, part {chunk_index}",
                    )
                    response = self.gateway.chat_json(
                        model=self.profile["judge_model_id"],
                        prompt=judge_prompt(chunk, chunk_findings),
                        max_tokens=int(self.profile.get("judge_max_tokens", 2048)),
                        retries=int(self.profile.get("judge_retries", 2)),
                    )
                    results.update(
                        {
                            str(item.get("answer_key")): item
                            for item in response.get("results", [])
                            if isinstance(item, dict) and item.get("answer_key")
                        }
                    )
                except Exception as exc:
                    for answer in answers:
                        if (answer.answer_key or answer.id) in chunk_ids:
                            answer.judge_reason = (
                                f"Reasonableness model unavailable for this chunk: {str(exc)[:180]}"
                            )

            findings_by_question: dict[str, list[dict[str, Any]]] = {}
            for finding in findings:
                findings_by_question.setdefault(str(finding.get("answer_key")), []).append(finding)
            for answer in answers:
                if answer.reasonableness_status == "not_applicable":
                    continue
                answer_key = answer.answer_key or answer.id
                result = results.get(answer_key)
                related = findings_by_question.get(answer_key, [])
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
                if status not in {"reasonable", "review_required", "not_applicable"}:
                    status = "review_required"
                confidence = max(0.0, min(1.0, float(result.get("confidence") or 0)))
                suggestion = result.get("suggestion")
                basis = str(result.get("evidence_basis") or "none")
                answer.reasonableness_status = status
                answer.judge_suggestion = suggestion
                answer.judge_reason = clean_text(result.get("reason"))
                answer.judge_confidence = confidence
                answer.rule_refs = [str(item["rule_id"]) for item in related]
                if related:
                    deterministic_reason = "; ".join(
                        str(item["message"]) for item in related
                    )
                    answer.reasonableness_status = "review_required"
                    answer.review_status = "pending"
                    answer.judge_reason = "; ".join(
                        part for part in (deterministic_reason, answer.judge_reason) if part
                    )
                    if answer.judge_suggestion is None:
                        answer.judge_suggestion = next(
                            (
                                item.get("suggestion")
                                for item in related
                                if item.get("suggestion") is not None
                            ),
                            None,
                        )
                if status in {"corrected", "review_required"} or suggestion is not None:
                    answer.review_status = "pending"
                    answer.reasonableness_status = "review_required"
                    if status == "corrected":
                        answer.judge_reason = (
                            "Suggestion only — scanner value was not changed. " + answer.judge_reason
                        ).strip()
            job.progress = 0.76 + ((group_index + 1) / max(1, len(groups))) * 0.14
            self.notify("judging", job.progress, job.stage_message)
            db.commit()


def normalized_mismatch(resolved: Any, qwen: Any, yolo: Any) -> bool:
    def norm(value: Any) -> str:
        if isinstance(value, list):
            return json.dumps(sorted(str(item).casefold().strip() for item in value), ensure_ascii=False)
        return str(value).casefold().strip() if value is not None else ""

    candidates = {norm(value) for value in (qwen, yolo) if value is not None}
    return bool(candidates) and norm(resolved) not in candidates
