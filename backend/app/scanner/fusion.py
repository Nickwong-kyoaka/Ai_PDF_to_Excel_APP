from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from .yolo import Detection


SELECTION_TYPES = {
    "single_choice",
    "multi_choice",
    "yes_no",
    "consent",
    "scale",
    "matrix",
    "matrix_row",
}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalized(value: Any) -> str:
    if isinstance(value, list):
        return json.dumps(sorted(clean_text(v).casefold() for v in value), ensure_ascii=False)
    return clean_text(value).casefold()


def valid_bbox(value: Any) -> list[float] | None:
    if not isinstance(value, list) or len(value) != 4:
        return None
    try:
        bbox = [max(0.0, min(1.0, float(item))) for item in value]
    except (TypeError, ValueError):
        return None
    return bbox if bbox[2] > bbox[0] and bbox[3] > bbox[1] else None


def bbox_iou(a: list[float], b: list[float]) -> float:
    x1, y1 = max(a[0], b[0]), max(a[1], b[1])
    x2, y2 = min(a[2], b[2]), min(a[3], b[3])
    intersection = max(0.0, x2 - x1) * max(0.0, y2 - y1)
    area_a = (a[2] - a[0]) * (a[3] - a[1])
    area_b = (b[2] - b[0]) * (b[3] - b[1])
    return intersection / max(1e-9, area_a + area_b - intersection)


def center_distance(a: list[float], b: list[float]) -> float:
    ac = ((a[0] + a[2]) / 2, (a[1] + a[3]) / 2)
    bc = ((b[0] + b[2]) / 2, (b[1] + b[3]) / 2)
    return math.dist(ac, bc)


def item_key(item: dict[str, Any], index: int = 0) -> str:
    return clean_text(item.get("question_id")) or f"row-{index}-{clean_text(item.get('question_text'))[:80]}"


def item_value(item: dict[str, Any]) -> Any:
    answer_type = clean_text(item.get("answer_type"))
    selected = item.get("selected_options") if isinstance(item.get("selected_options"), list) else []
    if answer_type in SELECTION_TYPES and selected:
        return selected if answer_type == "multi_choice" else selected[0]
    return item.get("value")


def _option_records(item: dict[str, Any]) -> list[dict[str, Any]]:
    options = item.get("allowed_options")
    if not isinstance(options, list):
        return []
    records = []
    for option in options:
        if isinstance(option, str):
            records.append({"label": option, "bbox": None})
        elif isinstance(option, dict):
            records.append({"label": clean_text(option.get("label")), "bbox": valid_bbox(option.get("bbox"))})
    return [record for record in records if record["label"]]


def map_yolo_to_options(item: dict[str, Any], detections: list[Detection]) -> tuple[Any, list[dict[str, Any]]]:
    options = _option_records(item)
    mapped: list[tuple[str, Detection]] = []
    for detection in detections:
        if detection.mark_class == "strikeout":
            continue
        candidates: list[tuple[float, str]] = []
        for option in options:
            bbox = option["bbox"]
            if not bbox:
                continue
            overlap = bbox_iou(detection.bbox, bbox)
            distance = center_distance(detection.bbox, bbox)
            if overlap > 0 or distance < 0.075:
                candidates.append((overlap * 4 - distance, option["label"]))
        if candidates:
            mapped.append((max(candidates)[1], detection))
    if not mapped:
        return None, []
    labels = list(dict.fromkeys(label for label, _ in mapped))
    evidence = [
        {
            "source": "yolo",
            "label": label,
            "mark_class": detection.mark_class,
            "bbox": detection.bbox,
            "confidence": detection.confidence,
        }
        for label, detection in mapped
    ]
    value: Any = labels if clean_text(item.get("answer_type")) == "multi_choice" else labels[0]
    return value, evidence


@dataclass(slots=True)
class FusedAnswer:
    item: dict[str, Any]
    qwen_value: Any
    yolo_value: Any
    scanner_value: Any
    confidence: float
    reason: str
    evidence: list[dict[str, Any]]
    needs_review: bool
    needs_tiebreak: bool
    verifier_value: Any = None
    verifier_model_id: str | None = None


def fuse_primary_only(
    primary_items: list[dict[str, Any]],
    primary_model_id: str,
) -> list[FusedAnswer]:
    """Keep high-confidence primary results when balanced mode skips a page verifier.

    This is deliberately distinct from dual-model agreement: the reason and evidence
    make it clear that no independent visual confirmation was performed.
    """

    output: list[FusedAnswer] = []
    for item in primary_items:
        value = item_value(item)
        confidence = max(0.0, min(1.0, float(item.get("confidence") or 0) * 0.90))
        output.append(
            FusedAnswer(
                item=item,
                qwen_value=value,
                yolo_value=None,
                scanner_value=value,
                confidence=confidence,
                reason="Balanced mode: high-confidence primary extraction; verifier skipped",
                evidence=_model_evidence(item, "primary_vision", primary_model_id),
                needs_review=confidence < 0.80,
                needs_tiebreak=False,
                verifier_value=None,
                verifier_model_id=None,
            )
        )
    return output


def reconcile_qwen(first: dict[str, Any], second: dict[str, Any] | None) -> tuple[Any, float, str, bool]:
    first_value = item_value(first)
    first_conf = float(first.get("confidence") or 0)
    if not second:
        return first_value, first_conf * 0.85, "Only one valid Qwen pass", True
    second_value = item_value(second)
    second_conf = float(second.get("confidence") or 0)
    if normalized(first_value) == normalized(second_value):
        return first_value, min(0.99, max(first_conf, second_conf) + 0.04), "Two independent Qwen passes agree", False
    chosen = first_value if first_conf >= second_conf else second_value
    return chosen, max(first_conf, second_conf) * 0.72, "Independent Qwen passes disagree", True


def fuse_qwen_passes(
    first_items: list[dict[str, Any]],
    second_items: list[dict[str, Any]],
    model_id: str,
) -> list[FusedAnswer]:
    """Fuse two sequential passes of one vision model without implying YOLO evidence."""

    second_map = {item_key(item, index): item for index, item in enumerate(second_items)}
    output: list[FusedAnswer] = []
    for index, item in enumerate(first_items):
        value, confidence, reason, conflict = reconcile_qwen(
            item, second_map.get(item_key(item, index))
        )
        output.append(
            FusedAnswer(
                item=item,
                qwen_value=value,
                yolo_value=None,
                scanner_value=value,
                confidence=confidence,
                reason=reason,
                evidence=_model_evidence(item, "primary_vision", model_id),
                needs_review=conflict or confidence < 0.80,
                needs_tiebreak=conflict,
            )
        )
    return output


def fuse_page(
    first_items: list[dict[str, Any]],
    second_items: list[dict[str, Any]],
    detections: list[Detection],
    yolo_available: bool,
) -> list[FusedAnswer]:
    second_map = {item_key(item, index): item for index, item in enumerate(second_items)}
    output: list[FusedAnswer] = []
    for index, item in enumerate(first_items):
        qwen_value, qwen_conf, qwen_reason, qwen_conflict = reconcile_qwen(
            item, second_map.get(item_key(item, index))
        )
        yolo_value, yolo_evidence = map_yolo_to_options(item, detections)
        evidence = list(yolo_evidence)
        answer_bbox = valid_bbox(item.get("answer_bbox"))
        if answer_bbox:
            evidence.append(
                {
                    "source": "qwen",
                    "label": "answer region",
                    "bbox": answer_bbox,
                    "confidence": float(item.get("confidence") or 0),
                }
            )
        selection = clean_text(item.get("answer_type")) in SELECTION_TYPES
        if selection and yolo_available and yolo_value is not None:
            if normalized(yolo_value) == normalized(qwen_value):
                output.append(FusedAnswer(item, qwen_value, yolo_value, qwen_value, min(0.995, qwen_conf + 0.06), "Qwen and YOLO agree", evidence, False, False))
            else:
                output.append(FusedAnswer(item, qwen_value, yolo_value, qwen_value, qwen_conf * 0.68, "Qwen and YOLO disagree; cropped verification required", evidence, True, True))
        elif selection and yolo_available and yolo_value is None and qwen_value is not None and normalized(qwen_value) != "":
            output.append(FusedAnswer(item, qwen_value, None, qwen_value, qwen_conf * 0.78, "Qwen found a selection but YOLO found no corresponding mark", evidence, True, True))
        else:
            reason = qwen_reason if not selection else f"{qwen_reason}; custom YOLO unavailable"
            output.append(FusedAnswer(item, qwen_value, yolo_value, qwen_value, qwen_conf, reason, evidence, qwen_conflict or qwen_conf < 0.80, qwen_conflict))
    return output


def _match_score(primary: dict[str, Any], verifier: dict[str, Any]) -> float:
    primary_id = item_key(primary)
    verifier_id = item_key(verifier)
    if primary_id and verifier_id and primary_id.casefold() == verifier_id.casefold():
        return 2.0
    primary_text = clean_text(primary.get("question_text")).casefold()
    verifier_text = clean_text(verifier.get("question_text")).casefold()
    text_score = SequenceMatcher(None, primary_text, verifier_text).ratio() if primary_text and verifier_text else 0.0
    primary_box = valid_bbox(primary.get("question_bbox")) or valid_bbox(primary.get("answer_bbox"))
    verifier_box = valid_bbox(verifier.get("question_bbox")) or valid_bbox(verifier.get("answer_bbox"))
    geometry_score = bbox_iou(primary_box, verifier_box) if primary_box and verifier_box else 0.0
    return text_score * 0.78 + geometry_score * 0.22


def _model_evidence(item: dict[str, Any], source: str, model_id: str) -> list[dict[str, Any]]:
    bbox = valid_bbox(item.get("answer_bbox")) or valid_bbox(item.get("question_bbox"))
    if not bbox:
        return []
    return [
        {
            "source": source,
            "model_id": model_id,
            "label": "answer region",
            "bbox": bbox,
            "confidence": float(item.get("confidence") or 0),
        }
    ]


def fuse_vision_models(
    primary_items: list[dict[str, Any]],
    verifier_items: list[dict[str, Any]],
    primary_model_id: str,
    verifier_model_id: str,
) -> list[FusedAnswer]:
    """Associate and fuse two independent vision-model extractions without YOLO."""

    unmatched = set(range(len(verifier_items)))
    output: list[FusedAnswer] = []
    for primary in primary_items:
        candidates = sorted(
            ((_match_score(primary, verifier_items[index]), index) for index in unmatched),
            reverse=True,
        )
        matched: dict[str, Any] | None = None
        if candidates and candidates[0][0] >= 0.66:
            _, matched_index = candidates[0]
            unmatched.remove(matched_index)
            matched = verifier_items[matched_index]

        primary_value = item_value(primary)
        primary_confidence = float(primary.get("confidence") or 0)
        evidence = _model_evidence(primary, "primary_vision", primary_model_id)
        if matched is None:
            output.append(
                FusedAnswer(
                    item=primary,
                    qwen_value=primary_value,
                    yolo_value=None,
                    scanner_value=primary_value,
                    confidence=primary_confidence * 0.78,
                    reason="Primary vision model found an answer that the verifier did not match",
                    evidence=evidence,
                    needs_review=True,
                    needs_tiebreak=True,
                    verifier_value=None,
                    verifier_model_id=verifier_model_id,
                )
            )
            continue

        verifier_value = item_value(matched)
        verifier_confidence = float(matched.get("confidence") or 0)
        evidence.extend(_model_evidence(matched, "verifier_vision", verifier_model_id))
        if normalized(primary_value) == normalized(verifier_value):
            confidence = min(0.99, max(primary_confidence, verifier_confidence) + 0.05)
            low_confidence = confidence < 0.80
            output.append(
                FusedAnswer(
                    item=primary,
                    qwen_value=primary_value,
                    yolo_value=None,
                    scanner_value=primary_value,
                    confidence=confidence,
                    reason="Primary and independent verifier vision models agree",
                    evidence=evidence,
                    needs_review=low_confidence,
                    needs_tiebreak=False,
                    verifier_value=verifier_value,
                    verifier_model_id=verifier_model_id,
                )
            )
        else:
            chosen = primary_value if primary_confidence >= verifier_confidence else verifier_value
            output.append(
                FusedAnswer(
                    item=primary,
                    qwen_value=primary_value,
                    yolo_value=None,
                    scanner_value=chosen,
                    confidence=max(primary_confidence, verifier_confidence) * 0.66,
                    reason="Primary and independent verifier vision models disagree; cropped adjudication required",
                    evidence=evidence,
                    needs_review=True,
                    needs_tiebreak=True,
                    verifier_value=verifier_value,
                    verifier_model_id=verifier_model_id,
                )
            )

    for index in sorted(unmatched):
        verifier = verifier_items[index]
        verifier_value = item_value(verifier)
        verifier_confidence = float(verifier.get("confidence") or 0)
        output.append(
            FusedAnswer(
                item=verifier,
                qwen_value=None,
                yolo_value=None,
                scanner_value=verifier_value,
                confidence=verifier_confidence * 0.72,
                reason="Independent verifier found an answer that the primary model missed",
                evidence=_model_evidence(verifier, "verifier_vision", verifier_model_id),
                needs_review=True,
                needs_tiebreak=True,
                verifier_value=verifier_value,
                verifier_model_id=verifier_model_id,
            )
        )
    return output
