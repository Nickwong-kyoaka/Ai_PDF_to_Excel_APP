from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any

from .yolo import Detection


SELECTION_TYPES = {"single_choice", "multi_choice", "yes_no", "consent", "scale", "matrix"}


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
