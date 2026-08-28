from __future__ import annotations

import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image


MARK_CLASSES = [
    "tick",
    "cross",
    "filled_mark",
    "circle",
    "underline_selection",
    "strikeout",
]


@dataclass(slots=True)
class Detection:
    mark_class: str
    bbox: list[float]
    confidence: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_names(value: Any) -> dict[int, str]:
    if isinstance(value, dict):
        return {int(key): str(label) for key, label in value.items()}
    if isinstance(value, list):
        return {index: str(label) for index, label in enumerate(value)}
    if isinstance(value, str) and value.strip():
        for parser in (json.loads, ast.literal_eval):
            try:
                return _parse_names(parser(value))
            except (ValueError, SyntaxError, TypeError, json.JSONDecodeError):
                continue
    return {index: label for index, label in enumerate(MARK_CLASSES)}


def _box_iou(box: np.ndarray, boxes: np.ndarray) -> np.ndarray:
    x1 = np.maximum(box[0], boxes[:, 0])
    y1 = np.maximum(box[1], boxes[:, 1])
    x2 = np.minimum(box[2], boxes[:, 2])
    y2 = np.minimum(box[3], boxes[:, 3])
    intersection = np.maximum(0.0, x2 - x1) * np.maximum(0.0, y2 - y1)
    area_a = max(0.0, float(box[2] - box[0])) * max(0.0, float(box[3] - box[1]))
    area_b = np.maximum(0.0, boxes[:, 2] - boxes[:, 0]) * np.maximum(0.0, boxes[:, 3] - boxes[:, 1])
    return intersection / np.maximum(area_a + area_b - intersection, 1e-9)


def _nms(boxes: np.ndarray, scores: np.ndarray, threshold: float) -> list[int]:
    order = scores.argsort()[::-1]
    keep: list[int] = []
    while order.size:
        current = int(order[0])
        keep.append(current)
        if order.size == 1:
            break
        remaining = order[1:]
        order = remaining[_box_iou(boxes[current], boxes[remaining]) <= threshold]
    return keep


def decode_yolo_output(
    output: np.ndarray,
    *,
    names: dict[int, str],
    original_size: tuple[int, int],
    input_size: tuple[int, int],
    scale: float,
    padding: tuple[int, int],
    confidence: float,
    iou: float,
) -> list[Detection]:
    data = np.asarray(output, dtype=np.float32).squeeze()
    if data.ndim != 2:
        raise ValueError(f"Unsupported YOLO output shape: {tuple(np.asarray(output).shape)}")
    expected_columns = {6, 4 + len(names), 5 + len(names)}
    if data.shape[1] not in expected_columns and data.shape[0] in expected_columns:
        data = data.T
    class_count = len(names)
    if data.shape[1] == 6:
        boxes = data[:, :4].copy()
        scores = data[:, 4]
        class_ids = data[:, 5].astype(int)
    elif data.shape[1] in {4 + class_count, 5 + class_count}:
        xywh = data[:, :4]
        class_scores = data[:, -class_count:]
        class_ids = class_scores.argmax(axis=1)
        scores = class_scores[np.arange(len(class_scores)), class_ids]
        if data.shape[1] == 5 + class_count:
            scores = scores * data[:, 4]
        boxes = np.column_stack(
            (
                xywh[:, 0] - xywh[:, 2] / 2,
                xywh[:, 1] - xywh[:, 3] / 2,
                xywh[:, 0] + xywh[:, 2] / 2,
                xywh[:, 1] + xywh[:, 3] / 2,
            )
        )
    else:
        raise ValueError(f"YOLO output has {data.shape[1]} columns for {class_count} classes")

    selected = np.where(scores >= confidence)[0]
    if not selected.size:
        return []
    boxes, scores, class_ids = boxes[selected], scores[selected], class_ids[selected]
    input_width, input_height = input_size
    if boxes.size and float(np.nanmax(np.abs(boxes))) <= 2.0:
        boxes[:, [0, 2]] *= input_width
        boxes[:, [1, 3]] *= input_height

    keep: list[int] = []
    for class_id in np.unique(class_ids):
        class_indexes = np.where(class_ids == class_id)[0]
        keep.extend(class_indexes[index] for index in _nms(boxes[class_indexes], scores[class_indexes], iou))

    original_width, original_height = original_size
    pad_x, pad_y = padding
    detections: list[Detection] = []
    for index in sorted(keep, key=lambda item: float(scores[item]), reverse=True):
        label = names.get(int(class_ids[index]), str(int(class_ids[index])))
        if label not in MARK_CLASSES:
            continue
        x1, y1, x2, y2 = boxes[index]
        x1 = np.clip((x1 - pad_x) / scale, 0, original_width)
        x2 = np.clip((x2 - pad_x) / scale, 0, original_width)
        y1 = np.clip((y1 - pad_y) / scale, 0, original_height)
        y2 = np.clip((y2 - pad_y) / scale, 0, original_height)
        if x2 <= x1 or y2 <= y1:
            continue
        detections.append(
            Detection(
                label,
                [float(x1 / original_width), float(y1 / original_height), float(x2 / original_width), float(y2 / original_height)],
                float(scores[index]),
            )
        )
    return detections


class YoloMarkDetector:
    """Small ONNX Runtime detector used by both the server and packaged desktop app."""

    def __init__(self, weights_path: Path, confidence: float = 0.30, iou: float = 0.45):
        self.weights_path = weights_path
        self.confidence = confidence
        self.iou = iou
        self._session = None
        self._input_name = ""
        self._input_size = (640, 640)
        self._names = {index: label for index, label in enumerate(MARK_CLASSES)}
        self._provider = ""
        self._error: str | None = None
        self._warning: str | None = None

    def _load(self) -> None:
        if self._session is not None or self._error:
            return
        if not self.weights_path.exists():
            self._error = "Custom YOLO ONNX weights are not installed"
            return
        try:
            import onnxruntime as ort

            if hasattr(ort, "preload_dlls"):
                try:
                    ort.preload_dlls(directory="")
                except Exception as exc:
                    self._warning = f"GPU libraries could not be preloaded: {str(exc)[:180]}"
            available = ort.get_available_providers()
            providers = [name for name in ("CUDAExecutionProvider", "CPUExecutionProvider") if name in available]
            if not providers:
                raise RuntimeError("ONNX Runtime has no usable execution provider")
            options = ort.SessionOptions()
            options.log_severity_level = 3
            try:
                self._session = ort.InferenceSession(
                    str(self.weights_path), sess_options=options, providers=providers
                )
            except Exception as gpu_exc:
                if "CPUExecutionProvider" not in available:
                    raise
                self._warning = f"CUDA unavailable; YOLO is using CPU: {str(gpu_exc)[:180]}"
                self._session = ort.InferenceSession(
                    str(self.weights_path),
                    sess_options=options,
                    providers=["CPUExecutionProvider"],
                )
            self._provider = self._session.get_providers()[0]
            model_input = self._session.get_inputs()[0]
            self._input_name = model_input.name
            shape = model_input.shape
            height = int(shape[2]) if len(shape) > 3 and isinstance(shape[2], int) and shape[2] > 0 else 640
            width = int(shape[3]) if len(shape) > 3 and isinstance(shape[3], int) and shape[3] > 0 else 640
            self._input_size = (width, height)
            metadata = self._session.get_modelmeta().custom_metadata_map or {}
            self._names = _parse_names(metadata.get("names") or metadata.get("classes"))
        except Exception as exc:
            self._session = None
            self._error = str(exc)

    def health(self) -> dict[str, Any]:
        self._load()
        return {
            "status": "online" if self._session is not None else "not_ready",
            "weights": str(self.weights_path),
            "classes": list(self._names.values()),
            "provider": self._provider,
            "error": self._error,
            "warning": self._warning,
        }

    def detect(self, image: Image.Image) -> list[Detection]:
        self._load()
        if self._session is None:
            return []
        source = image.convert("RGB")
        original_width, original_height = source.size
        input_width, input_height = self._input_size
        scale = min(input_width / original_width, input_height / original_height)
        resized_width = max(1, round(original_width * scale))
        resized_height = max(1, round(original_height * scale))
        resized = source.resize((resized_width, resized_height), Image.Resampling.BILINEAR)
        pad_x = (input_width - resized_width) // 2
        pad_y = (input_height - resized_height) // 2
        canvas = np.full((input_height, input_width, 3), 114, dtype=np.uint8)
        canvas[pad_y : pad_y + resized_height, pad_x : pad_x + resized_width] = np.asarray(resized)
        tensor = np.transpose(canvas.astype(np.float32) / 255.0, (2, 0, 1))[None, ...]
        output = self._session.run(None, {self._input_name: tensor})[0]
        return decode_yolo_output(
            output,
            names=self._names,
            original_size=(original_width, original_height),
            input_size=self._input_size,
            scale=scale,
            padding=(pad_x, pad_y),
            confidence=self.confidence,
            iou=self.iou,
        )

    def release(self) -> None:
        self._session = None
