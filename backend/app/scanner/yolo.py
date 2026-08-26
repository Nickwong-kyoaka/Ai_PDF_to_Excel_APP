from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

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


class YoloMarkDetector:
    def __init__(self, weights_path: Path, confidence: float = 0.30, iou: float = 0.45):
        self.weights_path = weights_path
        self.confidence = confidence
        self.iou = iou
        self._model = None
        self._error: str | None = None

    def _load(self) -> None:
        if self._model is not None or self._error:
            return
        if not self.weights_path.exists():
            self._error = "Custom YOLO weights are not installed"
            return
        try:
            from ultralytics import YOLO

            self._model = YOLO(str(self.weights_path), task="detect")
        except Exception as exc:
            self._error = str(exc)

    def health(self) -> dict[str, Any]:
        self._load()
        return {
            "status": "online" if self._model is not None else "not_ready",
            "weights": str(self.weights_path),
            "classes": MARK_CLASSES,
            "error": self._error,
        }

    def detect(self, image: Image.Image) -> list[Detection]:
        self._load()
        if self._model is None:
            return []
        results = self._model.predict(
            source=image, conf=self.confidence, iou=self.iou, verbose=False, device=0
        )
        detections: list[Detection] = []
        for result in results:
            names = result.names
            if result.boxes is None:
                continue
            for xyxyn, confidence, class_id in zip(
                result.boxes.xyxyn.cpu().tolist(),
                result.boxes.conf.cpu().tolist(),
                result.boxes.cls.cpu().tolist(),
                strict=False,
            ):
                label = str(names.get(int(class_id), int(class_id)))
                if label not in MARK_CLASSES:
                    continue
                detections.append(
                    Detection(label, [max(0.0, min(1.0, float(v))) for v in xyxyn], float(confidence))
                )
        return detections

    def release(self) -> None:
        self._model = None
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except ImportError:
            pass
