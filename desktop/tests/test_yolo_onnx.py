from __future__ import annotations

import numpy as np

from backend.app.scanner.yolo import MARK_CLASSES, decode_yolo_output


NAMES = {index: label for index, label in enumerate(MARK_CLASSES)}


def decode(output: np.ndarray):
    return decode_yolo_output(
        output,
        names=NAMES,
        original_size=(1000, 500),
        input_size=(640, 640),
        scale=0.64,
        padding=(0, 160),
        confidence=0.3,
        iou=0.45,
    )


def test_end_to_end_output_preserves_short_detection_axis_and_transforms_boxes() -> None:
    # Two Nx6 detections must not be mistaken for the raw (features x anchors) layout.
    output = np.array([[[64, 224, 192, 288, 0.9, 0], [65, 225, 191, 287, 0.7, 0]]], dtype=np.float32)
    detections = decode(output)
    assert len(detections) == 1
    assert detections[0].mark_class == "tick"
    assert detections[0].confidence == pytest.approx(0.9)
    assert detections[0].bbox == pytest.approx([0.1, 0.2, 0.3, 0.4])


def test_raw_yolo_output_is_transposed_and_decoded() -> None:
    raw = np.zeros((1, 10, 2), dtype=np.float32)
    raw[0, :4, 0] = [320, 320, 64, 64]
    raw[0, 4 + 1, 0] = 0.85  # cross
    raw[0, :4, 1] = [100, 100, 20, 20]
    raw[0, 4 + 2, 1] = 0.1
    detections = decode(raw)
    assert len(detections) == 1
    assert detections[0].mark_class == "cross"


import pytest
