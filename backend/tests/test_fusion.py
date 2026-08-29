from app.scanner.fusion import bbox_iou, fuse_page, fuse_vision_models
from app.scanner.yolo import Detection


def item(value="Yes"):
    return {
        "question_id": "Q1",
        "question_text": "Do you agree?",
        "answer_type": "yes_no",
        "allowed_options": [
            {"label": "Yes", "bbox": [0.1, 0.1, 0.2, 0.2]},
            {"label": "No", "bbox": [0.3, 0.1, 0.4, 0.2]},
        ],
        "selected_options": [value],
        "value": value,
        "answer_bbox": [0.1, 0.1, 0.4, 0.2],
        "confidence": 0.92,
    }


def test_qwen_yolo_agreement_is_confirmed():
    result = fuse_page(
        [item()],
        [item()],
        [Detection("tick", [0.12, 0.12, 0.18, 0.18], 0.96)],
        True,
    )[0]
    assert result.scanner_value == "Yes"
    assert result.yolo_value == "Yes"
    assert result.needs_review is False
    assert "agree" in result.reason


def test_qwen_yolo_disagreement_requests_tiebreak():
    result = fuse_page(
        [item("Yes")],
        [item("Yes")],
        [Detection("cross", [0.32, 0.12, 0.38, 0.18], 0.97)],
        True,
    )[0]
    assert result.yolo_value == "No"
    assert result.needs_tiebreak is True
    assert result.needs_review is True


def test_bbox_iou():
    assert bbox_iou([0, 0, 1, 1], [0.5, 0.5, 1, 1]) == 0.25


def test_dual_vision_agreement_is_confirmed_without_yolo():
    result = fuse_vision_models(
        [item("Yes")],
        [item("Yes")],
        "qwen/qwen3-vl-8b",
        "google/gemma-3-4b",
    )[0]
    assert result.qwen_value == "Yes"
    assert result.verifier_value == "Yes"
    assert result.yolo_value is None
    assert result.needs_review is False
    assert "agree" in result.reason


def test_dual_vision_disagreement_requires_cropped_adjudication():
    result = fuse_vision_models(
        [item("Yes")],
        [item("No")],
        "qwen/qwen3-vl-8b",
        "google/gemma-3-4b",
    )[0]
    assert result.verifier_value == "No"
    assert result.needs_tiebreak is True
    assert result.needs_review is True


def test_verifier_only_question_is_preserved_for_review():
    result = fuse_vision_models(
        [],
        [item("Yes")],
        "qwen/qwen3-vl-8b",
        "google/gemma-3-4b",
    )[0]
    assert result.qwen_value is None
    assert result.verifier_value == "Yes"
    assert result.needs_review is True
