from app.scanner.fusion import bbox_iou, fuse_page
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
