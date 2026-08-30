from pathlib import Path

from PIL import Image

from app.config import Settings
from app.scanner.extractor import (
    QuestionnaireExtractor,
    chunk_judge_records,
    compact_answers_to_items,
)
from app.scanner.fusion import FusedAnswer


def extracted_item(value: str = "Yes") -> dict:
    return {
        "question_id": "Q1",
        "question_text": "Do you agree?",
        "answer_type": "yes_no",
        "allowed_options": ["Yes", "No"],
        "selected_options": [value],
        "value": value,
        "question_bbox": [0.1, 0.1, 0.8, 0.3],
        "answer_bbox": [0.6, 0.1, 0.8, 0.3],
        "confidence": 0.9,
    }


def make_extractor(tmp_path: Path) -> QuestionnaireExtractor:
    settings = Settings(
        data_dir=tmp_path,
        legacy_v14_path=tmp_path / "missing-v14.py",
        yolo_weights=tmp_path / "missing.onnx",
    )
    settings.ensure_directories()
    extractor = QuestionnaireExtractor(
        settings,
        {
            "extractor_model_id": "qwen/qwen3-vl-8b",
            "verifier_model_id": "google/gemma-3-4b",
            "judge_model_id": "qwen/qwen3-vl-8b",
            "image_max_side": 1200,
        },
        manage_models=False,
    )
    extractor.orient = lambda image: image
    extractor.legacy.enhance = lambda image: image
    extractor.legacy.zoom_tiles = lambda image, max_tiles=4: []
    return extractor


def test_dual_models_run_sequentially_and_agree(tmp_path, monkeypatch):
    extractor = make_extractor(tmp_path)
    monkeypatch.setattr(
        "app.scanner.extractor.render_page",
        lambda *args, **kwargs: Image.new("RGB", (100, 100), "white"),
    )
    calls: list[str] = []

    def fake_pass(image, page_number, total_pages, pass_name, include_tiles, model_id=None):
        calls.append(model_id)
        return [extracted_item()]

    extractor.extract_pass = fake_pass
    answers, debug = extractor.extract_one_page(Path("survey.pdf"), 1, 1, False)

    assert calls == ["qwen/qwen3-vl-8b", "google/gemma-3-4b"]
    assert answers[0].qwen_value == "Yes"
    assert answers[0].verifier_value == "Yes"
    assert answers[0].needs_review is False
    assert debug["model_errors"] == {}


def test_verifier_result_survives_primary_model_failure(tmp_path, monkeypatch):
    extractor = make_extractor(tmp_path)
    monkeypatch.setattr(
        "app.scanner.extractor.render_page",
        lambda *args, **kwargs: Image.new("RGB", (100, 100), "white"),
    )
    extractor.tiebreak = lambda image, fused: fused

    def fake_pass(image, page_number, total_pages, pass_name, include_tiles, model_id=None):
        if model_id.startswith("qwen/"):
            raise RuntimeError("primary unavailable")
        return [extracted_item()]

    extractor.extract_pass = fake_pass
    answers, debug = extractor.extract_one_page(Path("survey.pdf"), 1, 1, False)

    assert answers[0].qwen_value is None
    assert answers[0].verifier_value == "Yes"
    assert answers[0].needs_review is True
    assert "primary" in debug["model_errors"]


def test_balanced_mode_skips_verifier_for_high_confidence_page(tmp_path, monkeypatch):
    extractor = make_extractor(tmp_path)
    extractor.profile.update(
        {
            "verification_mode": "selective",
            "verifier_confidence_threshold": 0.86,
            "verifier_audit_interval": 10,
        }
    )
    monkeypatch.setattr(
        "app.scanner.extractor.render_page",
        lambda *args, **kwargs: Image.new("RGB", (100, 100), "white"),
    )
    calls: list[str] = []

    def fake_pass(image, page_number, total_pages, pass_name, include_tiles, model_id=None):
        calls.append(model_id)
        return [extracted_item()]

    extractor.extract_pass = fake_pass
    answers, debug = extractor.extract_one_page(Path("survey.pdf"), 1, 60, False)

    assert calls == ["qwen/qwen3-vl-8b"]
    assert debug["verifier_skipped"] is True
    assert answers[0].verifier_model_id is None
    assert "verifier skipped" in answers[0].reason


def test_balanced_mode_verifies_uncertain_and_audit_pages(tmp_path):
    extractor = make_extractor(tmp_path)
    extractor.profile.update(
        {
            "verification_mode": "selective",
            "verifier_confidence_threshold": 0.86,
            "verifier_audit_interval": 10,
        }
    )
    uncertain = extracted_item()
    uncertain["confidence"] = 0.7

    assert extractor.should_verify_page([uncertain], 1) is True
    assert extractor.should_verify_page([extracted_item()], 10) is True
    assert extractor.should_verify_page([extracted_item()], 1) is False


def test_page_conflicts_are_adjudicated_in_one_request(tmp_path):
    extractor = make_extractor(tmp_path)
    calls: list[dict] = []

    class Gateway:
        def chat_json(self, **kwargs):
            calls.append(kwargs)
            return {
                "results": [
                    {
                        "question_id": "Q1",
                        "value": "Yes",
                        "confidence": 0.94,
                        "reason": "visible tick",
                        "resolved": True,
                    },
                    {
                        "question_id": "Q2",
                        "value": "No",
                        "confidence": 0.93,
                        "reason": "visible cross",
                        "resolved": True,
                    },
                ]
            }

    extractor.gateway = Gateway()
    answers = [
        FusedAnswer(
            item={**extracted_item("Yes"), "question_id": "Q1"},
            qwen_value="Yes",
            yolo_value=None,
            scanner_value="Yes",
            confidence=0.5,
            reason="conflict",
            evidence=[],
            needs_review=True,
            needs_tiebreak=True,
            verifier_value="No",
            verifier_model_id="google/gemma-3-4b",
        ),
        FusedAnswer(
            item={**extracted_item("No"), "question_id": "Q2"},
            qwen_value="No",
            yolo_value=None,
            scanner_value="No",
            confidence=0.5,
            reason="conflict",
            evidence=[],
            needs_review=True,
            needs_tiebreak=True,
            verifier_value="Yes",
            verifier_model_id="google/gemma-3-4b",
        ),
    ]

    extractor.tiebreak_page(Image.new("RGB", (100, 100), "white"), answers)

    assert len(calls) == 1
    assert calls[0]["images"][0].size == (1200, 190)
    assert all(not answer.needs_tiebreak for answer in answers)
    assert [answer.scanner_value for answer in answers] == ["Yes", "No"]


def test_reasonableness_records_are_chunked_for_local_context_limits():
    records = [
        {
            "question_id": f"Q{index}",
            "question_text": "Question " + ("x" * 300),
            "scanner_value": "Yes",
        }
        for index in range(55)
    ]
    chunks = chunk_judge_records(records, max_items=20, max_json_chars=8000)

    assert len(chunks) >= 3
    assert [record["question_id"] for chunk in chunks for record in chunk] == [
        record["question_id"] for record in records
    ]
    assert all(len(chunk) <= 20 for chunk in chunks)


def test_dense_matrix_keeps_every_template_row_as_an_independent_answer():
    template = [
        {
            "template_question_id": f"P3:MATRIX:R{row}",
            "question_text": f"Matrix row {row}",
            "answer_type": "matrix",
            "allowed_options": ["Yes", "No"],
            "answer_bbox": [0.1, row / 25, 0.9, (row + 1) / 25],
        }
        for row in range(1, 20)
    ]
    response = {
        "answers": [
            {
                "template_question_id": item["template_question_id"],
                "value": "Yes" if index % 2 else "No",
                "blank": False,
                "confidence": 0.9,
                "mark_type": "tick",
            }
            for index, item in enumerate(template)
        ]
    }

    answers = compact_answers_to_items(template, response)

    assert len(answers) == 19
    assert len({answer["template_question_id"] for answer in answers}) == 19


def test_missing_compact_answer_uses_one_targeted_crop_repair(tmp_path, monkeypatch):
    extractor = make_extractor(tmp_path)
    extractor.profile.update({"template_mode": True, "verifier_model_id": None})
    extractor.template_pages["1"] = [
        {
            "template_question_id": "P1:Q1",
            "question_id": "P1:Q1",
            "question_text": "First",
            "answer_type": "short_text",
            "allowed_options": [],
            "question_bbox": [0.05, 0.05, 0.95, 0.25],
            "answer_bbox": [0.5, 0.08, 0.9, 0.2],
        },
        {
            "template_question_id": "P1:Q2",
            "question_id": "P1:Q2",
            "question_text": "Second",
            "answer_type": "short_text",
            "allowed_options": [],
            "question_bbox": [0.05, 0.3, 0.95, 0.5],
            "answer_bbox": [0.5, 0.33, 0.9, 0.46],
        },
    ]
    monkeypatch.setattr(
        "app.scanner.extractor.render_page",
        lambda *args, **kwargs: Image.new("RGB", (200, 200), "white"),
    )
    calls: list[dict] = []

    class Gateway:
        def chat_json(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return {
                    "answers": [
                        {
                            "template_question_id": "P1:Q1",
                            "value": "A",
                            "blank": False,
                            "confidence": 0.95,
                        }
                    ]
                }
            return {
                "answers": [
                    {
                        "template_question_id": "P1:Q2",
                        "value": "B",
                        "blank": False,
                        "confidence": 0.92,
                    }
                ]
            }

    extractor.gateway = Gateway()
    answers, debug = extractor.extract_one_page(
        Path("survey.pdf"), 1, 1, False, page_ordinal=1
    )

    assert [answer.scanner_value for answer in answers] == ["A", "B"]
    assert len(calls) == 2
    assert len(calls[1]["images"]) == 1
    assert debug["targeted_repair_used"] is True
