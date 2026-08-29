from pathlib import Path

from PIL import Image

from app.config import Settings
from app.scanner.extractor import QuestionnaireExtractor, chunk_judge_records


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
