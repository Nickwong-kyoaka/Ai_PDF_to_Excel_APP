from __future__ import annotations

import json
from typing import Any


EXTRACTION_SCHEMA = {
    "meta": {"page_language": "English/Traditional Chinese/Simplified Chinese/Mixed", "page_type": "string"},
    "items": [
        {
            "question_id": "stable visible id or generated page-row id",
            "question_text": "printed question text",
            "section": "section heading or empty",
            "answer_type": "single_choice/multi_choice/yes_no/consent/scale/matrix/short_text/long_text/date/time/number/signature/other",
            "allowed_options": [{"label": "printed option", "bbox": [0.0, 0.0, 1.0, 1.0]}],
            "selected_options": ["only visibly selected labels"],
            "value": "answer, list, number, or null",
            "question_bbox": [0.0, 0.0, 1.0, 1.0],
            "answer_bbox": [0.0, 0.0, 1.0, 1.0],
            "blank": False,
            "confidence": 0.0,
            "reason": "short visual evidence reason",
        }
    ],
    "quality_flags": [],
}


def extraction_prompt(page_number: int, total_pages: int, pass_name: str) -> str:
    return f"""
Extract every questionnaire answer visible on page {page_number} of {total_pages}.
This is independent {pass_name}; inspect the image yourself.

Rules:
1. Support English, Traditional Chinese, Simplified Chinese, and mixed pages without translating answer values.
2. Separate allowed_options (all printed choices) from selected_options (only visibly marked choices).
3. A printed checkbox, digit, or option is not selected without a visible tick, cross, fill, circle, or selection underline.
4. Never infer gender, health, consent, or any answer from names or context. Use only physical marks and handwriting.
5. For corrected marks, use the visibly final mark and mention the correction in reason.
6. Keep blanks as null with blank=true. Do not use "unclear" as an answer.
7. Transcribe short and long handwritten answers exactly; do not improve grammar or judge reasonableness.
8. Bboxes are normalized [x1,y1,x2,y2] in 0..1 relative to the supplied full page.
9. Return one JSON object only and use this shape:
{json.dumps(EXTRACTION_SCHEMA, ensure_ascii=False)}
""".strip()


def orientation_prompt() -> str:
    return "Inspect only page orientation. Return {\"rotation_degrees\":0|90|180|270,\"confidence\":0..1}."


def conflict_prompt(item: dict[str, Any], candidate_values: list[Any]) -> str:
    return f"""
Resolve one mark-reading conflict from the supplied crop. Do not re-extract other questions.
Question data: {json.dumps(item, ensure_ascii=False)}
Candidates: {json.dumps(candidate_values, ensure_ascii=False)}
Return {{"value":...,"selected_options":[],"confidence":0..1,"reason":"visible mark evidence","resolved":true|false}}.
If the crop cannot establish the physical mark, resolved must be false.
""".strip()


def judge_prompt(items: list[dict[str, Any]], deterministic_findings: list[dict[str, Any]]) -> str:
    return f"""
Check whether the extracted questionnaire answers are reasonable. This is data-quality review, not medical diagnosis.
Never change subjective/free-text opinions. Never invent a value. A correction is allowed only when supported by a
printed allowed option, a deterministic rule, or unambiguous cross-field evidence in this same questionnaire.

Answers: {json.dumps(items, ensure_ascii=False)}
Deterministic findings: {json.dumps(deterministic_findings, ensure_ascii=False)}

Return {{"results":[{{"question_id":"...","status":"reasonable|corrected|review_required|not_applicable",
"suggestion":null,"reason":"short explanation","confidence":0..1,
"evidence_basis":"none|deterministic_rule|printed_option|cross_field"}}]}}.
Return one result for every supplied question_id. JSON only.
""".strip()
