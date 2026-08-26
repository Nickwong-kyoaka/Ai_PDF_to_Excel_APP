from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


SELECTION_TYPES = {"single_choice", "multi_choice", "yes_no", "consent", "scale", "matrix"}


def normalized(value: Any) -> str:
    if isinstance(value, list):
        return "|".join(sorted(str(item).casefold().strip() for item in value))
    return "" if value is None else str(value).casefold().strip()


def edit_distance(left: str, right: str) -> int:
    previous = list(range(len(right) + 1))
    for index, left_char in enumerate(left, start=1):
        current = [index]
        for right_index, right_char in enumerate(right, start=1):
            current.append(min(current[-1] + 1, previous[right_index] + 1, previous[right_index - 1] + (left_char != right_char)))
        previous = current
    return previous[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Check FormSight release acceptance metrics")
    parser.add_argument("prediction", type=Path)
    parser.add_argument("gold", type=Path)
    parser.add_argument("--baseline", type=Path, help="Optional v14 normalized ResultV2 JSON")
    args = parser.parse_args()
    prediction = json.loads(args.prediction.read_text(encoding="utf-8"))
    gold = json.loads(args.gold.read_text(encoding="utf-8"))
    predicted = {str(item["question_id"]): item for item in prediction["answers"]}
    gold_items = {str(item["question_id"]): item for item in gold["answers"]}
    selections = []
    text_accuracy = []
    correction_outcomes = []
    for question_id, expected in gold_items.items():
        actual = predicted.get(question_id, {})
        expected_value = expected.get("final_value", expected.get("scanner_value"))
        actual_value = actual.get("final_value", actual.get("scanner_value"))
        if expected.get("answer_type") in SELECTION_TYPES:
            selections.append(normalized(expected_value) == normalized(actual_value))
        elif expected.get("answer_type") in {"short_text", "long_text", "date", "number"}:
            left, right = normalized(expected_value), normalized(actual_value)
            text_accuracy.append(1 - edit_distance(left, right) / max(1, len(left)))
        if actual.get("final_source") == "qwen_judge":
            correction_outcomes.append(normalized(expected_value) == normalized(actual_value))
    metrics = {
        "selection_exact_accuracy": sum(selections) / max(1, len(selections)),
        "text_character_accuracy": sum(text_accuracy) / max(1, len(text_accuracy)),
        "automatic_correction_precision": sum(correction_outcomes) / max(1, len(correction_outcomes)),
        "selection_count": len(selections),
        "text_count": len(text_accuracy),
        "automatic_correction_count": len(correction_outcomes),
    }
    if args.baseline:
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
        baseline_values = {str(item["question_id"]): item.get("final_value", item.get("scanner_value")) for item in baseline["answers"]}
        baseline_correct = [normalized(gold_items[q].get("final_value")) == normalized(baseline_values.get(q)) for q in gold_items if gold_items[q].get("answer_type") in SELECTION_TYPES]
        baseline_score = sum(baseline_correct) / max(1, len(baseline_correct))
        metrics["selection_improvement_over_v14"] = metrics["selection_exact_accuracy"] - baseline_score
    print(json.dumps(metrics, indent=2))
    failures = []
    if metrics["selection_exact_accuracy"] < 0.97: failures.append("selection accuracy < 97%")
    if metrics["text_character_accuracy"] < 0.90: failures.append("text character accuracy < 90%")
    if correction_outcomes and metrics["automatic_correction_precision"] < 0.98: failures.append("correction precision < 98%")
    if args.baseline and metrics.get("selection_improvement_over_v14", 0) < 0.02: failures.append("selection improvement over v14 < 2 percentage points")
    if failures:
        raise SystemExit("RELEASE BLOCKED: " + "; ".join(failures))
    print("RELEASE METRICS PASSED")


if __name__ == "__main__":
    main()
