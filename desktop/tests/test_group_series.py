from __future__ import annotations

import pytest
from PIL import Image, ImageDraw

from desktop.group_series import (
    build_fixed_size_series,
    clone_page_pattern,
    expected_questionnaire_count,
    infer_safe_series_groups,
    numbered_participant_ids,
)


def ranges(groups):
    return [(group.start_page, group.end_page) for group in groups]


def test_fixed_size_series_covers_remainder() -> None:
    assert ranges(build_fixed_size_series(8, 3)) == [(1, 3), (4, 6), (7, 8)]


def test_one_page_and_one_document_presets() -> None:
    assert ranges(build_fixed_size_series(3, 1)) == [(1, 1), (2, 2), (3, 3)]
    assert ranges(build_fixed_size_series(3, 3)) == [(1, 3)]


def test_clone_pattern_drops_source_specific_participant_ids() -> None:
    groups = build_fixed_size_series(6, 2)
    groups[0].participant_id = "CSA001"
    cloned = clone_page_pattern(groups, 6)
    assert ranges(cloned) == [(1, 2), (3, 4), (5, 6)]
    assert all(group.participant_id is None for group in cloned)


def test_clone_pattern_rejects_wrong_page_count() -> None:
    with pytest.raises(ValueError):
        clone_page_pattern(build_fixed_size_series(6, 2), 5)


def test_numbered_participant_ids_are_zero_padded() -> None:
    assert numbered_participant_ids(3, "CSA-", 8) == ["CSA-008", "CSA-009", "CSA-010"]


def test_filename_range_is_inclusive() -> None:
    assert expected_questionnaire_count("survey_001-010.pdf") == 10
    assert expected_questionnaire_count("batch_083.pdf") == 1
    assert expected_questionnaire_count("unlabelled.pdf") is None


def test_repeated_six_page_cycle_produces_ten_questionnaires(tmp_path) -> None:
    pages = []
    for questionnaire in range(10):
        for ordinal in range(6):
            image = Image.new("RGB", (480, 640), "white")
            draw = ImageDraw.Draw(image)
            draw.rectangle((30, 30, 450, 610), outline="black", width=3)
            for row in range(ordinal + 2):
                y = 80 + row * 50
                draw.line((60, y, 420, y), fill="black", width=2)
            draw.ellipse((70 + questionnaire, 570, 78 + questionnaire, 578), fill="black")
            path = tmp_path / f"page-{len(pages) + 1:04d}.jpg"
            image.save(path)
            pages.append(path)

    result = infer_safe_series_groups("survey_001-010.pdf", pages)

    assert result.safe_for_one_take is True
    assert result.expected_questionnaires == 10
    assert result.pages_per_questionnaire == 6
    assert ranges(result.groups) == [
        (1, 6),
        (7, 12),
        (13, 18),
        (19, 24),
        (25, 30),
        (31, 36),
        (37, 42),
        (43, 48),
        (49, 54),
        (55, 60),
    ]


def test_single_page_input_is_deterministically_one_questionnaire(tmp_path) -> None:
    path = tmp_path / "questionnaire.jpg"
    Image.new("RGB", (80, 100), "white").save(path)

    result = infer_safe_series_groups(path.name, [path])

    assert result.safe_for_one_take is True
    assert ranges(result.groups) == [(1, 1)]
