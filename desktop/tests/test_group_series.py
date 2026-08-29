from __future__ import annotations

import pytest

from desktop.group_series import build_fixed_size_series, clone_page_pattern, numbered_participant_ids


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
