from __future__ import annotations

from backend.app.documents import ProposedGroup, validate_group_partition


def build_fixed_size_series(page_count: int, pages_per_questionnaire: int) -> list[ProposedGroup]:
    """Build a complete, non-overlapping page series for one source document."""
    if page_count < 1:
        raise ValueError("Page count must be at least 1")
    if pages_per_questionnaire < 1:
        raise ValueError("Pages per questionnaire must be at least 1")

    groups: list[ProposedGroup] = []
    for start_page in range(1, page_count + 1, pages_per_questionnaire):
        groups.append(
            ProposedGroup(
                start_page=start_page,
                end_page=min(page_count, start_page + pages_per_questionnaire - 1),
                participant_id=None,
                confidence=1.0,
                reason="Page-series preset",
            )
        )
    return groups


def clone_page_pattern(groups: list[ProposedGroup], page_count: int) -> list[ProposedGroup]:
    """Copy only a validated page-range pattern, leaving participant IDs source-specific."""
    validate_group_partition([(group.start_page, group.end_page) for group in groups], page_count)
    return [
        ProposedGroup(
            start_page=group.start_page,
            end_page=group.end_page,
            participant_id=None,
            confidence=1.0,
            reason="Copied page-series pattern",
        )
        for group in groups
    ]


def numbered_participant_ids(group_count: int, prefix: str, first_number: int) -> list[str]:
    if group_count < 0:
        raise ValueError("Group count cannot be negative")
    cleaned_prefix = prefix.strip() or "ID-"
    return [f"{cleaned_prefix}{number:03d}" for number in range(first_number, first_number + group_count)]
