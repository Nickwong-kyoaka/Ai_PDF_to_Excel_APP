from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageChops, ImageFilter, ImageOps, ImageStat

from backend.app.documents import ProposedGroup, validate_group_partition


FILENAME_RANGE_RE = re.compile(
    r"(?<!\d)(?P<first>\d{1,6})\s*[-\u2013\u2014]\s*(?P<last>\d{1,6})(?!\d)"
)
FILENAME_SINGLE_RE = re.compile(r"(?:^|[_\-\s])(?P<number>\d{1,6})$")


@dataclass(slots=True, frozen=True)
class GroupingInference:
    groups: list[ProposedGroup]
    expected_questionnaires: int | None
    pages_per_questionnaire: int | None
    confidence: float
    reason: str
    safe_for_one_take: bool


def expected_questionnaire_count(filename: str) -> int | None:
    """Read an inclusive questionnaire count from names such as 001-010 or 083."""

    stem = Path(filename).stem
    matches = list(FILENAME_RANGE_RE.finditer(stem))
    if matches:
        match = matches[-1]
        first = int(match.group("first"))
        last = int(match.group("last"))
        if last >= first:
            return last - first + 1
        return None
    return 1 if FILENAME_SINGLE_RE.search(stem) else None


def _layout_image(path: Path) -> Image.Image:
    image = Image.open(path).convert("L")
    image = ImageOps.autocontrast(image)
    image = image.resize((96, 128), Image.Resampling.BILINEAR)
    return image.filter(ImageFilter.FIND_EDGES)


def page_cycle_similarity(page_paths: list[Path], pages_per_questionnaire: int) -> float:
    """Measure repeated printed layouts while tolerating handwriting and response marks."""

    if pages_per_questionnaire < 1 or not page_paths:
        return 0.0
    questionnaire_count = len(page_paths) // pages_per_questionnaire
    if questionnaire_count <= 1:
        return 1.0
    fingerprints = [_layout_image(path) for path in page_paths]
    scores: list[float] = []
    # Compare each page with the same ordinal in the first questionnaire. Printed
    # structure dominates these small edge maps, while handwriting occupies little area.
    for questionnaire_index in range(1, questionnaire_count):
        for page_ordinal in range(pages_per_questionnaire):
            reference = fingerprints[page_ordinal]
            candidate = fingerprints[
                questionnaire_index * pages_per_questionnaire + page_ordinal
            ]
            mean_difference = float(ImageStat.Stat(ImageChops.difference(reference, candidate)).mean[0])
            scores.append(max(0.0, 1.0 - mean_difference / 255.0))
    return sum(scores) / len(scores) if scores else 0.0


def best_template_group_index(page_paths: list[Path], groups: list[ProposedGroup]) -> int:
    """Choose the clearest questionnaire for one-time printed-schema discovery."""

    scores: list[tuple[float, int]] = []
    for index, group in enumerate(groups):
        page_scores: list[float] = []
        for page_number in range(group.start_page, group.end_page + 1):
            try:
                image = Image.open(page_paths[page_number - 1]).convert("L").resize((256, 320))
                contrast = float(ImageStat.Stat(image).stddev[0])
                edges = image.filter(ImageFilter.FIND_EDGES)
                edge_strength = float(ImageStat.Stat(edges).mean[0])
                page_scores.append(contrast + edge_strength * 1.5)
            except (OSError, IndexError):
                page_scores.append(0.0)
        scores.append((sum(page_scores) / max(1, len(page_scores)), index))
    return max(scores, default=(0.0, 0))[1]


def infer_safe_series_groups(filename: str, page_paths: list[Path]) -> GroupingInference:
    """Infer deterministic groups for automatic one-take mode.

    A filename count is accepted only when it divides the document exactly and the
    repeated page layouts agree. Uncertain documents are returned as one proposal
    for optional manual review, but are not marked safe for unattended processing.
    """

    page_count = len(page_paths)
    if page_count < 1:
        return GroupingInference([], None, None, 0.0, "Document has no pages", False)
    if page_count == 1:
        reason = "Single-page image/document is one questionnaire"
        return GroupingInference(
            [ProposedGroup(1, 1, None, 1.0, reason)], 1, 1, 1.0, reason, True
        )
    expected = expected_questionnaire_count(filename)
    if expected and page_count % expected == 0:
        pages_per = page_count // expected
        similarity = page_cycle_similarity(page_paths, pages_per)
        confidence = 0.99 if expected == 1 else min(0.99, 0.72 + similarity * 0.27)
        safe = expected == 1 or similarity >= 0.90
        reason = (
            f"Filename indicates {expected} questionnaire(s); {page_count} pages form "
            f"{pages_per}-page cycles; layout similarity {similarity:.1%}"
        )
        return GroupingInference(
            build_fixed_size_series(page_count, pages_per),
            expected,
            pages_per,
            confidence,
            reason,
            safe,
        )
    if expected:
        reason = (
            f"Filename indicates {expected} questionnaire(s), but {page_count} pages "
            "cannot be divided evenly"
        )
    else:
        reason = "No reliable questionnaire count was found in the filename"
    return GroupingInference(
        [ProposedGroup(1, page_count, None, 0.25, reason)],
        expected,
        None,
        0.25,
        reason,
        False,
    )


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
