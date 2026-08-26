from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pymupdf as fitz
from PIL import Image, ImageOps


PID_RE = re.compile(r"\b(?:CSA|[A-C])\s*[-_ ]?\s*0*(\d{1,5})\b", re.IGNORECASE)


@dataclass(slots=True)
class DocumentInfo:
    page_count: int
    embedded_text: list[str]
    page_images: list[Path]


@dataclass(slots=True)
class ProposedGroup:
    start_page: int
    end_page: int
    participant_id: str | None
    confidence: float
    reason: str


def _canonical_pid(match: re.Match[str]) -> str:
    full = match.group(0).replace(" ", "").replace("_", "").replace("-", "")
    prefix_match = re.match(r"[A-Za-z]+", full)
    prefix = (prefix_match.group(0) if prefix_match else "ID").upper()
    return f"{prefix}{int(match.group(1)):03d}"


def inspect_and_render(
    source: Path, pages_root: Path, *, max_pages: int, thumbnail_dpi: int = 110
) -> DocumentInfo:
    pages_root.mkdir(parents=True, exist_ok=True)
    suffix = source.suffix.casefold()
    texts: list[str] = []
    images: list[Path] = []
    if suffix == ".pdf":
        try:
            document = fitz.open(source)
        except Exception as exc:
            raise ValueError(f"Invalid or corrupted PDF: {exc}") from exc
        try:
            if document.needs_pass:
                raise ValueError("Password-protected PDFs are not supported")
            if document.page_count < 1:
                raise ValueError("The PDF has no pages")
            if document.page_count > max_pages:
                raise ValueError(f"The PDF exceeds the {max_pages}-page limit")
            scale = thumbnail_dpi / 72
            for index, page in enumerate(document):
                texts.append(page.get_text("text")[:12000])
                pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
                image_path = pages_root / f"page-{index + 1:04d}.jpg"
                pixmap.save(image_path)
                images.append(image_path)
        finally:
            document.close()
        return DocumentInfo(len(images), texts, images)

    try:
        image = Image.open(source)
        image.seek(0)
        image = ImageOps.exif_transpose(image).convert("RGB")
        image.thumbnail((1800, 1800), Image.Resampling.LANCZOS)
        image_path = pages_root / "page-0001.jpg"
        image.save(image_path, quality=90)
    except Exception as exc:
        raise ValueError(f"Invalid or corrupted image: {exc}") from exc
    return DocumentInfo(1, [""], [image_path])


def propose_groups(texts: list[str]) -> list[ProposedGroup]:
    if not texts:
        return []
    page_pids: list[str | None] = []
    for text in texts:
        match = PID_RE.search(text[:5000])
        page_pids.append(_canonical_pid(match) if match else None)

    starts = [0]
    current_pid = page_pids[0]
    for index in range(1, len(page_pids)):
        page_pid = page_pids[index]
        if page_pid and current_pid and page_pid != current_pid:
            starts.append(index)
            current_pid = page_pid
        elif page_pid and not current_pid:
            current_pid = page_pid

    groups: list[ProposedGroup] = []
    for group_index, start in enumerate(starts):
        end_exclusive = starts[group_index + 1] if group_index + 1 < len(starts) else len(texts)
        pid = next((page_pids[i] for i in range(start, end_exclusive) if page_pids[i]), None)
        has_pid_boundary = len(starts) > 1
        groups.append(
            ProposedGroup(
                start_page=start + 1,
                end_page=end_exclusive,
                participant_id=pid,
                confidence=0.92 if has_pid_boundary else (0.72 if pid else 0.35),
                reason=(
                    "Participant ID boundary detected from embedded text"
                    if has_pid_boundary
                    else "One questionnaire proposed; confirm page boundaries before processing"
                ),
            )
        )
    return groups


def validate_group_partition(groups: list[tuple[int, int]], page_count: int) -> None:
    expected = 1
    for start, end in sorted(groups):
        if start != expected:
            raise ValueError("Groups must cover every page exactly once without gaps or overlaps")
        if end < start or end > page_count:
            raise ValueError("A group contains an invalid page range")
        expected = end + 1
    if expected != page_count + 1:
        raise ValueError("Groups must cover every page exactly once")
