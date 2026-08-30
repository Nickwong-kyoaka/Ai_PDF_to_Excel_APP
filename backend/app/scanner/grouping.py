from __future__ import annotations

from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageOps

from ..documents import ProposedGroup
from .lmstudio import LMStudioGateway


def contact_sheet(page_paths: list[Path], first_page_number: int) -> Image.Image:
    thumbnails: list[Image.Image] = []
    for offset, path in enumerate(page_paths):
        image = Image.open(path).convert("RGB")
        image.thumbnail((360, 480), Image.Resampling.LANCZOS)
        framed = Image.new("RGB", (380, 520), "white")
        framed.paste(image, ((380 - image.width) // 2, 32))
        draw = ImageDraw.Draw(framed)
        draw.rectangle((0, 0, 379, 519), outline="#6b7d77", width=2)
        draw.text((12, 9), f"PAGE {first_page_number + offset}", fill="#102f2a")
        thumbnails.append(framed)
    columns = min(4, len(thumbnails))
    rows = (len(thumbnails) + columns - 1) // columns
    sheet = Image.new("RGB", (columns * 380, rows * 520), "#dfe6e2")
    for index, image in enumerate(thumbnails):
        sheet.paste(image, ((index % columns) * 380, (index // columns) * 520))
    return sheet


def visual_grouping(
    page_paths: list[Path],
    gateway: LMStudioGateway,
    model_id: str,
    *,
    retries: int = 1,
) -> list[ProposedGroup]:
    if len(page_paths) <= 1:
        return [ProposedGroup(1, 1, None, 0.8, "Single-page document")]
    starts: dict[int, dict[str, Any]] = {1: {"participant_id": None, "confidence": 0.6}}
    batch_size = 12
    for batch_start in range(0, len(page_paths), batch_size):
        batch = page_paths[batch_start : batch_start + batch_size]
        first_page = batch_start + 1
        last_page = batch_start + len(batch)
        prompt = f"""
The contact sheet contains PDF pages {first_page} through {last_page}, each visibly labelled PAGE N.
Identify pages that begin a new person's questionnaire. Continuation pages are not starts.
Use repeated cover/header structure and visible participant IDs, but do not invent missing IDs.
Return {{"starts":[{{"page":1,"participant_id":null,"confidence":0.0,"reason":"short visual reason"}}]}}.
Only return start pages inside this batch. Page 1 of the whole PDF must be a start.
""".strip()
        result = gateway.chat_json(
            model=model_id,
            prompt=prompt,
            images=[contact_sheet(batch, first_page)],
            max_tokens=1200,
            retries=retries,
        )
        for item in result.get("starts", []):
            if not isinstance(item, dict):
                continue
            page = int(item.get("page") or 0)
            if first_page <= page <= last_page:
                starts[page] = {
                    "participant_id": str(item.get("participant_id") or "").strip() or None,
                    "confidence": max(0.0, min(1.0, float(item.get("confidence") or 0))),
                    "reason": str(item.get("reason") or "Visual questionnaire boundary"),
                }
    ordered = sorted(starts)
    groups = []
    for index, start in enumerate(ordered):
        end = ordered[index + 1] - 1 if index + 1 < len(ordered) else len(page_paths)
        metadata = starts[start]
        groups.append(
            ProposedGroup(
                start_page=start,
                end_page=end,
                participant_id=metadata.get("participant_id"),
                confidence=float(metadata.get("confidence") or 0),
                reason=str(metadata.get("reason") or "Visual questionnaire boundary"),
            )
        )
    return groups
