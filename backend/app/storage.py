from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

from fastapi import HTTPException, UploadFile


ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._()\-\u3400-\u9fff]+")


def safe_filename(name: str) -> str:
    cleaned = SAFE_NAME_RE.sub("_", Path(name).name).strip("._")
    return (cleaned or "questionnaire")[:180]


async def save_upload(
    upload: UploadFile, destination: Path, max_bytes: int
) -> tuple[str, int]:
    extension = Path(upload.filename or "").suffix.casefold()
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=415, detail="Supported formats: PDF, PNG, JPEG, TIFF")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    digest = hashlib.sha256()
    total = 0
    try:
        with temporary.open("wb") as handle:
            while chunk := await upload.read(1024 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    raise HTTPException(status_code=413, detail="Upload is larger than the configured limit")
                digest.update(chunk)
                handle.write(chunk)
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        await upload.close()
    return digest.hexdigest(), total


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_under(root: Path, stored_path: str) -> Path:
    root_resolved = root.resolve()
    candidate = Path(stored_path).resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid storage path") from exc
    return candidate
