from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
from fastapi import APIRouter, Depends

from ..config import get_settings
from ..scanner.lmstudio import LMStudioGateway
from ..scanner.yolo import YoloMarkDetector
from ..security import require_roles


router = APIRouter(tags=["system"])
settings = get_settings()


def worker_health() -> dict[str, Any]:
    path = settings.data_dir / "worker-heartbeat.json"
    if not path.exists():
        return {"status": "offline", "message": "Worker has not reported yet"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        timestamp = datetime.fromisoformat(payload["time"])
        age = (datetime.now(timezone.utc) - timestamp).total_seconds()
        return {**payload, "status": payload.get("status") if age < 30 else "offline", "age_seconds": age}
    except Exception as exc:
        return {"status": "offline", "error": str(exc)}


def gpu_info() -> dict[str, Any]:
    try:
        completed = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total,memory.used,driver_version", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        rows = []
        for line in completed.stdout.splitlines():
            name, total, used, driver = [item.strip() for item in line.split(",", 3)]
            rows.append({"name": name, "memory_total_mb": int(total), "memory_used_mb": int(used), "driver": driver})
        return {"status": "online", "devices": rows}
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)[:180]}


@router.get("/health")
def health():
    return {"status": "ok", "service": "FormSight API", "version": "0.1.0"}


@router.get("/system/preflight")
def preflight(_admin=Depends(require_roles("admin"))):
    disk = shutil.disk_usage(settings.data_dir)
    lmstudio = LMStudioGateway(settings.lmstudio_base_url, settings.lmstudio_token).health()
    yolo = YoloMarkDetector(settings.yolo_weights).health()
    loopback = "127.0.0.1" in settings.lmstudio_base_url or "localhost" in settings.lmstudio_base_url
    admin_password = settings.effective_admin_password
    security_ready = loopback and bool(settings.lmstudio_token) and settings.cookie_secure and not admin_password.startswith("Change-")
    return {
        "status": "ready" if lmstudio["status"] == "online" and yolo["status"] == "online" and security_ready else "attention_required",
        "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine()},
        "python": platform.python_version(),
        "cpu": {"logical_cores": psutil.cpu_count(), "avx2_check": "run scripts/preflight.ps1 for Windows CPU flags"},
        "memory": {"total_gb": round(psutil.virtual_memory().total / 1024**3, 1), "available_gb": round(psutil.virtual_memory().available / 1024**3, 1)},
        "disk": {"free_gb": round(disk.free / 1024**3, 1), "total_gb": round(disk.total / 1024**3, 1)},
        "gpu": gpu_info(),
        "lmstudio": lmstudio,
        "yolo": yolo,
        "worker": worker_health(),
        "security": {"lmstudio_loopback": loopback, "api_token_configured": bool(settings.lmstudio_token), "cookie_secure": settings.cookie_secure, "bootstrap_password_changed": not admin_password.startswith("Change-")},
    }
