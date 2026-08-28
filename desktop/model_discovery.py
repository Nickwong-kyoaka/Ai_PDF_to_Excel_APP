from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass(slots=True, frozen=True)
class DetectedModel:
    api_id: str
    key: str
    display_name: str
    architecture: str
    quantization: str
    context_length: int
    vision: bool
    score: int


@dataclass(slots=True)
class DiscoveryResult:
    status: str
    base_url: str
    port: int
    vision_models: list[DetectedModel] = field(default_factory=list)
    judge_models: list[DetectedModel] = field(default_factory=list)
    selected_vision: DetectedModel | None = None
    selected_judge: DetectedModel | None = None
    message: str = ""


def discover_lmstudio_port() -> int:
    executable = shutil.which("lms")
    if not executable:
        return 1234
    flags = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
    try:
        completed = subprocess.run(
            [executable, "server", "status", "--json", "--quiet"],
            capture_output=True,
            text=True,
            timeout=4,
            check=False,
            creationflags=flags,
        )
        text = completed.stdout.strip()
        start, end = text.find("{"), text.rfind("}")
        payload = json.loads(text[start : end + 1]) if start >= 0 and end >= start else {}
        port = int(payload.get("port") or 0)
        return port if payload.get("running") and 1 <= port <= 65535 else 1234
    except (OSError, ValueError, json.JSONDecodeError, subprocess.SubprocessError):
        return 1234


def _text(model: dict[str, Any]) -> str:
    return " ".join(
        str(model.get(key) or "") for key in ("key", "display_name", "architecture", "publisher", "params_string")
    ).casefold()


def _is_vision(model: dict[str, Any]) -> bool:
    text = _text(model)
    capabilities = model.get("capabilities") or {}
    declared_vision = isinstance(capabilities, dict) and capabilities.get("vision") is True
    return "qwen" in text and (
        declared_vision
        or any(
            marker in text
            for marker in (
                "qwen3-vl",
                "qwen3_vl",
                "qwen3vl",
                "qwen2.5-vl",
                "qwen2_5_vl",
                " vision",
                "-vl",
                "_vl",
            )
        )
    )


def _vision_score(model: dict[str, Any]) -> int:
    text = _text(model)
    if "qwen/qwen3-vl-8b" in text:
        return 400
    if "qwen3-vl" in text or "qwen3_vl" in text or "qwen3vl" in text:
        return 300
    if "qwen2.5-vl" in text or "qwen2_5_vl" in text:
        return 200
    return 100 if _is_vision(model) else 0


def _judge_score(model: dict[str, Any]) -> int:
    text = _text(model)
    if "qwen" not in text or _is_vision(model):
        return 0
    if "qwen3" in text and "8b" in text:
        return 300
    if "qwen3" in text:
        return 200
    return 100


def select_loaded_models(payload: dict[str, Any]) -> tuple[list[DetectedModel], list[DetectedModel]]:
    vision: list[DetectedModel] = []
    judges: list[DetectedModel] = []
    for model in payload.get("models", []):
        if not isinstance(model, dict) or model.get("type") not in {None, "llm"}:
            continue
        instances = model.get("loaded_instances")
        if not isinstance(instances, list) or not instances:
            continue
        instance = instances[0] if isinstance(instances[0], dict) else {}
        key = str(model.get("key") or instance.get("id") or "").strip()
        api_id = str(instance.get("id") or key).strip()
        if not api_id:
            continue
        quantization = model.get("quantization") or {}
        context_length = int((instance.get("config") or {}).get("context_length") or model.get("max_context_length") or 0)
        common = {
            "api_id": api_id,
            "key": key,
            "display_name": str(model.get("display_name") or key),
            "architecture": str(model.get("architecture") or ""),
            "quantization": str(quantization.get("name") if isinstance(quantization, dict) else quantization or ""),
            "context_length": context_length,
        }
        vision_score = _vision_score(model)
        judge_score = _judge_score(model)
        if vision_score:
            vision.append(DetectedModel(**common, vision=True, score=vision_score))
        if judge_score:
            judges.append(DetectedModel(**common, vision=False, score=judge_score))
    vision.sort(key=lambda item: (-item.score, item.display_name.casefold(), item.api_id))
    judges.sort(key=lambda item: (-item.score, item.display_name.casefold(), item.api_id))
    return vision, judges


def loopback_binding_only(port: int) -> bool | None:
    """Return False for a wildcard/LAN listener, True for loopback, or None if inspection failed."""

    flags = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
    try:
        completed = subprocess.run(
            ["netstat", "-ano", "-p", "TCP"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            creationflags=flags,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    found = False
    for line in completed.stdout.splitlines():
        columns = line.split()
        if len(columns) < 4 or columns[0].upper() != "TCP" or columns[-2].upper() != "LISTENING":
            continue
        local = columns[1]
        if not local.endswith(f":{port}"):
            continue
        found = True
        host = local[: -(len(str(port)) + 1)].strip("[]").casefold()
        if host not in {"127.0.0.1", "::1"}:
            return False
    return True if found else None


def discover_models(timeout: float = 5.0) -> DiscoveryResult:
    port = discover_lmstudio_port()
    base_url = f"http://127.0.0.1:{port}"
    try:
        response = httpx.get(f"{base_url}/api/v1/models", timeout=timeout)
        if response.status_code in {401, 403}:
            return DiscoveryResult(
                "authentication_required", base_url, port,
                message="LM Studio authentication is enabled. Disable it for this loopback-only desktop app.",
            )
        response.raise_for_status()
        vision, judges = select_loaded_models(response.json())
    except Exception as exc:
        return DiscoveryResult(
            "offline", base_url, port,
            message=f"Start the LM Studio local server, then load a Qwen VL model. ({str(exc)[:140]})",
        )
    binding = loopback_binding_only(port)
    if binding is False:
        return DiscoveryResult(
            "network_exposed",
            base_url,
            port,
            message=(
                "LM Studio is listening beyond this PC. In LM Studio Server Settings, disable "
                "'Serve on Local Network', restart the server, then refresh. / LM Studio 正在對外監聽；"
                "請停用「Serve on Local Network」、重新啟動伺服器，再按重新偵測。"
            ),
        )
    if binding is None:
        return DiscoveryResult(
            "binding_unknown",
            base_url,
            port,
            message=(
                "Could not verify that LM Studio is loopback-only. Restart LM Studio with "
                "'Serve on Local Network' disabled. / 無法確認 LM Studio 僅限本機；請停用網路分享後重新啟動。"
            ),
        )
    if not vision:
        return DiscoveryResult(
            "no_vision_model", base_url, port, judge_models=judges,
            message="LM Studio is online, but no loaded Qwen vision model was detected.",
        )
    selected_vision = vision[0]
    selected_judge = judges[0] if judges else selected_vision
    return DiscoveryResult(
        "ready",
        base_url,
        port,
        vision_models=vision,
        judge_models=judges,
        selected_vision=selected_vision,
        selected_judge=selected_judge,
        message="Loaded models detected automatically.",
    )
