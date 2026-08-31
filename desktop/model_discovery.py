from __future__ import annotations

import json
import ipaddress
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from PIL import Image, ImageDraw

from backend.app.scanner.lmstudio import LMStudioGateway


RECENT_SERVER_LIMIT = 5


def _history_path() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
    return base / "FormSight Local" / "lmstudio-servers.json"


def _allowed_private_host(host: str) -> bool:
    lowered = host.casefold().rstrip(".")
    if lowered == "localhost":
        return True
    try:
        address = ipaddress.ip_address(lowered)
    except ValueError:
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", lowered):
            return lowered.endswith((".local", ".lan", ".internal", ".home.arpa"))
        return True  # A single-label Windows/LAN computer name.
    shared_v4 = ipaddress.ip_network("100.64.0.0/10")
    return bool(
        address.is_loopback
        or address.is_private
        or address.is_link_local
        or (isinstance(address, ipaddress.IPv4Address) and address in shared_v4)
    )


def normalize_server_address(value: str, *, allow_public: bool = False) -> str:
    """Normalize an LM Studio endpoint; routable IPs require explicit opt-in."""

    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Enter an LM Studio computer name or private IP address")
    if "://" not in raw:
        raw = "http://" + raw
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("LM Studio server must use http:// or https://")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Credentials, query strings, and fragments are not allowed in the server address")
    host = parsed.hostname or ""
    if not host:
        raise ValueError("Enter an LM Studio server host")
    if not _allowed_private_host(host):
        try:
            public_ip = ipaddress.ip_address(host)
        except ValueError:
            public_ip = None
        if not allow_public or public_ip is None:
            raise ValueError(
                "This is a public/routable address. Enable the advanced public-server option "
                "only when its firewall is restricted to this PC or VPN."
            )
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 1234)
    except ValueError as exc:
        raise ValueError("LM Studio server port must be between 1 and 65535") from exc
    if not 1 <= port <= 65535:
        raise ValueError("LM Studio server port must be between 1 and 65535")
    path = parsed.path.rstrip("/")
    if path not in {"", "/v1", "/api/v1"}:
        raise ValueError("Enter only the LM Studio server address, without an API path")
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{parsed.scheme}://{rendered_host}:{port}"


def is_routable_server_address(value: str) -> bool:
    """Return True only for explicit globally routable IP addresses."""

    try:
        normalized = normalize_server_address(value, allow_public=True)
        host = urlsplit(normalized).hostname or ""
        address = ipaddress.ip_address(host)
    except (ValueError, TypeError):
        return False
    shared_v4 = ipaddress.ip_network("100.64.0.0/10")
    return not bool(
        address.is_loopback
        or address.is_private
        or address.is_link_local
        or (isinstance(address, ipaddress.IPv4Address) and address in shared_v4)
    )


def load_recent_servers() -> list[str]:
    try:
        payload = json.loads(_history_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    recent: list[str] = []
    for value in payload:
        try:
            normalized = normalize_server_address(str(value), allow_public=True)
        except ValueError:
            continue
        if normalized not in recent:
            recent.append(normalized)
    return recent[:RECENT_SERVER_LIMIT]


def remember_server(value: str, *, allow_public: bool = False) -> None:
    normalized = normalize_server_address(value, allow_public=allow_public)
    recent = [normalized, *[item for item in load_recent_servers() if item != normalized]]
    path = _history_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(recent[:RECENT_SERVER_LIMIT], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


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
    selected_verifier: DetectedModel | None = None
    selected_judge: DetectedModel | None = None
    probe_results: dict[str, str] = field(default_factory=dict)
    message: str = ""


def probe_model_capability(base_url: str, model_id: str, timeout: float = 30.0) -> tuple[bool, str]:
    """Verify that a loaded model can actually see an image and obey a tiny JSON schema."""

    image = Image.new("RGB", (128, 128), "white")
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, 63, 63), fill="red")
    draw.rectangle((64, 0, 127, 63), fill="green")
    draw.rectangle((0, 64, 63, 127), fill="yellow")
    draw.rectangle((64, 64, 127, 127), fill="blue")
    gateway = LMStudioGateway(base_url, "", timeout=timeout)
    try:
        result = gateway.chat_json(
            model=model_id,
            prompt=(
                'Inspect the four-color test image. Return exactly one JSON object matching '
                '{"vision":true,"bottom_right":"blue"}. Use lowercase English for the color.'
            ),
            images=[image],
            max_tokens=96,
            retries=0,
        )
    except Exception as exc:
        return False, str(exc)[:300]
    vision = result.get("vision") is True
    color = str(result.get("bottom_right") or "").strip().casefold()
    if not vision or color not in {"blue", "#0000ff"}:
        return False, f"vision/schema test returned {result!r}"[:300]
    return True, "vision + strict JSON passed"


def probe_text_model_capability(
    base_url: str, model_id: str, timeout: float = 20.0
) -> tuple[bool, str]:
    """Verify a reasonableness model without requiring image support."""

    gateway = LMStudioGateway(base_url, "", timeout=timeout)
    try:
        result = gateway.chat_json(
            model=model_id,
            prompt='Return exactly {"text_json":true,"value":"否"}. Do not translate the value.',
            max_tokens=64,
            retries=0,
        )
    except Exception as exc:
        return False, str(exc)[:300]
    if result.get("text_json") is not True or str(result.get("value") or "") != "否":
        return False, f"text/schema test returned {result!r}"[:300]
    return True, "text + strict JSON passed"


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
    return declared_vision or any(
        marker in text
        for marker in (
            "qwen3-vl",
            "qwen3_vl",
            "qwen3vl",
            "qwen2.5-vl",
            "qwen2_5_vl",
            "gemma-3-4b",
            "gemma3",
            "internvl",
            "minicpm-v",
            " vision",
            "-vl",
            "_vl",
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
    if "qwen" in text and _is_vision(model):
        return 100
    return 10 if _is_vision(model) else 0


def select_verifier_model(
    models: list[DetectedModel], primary: DetectedModel
) -> DetectedModel | None:
    def verifier_score(model: DetectedModel) -> tuple[int, str]:
        text = f"{model.key} {model.display_name} {model.architecture}".casefold()
        if model.api_id == primary.api_id or "qwen" in text:
            return (-1, model.display_name.casefold())
        if "gemma-3-4b" in text or ("gemma3" in text and "4b" in text):
            return (500, model.display_name.casefold())
        if "internvl" in text and "4b" in text:
            return (450, model.display_name.casefold())
        if any(size in text for size in ("2b", "3b", "4b", "5b")):
            return (400, model.display_name.casefold())
        return (300, model.display_name.casefold())

    candidates = [model for model in models if verifier_score(model)[0] >= 0]
    return max(candidates, key=verifier_score, default=None)


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


def discover_models(
    timeout: float = 5.0,
    base_url: str | None = None,
    *,
    allow_public: bool = False,
) -> DiscoveryResult:
    manual_target = bool(base_url and base_url.strip())
    if manual_target:
        try:
            target = normalize_server_address(str(base_url), allow_public=allow_public)
        except ValueError as exc:
            return DiscoveryResult("invalid_server", str(base_url), 0, message=str(exc))
        parsed = urlsplit(target)
        port = int(parsed.port or (443 if parsed.scheme == "https" else 1234))
    else:
        port = discover_lmstudio_port()
        target = f"http://127.0.0.1:{port}"
    try:
        response = httpx.get(f"{target}/api/v1/models", timeout=timeout)
        if response.status_code in {401, 403}:
            return DiscoveryResult(
                "authentication_required", target, port,
                message="LM Studio authentication is enabled. This desktop build does not send an API token.",
            )
        response.raise_for_status()
        vision, judges = select_loaded_models(response.json())
    except Exception as exc:
        return DiscoveryResult(
            "offline", target, port,
            message=f"Could not reach {target}. Start its LM Studio server and check the address/firewall. ({str(exc)[:120]})",
        )
    if not manual_target:
        binding = loopback_binding_only(port)
        if binding is False:
            return DiscoveryResult(
                "network_exposed",
                target,
                port,
                message=(
                    "Auto-detected LM Studio is listening beyond this PC. Select its explicit private-LAN "
                    "address if this is intentional, or disable 'Serve on Local Network'."
                ),
            )
        if binding is None:
            return DiscoveryResult(
                "binding_unknown",
                target,
                port,
                message=(
                    "Could not verify the auto-detected server binding. Enter its explicit localhost/private-LAN "
                    "address, or restart LM Studio with network sharing disabled."
                ),
            )
    qwen_vision = [
        model
        for model in vision
        if "qwen" in f"{model.key} {model.display_name} {model.architecture}".casefold()
    ]
    if not qwen_vision:
        return DiscoveryResult(
            "no_vision_model", target, port, judge_models=judges,
            vision_models=vision,
            message="LM Studio is online, but no loaded Qwen vision model was detected.",
        )
    selected_vision = qwen_vision[0]
    selected_verifier = select_verifier_model(vision, selected_vision)
    selected_judge = judges[0] if judges else selected_vision
    judge_probe = "reusing primary Qwen vision model"
    if selected_judge.api_id != selected_vision.api_id:
        judge_passed, judge_probe = probe_text_model_capability(target, selected_judge.api_id)
        if not judge_passed:
            selected_judge = selected_vision
            judge_probe = f"text model rejected; reusing primary ({judge_probe})"
    if not selected_verifier:
        passed, detail = probe_model_capability(target, selected_vision.api_id)
        if not passed:
            return DiscoveryResult(
                "model_probe_failed",
                target,
                port,
                vision_models=vision,
                judge_models=judges,
                selected_vision=selected_vision,
                selected_judge=selected_judge,
                probe_results={"primary": detail, "judge": judge_probe},
                message=f"The primary model failed the preflight image/JSON test: {detail}.",
            )
        return DiscoveryResult(
            "qwen_only",
            target,
            port,
            vision_models=vision,
            judge_models=judges,
            selected_vision=selected_vision,
            selected_judge=selected_judge,
            probe_results={"primary": detail, "judge": judge_probe},
            message=(
                "Qwen-only mode passed preflight. For selective independent verification, load "
                "a non-Qwen vision model such as google/gemma-3-4b Q4."
            ),
        )
    probe_results: dict[str, str] = {}
    for role, model in (("primary", selected_vision), ("verifier", selected_verifier)):
        passed, detail = probe_model_capability(target, model.api_id)
        probe_results[role] = detail
        if not passed:
            return DiscoveryResult(
                "model_probe_failed",
                target,
                port,
                vision_models=vision,
                judge_models=judges,
                selected_vision=selected_vision,
                selected_verifier=selected_verifier,
                selected_judge=selected_judge,
                probe_results={**probe_results, "judge": judge_probe},
                message=(
                    f"The {role} model failed the preflight image/JSON test: {detail}. "
                    "Reload a vision-capable model before scanning."
                ),
            )
    return DiscoveryResult(
        "ready",
        target,
        port,
        vision_models=vision,
        judge_models=judges,
        selected_vision=selected_vision,
        selected_verifier=selected_verifier,
        selected_judge=selected_judge,
        probe_results={**probe_results, "judge": judge_probe},
        message="Sequential dual-model consensus passed the image/JSON preflight.",
    )
