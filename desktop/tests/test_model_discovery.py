from __future__ import annotations

import json
from types import SimpleNamespace

import httpx

from desktop import model_discovery


def loaded_model(
    key: str,
    *,
    architecture: str,
    display: str | None = None,
    loaded: bool = True,
    vision: bool | None = None,
) -> dict:
    return {
        "type": "llm",
        "key": key,
        "display_name": display or key,
        "architecture": architecture,
        "capabilities": {"vision": vision} if vision is not None else {},
        "quantization": {"name": "Q4_K_M"},
        "max_context_length": 32768,
        "loaded_instances": (
            [{"id": f"instance::{key}", "config": {"context_length": 16384}}] if loaded else []
        ),
    }


def test_loaded_model_priority_and_non_qwen_verifier() -> None:
    payload = {
        "models": [
            loaded_model("qwen/qwen2.5-vl-7b", architecture="qwen2_5_vl"),
            loaded_model("qwen/qwen3-vl-4b", architecture="qwen3_vl"),
            loaded_model("qwen/qwen3-vl-8b", architecture="qwen3_vl"),
            loaded_model("qwen/qwen3-8b", architecture="qwen3"),
            loaded_model("google/gemma-3-4b", architecture="gemma3", vision=True),
            loaded_model("qwen/qwen3-vl-32b", architecture="qwen3_vl", loaded=False),
        ]
    }
    vision, judges = model_discovery.select_loaded_models(payload)
    assert vision[0].key == "qwen/qwen3-vl-8b"
    assert judges[0].key == "qwen/qwen3-8b"
    assert all(model.key != "qwen/qwen3-vl-32b" for model in vision)
    assert model_discovery.select_verifier_model(vision, vision[0]).key == "google/gemma-3-4b"


def test_lms_status_port_discovery(monkeypatch) -> None:
    monkeypatch.setattr(model_discovery.shutil, "which", lambda _name: "lms.exe")
    monkeypatch.setattr(
        model_discovery.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout="notice\n" + json.dumps({"running": True, "port": 2468})
        ),
    )
    assert model_discovery.discover_lmstudio_port() == 2468


def test_manual_server_address_is_normalized_and_public_targets_are_rejected() -> None:
    assert model_discovery.normalize_server_address("192.168.10.25:2468") == "http://192.168.10.25:2468"
    assert model_discovery.normalize_server_address("gpu-pc") == "http://gpu-pc:1234"
    assert model_discovery.normalize_server_address("https://scanner.internal/v1") == "https://scanner.internal:443"

    for value in ("https://example.com", "8.8.8.8:1234", "ftp://192.168.1.4:1234"):
        try:
            model_discovery.normalize_server_address(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Public/unsupported target was accepted: {value}")


def test_recent_private_servers_are_saved_without_duplicates(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    model_discovery.remember_server("192.168.1.50:1234")
    model_discovery.remember_server("http://192.168.1.50:1234")
    model_discovery.remember_server("gpu-pc:2468")

    assert model_discovery.load_recent_servers() == [
        "http://gpu-pc:2468",
        "http://192.168.1.50:1234",
    ]


def test_discovery_reports_authentication_and_offline(monkeypatch) -> None:
    monkeypatch.setattr(model_discovery, "discover_lmstudio_port", lambda: 1234)
    monkeypatch.setattr(model_discovery, "loopback_binding_only", lambda _port: True)
    monkeypatch.setattr(
        model_discovery.httpx,
        "get",
        lambda *args, **kwargs: httpx.Response(401, request=httpx.Request("GET", args[0])),
    )
    assert model_discovery.discover_models().status == "authentication_required"

    def offline(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(model_discovery.httpx, "get", offline)
    assert model_discovery.discover_models().status == "offline"


def test_discovery_requires_independent_non_qwen_verifier(monkeypatch) -> None:
    monkeypatch.setattr(model_discovery, "discover_lmstudio_port", lambda: 5555)
    monkeypatch.setattr(model_discovery, "loopback_binding_only", lambda _port: True)
    payload = {"models": [loaded_model("qwen/qwen3-vl-8b", architecture="qwen3_vl")]}
    monkeypatch.setattr(
        model_discovery.httpx,
        "get",
        lambda *args, **kwargs: SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: payload,
        ),
    )
    result = model_discovery.discover_models()
    assert result.status == "verifier_required"
    assert result.base_url == "http://127.0.0.1:5555"
    assert result.selected_judge == result.selected_vision


def test_discovery_selects_gemma_verifier_and_reuses_primary_for_judging(monkeypatch) -> None:
    monkeypatch.setattr(model_discovery, "discover_lmstudio_port", lambda: 5555)
    monkeypatch.setattr(model_discovery, "loopback_binding_only", lambda _port: True)
    payload = {
        "models": [
            loaded_model("qwen/qwen3-vl-8b", architecture="qwen3_vl"),
            loaded_model("google/gemma-3-4b", architecture="gemma3", vision=True),
        ]
    }
    monkeypatch.setattr(
        model_discovery.httpx,
        "get",
        lambda *args, **kwargs: SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: payload,
        ),
    )
    result = model_discovery.discover_models()
    assert result.status == "ready"
    assert result.selected_vision.key == "qwen/qwen3-vl-8b"
    assert result.selected_verifier.key == "google/gemma-3-4b"
    assert result.selected_judge == result.selected_vision


def test_manual_private_server_is_probed_without_local_binding_check(monkeypatch) -> None:
    payload = {
        "models": [
            loaded_model("qwen/qwen3-vl-8b", architecture="qwen3_vl"),
            loaded_model("google/gemma-3-4b", architecture="gemma3", vision=True),
        ]
    }
    requested: list[str] = []

    def fake_get(url, **kwargs):
        requested.append(url)
        return SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: payload,
        )

    monkeypatch.setattr(model_discovery.httpx, "get", fake_get)
    monkeypatch.setattr(
        model_discovery,
        "loopback_binding_only",
        lambda _port: (_ for _ in ()).throw(AssertionError("manual target checked local netstat")),
    )

    result = model_discovery.discover_models(base_url="192.168.5.8:5678")

    assert result.status == "ready"
    assert result.base_url == "http://192.168.5.8:5678"
    assert result.port == 5678
    assert requested == ["http://192.168.5.8:5678/api/v1/models"]


def test_discovery_rejects_network_exposed_server(monkeypatch) -> None:
    monkeypatch.setattr(model_discovery, "discover_lmstudio_port", lambda: 1234)
    monkeypatch.setattr(model_discovery, "loopback_binding_only", lambda _port: False)
    payload = {"models": [loaded_model("qwen/qwen3-vl-8b", architecture="qwen3_vl")]}
    monkeypatch.setattr(
        model_discovery.httpx,
        "get",
        lambda *args, **kwargs: SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: payload,
        ),
    )
    result = model_discovery.discover_models()
    assert result.status == "network_exposed"
    assert "Serve on Local Network" in result.message
