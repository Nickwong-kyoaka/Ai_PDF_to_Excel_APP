from __future__ import annotations

import base64

import pytest

from app.config import Settings


def encoded(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def test_deployment_wizard_secrets_and_models_are_configurable():
    settings = Settings(
        bootstrap_admin_password_b64=encoded("管理員-Strong-$-Password"),
        lmstudio_api_key_b64=encoded("lm-token-with-special-#-value"),
        extractor_model_id="local/qwen-vision",
        judge_model_id="local/qwen-judge",
    )

    assert settings.effective_admin_password == "管理員-Strong-$-Password"
    assert settings.lmstudio_token == "lm-token-with-special-#-value"
    assert settings.extractor_model_id == "local/qwen-vision"
    assert settings.judge_model_id == "local/qwen-judge"


def test_invalid_encoded_secret_is_rejected():
    settings = Settings(lmstudio_api_key_b64="not-base64!")
    with pytest.raises(ValueError, match="Invalid base64"):
        _ = settings.lmstudio_token
