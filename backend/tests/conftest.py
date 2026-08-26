from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


TEST_ROOT = Path(tempfile.mkdtemp(prefix="formsight-tests-"))
os.environ["FORMSIGHT_ENVIRONMENT"] = "test"
os.environ["FORMSIGHT_DATA_DIR"] = str(TEST_ROOT)
os.environ["FORMSIGHT_DATABASE_URL"] = f"sqlite:///{(TEST_ROOT / 'test.db').as_posix()}"
os.environ["FORMSIGHT_BOOTSTRAP_ADMIN_EMAIL"] = "admin@test.local"
os.environ["FORMSIGHT_BOOTSTRAP_ADMIN_PASSWORD"] = "A-strong-test-password!"
os.environ["FORMSIGHT_COOKIE_SECURE"] = "false"

from app.main import app  # noqa: E402


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def authenticated(client: TestClient):
    response = client.post(
        "/api/auth/login",
        json={"email": "admin@test.local", "password": "A-strong-test-password!"},
    )
    assert response.status_code == 200, response.text
    return client, response.json()["csrf_token"]
