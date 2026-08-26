from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="FORMSIGHT_", extra="ignore"
    )

    app_name: str = "FormSight"
    environment: str = "development"
    data_dir: Path = Path("./data")
    database_url: str = "sqlite:///./data/formsight.db"
    frontend_origins: list[str] | str = ["http://localhost:3000"]
    cookie_secure: bool = False
    session_hours: int = 12
    bootstrap_admin_email: str = "admin@formsight.local"
    bootstrap_admin_password: str = "Change-this-before-LAN-use"
    lmstudio_base_url: str = "http://127.0.0.1:1234/v1"
    lmstudio_api_key: str = ""
    legacy_v14_path: Path = Path(
        "../universal_questionnaire_lmstudio_extractor_v14_consensus_geometry.py"
    )
    yolo_weights: Path = Path("./models/questionnaire_marks.onnx")
    retention_days: int = 30
    max_upload_mb: int = 250
    max_pages: int = 500
    worker_poll_seconds: float = 1.5
    judge_correction_threshold: float = 0.90

    @field_validator("frontend_origins", mode="before")
    @classmethod
    def parse_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def artifacts_dir(self) -> Path:
        return self.data_dir / "artifacts"

    @property
    def pages_dir(self) -> Path:
        return self.data_dir / "pages"

    @property
    def annotations_dir(self) -> Path:
        return self.data_dir / "annotations"

    def ensure_directories(self) -> None:
        for path in (
            self.data_dir,
            self.uploads_dir,
            self.artifacts_dir,
            self.pages_dir,
            self.annotations_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    return Settings()
