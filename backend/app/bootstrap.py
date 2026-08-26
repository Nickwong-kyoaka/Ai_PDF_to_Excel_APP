from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import Settings
from .models import ModelProfile, User
from .security import hash_password, normalize_email


DEFAULT_PROFILE = {
    "slug": "rtx-5060ti-16gb-maximum",
    "name": "RTX 5060 Ti · Maximum accuracy",
    "extractor_model_id": "qwen/qwen3-vl-8b",
    "judge_model_id": "qwen/qwen3-8b",
    "quantization": "Q4_K_M",
    "context_length": 32768,
    "max_concurrency": 1,
    "image_max_side": 3000,
    "verification_mode": "maximum",
    "approved": True,
    "is_default": True,
}


def bootstrap_defaults(db: Session, settings: Settings) -> None:
    if not db.scalar(select(User.id).limit(1)):
        db.add(
            User(
                email=normalize_email(settings.bootstrap_admin_email),
                display_name="FormSight Administrator",
                role="admin",
                password_hash=hash_password(settings.effective_admin_password),
            )
        )
    if not db.scalar(select(ModelProfile.id).limit(1)):
        profile = {
            **DEFAULT_PROFILE,
            "extractor_model_id": settings.extractor_model_id,
            "judge_model_id": settings.judge_model_id,
            "quantization": settings.model_quantization,
        }
        db.add(ModelProfile(**profile))
    db.commit()
