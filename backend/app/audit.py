from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from .models import AuditEvent


def record_audit(
    db: Session,
    action: str,
    *,
    actor_id: str | None = None,
    job_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    db.add(
        AuditEvent(
            actor_id=actor_id,
            job_id=job_id,
            action=action,
            metadata_json=metadata or {},
        )
    )
