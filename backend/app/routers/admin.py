from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from ..audit import record_audit
from ..config import get_settings
from ..database import get_db
from ..models import Annotation, ModelProfile, Rule, User, YoloModel
from ..retention import purge_expired
from ..schemas import (
    AnnotationCreate,
    AnnotationPublic,
    ModelProfileCreate,
    ModelProfilePublic,
    RuleCreate,
    RulePublic,
    UserCreate,
    UserPublic,
    UserUpdate,
)
from ..security import get_current_user, hash_password, normalize_email, require_roles


router = APIRouter(tags=["administration"])
settings = get_settings()


@router.get("/model-profiles", response_model=list[ModelProfilePublic])
def list_profiles(
    db: Annotated[Session, Depends(get_db)],
    _user=Depends(get_current_user),
):
    return db.scalars(
        select(ModelProfile).where(ModelProfile.approved.is_(True)).order_by(ModelProfile.is_default.desc(), ModelProfile.name)
    ).all()


@router.post("/admin/model-profiles", response_model=ModelProfilePublic, status_code=201)
def create_profile(
    payload: ModelProfileCreate,
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    if db.scalar(select(ModelProfile.id).where(ModelProfile.slug == payload.slug)):
        raise HTTPException(status_code=409, detail="Profile slug already exists")
    if payload.is_default:
        db.execute(update(ModelProfile).values(is_default=False))
    profile = ModelProfile(**payload.model_dump())
    db.add(profile)
    record_audit(db, "model_profile.created", actor_id=admin.id, metadata={"slug": profile.slug})
    db.commit()
    db.refresh(profile)
    return profile


@router.get("/admin/users", response_model=list[UserPublic])
def list_users(
    db: Annotated[Session, Depends(get_db)],
    _admin=Depends(require_roles("admin")),
):
    return db.scalars(select(User).order_by(User.created_at)).all()


@router.post("/admin/users", response_model=UserPublic, status_code=201)
def create_user(
    payload: UserCreate,
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    email = normalize_email(payload.email)
    if db.scalar(select(User.id).where(User.email == email)):
        raise HTTPException(status_code=409, detail="Email already exists")
    user = User(
        email=email,
        display_name=payload.display_name,
        role=payload.role,
        password_hash=hash_password(payload.password),
    )
    db.add(user)
    record_audit(db, "user.created", actor_id=admin.id, metadata={"user_id": user.id, "role": user.role})
    db.commit()
    db.refresh(user)
    return user


@router.patch("/admin/users/{user_id}", response_model=UserPublic)
def update_user(
    user_id: str,
    payload: UserUpdate,
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.id == admin.id and payload.is_active is False:
        raise HTTPException(status_code=409, detail="You cannot disable your own account")
    changes = payload.model_dump(exclude_unset=True)
    password = changes.pop("password", None)
    for key, value in changes.items():
        setattr(user, key, value)
    if password:
        user.password_hash = hash_password(password)
    record_audit(db, "user.updated", actor_id=admin.id, metadata={"user_id": user.id})
    db.commit()
    return user


@router.get("/rules", response_model=list[RulePublic])
def list_rules(
    db: Annotated[Session, Depends(get_db)],
    _user=Depends(get_current_user),
):
    return db.scalars(select(Rule).order_by(Rule.created_at.desc())).all()


@router.post("/admin/rules", response_model=RulePublic, status_code=201)
def create_rule(
    payload: RuleCreate,
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    allowed_operators = {"range", "allowed", "regex", "required_if", "equals_field", "date_after_field"}
    if payload.definition.get("operator") not in allowed_operators:
        raise HTTPException(status_code=422, detail=f"operator must be one of {sorted(allowed_operators)}")
    rule = Rule(**payload.model_dump(), created_by=admin.id)
    db.add(rule)
    record_audit(db, "rule.created", actor_id=admin.id, metadata={"rule_id": rule.id})
    db.commit()
    db.refresh(rule)
    return rule


@router.delete("/admin/rules/{rule_id}", status_code=204)
def delete_rule(
    rule_id: str,
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    rule = db.get(Rule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    db.delete(rule)
    record_audit(db, "rule.deleted", actor_id=admin.id, metadata={"rule_id": rule_id})
    db.commit()


@router.get("/admin/annotations", response_model=list[AnnotationPublic])
def list_annotations(
    db: Annotated[Session, Depends(get_db)],
    _admin=Depends(require_roles("admin")),
    source_id: str | None = None,
):
    statement = select(Annotation).order_by(Annotation.source_id, Annotation.page_number)
    if source_id:
        statement = statement.where(Annotation.source_id == source_id)
    return db.scalars(statement).all()


@router.post("/admin/annotations", response_model=AnnotationPublic, status_code=201)
def create_annotation(
    payload: AnnotationCreate,
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    image_path = settings.pages_dir / payload.source_id / f"page-{payload.page_number:04d}.jpg"
    if not image_path.exists():
        raise HTTPException(status_code=422, detail="Annotation source page does not exist")
    annotation_data = payload.model_dump(exclude={"image_path"})
    annotation = Annotation(**annotation_data, image_path=str(image_path.resolve()), annotator_id=admin.id)
    db.add(annotation)
    record_audit(db, "annotation.created", actor_id=admin.id, metadata={"class": annotation.mark_class})
    db.commit()
    db.refresh(annotation)
    return annotation


@router.delete("/admin/annotations/{annotation_id}", status_code=204)
def delete_annotation(
    annotation_id: str,
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    annotation = db.get(Annotation, annotation_id)
    if not annotation:
        raise HTTPException(status_code=404, detail="Annotation not found")
    db.delete(annotation)
    record_audit(db, "annotation.deleted", actor_id=admin.id, metadata={"annotation_id": annotation_id})
    db.commit()


@router.get("/admin/yolo-models")
def list_yolo_models(
    db: Annotated[Session, Depends(get_db)],
    _admin=Depends(require_roles("admin")),
):
    return db.scalars(select(YoloModel).order_by(YoloModel.created_at.desc())).all()


@router.post("/admin/yolo-models", status_code=201)
def register_yolo_model(
    payload: dict[str, Any],
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    version = str(payload.get("version", "")).strip()
    weights_path = Path(str(payload.get("weights_path", "")))
    if not version or not weights_path.exists():
        raise HTTPException(status_code=422, detail="A unique version and existing weights_path are required")
    if db.scalar(select(YoloModel.id).where(YoloModel.version == version)):
        raise HTTPException(status_code=409, detail="YOLO version already exists")
    active = bool(payload.get("active"))
    if active:
        db.execute(update(YoloModel).values(active=False))
    model = YoloModel(
        version=version,
        weights_path=str(weights_path.resolve()),
        metrics=payload.get("metrics") or {},
        classes=payload.get("classes") or [],
        active=active,
    )
    db.add(model)
    record_audit(db, "yolo_model.registered", actor_id=admin.id, metadata={"version": version})
    db.commit()
    db.refresh(model)
    return model


@router.post("/admin/retention/run")
def run_retention(
    db: Annotated[Session, Depends(get_db)],
    admin=Depends(require_roles("admin")),
):
    result = purge_expired(db, settings)
    record_audit(db, "retention.manual_run", actor_id=admin.id, metadata=result)
    db.commit()
    return result
