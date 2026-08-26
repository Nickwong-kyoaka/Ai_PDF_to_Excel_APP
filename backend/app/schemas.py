from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


Role = Literal["admin", "operator", "reviewer"]
ReviewAction = Literal["accept_qwen", "revert_scanner", "edit"]


class APIModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class UserPublic(APIModel):
    id: str
    email: str
    display_name: str
    role: Role
    is_active: bool
    created_at: datetime


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=512)


class LoginResponse(BaseModel):
    user: UserPublic
    csrf_token: str


class UserCreate(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    display_name: str = Field(min_length=1, max_length=120)
    role: Role = "operator"
    password: str = Field(min_length=12, max_length=512)


class UserUpdate(BaseModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=120)
    role: Role | None = None
    password: str | None = Field(default=None, min_length=12, max_length=512)
    is_active: bool | None = None


class ModelProfilePublic(APIModel):
    id: str
    slug: str
    name: str
    extractor_model_id: str
    judge_model_id: str
    quantization: str
    context_length: int
    max_concurrency: int
    image_max_side: int
    verification_mode: str
    approved: bool
    is_default: bool


class ModelProfileCreate(BaseModel):
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{1,79}$")
    name: str = Field(min_length=2, max_length=120)
    extractor_model_id: str
    judge_model_id: str
    quantization: str = "Q4_K_M"
    context_length: int = Field(default=32768, ge=4096, le=1048576)
    max_concurrency: int = Field(default=1, ge=1, le=32)
    image_max_side: int = Field(default=3000, ge=1024, le=8192)
    verification_mode: Literal["fast", "careful", "maximum"] = "maximum"
    approved: bool = True
    is_default: bool = False


class GroupProposal(APIModel):
    id: str
    group_index: int
    start_page: int
    end_page: int
    participant_id: str | None
    confidence: float
    reason: str
    confirmed: bool


class GroupInput(BaseModel):
    start_page: int = Field(ge=1)
    end_page: int = Field(ge=1)
    participant_id: str | None = Field(default=None, max_length=120)

    @field_validator("end_page")
    @classmethod
    def end_after_start(cls, value: int, info) -> int:  # type: ignore[no-untyped-def]
        start = info.data.get("start_page")
        if start and value < start:
            raise ValueError("end_page must be greater than or equal to start_page")
        return value


class ConfirmGroupsRequest(BaseModel):
    groups: list[GroupInput] = Field(min_length=1)


class ArtifactPublic(APIModel):
    id: str
    kind: str
    draft: bool
    filename: str
    created_at: datetime


class JobPublic(APIModel):
    id: str
    filename: str
    media_type: str
    status: str
    page_count: int
    language: str
    groups_confirmed: bool
    progress: float
    stage_message: str
    error: str | None
    draft_artifacts_ready: bool
    created_at: datetime
    updated_at: datetime
    expires_at: datetime
    groups: list[GroupProposal] = []
    artifacts: list[ArtifactPublic] = []


class EvidenceBox(BaseModel):
    page_number: int
    bbox: list[float] = Field(min_length=4, max_length=4)
    source: str
    label: str = ""
    confidence: float = Field(default=0.0, ge=0, le=1)


class ResultV2Answer(APIModel):
    id: str
    page_number: int
    question_id: str
    question_text: str
    section: str
    answer_type: str
    allowed_options: list[Any]
    selected_options: list[Any]
    qwen_value: Any = None
    yolo_value: Any = None
    scanner_value: Any = None
    scanner_confidence: float
    fusion_reason: str
    evidence: list[dict[str, Any]]
    reasonableness_status: str
    judge_suggestion: Any = None
    judge_reason: str
    judge_confidence: float
    rule_refs: list[str]
    final_value: Any = None
    final_source: str
    review_status: str
    reviewer_id: str | None
    reviewed_at: datetime | None
    review_comment: str


class ResultV2(BaseModel):
    schema_version: Literal["2.0"] = "2.0"
    job: JobPublic
    answers: list[ResultV2Answer]
    unresolved_count: int


class ReviewRequest(BaseModel):
    action: ReviewAction
    value: Any = None
    comment: str = Field(default="", max_length=2000)


class RuleCreate(BaseModel):
    name: str = Field(min_length=2, max_length=160)
    form_pattern: str = Field(default="*", max_length=255)
    enabled: bool = True
    severity: Literal["info", "review", "error"] = "review"
    definition: dict[str, Any]


class RulePublic(APIModel):
    id: str
    name: str
    form_pattern: str
    enabled: bool
    severity: str
    definition: dict[str, Any]
    created_by: str
    created_at: datetime
    updated_at: datetime


class AnnotationCreate(BaseModel):
    source_id: str = Field(min_length=1, max_length=255)
    page_number: int = Field(ge=1)
    image_path: str
    mark_class: Literal["tick", "cross", "filled_mark", "circle", "underline_selection", "strikeout"]
    bbox: list[float] = Field(min_length=4, max_length=4)
    split: Literal["train", "val", "test"] = "train"

    @field_validator("bbox")
    @classmethod
    def valid_bbox(cls, value: list[float]) -> list[float]:
        x1, y1, x2, y2 = value
        if not all(0 <= item <= 1 for item in value) or x2 <= x1 or y2 <= y1:
            raise ValueError("bbox must be normalized [x1,y1,x2,y2] with positive area")
        return value


class AnnotationPublic(APIModel):
    id: str
    source_id: str
    page_number: int
    image_path: str
    mark_class: str
    bbox: list[float]
    split: str
    annotator_id: str
    created_at: datetime


class HealthResponse(BaseModel):
    status: str
    database: str
    lmstudio: dict[str, Any]
    yolo: dict[str, Any]
    worker: dict[str, Any]
