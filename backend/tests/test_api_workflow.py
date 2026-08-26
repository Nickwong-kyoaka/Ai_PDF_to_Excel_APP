from __future__ import annotations

import io

from fastapi.testclient import TestClient
from PIL import Image, ImageDraw

from app.database import SessionLocal
from app.models import Answer, Job, QuestionnaireGroup


def sample_png() -> bytes:
    image = Image.new("RGB", (900, 1200), "white")
    draw = ImageDraw.Draw(image)
    draw.text((80, 80), "Question 1: Yes [x] No [ ]", fill="black")
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def create_confirmed_job(client: TestClient, csrf: str) -> dict:
    profiles = client.get("/api/model-profiles").json()
    response = client.post(
        "/api/jobs",
        headers={"X-CSRF-Token": csrf},
        data={"profile_id": profiles[0]["id"], "language": "auto"},
        files={"file": ("questionnaire.png", sample_png(), "image/png")},
    )
    assert response.status_code == 201, response.text
    job = response.json()
    assert job["status"] == "awaiting_confirmation"
    response = client.post(
        f"/api/jobs/{job['id']}/groups/confirm",
        headers={"X-CSRF-Token": csrf},
        json={"groups": [{"start_page": 1, "end_page": 1, "participant_id": "TEST001"}]},
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_health_and_authentication(client: TestClient):
    assert client.get("/api/health").json()["status"] == "ok"
    assert client.get("/api/jobs").status_code == 401
    login = client.post(
        "/api/auth/login",
        json={"email": "admin@test.local", "password": "A-strong-test-password!"},
    )
    assert login.status_code == 200
    assert login.json()["user"]["role"] == "admin"
    assert client.get("/api/auth/me").status_code == 200


def test_upload_group_review_and_final_export(authenticated):
    client, csrf = authenticated
    job_data = create_confirmed_job(client, csrf)
    assert job_data["status"] == "queued"
    with SessionLocal() as db:
        job = db.get(Job, job_data["id"])
        group = db.query(QuestionnaireGroup).filter_by(job_id=job.id).one()
        answer = Answer(
            job_id=job.id,
            group_id=group.id,
            page_number=1,
            question_id="Q1",
            question_text="Question 1",
            answer_type="yes_no",
            allowed_options=[{"label": "Yes"}, {"label": "No"}],
            selected_options=["Yes"],
            qwen_value="Yes",
            yolo_value="Yes",
            scanner_value="Yes",
            scanner_confidence=0.99,
            fusion_reason="Qwen and YOLO agree",
            reasonableness_status="corrected",
            judge_suggestion="No",
            judge_reason="Qwen corrected — pending human review.",
            judge_confidence=0.93,
            final_value="No",
            final_source="qwen_judge",
            review_status="pending",
        )
        db.add(answer)
        job.status = "review_needed"
        db.commit()
        answer_id = answer.id

    result = client.get(f"/api/jobs/{job_data['id']}/result")
    assert result.status_code == 200
    assert result.json()["schema_version"] == "2.0"
    assert result.json()["answers"][0]["scanner_value"] == "Yes"

    reviewed = client.post(
        f"/api/jobs/{job_data['id']}/answers/{answer_id}/review",
        headers={"X-CSRF-Token": csrf},
        json={"action": "revert_scanner", "comment": "Verified against the page"},
    )
    assert reviewed.status_code == 200, reviewed.text
    final = client.post(
        f"/api/jobs/{job_data['id']}/finalize",
        headers={"X-CSRF-Token": csrf},
    )
    assert final.status_code == 200, final.text
    assert final.json()["status"] == "finalized"
    assert {artifact["kind"] for artifact in final.json()["artifacts"]} == {"json", "excel", "annotated_pdf"}


def test_csrf_blocks_upload(authenticated):
    client, _csrf = authenticated
    response = client.post(
        "/api/jobs",
        files={"file": ("questionnaire.png", sample_png(), "image/png")},
    )
    assert response.status_code == 403
