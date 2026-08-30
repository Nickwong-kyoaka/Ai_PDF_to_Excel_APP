from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Answer, Base, Job, ModelProfile, QuestionnaireGroup, User
from app.scanner.extractor import QuestionnaireExtractor


def test_reasonableness_suggestions_never_replace_scanner_values(tmp_path):
    engine = create_engine(f"sqlite:///{(tmp_path / 'flag-only.db').as_posix()}")
    Base.metadata.create_all(engine)
    settings = Settings(
        data_dir=tmp_path / "data",
        legacy_v14_path=tmp_path / "missing.py",
        yolo_weights=tmp_path / "missing.onnx",
    )
    settings.ensure_directories()

    with Session(engine, expire_on_commit=False) as db:
        user = User(
            email="judge@test.local",
            display_name="Judge Test",
            role="operator",
            password_hash="unused",
        )
        profile = ModelProfile(
            slug="judge",
            name="Judge",
            extractor_model_id="qwen-vl",
            judge_model_id="qwen-vl",
        )
        db.add_all([user, profile])
        db.flush()
        job = Job(
            owner_id=user.id,
            profile_id=profile.id,
            filename="questionnaire.pdf",
            stored_path=str(tmp_path / "questionnaire.pdf"),
            media_type="application/pdf",
            sha256="0" * 64,
            status="judging",
            page_count=6,
            groups_confirmed=True,
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
        db.add(job)
        db.flush()
        group = QuestionnaireGroup(
            job_id=job.id,
            group_index=0,
            start_page=1,
            end_page=6,
            confirmed=True,
        )
        db.add(group)
        db.flush()
        originals = [("A:Q1", "臺北"), ("A:Q2", "沒有"), ("A:P6:Q8", 0)]
        for index, (answer_key, value) in enumerate(originals, start=1):
            db.add(
                Answer(
                    answer_key=answer_key,
                    job_id=job.id,
                    group_id=group.id,
                    page_number=index,
                    page_ordinal=index,
                    question_id="Q8",
                    template_question_id=f"P{index}:Q8",
                    question_text=f"Question {index}",
                    answer_type="short_text",
                    scanner_value=value,
                    final_value="old incorrect value",
                    final_source="qwen_judge",
                )
            )
        db.commit()

        extractor = QuestionnaireExtractor(
            settings,
            {
                "extractor_model_id": "qwen-vl",
                "judge_model_id": "qwen-vl",
                "judge_retries": 0,
            },
            manage_models=False,
        )

        class Gateway:
            def chat_json(self, **_kwargs):
                return {
                    "results": [
                        {
                            "answer_key": answer_key,
                            "status": "corrected",
                            "suggestion": 1,
                            "confidence": 0.99,
                            "reason": "numeric normalization",
                        }
                        for answer_key, _value in originals
                    ]
                }

        extractor.gateway = Gateway()
        extractor.judge_job(db, job)

        answers = list(db.scalars(select(Answer).order_by(Answer.page_number)).all())
        assert [answer.final_value for answer in answers] == ["臺北", "沒有", 0]
        assert all(answer.final_source == "scanner" for answer in answers)
        assert all(answer.judge_suggestion == 1 for answer in answers)
        assert all(answer.review_status == "pending" for answer in answers)
        assert all(answer.reasonableness_status == "review_required" for answer in answers)
