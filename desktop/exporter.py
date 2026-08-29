from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable

from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.models import Answer, Job, LocalBatch, LocalBatchItem, QuestionnaireGroup, ReviewEvent
from backend.app.scanner.fusion import normalized


SHEETS = (
    "Questionnaires",
    "Long_Answers",
    "Page_Extracts",
    "Conflicts",
    "Failed_Jobs",
    "QA_Summary",
    "Data_Analysis",
    "Run_Log",
    "Reasonableness",
    "Review_Audit",
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _language_for(answers: Iterable[Answer]) -> str:
    text = " ".join(f"{item.question_text} {item.final_value or ''}" for item in answers)
    has_cjk = any("\u3400" <= char <= "\u9fff" for char in text)
    has_latin = any(char.isascii() and char.isalpha() for char in text)
    if has_cjk and has_latin:
        return "Mixed / 中英混合"
    if has_cjk:
        return "Chinese / 中文"
    if has_latin:
        return "English / 英文"
    return "Unknown / 未知"


def _flag_for(answer: Answer) -> str:
    flags: list[str] = []
    if answer.review_status == "pending":
        flags.append("REVIEW_REQUIRED")
    if answer.final_source == "qwen_judge":
        flags.append("QWEN_CORRECTION_PENDING_REVIEW")
    if answer.reasonableness_status not in {None, "reasonable", "not_checked"}:
        flags.append(str(answer.reasonableness_status).upper())
    if (
        answer.verifier_value is not None
        and answer.qwen_value is not None
        and normalized(answer.verifier_value) != normalized(answer.qwen_value)
    ):
        flags.append("PRIMARY_VERIFIER_CONFLICT")
    return "; ".join(dict.fromkeys(flags)) or "OK"


def _write_rows(sheet, headers: list[str], rows: Iterable[dict[str, Any]]) -> int:
    sheet.append(headers)
    header_fill = PatternFill("solid", fgColor="17365D")
    for cell in sheet[1]:
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    count = 0
    for row in rows:
        sheet.append([_cell_value(row.get(header)) for header in headers])
        count += 1

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for index, header in enumerate(headers, 1):
        values = [str(header)]
        for cell in sheet.iter_cols(min_col=index, max_col=index, min_row=2, max_row=min(sheet.max_row, 202)):
            values.extend("" if item.value is None else str(item.value) for item in cell)
        width = min(55, max(10, max(len(value) for value in values) + 2))
        sheet.column_dimensions[get_column_letter(index)].width = width
    return count


def _cell_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _answer_row(
    *,
    item: LocalBatchItem,
    job: Job,
    group: QuestionnaireGroup,
    answer: Answer,
    language: str,
) -> dict[str, Any]:
    evidence = answer.evidence or []
    primary_evidence = [
        entry
        for entry in evidence
        if entry.get("source") in {"qwen", "primary_vision", "primary_adjudicator"}
    ]
    verifier_evidence = [entry for entry in evidence if entry.get("source") == "verifier_vision"]
    return {
        "Source_File": Path(item.original_path).name,
        "Source_File_Index": item.order_index + 1,
        "Questionnaire_Index": group.group_index + 1,
        "Page_Range": f"{group.start_page}-{group.end_page}",
        "Participant_ID": group.participant_id,
        "Detected_Language": language,
        "Answer_ID": answer.id,
        "Question_ID": answer.question_id,
        "Question_Text": answer.question_text,
        "Answer_Type": answer.answer_type,
        "Allowed_Options": answer.allowed_options,
        "Selected_Options": answer.selected_options,
        "Primary_Model_ID": (job.profile_snapshot or {}).get("extractor_model_id"),
        "Verifier_Model_ID": answer.verifier_model_id
        or (job.profile_snapshot or {}).get("verifier_model_id"),
        "Primary_Model_Value": answer.qwen_value,
        "Verifier_Model_Value": answer.verifier_value,
        "Primary_Model_Evidence": primary_evidence,
        "Verifier_Model_Evidence": verifier_evidence,
        "Scanner_Value_Immutable": answer.scanner_value,
        "Scanner_Confidence": answer.scanner_confidence,
        "Fusion_Reason": answer.fusion_reason,
        "Reasonableness_Status": answer.reasonableness_status,
        "Judge_Suggestion": answer.judge_suggestion,
        "Judge_Reason": answer.judge_reason,
        "Rule_References": answer.rule_refs,
        "Final_Value": answer.final_value,
        "Final_Source": answer.final_source,
        "Review_Status": answer.review_status,
        "Flag_Status": _flag_for(answer),
        "Source_Page": answer.page_number,
        "Updated_At": answer.updated_at,
    }


def write_source_excel(
    db: Session,
    batch: LocalBatch,
    item: LocalBatchItem,
    destination: str | Path,
) -> dict[str, Any]:
    """Write the workbook corresponding to one selected PDF or image."""

    destination = Path(destination).expanduser().resolve()
    if destination.suffix.lower() != ".xlsx":
        destination = destination.with_suffix(".xlsx")
    destination.parent.mkdir(parents=True, exist_ok=True)

    items = [item]
    item_by_job = {item.job_id: item for item in items if item.job_id}
    jobs = list(db.scalars(select(Job).where(Job.id.in_(list(item_by_job)))).all()) if item_by_job else []
    jobs.sort(key=lambda job: item_by_job[job.id].order_index)

    questionnaire_rows: list[dict[str, Any]] = []
    answer_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    conflict_rows: list[dict[str, Any]] = []
    reason_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    source_stats: Counter[str] = Counter()

    for job in jobs:
        item = item_by_job[job.id]
        groups = list(
            db.scalars(
                select(QuestionnaireGroup)
                .where(QuestionnaireGroup.job_id == job.id)
                .order_by(QuestionnaireGroup.group_index.asc())
            ).all()
        )
        for group in groups:
            answers = list(
                db.scalars(
                    select(Answer)
                    .where(Answer.group_id == group.id)
                    .order_by(Answer.page_number.asc(), Answer.question_id.asc())
                ).all()
            )
            language = _language_for(answers)
            flags = sum(_flag_for(answer) != "OK" for answer in answers)
            source_name = Path(item.original_path).name
            source_stats[source_name] += len(answers)
            questionnaire_rows.append(
                {
                    "Source_File": source_name,
                    "Source_File_Index": item.order_index + 1,
                    "Questionnaire_Index": group.group_index + 1,
                    "Page_Range": f"{group.start_page}-{group.end_page}",
                    "Participant_ID": group.participant_id,
                    "Detected_Language": language,
                    "Answers": len(answers),
                    "Flagged_Answers": flags,
                    "Job_Status": job.status,
                    "Model_Profile": job.profile_snapshot,
                }
            )
            for answer in answers:
                row = _answer_row(item=item, job=job, group=group, answer=answer, language=language)
                answer_rows.append(row)
                page_rows.append(
                    {
                        key: row[key]
                        for key in (
                            "Source_File",
                            "Source_File_Index",
                            "Questionnaire_Index",
                            "Page_Range",
                            "Participant_ID",
                            "Detected_Language",
                            "Source_Page",
                            "Question_ID",
                            "Question_Text",
                            "Primary_Model_Evidence",
                            "Verifier_Model_Evidence",
                            "Scanner_Value_Immutable",
                            "Final_Value",
                            "Flag_Status",
                        )
                    }
                )
                if row["Flag_Status"] != "OK":
                    conflict_rows.append(row)
                reason_rows.append(
                    {
                        key: row[key]
                        for key in (
                            "Source_File",
                            "Source_File_Index",
                            "Questionnaire_Index",
                            "Participant_ID",
                            "Question_ID",
                            "Question_Text",
                            "Scanner_Value_Immutable",
                            "Reasonableness_Status",
                            "Judge_Suggestion",
                            "Judge_Reason",
                            "Rule_References",
                            "Final_Value",
                            "Final_Source",
                            "Flag_Status",
                        )
                    }
                )

            answer_by_id = {answer.id: answer for answer in answers}
            events = (
                list(
                    db.scalars(
                        select(ReviewEvent)
                        .where(ReviewEvent.answer_id.in_(list(answer_by_id)))
                        .order_by(ReviewEvent.created_at.asc())
                    ).all()
                )
                if answer_by_id
                else []
            )
            for event in events:
                answer = answer_by_id.get(event.answer_id)
                audit_rows.append(
                    {
                        "Source_File": source_name,
                        "Source_File_Index": item.order_index + 1,
                        "Questionnaire_Index": group.group_index + 1,
                        "Participant_ID": group.participant_id,
                        "Question_ID": answer.question_id if answer else None,
                        "Action": event.action,
                        "Old_Value": event.previous_value,
                        "New_Value": event.new_value,
                        "Comment": event.comment,
                        "Created_At": event.created_at,
                    }
                )

    failed_rows = [
        {
            "Source_File": Path(item.original_path).name,
            "Source_File_Index": item.order_index + 1,
            "Status": item.status,
            "Error": item.error,
            "Updated_At": item.updated_at,
        }
        for item in items
        if item.status == "failed"
    ]

    unresolved = len(conflict_rows)
    status_label = "COMPLETED — FLAGS PRESENT" if unresolved or failed_rows else "COMPLETED"
    qa_rows = [
        {"Metric": "Workbook_Status", "Value": status_label},
        {"Metric": "Source_Files", "Value": len(items)},
        {"Metric": "Questionnaires", "Value": len(questionnaire_rows)},
        {"Metric": "Answers", "Value": len(answer_rows)},
        {"Metric": "Flagged_Answers", "Value": unresolved},
        {"Metric": "Failed_Inputs", "Value": len(failed_rows)},
        {
            "Metric": "Qwen_Corrections_Pending_Review",
            "Value": sum(row["Final_Source"] == "qwen_judge" for row in answer_rows),
        },
        {"Metric": "Batch_ID", "Value": batch.id},
        {"Metric": "Generated_At_UTC", "Value": _utcnow().isoformat()},
    ]
    analysis_rows = [
        {"Source_File": name, "Answer_Count": count}
        for name, count in sorted(source_stats.items(), key=lambda pair: pair[0].casefold())
    ]
    run_rows = [
        {
            "Batch_ID": batch.id,
            "Source_File": Path(item.original_path).name,
            "Source_File_Index": item.order_index + 1,
            "Status": item.status,
            "Error": item.error,
            "Started_At": item.started_at,
            "Finished_At": item.finished_at,
            "Vision_Model": batch.extractor_model_id,
            "Verifier_Model": batch.verifier_model_id,
            "Judge_Model": batch.judge_model_id,
            "LM_Studio": batch.lmstudio_base_url,
        }
        for item in items
    ]

    wb = Workbook()
    wb.remove(wb.active)
    for name in SHEETS:
        wb.create_sheet(name)
    wb.properties.title = f"FormSight Local — {status_label}"
    wb.properties.subject = f"Questionnaire extraction for {Path(item.original_path).name}"
    wb.properties.creator = "FormSight Local"

    questionnaire_headers = [
        "Source_File",
        "Source_File_Index",
        "Questionnaire_Index",
        "Page_Range",
        "Participant_ID",
        "Detected_Language",
        "Answers",
        "Flagged_Answers",
        "Job_Status",
        "Model_Profile",
    ]
    answer_headers = [
        "Source_File",
        "Source_File_Index",
        "Questionnaire_Index",
        "Page_Range",
        "Participant_ID",
        "Detected_Language",
        "Answer_ID",
        "Question_ID",
        "Question_Text",
        "Answer_Type",
        "Allowed_Options",
        "Selected_Options",
        "Primary_Model_ID",
        "Verifier_Model_ID",
        "Primary_Model_Value",
        "Verifier_Model_Value",
        "Primary_Model_Evidence",
        "Verifier_Model_Evidence",
        "Scanner_Value_Immutable",
        "Scanner_Confidence",
        "Fusion_Reason",
        "Reasonableness_Status",
        "Judge_Suggestion",
        "Judge_Reason",
        "Rule_References",
        "Final_Value",
        "Final_Source",
        "Review_Status",
        "Flag_Status",
        "Source_Page",
        "Updated_At",
    ]
    page_headers = [
        "Source_File",
        "Source_File_Index",
        "Questionnaire_Index",
        "Page_Range",
        "Participant_ID",
        "Detected_Language",
        "Source_Page",
        "Question_ID",
        "Question_Text",
        "Primary_Model_Evidence",
        "Verifier_Model_Evidence",
        "Scanner_Value_Immutable",
        "Final_Value",
        "Flag_Status",
    ]
    reason_headers = [
        "Source_File",
        "Source_File_Index",
        "Questionnaire_Index",
        "Participant_ID",
        "Question_ID",
        "Question_Text",
        "Scanner_Value_Immutable",
        "Reasonableness_Status",
        "Judge_Suggestion",
        "Judge_Reason",
        "Rule_References",
        "Final_Value",
        "Final_Source",
        "Flag_Status",
    ]
    _write_rows(wb["Questionnaires"], questionnaire_headers, questionnaire_rows)
    _write_rows(wb["Long_Answers"], answer_headers, answer_rows)
    _write_rows(wb["Page_Extracts"], page_headers, page_rows)
    _write_rows(wb["Conflicts"], answer_headers, conflict_rows)
    _write_rows(wb["Failed_Jobs"], ["Source_File", "Source_File_Index", "Status", "Error", "Updated_At"], failed_rows)
    _write_rows(wb["QA_Summary"], ["Metric", "Value"], qa_rows)
    _write_rows(wb["Data_Analysis"], ["Source_File", "Answer_Count"], analysis_rows)
    _write_rows(
        wb["Run_Log"],
        [
            "Batch_ID",
            "Source_File",
            "Source_File_Index",
            "Status",
            "Error",
            "Started_At",
            "Finished_At",
            "Vision_Model",
            "Verifier_Model",
            "Judge_Model",
            "LM_Studio",
        ],
        run_rows,
    )
    _write_rows(wb["Reasonableness"], reason_headers, reason_rows)
    _write_rows(
        wb["Review_Audit"],
        [
            "Source_File",
            "Source_File_Index",
            "Questionnaire_Index",
            "Participant_ID",
            "Question_ID",
            "Action",
            "Old_Value",
            "New_Value",
            "Comment",
            "Created_At",
        ],
        audit_rows,
    )

    if analysis_rows:
        chart = BarChart()
        chart.title = "Answers by source file"
        chart.y_axis.title = "Answers"
        chart.x_axis.title = "Source file"
        data = Reference(wb["Data_Analysis"], min_col=2, min_row=1, max_row=len(analysis_rows) + 1)
        labels = Reference(wb["Data_Analysis"], min_col=1, min_row=2, max_row=len(analysis_rows) + 1)
        chart.add_data(data, titles_from_data=True)
        chart.set_categories(labels)
        chart.height = 7
        chart.width = 14
        wb["Data_Analysis"].add_chart(chart, "D2")

    temp_path = destination.with_name(f".{destination.stem}.writing.xlsx")
    wb.save(temp_path)
    temp_path.replace(destination)
    return {
        "path": str(destination),
        "status": status_label,
        "source_files": len(items),
        "questionnaires": len(questionnaire_rows),
        "answers": len(answer_rows),
        "flags": unresolved,
        "failed": len(failed_rows),
    }
