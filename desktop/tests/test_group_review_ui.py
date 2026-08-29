from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QMessageBox

from desktop.runner import GroupDraft
from desktop.ui import GroupReviewDialog


def draft(job_id: str, source: str, page_count: int) -> GroupDraft:
    return GroupDraft(
        job_id=job_id,
        source_file=source,
        page_count=page_count,
        group_index=0,
        start_page=1,
        end_page=page_count,
        participant_id=None,
        confidence=0.82,
        reason="Automatic document boundary",
    )


def test_series_preset_copies_to_same_length_files_and_fills_ids(monkeypatch) -> None:
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.StandardButton.Ok)
    app = QApplication.instance() or QApplication([])
    dialog = GroupReviewDialog([draft("a", "first.pdf", 6), draft("b", "second.pdf", 6)])

    dialog._apply_fixed_size(2)
    dialog.copy_pattern()
    dialog.id_prefix.setText("CSA-")
    dialog.id_start.setValue(4)
    dialog.fill_participant_ids()
    result = dialog.result_groups()

    assert [(group.start_page, group.end_page) for group in result["a"]] == [(1, 2), (3, 4), (5, 6)]
    assert [group.participant_id for group in result["a"]] == ["CSA-004", "CSA-005", "CSA-006"]
    assert [(group.start_page, group.end_page) for group in result["b"]] == [(1, 2), (3, 4), (5, 6)]
    assert all(group.participant_id is None for group in result["b"])
    assert "Complete page coverage" in dialog.validation_label.text()
    dialog.close()
    app.processEvents()
