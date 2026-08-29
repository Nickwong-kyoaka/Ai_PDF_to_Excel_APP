from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QInputDialog, QMessageBox
from PIL import Image

from desktop.runner import GroupDraft
from desktop.ui import GroupReviewDialog, MainWindow


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


def test_bulk_series_label_updates_output_preview(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(QInputDialog, "getText", lambda *args, **kwargs: ("Series Alpha", True))
    app = QApplication.instance() or QApplication([])
    first = tmp_path / "part-one.png"
    second = tmp_path / "part-two.jpg"
    Image.new("RGB", (20, 20), "white").save(first)
    Image.new("RGB", (20, 20), "white").save(second)
    window = MainWindow(auto_detect=False)
    window._add_paths([first, second])
    window.file_table.selectAll()

    window.set_selected_series_label()

    assert window.series_labels == ["Series Alpha", "Series Alpha"]
    assert window.file_table.item(0, 4).text() == "Series Alpha_FormSight.xlsx"
    assert window.file_table.item(1, 4).text() == "Series Alpha_FormSight.xlsx"
    assert "1 labelled series workbook" in window.file_summary_label.text()
    window.close()
    app.processEvents()
