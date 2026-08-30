from __future__ import annotations

import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from PySide6.QtCore import QThread, Qt, QTimer, Signal
from PySide6.QtGui import QColor, QDesktopServices
from PySide6.QtCore import QUrl
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from backend.app.documents import ProposedGroup, validate_group_partition
from . import __version__
from .group_series import build_fixed_size_series, clone_page_pattern, numbered_participant_ids
from .model_discovery import DiscoveryResult, discover_models
from .runner import (
    ALLOWED_SUFFIXES,
    GroupDraft,
    LocalBatchRunner,
    RunnerEvent,
    normalize_series_label,
    series_workbook_filename,
)
from .runtime import DesktopRuntime, create_runtime


TEXT = {
    "en": {
        "title": "FormSight Local",
        "subtitle": "Batch PDFs → one consolidated Excel workbook per series label",
        "step_models": "1  MODELS",
        "step_files": "2  FILES",
        "step_groups": "3  GROUP & OUTPUT",
        "step_scan": "4  SCAN",
        "language": "介面語言",
        "lm": "LM Studio",
        "yolo": "Sequential consensus",
        "refresh": "Refresh detection",
        "add_files": "Add Files",
        "add_folder": "Add Folder",
        "remove": "Remove",
        "clear": "Clear",
        "up": "Move Up",
        "down": "Move Down",
        "set_label": "Set Series Label",
        "files": "Input questionnaires",
        "output": "Output folder",
        "browse": "Browse…",
        "run_mode": "Run mode",
        "auto_one_take": "Automatic one-take — no pauses (recommended)",
        "review_first": "Review page groups before scanning",
        "performance": "Processing profile",
        "balanced": "Balanced — selective verifier (recommended)",
        "maximum": "Maximum accuracy — verify every page",
        "vision": "Primary vision model + judge",
        "judge": "Independent non-Qwen verifier",
        "start": "Start Scan",
        "cancel": "Cancel",
        "resume": "Resume Last Batch",
        "open": "Open Output Folder",
        "ready": "Ready",
        "not_ready": "Not ready",
        "qwen_only": "Load both the primary and verifier models in LM Studio",
        "yolo_ready": "Dual-model pipeline ready",
        "source": "Source file",
        "type": "Type",
        "path": "Location",
        "status": "Status",
        "workbook": "Output workbook",
        "series_label": "Series label",
        "drop_hint": "Drop files or folders anywhere in this window",
        "file_summary_empty": "No questionnaires added yet",
        "file_summary": "{count} input file(s) → {series} labelled series workbook(s)",
        "output_hint": "Automatic one-take continues from grouping through Excel without another click. PDFs with the same series label are combined into one workbook.",
        "label_help": "Select one or more rows, then assign the same label to combine them.",
        "log": "Run log",
        "select_files": "Select questionnaires",
        "select_folder": "Select a folder",
        "select_output": "Choose the folder for the Excel workbooks",
        "need_files": "Add at least one questionnaire file.",
        "need_output": "Choose an output folder.",
        "completed": "The labelled series Excel workbooks are ready.",
    },
    "zh": {
        "title": "FormSight 本機版",
        "subtitle": "批量 PDF → 每個系列標籤合併輸出一個 Excel 活頁簿",
        "step_models": "1  模型",
        "step_files": "2  檔案",
        "step_groups": "3  分組及輸出",
        "step_scan": "4  掃描",
        "language": "Interface language",
        "lm": "LM Studio",
        "yolo": "順序式雙模型共識",
        "refresh": "重新偵測",
        "add_files": "加入檔案",
        "add_folder": "加入資料夾",
        "remove": "移除",
        "clear": "清除",
        "up": "上移",
        "down": "下移",
        "set_label": "設定系列標籤",
        "files": "輸入問卷",
        "output": "輸出資料夾",
        "browse": "瀏覽…",
        "run_mode": "執行模式",
        "auto_one_take": "全自動一次完成 — 中途不停頓（建議）",
        "review_first": "掃描前檢查頁面分組",
        "performance": "處理模式",
        "balanced": "平衡模式 — 只驗證可疑頁面（建議）",
        "maximum": "最高準確度 — 每頁雙模型驗證",
        "vision": "主要視覺模型兼合理性判斷",
        "judge": "獨立非 Qwen 驗證模型",
        "start": "開始掃描",
        "cancel": "取消",
        "resume": "恢復上次批次",
        "open": "開啟輸出資料夾",
        "ready": "準備完成",
        "not_ready": "尚未準備",
        "qwen_only": "請在 LM Studio 同時載入主要及驗證模型",
        "yolo_ready": "雙模型流程準備完成",
        "source": "來源檔案",
        "type": "類型",
        "path": "位置",
        "status": "狀態",
        "workbook": "輸出活頁簿",
        "series_label": "系列標籤",
        "drop_hint": "可將檔案或資料夾拖放到此視窗任何位置",
        "file_summary_empty": "尚未加入問卷",
        "file_summary": "{count} 個輸入檔案 → {series} 個系列 Excel 活頁簿",
        "output_hint": "全自動模式按開始後會由分組一直執行至 Excel，中途毋須再按鍵；相同系列標籤的 PDF 會合併至同一活頁簿。",
        "label_help": "選取一列或多列，再設定相同標籤即可合併輸出。",
        "log": "執行記錄",
        "select_files": "選擇問卷",
        "select_folder": "選擇資料夾",
        "select_output": "選擇 Excel 活頁簿輸出資料夾",
        "need_files": "請加入至少一個問卷檔案。",
        "need_output": "請選擇輸出資料夾。",
        "completed": "各系列標籤對應的 Excel 活頁簿已完成。",
    },
}


class DiscoveryThread(QThread):
    result_ready = Signal(object)

    def run(self) -> None:
        self.result_ready.emit(discover_models())


class BatchThread(QThread):
    event = Signal(object)
    prepared = Signal(str, object)
    succeeded = Signal(object)
    failed = Signal(str)

    def __init__(self, operation: Callable[[LocalBatchRunner], Any], runtime: DesktopRuntime):
        super().__init__()
        self.runner = LocalBatchRunner(runtime, self.event.emit)
        self.operation = operation

    def run(self) -> None:
        try:
            value = self.operation(self.runner)
            if isinstance(value, tuple) and len(value) == 2 and value[0] == "prepared":
                batch_id = str(value[1])
                self.prepared.emit(batch_id, self.runner.group_drafts(batch_id))
            else:
                self.succeeded.emit(value)
        except Exception as exc:
            details = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            self.failed.emit(details)

    def cancel(self) -> None:
        self.runner.request_cancel()


class GroupReviewDialog(QDialog):
    def __init__(self, drafts: list[GroupDraft], parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Questionnaire series / 問卷系列分組")
        self.resize(1120, 710)
        self._active_job_id: str | None = None
        self._job_order: list[str] = []
        self._source_names: dict[str, str] = {}
        self._page_counts: dict[str, int] = {}
        self._groups: dict[str, list[ProposedGroup]] = defaultdict(list)
        for draft in drafts:
            if draft.job_id not in self._source_names:
                self._job_order.append(draft.job_id)
                self._source_names[draft.job_id] = draft.source_file
                self._page_counts[draft.job_id] = draft.page_count
            self._groups[draft.job_id].append(
                ProposedGroup(
                    start_page=draft.start_page,
                    end_page=draft.end_page,
                    participant_id=draft.participant_id,
                    confidence=draft.confidence,
                    reason=draft.reason,
                )
            )

        layout = QVBoxLayout(self)
        heading = QLabel("Confirm questionnaire series / 確認問卷系列")
        heading.setObjectName("dialogTitle")
        layout.addWidget(heading)
        note = QLabel(
            "Work through one source at a time. Every page must be covered exactly once. "
            "Use a preset for regular batches, then adjust ranges or IDs if needed.\n"
            "逐一檢查每個來源；每頁必須恰好出現一次。規則批次可先套用快速分組，再按需要調整頁碼或編號。"
        )
        note.setWordWrap(True)
        layout.addWidget(note)

        navigator = QFrame()
        navigator.setObjectName("panel")
        navigator_layout = QHBoxLayout(navigator)
        self.previous_button = QPushButton("‹ Previous / 上一個")
        self.file_combo = QComboBox()
        self.next_button = QPushButton("Next / 下一個 ›")
        for index, job_id in enumerate(self._job_order, start=1):
            self.file_combo.addItem(f"{index}. {self._source_names[job_id]}", job_id)
        navigator_layout.addWidget(self.previous_button)
        navigator_layout.addWidget(self.file_combo, 1)
        navigator_layout.addWidget(self.next_button)
        layout.addWidget(navigator)

        self.document_summary = QLabel()
        self.document_summary.setObjectName("seriesSummary")
        layout.addWidget(self.document_summary)

        preset_frame = QFrame()
        preset_frame.setObjectName("softPanel")
        preset_layout = QHBoxLayout(preset_frame)
        preset_layout.addWidget(QLabel("Quick series / 快速分組:"))
        self.one_document_button = QPushButton("One questionnaire / 整份一份")
        self.one_page_button = QPushButton("Each page / 每頁一份")
        self.pages_per_group = QSpinBox()
        self.pages_per_group.setRange(1, 9999)
        self.pages_per_group.setValue(2)
        self.pages_per_group.setSuffix(" pages / 頁")
        self.apply_size_button = QPushButton("Apply / 套用")
        self.copy_pattern_button = QPushButton("Copy to same-length files / 複製至同頁數檔案")
        for widget in (
            self.one_document_button,
            self.one_page_button,
            self.pages_per_group,
            self.apply_size_button,
            self.copy_pattern_button,
        ):
            preset_layout.addWidget(widget)
        preset_layout.addStretch()
        layout.addWidget(preset_frame)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["Questionnaire / 問卷", "Start / 起", "End / 迄", "Participant ID / 參加者編號", "Confidence", "Reason / 原因"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        controls = QHBoxLayout()
        self.split_button = QPushButton("Split selected / 分割所選")
        self.merge_button = QPushButton("Merge with next / 與下一組合併")
        controls.addWidget(self.split_button)
        controls.addWidget(self.merge_button)
        controls.addSpacing(20)
        controls.addWidget(QLabel("Auto IDs / 自動編號:"))
        self.id_prefix = QLineEdit("ID-")
        self.id_prefix.setMaximumWidth(110)
        self.id_start = QSpinBox()
        self.id_start.setRange(0, 999999)
        self.id_start.setValue(1)
        self.fill_ids_button = QPushButton("Fill current file / 填寫目前檔案")
        controls.addWidget(self.id_prefix)
        controls.addWidget(self.id_start)
        controls.addWidget(self.fill_ids_button)
        controls.addStretch()
        layout.addLayout(controls)

        self.validation_label = QLabel()
        self.validation_label.setObjectName("validation")
        layout.addWidget(self.validation_label)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("Confirm all & scan / 確認全部並掃描")
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.file_combo.currentIndexChanged.connect(self._switch_document)
        self.previous_button.clicked.connect(lambda: self._move_document(-1))
        self.next_button.clicked.connect(lambda: self._move_document(1))
        self.one_document_button.clicked.connect(lambda: self._apply_fixed_size(self._active_page_count()))
        self.one_page_button.clicked.connect(lambda: self._apply_fixed_size(1))
        self.apply_size_button.clicked.connect(lambda: self._apply_fixed_size(self.pages_per_group.value()))
        self.copy_pattern_button.clicked.connect(self.copy_pattern)
        self.split_button.clicked.connect(self.split_selected)
        self.merge_button.clicked.connect(self.merge_selected)
        self.fill_ids_button.clicked.connect(self.fill_participant_ids)
        if self._job_order:
            self._render_document(self._job_order[0])

    def _active_page_count(self) -> int:
        return self._page_counts.get(self._active_job_id or "", 1)

    def _spin(self, row: int, column: int) -> QSpinBox:
        widget = self.table.cellWidget(row, column)
        if not isinstance(widget, QSpinBox):
            raise TypeError("Page range control is missing")
        return widget

    def _participant(self, row: int) -> QLineEdit:
        widget = self.table.cellWidget(row, 3)
        if not isinstance(widget, QLineEdit):
            raise TypeError("Participant ID control is missing")
        return widget

    def _capture_active(self) -> None:
        if not self._active_job_id:
            return
        current: list[ProposedGroup] = []
        previous = self._groups.get(self._active_job_id, [])
        for row in range(self.table.rowCount()):
            prior = previous[row] if row < len(previous) else None
            current.append(
                ProposedGroup(
                    start_page=self._spin(row, 1).value(),
                    end_page=self._spin(row, 2).value(),
                    participant_id=self._participant(row).text().strip() or None,
                    confidence=prior.confidence if prior else 1.0,
                    reason=prior.reason if prior else "Edited by operator",
                )
            )
        self._groups[self._active_job_id] = current

    def _render_document(self, job_id: str) -> None:
        self._active_job_id = job_id
        page_count = self._page_counts[job_id]
        groups = self._groups[job_id]
        self.table.setRowCount(0)
        for row, group in enumerate(groups):
            self.table.insertRow(row)
            number = QTableWidgetItem(str(row + 1))
            number.setFlags(number.flags() & ~Qt.ItemFlag.ItemIsEditable)
            number.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.table.setItem(row, 0, number)
            for column, value in ((1, group.start_page), (2, group.end_page)):
                spinner = QSpinBox()
                spinner.setRange(1, page_count)
                spinner.setValue(value)
                spinner.valueChanged.connect(self._update_summary)
                self.table.setCellWidget(row, column, spinner)
            participant = QLineEdit(group.participant_id or "")
            participant.setPlaceholderText("Optional / 可留空")
            self.table.setCellWidget(row, 3, participant)
            confidence = QTableWidgetItem(f"{group.confidence:.0%}")
            confidence.setFlags(confidence.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.table.setItem(row, 4, confidence)
            reason = QTableWidgetItem(group.reason)
            reason.setFlags(reason.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.table.setItem(row, 5, reason)
        if groups:
            self.table.selectRow(0)
        self._update_summary()

    def _switch_document(self, index: int) -> None:
        if index < 0:
            return
        self._capture_active()
        job_id = str(self.file_combo.itemData(index))
        self._render_document(job_id)

    def _move_document(self, offset: int) -> None:
        target = self.file_combo.currentIndex() + offset
        if 0 <= target < self.file_combo.count():
            self.file_combo.setCurrentIndex(target)

    def _apply_fixed_size(self, size: int) -> None:
        if not self._active_job_id:
            return
        self._groups[self._active_job_id] = build_fixed_size_series(self._active_page_count(), size)
        self._render_document(self._active_job_id)

    def copy_pattern(self) -> None:
        if not self._active_job_id:
            return
        self._capture_active()
        page_count = self._active_page_count()
        pattern = self._groups[self._active_job_id]
        try:
            validate_group_partition([(group.start_page, group.end_page) for group in pattern], page_count)
        except ValueError as exc:
            QMessageBox.warning(self, "Invalid series / 分組無效", str(exc))
            return
        copied = 0
        for job_id in self._job_order:
            if job_id != self._active_job_id and self._page_counts[job_id] == page_count:
                self._groups[job_id] = clone_page_pattern(pattern, page_count)
                copied += 1
        QMessageBox.information(
            self,
            "Pattern copied / 已複製分組",
            f"Applied to {copied} other same-length file(s).\n已套用至 {copied} 個相同頁數的其他檔案。",
        )

    def split_selected(self) -> None:
        row = self.table.currentRow()
        if row < 0 or not self._active_job_id:
            return
        self._capture_active()
        groups = self._groups[self._active_job_id]
        start = groups[row].start_page
        end = groups[row].end_page
        if start >= end:
            QMessageBox.information(
                self, "Cannot split / 無法分割", "Select a group containing at least two pages. / 請選擇至少包含兩頁的分組。"
            )
            return
        middle = (start + end) // 2
        original = groups[row]
        groups[row] = ProposedGroup(
            start_page=start,
            end_page=middle,
            participant_id=original.participant_id,
            confidence=1.0,
            reason="Split by operator",
        )
        groups.insert(row + 1, ProposedGroup(
            start_page=middle + 1,
            end_page=end,
            participant_id=None,
            confidence=1.0,
            reason="Split by operator",
        ))
        self._render_document(self._active_job_id)
        self.table.selectRow(row + 1)

    def merge_selected(self) -> None:
        row = self.table.currentRow()
        if row < 0 or not self._active_job_id:
            return
        self._capture_active()
        groups = self._groups[self._active_job_id]
        if row + 1 >= len(groups):
            QMessageBox.information(self, "Cannot merge / 無法合併", "Select a group that has a following group. / 請選擇後面仍有分組的一列。")
            return
        first, second = groups[row], groups[row + 1]
        groups[row] = ProposedGroup(
            start_page=min(first.start_page, second.start_page),
            end_page=max(first.end_page, second.end_page),
            participant_id=first.participant_id or second.participant_id,
            confidence=1.0,
            reason="Merged by operator",
        )
        groups.pop(row + 1)
        self._render_document(self._active_job_id)
        self.table.selectRow(row)

    def fill_participant_ids(self) -> None:
        if not self._active_job_id:
            return
        ids = numbered_participant_ids(self.table.rowCount(), self.id_prefix.text(), self.id_start.value())
        for row, participant_id in enumerate(ids):
            self._participant(row).setText(participant_id)
        self._capture_active()

    def _update_summary(self) -> None:
        if not self._active_job_id:
            return
        source = self._source_names[self._active_job_id]
        page_count = self._active_page_count()
        total_files = len(self._job_order)
        current_file = self._job_order.index(self._active_job_id) + 1
        self.document_summary.setText(
            f"File {current_file} of {total_files} · {source} · {page_count} page(s) · "
            f"{self.table.rowCount()} questionnaire(s)  /  檔案 {current_file}/{total_files} · "
            f"{page_count} 頁 · {self.table.rowCount()} 份問卷"
        )
        try:
            ranges = [(self._spin(row, 1).value(), self._spin(row, 2).value()) for row in range(self.table.rowCount())]
            validate_group_partition(ranges, page_count)
            self.validation_label.setText("● Complete page coverage — ready to confirm / 頁面完整無重疊，可確認")
            self.validation_label.setStyleSheet("color: #117d65; font-weight: 700;")
        except (TypeError, ValueError) as exc:
            self.validation_label.setText(f"● Fix this series: {exc} / 請修正此分組")
            self.validation_label.setStyleSheet("color: #b42318; font-weight: 700;")
        index = self.file_combo.currentIndex()
        self.previous_button.setEnabled(index > 0)
        self.next_button.setEnabled(index + 1 < self.file_combo.count())

    def result_groups(self) -> dict[str, list[ProposedGroup]]:
        self._capture_active()
        return {
            job_id: [
                ProposedGroup(
                    start_page=group.start_page,
                    end_page=group.end_page,
                    participant_id=group.participant_id,
                    confidence=group.confidence,
                    reason="Confirmed in FormSight Local",
                )
                for group in groups
            ]
            for job_id, groups in self._groups.items()
        }

    def _validate_and_accept(self) -> None:
        try:
            grouped = self.result_groups()
            for job_id, groups in grouped.items():
                validate_group_partition(
                    [(group.start_page, group.end_page) for group in groups], self._page_counts[job_id]
                )
        except (TypeError, ValueError) as exc:
            QMessageBox.warning(self, "Invalid groups / 分組無效", str(exc))
            return
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self, *, auto_detect: bool = True):
        super().__init__()
        self.language = "en"
        self.discovery: DiscoveryResult | None = None
        self.runtime: DesktopRuntime | None = None
        self.discovery_thread: DiscoveryThread | None = None
        self.batch_thread: BatchThread | None = None
        self.current_batch_id: str | None = None
        self.output_ready: Path | None = None
        self.paths: list[Path] = []
        self.series_labels: list[str] = []
        self._refreshing_files = False
        self._resume_prompted = False
        self.setMinimumSize(1180, 800)
        self.setAcceptDrops(True)
        self._build_ui()
        self._apply_style()
        self.retranslate()
        if auto_detect:
            QTimer.singleShot(120, self.refresh_models)

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        outer = QVBoxLayout(root)
        outer.setContentsMargins(22, 18, 22, 18)
        outer.setSpacing(12)

        title_row = QHBoxLayout()
        title_box = QVBoxLayout()
        self.title_label = QLabel()
        self.title_label.setObjectName("title")
        self.subtitle_label = QLabel()
        self.subtitle_label.setObjectName("subtitle")
        title_box.addWidget(self.title_label)
        title_box.addWidget(self.subtitle_label)
        title_row.addLayout(title_box)
        title_row.addStretch()
        self.language_combo = QComboBox()
        self.language_combo.addItem("English", "en")
        self.language_combo.addItem("繁體中文", "zh")
        self.language_combo.currentIndexChanged.connect(self._change_language)
        title_row.addWidget(self.language_combo)
        outer.addLayout(title_row)

        step_row = QHBoxLayout()
        self.step_labels: list[QLabel] = []
        for _ in range(4):
            label = QLabel()
            label.setObjectName("stepPill")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            step_row.addWidget(label, 1)
            self.step_labels.append(label)
        outer.addLayout(step_row)

        readiness = QHBoxLayout()
        self.lm_card, self.lm_title, self.lm_status = self._status_card()
        self.yolo_card, self.yolo_title, self.yolo_status = self._status_card()
        readiness.addWidget(self.lm_card, 1)
        readiness.addWidget(self.yolo_card, 1)
        self.refresh_button = QPushButton()
        self.refresh_button.clicked.connect(self.refresh_models)
        readiness.addWidget(self.refresh_button)
        outer.addLayout(readiness)

        model_frame = QFrame()
        model_frame.setObjectName("panel")
        model_form = QFormLayout(model_frame)
        self.vision_label = QLabel()
        self.judge_label = QLabel()
        self.vision_combo = QComboBox()
        self.judge_combo = QComboBox()
        model_form.addRow(self.vision_label, self.vision_combo)
        model_form.addRow(self.judge_label, self.judge_combo)
        outer.addWidget(model_frame)

        file_header = QHBoxLayout()
        self.files_label = QLabel()
        self.files_label.setObjectName("section")
        file_header.addWidget(self.files_label)
        file_header.addStretch()
        self.add_files_button = QPushButton()
        self.add_folder_button = QPushButton()
        self.remove_button = QPushButton()
        self.clear_button = QPushButton()
        self.up_button = QPushButton()
        self.down_button = QPushButton()
        self.set_label_button = QPushButton()
        for button in (
            self.add_files_button,
            self.add_folder_button,
            self.set_label_button,
            self.remove_button,
            self.clear_button,
            self.up_button,
            self.down_button,
        ):
            file_header.addWidget(button)
        self.add_files_button.clicked.connect(self.add_files)
        self.add_folder_button.clicked.connect(self.add_folder)
        self.remove_button.clicked.connect(self.remove_selected)
        self.set_label_button.clicked.connect(self.set_selected_series_label)
        self.clear_button.clicked.connect(self.clear_files)
        self.up_button.clicked.connect(lambda: self.move_selected(-1))
        self.down_button.clicked.connect(lambda: self.move_selected(1))
        outer.addLayout(file_header)

        self.file_table = QTableWidget(0, 6)
        self.file_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.file_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.file_table.setEditTriggers(
            QAbstractItemView.EditTrigger.DoubleClicked
            | QAbstractItemView.EditTrigger.SelectedClicked
            | QAbstractItemView.EditTrigger.EditKeyPressed
        )
        self.file_table.setAlternatingRowColors(True)
        self.file_table.verticalHeader().setVisible(False)
        self.file_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.file_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.file_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.file_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self.file_table.itemChanged.connect(self._series_label_edited)
        outer.addWidget(self.file_table, 2)
        file_footer = QHBoxLayout()
        self.file_summary_label = QLabel()
        self.file_summary_label.setObjectName("summary")
        self.drop_hint_label = QLabel()
        self.drop_hint_label.setObjectName("muted")
        self.label_help_label = QLabel()
        self.label_help_label.setObjectName("muted")
        file_footer.addWidget(self.file_summary_label)
        file_footer.addSpacing(16)
        file_footer.addWidget(self.label_help_label)
        file_footer.addStretch()
        file_footer.addWidget(self.drop_hint_label)
        outer.addLayout(file_footer)

        output_frame = QFrame()
        output_frame.setObjectName("panel")
        output_layout = QVBoxLayout(output_frame)
        output_line = QHBoxLayout()
        self.output_label = QLabel()
        self.output_edit = QLineEdit()
        self.output_browse_button = QPushButton()
        self.output_browse_button.clicked.connect(self.choose_output)
        output_line.addWidget(self.output_label)
        output_line.addWidget(self.output_edit, 1)
        output_line.addWidget(self.output_browse_button)
        output_layout.addLayout(output_line)
        mode_form = QFormLayout()
        self.run_mode_label = QLabel()
        self.run_mode_combo = QComboBox()
        self.performance_label = QLabel()
        self.performance_combo = QComboBox()
        mode_form.addRow(self.run_mode_label, self.run_mode_combo)
        mode_form.addRow(self.performance_label, self.performance_combo)
        output_layout.addLayout(mode_form)
        self.output_hint_label = QLabel()
        self.output_hint_label.setObjectName("muted")
        self.output_hint_label.setWordWrap(True)
        output_layout.addWidget(self.output_hint_label)
        outer.addWidget(output_frame)

        action_row = QHBoxLayout()
        self.start_button = QPushButton()
        self.start_button.setObjectName("primary")
        self.start_button.clicked.connect(self.start_batch)
        self.cancel_button = QPushButton()
        self.cancel_button.clicked.connect(self.cancel_batch)
        self.cancel_button.setEnabled(False)
        self.resume_button = QPushButton()
        self.resume_button.clicked.connect(self.resume_last_batch)
        self.open_button = QPushButton()
        self.open_button.clicked.connect(self.open_excel)
        self.open_button.setEnabled(False)
        action_row.addWidget(self.start_button)
        action_row.addWidget(self.cancel_button)
        action_row.addWidget(self.resume_button)
        action_row.addStretch()
        action_row.addWidget(self.open_button)
        outer.addLayout(action_row)

        self.progress = QProgressBar()
        self.progress.setRange(0, 1000)
        self.progress.setValue(0)
        self.progress.setFormat("Idle / 閒置")
        outer.addWidget(self.progress)
        self.log_label = QLabel()
        self.log_label.setObjectName("section")
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumHeight(150)
        outer.addWidget(self.log_label)
        outer.addWidget(self.log)

    def _status_card(self) -> tuple[QFrame, QLabel, QLabel]:
        frame = QFrame()
        frame.setObjectName("statusCard")
        layout = QVBoxLayout(frame)
        title = QLabel()
        title.setObjectName("cardTitle")
        status = QLabel("Checking… / 正在檢查…")
        status.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(status)
        return frame, title, status

    def _apply_style(self) -> None:
        self.setStyleSheet(
            """
            QWidget { font-family: "Segoe UI", "Microsoft JhengHei"; font-size: 10pt; color: #17312d; }
            QMainWindow { background: #eef3f0; }
            QLabel#title { font-size: 25pt; font-weight: 700; color: #0d3b35; }
            QLabel#subtitle { color: #56706b; font-size: 11pt; }
            QLabel#section { font-size: 11pt; font-weight: 700; color: #173f39; }
            QLabel#stepPill { background: #dfe9e6; border: 1px solid #c3d3ce; border-radius: 10px; padding: 5px; color: #41635d; font-size: 9pt; font-weight: 700; }
            QLabel#summary { color: #0b675b; font-weight: 700; }
            QLabel#muted { color: #647b76; }
            QLabel#dialogTitle { font-size: 18pt; font-weight: 700; color: #0d3b35; }
            QLabel#seriesSummary { background: #e8f3ef; color: #174e45; border-radius: 6px; padding: 8px; font-weight: 700; }
            QFrame#panel, QFrame#statusCard { background: white; border: 1px solid #ccd9d5; border-radius: 8px; }
            QFrame#softPanel { background: #f7faf9; border: 1px solid #d6e2de; border-radius: 7px; }
            QLabel#cardTitle { font-weight: 700; color: #355f58; }
            QPushButton { background: white; border: 1px solid #a9bbb6; border-radius: 5px; padding: 7px 11px; }
            QPushButton:hover { background: #e4efeb; }
            QPushButton:disabled { color: #93a29e; background: #eef1f0; }
            QPushButton#primary { background: #0b675b; color: white; border: 0; font-weight: 700; padding: 9px 20px; }
            QPushButton#primary:hover { background: #095449; }
            QTableWidget, QTextEdit, QLineEdit, QComboBox { background: white; border: 1px solid #bdcdc8; border-radius: 4px; }
            QTableWidget::item:selected { background: #cfe9e2; color: #133b34; }
            QHeaderView::section { background: #dfe9e6; padding: 6px; border: 0; border-right: 1px solid #c6d4d0; font-weight: 600; }
            QProgressBar { border: 1px solid #a8bbb5; border-radius: 5px; text-align: center; background: white; min-height: 23px; }
            QProgressBar::chunk { background: #19a38c; border-radius: 4px; }
            """
        )

    def tr(self, key: str) -> str:  # type: ignore[override]
        return TEXT[self.language][key]

    def _change_language(self) -> None:
        self.language = str(self.language_combo.currentData())
        self.retranslate()

    def retranslate(self) -> None:
        self.setWindowTitle(f"{self.tr('title')} {__version__}")
        self.title_label.setText(self.tr("title"))
        self.subtitle_label.setText(self.tr("subtitle"))
        for label, key in zip(
            self.step_labels,
            ("step_models", "step_files", "step_groups", "step_scan"),
            strict=True,
        ):
            label.setText(self.tr(key))
        self.lm_title.setText(self.tr("lm"))
        self.yolo_title.setText(self.tr("yolo"))
        self.refresh_button.setText(self.tr("refresh"))
        self.vision_label.setText(self.tr("vision"))
        self.judge_label.setText(self.tr("judge"))
        self.files_label.setText(self.tr("files"))
        self.add_files_button.setText(self.tr("add_files"))
        self.add_folder_button.setText(self.tr("add_folder"))
        self.remove_button.setText(self.tr("remove"))
        self.clear_button.setText(self.tr("clear"))
        self.up_button.setText(self.tr("up"))
        self.down_button.setText(self.tr("down"))
        self.set_label_button.setText(self.tr("set_label"))
        self.output_label.setText(self.tr("output"))
        self.output_browse_button.setText(self.tr("browse"))
        self.run_mode_label.setText(self.tr("run_mode"))
        self.performance_label.setText(self.tr("performance"))
        run_mode = self.run_mode_combo.currentData() or "automatic"
        self.run_mode_combo.clear()
        self.run_mode_combo.addItem(self.tr("auto_one_take"), "automatic")
        self.run_mode_combo.addItem(self.tr("review_first"), "review")
        run_index = self.run_mode_combo.findData(run_mode)
        self.run_mode_combo.setCurrentIndex(max(0, run_index))
        performance_mode = self.performance_combo.currentData() or "balanced"
        self.performance_combo.clear()
        self.performance_combo.addItem(self.tr("balanced"), "balanced")
        self.performance_combo.addItem(self.tr("maximum"), "maximum")
        performance_index = self.performance_combo.findData(performance_mode)
        self.performance_combo.setCurrentIndex(max(0, performance_index))
        self.cancel_button.setText(self.tr("cancel"))
        self.resume_button.setText(self.tr("resume"))
        self.open_button.setText(self.tr("open"))
        self.log_label.setText(self.tr("log"))
        self.drop_hint_label.setText(self.tr("drop_hint"))
        self.label_help_label.setText(self.tr("label_help"))
        self.output_hint_label.setText(self.tr("output_hint"))
        self.file_table.setHorizontalHeaderLabels(
            [
                self.tr("source"),
                self.tr("type"),
                self.tr("series_label"),
                self.tr("path"),
                self.tr("workbook"),
                self.tr("status"),
            ]
        )
        self._update_file_summary()
        if self.discovery:
            self._show_discovery(self.discovery)

    def refresh_models(self) -> None:
        if self.discovery_thread and self.discovery_thread.isRunning():
            return
        self.refresh_button.setEnabled(False)
        self.start_button.setEnabled(False)
        self.lm_status.setText("Checking LM Studio… / 正在偵測 LM Studio…")
        self.discovery_thread = DiscoveryThread(self)
        self.discovery_thread.result_ready.connect(self._show_discovery)
        self.discovery_thread.finished.connect(lambda: self.refresh_button.setEnabled(True))
        self.discovery_thread.start()

    def _show_discovery(self, result: DiscoveryResult) -> None:
        self.discovery = result
        self.runtime = create_runtime(result.base_url)
        ready = result.status == "ready"
        color = "#117d65" if ready else "#b14a3c"
        self.lm_status.setStyleSheet(f"color: {color}; font-weight: 600;")
        if ready and result.selected_vision and result.selected_verifier:
            self.lm_status.setText(
                f"● {self.tr('ready')} — {result.selected_vision.display_name}\n"
                f"+ {result.selected_verifier.display_name} · 127.0.0.1:{result.port}"
            )
        else:
            self.lm_status.setText(f"● {self.tr('not_ready')} — {result.message}")
        self.vision_combo.clear()
        self.judge_combo.clear()
        primary_options = [
            model
            for model in result.vision_models
            if "qwen" in f"{model.key} {model.display_name} {model.architecture}".casefold()
        ]
        verifier_options = [
            model
            for model in result.vision_models
            if "qwen" not in f"{model.key} {model.display_name} {model.architecture}".casefold()
        ]
        for model in primary_options:
            self.vision_combo.addItem(f"{model.display_name} [{model.api_id}]", model.api_id)
        for model in verifier_options:
            self.judge_combo.addItem(f"{model.display_name} [{model.api_id}]", model.api_id)
        if result.selected_vision:
            index = self.vision_combo.findData(result.selected_vision.api_id)
            if index >= 0:
                self.vision_combo.setCurrentIndex(index)
        if result.selected_verifier:
            index = self.judge_combo.findData(result.selected_verifier.api_id)
            if index >= 0:
                self.judge_combo.setCurrentIndex(index)
        self.yolo_status.setStyleSheet(
            f"color: {'#117d65' if ready else '#a86b00'}; font-weight: 600;"
        )
        self.yolo_status.setText(
            (
                f"● {self.tr('yolo_ready')}\n"
                "Primary → verifier → cropped adjudication → reasonableness\n"
                "Recommended: Q4 · 8k–12k context · Flash Attention · full GPU offload"
                if ready
                else f"● {self.tr('qwen_only')}"
            )
        )
        self.start_button.setEnabled(ready and bool(self.paths) and not self._busy())
        if self.runtime:
            try:
                removed = LocalBatchRunner(self.runtime).purge_expired()
                if removed:
                    self._log(f"Purged {removed} expired local batch(es).")
            except Exception as exc:
                self._log(f"Retention cleanup warning: {exc}")
            if ready and not self._resume_prompted:
                self._resume_prompted = True
                resumable = LocalBatchRunner(self.runtime).latest_resumable_batch()
                if resumable:
                    QTimer.singleShot(0, self._offer_crash_resume)

    def _offer_crash_resume(self) -> None:
        if self._busy():
            return
        choice = QMessageBox.question(
            self,
            "Resume unfinished batch / 恢復未完成批次",
            "An unfinished batch was recovered. Completed pages and series workbooks are checkpointed. "
            "Resume now?\n已找到未完成批次；已完成頁面及系列活頁簿均已保存。現在恢復？",
        )
        if choice == QMessageBox.StandardButton.Yes:
            self.resume_last_batch()

    def add_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self,
            self.tr("select_files"),
            "",
            "Questionnaires (*.pdf *.png *.jpg *.jpeg *.tif *.tiff)",
        )
        self._add_paths(Path(value) for value in files)

    def add_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, self.tr("select_folder"))
        if not folder:
            return
        self._add_paths(self._expand_input_paths([Path(folder)]))

    def _expand_input_paths(self, paths) -> list[Path]:
        candidates: list[Path] = []
        for path in paths:
            if path.is_dir():
                candidates.extend(
                    child
                    for child in path.rglob("*")
                    if child.is_file() and child.suffix.casefold() in ALLOWED_SUFFIXES
                )
            elif path.is_file() and path.suffix.casefold() in ALLOWED_SUFFIXES:
                candidates.append(path)
        return sorted(candidates, key=lambda value: str(value).casefold())

    def _add_paths(self, paths) -> None:
        existing = {str(path).casefold() for path in self.paths}
        for path in paths:
            resolved = path.expanduser().resolve()
            if resolved.suffix.casefold() not in ALLOWED_SUFFIXES or str(resolved).casefold() in existing:
                continue
            self.paths.append(resolved)
            self.series_labels.append(self._default_series_label(resolved.stem))
            existing.add(str(resolved).casefold())
        if self.paths and not self.output_edit.text().strip():
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            self.output_edit.setText(str(Path.home() / "Documents" / f"FormSight Output {stamp}"))
        self._refresh_file_table()

    def _default_series_label(self, stem: str) -> str:
        base = normalize_series_label(stem)
        existing = {label.casefold() for label in self.series_labels}
        if base.casefold() not in existing:
            return base
        suffix = 2
        while f"{base} #{suffix}".casefold() in existing:
            suffix += 1
        return f"{base} #{suffix}"

    def _refresh_file_table(self) -> None:
        self._refreshing_files = True
        self.file_table.setRowCount(0)
        palette = ("#d9f1ea", "#e7e3f7", "#fae8ce", "#dcebf7", "#f5dfe7", "#e7efd5")
        label_colors: dict[str, str] = {}
        for index, path in enumerate(self.paths):
            row = self.file_table.rowCount()
            self.file_table.insertRow(row)
            label = self.series_labels[index]
            workbook_name = series_workbook_filename(label)
            values = [
                path.name,
                path.suffix.upper().lstrip("."),
                label,
                str(path.parent),
                workbook_name,
                "Pending / 待處理",
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setToolTip(str(path) if column in {0, 3} else value)
                if column != 2:
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                else:
                    label_key = label.casefold()
                    if label_key not in label_colors:
                        label_colors[label_key] = palette[len(label_colors) % len(palette)]
                    item.setBackground(QColor(label_colors[label_key]))
                    item.setToolTip(
                        f"Same label = same workbook / 相同標籤 = 同一活頁簿\n{workbook_name}"
                    )
                self.file_table.setItem(row, column, item)
        self._refreshing_files = False
        self._update_file_summary()

    def set_selected_series_label(self) -> None:
        rows = sorted({index.row() for index in self.file_table.selectedIndexes()})
        if not rows and self.file_table.currentRow() >= 0:
            rows = [self.file_table.currentRow()]
        if not rows:
            QMessageBox.information(
                self,
                self.tr("title"),
                "Select one or more files first. / 請先選取一個或多個檔案。",
            )
            return
        initial = self.series_labels[rows[0]] if len(rows) == 1 else "Series 1"
        value, accepted = QInputDialog.getText(
            self,
            "Series label / 系列標籤",
            "All selected PDFs will be combined into this Excel label:\n所有已選 PDF 將按此標籤合併輸出：",
            text=initial,
        )
        if not accepted:
            return
        try:
            label = normalize_series_label(value)
        except ValueError as exc:
            QMessageBox.warning(self, "Invalid label / 標籤無效", str(exc))
            return
        for row in rows:
            if 0 <= row < len(self.series_labels):
                self.series_labels[row] = label
        self._refresh_file_table()
        for row in rows:
            self.file_table.selectRow(row)

    def _series_label_edited(self, item: QTableWidgetItem) -> None:
        if self._refreshing_files or item.column() != 2 or not (0 <= item.row() < len(self.series_labels)):
            return
        try:
            self.series_labels[item.row()] = normalize_series_label(item.text())
        except ValueError:
            item.setText(self.series_labels[item.row()])
            return
        current_row = item.row()
        self._refresh_file_table()
        self.file_table.selectRow(current_row)

    def _update_file_summary(self) -> None:
        count = len(self.paths)
        series_count = len({label.casefold() for label in self.series_labels})
        self.file_summary_label.setText(
            self.tr("file_summary").format(count=count, series=series_count)
            if count
            else self.tr("file_summary_empty")
        )
        self.start_button.setText(f"{self.tr('start')} · {count}" if count else self.tr("start"))
        ready = bool(self.discovery and self.discovery.status == "ready")
        self.start_button.setEnabled(bool(count and ready and not self._busy()))

    def remove_selected(self) -> None:
        rows = sorted({index.row() for index in self.file_table.selectedIndexes()}, reverse=True)
        for row in rows:
            if 0 <= row < len(self.paths):
                self.paths.pop(row)
                self.series_labels.pop(row)
        self._refresh_file_table()

    def clear_files(self) -> None:
        self.paths.clear()
        self.series_labels.clear()
        self._refresh_file_table()

    def move_selected(self, direction: int) -> None:
        row = self.file_table.currentRow()
        target = row + direction
        if row < 0 or not (0 <= target < len(self.paths)):
            return
        self.paths[row], self.paths[target] = self.paths[target], self.paths[row]
        self.series_labels[row], self.series_labels[target] = (
            self.series_labels[target],
            self.series_labels[row],
        )
        self._refresh_file_table()
        self.file_table.selectRow(target)

    def choose_output(self) -> None:
        current = Path(self.output_edit.text().strip()).expanduser() if self.output_edit.text().strip() else Path.home()
        initial = current if current.is_dir() else current.parent
        folder = QFileDialog.getExistingDirectory(
            self,
            self.tr("select_output"),
            str(initial),
        )
        if folder:
            self.output_edit.setText(folder)

    def start_batch(self) -> None:
        if not self.paths:
            QMessageBox.warning(self, self.tr("title"), self.tr("need_files"))
            return
        output = self.output_edit.text().strip()
        if not output:
            QMessageBox.warning(self, self.tr("title"), self.tr("need_output"))
            return
        if not self.discovery or self.discovery.status != "ready" or not self.runtime:
            QMessageBox.warning(self, self.tr("title"), self.discovery.message if self.discovery else "LM Studio is not ready")
            return
        self.output_ready = None
        self.open_button.setEnabled(False)
        sources = list(self.paths)
        review = self.run_mode_combo.currentData() == "review"
        processing_mode = str(self.performance_combo.currentData() or "balanced")
        vision_id = str(self.vision_combo.currentData())
        verifier_id = str(self.judge_combo.currentData())
        discovery = self.discovery

        def prepare(runner: LocalBatchRunner):
            batch_id = runner.create_batch(
                sources,
                output,
                discovery,
                review_groups=review,
                extractor_model_id=vision_id,
                verifier_model_id=verifier_id,
                judge_model_id=vision_id,
                series_labels=list(self.series_labels),
                processing_mode=processing_mode,
            )
            if review:
                return "prepared", batch_id
            return runner.execute_batch(batch_id)

        self._run_thread(prepare, preparing=True)

    def _run_thread(self, operation: Callable[[LocalBatchRunner], Any], *, preparing: bool = False) -> None:
        if not self.runtime:
            return
        self._set_busy(True)
        self.batch_thread = BatchThread(operation, self.runtime)
        self.batch_thread.event.connect(self._handle_event)
        self.batch_thread.prepared.connect(self._handle_prepared)
        self.batch_thread.succeeded.connect(self._handle_success)
        self.batch_thread.failed.connect(self._handle_failure)
        self.batch_thread.start()
        if preparing:
            self._log("Preparing durable local batch / 正在準備可恢復的本機批次")

    def _handle_prepared(self, batch_id: str, drafts: list[GroupDraft]) -> None:
        self.current_batch_id = batch_id
        self._set_busy(False)
        dialog = GroupReviewDialog(drafts, self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            self._log("Group review cancelled. The prepared batch can be resumed later.")
            return
        assert self.runtime
        try:
            LocalBatchRunner(self.runtime).confirm_groups(batch_id, dialog.result_groups())
        except Exception as exc:
            QMessageBox.critical(self, "Group confirmation failed", str(exc))
            return
        self._run_thread(lambda runner: runner.execute_batch(batch_id))

    def _handle_event(self, event: RunnerEvent) -> None:
        self.current_batch_id = event.batch_id
        self.progress.setValue(round(event.progress * 1000))
        self.progress.setFormat(f"{event.progress:.0%} — {event.message}")
        self._log(event.message)
        if event.source_index is not None and 0 <= event.source_index < self.file_table.rowCount():
            self.file_table.item(event.source_index, 5).setText(event.message)

    def _handle_success(self, result: Any) -> None:
        self._set_busy(False)
        if result is None:
            self._log("Batch paused / 批次已暫停")
            return
        output_directory = (
            Path(str(result["output_directory"]))
            if isinstance(result, dict) and result.get("output_directory")
            else None
        )
        self.output_ready = output_directory
        self.open_button.setEnabled(bool(output_directory and output_directory.exists()))
        self.progress.setValue(1000)
        self.progress.setFormat(str(result.get("status", "COMPLETED")) if isinstance(result, dict) else "COMPLETED")
        self._update_rows_from_batch()
        count = int(result.get("workbooks", 0)) if isinstance(result, dict) else 0
        QMessageBox.information(
            self,
            self.tr("title"),
            (
                f"{self.tr('completed')}\n\n{count} workbook(s) / 活頁簿\n{output_directory}"
                if output_directory
                else self.tr("completed")
            ),
        )

    def _handle_failure(self, message: str) -> None:
        self._set_busy(False)
        self._log(f"ERROR: {message}")
        QMessageBox.critical(self, "FormSight Local", message)
        self._update_rows_from_batch()

    def cancel_batch(self) -> None:
        if self.batch_thread and self.batch_thread.isRunning():
            self.cancel_button.setEnabled(False)
            self.batch_thread.cancel()
            self._log("Cancellation requested; finishing the current LM Studio request…")

    def resume_last_batch(self) -> None:
        if not self.runtime or not self.discovery or self.discovery.status != "ready":
            QMessageBox.warning(self, self.tr("title"), "LM Studio must be ready before resuming.")
            return
        batch_id = LocalBatchRunner(self.runtime).latest_resumable_batch()
        if not batch_id:
            QMessageBox.information(self, self.tr("title"), "No resumable batch was found.")
            return
        self.current_batch_id = batch_id
        status = LocalBatchRunner(self.runtime).batch_status(batch_id)
        if status["status"] == "awaiting_confirmation":
            drafts = LocalBatchRunner(self.runtime).group_drafts(batch_id)
            self._handle_prepared(batch_id, drafts)
            return
        self._run_thread(lambda runner: runner.resume_batch(batch_id))

    def _update_rows_from_batch(self) -> None:
        if not self.runtime or not self.current_batch_id:
            return
        try:
            status = LocalBatchRunner(self.runtime).batch_status(self.current_batch_id)
        except Exception:
            return
        items = status.get("items", [])
        by_path = {str(path).casefold(): index for index, path in enumerate(self.paths)}
        for item in items:
            row = by_path.get(str(item["source"]).casefold())
            if row is None or row >= self.file_table.rowCount():
                continue
            text = str(item["status"])
            if item.get("error"):
                text += f" — {item['error']}"
            if item.get("output_path"):
                self.file_table.item(row, 4).setText(Path(str(item["output_path"])).name)
            cell = self.file_table.item(row, 5)
            cell.setText(text)
            if item["status"] == "failed":
                cell.setForeground(QColor("#b42318"))

    def open_excel(self) -> None:
        if self.output_ready and self.output_ready.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(self.output_ready)))

    def _set_busy(self, busy: bool) -> None:
        for widget in (
            self.add_files_button,
            self.add_folder_button,
            self.remove_button,
            self.clear_button,
            self.up_button,
            self.down_button,
            self.set_label_button,
            self.file_table,
            self.output_browse_button,
            self.run_mode_combo,
            self.performance_combo,
            self.vision_combo,
            self.judge_combo,
            self.refresh_button,
            self.resume_button,
        ):
            widget.setEnabled(not busy)
        self.start_button.setEnabled(
            not busy and bool(self.paths) and bool(self.discovery and self.discovery.status == "ready")
        )
        self.cancel_button.setEnabled(busy)

    def _busy(self) -> bool:
        return bool(self.batch_thread and self.batch_thread.isRunning())

    def _log(self, message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log.append(f"[{stamp}] {message}")

    def dragEnterEvent(self, event) -> None:  # type: ignore[no-untyped-def]
        if event.mimeData().hasUrls() and any(url.isLocalFile() for url in event.mimeData().urls()):
            event.acceptProposedAction()
            return
        event.ignore()

    def dropEvent(self, event) -> None:  # type: ignore[no-untyped-def]
        local_paths = [Path(url.toLocalFile()) for url in event.mimeData().urls() if url.isLocalFile()]
        self._add_paths(self._expand_input_paths(local_paths))
        event.acceptProposedAction()

    def closeEvent(self, event) -> None:  # type: ignore[no-untyped-def]
        if self._busy():
            choice = QMessageBox.question(
                self,
                "FormSight Local",
                "A batch is running. Pause it and exit?\n批次正在執行。是否暫停並離開？",
            )
            if choice != QMessageBox.StandardButton.Yes:
                event.ignore()
                return
            assert self.batch_thread
            self.batch_thread.cancel()
            self.batch_thread.wait(3000)
        event.accept()


def run() -> int:
    app = QApplication.instance() or QApplication([])
    app.setApplicationName("FormSight Local")
    app.setOrganizationName("FormSight")
    window = MainWindow()
    window.show()
    return app.exec()
