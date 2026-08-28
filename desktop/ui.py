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
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
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
from backend.app.scanner.yolo import YoloMarkDetector

from . import __version__
from .model_discovery import DiscoveryResult, discover_models
from .runner import ALLOWED_SUFFIXES, GroupDraft, LocalBatchRunner, RunnerEvent
from .runtime import DesktopRuntime, create_runtime


TEXT = {
    "en": {
        "title": "FormSight Local",
        "subtitle": "Batch input → one corresponding Excel workbook per file",
        "language": "介面語言",
        "lm": "LM Studio",
        "yolo": "YOLO marks",
        "refresh": "Refresh detection",
        "add_files": "Add Files",
        "add_folder": "Add Folder",
        "remove": "Remove",
        "clear": "Clear",
        "up": "Move Up",
        "down": "Move Down",
        "files": "Input questionnaires",
        "output": "Output folder",
        "browse": "Browse…",
        "review": "Review page groups before scanning",
        "vision": "Vision extractor",
        "judge": "Reasonableness checker",
        "start": "Start Scan",
        "cancel": "Cancel",
        "resume": "Resume Last Batch",
        "open": "Open Output Folder",
        "ready": "Ready",
        "not_ready": "Not ready",
        "qwen_only": "Qwen-only mode — no accepted YOLO ONNX weights found",
        "yolo_ready": "ONNX detector ready",
        "source": "Source file",
        "type": "Type",
        "path": "Location",
        "status": "Status",
        "log": "Run log",
        "select_files": "Select questionnaires",
        "select_folder": "Select a folder",
        "select_output": "Choose the folder for the Excel workbooks",
        "need_files": "Add at least one questionnaire file.",
        "need_output": "Choose an output folder.",
        "completed": "The separate Excel workbooks are ready.",
    },
    "zh": {
        "title": "FormSight 本機版",
        "subtitle": "批量輸入 → 每個檔案各自輸出一個 Excel 活頁簿",
        "language": "Interface language",
        "lm": "LM Studio",
        "yolo": "YOLO 標記辨識",
        "refresh": "重新偵測",
        "add_files": "加入檔案",
        "add_folder": "加入資料夾",
        "remove": "移除",
        "clear": "清除",
        "up": "上移",
        "down": "下移",
        "files": "輸入問卷",
        "output": "輸出資料夾",
        "browse": "瀏覽…",
        "review": "掃描前檢查頁面分組",
        "vision": "視覺擷取模型",
        "judge": "合理性檢查模型",
        "start": "開始掃描",
        "cancel": "取消",
        "resume": "恢復上次批次",
        "open": "開啟輸出資料夾",
        "ready": "準備完成",
        "not_ready": "尚未準備",
        "qwen_only": "僅使用 Qwen 模式 — 找不到已核准的 YOLO ONNX 權重",
        "yolo_ready": "ONNX 偵測器準備完成",
        "source": "來源檔案",
        "type": "類型",
        "path": "位置",
        "status": "狀態",
        "log": "執行記錄",
        "select_files": "選擇問卷",
        "select_folder": "選擇資料夾",
        "select_output": "選擇 Excel 活頁簿輸出資料夾",
        "need_files": "請加入至少一個問卷檔案。",
        "need_output": "請選擇輸出資料夾。",
        "completed": "各檔案對應的 Excel 活頁簿已完成。",
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
        self.setWindowTitle("Review page groups / 檢查頁面分組")
        self.resize(1050, 560)
        layout = QVBoxLayout(self)
        note = QLabel(
            "Each PDF page must be covered exactly once. Edit Start/End or Participant ID; "
            "use Split to create another questionnaire.\n"
            "每一頁必須恰好屬於一個分組。可修改起始／結束頁及參加者編號，或用「分割」新增問卷。"
        )
        note.setWordWrap(True)
        layout.addWidget(note)
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(
            ["File / 檔案", "Group", "Pages", "Start / 起", "End / 迄", "Participant ID", "Confidence", "Reason"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeMode.Stretch)
        for draft in drafts:
            self._append(draft)
        layout.addWidget(self.table)

        controls = QHBoxLayout()
        split_button = QPushButton("Split selected / 分割所選")
        merge_button = QPushButton("Merge selected / 合併所選")
        split_button.clicked.connect(self.split_selected)
        merge_button.clicked.connect(self.merge_selected)
        controls.addWidget(split_button)
        controls.addWidget(merge_button)
        controls.addStretch()
        layout.addLayout(controls)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _append(self, draft: GroupDraft, row: int | None = None) -> None:
        row = self.table.rowCount() if row is None else row
        self.table.insertRow(row)
        values = [
            draft.source_file,
            str(draft.group_index + 1),
            str(draft.page_count),
            str(draft.start_page),
            str(draft.end_page),
            draft.participant_id or "",
            f"{draft.confidence:.0%}",
            draft.reason,
        ]
        for column, value in enumerate(values):
            item = QTableWidgetItem(value)
            if column == 0:
                item.setData(Qt.ItemDataRole.UserRole, draft.job_id)
                item.setData(Qt.ItemDataRole.UserRole + 1, draft.page_count)
            if column in {0, 1, 2, 6, 7}:
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.table.setItem(row, column, item)

    def split_selected(self) -> None:
        row = self.table.currentRow()
        if row < 0:
            return
        start = int(self.table.item(row, 3).text())
        end = int(self.table.item(row, 4).text())
        if start >= end:
            QMessageBox.information(self, "Cannot split", "Select a group containing at least two pages.")
            return
        middle = (start + end) // 2
        self.table.item(row, 4).setText(str(middle))
        first = self.table.item(row, 0)
        draft = GroupDraft(
            job_id=str(first.data(Qt.ItemDataRole.UserRole)),
            source_file=first.text(),
            page_count=int(first.data(Qt.ItemDataRole.UserRole + 1)),
            group_index=row + 1,
            start_page=middle + 1,
            end_page=end,
            participant_id=None,
            confidence=1.0,
            reason="Split by operator",
        )
        self._append(draft, row + 1)
        self._renumber()

    def merge_selected(self) -> None:
        row = self.table.currentRow()
        if row < 0:
            return
        job_id = self.table.item(row, 0).data(Qt.ItemDataRole.UserRole)
        neighbor = row - 1 if row > 0 and self.table.item(row - 1, 0).data(Qt.ItemDataRole.UserRole) == job_id else row + 1
        if neighbor >= self.table.rowCount() or self.table.item(neighbor, 0).data(Qt.ItemDataRole.UserRole) != job_id:
            QMessageBox.information(self, "Cannot merge", "No adjacent group belongs to the same PDF.")
            return
        keep, remove = (neighbor, row) if neighbor < row else (row, neighbor)
        start = min(int(self.table.item(keep, 3).text()), int(self.table.item(remove, 3).text()))
        end = max(int(self.table.item(keep, 4).text()), int(self.table.item(remove, 4).text()))
        self.table.item(keep, 3).setText(str(start))
        self.table.item(keep, 4).setText(str(end))
        self.table.removeRow(remove)
        self._renumber()

    def _renumber(self) -> None:
        counts: defaultdict[str, int] = defaultdict(int)
        for row in range(self.table.rowCount()):
            job_id = str(self.table.item(row, 0).data(Qt.ItemDataRole.UserRole))
            counts[job_id] += 1
            self.table.item(row, 1).setText(str(counts[job_id]))

    def result_groups(self) -> dict[str, list[ProposedGroup]]:
        result: dict[str, list[ProposedGroup]] = defaultdict(list)
        for row in range(self.table.rowCount()):
            source = self.table.item(row, 0)
            job_id = str(source.data(Qt.ItemDataRole.UserRole))
            result[job_id].append(
                ProposedGroup(
                    start_page=int(self.table.item(row, 3).text()),
                    end_page=int(self.table.item(row, 4).text()),
                    participant_id=self.table.item(row, 5).text().strip() or None,
                    confidence=1.0,
                    reason="Confirmed in FormSight Local",
                )
            )
        return dict(result)

    def _validate_and_accept(self) -> None:
        try:
            grouped = self.result_groups()
            page_counts: dict[str, int] = {}
            for row in range(self.table.rowCount()):
                source = self.table.item(row, 0)
                page_counts[str(source.data(Qt.ItemDataRole.UserRole))] = int(
                    source.data(Qt.ItemDataRole.UserRole + 1)
                )
            for job_id, groups in grouped.items():
                validate_group_partition(
                    [(group.start_page, group.end_page) for group in groups], page_counts[job_id]
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
        self.setMinimumSize(1080, 760)
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
        for button in (
            self.add_files_button,
            self.add_folder_button,
            self.remove_button,
            self.clear_button,
            self.up_button,
            self.down_button,
        ):
            file_header.addWidget(button)
        self.add_files_button.clicked.connect(self.add_files)
        self.add_folder_button.clicked.connect(self.add_folder)
        self.remove_button.clicked.connect(self.remove_selected)
        self.clear_button.clicked.connect(self.clear_files)
        self.up_button.clicked.connect(lambda: self.move_selected(-1))
        self.down_button.clicked.connect(lambda: self.move_selected(1))
        outer.addLayout(file_header)

        self.file_table = QTableWidget(0, 4)
        self.file_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.file_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.file_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.file_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.file_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        outer.addWidget(self.file_table, 2)

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
        self.review_checkbox = QCheckBox()
        self.review_checkbox.setChecked(True)
        output_layout.addWidget(self.review_checkbox)
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
            QFrame#panel, QFrame#statusCard { background: white; border: 1px solid #ccd9d5; border-radius: 8px; }
            QLabel#cardTitle { font-weight: 700; color: #355f58; }
            QPushButton { background: white; border: 1px solid #a9bbb6; border-radius: 5px; padding: 7px 11px; }
            QPushButton:hover { background: #e4efeb; }
            QPushButton:disabled { color: #93a29e; background: #eef1f0; }
            QPushButton#primary { background: #0b675b; color: white; border: 0; font-weight: 700; padding: 9px 20px; }
            QPushButton#primary:hover { background: #095449; }
            QTableWidget, QTextEdit, QLineEdit, QComboBox { background: white; border: 1px solid #bdcdc8; border-radius: 4px; }
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
        self.output_label.setText(self.tr("output"))
        self.output_browse_button.setText(self.tr("browse"))
        self.review_checkbox.setText(self.tr("review"))
        self.start_button.setText(self.tr("start"))
        self.cancel_button.setText(self.tr("cancel"))
        self.resume_button.setText(self.tr("resume"))
        self.open_button.setText(self.tr("open"))
        self.log_label.setText(self.tr("log"))
        self.file_table.setHorizontalHeaderLabels(
            [self.tr("source"), self.tr("type"), self.tr("path"), self.tr("status")]
        )
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
        if ready and result.selected_vision:
            self.lm_status.setText(
                f"● {self.tr('ready')} — {result.selected_vision.display_name}\n127.0.0.1:{result.port}"
            )
        else:
            self.lm_status.setText(f"● {self.tr('not_ready')} — {result.message}")
        self.vision_combo.clear()
        self.judge_combo.clear()
        for model in result.vision_models:
            self.vision_combo.addItem(f"{model.display_name} [{model.api_id}]", model.api_id)
        judge_options = list(result.judge_models)
        if result.selected_vision and all(item.api_id != result.selected_vision.api_id for item in judge_options):
            judge_options.append(result.selected_vision)
        for model in judge_options:
            suffix = " — reuse vision / 重用視覺模型" if model.vision else ""
            self.judge_combo.addItem(f"{model.display_name}{suffix}", model.api_id)
        if result.selected_judge:
            index = self.judge_combo.findData(result.selected_judge.api_id)
            if index >= 0:
                self.judge_combo.setCurrentIndex(index)
        weights_ready = self.runtime.weights_path.exists()
        detector_health: dict[str, Any] = {}
        if weights_ready:
            detector = YoloMarkDetector(self.runtime.weights_path)
            detector_health = detector.health()
            detector.release()
            weights_ready = detector_health.get("status") == "online"
        self.yolo_status.setStyleSheet(
            f"color: {'#117d65' if weights_ready else '#a86b00'}; font-weight: 600;"
        )
        self.yolo_status.setText(
            f"● {self.tr('yolo_ready')} — {detector_health.get('provider', '')}\n"
            f"{detector_health.get('warning') or self.runtime.weights_path}"
            if weights_ready
            else f"● {self.tr('qwen_only')}\n{detector_health.get('error') or ''}"
        )
        self.start_button.setEnabled(ready and not self._busy())
        if self.runtime:
            try:
                removed = LocalBatchRunner(self.runtime).purge_expired()
                if removed:
                    self._log(f"Purged {removed} expired local batch(es).")
            except Exception as exc:
                self._log(f"Retention cleanup warning: {exc}")

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
        candidates = sorted(
            (path for path in Path(folder).rglob("*") if path.is_file() and path.suffix.casefold() in ALLOWED_SUFFIXES),
            key=lambda path: str(path).casefold(),
        )
        self._add_paths(candidates)

    def _add_paths(self, paths) -> None:
        existing = {str(path).casefold() for path in self.paths}
        for path in paths:
            resolved = path.expanduser().resolve()
            if resolved.suffix.casefold() not in ALLOWED_SUFFIXES or str(resolved).casefold() in existing:
                continue
            self.paths.append(resolved)
            existing.add(str(resolved).casefold())
        if self.paths and not self.output_edit.text().strip():
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            self.output_edit.setText(str(Path.home() / "Documents" / f"FormSight Output {stamp}"))
        self._refresh_file_table()

    def _refresh_file_table(self) -> None:
        self.file_table.setRowCount(0)
        for path in self.paths:
            row = self.file_table.rowCount()
            self.file_table.insertRow(row)
            values = [path.name, path.suffix.upper().lstrip("."), str(path.parent), "Pending / 待處理"]
            for column, value in enumerate(values):
                self.file_table.setItem(row, column, QTableWidgetItem(value))

    def remove_selected(self) -> None:
        rows = sorted({index.row() for index in self.file_table.selectedIndexes()}, reverse=True)
        for row in rows:
            if 0 <= row < len(self.paths):
                self.paths.pop(row)
        self._refresh_file_table()

    def clear_files(self) -> None:
        self.paths.clear()
        self._refresh_file_table()

    def move_selected(self, direction: int) -> None:
        row = self.file_table.currentRow()
        target = row + direction
        if row < 0 or not (0 <= target < len(self.paths)):
            return
        self.paths[row], self.paths[target] = self.paths[target], self.paths[row]
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
        review = self.review_checkbox.isChecked()
        vision_id = str(self.vision_combo.currentData())
        judge_id = str(self.judge_combo.currentData())
        discovery = self.discovery

        def prepare(runner: LocalBatchRunner):
            batch_id = runner.create_batch(
                sources,
                output,
                discovery,
                review_groups=review,
                extractor_model_id=vision_id,
                judge_model_id=judge_id,
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
            self.file_table.item(event.source_index, 3).setText(event.message)

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
            cell = self.file_table.item(row, 3)
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
            self.output_browse_button,
            self.review_checkbox,
            self.vision_combo,
            self.judge_combo,
            self.refresh_button,
            self.resume_button,
        ):
            widget.setEnabled(not busy)
        self.start_button.setEnabled(not busy and bool(self.discovery and self.discovery.status == "ready"))
        self.cancel_button.setEnabled(busy)

    def _busy(self) -> bool:
        return bool(self.batch_thread and self.batch_thread.isRunning())

    def _log(self, message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log.append(f"[{stamp}] {message}")

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
