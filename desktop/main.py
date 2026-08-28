from __future__ import annotations

import multiprocessing
import os
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication

from desktop.model_discovery import discover_models
from desktop.runtime import create_runtime
from desktop.ui import MainWindow, run


def main() -> int:
    smoke = "--smoke-test" in sys.argv
    smoke_log = Path(os.environ.get("LOCALAPPDATA") or Path.cwd()) / "FormSightLocal-smoke.log"
    if smoke:
        smoke_log.parent.mkdir(parents=True, exist_ok=True)
        smoke_log.write_text("main\n", encoding="utf-8")
    multiprocessing.freeze_support()
    if smoke:
        with smoke_log.open("a", encoding="utf-8") as handle:
            handle.write("freeze_support\n")
        app = QApplication.instance() or QApplication([])
        discovery = discover_models()
        with smoke_log.open("a", encoding="utf-8") as handle:
            handle.write(f"discovery:{discovery.status}\n")
        create_runtime(discovery.base_url)
        with smoke_log.open("a", encoding="utf-8") as handle:
            handle.write("runtime\n")
        window = MainWindow(auto_detect=False)
        window._show_discovery(discovery)
        app.processEvents()
        with smoke_log.open("a", encoding="utf-8") as handle:
            handle.write("complete\n")
        return 0
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
