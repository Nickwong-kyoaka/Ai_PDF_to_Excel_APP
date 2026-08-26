from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .config import get_settings
from .database import SessionLocal, init_database
from .retention import purge_expired


def backup(destination: Path) -> Path:
    settings = get_settings()
    if not settings.database_url.startswith("sqlite:///"):
        raise RuntimeError("The bundled backup command currently supports SQLite only")
    source = Path(settings.database_url.removeprefix("sqlite:///"))
    if not source.is_absolute():
        source = (Path.cwd() / source).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / f"formsight-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.db"
    with sqlite3.connect(source) as source_db, sqlite3.connect(target) as target_db:
        source_db.backup(target_db)
    manifest = target.with_suffix(".json")
    manifest.write_text(
        json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "database": target.name,
                "note": "Questionnaire files and model weights are intentionally excluded; back them up under your approved PII policy.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="FormSight maintenance")
    subparsers = parser.add_subparsers(dest="command", required=True)
    backup_parser = subparsers.add_parser("backup")
    backup_parser.add_argument("destination", type=Path)
    subparsers.add_parser("purge")
    args = parser.parse_args()
    init_database()
    if args.command == "backup":
        print(backup(args.destination))
    else:
        with SessionLocal() as db:
            print(json.dumps(purge_expired(db, get_settings())))


if __name__ == "__main__":
    main()
