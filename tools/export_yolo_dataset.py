from __future__ import annotations

import argparse
import shutil
import sys
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.database import SessionLocal, init_database  # noqa: E402
from app.models import Annotation  # noqa: E402
from app.scanner.yolo import MARK_CLASSES  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Export reviewed FormSight annotations to YOLO format")
    parser.add_argument("output", type=Path)
    parser.add_argument("--minimum-real-test-per-class", type=int, default=50)
    args = parser.parse_args()
    init_database()
    with SessionLocal() as db:
        annotations = db.query(Annotation).order_by(Annotation.source_id, Annotation.page_number).all()
    if not annotations:
        raise SystemExit("No annotations exist. Use the admin annotation workspace first.")

    source_splits: dict[str, set[str]] = defaultdict(set)
    by_image: dict[tuple[str, int, str, str], list[Annotation]] = defaultdict(list)
    for annotation in annotations:
        source_splits[annotation.source_id].add(annotation.split)
        by_image[(annotation.source_id, annotation.page_number, annotation.image_path, annotation.split)].append(annotation)
    leakage = [source for source, splits in source_splits.items() if len(splits) > 1]
    if leakage:
        raise SystemExit(f"Participant/document leakage detected across splits: {', '.join(leakage[:10])}")

    if args.output.exists():
        raise SystemExit(f"Output already exists: {args.output}. Use a new versioned dataset directory.")
    for split in ("train", "val", "test"):
        (args.output / "images" / split).mkdir(parents=True, exist_ok=True)
        (args.output / "labels" / split).mkdir(parents=True, exist_ok=True)

    counts = Counter()
    test_counts = Counter()
    for (source_id, page_number, image_path, split), records in by_image.items():
        source = Path(image_path)
        if not source.exists():
            raise SystemExit(f"Annotated source image is missing: {source}")
        stem = f"{source_id}-p{page_number:04d}"
        target_image = args.output / "images" / split / f"{stem}{source.suffix.casefold()}"
        shutil.copy2(source, target_image)
        labels = []
        for record in records:
            class_id = MARK_CLASSES.index(record.mark_class)
            x1, y1, x2, y2 = record.bbox
            labels.append(f"{class_id} {(x1 + x2) / 2:.8f} {(y1 + y2) / 2:.8f} {x2 - x1:.8f} {y2 - y1:.8f}")
            counts[record.mark_class] += 1
            if split == "test":
                test_counts[record.mark_class] += 1
        (args.output / "labels" / split / f"{stem}.txt").write_text("\n".join(labels) + "\n", encoding="utf-8")

    yaml = [f"path: {args.output.resolve().as_posix()}", "train: images/train", "val: images/val", "test: images/test", "names:"]
    yaml.extend(f"  {index}: {name}" for index, name in enumerate(MARK_CLASSES))
    (args.output / "dataset.yaml").write_text("\n".join(yaml) + "\n", encoding="utf-8")
    print("All annotations:", dict(counts))
    print("Held-out real annotations:", dict(test_counts))
    under = [name for name in MARK_CLASSES if test_counts[name] < args.minimum_real_test_per_class]
    if under:
        print("EXPERIMENTAL classes below release minimum:", ", ".join(under))
    print(args.output / "dataset.yaml")


if __name__ == "__main__":
    main()
