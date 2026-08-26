from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and export the FormSight mark detector")
    parser.add_argument("dataset", type=Path, help="Path to dataset.yaml")
    parser.add_argument("--base", default="yolo11s.pt", help="Ultralytics detection checkpoint")
    parser.add_argument("--epochs", type=int, default=150)
    parser.add_argument("--image-size", type=int, default=1280)
    parser.add_argument("--batch", type=int, default=-1)
    parser.add_argument("--name", default="formsight-marks-v1")
    args = parser.parse_args()
    if not args.dataset.exists():
        raise SystemExit(f"Dataset not found: {args.dataset}")
    try:
        from ultralytics import YOLO
    except ImportError as exc:
        raise SystemExit("Install backend/requirements-ml.txt before training") from exc

    model = YOLO(args.base)
    training = model.train(
        data=str(args.dataset),
        epochs=args.epochs,
        imgsz=args.image_size,
        batch=args.batch,
        device=0,
        project=str(args.dataset.parent / "runs"),
        name=args.name,
        degrees=2.5,
        translate=0.04,
        scale=0.20,
        perspective=0.0002,
        hsv_h=0.01,
        hsv_s=0.20,
        hsv_v=0.20,
        fliplr=0,
        flipud=0,
        close_mosaic=20,
    )
    best = Path(training.save_dir) / "weights" / "best.pt"
    accepted = YOLO(best)
    metrics = accepted.val(data=str(args.dataset), split="test", imgsz=args.image_size, device=0)
    exported = accepted.export(format="onnx", imgsz=args.image_size, dynamic=True, simplify=True)
    summary = {
        "checkpoint": str(best.resolve()),
        "onnx": str(Path(exported).resolve()),
        "precision": float(metrics.box.mp),
        "recall": float(metrics.box.mr),
        "map50": float(metrics.box.map50),
        "map50_95": float(metrics.box.map),
        "per_class_map50_95": [float(value) for value in metrics.box.maps],
    }
    summary_path = Path(training.save_dir) / "release-metrics.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(summary_path)


if __name__ == "__main__":
    main()
