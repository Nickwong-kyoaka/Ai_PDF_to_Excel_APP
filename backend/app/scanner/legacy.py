from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

from PIL import Image, ImageEnhance, ImageFilter, ImageOps


class V14Compatibility:
    """Uses safe image helpers from v14 when the original module is available."""

    def __init__(self, path: Path):
        self.path = path
        self.module: ModuleType | None = None
        if path.exists():
            try:
                spec = importlib.util.spec_from_file_location("formsight_legacy_v14", path)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self.module = module
            except Exception:
                self.module = None

    @property
    def available(self) -> bool:
        return self.module is not None

    def enhance(self, image: Image.Image) -> Image.Image:
        if self.module and hasattr(self.module, "enhance_image"):
            return self.module.enhance_image(image)
        prepared = ImageOps.autocontrast(ImageOps.grayscale(image)).convert("RGB")
        prepared = ImageEnhance.Contrast(prepared).enhance(1.12)
        return prepared.filter(ImageFilter.SHARPEN)

    def zoom_tiles(self, image: Image.Image, max_tiles: int = 4) -> list[Image.Image]:
        if self.module and hasattr(self.module, "make_zoom_tiles"):
            return list(self.module.make_zoom_tiles(image, max_tiles=max_tiles))
        width, height = image.size
        overlap = 0.08
        boxes = [
            (0, 0, int(width * (0.55 + overlap)), int(height * (0.55 + overlap))),
            (int(width * (0.45 - overlap)), 0, width, int(height * (0.55 + overlap))),
            (0, int(height * (0.45 - overlap)), int(width * (0.55 + overlap)), height),
            (int(width * (0.45 - overlap)), int(height * (0.45 - overlap)), width, height),
        ]
        return [image.crop(box) for box in boxes[:max_tiles]]
