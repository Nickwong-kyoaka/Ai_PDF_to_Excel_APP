# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs


ROOT = Path(SPECPATH).resolve().parents[0]
datas = [
    (str(ROOT / "universal_questionnaire_lmstudio_extractor_v14_consensus_geometry.py"), "."),
]
weights = ROOT / "backend" / "models" / "questionnaire_marks.onnx"
if weights.exists():
    datas.append((str(weights), "models"))

binaries = collect_dynamic_libs("onnxruntime")
for package in (
    "nvidia.cublas",
    "nvidia.cuda_nvrtc",
    "nvidia.cuda_runtime",
    "nvidia.cudnn",
    "nvidia.cufft",
    "nvidia.curand",
    "nvidia.nvjitlink",
):
    try:
        binaries += collect_dynamic_libs(package)
    except Exception:
        # CPU fallback remains available when an intentionally CPU-only build environment is used.
        pass
hiddenimports = [
    "onnxruntime.capi._pybind_state",
    "pydantic_settings",
    "sqlalchemy.dialects.sqlite",
    "PIL.ImageQt",
    "pymupdf",
]
datas += collect_data_files("certifi")

a = Analysis(
    [str(ROOT / "desktop" / "main.py")],
    pathex=[str(ROOT)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["fastapi", "uvicorn", "starlette", "ultralytics", "torch", "torchvision"],
    noarchive=False,
    optimize=1,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FormSightLocal",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=os.environ.get("FORMSIGHT_BUILD_CONSOLE") == "1",
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="FormSightLocal",
)
