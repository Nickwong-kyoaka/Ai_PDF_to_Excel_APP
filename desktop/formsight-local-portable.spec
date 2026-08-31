# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files


ROOT = Path(SPECPATH).resolve().parents[0]
datas = [
    (str(ROOT / "universal_questionnaire_lmstudio_extractor_v14_consensus_geometry.py"), "."),
]
datas += collect_data_files("certifi")

a = Analysis(
    [str(ROOT / "desktop" / "main.py")],
    pathex=[str(ROOT)],
    binaries=[],
    datas=datas,
    hiddenimports=[
        "pydantic_settings",
        "sqlalchemy.dialects.sqlite",
        "PIL.ImageQt",
        "pymupdf",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "fastapi",
        "uvicorn",
        "starlette",
        "ultralytics",
        "onnxruntime",
        "nvidia",
        "torch",
        "torchvision",
    ],
    noarchive=False,
    optimize=1,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="FormSight-Local-Portable",
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
    version=str(ROOT / "desktop" / "portable-version.txt"),
    runtime_tmpdir=None,
)
