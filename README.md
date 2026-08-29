# FormSight

FormSight is a private Chinese/English questionnaire scanner for Windows model PCs. A FastAPI service stores durable jobs and audit data, a single GPU worker runs Qwen through LM Studio and a custom YOLO mark detector, and a bilingual React interface lets operators confirm page groups and reviewers approve every model correction.

The repository also contains **FormSight Local**, a separate single-user PySide6 desktop edition. It has no login or web server, never asks for an API URL/token, automatically detects an already-running loopback LM Studio server, accepts multiple questionnaire files, and generates one corresponding Excel workbook for every selected input.

## FormSight Local desktop edition

On the destination Windows 10/11 x64 PC:

1. Install LM Studio 0.4+, start its local server with the default loopback-only/authentication-off settings, and load two Q4 vision models: `qwen/qwen3-vl-8b` as the primary and `google/gemma-3-4b` as the independent verifier. For a 16 GB RTX 5060 Ti, use a 16384-token context, enable Flash Attention, and keep the KV cache in system RAM for both.
2. Run `FormSight-Local-Setup.exe`. Python, Node.js, FastAPI, and an API key are not required on the destination PC.
3. Launch **FormSight Local**, drag in or select any number of PDF/PNG/JPEG/single-page TIFF questionnaires, choose an output folder, optionally review PDF page groups, and start the scan. Each selected file receives a separate `<source-name>_FormSight.xlsx`; multiple questionnaires detected inside the same PDF remain together in that PDF's workbook.

The page-series review works through one PDF at a time and provides one-document, one-page, and fixed-pages-per-questionnaire presets. A validated page-range pattern can be copied to every same-length PDF, participant IDs can be auto-numbered, and live validation prevents gaps or overlapping pages before scanning.

The local application uses only models already loaded in LM Studio and does not download, load, or unload them. It runs the Qwen primary pass and Gemma verifier pass sequentially—never in parallel—then uses Qwen again for cropped conflict adjudication and the reasonableness check. This provides model-family diversity without keeping a third model in memory. YOLO and ONNX Runtime are no longer part of the local installer; the web-server edition retains its separate optional YOLO pipeline.

Local working records and restart state are stored under `%LOCALAPPDATA%\FormSight Local` and expired questionnaire data is purged after 30 days. Every input workbook contains `Questionnaires`, `Long_Answers`, `Page_Extracts`, `Conflicts`, `Failed_Jobs`, `QA_Summary`, `Data_Analysis`, `Run_Log`, `Reasonableness`, and `Review_Audit`. A corrupted or failed input still receives its own failure workbook, successful inputs are not lost, and unresolved findings are labelled `COMPLETED — FLAGS PRESENT`.

To build the installer on a developer PC, install 64-bit Python 3.11 or 3.12 and Inno Setup 6, then double-click `Build-FormSight-Local.bat` or run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build-desktop.ps1
```

The repeatable build creates a PyInstaller one-folder application, wraps it as `release\FormSight-Local-Setup.exe`, and creates a transfer-ready `release\FormSight-Local-v0.3.0-Transfer.zip`. The ZIP contains only the installer, a bilingual quick-start guide, and its SHA-256 checksum. If the installer already exists, `Package-FormSight-Transfer.bat` can rebuild the ZIP without recompiling the app. LM Studio remains responsible for GPU inference, so the desktop installer does not bundle model weights, CUDA libraries, or ONNX Runtime.

Questionnaire text and images are untrusted data. The model gateway explicitly refuses document-borne instructions, disables tool use, and accepts only schema-validated JSON. LM Studio stays on `127.0.0.1`; browsers only connect to the HTTPS web application.

## What is implemented

- Named `admin`, `operator`, and `reviewer` accounts with Argon2id passwords, secure server sessions, CSRF protection, throttled login, ownership checks, and audit events.
- PDF, PNG, JPEG, and TIFF validation, password-protected PDF rejection, page previews, automatic participant-ID grouping, and mandatory manual group confirmation.
- Durable SQLite/WAL queue with one GPU worker, progress events, retry, cancel, restart recovery, and 30-day PII purge.
- Two-pass Qwen3-VL extraction, v14-compatible image enhancement/tiling when the original source is present, optional custom YOLO detection, deterministic fusion, cropped conflict tie-breaks, and a separate Qwen reasonableness stage.
- Immutable scanner values, clearly separated Qwen/YOLO/fused/judge/final values, reviewer accept/edit/revert actions, and gated finalization.
- ResultV2 JSON, backward-oriented Excel sheets, Reasonableness and Review_Audit sheets, and annotated evidence PDFs in draft and final variants.
- Admin pages for health, users, approved model profiles, safe validation rules, and a browser annotation workspace.
- Versioned YOLO dataset export, leakage checks, training/export tooling, and release-metric evaluation.

## Windows 10 quick start

### Recommended: one-click wizard

After cloning or extracting the repository on the GPU server, double-click `Deploy-FormSight.bat`. The wizard checks Python, Node.js, NVIDIA tooling, and Caddy; securely prompts for the administrator password and LM Studio token; asks for model IDs, hostname, and YOLO weights; writes the local configuration; installs dependencies; runs preflight checks; starts FormSight; and can register automatic startup and retention cleanup.

Install Python 3.11+, Node.js 22.13+, the tested NVIDIA driver, LM Studio 0.4+, and Caddy before running the wizard. LM Studio model files and accepted YOLO weights remain external deployment artifacts because they are too large and hardware-specific for Git.

### Manual installation

1. On the GPU server, clone this repository and enter it:

   ```powershell
   git clone https://github.com/Nickwong-kyoaka/Ai_PDF_to_Excel_APP.git FormSight
   Set-Location FormSight
   ```

2. Install Python 3.11+, Node.js 22.13+, an NVIDIA driver, LM Studio 0.4+, and optionally Caddy.
3. Run `powershell -ExecutionPolicy Bypass -File scripts\install.ps1 -WithML` for the complete YOLO runtime, or omit `-WithML` while developing without detector weights.
4. Edit `backend\.env`: change the bootstrap password before LAN use, add the LM Studio API token, and verify all model IDs.
5. In LM Studio, download/load the approved Qwen vision and judge models, require API-token authentication, bind only to `127.0.0.1:1234`, and leave CORS/MCP/tool access disabled.
6. Put accepted custom weights at `backend\models\questionnaire_marks.onnx`.
7. Run `scripts\preflight.ps1`, then `scripts\start.ps1`.
8. For LAN/VPN HTTPS, copy `Caddyfile.example` to `Caddyfile`, replace `formsight.internal` with the internal DNS name, run Caddy as the proxy, and trust its internal CA on managed client PCs.

The development UI uses `http://localhost:3000` and the API uses `http://127.0.0.1:8000`. Production users should use only the HTTPS proxy address.

Only the GPU server needs Python, CUDA/YOLO, and LM Studio. Other LAN/VPN PCs use FormSight entirely through a browser and must not connect directly to ports 1234, 3000, or 8000.

## Models and profiles

The default profile targets an RTX 5060 Ti with 16 GB VRAM and 32 GB RAM:

- extractor: `qwen/qwen3-vl-8b`, Q4_K_M, 32k context;
- reasonableness judge: `qwen/qwen3-8b`, Q4_K_M;
- verification: Maximum Accuracy, one active job.

Model IDs vary between LM Studio revisions. `scripts\model-check.ps1` lists missing IDs, and an administrator may register approved alternatives. A profile is snapshotted onto every job and cannot change while that job is processing. Larger A100/multi-GPU profiles can be added without changing scanner code.

The original `universal_questionnaire_lmstudio_extractor_v14_consensus_geometry.py` is bundled as a compatibility source configured by `FORMSIGHT_LEGACY_V14_PATH`. Safe enhancement and zoom helpers are reused when found; the server remains operational if an administrator intentionally removes it.

## YOLO data and release process

1. Upload representative English and Chinese questionnaires and use **Annotations** to draw tight boxes around `tick`, `cross`, `filled_mark`, `circle`, `underline_selection`, and `strikeout` marks.
2. Assign every participant/document to exactly one of train, validation, or held-out test. The exporter blocks cross-split source leakage.
3. From the project root, run:

   ```powershell
   backend\.venv\Scripts\python.exe tools\export_yolo_dataset.py datasets\marks-v1
   backend\.venv\Scripts\python.exe tools\train_yolo.py datasets\marks-v1\dataset.yaml
   ```

4. A mark class is experimental until it has at least 50 real held-out examples and reaches at least 95% precision and recall.
5. Build normalized gold ResultV2 JSON and compare the candidate with v14:

   ```powershell
   backend\.venv\Scripts\python.exe tools\evaluate_release.py candidate.json gold.json --baseline v14.json
   ```

The evaluator blocks release below 97% selection exact accuracy, 90% character accuracy, 98% automatic-correction precision when corrections exist, or a two-point selection improvement over v14.

## Operations

- `scripts\status.ps1`: process and endpoint status.
- `scripts\stop.ps1`: safely stops only processes recorded for this installation.
- `scripts\backup.ps1`: uses SQLite's online backup API; questionnaire files are intentionally excluded unless your approved PII backup policy includes them.
- `scripts\cleanup.ps1`: performs the 30-day purge immediately.
- `scripts\register-startup.ps1`: registers server startup and daily retention tasks; run from an elevated PowerShell session.
- API documentation: `/docs` on the backend address.

## Development validation

```powershell
backend\.venv\Scripts\python.exe -m pytest backend\tests
npm run lint
npm run build
```

Custom YOLO weights and real LM Studio models are deployment artifacts, not source-controlled files. Accuracy thresholds cannot be claimed until the held-out corpus satisfies the minimum counts and release evaluator.
