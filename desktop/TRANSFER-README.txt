FORMSIGHT LOCAL — DESTINATION PC QUICK START
FormSight 本機版 — 目標電腦快速安裝

This transfer package contains the Windows installer. Model files are not included.
此傳輸套件包含 Windows 安裝程式，但不包含模型檔案。

A separate FormSight-Local-Portable.exe release can be run directly without installation or administrator rights.
另有獨立的 FormSight-Local-Portable.exe，可直接執行，不需安裝或管理員權限。

DESTINATION PC / 目標電腦

1. Install LM Studio 0.4 or newer.
   安裝 LM Studio 0.4 或更新版本。

2. Download and load these Q4 vision models in LM Studio (the verifier is recommended but optional):
   在 LM Studio 下載並載入以下 Q4 視覺模型（建議使用驗證模型，但不強制）：
   - qwen/qwen3-vl-8b (primary extraction and fallback reasonableness judge / 主要擷取及後備合理性判斷)
   - google/gemma-3-4b (independent verifier / 獨立驗證)
   - Optional: qwen/qwen3-8b text model (preferred flag-only reasonableness judge if VRAM/RAM permits /
     可選：如顯示記憶體／記憶體許可，作為較佳的只標記合理性判斷模型)

3. Recommended RTX 5060 Ti 16 GB settings for both models:
   RTX 5060 Ti 16 GB 建議兩個模型均使用：
   - Context length: 8192 to 12288
   - Flash Attention: enabled
   - GPU offload: all model layers
   - KV cache: GPU/Auto if both models fit; system RAM only if necessary
   Keep the selected models loaded. FormSight smoke-tests image support and strict JSON first,
   then sends requests sequentially, never in parallel.
   保持所選模型已載入；FormSight 會先測試視覺及 JSON 能力，然後只會順序呼叫。

4. Start LM Studio with authentication off. If it is on this PC, use loopback (127.0.0.1).
   If it is on another trusted GPU PC, enable LAN serving and restrict its firewall to this PC/VPN.
   啟動 LM Studio 並關閉驗證。同一電腦請使用 127.0.0.1；若使用另一部可信 GPU 電腦，
   請啟用 LAN 服務，並在防火牆只允許本電腦或 VPN 存取。

5. Verify FormSight-Local-Setup.exe against SHA256SUMS.txt, then run it.
   依 SHA256SUMS.txt 核對安裝程式，再執行 FormSight-Local-Setup.exe。

6. Launch FormSight Local. Keep Auto-detect, or enter the exact private server address, for example
   192.168.1.50:1234 or gpu-pc:1234, then click Refresh. Wait for the expected models to appear in green.
   啟動 FormSight 本機版。可保留自動偵測，或輸入指定私人伺服器位址（例如
   192.168.1.50:1234 或 gpu-pc:1234），再按重新偵測並確認綠色狀態顯示正確模型。

7. Select related file rows and click Set Series Label. Every PDF with the same label is combined
   into one Excel workbook, even when one PDF contains one questionnaire and another contains many.
   選取相關檔案列並按「設定系列標籤」。所有相同標籤的 PDF 會合併至同一 Excel；
   每個 PDF 可分別包含一份或多份問卷。

8. Choose an output folder. Keep the default Automatic one-take + Balanced choices, then click Start once.
   The app continues through grouping, scanning, checking, recovery, and Excel output without another click.
   選擇輸出資料夾，保留預設「全自動一次完成」及「平衡模式」，然後只按一次開始；
   程式會自動完成分組、掃描、合理性檢查、錯誤恢復及 Excel 輸出，中途毋須再按鍵。

OPTIONAL FOCUS EXTRACTION / 可選重點提取

- Choose "Auto-group, then circle focus areas" to inspect corresponding pages from questionnaire 1 and 2.
  Draw one or more boxes around answer areas; those normalized boxes are reused for matching questionnaires
  and matching PDFs in the same labelled series. Only the compact crop sheet is sent to the vision models.
  選擇「自動分組，再從首兩份問卷圈選重點」即可比較第 1、2 份問卷的相同頁面；
  圈選一個或多個答案區後，程式會套用至同標籤、同版式的其他問卷及 PDF，並只傳送重點裁切圖。
- Switch between both samples before saving. Use Undo to remove the last box. "Use full page" clears
  the current page's boxes; every unmarked page type automatically falls back to the full page.
  儲存前可切換兩個樣本；「復原」會移除上一個框；「使用整頁」會清除該頁重點框。
  所有未圈選頁面類型均會自動安全回退至整頁掃描。

SPEED PROFILES / 速度模式

- Balanced (recommended): Qwen scans every page; Gemma checks uncertain or overwritten answers,
  the first two calibration questionnaires, and a 10% audit sample.
  平衡模式（建議）：Qwen 掃描每頁；Gemma 只檢查可疑頁、頭兩份校準問卷及 10% 抽查。
- Higher accuracy: larger images and a 20% verifier audit. It is slower.
  較高準確度：使用較大圖像及 20% 驗證抽查，速度較慢。
- Qwen-only is an explicit model dropdown choice when no compatible verifier is loaded.
  如沒有兼容驗證模型，可在模型下拉選單明確選擇「僅 Qwen」。

PAGE-SERIES REVIEW / 問卷系列分組

- Automatic mode reads ranges such as 001-010, verifies the repeated page cycle, and safely skips
  uncertain files without stopping the rest of the series.
  自動模式會讀取 001-010 等範圍、驗證重複頁面週期，並安全跳過不確定檔案而不中斷其他處理。
- Work through one PDF at a time with Previous/Next.
  使用「上一個／下一個」逐一檢查 PDF。
- Choose one questionnaire, one questionnaire per page, or a fixed pages-per-questionnaire series.
  可選整份一個問卷、每頁一個問卷，或指定每份問卷頁數。
- Copy a page pattern to other PDFs with the same page count.
  可將分組模式複製到頁數相同的其他 PDF。
- Auto-fill participant IDs and confirm the green complete-coverage message.
  可自動填寫參加者編號；確認頁面完整覆蓋提示為綠色。
- Review page thumbnails, filename-inferred expected count, detected cycle, and confidence.
  檢查頁面縮圖、從檔名推斷的預期數量、頁面週期及信心。

LONG-RUN SAFETY / 長時間執行保護

- Normal requests stop after 90 seconds. Permanent 400/404/422 errors are not retried;
  malformed JSON is repaired from saved text without resending the page.
  一般請求最多 90 秒；永久性 400/404/422 錯誤不會重試；格式壞掉的 JSON 會從已儲存文字修復。
- Missing answer records receive at most one targeted crop repair; the whole page is not repeatedly resent.
  遺漏答案最多只會進行一次答案區域修復，不會重複傳送整頁。
- SQLite checkpoints every page and the partial Excel workbook is atomically refreshed after every questionnaire.
  SQLite 每頁儲存，部分 Excel 在每份問卷完成後原子更新。
- Reasonableness is flag-only: suggestions never replace the immutable scanner value.
  合理性檢查只會標記；建議永遠不會取代不可變更的掃描值。

No Python, Node.js, API token, web server, or YOLO installation is required. A server address is optional.
不需安裝 Python、Node.js、API token、網頁伺服器或 YOLO；伺服器位址只在需要時輸入。

Local working data is stored under %LOCALAPPDATA%\FormSight Local and is purged after 30 days.
本機工作資料儲存於 %LOCALAPPDATA%\FormSight Local，並於 30 天後清除。
