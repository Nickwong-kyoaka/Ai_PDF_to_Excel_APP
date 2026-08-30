FORMSIGHT LOCAL — DESTINATION PC QUICK START
FormSight 本機版 — 目標電腦快速安裝

This transfer package contains the Windows installer. Model files are not included.
此傳輸套件包含 Windows 安裝程式，但不包含模型檔案。

DESTINATION PC / 目標電腦

1. Install LM Studio 0.4 or newer.
   安裝 LM Studio 0.4 或更新版本。

2. Download and load both Q4 vision models in LM Studio:
   在 LM Studio 下載並載入以下兩個 Q4 視覺模型：
   - qwen/qwen3-vl-8b (primary extraction and final judge / 主要擷取及最終判斷)
   - google/gemma-3-4b (independent verifier / 獨立驗證)

3. Recommended RTX 5060 Ti 16 GB settings for both models:
   RTX 5060 Ti 16 GB 建議兩個模型均使用：
   - Context length: 8192 to 12288
   - Flash Attention: enabled
   - GPU offload: all model layers
   - KV cache: GPU/Auto if both models fit; system RAM only if necessary
   Keep both models loaded. FormSight sends requests sequentially, never in parallel.
   保持兩個模型已載入；FormSight 只會順序呼叫，不會平行執行。

4. Start the LM Studio local server on loopback (127.0.0.1), with authentication off.
   啟動 LM Studio 本機伺服器，使用 127.0.0.1 並關閉驗證。

5. Verify FormSight-Local-Setup.exe against SHA256SUMS.txt, then run it.
   依 SHA256SUMS.txt 核對安裝程式，再執行 FormSight-Local-Setup.exe。

6. Launch FormSight Local. Wait for both green readiness cards, then drag in files or folders.
   啟動 FormSight 本機版；等待兩個狀態卡顯示綠色，再拖入檔案或資料夾。

7. Select related file rows and click Set Series Label. Every PDF with the same label is combined
   into one Excel workbook, even when one PDF contains one questionnaire and another contains many.
   選取相關檔案列並按「設定系列標籤」。所有相同標籤的 PDF 會合併至同一 Excel；
   每個 PDF 可分別包含一份或多份問卷。

8. Choose an output folder. Keep the default Automatic one-take + Balanced choices, then click Start once.
   The app continues through grouping, scanning, checking, recovery, and Excel output without another click.
   選擇輸出資料夾，保留預設「全自動一次完成」及「平衡模式」，然後只按一次開始；
   程式會自動完成分組、掃描、合理性檢查、錯誤恢復及 Excel 輸出，中途毋須再按鍵。

SPEED PROFILES / 速度模式

- Balanced (recommended): Qwen scans every page; Gemma checks uncertain/corrected/matrix pages and
  a 10% audit sample. Conflicts on one page are judged together.
  平衡模式（建議）：Qwen 掃描每頁；Gemma 只檢查可疑、更正、矩陣頁及 10% 抽查頁；同頁衝突一次判斷。
- Maximum accuracy: both models inspect every page. This is slower and should be used only when required.
  最高準確度：每頁均由兩個模型檢查，速度較慢，只在需要時使用。

PAGE-SERIES REVIEW / 問卷系列分組

- Work through one PDF at a time with Previous/Next.
  使用「上一個／下一個」逐一檢查 PDF。
- Choose one questionnaire, one questionnaire per page, or a fixed pages-per-questionnaire series.
  可選整份一個問卷、每頁一個問卷，或指定每份問卷頁數。
- Copy a page pattern to other PDFs with the same page count.
  可將分組模式複製到頁數相同的其他 PDF。
- Auto-fill participant IDs and confirm the green complete-coverage message.
  可自動填寫參加者編號；確認頁面完整覆蓋提示為綠色。

LONG-RUN SAFETY / 長時間執行保護

- Models are called sequentially. Balanced requests stop after 120 seconds instead of waiting repeatedly;
  context-limit responses immediately retry with a smaller output budget.
  模型只會順序呼叫；平衡模式每次請求最多 120 秒，不會長時間重複等待；超出 context 時會立即降低輸出 token 再試。
- Failed pages retry with a smaller image and then become visible flags without stopping the batch.
  失敗頁面會用較小圖片再試；仍失敗則標記，但不會停止整個批次。
- Completed pages and partial series Excel files are checkpointed. Reopening the app offers recovery.
  已完成頁面及系列 Excel 會持續保存；重新開啟程式時可恢復未完成批次。

No Python, Node.js, API URL, API token, web server, or YOLO installation is required.
不需安裝 Python、Node.js、API 網址、API token、網頁伺服器或 YOLO。

Local working data is stored under %LOCALAPPDATA%\FormSight Local and is purged after 30 days.
本機工作資料儲存於 %LOCALAPPDATA%\FormSight Local，並於 30 天後清除。
