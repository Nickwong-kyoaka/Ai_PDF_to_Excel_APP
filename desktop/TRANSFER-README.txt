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
   - Context length: 16384
   - Flash Attention: enabled
   - KV cache: system RAM
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

8. Choose an output folder and start scanning. A label such as Study-A creates Study-A_FormSight.xlsx.
   選擇輸出資料夾並開始掃描；例如 Study-A 標籤會建立 Study-A_FormSight.xlsx。

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

- Models are called sequentially. Context-heavy requests automatically retry with a smaller output budget.
  模型只會順序呼叫；超出 context 時會自動降低輸出 token 再試。
- Failed pages retry with a smaller image and then become visible flags without stopping the batch.
  失敗頁面會用較小圖片再試；仍失敗則標記，但不會停止整個批次。
- Completed pages and partial series Excel files are checkpointed. Reopening the app offers recovery.
  已完成頁面及系列 Excel 會持續保存；重新開啟程式時可恢復未完成批次。

No Python, Node.js, API URL, API token, web server, or YOLO installation is required.
不需安裝 Python、Node.js、API 網址、API token、網頁伺服器或 YOLO。

Local working data is stored under %LOCALAPPDATA%\FormSight Local and is purged after 30 days.
本機工作資料儲存於 %LOCALAPPDATA%\FormSight Local，並於 30 天後清除。
