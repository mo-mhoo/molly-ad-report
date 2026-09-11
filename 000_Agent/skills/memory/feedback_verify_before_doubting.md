---
name: feedback-verify-before-doubting
description: 使用者提到「之前做過/上週測過」某能力時，先查 session 記錄自行查證，不要要求使用者反覆自證
metadata: 
  node_type: memory
  type: feedback
  originSessionId: eb43eb65-7d04-4934-ac2b-ec02cd8ad3e7
  modified: 2026-08-17T02:24:33.213Z
---

當使用者說某件事「上週測過」「之前做過」而目前這個工作目錄/repo 找不到對應紀錄時，不要直接下結論說「沒有這個能力」或反覆用 AskUserQuestion 要求使用者選項證明——先用 `mcp__ccd_session_mgmt__list_sessions` / `search_session_transcripts` / `list_events` 查其他 session 的實際歷史，很多操作（例如用 Bash + Python 臨時腳本直接呼叫 Google Ads API mutate）是 ad-hoc 執行、沒有寫成專案內的固定檔案或 commit，只存在於某次 session 的 transcript 裡，光看 repo 程式碼和 git log 找不到。

**Why：** 2026-08-17 御熹堂 PMax 資產群組任務中，只憑 repo 內程式碼（`google_ads_fetcher.py` 只有查詢功能）跟 git log 就斷定「沒有 Google Ads 寫入工具」，反覆讓使用者確認、選選項證明，使用者明確表示「你就是 API 做的」後我還要求更多佐證，讓使用者感到被誤導、浪費時間爭辯已經發生過的事。事後查 session `local_57968cb2-5cdb-4550-8ff5-4bae73bf37b0`（「Pmax 廣告上傳」，2026-08-13）證實確實用 Google Ads Python client library + Bash 執行過 AssetGroupService mutate 呼叫直接編輯資產群組的標題/說明/圖片，是真的 API 寫入，只是沒有留下持久化的程式檔案。

**How to apply：** 遇到使用者提「之前做過 X」而我在目前上下文裡找不到證據時，先用 session 搜尋工具查證（通常很快），查到就直接承認並沿用同樣方法繼續，不要讓使用者重複舉證；查不到才誠實告知「我查了 session 記錄沒找到對應紀錄，能不能提供更多線索」。這個原則不限 Google Ads，任何「repo 找不到但使用者說做過」的情境都適用。

相關：[[project_google_ads_integration]]、[[feedback_meta_ads_write_safety]]
