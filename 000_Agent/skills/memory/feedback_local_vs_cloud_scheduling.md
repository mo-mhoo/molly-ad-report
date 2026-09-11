---
name: feedback-local-vs-cloud-scheduling
description: 排程自動化要選對機制——需要本機憑證/skill檔案的任務用本機 scheduled-tasks，不能用雲端 RemoteTrigger routine
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fb4f602a-66bd-48e0-aa9b-9da3c52d9a4f
  modified: 2026-08-19T02:10:14.167Z
---

建排程自動化任務前，先判斷這個任務需不需要**本機檔案或憑證**（`.env` 裡的 API token、`C:\AI小摸` 專案程式、`~/.claude/skills/` 底下的 skill 檔案）。需要的話，一定要用**本機排程機制**（`mcp__scheduled-tasks__create_scheduled_task`），不能用 `/schedule` skill 走的 `RemoteTrigger`（雲端 CCR routine）。

**Why：** `/schedule` skill 建立的是雲端 Claude Code Cloud Runner（CCR）routine——每次觸發會在 Anthropic 雲端起一個獨立沙箱，只帶一份 GitHub repo 的 git checkout，**完全沒有本機檔案系統、`.env`、或 `~/.claude/skills/` 的存取權**。2026-08-19 建立 `google-ads-atl-review` 覆盤 skill 時，一開始照使用者要求走 `/schedule`（RemoteTrigger），中途才發現這個 skill 依賴 `C:\AI小摸\.env` 的 Google Ads API 憑證（`GOOGLE_ADS_DEVELOPER_TOKEN` 等，且 `.env` 本來就是 `.gitignore` 排除、不會進 git repo），雲端 routine 每週觸發都會因為讀不到憑證而失敗，是空跑。改用 `mcp__scheduled-tasks__create_scheduled_task`（本機排程，任務存在 `C:\Users\Molly Ho\.claude\scheduled-tasks\{taskId}\SKILL.md`，跑在本機 Claude Code app 裡）才解決。

**How to apply：**
- 任務會呼叫 `google_ads_fetcher.py`／`meta_fetcher.py`／`ga4_fetcher.py` 這類讀 `.env` 的專案腳本，或會用到某個只存在本機 `~/.claude/skills/` 底下的 skill → 用**本機排程**（`mcp__scheduled-tasks__*`）。既有的 `meta-flight-pause-*`、`kaohsiung-events-weekly-update` 等本機排程任務都是這個模式（SKILL.md 裡明講用 Bash 工具 cd 到 `C:\AI小摸` 執行）。
- 任務純粹是操作已連結的 MCP 連接器（例如已授權的 claude.ai connector）、或是純程式碼分析且不需要本機憑證 → 才適合用 `/schedule`（RemoteTrigger 雲端 routine）。
- 本機排程的限制：**只有在本機 Claude Code app 開著時才會觸發**，app 關著時要等下次開啟才補跑——這點要主動告知使用者，避免她以為排程一定準時執行。
- 建立本機排程任務時，prompt 要自包含（每次執行都是全新 session，沒有這次對話的記憶），且要明講「不自動推播到外部管道」「不自動執行任何帳戶寫入」這類安全邊界，比照 [[feedback_meta_ads_write_safety]]。

相關：[[project_google_ads_integration]]、[[feedback_activity_creative_cloud_routine]]（注意：那則記錄講的「用 Claude 雲端排程 routine」是指走期到期暫停這類**不需要本機憑證、且動作单純**的情境，跟這裡「需要本機 Google Ads API 憑證」的情境不衝突，判斷準則一樣是看任務需不需要本機檔案存取）
