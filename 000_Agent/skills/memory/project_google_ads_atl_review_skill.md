---
name: project-google-ads-atl-review-skill
description: google-ads-atl-review skill 已建立（診斷+建議清單，不寫入），每週一9點本機排程自動跑一次
metadata: 
  node_type: memory
  type: project
  originSessionId: fb4f602a-66bd-48e0-aa9b-9da3c52d9a4f
  modified: 2026-08-19T02:11:10.609Z
---

2026-08-19 建立 `google-ads-atl-review` skill（`~/.claude/skills/google-ads-atl-review/SKILL.md`），
用於定期覆盤 Google Ads ATL 活動：花費/真實購買轉換數/ROAS 趨勢、零轉換花費關鍵字（套用
[[feedback_google_ads_atl_keyword_optimization]] 的意圖字尾方法論＋營收交叉核對）、關鍵字生效狀態驗證、
GA4 商品整體營收交叉檢查。**只出診斷+建議暫停/新增清單，不在 skill 裡執行任何帳戶寫入**——真的要調整
時仍走既有方法論的手動流程（先測試1+1、API驗證）。

**Why：** Molly 要求要有一個定期覆盤機制，避免關鍵字優化做完後沒人回頭追蹤成效，也避免每次覆盤都要
重新手動兜資料/重講一次判斷邏輯。

**How to apply：**
- 已建立本機排程任務 `google-ads-atl-review-weekly`（`mcp__scheduled-tasks__create_scheduled_task`，
  儲存在 `C:\Users\Molly Ho\.claude\scheduled-tasks\google-ads-atl-review-weekly\SKILL.md`），每週一
  09:02 本機時間自動觸發，結果留在雲端/本機 session 裡讓 Molly 自己回來看，**不自動推播、不自動寫入
  帳戶**。這是本機排程（不是 RemoteTrigger 雲端 routine），因為 skill 依賴 `C:\AI小摸\.env` 的 Google
  Ads API 憑證，細節與教訓見 [[feedback_local_vs_cloud_scheduling]]。
- 排程只在本機 Claude Code app 開著時才會觸發，app 關著時要等下次開啟才補跑。
- Skill 預設範圍是毛孩時代（合併主帳號 `1014276621`＋部落格子帳號 `5079367835`）＋御熹堂
  `8669832537`；御熹堂的「真實購買」conversion_action_name 跟 GA4 property id 目前都還沒確認過，
  第一次跑到御熹堂時要先查出來，回填到 skill 的 `references/account_ids.md`／`references/ga4_cross_check.md`。
- Skill 底下的 `scripts/queries.py` 是純讀取查詢（活動趨勢、search_term_view 零轉換清單、關鍵字營收
  查詢、三層 status 驗證），跟專案根目錄的 `google_ads_fetcher.py` 不重疊，之後如果要擴充覆盤邏輯，
  優先改這個 skill 的 scripts，不要動 `google_ads_fetcher.py`（那支是給 `app.py` 通用維度拆解用的）。

相關：[[project_google_ads_integration]]、[[feedback_google_ads_atl_keyword_optimization]]、
[[feedback_local_vs_cloud_scheduling]]
