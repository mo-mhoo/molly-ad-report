---
name: feedback-meta-ad-copy-adset-via-mcp
description: meta-ad-copy skill 判斷/查詢建議 adset 要用 Meta Ads MCP，不能只靠本機 meta_ad_copy_tool.py 的 /api/campaign_adsets
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 73a775f4-a70c-486a-a24c-4b283b6176c5
  modified: 2026-08-19T08:44:55.517Z
---

[[meta-ad-copy]] skill 在 plan 階段若沒帶出 target_adset_id 候選（例如本機 Flask 工具 `/api/campaign_adsets` 因為 Meta API rate limit 失敗），要改用 Meta Ads MCP（`mcp__0d74ec43-1f83-4839-b189-d6ae29575565__ads_get_ad_entities`）直接查該 campaign 底下的 adset 清單，而不是回頭跟 Molly 要 adset ID 或乾等本機工具重試。

**Why**: Molly 2026-08-19 明確糾正——這個 skill 本來就該透過 MCP 判斷給建議 adset，不是本機工具卡住就直接拋回去問她。

**How to apply**: 查詢方式是 `level="adset"`，`filtering=[{"field":"adset.campaign_id","operator":"EQUAL","value":["<campaign_id>"]}]`，`fields=["id","name","status","effective_status"]`，`ad_account_id` 用不帶 `act_` 前綴的數字帳號 ID。每次對話對這個 MCP 的呼叫要帶同一個 20 碼 `client_conversation_id`。查到多個 adset 時才需要列出來給 Molly 選；只有一個 ACTIVE adset 時可以直接採用、在回報表格裡註明即可，不用另外多問一輪。
