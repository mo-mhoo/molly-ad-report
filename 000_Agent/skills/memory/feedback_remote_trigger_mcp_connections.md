---
name: feedback-remote-trigger-mcp-connections
description: "RemoteTrigger(排程雲端agent)建立routine時,mcp_connections留空會自動掛上所有已連接connector,不代表沒連接"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 48842bc0-5c83-4d29-9d78-691e9f1847e6
  modified: 2026-07-31T01:21:55.654Z
---

用 `/schedule` 或 RemoteTrigger 建立雲端排程 routine 時,skill 載入當下給的「已連接 MCP connector 清單」可能是不準/過期的(即使重新載入 skill 也一樣),尤其是自訂(Custom)connector 像 Meta Ads MCP 這種,清單可能顯示「沒有連接」但其實帳號上已經連好了。

**正確驗證方式**:`mcp_connections` 是 create body 的可選欄位,省略它不代表 routine 沒有 MCP 存取權——系統會自動把使用者當下所有已連接的 connector 全部掛到新建的 routine 上。要確認實際掛了哪些,直接送一次不帶 `mcp_connections` 的建立請求,從回傳結果裡的 `mcp_connections` 陣列讀出實際結果(裡面會有 connector_uuid、name、url),比先入為主相信 skill 給的清單、或叫使用者去 UI 裡手動找 connector_uuid 快很多。

**Why:** 2026-07-31 幫御熹堂官網排一個回頭查廣告數據的routine,skill兩次都顯示「No connected MCP connectors found」,導致繞了一大圈:先問使用者要不要連connector、叫使用者去瀏覽器找UUID、甚至嘗試用Claude Browser工具去登入查(失敗,沒有使用者session)。最後省略mcp_connections欄位直接建立,才發現Meta Ads MCP、Google Calendar、Gmail、Google Drive等全部已經連著,系統自動全掛上去了。

**How to apply:** 之後遇到需要幫排程routine掛connector的情境:
1. 先不要相信skill載入時顯示的connector清單是準的,尤其是自訂/Custom類型的connector
2. 直接嘗試省略`mcp_connections`欄位建立routine,從API回傳結果讀實際掛載狀況,再決定要不要用update調整
3. 如果回傳的connector不是想要的那個,才需要再手動指定connector_uuid(這時才需要使用者去UI裡找,或從其他已知的相同connector_uuid的工具名稱反推,例如本地session若已有`mcp__<uuid>__xxx`工具在用,那個uuid就是同一個connector的uuid)
