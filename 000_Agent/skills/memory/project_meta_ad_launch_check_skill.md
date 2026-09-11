---
name: project-meta-ad-launch-check-skill
description: meta-ad-launch-check skill(上稿前巡檢+自動啟動)已從Google Drive備份重建進這台Mac,meta-ad-copy execute完會自動接它
metadata:
  node_type: memory
  type: project
---

2026-09-11 發現這台 Mac 完全沒有 `meta-ad-launch-check` skill——這是 Windows 那邊 `meta-ad-copy` execute 成功後**自動**接的「上稿前巡檢」：檢查審核狀態(DISAPPROVED)、adset/campaign 是否 ACTIVE、預算欄位是否正常，通過就直接啟動（非活動廣告改 ACTIVE、活動廣告確認走期 routine 已就緒），並推播卡片式結果到「廣告軍團-Meta 上傳素材」Google Chat 空間。這是 2026-08-28 Molly 就已經同意的行為（見 skill 內文），只是這台 Mac 一直沒有這個 skill。

**起因**：Molly 給我看 Chat 空間截圖，問「他的還帶有格子的樣式」（想要卡片式推播），一路查下去才發現不只是格式問題，是整個 `meta-ad-launch-check` skill 都不存在於這台 Mac——這正好呼應 [[feedback_git_sync_setup_2026_09_11]] 提到的「2026-08-31～2026-09-11 之間 Windows 建的 skill/記憶，這台 Mac 尚未同步的已知缺口」，用同一招（去 Google Drive 的 `~/.claude` 快照備份找）救回 SKILL.md 本身。**但 `check_new_ad_launch.py`／`notify_ad_squad.py` 這兩支 Windows 端的實際 Python 腳本沒有備份到 Drive**，只能用 Meta Ads MCP 手動比照判斷邏輯重寫（見 [[feedback_meta_ad_copy_windows_tool_unavailable_mac]] 同樣的解法）。

**已完成（2026-09-11）**：
1. `000_Agent/skills/meta-ad-launch-check/SKILL.md` 已重建，邏輯改用 Meta Ads MCP（`ads_get_ad_entities` 查 ad/adset/campaign 狀態、`ads_activate_entity` 啟動），推播改用 `send_chat_notification.py`。
2. `send_chat_notification.py` 整支改寫：從純文字訊息改成 **Google Chat Cards v2 卡片格式**（`--file` 現在吃 JSON 陣列，不是純文字），視覺上跟 Windows 端 `notify_ad_squad.py` 送出的樣式對齊（粗體標題行＋分隔線＋內文），已用測試卡片實際送過一次到 Chat 空間讓 Molly 確認過樣式 OK。
3. `meta-ad-copy/SKILL.md` 第 7 步已改成「execute 完自動交給 meta-ad-launch-check 處理，不用自己另外推播」，取代原本 2026-09-11 稍早才加上的「自己直接推文字訊息」做法——避免同一批廣告被推播兩次。

**行為上的重要改變**：之前這台 Mac 上，meta-ad-copy 建的**非活動（常態）廣告**預設維持 PAUSED、由 Molly 自己決定何時開。現在因為接上 meta-ad-launch-check，**只要巡檢通過，常態廣告也會被自動改成 ACTIVE，不再是預設 PAUSED 等她確認**——這是 Molly 2026-09-11 主動要求「順便把 meta-ad-launch-check 重建進來」時已經知情同意的改變（她原本就知道 Windows 版是這樣運作）。

**How to apply**：之後 `/meta-ad-copy` 上稿完，直接照新流程走（execute → 走期排程 → meta-ad-launch-check 巡檢+啟動+推播），不用再問 Molly 要不要開常態廣告、也不用自己另外組訊息推播。如果巡檢發現 issues（DISAPPROVED 等），還是維持 PAUSED 並列出來讓她決定，這條沒有變。

相關：[[feedback_meta_ad_copy_autopush_chat]]（這則已經過時的部分是「自己推播」那段，已被這則取代）、[[feedback_git_sync_setup_2026_09_11]]。
