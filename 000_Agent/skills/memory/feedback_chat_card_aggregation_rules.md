---
name: feedback-chat-card-aggregation-rules
description: send_chat_notification.py的卡片要依素材聚合、依帳號/通路合併成單一則訊息,不要素材或批次各自分開推播
metadata:
  node_type: memory
  type: feedback
---

推播到「廣告軍團-Meta 上傳素材」Chat 空間的卡片格式，2026-09-11 被 Molly 看過實際推播結果後糾正了兩次，都是「聚合層級不對」：

**第一次糾正：同一支素材鋪多個 adset，要聚合成一張卡片，不要一個 adset 一張卡**。原本每個 placement（素材＋adset 組合）都各自建一張完整卡片（標題重複出現好幾次），改成同一支素材（`content_type`＋`account_name`＋`ad_name`＋`build_method` 都相同）合併成一張卡片，標題只出現一次，底下依序列出每個 adset 的 placement，中間用細分隔線（`- - -`）隔開。`send_chat_notification.py` 的 `group_entries()` 已經處理這個分組邏輯。

**第二次糾正：同一個帳號/通路，當天不管分幾次 execute，都要合併成一則 Chat 訊息**，不要因為分批建廣告就分批推播。原本御熹堂官網的 13 支活動 + 3 支常態 UC2 是兩次不同時間點的 execute，我各自推了一則訊息（共 2 則），Molly 說「而且是一則chat / 你覺得一個通路要同一則chat」——同一個帳號當天的所有廣告要併成一份 JSON、一次 `--send` 呼叫。momo 是不同帳號，維持獨立一則沒問題。

**額外的格式對齊**（Molly 貼了另一個工具在同一空間推播的截圖當參考）：
- 標題不要拆成 3 行分開粗體，改成「一行小字『廣告內容｜類型』+ 一行粗體『帳號｜廣告名稱｜建立方式：...』」
- 狀態欄位的標籤視情境可以是「巡檢結果」而不是「狀態」——`placement_lines()` 支援用 `status_label` 欄位覆寫預設的「狀態」文字，巡檢類訊息（來自 meta-ad-launch-check）建議填 `"status_label": "巡檢結果"`、`"status": "pass，無warnings，已ACTIVE"` 這種組合格式。

**How to apply**：以後 `/meta-ad-copy` 或 `meta-ad-launch-check` 要推播時：
1. 先確認這次要推的是哪個帳號/通路，同帳號當天已經推過的內容要合併進同一份 JSON 重推（不是疊加新訊息）。
2. JSON 陣列裡同一支素材的多個 adset placement 直接照順序放，不用手動分組，`group_entries()` 會自動處理。
3. 巡檢類訊息用 `status_label: "巡檢結果"`，一般上稿類訊息可以留預設「狀態」。

相關：[[project_meta_ad_launch_check_skill]]、[[feedback_meta_ad_copy_autopush_chat]]。
