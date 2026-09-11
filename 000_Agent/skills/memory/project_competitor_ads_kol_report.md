---
name: project-competitor-ads-kol-report
description: 毛孩時代競品/同品類KOL Meta廣告分析 — 固定Artifact連結，用 competitor-ads-update skill 更新
metadata: 
  node_type: memory
  type: project
  originSessionId: ad576c12-3c34-4631-93a8-0e3fa391dc1d
  modified: 2026-08-12T10:31:16.201Z
---

毛孩時代競品（汪喵星球/木入森/嬌寵醫生）＋同品類素人/KOL的 Meta 廣告分析報告，固定發布在 Artifact：`https://claude.ai/code/artifact/0ad20126-e712-47e2-9e87-19c296313036`。

**Why**：原始版本（2026-07-09抓取）是外部顧問用完整CSV人工/演算法分類做的358則深度報告，供 Cyber 顧問會議用。2026-08-12 起改用 Meta Ads Library API 即時查詢維護更新版，方法論改為「連結標題文字關鍵字粗判」，樣本量受API限制（每品牌最多50則、無分頁），準確度低於原版，報告內附警示區塊說明差異。

**How to apply**：Molly 說「更新競品分析」「查一下競品廣告」等指令時，觸發 [[competitor-ads-update]] skill（`C:\Users\Molly Ho\.claude\skills\competitor-ads-update\SKILL.md`），會自動查品牌+KOL現況、跟前一版比對變化、republish回同一個Artifact連結（不要發布成新連結）。嬌寵醫生目前(2026-08-12)查無投放中廣告，粉專是 facebook.com/ProudPet.TW，待每次更新時重新確認。
