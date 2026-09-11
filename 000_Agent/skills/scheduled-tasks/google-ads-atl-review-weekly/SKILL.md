---
name: google-ads-atl-review-weekly
description: 每週一早上9點覆盤 Google Ads ATL 活動成效，輸出診斷＋建議暫停/新增關鍵字清單
---

執行 google-ads-atl-review skill（用 Skill 工具呼叫 `google-ads-atl-review`），對預設範圍（毛孩時代官網含部落格子帳號、御熹堂）跑一次 Google Ads ATL 活動覆盤。

比較區間預設近7日 vs 前7日、近14日 vs 前14日兩組（依 skill 內建邏輯）。

輸出完整診斷報告（帳戶層趨勢、零轉換花費關鍵字表、建議暫停/新增清單、GA4交叉檢查、landing page提醒），直接留在這個 session 裡，不要自動推播到 Google Chat 或任何外部管道，也不要執行任何帳戶寫入（暫停/新增關鍵字）——那些都要等 Molly 自己回來看過、明確確認後才能動手。

如果查詢過程中遇到帳號/憑證問題（例如某品牌 conversion_action_name 或 GA4 property id 尚未確認），照 skill 裡的指示誠實列出待確認事項，不要編數字。