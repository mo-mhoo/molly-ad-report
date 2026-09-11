---
name: atl-quanmao-siliao-bid-review
description: 覆盤毛孩時代主帳號「19.全貓飼料 ATL」預算+出價上限調整成效
---

背景：2026-08-21 在毛孩時代主帳號（Google Ads customer_id 1014276621）對活動「19.全貓飼料-Blog_全貓飼料系列_ATL」（campaign.id 21299859589）做了兩個調整，因為診斷發現這個活動連原本的日預算都花不滿（過去14天 search_budget_lost_impression_share 全部是0.0），瓶頸是出價排名（search_rank_lost_impression_share 約70~73%），不是預算：
1. 日預算 7000 → 10000 TWD（campaignBudgets/13586503359）
2. Target Spend 出價策略的 CPC 出價上限 14 → 18 TWD（campaign.target_spend.cpc_bid_ceiling_micros）

調整前 14 天基準（2026-08-07~2026-08-20）：日均花費約 3550~4780 TWD、平均CPC約13.25~13.65 TWD、search_rank_lost_impression_share約70~73%、search_impression_share約27~32%、search_absolute_top_impression_share固定約9.99%、CTR約7~8.8%。

任務：用專案 C:\AI小摸 的 google_ads_fetcher.py（讀取 C:\AI小摸\.env 的 Google Ads API 憑證：GOOGLE_ADS_DEVELOPER_TOKEN / GOOGLE_ADS_CLIENT_ID / GOOGLE_ADS_CLIENT_SECRET / GOOGLE_ADS_REFRESH_TOKEN / GOOGLE_ADS_LOGIN_CUSTOMER_ID）直接查 Google Ads API（customer_id=1014276621, campaign.id=21299859589），比較 2026-08-21 起至今這段期間 vs 調整前14天基準的：
- 日花費趨勢（有沒有真的花得比較多，逐步逼近或超過10000預算）
- 平均CPC（有沒有因為上限拉高而上升）
- search_rank_lost_impression_share / search_impression_share / search_absolute_top_impression_share（排名損失有沒有下降、曝光份額有沒有提升）
- search_budget_lost_impression_share（確認預算是否仍未卡住）
- clicks、conversions、conversions_value、CTR（成效有沒有跟著提升，不是只有花費變多）
- 明確列出比較的實際日期區間（調整前 vs 調整後），不要用「最近」這種模糊字眼

輸出：診斷「出價上限拉高是否有效解決排名瓶頸」，並給下一步建議（例如：效果不明顯要不要再拉高上限、或改用 Maximize Conversions/tCPA 等自動出價策略；效果好的話這個模式可以套用到其他類似「預算沒花完但排名損失高」的ATL活動）。如果本次沒有 Google Ads MCP connector 或查詢失敗，誠實列出檢查清單、不要編數字。

這是本機排程任務（依賴本機 .env 憑證），結果留在本機/雲端 session 讓 Molly 自己回來看，不自動推播、不自動再次寫入帳戶調整任何設定。