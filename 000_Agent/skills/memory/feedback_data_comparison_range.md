---
name: feedback-data-comparison-range
description: 跟用戶分析/呈現廣告數據時，一律先列出比較區間（例如「昨日 vs 過去7日均值」的實際日期範圍），才給結論
metadata:
  node_type: memory
  type: feedback
  originSessionId: aa932875-adab-4fc1-836e-6ee24066cdca
  modified: 2026-07-31T01:13:53.467Z
---

跟 Molly 分析或呈現廣告數據時，開頭一定要先講清楚「比較的是哪一段時間 vs 哪一段時間」（寫出實際日期範圍，不是只講「近期」「均值」這種模糊詞），再給判斷或結論。

**Why:** 2026-07-30，在 `meta-creative-check`（素材成效巡檢）skill 的資料呈現上，Molly 明確要求「在跟別人分析數據的時候，記得都要先列出比較區間」——這是廣告投手的基本溝通習慣，數據脫離時間區間就沒有意義，且主管/團隊看報表時第一件事就是確認資料涵蓋範圍。

**How to apply:**
- 任何跟數據相關的輸出（週報、日巡檢、素材判讀、月度總結等），第一行或最前面就要標明比較區間的實際日期，例如「比較區間：昨日（2026-07-30）vs 過去 7 日均值（2026-07-22 ~ 2026-07-28）」。
- 不只是給用戶看的訊息，推播到 Google Chat 之類外部可見的地方（見 [[feedback_meta_ads_write_safety]]）也要在標題/副標帶上區間，不能只寫日期不寫比較對象。
- 這條跟 [[project_ads_optimization_training]] 的判斷訓練也有關：任何異常/汰換判斷背後都隱含一個比較區間，先講清楚區間本身就是判斷可信度的一部分。
