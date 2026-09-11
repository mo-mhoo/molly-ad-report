---
name: feedback-meta-report-creative-level
description: 「整理/看 Meta 成效」預設要做到素材(ad)層級分析，不能只停在 campaign/ad set 層級
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 18feae50-a15e-4315-8bd9-a7fa64514e0e
  modified: 2026-08-11T00:54:53.770Z
---

2026-08-11，幫毛孩時代蝦皮 CPAS 帳號做「html 整理...meta 成效」，只抓了 campaign level 數據做 ATL/BTL 整理，用戶回饋「整理得不好，沒有素材的分析，只是一直看 ad set」——明確要求成效報告要往下切到素材（ad/creative）層級，不能停在活動層級就當作「整理成效」。

**Why:** 投手真正要判斷的是「哪支素材該留、該汰換、該加碼」，campaign/ad set 層級的加總看不出素材差異，對優化決策沒有實際幫助。

**How to apply:**
- 之後只要用戶說「整理/看/分析 Meta 成效」（沒有明講只要帳號或活動層級摘要），預設要用 `ads_get_ad_entities`（level=ad）抓到素材層級，不要只做到 campaign/adset 就交件。
- 聚合單位是「素材名稱」不是 ad id，見 [[feedback_creative_report_template]] 的規則（同一素材常掛多個 ad set/campaign，要先按素材名稱分組加總再算 CTR/CPA/ROAS）。
- ATL/BTL 判斷邏輯、互動率 vs CTR 的差異，套用 [[feedback_atl_btl_testing_logic]]。
- 若用戶要「看圖」的素材報告（帶縮圖），才套用 [[feedback_creative_report_template]] 完整抓圖流程；若只要數字分析，素材層級表格＋排名即可，不必每次都抓圖，除非用戶要求或素材數量少到值得配圖。
- campaign/ad set 層級摘要可以當作報告的「上層總覽」，但不能是唯一內容，素材排名/汰換建議才是重點。

相關：[[project_ads_optimization_training]]、[[project_meta_insights_report]]
