---
name: project-ads-optimization-training
description: 目標 2026-08 前讓 AI 數位廣告優化/分析判斷達到人類90分標準，持續累積回饋
metadata: 
  node_type: memory
  type: project
  originSessionId: 44f0b925-b992-4e2b-957c-8adf1b0d3b8d
---

用戶設定目標：2026-08 前，AI 在「數位廣告優化與分析」判斷上要達到人類資深投手 90 分水準。

**Why:** 用戶（TSA Group 廣告投手）想讓 AI 分身能獨立做出可信的優化建議，逐步減少人工複核。

**How to apply:** 採用持續回饋訓練法——每次 AI 給出優化建議後，請用戶回報「採納/不採納/結果如何」，AI 立即將落差記錄為 [[feedback_ads_optimization]] 中的具體原則（錯誤判斷 → 正確判斷 → 原因）。每隔一段時間（建議每2-4週）回顧累積的 feedback，看是否該升級進 [[anthropic-skills:tsa-meta-ads-operations]] skill 規則。

## 追蹤方式
- 每次優化建議後主動問：「這個建議你會採納嗎？實際結果如何？」
- 回饋記錄存到 `feedback_ads_optimization.md`
- 8月前定期（建議7月中）回顧一次差距

## 待辦
- 尚未做基準測試（拿過去真實案例對比 AI vs 人類判斷）
