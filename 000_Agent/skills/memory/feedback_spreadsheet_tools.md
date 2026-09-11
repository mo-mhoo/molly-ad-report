---
name: 試算表操作工具偏好
description: 處理試算表時的工具使用規則：用 gviz CSV API 讀取資料，用 browser_batch 批次執行，不截圖導覽
type: feedback
originSessionId: 054c7892-286a-4633-83e9-fae35a5ec041
---
試算表資料讀取直接用 gviz CSV API，瀏覽器操作用 browser_batch 批次執行，不要用截圖或逐步導覽方式。

**Why:** 更快、更有效率，截圖導覽方式太慢且容易出錯。

**How to apply:** 任何涉及 Google Sheets 或試算表的任務，優先呼叫 gviz CSV API 取得資料（`/gviz/tq?tqx=out:csv`），瀏覽器自動化步驟用 `browser_batch` 一次送出，避免單步截圖確認。
