---
name: feedback-meta-ad-copy-adset-suggestion-list
description: meta-ad-copy 的 adset 判斷要給候選清單，不要只列單一比對結果或被工具警告卡住
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5c30b0de-de7f-4d30-b32e-8d6efd0c7ea9
  modified: 2026-08-20T01:24:13.785Z
---

跑 meta-ad-copy 的 plan 時，目標 adset 的判斷要給 Molly 一份**候選清單**，不能只丟工具自動比對到的單一 adset（尤其工具帶 ⚠ warning 時，不能直接當作阻擋條件）。

**Why:** 2026-08-20 上「國際狗狗日」精選集素材時，工具對唯一候選 adset「舊客｜狗｜排除PUR(30)」標示「⚠ 目錄不相容」，但 Molly 判斷實際上不衝突，甚至可以放 DPA 這類目錄型 adset。工具的相容性判斷不夠準，不能單憑警告就限縮成沒得選或要求她另外指定；反而應該把可能的候選 adset（含被標警告的）都列出來，讓她自己判斷取捨，而不是只呈現一個選項或被警告嚇退。

**How to apply:** 之後在 [[meta-ad-copy 相關流程]] 整理 plan 表格時，「目標 adset」欄位盡量列出多個候選（即使工具只給一個、或帶警告），並保留原始警告文字讓她自己判斷是否採用，不要自己先幫她排除掉。

**追加（同日確認）：** 光列候選清單還不夠，每個候選 adset 要附一句判斷依據（受眾定義／適合這次素材的理由），不能只丟 adset 名稱跟 ID 讓她自己猜為什麼推薦。
