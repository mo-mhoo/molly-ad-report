---
name: feedback-adset-choice-not-a-default
description: 廣告投放選擇了某個adset不代表下次可以直接沿用當預設,每次都要重新查證+重新問
metadata:
  node_type: memory
  type: feedback
---

Meta 廣告上稿時，某次問過 Molly 後選定的 adset（例如 RT adset 在多個候選之間交替出現、沒有固定答案的情況），**不能被我自己升級成往後的預設值**，即使我把它寫進了 memory。

**Why**：2026-09-11 處理御熹堂官網活動素材時，魚油/紅麴的 RT adset 歷史上在兩個 adset 間交替出現，那次問過 Molly 後她選了「VC7+ATC7」，我在回報跟 memory 裡都把它寫成「之後統一用這個」的語氣。Molly 糾正：「我希望你每次還是要重新判斷」——她要的是每次都重新查帳號現況、重新問她，不是把單次的選擇結果固化成規則。

**How to apply**：
- 寫入 memory 記錄「這次用了 X」時，要明確標註這只是某一次的快照/歷史紀錄，不是規則，避免未來的我（或其他 session）看到記憶就跳過重新判斷這一步。
- 帳號結構（adset/campaign）本身會變動（既有 adset 停用、換新 adset），任何「候選清單」類的記憶都要當作背景參考，執行前一律用工具重新查詢當下實際存在、有效的候選，並列出來給 Molly 確認，不能因為記憶裡寫過某個 ID 就跳過這一步直接沿用。
- 相關案例見 [[project_yuxitang_activity_adset_pattern]]（已依此原則修正過用語）、[[feedback_meta_ad_copy_adset_suggestion_list]]。
