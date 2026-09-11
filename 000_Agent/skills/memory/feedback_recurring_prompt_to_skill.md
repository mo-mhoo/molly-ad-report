---
name: feedback-recurring-prompt-to-skill
description: Molly 重複要講的指令一律做成 skill，不要做成中控台按鈕或複製貼上流程
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 906cfe69-2d4b-43f7-a2e7-f5bb2d2aaf78
  modified: 2026-08-10T08:22:24.989Z
---

Molly 提到「每次都要跟你說一遍」的指令時（例如上稿要重複交代品牌、帳號、用 post id 還是複製），直接提案做成 skill，不要往「在中控台加一顆按鈕複製 prompt 文字」那個方向走。

**Why:** 她 2026-08-10 問能不能在 `control_center.html` 加常用 prompt 按鈕一鍵送指令。實際限制是：發布版 Artifact 沒有送訊息進對話框的能力，本機 HTML 更不可能，最多只能複製到剪貼簿再貼回來——反而比打一個斜線指令慢。skill 則是天生跨裝置（手機 Claude app 也能用斜線指令）、能把前提條件寫死在裡面，一勞永逸。

**How to apply:** 聽到重複性 prompt → 先找專案裡是否已有對應腳本可重用（她很常已經寫好了，例如 `meta_ad_copy_tool.py`），有的話 skill 只要當薄包裝，把「每次都要交代的前提」寫進去；skill 裡要明確寫「不要反問這些」。寫入類操作記得帶上 [[feedback_meta_ads_write_safety]] 的確認步驟。中控台 `control_center.html` 維持純網址啟動器定位。
