---
name: project-activity-creative-flight-routine-pending
description: 活動素材走期自動暫停已在這台電腦實作完成；若另一台電腦也有重複的，該邊要負責統整成一個
metadata: 
  node_type: memory
  type: project
  originSessionId: d6a636ed-d8e1-4474-bb2d-2a923cdb5ffd
  modified: 2026-08-14T03:17:39.896Z
---

**狀態：已在這台電腦實作完成（2026-08-14）**，不再是「待確認」。

背景：Molly 記得曾交代過「活動素材要用 Claude routines 雲端排程」處理走期，但這台電腦一開始查不到對應記錄，她懷疑是在**另一台電腦**講的。討論後她明確要求**這台先直接建立**，不用等另一台確認，只要備註清楚、萬一真的重複由查到的那一邊負責統整。

**這台電腦已完成的部分：**
1. **[[meta-ad-copy]] skill 的 SKILL.md 已加上第 6 步**——之後每次用這個 skill 建立帶走期字首（`[~MM/DD]`）的活動廣告，execute 成功後會自動排一個一次性雲端 routine，到期日隔天 00:00（Asia/Taipei）呼叫 Meta API 把該廣告設 PAUSED，不用再問 Molly。範圍限定在這個 skill 建立的廣告，不掃其他既有廣告。
2. 已補排了 2026-08-14 這次上稿的兩支廣告（`120250147089150026`、`120250147090520026`，[~8/16]封館慶95折活動）的一次性 routine：`meta-flight-pause-20260817-fengguanqing95`，2026/08/17 00:00 執行。

**Why：** 兩台電腦的 scheduled-tasks／memory 都是本機儲存、不會同步，這台沒辦法確認另一台的狀態，但 Molly 判斷「重複執行 PAUSED」是無害的（idempotent），與其卡著等確認，不如先建立、事後有重複再處理。

**How to apply（如果是在另一台電腦看到這則）：**
- 先查那台的 `list_scheduled_tasks` 有沒有類似功能的 routine，以及 [[meta-ad-copy]] skill 的 SKILL.md 是否也已經有走期自動暫停這一段。
- 若那台也有／也改過：**兩邊只留一個**——建議留邏輯較新/較完整的那份，把重複的 routine 停用或刪除，SKILL.md 版本也統一。
- 若那台沒有：把這台的 SKILL.md 第 6 步同步過去即可，不用重新設計。
- 處理完後回頭更新這則記憶，記錄最後統整結果。
