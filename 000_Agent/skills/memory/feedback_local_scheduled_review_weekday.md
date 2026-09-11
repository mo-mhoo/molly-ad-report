---
name: feedback-local-scheduled-review-weekday
description: 本機排程的覆盤任務（如 ATL 調整後追蹤）要排在週一到週五，避開週六日
metadata: 
  node_type: memory
  type: feedback
  originSessionId: f535be03-1382-47b1-b8cd-421c4a3cd09c
  modified: 2026-08-21T00:21:44.864Z
---

排 ATL/廣告調整後的本機覆盤任務（`mcp__scheduled-tasks__create_scheduled_task` 的 `fireAt`）時，選的日期要落在週一到週五，不要挑到週六或週日。

**Why：** 本機排程任務只在本機 Claude Code App 開著時才會觸發（見 [[feedback_local_vs_cloud_scheduling]]），排在週末的話 App 很可能沒開，會拖到下週一才補跑，等於「5-7天後覆盤」的天數規劃白算了，Molly 明確要求要避開這個情況。

**How to apply：** 每次用 `fireAt` 排一次性覆盤任務時，先確認選到的日期是週一到週五；如果自然算出的天數（例如+7天）落在週六日，往前或往後調整到最近的平日，而不是照原本天數硬排。
