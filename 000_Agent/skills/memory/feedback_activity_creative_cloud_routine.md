---
name: feedback-activity-creative-cloud-routine
description: 活動素材（有走期的檔期廣告）的走期到期處理要用 Claude 雲端排程 routine，不是其他自動化方式
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d6a636ed-d8e1-4474-bb2d-2a923cdb5ffd
  modified: 2026-08-14T03:26:01.267Z
---

「活動」類素材（廣告名稱帶 `[~MM/DD]` 這種走期字首、有明確檔期結束日的廣告，例如封館慶、週年慶等），走期到期後的處理方式要用 **Claude Code 的雲端排程 routine**（`/schedule` 或 RemoteTrigger 建立的 cron routine）來做，不是寫進 [[project_ad_agent]] 或 [[project_molly_ad_report]] 這些既有工具腳本裡、也不是純靠人工記得去 Meta 後台手動關。

**Why:** Molly 2026-08-14 提到她記得交代過這個方向（可能是在另一台電腦講的），這是她對「檔期廣告怎麼收尾」這件事的既定偏好，不是這次臨時起意。

**How to apply:** 之後遇到任何有明確走期/檔期結束日的活動素材，如果要幫她做到期自動處理，優先方案是建一個 Claude 雲端排程 routine，而不是其他機制（例如寫死在某支 .py 排程腳本裡）。

**已實作（2026-08-14）：** 用 [[meta-ad-copy]] skill 建立活動廣告這個路徑，已經改好了——SKILL.md 加了第 6 步，execute 成功後若廣告名稱帶走期字首（`[~MM/DD]`），會自動解析走期、排**兩個**一次性 routine：起始日當天 00:00（Asia/Taipei）自動把廣告改 **ACTIVE**、到期日隔天 00:00 自動改 **PAUSED**，範圍只限這個 skill 建的廣告，不用再問 Molly。

**重要：走期開始自動開啟是 Molly 明確要的（2026-08-14 補充確認），會蓋掉「新廣告先 PAUSED 等她審核」這個預設**——PAUSED 只是建立時的初始狀態，走期一到就會被 routine 自動打開，不會等她去 Meta 後台看過。如果解析出的起始日 00:00 已經是過去（上稿當天剛好就是起始日），skill 不會自己猜著開，而是在回報時問她要不要現在就開。

細節與另一台電腦可能重複的處理方式見 [[project_activity_creative_flight_routine_pending]]。

**尚未涵蓋的部分：** Molly 手動上稿（不透過 meta-ad-copy skill）的活動廣告，目前沒有自動掛走期開關 routine，如果之後要擴大到這個範圍，要另外跟她確認。
