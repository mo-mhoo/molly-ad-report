---
name: project-kaohsiung-events-radar
description: 高雄週末活動雷達 Artifact — 追蹤高雄大型場館週末活動避免塞車
metadata: 
  node_type: memory
  type: project
  originSessionId: 2f34e0f9-e9ca-4956-b928-7e2432400cb1
---

使用者每次下班南下高雄時，若當天有大型演唱會/活動會塞車，希望能提早知道週五六日的高雄大型活動場次。

已建立 Artifact「高雄 2026 整年度活動雷達」：https://claude.ai/code/artifact/95db93a3-3d94-4a84-9be2-e7fc9af05187
- 檔案路徑（每次更新後需重新 Artifact publish 同一路徑以保留連結）：session scratchpad 下的 `kaohsiung_weekend_events.html`（scratchpad 路徑每個 session 會變，需重新 Write 檔案再 publish，並帶上 `url` 參數指向上面的網址以更新同一份 Artifact，而不是 file_path 相同——注意 file_path 是暫存路徑，不會跨 session 存在，所以更新時必須先重寫檔案內容，再用 `url` 參數 publish 到既有連結）。
- 設計：橫向月份分組列表、週五六日日期用橘色標示、已結束場次用刪除線淡化、頂部有「最近警報」卡片顯示下一場倒數。

**追蹤場館範圍**：高雄巨蛋、世運主場館（國家體育場）、高雄流行音樂中心（海音館，苓雅真愛碼頭一帶）、夢時代（含高雄啤酒節、跨年晚會）、高雄展覽館。

**Why**：第一版整理時完全漏掉「高雄流行音樂中心」這個場館（東海個人演唱會、DxS 高雄巨蛋連三天場次也一開始漏掉），使用者事後補充才發現。之後查詢/更新時務必：
1. 個別搜尋這五個場館關鍵字，不能只搜「高雄巨蛋」+「世運主場館」。
2. 注意很多演唱會是連續多天（例如 DxS 7/24–26 三天、EXO 兩天），第一輪搜尋常常只抓到其中一天，要交叉確認是否為連續場次。

**How to apply**：已建立每週排程任務 `kaohsiung-events-weekly-update`（cron `0 9 * * 1`，每週一早上9點），任務檔案在 `C:\Users\Molly Ho\.claude\scheduled-tasks\kaohsiung-events-weekly-update\SKILL.md`。任務內容：搜尋上述五個場館的最新公布場次、更新 Artifact 內的 events/tbdItems 陣列、更新資料日期戳記，並用 `url` 參數重新 publish 到同一個 Artifact 網址（不能只用 file_path，因為排程每次都是全新 session、暫存路徑不同，一定要帶 url 才不會產生新連結）。若使用者之後想改時間，用 update_scheduled_task 改 `kaohsiung-events-weekly-update` 這個 taskId，不要重新建立。

**版面演進**：第一版是直向清單，第二版加上月份分隔線＋今天分隔線，第三版改成使用者要求的「橫向月份卡片牆」：從當月排到12月橫向捲動，過去場次全部收進頁面下方預設收合的 `<details>` 區塊，不佔版面。過去/未來的判斷是頁面前端 JS 用 `new Date()` 即時算的，不是寫死的，所以資料本身不用手動搬移過去/未來，只要维护 events 陣列內容即可。第四版加了 🇰🇷 韓星標籤（`kr: true` 欄位，藍色底），只標卡司查證過確定有韓籍藝人的場次。

**Why（韓星標記務必查證）**：使用者原本以為「幫我寫上行事曆」是指建立 Google Calendar 事件，我猜錯方向問了要選哪個日曆，使用者回「不是」才澄清是指在這個網頁上標記。確認方向後使用者又補一句「不要幻覺」——明確要求韓星分類要基於查證到的公開卡司資訊，不確定國籍或卡司未公布時寧可不標。

**How to apply**：之後不管是我自己更新還是排程任務更新，新增場次時要判斷 kr 欄位都要先查證卡司（藝人所屬經紀公司/團體國籍），混合卡司音樂節只有在韓籍藝人是主要卡司時才標；卡司未公布或不確定一律不加 kr 欄位，不要用團名聽起來像韓文或直覺猜測。這個規則已經寫進 `kaohsiung-events-weekly-update` 排程任務的 prompt 裡。
