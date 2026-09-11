---
name: meta-flight-pause-20260827-goudog-milksha
description: 走期到期自動暫停「狗狗日x迷客夏」全館活動廣告（毛孩時代_官網，BTL x2 + ATL Interact）
---

這是 meta-ad-copy skill 建立的走期廣告到期自動暫停任務。這是 Molly 在建立廣告時已經明確同意的走期自動開關機制，不需要再跟她確認才執行。

帳號：毛孩時代_官網（account_id 1318362572209550）

要處理的廣告（走期到 2026/08/26 止，今天 8/27 00:00 開始要暫停）：
- ad_id 120250258391060026，所在 adset：舊客｜購買/名單排除PUR(30)（120232396212020026）
- ad_id 120250258392400026，所在 adset：舊客｜狗｜排除PUR(30)（120232814878410026）
- ad_id 120250258456110026，所在 adset：新貼文｜活動（120236259363790026，campaign「流量ATL(Interact)｜新客」）

三支廣告名稱都是：[~8/26]20260819_活動_單圖_全館活動_狗狗日x迷客夏_all

執行步驟：
1. 讀取 C:\AI小摸\.env 裡的 META_ACCESS_TOKEN
2. 對每一支 ad_id，先用 Graph API v21.0 GET https://graph.facebook.com/v21.0/{ad_id}?fields=effective_status,name 查目前狀態
3. 如果已經是 PAUSED（代表 Molly 可能自己先手動關了），跳過該支，不用再打
4. 如果不是 PAUSED，用 POST https://graph.facebook.com/v21.0/{ad_id} 帶 status=PAUSED 暫停
5. 三支都處理完後，用一段文字回報結果：哪支成功暫停、哪支本來就已經是目標狀態、有沒有失敗（附錯誤訊息）