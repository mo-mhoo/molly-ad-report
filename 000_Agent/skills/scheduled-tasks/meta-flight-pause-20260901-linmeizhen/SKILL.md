---
name: meta-flight-pause-20260901-linmeizhen
description: 走期到期自動暫停：御熹堂_官網「代言人林美貞-分潤-新客活動」影片素材，8/31到期後隔天0點關閉
---

這是 Molly 用 meta-ad-copy skill 建立走期廣告時，明確要求自動排好的到期暫停 routine，走期一到不需要再跟她確認，直接執行。

## 背景
廣告帳號：御熹堂_官網（account_id = 800743741924152，act_800743741924152）
素材名稱：`[~8/31]20260818_活動_影片_代言人林美貞-分潤-新客活動_All`
走期：起始 2026-08-18，到期 2026-08-31，本 routine 在到期隔天（2026-09-01）00:00 Asia/Taipei 觸發，要把這批廣告全部暫停。

## 要處理的廣告（ad_id / 所在 adset / 所在 campaign）
1. `120253352485480735` — 新客｜Broad｜排除購買名單 — 流量ATL(PV)｜新客｜測試包(202607)（原始來源廣告）
2. `120253353810440735` — 新客｜LAL會員5-10%｜排除購買名單 — 流量ATL(PV)｜新客｜測試包(202607)
3. `120253353810590735` — 新客｜美容養顏興趣｜排除購買名單 — 流量ATL(PV)｜新客｜測試包(202607)
4. `120253353837580735` — 新客｜比較文粉絲頁互動 — 轉換BTL(CPA)｜新客｜活動
5. `120253353859450735` — 新客｜LAL高價值會員(10%)排除名單 — 轉換BTL(CPA)｜新客｜活動
6. `120253353859430735` — 新客｜RT(180)排除會員名單 — 轉換BTL(CPA)｜新客｜活動

## 執行步驟
對上面每一個 ad_id：
1. 用 Meta Graph API v21.0 `GET https://graph.facebook.com/v21.0/{ad_id}?fields=name,effective_status&access_token={META_ACCESS_TOKEN}`（token 讀 `C:\AI小摸\.env` 裡的 `META_ACCESS_TOKEN`）查目前 `effective_status`。
2. 若已經是 `PAUSED`（可能 Molly 自己先手動關了）就跳過，不用再送請求。
3. 否則 `POST https://graph.facebook.com/v21.0/{ad_id}` 帶 `status=PAUSED&access_token={META_ACCESS_TOKEN}`，把它關掉。

這是 Molly 在建立時已同意的走期自動關閉，不需要再跟她確認才執行。

## 回報
執行完用一段文字回報結果：哪幾支成功暫停、哪幾支本來就已經是 PAUSED 直接跳過、有沒有失敗（附錯誤訊息）。