---
name: project-google-ads-integration
description: Google Ads API 憑證已完成設定；毛孩時代/御熹堂帳號 ID 與 ATL/BTL 分類實戰筆記
metadata: 
  node_type: memory
  type: project
  originSessionId: 07d81905-cac3-4a21-a645-1333a5f57a07
  modified: 2026-08-12T03:00:44.844Z
---

2026-08-10 完成 Google Ads API OAuth 設定：`.env` 的 `GOOGLE_ADS_DEVELOPER_TOKEN` / `CLIENT_ID` / `CLIENT_SECRET` / `REFRESH_TOKEN` / `CUSTOMER_ID` / `LOGIN_CUSTOMER_ID` 皆已填妥並驗證可正常呼叫 [google_ads_fetcher.py](../../../../AI小摸/google_ads_fetcher.py)。

**Why：** 原本 BTL/YoY 週報只能靠截圖手動謄寫，數字常被截斷；改直接打 API 可以拿到精確到小數點的即時數據，且能反查驗證舊截圖的分類邏輯。

**How to apply：**
- MCC（代理商中心）帳戶 ID：`4195240594`（騰勢(股)），底下掛所有 TSA 集團品牌的 Google Ads 帳號。
- **毛孩時代官網的 Google Ads 花費要合併兩個帳號才是完整數字**：主帳號 `1014276621` + 部落格子帳號 `1026904578`（PfM_45057582_毛孩時代）。只查主帳號會少算約 20-25% 的 ATL 花費。
- 御熹堂 主帳號：`8669832537`（不要跟毛孩時代搞混，兩者花費量級差很多）。
- `.env` 目前預設的 `GOOGLE_ADS_CUSTOMER_ID` 是毛孩時代主帳號；查其他品牌要在程式裡直接傳入該品牌的 customer_id，不用改 `.env`（`google_ads_fetcher.py` 的函式本來就是吃 customer_id 參數）。
- **ATL/BTL 分類規則（Google Ads 活動命名）**：名稱含「ATL」→ATL；含「BTL」或「BT｜」（BT 加全形直線，命名不完整但語意等同 BTL）或「轉換」（如 YouTube 轉換活動）→BTL。純用 `'BTL' in name` 字串比對會漏掉 `BT｜` 開頭的 AIMax 系列活動，導致 BTL 收益少算約 5-8%。
- **`metrics.conversions` 預設值有雷**：Google Ads 帳戶若設定多個轉換動作（例如毛孩時代同時追蹤「新網站_完成訂單」「新網站_造訪任何頁面」「新網站_商品加入購物車」等），預設 `conversions` 欄位會把這些全部加總，購買數被灌水（例如真實訂單 690 筆，帳面 conversions 卻有 8000+ 筆），導致 CPA/CVR/AOV 嚴重失真。若要精算轉換相關指標，需要 `segments.conversion_action_name` 篩選出真正的購買動作（本例中是「新網站_完成訂單」）；但呈現花費、CTR、CPC、CPM 這幾個跟轉換數無關的指標時，用預設查詢即可，不受影響。

- **毛孩時代蝦皮通路的 Google Ads 帳號名稱有陷阱**：帳號名稱看起來最直覺的「毛孩時代-蝦皮商城」(customer id `1273409762`) 其實是閒置帳號（花費常年 $0）；真正在投放蝦皮的帳號叫「**PfM_45057582_毛孩時代**」(customer id `5079367835`)，活動命名含「蝦皮/pmax/SEM」關鍵字。抓蝦皮通路數據前務必先查活動花費確認是不是選對帳號，不要只看帳號名稱直覺猜。
- Meta CPAS 帳號 ID（毛孩時代）：蝦皮 `1921433368254905`、momo `3317877141845136`（官網是 `1318362572209550`，見 [[anthropic-skills:tsa-meta-ads-operations]]）。CPAS 帳號的 `actions:omni_purchase` 欄位大多回傳空值，收益要用 `result_values`（indicator 會自動變成 `catalog_segment_value:omni_purchase`），且部分活動偶爾回傳 `result_values.indicator == "mixed"`，代表 campaign 層級算不出正確收益、需要拆到 adset 層級才能還原，先記為 $0 並在報表加註警語，不要當作真實 $0 呈現。
- **CPAS 帳號的購買「數」不在 `actions:omni_purchase`，要用 `results` 欄位**（自動對應到 `catalog_segment_actions:omni_purchase`）——第一次漏抓這個欄位時誤判成「CPAS 帳號完全沒有購買數據」，被使用者糾正後才發現。有了購買數就能正常算 CPA／CVR／AOV。
- **CPAS 帳號的整體「頻次」不要用 campaign 層級加權還原去估**（`impressions/frequency` 反推 reach 再加總，遇到同帳號多組窄受眾再行銷活動並存時會嚴重失真，實測飆到 30+ 不合理）——改成直接用 `level=ad_account`（而非 `campaign`）查該帳號整段期間的 `reach`／`frequency`，Meta 會回傳自己去重後的正確整體頻次，一次呼叫就搞定，不用自己估。加購數 `omni_add_to_cart` 則是在 campaign 層級和 ad_account 層級都測過，確認真的是空值（CPAS／目錄同步廣告的加購行為不會回傳給 Meta pixel），這個才是真正查不到、不是查詢方式問題。
- **教訓：欄位回空值時，先窮舉候選欄位／換查詢層級再下「查不到」的結論**，不要憑一次嘗試就對使用者說某個指標是平台限制——上面兩個問題（購買數、頻次）最後都證明是查詢方式錯了。
- **CPAS 加購數也是查詢方式錯了，不是真的沒資料**：Adspirer MCP 工具的 `omni_add_to_cart` 欄位對 CPAS 帳號幾乎抓不到（測到帳號層級 70 天只有個位數），但專案自己的 [meta_fetcher.py](../../../../AI小摸/meta_fetcher.py) 直接查 Graph API 原始 `actions`／`catalog_segment_actions` 陣列（`fetch_campaign_raw_rows` + `aggregate`），同一週期能抓到幾百筆加購（例如蝦皮某週 430 筆）。**日後遇到 Meta CPAS 數據，優先用 `meta_fetcher.py` 而非 Adspirer MCP**，因為前者是專案內已針對 CPAS 調過的工具（`_action_maps` 會在 `catalog_segment_actions` 回傳 `mixed` 時自動退回一般 `actions`／`action_values`，MCP 工具沒有這個 fallback，只看 `result_values` 遇到 `mixed` 就死路一條）。這次也因此發現先前用 MCP 工具算的 momo YoY 數字低估了 2025 基期，發布後才修正。
- **更根本的教訓：這個專案已經有自己寫好、驗證過的資料抓取工具（`meta_fetcher.py`、`google_ads_fetcher.py`），遇到 Meta／Google Ads 數據需求應該優先檢查專案內有沒有現成工具，而不是預設用外部 MCP 工具**——外部工具的欄位設計是通用的，不見得涵蓋這個帳戶結構（CPAS／catalog）的特殊情況；專案自己的工具是針對這些帳號的實際命名規則、CPAS fallback 邏輯調過的，更可靠。
- **做 YoY 比較時，「現在沒花費的帳號」不能直接排除，要查它去年同期有沒有花費**：蝦皮的 Google 廣告帳號在 2026 年換了新帳號（PfM_45057582），舊帳號「毛孩時代-蝦皮商城」2026 年花費是 $0 看似閒置，但 2025/8/1–8/9 同期舊帳號還有 $6,037 花費／$15,029 收益。只看新帳號會讓 2025 基期被低估，算出來的 YoY 漲幅嚴重失真（原本誤算花費 -27%／收益 +129%，補上舊帳號後正確答案是 -47%／+36%）。**日後只要帳號結構可能換過（新舊帳號、改名、搬遷 MCC），YoY 比較前一定要確認兩個年度用的是同一組帳號範圍，不能只看「現在還在花錢的帳號」。**
- **抓歷史週資料時務必逐週核對 spend 數字有沒有重複**：曾發生同一個 MCP 呼叫在不同 `time_range` 下回傳同一批數字（cache 疑慮），也曾發生手動彙整多筆並行結果時漏掉一個活動（momo 2025/8 MTD 少算了一個 campaign，導致當週 YoY 花費/收益/ROAS 全部算錯，發布後才修正）。抓多週資料時最好用不同天數區間交叉驗證加總是否兜得起來，別完全信任單次 parallel batch 的結果順序。

- **毛孩時代主帳號同一個關鍵字可能散落在多個活動裡**：例如「腸胃益生菌」相關關鍵字除了正在跑的「2.腸胃益生菌-Blog_1031010301_ATL」，還存在於「部落格-腸胃」系列（#2/#3）跟「06_官網_產品字_腸胃益生菌_1031010501」（底下甚至有個獨立命名為「症狀字」的廣告群組）——這些活動目前都是**活動層級 PAUSED**，裡面關鍵字狀態即使顯示 ENABLED 也不會真的花錢。查證某個關鍵字有沒有在花錢，除了看關鍵字自己的 status，也要確認 campaign.status 有沒有整個被暫停。詳細關鍵字優化方法論見 [[feedback_google_ads_atl_keyword_optimization]]。

相關：[[project_meta_insights_report]]、[[reference_ecommerce_calendar]]、[[feedback_html_report_format]]、[[feedback_google_ads_atl_keyword_optimization]]
