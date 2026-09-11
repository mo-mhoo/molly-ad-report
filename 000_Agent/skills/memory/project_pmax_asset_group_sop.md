---
name: project-pmax-asset-group-sop
description: 用 Google Ads API 建立/編輯 PMax 資產群組的實戰 SOP（角色資源上限、素材借用、常見錯誤）
metadata: 
  node_type: memory
  type: project
  originSessionId: eb43eb65-7d04-4934-ac2b-ec02cd8ad3e7
  modified: 2026-08-17T04:02:25.749Z
---

2026-08-17 御熹堂 PMax「BTL｜P-Max (ROAS出價)｜活動」建立 `[~8/31]20260817-全館活動` 資產群組時，累積的實戰 SOP（.env 憑證已在 [[project_google_ads_integration]] 中確認可用，這裡記的是「怎麼寫 mutate 腳本」的細節）：

**Why：** 過程中踩了好幾個坑（合併 mutate 失敗、Google 強制要求圖片才能建群組、移除操作查詢誤判等），下次直接照 SOP 做可以省掉重新試錯的時間。

**How to apply（依序）：**
1. 先查現有資產群組命名慣例（`[~到期日]起始日-活動名稱`）跟目前 ENABLED 那組的到達網址/圖片，作為新群組的基礎。
2. 標題/長標題/說明的字數要用「全形=2、半形=1」的權重去算，不能用 `len()` 直接算——標題上限30、長標題/說明上限90（換算大約中文13-14字/40字）。用戶要求「填滿」時上限是 15 標題／5 長標題／5 說明。
3. **就算狀態設 PAUSED，Google Ads API 建立資產群組時仍強制要求至少 1 張 MARKETING_IMAGE（橫式）+ 1 張 SQUARE_MARKETING_IMAGE（方形）**，純文字建群組會回傳 `NOT_ENOUGH_MARKETING_IMAGE_ASSET` / `NOT_ENOUGH_SQUARE_MARKETING_IMAGE_ASSET` 錯誤。沒有新圖時可以先借用現有其他資產群組的圖片（見下）。
4. **建立 asset_group + assets + asset_group_asset 連結，一定要包在同一個 `GoogleAdsService.Mutate`（`ga_service.mutate(request=...)`，裡面塞多個 `MutateOperation`）**，不能拆成 `AssetGroupService.mutate_asset_groups` / `AssetService.mutate_assets` / `AssetGroupAssetService.mutate_asset_group_assets` 三次分開呼叫——分開呼叫時暫時性 ID（負數）無法跨呼叫互相參照，會報 `Resource was not found`。
5. **要「引用」既有素材（不管圖片、Logo、還是 YouTube 影片），不用重新上傳**：直接在 `AssetGroupAssetOperation.create` 把 `asset` 設成既有的 resource_name（例如 `customers/{cid}/assets/{asset_id}`）即可。**查詢來源群組可借用的素材時，欄位要包含 `YOUTUBE_VIDEO`**，不要只查 `MARKETING_IMAGE/SQUARE_MARKETING_IMAGE/PORTRAIT_MARKETING_IMAGE/LOGO/BUSINESS_NAME`（這次第一輪就漏了 YouTube 影片，讓使用者誤以為是技術限制，其實只是查詢欄位沒列全）。
6. 要移除借來的佔位素材連結，用 `AssetGroupAssetOperation.remove = <asset_group_asset resource_name>`，格式是 `customers/{cid}/assetGroupAssets/{asset_group_id}~{asset_id}~{FIELD_TYPE_ENUM_NAME}`（可以先 SELECT `asset_group_asset.resource_name` 查出正確值再拿去 remove，不要自己組字串猜格式）。
7. **GAQL 查 `asset_group_asset` 一定要加 `WHERE asset_group_asset.status != 'REMOVED'`**，不然已經移除的連結還是會被列出來，會誤判成「移除沒生效」（這次浪費一輪排查，其實移除是成功的，只是查詢沒篩狀態）。
8. Python 腳本用 `load_dotenv()` 讀 `.env` 時，若腳本放在 repo 目錄之外（例如 scratchpad 暫存資料夾），預設搜尋路徑會找不到 `.env`，要明確傳 `dotenv_path=r"C:\AI小摸\.env"`。
9. `login_customer_id`／`customer_id` 傳給 `GoogleAdsClient` 前要先用 `re.sub(r"[^0-9]", "", v)` 去掉破折號/空白，不然報 `invalid login customer id`。
10. Windows 終端機用 cp950，中文字串 print 到 console 會亂碼（不影響資料正確性）——查詢結果一律寫成 UTF-8 檔案再用 Read 工具讀回來，不要直接看 console 輸出判斷內容對不對。

**額外資訊（Sitelink/Callout/Promotion 等 campaign_asset）SOP：**

11. 「額外資訊」是使用者口語說法，可能指 Sitelink（網站連結）、Callout（加註）、Promotion（促銷）任一種或全部，開工前先問清楚要哪幾種，不要自己猜一種就做。
12. 這些都是**活動層級（campaign_asset）**，會套用到整個 PMax 活動下所有資產群組，不是只套用到某一個資產群組——跟資產群組內的圖片/標題/說明（asset_group_asset，只影響單一群組）blast radius 不一樣，動手前要跟使用者確認清楚範圍。
13. **SitelinkAsset / CalloutAsset / PromotionAsset 本身都有原生 `start_date`/`end_date` 欄位（YYYY-MM-DD），可以直接設定檔期自動到期，不用像這個帳號過去的做法一樣手動 PAUSE**——`CampaignAsset`（連結表）本身沒有日期欄位，日期要設在子素材物件上（例如 `asset.sitelink_asset.start_date`）。查 API schema 用 `cls()._meta.fields.values()` 而不要用猜的。
14. `PromotionAsset` 的到達網址一樣是掛在最外層 `Asset.final_urls`，不是巢狀在 `asset.promotion_asset` 底下（跟 sitelink 同一套模式）；`percent_off` 欄位單位是 micros，`1,000,000 = 100%`。
15. GAQL 查 `asset.promotion_asset.final_urls` 這種欄位名稱前，先用 schema 內省法（見上）確認欄位真的存在，避免對著查詢報錯瞎猜欄位名。

**圖片來源：官網商品頁也能抓「壓框圖」當素材：**

16. 官網商品列表頁（例如 Shopify/cyberbiz 系統）的商品卡片，`<img class="primary-image">` 的 `src` 通常只是純白底商品照，**沒有促銷壓框**（紅旗/贈品階梯/檔期banner）；但同一張卡片的加入購物車按鈕上常有 `data-photo` 屬性，指向另一個尺寸（例如 600x600）的版本，那個才是「有壓框」的完整促銷合成圖，兩者圖檔內容不同，不能只看 `primary-image` 就判斷「這頁沒有壓框圖」。
17. 做法：`document.querySelectorAll('.btn-cart[data-photo]')` 之類的方式抓 DOM 上的 `data-photo`（或類似 cart 按鈕屬性），比只抓 `<img src>` 更完整；抓到 URL 後直接用 Python `requests.get()` 下載（公開 CDN 圖檔不需要認證），核對副檔名/尺寸後再用 AssetService 上傳到 Google Ads。
18. 找特定檔期的圖優先看該品牌 Drive 裡有沒有「日期資料夾」（例如 `0817-0831`），裡面通常會有官方做好的三尺寸橫式/方形/直式banner，比零散抓商品照更適合當 MARKETING_IMAGE/SQUARE_MARKETING_IMAGE/PORTRAIT_MARKETING_IMAGE，見 [[feedback_drive_asset_search_by_flight_dates]]。

相關：[[project_google_ads_integration]]、[[feedback_meta_ads_write_safety]]、[[feedback_verify_before_doubting]]、[[feedback_drive_asset_search_by_flight_dates]]
