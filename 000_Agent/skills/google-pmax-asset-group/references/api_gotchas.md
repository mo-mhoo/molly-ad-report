# Google Ads API 寫入 PMax 資產群組／額外資訊時的技術眉角

這份文件是踩過坑之後整理的細節，`scripts/` 底下的輔助函式已經把大部分處理掉了，
但手動組 mutate operation 時（例如上傳圖片、寫額外資訊）還是會直接碰到，讀過一次再動手。

## 1. 圖片是建立資產群組的硬性門檻

就算資產群組狀態設 `PAUSED`，Google Ads API 仍然要求**至少 1 張橫式（`MARKETING_IMAGE`）
+ 1 張方形（`SQUARE_MARKETING_IMAGE`）素材**才能建立成功，純文字建群組會直接被擋：

```
NOT_ENOUGH_MARKETING_IMAGE_ASSET / NOT_ENOUGH_SQUARE_MARKETING_IMAGE_ASSET
```

沒有新圖時，向使用者確認是否可以**先借用其他資產群組既有的圖片**（見第 2 點），
等使用者提供新圖後再換掉——不要為了通過這道門檻隨便塞一張不相關的圖。

## 2. 借用既有素材＝引用 resource_name，不用重新上傳

圖片、Logo、YouTube 影片都一樣：不用下載再重新上傳，直接在
`AssetGroupAssetOperation.create` 把 `asset` 設成既有素材的 resource_name
（例如 `customers/{cid}/assets/{asset_id}`）即可建立連結。

用 `query_helpers.list_asset_group_assets()` 查來源群組可借用的素材時，記得它已經把
`YOUTUBE_VIDEO` 也列進查詢欄位了——如果自己手寫查詢，很容易只想到圖片/Logo/品牌名，
漏掉影片，讓使用者誤以為「沒辦法借用既有影片」，其實只是查詢欄位沒列全。

## 3. 多個操作要包在同一個 `GoogleAdsService.Mutate`

建立 asset_group + 多個 asset（文字/圖片）+ 多個 asset_group_asset 連結，**必須**
全部包在同一次 `client.get_service("GoogleAdsService").mutate(request=...)` 呼叫裡
（`scripts/ads_client.py` 的 `combined_mutate()` 已經封裝好），不能拆成
`AssetGroupService.mutate_asset_groups` / `AssetService.mutate_assets` /
`AssetGroupAssetService.mutate_asset_group_assets` 三次分開呼叫。

原因：新建立的 asset_group / asset 在同一批操作裡是用暫時性負數 ID 互相參照
（例如 `customers/{cid}/assets/-1001`），這種參照只在同一次 Mutate 呼叫內有效，
拆開呼叫會直接報 `Resource was not found`。

## 4. 先 validate，再正式送出

`combined_mutate()` 預設 `validate_only=True`。腳本統一用一個 `--apply` 命令列參數
切換成正式送出（`validate_only=False`），流程永遠是：

1. 不帶 `--apply` 跑一次，確認沒有 GoogleAdsException
2. 帶 `--apply` 正式送出
3. 用 `query_helpers.count_asset_group_assets()` 或 `list_campaign_assets()` 查最終狀態，回報給使用者

## 5. 移除素材連結

要移除借來的佔位素材（例如先借用舊圖、拿到新圖後要把舊的換掉），用：

```python
op = client.get_type("AssetGroupAssetOperation")
op.remove = "<asset_group_asset 的 resource_name>"
```

resource_name 格式是 `customers/{cid}/assetGroupAssets/{asset_group_id}~{asset_id}~{FIELD_TYPE}`，
**先用 GAQL 查出正確的 `asset_group_asset.resource_name` 再拿去 remove**，不要自己組字串猜格式。

移除之後查詢一定要加 `WHERE asset_group_asset.status != 'REMOVED'`（`query_helpers.py`
的函式已經內建這個篩選），不然已經移除的連結還是會被列出來，會誤判成「移除沒生效」。

## 6. 額外資訊（Sitelink / Callout / Promotion）

「額外資訊」是口語說法，可能只指其中一種，也可能三種都要——**開工前先問清楚使用者要哪幾種**，
不要自己猜一種就做。

這些都是**活動層級**（`campaign_asset`），會套用到整個 PMax 活動下所有資產群組，
不是單一資產群組專屬。跟資產群組內的圖片/文案（`asset_group_asset`，只影響單一群組）
影響範圍不一樣，動手前跟使用者講清楚範圍。

`SitelinkAsset` / `CalloutAsset` / `PromotionAsset` 都有原生 `start_date`/`end_date`
欄位（`YYYY-MM-DD`），**可以直接設定檔期讓它自動到期，不用像這個帳號過去的做法一樣
事後手動 PAUSE**（帳號裡有大量過期後還停在 PAUSED 沒清掉的舊 Sitelink，就是沒設這兩個
欄位的結果）。日期是設在子素材物件上（例如 `asset.sitelink_asset.start_date`），
連結表 `CampaignAsset` 本身沒有日期欄位。

`PromotionAsset` 的到達網址跟 Sitelink 一樣是設在**最外層 `Asset.final_urls`**，
不是巢狀在 `asset.promotion_asset` 底下——漏設會報 `REQUIRED_NONEMPTY_LIST` 錯誤。
`percent_off` 欄位單位是 micros，`1,000,000 = 100%`（例如 17% 折扣寫 `170000`）。

查不確定的欄位名稱時，不要對著 GAQL 報錯瞎猜，用 schema 內省法直接確認：

```python
import google.ads.googleads.v25.common.types.asset_types as at
print([f.name for f in at.PromotionAsset()._meta.fields.values()])
```

## 7. 文案字數上限

標題/長標題/說明/Sitelink 文字/Callout 文字的字數上限，Google 是用「全形字元算 2、
半形字元算 1」的權重去算，不能用 Python 的 `len()` 直接算（中文句子會嚴重低估）。
用 `scripts/ads_client.py` 的 `weighted_len()` / `check_limit()`。

上限對照：

| 類型 | 上限（權重字數） |
|---|---|
| 標題（Headline） | 30 |
| 長標題（Long headline）/ 說明（Description） | 90 |
| Sitelink 連結文字 | 25 |
| Sitelink 說明（各行） | 35 |
| Callout 文字 | 25 |

使用者要求「填滿」文案時，PMax 資產群組的上限是 **15 條標題／5 條長標題／5 條說明**。

## 8. 官網素材來源：商品頁「壓框圖」藏在哪

官網商品列表頁（Shopify / cyberbiz 等系統）的商品卡片，`<img class="primary-image">`
的 `src` 通常只是純白底商品照，**沒有促銷壓框**（紅旗/贈品階梯/檔期 banner）。
壓框版（完整促銷合成圖）常常藏在「加入購物車」按鈕的 `data-photo` 屬性，指向另一個
尺寸（例如 600x600）的版本，兩者圖檔內容不同——不能只看 `primary-image` 就判斷
「這頁沒有壓框圖」。

用瀏覽器工具（`javascript_tool`）讀 DOM 抓，例如：

```js
Array.from(document.querySelectorAll('.btn-cart[data-photo]'))
  .map(b => ({name: b.getAttribute('data-name'), photo: 'https:' + b.getAttribute('data-photo')}))
```

抓到 URL 後可以直接用 Python `requests.get()` 下載（公開 CDN 圖檔不需要認證），核對
副檔名/尺寸後再上傳。

找特定檔期的官方素材優先看該品牌 Drive 裡有沒有「日期資料夾」（例如 `0817-0831`），
裡面通常會有做好的三尺寸橫式/方形/直式 banner，比零散抓商品照更適合當
`MARKETING_IMAGE`/`SQUARE_MARKETING_IMAGE`/`PORTRAIT_MARKETING_IMAGE`。

## 9. 目標對象信號（Audience Signal）是另一張表，容易漏掉

Google Ads 後台「目標對象信號」欄位（搜尋主題關鍵字＋目標對象）對應的資源是
`AssetGroupSignal`，**跟資產群組的圖片/文案（`AssetGroupAsset`）是完全不同的資源類型**，
借用舊群組素材時很容易只複製了 `AssetGroupAsset`、漏掉這張表，導致新群組在後台顯示
「未提供任何目標對象信號」。

查詢／複製寫法：

```python
q = """
SELECT asset_group_signal.audience.audience, asset_group_signal.search_theme.text
FROM asset_group_signal
WHERE asset_group_signal.asset_group = 'customers/{cid}/assetGroups/{old_group_id}'
"""
```

- 搜尋主題（Search theme）：`AssetGroupSignalOperation.create.search_theme.text = "關鍵字"`
- 目標對象（Audience）：`AssetGroupSignalOperation.create.audience.audience = "customers/{cid}/audiences/{audience_id}"`
  （引用既有 audience 資源，不用重新建立）

一樣包進 `combined_mutate()` 裡跟其他操作一起送。**每次建立新資產群組（尤其是借用舊組
素材當基礎時），都要順手檢查並複製這個表，不要只顧著圖片/文案就以為做完了。**

## 10. 環境雜項

- `python-dotenv` 的 `load_dotenv()` 預設搜尋路徑跟著腳本檔案位置走，不是跟著執行時的
  cwd。腳本放在專案目錄以外（例如暫存資料夾）時常常讀不到 `.env`，`ads_client.py` 裡
  已經寫死 `dotenv_path=r"C:\AI小摸\.env"`，沿用即可，不用自己重新猜路徑。
- Windows 終端機是 cp950，中文字串直接 `print` 到 console 會亂碼（資料本身沒問題，
  只是顯示壞掉）——查詢結果一律用 `ads_client.write_utf8()` 寫成 UTF-8 檔案，再用
  Read 工具讀回來確認內容，不要靠 console 輸出判斷對不對。
