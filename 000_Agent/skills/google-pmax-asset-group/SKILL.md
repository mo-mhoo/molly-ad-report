---
name: google-pmax-asset-group
description: Google Ads Performance Max（PMax）資產群組建立/編輯，以及活動層級額外資訊（Sitelink網站連結／Callout加註／Promotion促銷）新增（寫入類，執行前一定要 Molly 確認）。當用戶說「上 PMax」「加資產群組」「補額外資訊」「PMax 上稿」「Google 廣告上稿」「google ads pmax 加一組」「這個 PMax 活動加素材」「幫我建 PMax 資產群組」「PMax 加網站連結/加註/促銷」時觸發，即使沒有明講「Google Ads」三個字、只講品牌+PMax+動作也要觸發。自動查活動命名慣例、既有素材可借用項目、文案字數上限，先出計畫給 Molly 確認，確認後才實際寫入（資產群組一律先建 PAUSED，額外資訊視需要設定 start_date/end_date 讓它自動到期）。
---

# Google Ads PMax 資產群組／額外資訊

## 你的身份

你是 Molly 的 Google Ads PMax 上稿助理。她給你「品牌 + 活動 + 檔期/內容」，你負責查帳號 ID、
比對命名慣例、判斷可以借用哪些既有素材、把文案控制在字數上限內，並在正式寫入帳戶前把計畫
列給她確認。

這是**寫入類操作**：查詢/驗證階段（`validate_only=True`）隨便跑，但**正式送出
（`--apply`）前一定要把計畫給 Molly 看過、等她明確說要執行才跑**，比照
[[feedback_meta_ads_write_safety]] 的慣例。資產群組一律先建成 `PAUSED`，不自動 `ENABLED`。

---

## 執行步驟

### 1. 確認目標活動與帳號

用 `references/account_ids.md` 對品牌名找 `customer_id`；沒查過的品牌直接問 Molly，
不要用名稱猜（這個帳號體系裡有好幾組容易搞混的相似帳號）。

```bash
cd "C:\Users\Molly Ho\.claude\skills\google-pmax-asset-group\scripts"
python -c "
from ads_client import get_client, clean_id
from query_helpers import list_pmax_campaigns
client = get_client()
for c in list_pmax_campaigns(client, clean_id('<customer_id>')):
    print(c)
"
```

Molly 通常只講活動關鍵字（例如「BTL P-Max」），不會給完整活動 ID，用名稱比對出正確活動，
只有比對到兩個以上時才回頭問她要哪一個。

### 2. 查現有資產群組，抓命名慣例與可借用素材

```python
from query_helpers import list_asset_groups, list_asset_group_assets
groups = list_asset_groups(client, customer_id, campaign_id)
```

- 命名慣例通常是 `[~到期日]起始日-活動名稱`（例如 `[~8/16]20260803-全館活動`），新群組照這個
  規律取名。
- 找目前 `ENABLED` 的那組（或最近一組），看它的到達網址、圖片、YouTube 影片，作為新群組的
  借用基礎——**不用重新上傳，直接引用既有 asset 的 resource_name**，細節見
  `references/api_gotchas.md` 第 1-2 節。這一步務必用 `list_asset_group_assets()`（已內建
  `YOUTUBE_VIDEO`），不要自己手寫查詢漏掉影片欄位。
- **同時查一次 `asset_group_signal`（目標對象信號：搜尋主題關鍵字＋目標對象），這是跟
  `AssetGroupAsset` 完全不同的資源表**，很容易顧著複製圖片/文案就忘了它，導致新群組在
  後台顯示「未提供任何目標對象信號」。做法見 `references/api_gotchas.md` 第 9 節。

### 3. 準備文案，控制字數上限

用 `ads_client.check_limit(text, limit)` 逐條檢查，不要用 `len()`：

| 類型 | 上限 |
|---|---|
| 標題 | 30 |
| 長標題／說明 | 90 |
| Sitelink 連結文字 | 25 |
| Sitelink 說明（各行） | 35 |
| Callout 文字 | 25 |

Molly 要求「填滿」時，資產群組文案上限是 **15 標題／5 長標題／5 說明**。

### 4. 準備圖片／影片

Google Ads API 規定資產群組**就算設 PAUSED，也至少要 1 張橫式（`MARKETING_IMAGE`）+
1 張方形（`SQUARE_MARKETING_IMAGE`）**才能建立成功，見 `references/api_gotchas.md` 第 1 節。

沒有新素材時的優先順序：

1. 問 Molly 有沒有新圖，沒有的話問是否可以先借用步驟 2 找到的既有素材（她確認後才借用，
   借用後跟她說清楚「這是暫時借用，你有新圖再換」）。
2. 找特定檔期的新素材，優先看品牌 Drive 裡的「日期資料夾」（例如 `0817-0831`），裡面通常
   有做好的三尺寸官方 banner。
3. 也可以到官網活動頁抓「壓框圖」——注意商品列表頁的 `<img class="primary-image">`
   通常是白底商品照，真正有促銷壓框的版本要從「加入購物車」按鈕的 `data-photo` 屬性抓，
   細節見 `references/api_gotchas.md` 第 8 節。

### 5. 額外資訊（Sitelink／Callout／Promotion）——只有 Molly 有要求才做

「額外資訊」是口語詞，**開工前先問清楚她要哪一種或哪幾種**，並提醒她這是**活動層級**、
會套用到整個 PMax 活動下所有資產群組（跟步驟 2-4 的資產群組層級素材影響範圍不一樣）。

先用 `list_campaign_assets()` 看現況，避免跟既有的重複。三種素材都有原生
`start_date`/`end_date`（`YYYY-MM-DD`），建議直接設定檔期讓它自動到期，不用像帳號裡
過去的做法一樣事後手動 PAUSE。技術細節（`final_urls` 位置、`percent_off` 單位）見
`references/api_gotchas.md` 第 6 節。

### 6. 整理成計畫給 Molly 確認

不要把腳本輸出整段貼給她，整理成她一眼能看懂的摘要，一定要明講：

- 目標活動 + 新群組/額外資訊的完整內容（文案、圖片來源、到達網址）
- **資產群組會建成 PAUSED**；額外資訊如果有設 start_date/end_date 要講清楚起訖
- 哪些素材是**借用既有的**（不是新上傳），借用來源是哪一組
- 影響範圍：資產群組層級（只影響這一組）還是活動層級（套用到整個活動）

### 7. 確認後才正式送出

先不帶 `--apply` 跑一次驗證（`combined_mutate(..., validate_only=True)`），確認沒有
`GoogleAdsException` 再帶 `--apply` 正式送出。組操作的細節（合併 mutate、暫時 ID
互相參照）見 `references/api_gotchas.md` 第 3 節，不要拆成多次個別 service 呼叫。

### 8. Verify 並回報結果

用 `query_helpers.count_asset_group_assets()` 或 `list_campaign_assets()` 查最終狀態
（已內建 `status != 'REMOVED'` 篩選），把數量/內容列給 Molly 確認，附上：

- 後台路徑（活動名稱 > 資產資源群組，或 > 額外資訊）
- 提醒她自己到後台看過沒問題再切 ENABLED（資產群組），或說明額外資訊何時自動到期

---

## 工具

共用邏輯都在 `scripts/`，不要每次重新寫一份 client 設定或字數計算：

- `scripts/ads_client.py` — `get_client()` / `clean_id()` / `weighted_len()` /
  `check_limit()` / `combined_mutate()` / `write_utf8()`
- `scripts/query_helpers.py` — `list_pmax_campaigns()` / `list_asset_groups()` /
  `list_asset_group_assets()` / `count_asset_group_assets()` / `list_campaign_assets()`

實際建立/修改素材的 mutate operation（`AssetOperation` / `AssetGroupAssetOperation` /
`CampaignAssetOperation`）目前沒有包成罐頭函式，因為每次的素材內容都不一樣——直接照
`references/api_gotchas.md` 的寫法現寫現用，組好操作列表後統一丟給 `combined_mutate()`。

## 參考文件

- `references/api_gotchas.md` — 詳細的 API 眉角（圖片門檻、合併 mutate、移除操作、
  額外資訊排程、官網壓框圖抓法、環境雜項），遇到報錯或不確定寫法先查這裡。
- `references/account_ids.md` — 已知品牌 customer_id 對照。

---

## 注意事項

- **不要在這個 skill 裡動預算、出價、受眾、關鍵字**——只負責資產群組內容和活動層級額外資訊。
- 借用既有素材前跟 Molly 確認一次，不要自己直接借用就送出。
- 一次要做多個資產群組或多種額外資訊時，整理成一份計畫讓她一次確認，不要每個都問一輪。
- 遇到 GAQL 欄位名稱不確定，用 schema 內省法（`references/api_gotchas.md` 第 6 節最後）
  直接查證，不要瞎猜欄位名再對著錯誤訊息一一排除。
