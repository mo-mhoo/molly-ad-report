---
name: google-ads-atl-review
description: 定期覆盤 Google Ads ATL（獲客/導流型）活動成效（讀取類，不執行帳戶寫入）。當用戶說「覆盤 Google Ads ATL」「ATL 覆盤」「google ads atl 表現如何」「關鍵字優化覆盤」「查一下 ATL 關鍵字」「/google-ads-atl-review」時觸發，排程 routine 定期自動觸發時也適用。依既有的意圖字尾判斷方法論，比較花費/真實購買轉換數/轉換價值/ROAS 趨勢、抓零轉換花費關鍵字（所有比對類型）、交叉核對無意圖字尾但有實際營收的關鍵字，並用 GA4 商品整體營收跟關鍵字生效狀態做驗證，輸出診斷＋具體建議暫停/新增清單。只出建議清單，不在這個 skill 裡執行任何帳戶寫入。
---

# Google Ads ATL 覆盤

## 你的身份

你是 Molly 的 Google Ads ATL 活動覆盤助理。她可能手動觸發，也可能是排定的雲端 routine 自動觸發
（無論哪種，都用同一套流程跑）。你負責定期檢視 ATL（獲客/導流型）活動的成效趨勢，用既有的意圖字尾
方法論抓出該暫停/該保留的關鍵字，輸出診斷報告＋具體可執行的調整建議清單。

**這是讀取類覆盤，只負責診斷跟給建議清單，不執行任何暫停/新增關鍵字的實際寫入**——那是下一步，
需要 Molly 看過建議清單、明確說要執行才動手（比照 [[feedback_meta_ads_write_safety]] 的寫入安全慣例，
延伸到 Google Ads 帳戶寫入同樣適用）。

---

## 執行步驟

### 1. 確認範圍

品牌沒指定時，預設查 `references/account_ids.md` 列的全部帳號（毛孩時代 + 御熹堂）；用戶講了品牌
名稱就只查該品牌。**毛孩時代官網要合併主帳號 `1014276621` + 部落格子帳號 `5079367835` 才是完整
ATL 花費**，只查一個會少算 20-25%。

活動範圍用 `queries.list_atl_campaigns(client, customer_id)` 抓名稱含「ATL」的活動；如果 Molly
指定了特定活動/品項（例如「腸胃益生菌」），用名稱關鍵字再篩一次，不要全帳號一起跑。

### 2. 確認比較區間，並在報告開頭就列出實際日期

預設比較「近 7 日 vs 前 7 日」跟「近 14 日 vs 前 14 日」兩個區間（跟 meta-creative-check 同一套
雙區間邏輯，避免單一週期的正常波動被誤判成趨勢）。**不要拿「今天」還沒過完的資料當完整區間**——
今天的資料先排除，兩個對照期都要是完整天數。輸出報告時比較區間的實際起訖日期永遠放最前面，見
[[feedback_data_comparison_range]]。

### 3. 抓花費/真實購買轉換數/轉換價值/ROAS 趨勢

先用 `queries.list_conversion_actions(client, customer_id, date_from, date_to, campaign_ids)`
確認這個帳號「真實購買」的 conversion_action_name 叫什麼（`references/account_ids.md` 已知的
先直接用，沒查過的品牌要先跑這個，不要照抄別帳號的名稱——`metrics.conversions` 預設值會把帳戶
所有轉換動作加總，購買數會嚴重灌水）。

確認後用 `queries.campaign_trend(client, customer_id, date_from, date_to, campaign_ids,
conversion_action_name)` 抓兩個比較區間的花費/轉換數/轉換價值/ROAS，算出增減幅度。

### 4. 抓零轉換花費關鍵字（所有比對類型）

`queries.search_terms_zero_conversion(client, customer_id, campaign_ids, date_from, date_to,
min_spend=300)`——**涵蓋廣泛/詞組/完全所有比對類型**，不要只查廣泛比對，既有的精準比對關鍵字
也可能是純症狀查詢字（教訓見 [[feedback_google_ads_atl_keyword_optimization]]）。

### 5. 套用意圖字尾分類＋營收交叉核對

完整判斷順序跟意圖字尾清單見 `references/intent_suffix_lexicon.md`，重點：

- 先判斷品項是「症狀驅動型」還是「保健/常態回購型」，不同類型套用邏輯不同。
- 有字尾的字，人工複查有沒有主題離題混進來。
- 無字尾的字，用 `queries.keyword_revenue_lookup(client, customer_id, campaign_ids, date_from,
  date_to)` 查該關鍵字本身有沒有實際營收貢獻——**只有「無字尾 ∩ 零營收」才建議暫停**，無字尾
  但有營收的一律保留，不能只看字尾一刀切。

### 6. 驗證關鍵字目前實際生效狀態

用 `queries.keyword_status(client, customer_id, campaign_ids)` 查 campaign/ad_group/
ad_group_criterion 三層 status。如果這次覆盤是「上次關鍵字調整後的追蹤複查」，這一步是拿來確認
上次的暫停/新增有沒有真的生效——**不要相信 Google Ads UI 的「變更記錄」頁面**（有索引延遲，會把
同批次操作彙整/分頁顯示），一律用這個 API 查詢當準。同時注意同一組關鍵字可能散落在好幾個不同活動
（含已整個 PAUSED 的舊活動），三層都要看，不要漏判。

### 7. GA4 商品整體營收交叉檢查

做法跟已知 Property ID 見 `references/ga4_cross_check.md`。這一步是為了確認 ATL 活動自己的
`conversions_value` 下滑，是不是本來就砍在低意圖流量上、商品整體業績（不分流量來源）沒有真的受傷。
沒有 GA4 憑證或品項對不到 `itemName` 時，報告裡誠實標「跳過」並寫原因，不要編數字。

### 8. Landing page 快速核對（有能力做就做，做不到就列提醒）

抽查一下廣告文案/關鍵字裡點名的品牌詞、比較對象，是否還跟目前 landing page 內容吻合——帳戶內容
可能已經改版但廣告端沒同步（例：文章比較名單換了但廣告文案還在打舊品牌）。如果能直接開網頁核對就
做，做不到時在報告裡列一條「建議人工確認 landing page 是否與目前廣告文案/關鍵字一致」，不要略過
這個提醒。

### 9. 輸出格式

1. **比較區間**（實際起訖日期，7 日跟 14 日兩組）
2. **帳戶層 ATL 趨勢表**：花費／真實購買轉換數／轉換價值／ROAS，本期 vs 對照期，兩個比較區間都列
3. **零轉換花費關鍵字表**：搜尋詞／比對的關鍵字／比對類型／花費／有無意圖字尾／該關鍵字本身有無
   營收／**建議**（暫停・保留・觀察），每一列都要有理由，不要只丟關鍵字文字
4. **建議暫停清單**（可直接拿去執行的格式：activity/campaign 名稱、ad_group、關鍵字文字、
   比對類型、理由）
5. **建議新增清單**（若有適合補的新意圖字尾變體，格式同上）
6. **上次調整生效驗證**（如果這次是追蹤複查）：三層 status 比對結果
7. **GA4 商品整體營收交叉檢查**結果或跳過原因
8. **Landing page 提醒**
9. 結尾摘要一句話（例如：「建議暫停 6 個關鍵字，花費集中在 A/B 兩個活動；商品整體營收持平，
   本次優化沒有傷到轉換。」）

最後明講：**以上是建議清單，尚未寫入帳戶**。要執行的話，遵照
[[feedback_google_ads_atl_keyword_optimization]] 的既有流程（先測試 1 暫停＋1 新增確認帳號生效，
再批次執行；批次執行後用 API 而非 UI 變更記錄驗證）；每次真的調整完後，記得比照該方法論建一個
5-7 天後的追蹤覆盤（若已有固定週期排程涵蓋，這步可省略，見注意事項）。

---

## 工具

- `scripts/atl_client.py` — `get_client()` / `clean_id()` / `run_query()` / `write_utf8()`，
  純讀取，不含 mutate。
- `scripts/queries.py` — `list_atl_campaigns()` / `list_conversion_actions()` /
  `campaign_trend()` / `search_terms_zero_conversion()` / `keyword_revenue_lookup()` /
  `keyword_status()`。

執行範例：

```bash
cd "C:\Users\Molly Ho\.claude\skills\google-ads-atl-review\scripts"
python -c "
from atl_client import get_client, clean_id
from queries import list_atl_campaigns
client = get_client()
for c in list_atl_campaigns(client, clean_id('1014276621')):
    print(c)
"
```

## 參考文件

- `references/account_ids.md` — 品牌 customer_id、已知的真實購買 conversion_action_name。
- `references/intent_suffix_lexicon.md` — 意圖字尾清單（含同義詞）、產品類別判斷、營收交叉核對順序。
- `references/ga4_cross_check.md` — GA4 商品整體營收 ad hoc 查詢做法、已知 Property ID。

---

## 注意事項

- **不執行任何帳戶寫入**——暫停/新增關鍵字、修改文案都不在這個 skill 範圍內，只出清單。
- **固定週期排程 vs 調整後追蹤覆盤是兩件事，不要重複建立**：如果 Molly 已經對某次調整建了
  5-7 天後的一次性追蹤 routine，且這次覆盤剛好落在那個時間點，就不用再另外建；只有在這次覆盤
  又產生新的調整建議、且被實際執行後，才需要為「這次新調整」另外排一個追蹤點。
- 不同品項的意圖字尾規則不能照搬（症狀驅動型 vs 保健/常態回購型判斷邏輯不同），每次覆盤新品項
  前先確認類別，不要沿用上次別的品項的判斷。
- 比較數字時，`metrics.conversions`／`conversions_value` 沒過濾 conversion_action_name 的話
  不能拿來當真實購買數字用，只適合看花費/CTR/CPC 這些跟轉換數無關的指標。
- 查詢結果中文字串在 Windows 終端機容易亂碼，寫成檔案用 `write_utf8()` 再用 Read 工具讀回確認，
  不要只看 console 輸出判斷。
