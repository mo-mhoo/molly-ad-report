# 已知帳號 ID（ATL 覆盤範圍）

跟 `google-pmax-asset-group/references/account_ids.md` 同一份帳號體系，這裡只列覆盤預設會查的兩個品牌。若帳號 ID 有異動，兩份文件都要一起更新。

| 品牌 | customer_id | 備註 |
|---|---|---|
| 御熹堂 | `8669832537` | 官網主帳號 |
| 毛孩時代 | `1014276621` | 官網主帳號 |
| 毛孩時代（部落格子帳號） | `5079367835` | `PfM_45057582_毛孩時代`；**毛孩時代官網 ATL 花費要合併這兩個帳號才完整**，只查主帳號會少算約 20-25%（見 project_google_ads_integration）。 |

MCC（代理商中心）帳戶 `4195240594`（騰勢(股)）是 `.env` 裡 `GOOGLE_ADS_LOGIN_CUSTOMER_ID` 的預設值，`atl_client.get_client()` 已內建這個預設，不用另外傳。

其他品牌沒有確認過的 customer_id 時，直接問 Molly 要哪個帳號，不要用名稱猜。

## 已知「真實購買」conversion_action_name

`metrics.conversions` 預設值會把帳戶內所有轉換動作加總，購買數會嚴重灌水（見 project_google_ads_integration）。查真實購買轉換數/ROAS 前，先用 `queries.list_conversion_actions()` 確認這個帳號的真實購買動作叫什麼名字，不要照抄別帳號的名稱。

| 品牌 | 真實購買 conversion_action_name |
|---|---|
| 毛孩時代 | `新網站_完成訂單` |
| 御熹堂 | `購買完成.`（注意結尾有句點） |
