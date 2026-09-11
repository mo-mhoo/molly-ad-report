# GA4 商品整體營收交叉檢查

**為什麼要做這一步**：判斷 ATL 關鍵字優化有沒有傷到生意，不能只看該 ATL 活動自己的最後點擊歸因
`conversions_value`——這個數字砍關鍵字後本來就會跟著降。要用 GA4 電商 `itemName` 維度查該商品
「不分流量來源」的整體營收，才能看出砍掉的是不是本來就很少走到購買的低意圖流量（見
feedback_google_ads_atl_keyword_optimization 的驗證陷阱一節）。

## 現況：這是 ad hoc 查詢，`ga4_fetcher.py` 目前沒有這個維度

專案的 `ga4_fetcher.py`（`C:\AI小摸\ga4_fetcher.py`）目前只支援管道分組／來源媒介／到達頁面／日期／週／月
這幾個維度，metrics 固定是 `sessions/totalUsers/engagedSessions/conversions/totalRevenue`，**沒有
`itemName` + `itemRevenue` 這組電商維度**。不要假設它有、也不需要為了這一個覆盤用途去擴充主程式，
直接用它現成的 `_client()` 建 client，自己組一個一次性的 `RunReportRequest`：

```python
import sys
sys.path.insert(0, r"C:\AI小摸")
from datetime import date
from ga4_fetcher import _client
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

client = _client(r"C:\AI小摸\<service-account-json-檔名>")  # 路徑見 .env 的 GA4_SERVICE_ACCOUNT_JSON
request = RunReportRequest(
    property=f"properties/{property_id}",
    dimensions=[Dimension(name="itemName")],
    metrics=[Metric(name="itemsPurchased"), Metric(name="itemRevenue")],
    date_ranges=[DateRange(start_date=str(date_from), end_date=str(date_to))],
    limit=100000,
)
response = client.run_report(request)
```

篩出這次覆盤品項對應的 `itemName`（跟關鍵字/活動命名的品項要對上，不確定就列出全部品項讓 Molly 確認
哪個是對應商品），比較異動前後兩個區間的 `itemRevenue`，跟該 ATL 活動自己算出來的 `conversions_value`
變化並列呈現。

## 已知 Property ID

| 品牌 | GA4 Property ID |
|---|---|
| 毛孩時代 | `317413198`（`.env` 的 `GA4_PROPERTY_ID` 預設值） |
| 御熹堂 | 尚未確認——問 Molly 或到 GA4 後台「系統管理 > 資源詳情」查 |

## 這一步做不到時怎麼辦

沒有 GA4 憑證、或品項在 GA4 裡對不到明確的 `itemName`（例如品項用 SKU 命名、跟關鍵字用語對不上）時，
不要硬套數字，在覆盤報告裡誠實列成「GA4 交叉檢查：跳過（原因）」，不要編數字或省略這個檢查項目。
