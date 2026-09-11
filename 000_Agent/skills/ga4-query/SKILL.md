---
name: ga4-query
description: 查詢 GA4（Google Analytics 4）網站數據，涵蓋毛孩時代、御熹堂。當用戶說「查 GA4」「這週 GA4 流量」「毛孩時代 GA4 數據」「御熹堂 GA4 數據」「GA4 轉換/工作階段/使用者數」「/ga4-query」時觸發。用 Service Account 直接呼叫 Google Analytics Data API 查詢，不需要瀏覽器、不需要 OAuth 登入。
---

# GA4 數據查詢

## 你的任務

用戶想知道某個客戶（毛孩時代或御熹堂）網站在 GA4 的數據，例如工作階段、使用者數、互動率、轉換（key events）、或依來源/媒介拆解的流量。

## 前置需求

- 憑證檔：repo 根目錄的 `ga4-credentials.json`（Service Account 金鑰，已 gitignore，不會被版控）
- Property ID 對照表：repo 根目錄的 `ga4-properties.json`，格式 `{"毛孩時代": "123456789", "御熹堂": "987654321"}`
- 這兩個檔案缺一不可。若 `ga4-properties.json` 裡對應客戶的 property ID 是空字串，代表還沒設定，跟用戶要（GA4 後台 → 系統管理 → 資源詳情 可以查到，純數字）。

## 執行步驟

### 1. 確認客戶與查詢範圍

從用戶訊息判斷要查哪個客戶（毛孩時代／御熹堂／兩個都要）、日期區間（沒說的話預設「最近 7 天」）、要哪些指標。

### 2. 呼叫 GA4 Data API

用 Bash 工具在 repo 根目錄（`/Users/a111111/Downloads/Molly-AI Agent`）執行以下 Python 程式碼（直接執行，不要存成檔案；把 `__CLIENT__`、`__START__`、`__END__` 換成實際值）：

```python
import json, sys
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension

CRED_PATH = "ga4-credentials.json"
PROPERTIES_PATH = "ga4-properties.json"

with open(PROPERTIES_PATH, encoding="utf-8") as f:
    property_map = json.load(f)

client_name = "__CLIENT__"  # 例如 "毛孩時代" 或 "御熹堂"
property_id = property_map.get(client_name)
if not property_id:
    print(f"❌ ga4-properties.json 裡沒有 {client_name} 的 Property ID，請跟用戶要（GA4 後台 → 系統管理 → 資源詳情）")
    sys.exit(1)

credentials = service_account.Credentials.from_service_account_file(CRED_PATH)
client = BetaAnalyticsDataClient(credentials=credentials)

start_date = "__START__"  # 例如 "7daysAgo" 或 "2026-08-01"
end_date = "__END__"      # 例如 "today" 或 "2026-08-07"

try:
    # 總覽
    overview = client.run_report(RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
            Metric(name="engagementRate"),
            Metric(name="keyEvents"),
        ],
    ))
    # 依來源/媒介拆解
    by_channel = client.run_report(RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionDefaultChannelGroup")],
        metrics=[Metric(name="sessions"), Metric(name="keyEvents")],
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
    ))
except Exception as e:
    msg = str(e)
    if "PERMISSION_DENIED" in msg or "403" in msg:
        print(f"❌ 沒有權限查 {client_name} 的 GA4（property {property_id}）")
        print("請到 GA4 後台 → 系統管理 → 資源存取管理，把這個 service account 加為 Viewer：")
        print("ga4-mcp-ai@ga4-mcp-499806.iam.gserviceaccount.com")
    else:
        print(f"❌ 查詢失敗：{msg}")
    sys.exit(1)

row = overview.rows[0]
sessions, users, eng, key_events = [v.value for v in row.metric_values]
print(f"\n=== {client_name} GA4（{start_date} ~ {end_date}）===")
print(f"工作階段：{sessions}　使用者數：{users}　互動率：{float(eng)*100:.1f}%　轉換（key events）：{key_events}")

print(f"\n來源/媒介拆解：")
print(f"{'管道':<24}{'工作階段':>10}{'轉換':>10}")
for r in by_channel.rows:
    dim = r.dimension_values[0].value
    s, k = [v.value for v in r.metric_values]
    print(f"{dim:<24}{s:>10}{k:>10}")
```

**注意：**
- 憑證與 property ID 對照表都是相對路徑，Bash 執行前要確認 cwd 在 repo 根目錄
- 常用日期字串：`today`、`yesterday`、`NdaysAgo`（如 `7daysAgo`），也可以直接給 `YYYY-MM-DD`
- 兩個客戶都要查時，把上面程式碼包成迴圈跑兩次，或分兩次執行

### 3. 整理輸出

用表格或條列方式呈現總覽數字 + 來源/媒介拆解，中文欄位名稱，不要直接貼 API 回傳的原始物件。

### 4. 常見追問

- 「跟上週比呢？」→ 把 `start_date`/`end_date` 往前推一週再查一次，做對比
- 「轉換是指什麼？」→ GA4 的 key events（原 conversions），是後台設定的關鍵事件，跟 Meta 廣告後台的「轉換」定義不完全一樣，回答時要說明清楚
- 查詢失敗且是權限問題 → 直接告訴用戶要去 GA4 後台加哪個 service account 帳號當 Viewer（見程式碼裡的錯誤訊息）
