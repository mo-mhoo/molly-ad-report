---
name: project-ga4-integration
description: GA4 已串接進 C:\AI小摸 的 app.py，用 Service Account 驗證；毛孩時代、御熹堂 property ID 皆已測試成功
metadata: 
  node_type: memory
  type: project
  originSessionId: d5a98db9-fc72-4f0a-b2c5-a144d066e991
  modified: 2026-08-10T07:47:07.082Z
---

## 各品牌 GA4 Property ID
- 毛孩時代：`317413198`（`.env` 預設值，daily_meta_report.py 排程用這個）
- 御熹堂：`317428057`（2026-08-10 確認，同一把 Service Account 金鑰即可存取，不用另建）

## 現況（2026-08-05 完成串接並測試成功）
- 新增 [ga4_fetcher.py](../../../../AI小摸/ga4_fetcher.py)，介面比照 `meta_fetcher.py` / `google_ads_fetcher.py` 的 `fetch_dimension()`
- 已整合進 `app.py` 側邊欄「📥 從 GA4 抓數據」區塊
- 支援維度：管道分組、來源/媒介、到達頁面、日期、週、月；指標：sessions/users/conversions/revenue/engagement_rate/bounce_rate/conversion_rate

## 驗證方式：Service Account（不是 OAuth）
**Why:** daily_meta_report.py 每天排程自動跑，OAuth refresh token 在「測試」狀態的 GCP app 會 7 天過期，service account 沒有這個問題，適合無人值守自動化。

## GCP 專案與憑證
- GCP 專案：「GA4 MCP」，專案 ID `ga4-mcp-499806`（使用者原本就有這個專案，可能之前做過 MCP 用途）
- 服務帳戶：`ga4-mcp-ai@ga4-mcp-499806.iam.gserviceaccount.com`
- 金鑰檔實體位置：`C:\AI小摸\金鑰\Google Cloud\ga4-mcp-499806-f25ec2d4440c.json`（已加進 .gitignore 的 `金鑰/` 規則，不會被 commit）
- `.env` 已寫入 `GA4_SERVICE_ACCOUNT_JSON`（指向上述路徑）與 `GA4_PROPERTY_ID=317413198`（毛孩時代 GA4 資源）
- 該服務帳戶已在 GA4 資源存取權管理被加為「檢視者」

## How to apply
- 未來若要幫其他品牌串 GA4，同一個服務帳戶只要在對應 GA4 資源的存取權管理加「檢視者」即可重複使用，不用重建服務帳戶或金鑰
- **依品牌分別撈取**：app.py「📥 從 GA4 抓數據」區塊的 property_id 是文字輸入框，`.env` 只是預設值——直接在該欄位換填對應品牌的 property ID（見上方對照表）就能切換，不用改 `.env`、不用重啟。用 Python 腳本查詢時同理，把 `property_id` 參數換成對應 ID 即可
- Analytics **Admin API**（用來自動列出某把金鑰能存取哪些 GA4 資源）在這個 GCP 專案（`ga4-mcp-499806`）還沒啟用；目前是靠手動問使用者要哪個 property ID 來確認品牌對應
- 尚未串接：daily_meta_report.py / ads-weekly-report skill 的報表整合（等使用者確認要呈現哪些 GA4 指標，例如是否要跟 Meta 花費對比算 ROAS）
