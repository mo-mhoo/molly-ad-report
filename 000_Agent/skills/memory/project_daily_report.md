---
name: project-daily-report
description: 每日 Meta 廣告日報自動化：御熹堂＋毛孩時代，每天 9AM 發至 Google Chat
metadata: 
  node_type: memory
  type: project
  originSessionId: 9287c16c-db21-4702-8337-bda202b1f9ae
---

已建立每日 Meta 廣告成效日報自動化系統。

**Why:** 用戶需要每天早上看 御熹堂（3帳號）與毛孩時代（3帳號）的成效摘要、7日異常偵測、AI 建議。

**How to apply:** 後續若需要調整帳號清單、閾值、訊息格式，直接編輯 `C:\AI小摸\daily_meta_report.py`。

## 系統組成

| 檔案 | 用途 |
|---|---|
| `C:\AI小摸\daily_meta_report.py` | 主腳本：抓資料 → 異常偵測 → Claude 分析 → Google Chat |
| `C:\AI小摸\run_daily_report.bat` | 啟動器（Task Scheduler 呼叫此 .bat） |
| `C:\AI小摸\daily_report.log` | 執行日誌（每次追加） |

## 排程

- Windows 工作排程器任務名稱：`MetaAds每日日報`
- 時間：每天 09:00（台灣時間）
- Python 路徑：`C:\Users\Molly Ho\AppData\Local\Programs\Python\Python312\python.exe`

## 監控帳號 ID

| 客戶 | 帳號名稱 | Account ID |
|---|---|---|
| 御熹堂 | 御熹堂_官網 | 800743741924152 |
| 御熹堂 | 御熹堂_CPAS_momo | 7609150239116011 |
| 御熹堂 | 御熹堂_CPAS_蝦皮 | 1479378256009304 |
| 毛孩時代 | 毛孩時代_官網 | 1318362572209550 |
| 毛孩時代 | 毛孩時代_CPAS_momo | 3317877141845136 |
| 毛孩時代 | 毛孩時代_CPAS_蝦皮 | 1921433368254905 |

## 待完成

- `META_ACCESS_TOKEN` 還未填入 `.env`（需 Meta Business Manager 系統使用者 token）
- Google Chat Webhook 已設定好

## 複用

- `meta_fetcher.py` 的 `fetch_by_date()` — 不動
- `anthropic` 套件 + `load_dotenv()` — 同 ad_agent.py 模式
- Claude 模型：`claude-haiku-4-5-20251001`（便宜快速）
