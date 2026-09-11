---
name: project-meta-budget-pilot
description: meta-budget-pilot skill（廣告投手預算調整）建立經過與跟 molly-ad-report 的同步關係
metadata: 
  node_type: memory
  type: project
  originSessionId: 0ce0fb6c-d784-44e8-8c1e-f5395aecf7b3
  modified: 2026-08-20T03:20:17.361Z
---

2026-08-20 建立 `meta-budget-pilot` skill（`C:\Users\Molly Ho\.claude\skills\meta-budget-pilot\SKILL.md` + `C:\AI小摸\meta_budget_pilot.py`），依 ROAS(BTL)／互動率(ATL) 7日+14日雙區間趨勢自動診斷 campaign(CBO)/adset(ABO) 該調高調低預算，寫入前一定要 Molly 確認（見 [[feedback_meta_ads_write_safety]]）。

**Why**：Molly 想要一個能依成效趨勢建議調預算、調幅符合安全上限、已知檔期改走預算排程的工具。查證後發現 Meta 官方文件對「重大編輯門檻」只給方向性例子（$100→$101 沒事、$100→$1000 可能重進學習），沒有精確的每小時 % 數字；市面流傳的 20~25% 是業界經驗法則不是官方規範，工具用 ±20% 當保守上限。

**貨幣 offset 安全機制**：TWD 帳號的 `daily_budget` offset 是 1（不是常見的 100），外部已知有 AI agent 誤乘 100 造成 NT$11,168 實際超支的案例。`meta_budget_pilot.py` 內建 `CURRENCY_OFFSETS` 全表，一律先查帳號實際 `currency` 換算，不硬猜。

**跟 [[project_molly_ad_report]] 的同步關係（重要）**：一開始用 developers.facebook.com 文件字面猜寫的 `schedule`／`execute` 寫入邏輯（`/{id}/budget_schedules` 邊、`ABSOLUTE` 類型）完全是錯的，實際能用的機制是 `budget_schedule_specs`（`MULTIPLIER`，只支援 campaign 層，adset 一律 `Invalid parameter`），且有一整串已知 subcode（3858090／3858199／3858175／2446489）跟 ASC/CBO/CPAS 特殊活動的處理順序——這些都是 Molly 自己的 `molly-ad-report`（`C:\tmp\molly-ad-report\app.py` 的 `create_budget_schedule`／`adjust_campaign_budget`）已經在生產環境踩過坑、驗證過的做法。`meta_budget_pilot.py` 現在的寫入函式是直接照那邊搬過來的，**兩邊邏輯要保持同步**——molly-ad-report 那邊之後如果又修了新的錯誤碼處理或行為變化，要記得回頭同步更新 `meta_budget_pilot.py`，不要各自演化出不同版本。

**How to apply**：以後遇到「要不要幫 Meta 廣告帳戶寫某個新的批次/自動化功能」，先查 `molly-ad-report/app.py` 有沒有相關函式已經踩過坑，不要憑官方文件字面重新猜寫法。
