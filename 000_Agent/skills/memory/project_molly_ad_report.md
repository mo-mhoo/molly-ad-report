---
name: project-molly-ad-report
description: molly-ad-report Streamlit app 專案筆記：預算排程功能的已知 bug 與修法
metadata: 
  node_type: memory
  type: project
  originSessionId: 5553df52-a07a-4a48-97b5-86d59a063c5c
---

## 專案概況
- Repo: https://github.com/mo-mhoo/molly-ad-report
- 本機路徑：C:\tmp\molly-ad-report\app.py
- 部署：Streamlit Cloud（molly-ad-report.streamlit.app），push 到 main 自動部署
- 功能：Meta 廣告帳戶數據查看 + 預算排程建立/修改/刪除 + 快速加減碼 + 素材維度分析

## Meta API 錯誤碼筆記（create_budget_schedule handler 順序，不可更動）
- **3858090**：time_end 超出 campaign scheduled period。① 抓 stop_time 縮短 → ② 帶 daily_budget 重試 → ③ 回明確錯誤（不走 adset，adset 不支援 budget_schedule_specs）
- **3858199 / 3858175**：ASC/特殊活動建排程需帶 daily_budget 欄位（handler 必須在 "Invalid parameter" catch-all 之前）
- **2446489**：budget_rebalance_flag 在 v7.0 後廢棄，payload 絕對不能帶此欄位
- **1885648**：ASC campaign 改 daily_budget 失敗，需走 adset 層級（adjust_campaign_budget 用，非 schedule）
- **"Invalid parameter" catch-all**：必須放在所有已知 subcode handler 之後，且需排除 3858090/3858199/3858175，否則會攔截有專屬處理邏輯的錯誤（2026-07-17 教訓）
- **budget_schedule_specs 只支援 campaign 層**：不要對 adset 發此請求，一律 Invalid parameter
- `budget_rebalance_flag` 字段已廢棄，不可在任何 API payload 傳遞
- CBO campaign：所有 adset 無自己預算，3858090 時直接回 CBO 專屬訊息
- CPAS 協作廣告：3858090 + 所有方式失敗，需手動在 Meta 後台設定

## session_state 資料共用設計（2026-07-07）
- `campaigns` 與 `adj_campaigns` 現在同步：
  - 強制重整時同時寫 `campaigns` 和 `adj_campaigns`
  - 快速加減碼「載入/重新整理」時也同時寫兩個
- 素材維度分析的 adset 資料存在 `df_ads_as`、`df_ads_as_comp` 等

## 功能說明

### 預算排程區塊
- 使用 `st.dataframe(on_select="rerun", selection_mode="multi-row")`，支援 Shift+Click
- 全選/取消全選用 `_sched_btn_set` flag + `sched_sel_v` version counter 控制 rerun 時不被 dataframe 空 state 覆蓋
- 調整幅度 UI 放在：目標ROAS 下方 → 全選按鈕上方 → dataframe → 確認建立排程

### 快速加減碼區塊
- 同上 st.dataframe 多選設計
- ASC campaign 改預算：campaign 層失敗後自動試 adset 層
- 只顯示有 `daily_budget` 的活動（lifetime budget 跳過）

### 素材維度分析
- level=ad 和 level=adset 資料同時抓
- 維度彙總表 + Adset flat table + Ad flat table
- `parse_ad_dims` 支援兩種命名格式：`_` 分隔 和 `｜` 分隔混合

### KPI 表（build_table_html）
- 支援 comp_m（WoW/DoD/前期）、mom_m（MoM）、yoy_m（YoY）、mtd_m（MTD 絕對值欄）
- MTD：本月1日到今日，若本期已是 MTD 則不顯示
- 總計列：花費 > 連結點擊 > ROAS > 廣告收益 > 觸及成本

## 日期選項（2026-07-07）
- 今日、昨天、過去7天、本月至昨日（1號到昨天）、本月（含今日）（1號到今天）、自訂

## 已修 Bug 紀錄（2026-07-07）
- _build_flat_table 的 _agg 函數回傳 pandas Series 導致 `if cr` ValueError → 改 `.to_dict()`
- 調整幅度 & 方向 UI 位置：從最頂端移到目標ROAS 下方
- Rate limit 錯誤分開計數，顯示「⏳N 筆遭 rate limit 跳過」而非靜默消失

## 隊友協作注意
- push 前必須先 `git pull origin main --no-rebase`，隊友會頻繁推版
