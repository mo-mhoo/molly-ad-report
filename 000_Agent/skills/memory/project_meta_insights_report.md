---
name: project-meta-insights-report
description: Meta 廣告三層成效報表工具（campaign→adset→ad），撈數據入口與已知待辦（token 到期）
metadata:
  type: project
  originSessionId: 537a1b73-b5e4-4515-bd46-2089e379aedc
  modified: 2026-08-05T06:47:21.092Z
---

已在 `C:\AI小摸` 建好「撈數據」通用報表工具，支援 campaign → adset → ad 三層成效表。

**Why:** 使用者要能隨時說「撈數據」就拿到指定日期區間、三層拆解的花費/曝光/點擊/CTR/CPC/購買數/購買金額/ROAS 表格；未來要接官網營收＋GA 做多資料源交叉比對（FB 後台歸因在第三方 cookie 被擋後不準）。

## 檔案

| 檔案 | 用途 |
|---|---|
| `meta_fetcher.py` | 底層抓取（沿用既有），新增：`resolve_zh_date_range()` 中文日期詞解析（今天/昨天/近7天/近30天/這個月/上個月/這週/上週，或 YYYY-MM-DD~YYYY-MM-DD，以 Asia/Taipei 為基準）、`check_account_timezone()`、`fetch_hierarchy()`（一次 level=ad 呼叫在本地組出三層結構）、`_paginate()` 已加 rate limit（code 17/32/613）退避重試 |
| `meta_report.py` | 新檔。`get_report(account_id, access_token, date_range, account_name)` 是對話中直接呼叫的入口，回傳 markdown 表格＋異常標示；也有 CLI：`python meta_report.py --account <id> --range 近7天 --name <顯示名稱>` |

## 已知待辦

- **`META_ACCESS_TOKEN` 是一般 USER token，非永久 System User token，2026-08-18 到期**（用 debug_token 端點查出，scope 還帶了不必要的 `ads_management`）。使用者選擇先沿用，之後再換——8/18 前必須提醒換成永久 `ads_read` System User token，否則排程會突然失效。
- 多資料源比對（官網營收、GA）尚未動工，先設計成 row 格式一致方便未來接入，未建空殼檔案。

## 異常標示規則（重要，勿套錯）

- 「花費但零轉換」只對 **BTL**（campaign 名稱含 BTL/轉換/performance）套用；ATL 流量型不適用，呼應 [[feedback_atl_btl_testing_logic]]
- `MIN_SPEND_FOR_FLAG = 200`：花費低於此金額不列入異常，避免小額測試花費洗版
- API 版本已從過期的 v21.0 升到 v26.0（2026/8 當下最新穩定版）
