---
name: feedback-html-report-format
description: 使用者確認滿意的 HTML 廣告週報/月報格式規範與 Meta 漏斗指標算法
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 07d81905-cac3-4a21-a645-1333a5f57a07
  modified: 2026-08-10T08:20:08.849Z
---

2026-08-10，毛孩時代 Google+Meta MTD YoY 與 Meta WoW 週報做到這個版本後使用者說「還不錯」，記錄下確認有效的格式，之後做同類報表可以直接沿用。

**規則本體：**
1. **優先用 API 即時撈數據，不要憑截圖手動謄寫**——本次先用截圖數字建報表，使用者主動問「Google Ads 自動排呈報表你去撈取?」，改用 Google Ads API + Meta Ads MCP 直接撈之後，花費/CTR/CPC/CPM 都跟原截圖精確吻合，收益類指標反而更準（見 [[project_google_ads_integration]] 的 conversions 欄位陷阱）。
2. **報表結構**：Google 和 Meta 合併在同一個 HTML 頁面裡（不要分開好幾個檔案），用 `.block` + `.platform` 卡片樣式區分平台或 ATL/BTL，週比較和月度 YoY 各自成一個 `<section>`，中間用分隔線隔開。
3. **WoW 週表要有完整 13 個指標**，不能只列花費/ROAS 這種精簡版：花費、收益、ROAS、CPA、CVR、AOV、點>車%、車>買%、購物車CPA、CTR、CPC、CPM、頻次。使用者對照原本內部截圖格式抓出「你指標有少耶」，之後排版一定要對齊這個欄位清單。
4. **WoW 增減幅（%Δ）不要用綠/紅底色徽章**，使用者說「好醜」；改成純文字＋顏色箭頭（`▲/▼`），無背景色塊。
5. **長註解不要塞進表格儲存格**（會撐開/擠壓欄寬造成「跑版」），一律移到區塊底部的 `.footnote`，儲存格內只放簡短提示（例如「見備註*」）。
6. 手機窄螢幕要把表格轉成卡片式堆疊（`@media max-width:700px` 把 `<tr>` 變成 block、`<td>::before` 顯示 `data-label`），不要單純讓表格橫向 overflow-scroll，會讓使用者只看到單一欄位。

**Meta 漏斗指標算法（Meta 沒有直接欄位，要用 `omni_add_to_cart` 自己算）：**
- 點>車% = 加入購物車數(`omni_add_to_cart`) ÷ 點擊數(`clicks`) × 100
- 車>買% = 購買數(`actions:omni_purchase`) ÷ 加入購物車數 × 100
- 購物車CPA = 花費 ÷ 加入購物車數
- 頻次（多活動加總後的整體平均）不能直接加總每個活動的 frequency 取平均，要用 `impressions/frequency` 還原出約略 reach，再用 `總impressions ÷ 總reach_approx` 算出加權頻次。

相關：[[project_google_ads_integration]]、[[project_daily_report]]
