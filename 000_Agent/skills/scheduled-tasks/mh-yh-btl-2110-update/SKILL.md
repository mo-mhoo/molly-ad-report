---
name: mh-yh-btl-2110-update
description: 21:10 更新全通路 6/18 最終 BTL 數字並發送 Google Chat
---

現在是下午 21:10，請更新御熹堂與毛孩時代全通路（各 3 個帳號，共 6 個）今天的最新 BTL 數字，並在用戶確認後發送到 Google Chat。

步驟：
1. 先讀取 TSA 廣告操作規範 skill：anthropic-skills:tsa-meta-ads-operations

2. 用 Meta Ads MCP (ads_get_ad_entities) 並行查 6 個帳號今天的 campaign 數據（date_preset: "today"，level: campaign，fields: ["id", "name", "amount_spent", "purchase_roas", "result_values"]）：
   - 御熹堂 官網: 800743741924152
   - 御熹堂 蝦皮: 1479378256009304
   - 御熹堂 momo: 7609150239116011
   - 毛孩時代 官網: 1318362572209550
   - 毛孩時代 蝦皮: 1921433368254905
   - 毛孩時代 momo: 3317877141845136

3. 依 BTL 分類規則（campaign 名稱含 BTL、轉換、performance）計算花費與收益（ROAS = 收益 ÷ 花費）：
   - 御熹堂：只計算 BTL 花費與收益
   - 毛孩時代 蝦皮與 momo：ATL 花費也加進去一起計算總花費（收益維持僅算 BTL/轉換類）
   - 官網收益來源：result_values 中 indicator 為 action_values:offsite_conversion.fb_pixel_purchase 的 value
   - CPAS 帳號（蝦皮/momo）收益來源：result_values 中 indicator 為 catalog_segment_value:omni_purchase 的 value
   - 若 result_values 為 "mixed" 或只有 indicator 沒有 values，該 campaign 收益計為 0

4. 整理成預覽訊息給用戶確認，**不要自動發送**，等用戶確認後再執行。

預覽格式（今日截至目前）：
官網　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS X.XX）

5. 用戶確認後，用 WebFetch 工具 POST 兩則訊息到 Google Chat Webhook：

   Webhook URL：
   https://chat.googleapis.com/v1/spaces/AAQAIUKUO_Q/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=DjJugp5_VhGI8xdLNGURKaV8u_4JzszugnBW4k5QofY

   第一則（御熹堂）：
   📊 *御熹堂 BTL 成效 | [今日日期] 截至目前*

   官網　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS X.80）
   蝦皮　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS 3.32）
   momo　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS 4.56）

   第二則（毛孩時代）：
   📊 *毛孩時代 BTL 成效 | [今日日期] 截至目前*

   官網　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS 4.43）
   蝦皮　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS 4.90）
   momo　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS 3.65）

2025 全日參考數字：
- 御熹堂 官網: spend=11,249, rev=42,726, ROAS=3.80
- 御熹堂 蝦皮: spend=18,750, rev=62,317, ROAS=3.32
- 御熹堂 momo: spend=34,220, rev=155,894, ROAS=4.56
- 毛孩時代 官網: spend=13,708, rev=60,756, ROAS=4.43
- 毛孩時代 蝦皮: spend=33,528, rev=164,372, ROAS=4.90
- 毛孩時代 momo: spend=21,351, rev=77,908, ROAS=3.65