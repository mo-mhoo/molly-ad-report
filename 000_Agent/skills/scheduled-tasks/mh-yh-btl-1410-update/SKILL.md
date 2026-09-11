---
name: mh-yh-btl-1410-update
description: 14:10 提醒更新全通路 6/18 BTL 數字
---

現在是下午 14:10，提醒用戶：請更新御熹堂與毛孩時代全通路（各 3 個帳號，共 6 個）今天 6/18 的最新 BTL 數字。

步驟：
1. 用 Meta Ads MCP (ads_get_ad_entities) 重新查 6 個帳號今天的 campaign 數據：
   - 御熹堂 官網: 800743741924152
   - 御熹堂 蝦皮: 1479378256009304
   - 御熹堂 momo: 7609150239116011
   - 毛孩時代 官網: 1318362572209550
   - 毛孩時代 蝦皮: 1921433368254905
   - 毛孩時代 momo: 3317877141845136

2. 依 BTL 分類規則（campaign 名稱含 BTL、轉換、performance）計算花費與收益（ROAS = 收益 ÷ 花費）。
   - 毛孩時代 蝦皮與 momo 的 ATL 花費也加進去一起計算總花費（收益維持僅算 BTL）。
   - CPAS 帳號收益來源：catalog_segment_value:omni_purchase。
   - 官網收益來源：action_values:offsite_conversion.fb_pixel_purchase。

3. 將更新後的 6/18 截至目前數字，分別整理成兩份預覽訊息給用戶確認（御熹堂、毛孩時代各一份），**不要自動發送**，等用戶確認後再更新腳本並執行發送。

格式參考（6/18 截至目前）：
官網　花費 $X,XXX（去年全日進度 XX%）　收益 $X,XXX（去年全日進度 XX%）　ROAS X.XX（去年全日 ROAS X.XX）

2025 全日參考數字：
- 御熹堂 官網: spend=11,249, rev=42,726, ROAS=3.80
- 御熹堂 蝦皮: spend=18,750, rev=62,317, ROAS=3.32
- 御熹堂 momo: spend=34,220, rev=155,894, ROAS=4.56
- 毛孩時代 官網: spend=13,708, rev=60,756, ROAS=4.43
- 毛孩時代 蝦皮: spend=33,528, rev=164,372, ROAS=4.90
- 毛孩時代 momo: spend=21,351, rev=77,908, ROAS=3.65