---
name: feedback-catalog-sku-product-id-breakdown
description: 查 CPAS 目錄廣告單一 SKU 的花費/曝光/點擊，必須用 product_id breakdown 全帳號撈，不能用廣告名稱關鍵字篩
metadata: 
  node_type: memory
  type: feedback
  originSessionId: a04d77a0-5c8a-4901-8187-96a365a5b8f0
  modified: 2026-08-18T08:43:27.486Z
---

查 CPAS（蝦皮/momo 協作廣告）帳號裡**特定目錄商品/SKU** 的花費、曝光、點擊，正確作法是對整個廣告帳號用 Meta Insights API 的 `breakdowns: ["product_id"]`（level=ad）撈出所有商品的成效，再依 `product_id` 字串（格式為 `"<目錄商品ID>, <商品標題>"`，如 `"15942021921, 【御熹堂】專利 金絲燕窩 珍珠彈力膠原蛋白..."`）比對目標 SKU，加總篩出的列。

**Why：** 動態目錄廣告（「混合商品」「常態」等 campaign）常常不會把商品名/代號打進廣告名稱，只用廣告名稱關鍵字（例如商品代稱 "UC2"）篩選會漏掉這些廣告帶出的曝光/點擊，數字嚴重失真。實測案例（御熹堂蝦皮 UC2／SKU 1051050201，2026-08）：
- 用廣告名稱關鍵字篩選：算出 MTD MoM 花費 +388%、點擊 +951%
- 改用全帳號 product_id breakdown 正確加總：MTD MoM 花費 -5.1%、點擊(連結點擊) -13.1%
兩者方向完全相反，只有 product_id breakdown 才是跟目錄商品綁定的正確數字。

**How to apply：**
- 不要只查特定 campaign 或用 `ads_get_ad_entities` 對單一 campaign 下 breakdown（單一 campaign 撈 product_id 就可能超過 200 列、超出 MCP tool 的 token 上限）。
- 改用 [meta_fetcher.py](../../../../AI小摸/meta_fetcher.py) 同款的 direct Graph API call（`requests` + `_paginate` 邏輯），level=ad、breakdowns=product_id、對整個 ad account 撈，本地端（Python）用 product_id 字串比對 SKU/目錄商品ID 後加總 spend/impressions/clicks/inline_link_clicks，避免透過 MCP 回傳整包資料炸 token。
- 先確認目標 SKU 對應的目錄 product_id（可先在單一已知有掛該 SKU 的廣告上抓一次 breakdown，讀出它的 `product_id` 字串當比對關鍵字）。
- 此方法已在 [project_molly_ad_report.md](project_molly_ad_report.md)（毛孩魚油 YoY 缺口分析）與御熹堂 UC2 案例驗證過，之後只要是「查目錄裡某支商品的成效」都用這套。
