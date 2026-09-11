---
name: project-maohai-catalog-product-set
description: 毛孩官網廣告若要綁目錄產品，商品組合要選「排除團購/贈品/經銷」，不能用「所有商品」
metadata: 
  node_type: memory
  type: project
  originSessionId: d6a636ed-d8e1-4474-bb2d-2a923cdb5ffd
  modified: 2026-08-14T02:54:45.925Z
---

毛孩時代_官網帳號的廣告若要搭配目錄（Catalog，如「毛孩時代_Cyber」）顯示商品，「商品組合」欄位要選 **排除團購/贈品/經銷**（50項商品/152種款式），不要選「所有商品」。

**Why:** 「所有商品」目錄裡混了團購、贈品、經銷商專用品項，這些不該出現在一般廣告受眾看到的動態商品廣告裡，Molly 在 2026-08-14 上稿時特別點出這條規則。

**How to apply:** 之後不管是 [[project_maohai_official_campaign_structure]] 提到的哪一種 campaign bucket（BTL舊客/ASC/ATL新客），只要該廣告要綁目錄（asset_feed_spec / product_set_id），優先套用「排除團購/贈品/經銷」這個 product set，除非她另外指定。目前 meta-ad-copy skill 走 post_id 建立流程，不會自動綁目錄；這條規則主要用在她自己在 Ads Manager 手動建 DPA/目錄廣告，或未來 skill 擴充支援目錄廣告時要記得套用。
