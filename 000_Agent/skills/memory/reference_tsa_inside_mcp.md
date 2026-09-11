---
name: reference-tsa-inside-mcp
description: TSA inside MCP（query_sales/describe_sales_data等）的品牌權限範圍與查詢方法
metadata: 
  node_type: memory
  type: reference
  modified: 2026-08-18T00:58:07.257Z
  originSessionId: c2f9386a-cc60-472a-8939-28315d43d4bd
---

TSA 集團有一組 BI 查詢 MCP（工具前綴 `mcp__85236678-caff-4e5b-955e-cbad445d0808__`），暱稱「TSA inside」，提供銷售/廣告費/庫存查詢：`describe_sales_data`（必須先呼叫，回傳資料新鮮度、可查品牌、有效篩選值、量測維度字典）、`query_sales`（依日期範圍+最多3個groupBy維度聚合營收/數量/成本/毛利/訂單數）、`query_inventory`（即時庫存快照）。

**Why：** 2026-08-18 幫 Molly 查毛孩時代「雞肉貓糧1.5kg/袋(腸胃)」「魚肉貓糧1.5kg/袋(皮膚)」兩支品項業績，發現這條連線的 `describe_sales_data` 回傳可查品牌**只有「御熹堂」**——即使這兩支貓糧的規格名稱明確出現在系統回傳的「有效篩選值」清單裡（TSA集團應該是跨品牌共用同一份規格代碼表），實際用 spec/category 篩選或不篩選直接撈御熹堂全部有交易的 SKU，兩年份資料都查無任何貓糧/寵物商品——御熹堂的實際商品線是人用保健品（魚油、益生菌、關節保健品、膠原蛋白等），跟毛孩時代（寵物品牌）完全不同。直接指定 `brand:["毛孩時代"]` 查詢會被系統明確擋下，錯誤訊息：「你請求的品牌『毛孩時代』不在你目前可查詢的品牌範圍內」。

**How to apply：**
- 「有效篩選值清單裡出現某個值」不代表「目前帳號權限查得到那筆資料」——TSA inside 的 spec/product 字典可能是跨品牌共用池，查詢前務必先用 `describe_sales_data` 確認實際「可查品牌」清單，而不是看到規格名稱在清單裡就假設查得到。
- 遇到「品牌不在權限範圍」的情況，不要重複用同一個帳號嘗試繞過（換篩選條件、換查詢方式都沒用，是帳號層級的權限限制），直接告知使用者需要請管理員開通對應品牌權限。
- 這條連線目前只能查「御熹堂」；毛孩時代等其他 TSA 集團品牌的銷售數據若需要，暫時只能靠使用者手動提供檔案（如 xlsx 匯出）取代。
- `query_sales` 的 `product` 篩選要先用 `groupBy=['product']` 查一次取得 product_id/product_name 對照表，沒有內建的全量 SKU 清單可以直接查。
