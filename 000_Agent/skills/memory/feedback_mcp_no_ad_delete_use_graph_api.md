---
name: feedback-mcp-no-ad-delete-use-graph-api
description: Meta Ads MCP工具沒有刪除ad的功能,adset滿50支上限需要清舊廣告時要直接呼叫Graph API DELETE
metadata:
  node_type: memory
  type: feedback
---

這台 Mac 上能用的 Meta Ads MCP 工具組裡，**沒有「刪除廣告(ad)」的工具**——能刪除的只有 creative（`ads_creative_delete`）、自訂受眾（`ads_delete_custom_audience`）、目錄商品/商品組/feed（`ads_catalog_*_delete*`）、pixel 事件/參數（`ads_pixel_*_delete`），唯獨 ad/adset/campaign 本身沒有對應的刪除工具。

**Why**：2026-09-11 御熹堂 CPAS momo 帳號某個 adset（「舊客｜PUR(180)」）卡到 Meta 的「每個 adset 最多 50 支廣告（含 PAUSED）」上限，要上新素材前得先清掉幾支過期活動廣告騰位置。原本想找 MCP 工具刪除，翻遍工具列表發現沒有，改成暫停(PAUSED)也沒用——因為上限本來就把 PAUSED 算進去。

**How to apply**：遇到需要真正刪除 ad 物件的情況（不是暫停），改用 Graph API 直接呼叫：

```python
import urllib.request
url = f'https://graph.facebook.com/v21.0/{ad_id}'
req = urllib.request.Request(url, method='DELETE')
req.add_header('Authorization', f'Bearer {token}')
urllib.request.urlopen(req)
```

`token` 從 `.env` 的 `META_ACCESS_TOKEN` 讀（跟 [[project_meta_cpas_app_dev_mode_blocker]] 那次繞過開發模式限制用同一組 token 讀取方式，不要印出來），一支一支呼叫即可，回傳 `{"success":true}` 就是刪除成功。**這是真刪除、不可逆**，一定要先跟 Molly 確認清單再執行（這次她明確授權「過期活動素材可以刪掉，優先刪一年以上的」）。

判斷「過期」的標準：這批廣告的走期到期日大多都還沒滿一年（Meta 帳號的活動素材通常在到期後幾個月內就會被新一輪素材取代，很少放到滿一年才清），所以用 `created_time`（建立時間）超過一年當篩選標準比較實際，不要死板套「到期日+365天」算法。
