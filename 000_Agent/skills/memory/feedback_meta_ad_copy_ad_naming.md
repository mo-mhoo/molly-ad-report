---
name: feedback-meta-ad-copy-ad-naming
description: meta-ad-copy skill 建新廣告時 ad name 一律沿用素材庫原名稱，不要自己加 adset/campaign 後綴做區分
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 73a775f4-a70c-486a-a24c-4b283b6176c5
  modified: 2026-08-19T08:50:55.624Z
---

[[meta-ad-copy]] skill 建立新廣告時，`ad_name` 要跟素材庫（進稿追蹤表／原素材命名）完全一致，不要自己加 `_BTL舊客貓`、`_ASC腎臟` 這類 adset 或 campaign 後綴去區分同一支素材鋪到多個 adset 的情況。

**Why**: 2026-08-19 幫毛孩官網腎臟粉 4 支素材鋪三種平行 campaign（見 [[project_maohai_official_campaign_structure]]）建了 16 支廣告，自己加了後綴方便她在後台辨識，Molly 當場糾正「ad name 都跟素材庫一樣 不要給我加 ad set 便是」——同一素材建到不同 adset 用同名廣告即可，她自己會靠 adset/campaign 欄位分辨，不需要靠廣告名稱加註。

**How to apply**: 之後不管一支素材鋪到幾個 adset，`items.json` 裡每筆的 `ad_name` 都用原始素材名稱本身，不要拼接任何後綴。如果已經建完才發現加了後綴，用 `ads_update_entity`（`entity_type: "ad"`, `fields: {"name": "<原名>"}`）逐支改回來即可，不需要刪除重建。
