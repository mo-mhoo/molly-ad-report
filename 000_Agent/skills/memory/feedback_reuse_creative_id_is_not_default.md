---
name: feedback-reuse-creative-id-is-not-default
description: meta-ad-copy新建廣告時不能預設重用creative_id,單圖/影片要用post id(object_story_id)建立,重用creative_id只是App開發模式錯誤的備案
metadata:
  node_type: memory
  type: feedback
---

meta-ad-copy skill 從素材庫（或任何來源廣告）複製素材到正式 adset 時，建立新廣告的 `creative` 參數**不能預設用 `{"creative_id": "<來源ad的creative_id>"}` 直接重用同一顆 creative**。正確預設要照 SKILL.md「判斷規則」那節走：

- 單圖／單影片（非目錄、非輪播）→ **post id 建立**：`creative: {"object_story_id": "<pageID_postID>"}`，這個 `object_story_id` 從來源 creative 的 `effective_object_story_id` 拿。`ads_create_ad` 工具的 `creative` 參數本身就接受 `object_story_id`，不需要另外呼叫 `ads_create_creative`。
- 有目錄／輪播結構（`product_set_id`／`child_attachments`／CAROUSEL／COLLECTION）→ 整支複製
- **只有遇到 Meta 回錯「廣告創意貼文是由處於開發模式的應用程式所建立」（subcode 1885183）時，才 fallback 改用重用既有 creative_id**（見 [[project_meta_cpas_app_dev_mode_blocker]]）——這是備案，不是預設。

**Why**：2026-09-11 兩次都直接重用了來源 ad 的 creative_id（一次是「膠原3包/UC2/魚油/紅麴」活動素材批次共13支，一次是「UC2_鄭鈞云醫師常態影片」3支），理由是「觀察到帳號歷史上有些重複廣告共用同一個 creative_id」，但這個推論是錯的——那些歷史案例很可能本身就是 subcode 1885183 fallback 的結果，不代表這是建議的預設做法。Molly 在 UC2 常態影片那次用截圖點出「post id 建廣告的方式忘了？」，才發現這個系統性錯誤。用 post id 建立才能讓貼文的讚數／留言正確累積在同一則貼文上；直接重用 creative_id 雖然功能上也能跑，但不是 Molly 要的方式，且讓多支廣告共用同一顆 creative 物件會有「改一支動全部、刪一支影響全部」的副作用（見 SKILL.md 原本就有講這個風險，但那是講 fallback 情況下才能接受的取捨，不是預設要接受的取捨）。

**How to apply**：
1. 之後 `ads_create_ad` 建立非目錄/輪播的新廣告時，預設用 `object_story_id`，不要用 `creative_id`。
2. 只有明確遇到 subcode 1885183 錯誤時才切換成 `creative_id` fallback，並跟 Molly 說明原因。
3. 2026-09-11 活動素材那批（13支，見 [[project_yuxitang_activity_adset_pattern]]）也是用 `creative_id` 重用建的，還沒被 Molly 要求重建，如果之後要處理，要先問她要不要一併修正那批。
4. 相關記憶已更正：[[project_yuxitang_activity_adset_pattern]] 的「共用做法」段落。
