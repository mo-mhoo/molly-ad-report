---
name: feedback-meta-ad-copy-staging-adset-not-final
description: meta-ad-copy 素材若已存在於「Ad 素材庫」staging adset,不能直接對它 activate,一定要先提案正式目標 adset
metadata:
  node_type: memory
  type: feedback
---

御熹堂（以及可能其他 TSA 帳號）的帳號結構裡有一個專門的「Ad 素材庫」campaign（例如御熹堂官網 `120210717941760735`），底下的 adset（例如「Ad 素材庫｜活動2」`120253537789950735`）是**素材上稿暫存區**，不是實際投放位置。Molly 用 `/meta-ad-copy` 給素材名稱時，即使查到廣告已經用她給的確切名稱建在這個 adset 裡（PAUSED），那也只代表「素材已經準備好」，不代表「已經決定要投去哪裡」。

**Why**：2026-09-11 處理 `[~9/15]`/`[~9/18]` 這批活動素材時，查到 4 支素材已經以完全相同名稱存在「Ad 素材庫｜活動2」，我一看到已存在就直接對它們呼叫 `ads_activate_entity`，被 Molly 即時打斷糾正：「你還沒提案要放哪些正式 ad set」。素材庫的 ad 只是「這支素材長怎樣」的庫存卡，不是要投放的 adset，直接開它等於廣告只在素材庫裡空轉，不會真的觸及任何受眾。

**How to apply**：
1. 查到素材已存在於名稱含「素材庫」「Ad 素材庫」的 campaign/adset 時，視同「素材已就緒但尚未鋪到正式 adset」，不要因為找到同名 ad 就跳過 [[feedback_meta_ad_copy_adset_suggestion_list]] 那一步的候選 adset 分析。
2. 正常走 SKILL.md 步驟 3：查帳號內候選正式 campaign/adset（通常是「轉換BTL｜新客｜活動」「轉換BTL｜舊客｜活動」「轉換BTL｜RT｜活動」這類 always-on 活動型 campaign，如果該品項有專屬 campaign 如 UC2 的「轉換BTL｜ASC｜UC2」也要納入候選），列出候選清單附判斷依據給 Molly 確認。
3. 用素材庫裡那支 ad 的 `creative_id` 直接建新 ad 到正式 adset（`ads_create_ad` + `creative: {"creative_id": "<素材庫ad的creative_id>"}`），素材庫的原 ad 保持原樣不動、不用管它的狀態。
4. 御熹堂官網帳號具體的「新客/舊客/RT/品項專屬」adset 對照，見 [[project_yuxitang_activity_adset_pattern]]。
