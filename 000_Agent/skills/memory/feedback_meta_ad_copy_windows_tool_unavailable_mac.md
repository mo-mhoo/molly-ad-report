---
name: feedback-meta-ad-copy-windows-tool-unavailable-mac
description: meta-ad-copy skill 依賴的 C:\AI小摸\meta_ad_copy_cli.py 在這台 Mac（Molly-AI Agent 專案）不存在，要改用 Meta Ads MCP 手動比照同一套判斷邏輯操作
metadata:
  node_type: memory
  type: feedback
---

`~/.claude/skills/meta-ad-copy/SKILL.md` 裡的 `meta_ad_copy_cli.py`／`meta_ad_copy_tool.py`（`C:\AI小摸` 路徑）是給 Molly 的 Windows 機器用的，在這台 Mac 的「Molly-AI Agent」專案裡找不到對應腳本，`cd "C:\AI小摸"` 這類指令會直接失敗。

**正確做法**：不要嘗試在這台 Mac 找/裝這個工具，改用 Meta Ads MCP（`ads_get_ad_entities` / `ads_get_creatives` / `ads_create_creative` / `ads_create_ad` / `ads_activate_entity`）手動比照 SKILL.md「判斷規則」那節的邏輯自己做：

1. 查來源 ad（`ads_get_ad_entities` level=ad, object_ids=[ad_id]）拿到 name/campaign_id/adset_id/creative_id
2. 查 creative 內容（`ads_get_creatives` creative_ids=[...]，帶 body/title/object_type/effective_object_story_id）判斷是 SHARE(單圖/影片) 還是目錄/輪播結構
3. 查帳號內候選 campaign/adset（`ads_get_ad_entities` level=campaign 全撈一次，再用 level=adset + filtering campaign_id IN [...] 查子adset），依素材內容關鍵字人工比對，不是靠工具自動 match
4. 非目錄/輪播的素材，用 `ads_create_creative` 帶 `object_story_id`（格式 `pageID_postID`，從來源 creative 的 `effective_object_story_id` 拿）建一個全新 creative（不是重用來源 creative_id，避免牽連原廣告），一個素材建一個新 creative 即可，多個目標 adset 可以共用同一個新 creative_id
5. 用 `ads_create_ad` 帶 `creative: {"creative_id": "<新creative_id>"}` 逐個 adset 建立，ad_name 沿用原素材名稱（見 [[feedback_meta_ad_copy_ad_naming]]，不加後綴）
6. 確認後才呼叫 `ads_activate_entity` 逐支開啟，activate 後務必用 `ads_get_ad_entities` 查 `effective_status` 核對（可能是 IN_PROCESS 審核中，不是失敗，見 [[project_meta_ads_creative_duplication]] 提過的「建立後手動activate偶爾不會真的生效」，但 IN_PROCESS 跟真的沒生效要分清楚——IN_PROCESS 通常晚點會自動轉ACTIVE，不用重打）

**Why**：2026-09-06 幫 Molly 在毛孩官網帳號（`1318362572209550`）處理 `/meta-ad-copy 毛孩官網` 時發現這台 Mac 完全沒有這個 skill 依賴的 CLI 工具，只能整套手動重現判斷邏輯。跟 [[feedback_pmax_skill_env_incompatible_mac]] 是同一類問題——skill 原生環境是 Windows，這台 Mac 是另一個執行環境，不是 skill 寫錯，不要去改 skill 本體檔案。

**How to apply**：之後在這台 Mac 上跑 `meta-ad-copy` skill，直接跳過 CLI 呼叫步驟，照上面 6 步用 MCP 手動做，目標 campaign/adset 判斷不明確時（尤其素材內容橫跨多個分類，或候選 campaign 不只一個）用 AskUserQuestion 跟 Molly 確認，不要自己硬猜——這次「指定飼料買一送一」（犬貓通用，找不到專屬飼料 campaign）跟「全館活動類素材要鋪多廣」都是靠問她才選對範圍。
