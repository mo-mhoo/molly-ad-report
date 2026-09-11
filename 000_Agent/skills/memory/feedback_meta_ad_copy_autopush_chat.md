---
name: feedback-meta-ad-copy-autopush-chat
description: meta-ad-copy的Google Chat推播已改為預設自動送出,不用每次先問Molly確認(推播責任後來又交棒給meta-ad-launch-check,見下方更新)
metadata:
  node_type: memory
  type: feedback
---

**2026-09-11 當天稍晚更新**：這則記錄的「meta-ad-copy 自己直接推播」做法，已經被 [[project_meta_ad_launch_check_skill]] 取代——execute 完現在是交給 `meta-ad-launch-check` 做巡檢+推播，meta-ad-copy 自己不再直接送 Chat 訊息（避免推播兩次）。下面「自動送出不用問」這個原則本身還是對的，只是執行者從 meta-ad-copy 換成 meta-ad-launch-check，細節看新的那則記憶。

`/meta-ad-copy` skill 步驟7(推播上稿結果到「廣告軍團-Meta 上傳素材」Google Chat空間)，Molly 在 2026-09-11 明確要求改成**預設自動推播，不用每次先問她確認**——跟走期開關 routine（步驟6）一樣「自動執行」。SKILL.md 已同步修改。

**Why**：這個功能剛加上時我還維持「先給文字、問她要不要送」的兩段式確認（比照一般寫入類操作的安全習慣），但 Molly 直接問「直接推不用問？」，確認後選擇「以後都不用問，預設就推」。

**How to apply**：之後 `/meta-ad-copy` execute 完成，直接呼叫 `send_chat_notification.py --file <path> --send` 送出，不用先 dry-run 給她看過。只有推播本身失敗（腳本報錯）才需要跟她說並問怎麼處理。

**附帶修好的 bug**：`send_chat_notification.py` 第一版寫的 `PROJECT_ROOT = Path(__file__).resolve().parents[3]` 少算一層，實際指到 `000_Agent` 而非專案根目錄，導致找不到 `.env`。因為先前只測過 dry-run（不會觸發讀 webhook），沒測到 `--send` 才發現。已修正為 `parents[4]`。以後改這類「路徑往上找專案根目錄」的腳本，記得連 `--send`/實際執行路徑都要測過一次，不能只测 dry-run。
