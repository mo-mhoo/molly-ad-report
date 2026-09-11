---
name: project-meta-cpas-app-dev-mode-blocker
description: Meta 廣告建立卡 subcode 1885183（META_ACCESS_TOKEN 掛的 App 在開發模式）；已找到解法——重用既有 creative_id 建新廣告即可繞過，不需要等 App 切 Live
metadata: 
  node_type: memory
  type: project
  originSessionId: d7936907-8c0b-4938-b450-620d5527e899
  modified: 2026-08-19T08:42:15.803Z
---

用 [[meta-ad-copy]] skill 複製廣告時，反覆遇到同一個 Meta API 錯誤：

> 廣告創意貼文是由處於開發模式的應用程式所建立。必須設定為公開才可以建立此廣告。(subcode 1885183)

**已知出現過的 session／時間**：
- 2026-08-05「Meta 廣告批次複製工具」— 首次診斷，判定是帳號/App 設定層級限制，工具端無法繞過（結論不完整，見下）
- 2026-08-14「寵物飼料行銷文案」、2026-08-18「御熹堂 CPAS momo 活動」— 都只是複述同一個結論，沒人再往下測試
- 2026-08-19 毛孩時代_CPAS_蝦皮（1921433368254905）12 筆全部卡在這個錯誤，這次真正查清楚並解決

**根因**：用 `GET /debug_token`（`input_token=META_ACCESS_TOKEN`）查到 `.env` 這組憑證掛在 App **`tsa000(M)`（App ID: 1535543074833417）**，這個 App 本身在 Development Mode。只要透過這組 token **建立新的 creative／發佈新貼文**（不管是複製整支廣告、用 `object_story_id` 建 creative、還是用全新 `object_story_spec` 建 creative），Meta 都會擋——2026-08-19 實測：把素材內容簡化到只剩 page_id + 純文字連結，一樣被擋，證明跟來源貼文內容無關，純粹是「這個 App 不能發新貼文」。

**✅ 解法（已驗證成功，不需要等 App 切 Live）**：**跳過建立新 creative 這一步，直接重用來源廣告既有的 `creative_id`** 去建新廣告：

```python
_post(f'act_{account_id}/ads', {
    'name': name,
    'adset_id': target_adset_id,
    'creative': json.dumps({'creative_id': existing_creative_id}),  # 沿用來源廣告的 creative.id，不是 object_story_id
    'status': 'PAUSED',
})
```

因為沒有呼叫 `/adcreatives` 建新 creative、沒有觸發「發佈新貼文」的檢查，這條路徑完全繞過 App 開發模式限制。2026-08-19 用這個方法把毛孩蝦皮卡住的 12 筆全部成功建立（PAUSED）。

**這個解法是怎麼找到的**：Molly 在 claude.ai（另一個介面，用官方 Meta Ads MCP）問到同樣的問題，那邊的 `ads_create_ad` 工具因為介面設計上**不管有沒有 source_ad_id 都強制要求明確帶 `creative` 參數**，沒有「整支複製」這種高階捷徑，反而自然而然只能走「填入既有 creative_id」這條路，意外踩對了正確做法。詳見 [[feedback_dont_anchor_on_past_diagnosis]]——前面三次 session 都被 8/5 那次「無法繞過」的結論定錨，沒人再測試替代路徑。

**How to apply**：下次 execute 又跳這個 subcode（app_id 1535543074833417 / `tsa000(M)`），直接改用上面「重用既有 creative_id」的做法建新廣告，不用再走 `meta_ad_copy_tool.py` 現有的 `copy` 或 `post_id` 兩種 mode（兩者都會建新 creative、一律會被擋），也不用等 App 切 Live 才能動作。長期可以考慮把這個方式加進 `meta_ad_copy_tool.py` 當作正式的第三種 mode（例如 `reuse_creative`）。
