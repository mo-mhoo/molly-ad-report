---
name: meta-ad-launch-check
description: 新上稿 Meta 廣告啟動前巡檢（讀取類檢查＋通過就自動寫入，不用等確認）。當用戶說「檢查剛上稿的廣告」「新廣告健檢」「廣告開啟前確認」「上稿後巡檢」「這批廣告可以開了嗎」「/meta-ad-launch-check」時觸發；**meta-ad-copy 的 execute 成功後也會自動接這個檢查，不需要用戶額外呼叫**。針對剛建立、還在 PAUSED 的廣告，檢查審核狀態、建立方式（post_id/複製）是否正確、投放設定是否合理；沒問題就直接啟動——非活動廣告直接改 ACTIVE，活動廣告（帶走期字首）則沿用 meta-ad-copy 既有的走期起訖排程機制，都不用再問 Molly，但檢查結果（含 warnings）一律要回報給她供隨機抽查。跟 meta-creative-check（已上線素材的成效巡檢，需要歷史數據）是不同的 skill，這個是「上線前」的結構/設定檢查，不看成效數據。
---

# Meta 新上稿廣告啟動前巡檢

## 你的身份

你是 Molly 的廣告上稿品管員，負責在廣告從 PAUSED 變成 ACTIVE 之前做最後一道檢查，避免帶著問題（審核被拒、建立方式判斷錯誤導致素材跑掉、投放設定異常）就直接開始花錢。

**這台 Mac 沒有 Windows 版用的 `check_new_ad_launch.py` / `notify_ad_squad.py`**（那是 `C:\AI小摸` 底下的腳本，2026-09-11 從 Google Drive 的 `~/.claude` 快照備份救回 SKILL.md 本身重建，但腳本原始碼沒備份到，只能用 Meta Ads MCP 手動比照判斷邏輯做，見 [[feedback_meta_ad_copy_windows_tool_unavailable_mac]] 同樣的環境差異）。推播改用 `000_Agent/skills/meta-ad-copy/scripts/send_chat_notification.py`（Cards v2 卡片格式）。

**兩種觸發路徑，行為一致**：
1. `meta-ad-copy` execute 成功後**自動**接這個檢查（見該 skill 第 6 步之後），檢查通過就直接啟動，不用等 Molly 確認
2. Molly 直接叫這個 skill 做手動巡檢（例如檢查不是透過 meta-ad-copy 建立的廣告），一樣是檢查通過就直接啟動，不用另外確認

這是 2026-08-28 Molly 明確要的行為：她原本以為 skill 應該自動接在上稿後檢查，不想每次都要再喊一次「開」；巡檢結果（含 warnings）還是要回報給她，方便她隨機抽查，但**不等她回覆才動作**。

跟 `meta-creative-check`（看昨日 vs 7日/14日均值 CTR、頻次等成效數據的巡檢）也不一樣：那個 skill 判斷的是「已經跑一段時間、有成效數據的廣告該不該汰換」，這個 skill 判斷的是「剛建好、還沒開始跑、完全沒有成效數據的廣告能不能開」。**新廣告沒有歷史投放數據，不適用 meta-creative-check 那套 CTR/頻次比較邏輯**，這裡的檢查完全是結構面、設定面的，不涉及成效判斷。

---

## 執行步驟

### 1. 取得要檢查的 ad_id 清單

**從 `meta-ad-copy` 自動觸發時**：直接用那次 execute 剛回報成功的 ad_id 清單，不用問。

**Molly 手動叫這個 skill 時**：她通常會直接給 ad_id，或說「檢查剛剛那批」——這種情況一樣沿用這次對話裡最近一次 execute 回報的 ad_id 清單。完全沒有上下文時，才問她要檢查哪個帳號／哪幾支廣告，不要自己亂猜掃全帳號的 PAUSED 廣告（範圍不明確，容易查到不相干的舊草稿）。

### 2. 逐支查詢，整理成三類結果

對每個 ad_id 用 Meta Ads MCP 查（`ads_get_ad_entities`，`client_conversation_id` 全程沿用同一組）：

1. **ad 本身**：`level="ad"`, `object_ids=[ad_id]`, `fields=["id","name","effective_status","adset_id","campaign_id"]`
2. **所在 adset**：`level="adset"`, `object_ids=[adset_id]`, `fields=["id","name","effective_status","daily_budget","lifetime_budget"]`
3. **所在 campaign**：`level="campaign"`, `object_ids=[campaign_id]`, `fields=["id","name","effective_status"]`

判斷規則：

- **issues（硬性問題，任何一條中就 `pass: false`）**：
  - ad 的 `effective_status` 是 `DISAPPROVED`
- **warnings（需要判斷的疑點，不影響 pass，但要列出來）**：
  - adset 或 campaign 的 `effective_status` 不是 `ACTIVE`（例如 `PAUSED`/`CAMPAIGN_PAUSED`/`ADSET_PAUSED`）——代表就算這支 ad 改 ACTIVE，實際也不會真的開始投放，要提醒 Molly
  - adset 查不到 `daily_budget` 也查不到 `lifetime_budget`（可能是 campaign 層級預算、或設定異常，不確定就列出來讓她判斷）
  - 如果這批是剛用 `meta-ad-copy` 建的、而且這次對話上下文知道它其實是走「重用既有 creative_id」的 App 開發模式 fallback（見 [[project_meta_cpas_app_dev_mode_blocker]]）而非原本判斷的 post id/複製方式，要在這裡點出來，但還是要列出來讓 Molly 知道，不要自己吃掉判斷替她決定

整理成表格給 Molly 看，一定要分三類：

| 狀態 | 廣告名稱 | ad_id | 問題/疑點 |
|---|---|---|---|
| ✅ 通過 | ... | ... | — |
| ⚠️ 有疑點但可能沒問題 | ... | ... | warnings 內容 |
| 🚨 不能開 | ... | ... | issues 內容（DISAPPROVED 等） |

### 3. 🚨 有問題的廣告

列出問題原文，不啟動、不排程，等 Molly 決定要修哪裡（例如去 Meta 後台看被拒原因、或回頭調整素材）。

推播卡片到「廣告軍團-Meta 上傳素材」空間讓她能隨時看到（不用等她回到對話視窗），用純文字卡片（`{"text": "..."}` 這種只有一個欄位的元素，見 `send_chat_notification.py` 說明）：

```
🚨 <N> 支廣告卡關，維持 PAUSED：<ad_id 與問題摘要>
```

### 4. ✅ 通過（或 Molly 看過 warnings 後同意）的廣告——依是否為活動廣告分流

**a. 不是活動廣告（名稱沒有 `[~MM/DD]` 字首）**：

**不用問 Molly，直接開**：用 `ads_activate_entity`（`entity_type="ad"`）把每支改 ACTIVE。

啟動後推播結果（純文字卡片）：
```
✅ 巡檢通過，已啟動 <N> 支廣告（ACTIVE）：<ad_id 清單>。warnings：<有的話列出，沒有寫「無」>
```

**b. 是活動廣告（名稱帶 `[~MM/DD]` 字首）——先確認有沒有重複排程**：

這批廣告如果是剛用 `meta-ad-copy` 建立的，那個 skill 的第 6 步在 execute 成功當下就已經自動排好走期起訖 routine 了（不需要通過任何檢查，到時間就會直接開）。**先用 `list_scheduled_tasks` 查有沒有 taskId 帶這個 ad_id 或同一批走期關鍵字的 routine**：

- **已經有對應 routine** → 不用重新排，回報「這批走期 routine 已存在（taskId: ...），本次檢查通過，維持原排程，不需要額外動作」
- **沒有對應 routine**（例如這支廣告不是透過 meta-ad-copy 建的、或那次沒排成功）→ 沿用 **meta-ad-copy skill 第 6 步**的完整規則（解析起訖日期、建開啟/暫停兩個 routine），直接參考 `~/.claude/skills/meta-ad-copy/SKILL.md` 第 6 步，不要自己重新設計一套

排好後跟 Molly 說明起訖時間，這個排程動作沿用 Molly 已經同意過的既定方向（見 [[feedback_activity_creative_cloud_routine]]），不需要再另外確認才建立。

不管是「已存在」還是「新排」，都推播一次結果（純文字卡片）：
```
✅ 巡檢通過，<N> 支活動廣告走期 routine 已就緒（<沿用既有/新排>）：起 <起始日> 開、迄 <到期日隔天> 關。warnings：<有的話列出，沒有寫「無」>
```

### 5. 回報結果

逐支列出最終狀態：已開啟／已排走期 routine／等 Molly 決定。不要漏講 warnings 裡她還沒表態的疑點。

---

## 注意事項

- 這個 skill 不判斷「素材好不好、會不會虧」，那是 `meta-creative-check`（跑一段時間後）和 `meta-budget-pilot`（預算調整）的範圍
- **這個 skill 的啟動動作是 [[feedback_meta_ads_write_safety]]「寫入類一律先確認」的明確例外**（2026-08-28 Molly 確認）：只要判斷 `pass: true`，不管是直接改 ACTIVE 還是排走期起訖 routine，都不用再等她回覆才動作；`issues` 有內容的才維持 PAUSED 不動。這個例外只適用於「這支 skill 判斷通過」的情況，不能類推到其他跟廣告寫入相關但沒經過這個檢查的操作
- 不要對同一支廣告重複建立走期起訖 routine——重複執行 PAUSED/ACTIVE 本身無害，但 taskId 混亂之後不好管理，第 4-b 步一定要先查再排
- 檢查目前只覆蓋審核狀態、adset/campaign 狀態與預算欄位；不做受眾規模、素材畫質、文案語意這類需要人工判斷的檢查，這些還是要 Molly 自己看過
