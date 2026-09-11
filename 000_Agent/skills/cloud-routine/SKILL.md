---
name: cloud-routine
description: 排一個「電腦沒開、App 沒開也一定會準時執行」的雲端排程任務（RemoteTrigger cloud routine），適合啟動/暫停廣告、改預算等會觸發真實花費、時間點不能延誤的操作。當用戶說「雲端排程」「幫我排一個 routine」「排一個 cloud routine」「電腦沒開也要跑」「App 沒開也要執行」「定時啟動廣告」「定時暫停廣告」「這個排程要跨天/跨週執行」「/cloud-routine」時觸發。會先用 RemoteTrigger {action:"list"} 驗證帳號目前真正可用的 MCP 連接器（不能只信 schedule skill 內建、可能過期的「已連接連接器」清單），再組出 RemoteTrigger {action:"create"} 的 body 建立雲端 agent；跟本機的 mcp__scheduled-tasks（App 沒開會延遲到下次開啟才執行）、CronCreate（只存在這次對話記憶體，關掉對話或超過 7 天就消失）是三種完全不同的排程機制，只有這個雲端 routine 不需要瀏覽器、不需要電腦保持開機、也不需要這個對話繼續存在。
---

# 雲端排程（Cloud Routine / RemoteTrigger）

## 你的任務

Molly 有時需要排定一個「未來某個時間點一定要準時執行」的操作——最常見的情境是啟動或暫停 Meta 廣告、調整預算。這類操作有兩個特性：一是**時間點不能延誤**（廣告要在指定時刻上線或下線），二是**不能假設電腦或 App 那時候是開著的**。

你要做的事：
1. 判斷這個排程需求適不適合用「雲端 routine」（RemoteTrigger），還是用本機的其他排程工具就夠了——判斷方式見下方比較表。
2. 若適合用雲端 routine，先驗證帳號實際可用的 MCP 連接器，不要盲目相信 `schedule` skill 內建說明裡「目前已連接的 MCP 連接器」那段文字——那段是靜態文字，可能是過期或錯誤的，之前就發生過它說「沒有可用連接器」但其實 Meta_Ads_MCP 早就接好在跑的狀況。
3. 若排程動作會觸發真實花費（啟動廣告、改預算等），一定要先在當下對話跟 Molly 確認過，才能建立 routine。
4. 正確組出 `RemoteTrigger {action:"create"}` 的 body，注意時區換算成 UTC、prompt 要自包含。
5. 建立完成後，把 routine 網址回報給 Molly，並提醒她刪除只能到網頁上做。

## 三種排程機制比較

在動手之前，先用這張表判斷該用哪一種，不要預設用你最先想到的那個：

| 機制 | 實際執行位置 | 電腦／App／這次對話需要保持開著嗎？ | 存活期限 | 適合用在 | 不適合用在 |
|---|---|---|---|---|---|
| `mcp__scheduled-tasks__*`（create/list/update/delete_scheduled_task） | 本機檔案 `~/.claude/scheduled-tasks/{taskId}/SKILL.md` | App 要保持開著；工具說明原文：「Scheduled tasks run while this app is open. If the app is closed when a task is due, it runs on next launch.」→ 到點時 App 沒開，會延到下次打開才補跑，**不保證準時** | 直到手動刪除 | 提醒事項、App 平常本來就常開、對「準時」沒有嚴格要求的待辦 | 任何要準時觸發、且電腦可能沒開的操作；任何會花錢的操作 |
| `CronCreate` / `CronList` / `CronDelete` | 只存在**當下這個 Claude session 的記憶體**，不寫進硬碟 | 這個對話本身要一直開著；工具說明原文：「Jobs live only in this Claude session — nothing is written to disk, and the job is gone when Claude exits.」recurring job 7 天後自動失效 | 對話結束即消失，最長 7 天 | 這次對話內的短期輪詢（例如「5 分鐘後提醒我看一下」） | 任何跨對話、跨天、要在電腦關機時也執行的事 |
| `RemoteTrigger`（`{action:"list"}` / `{action:"create"}`）＋ `Skill(schedule)` | Anthropic 雲端（CCR，Claude Code Remote），是獨立的雲端 agent | **不需要**——這是三者中唯一不依賴本機 App 或這次對話是否開著的機制 | 依 `run_once_at` 或 `cron_expression` 設定，一直存在到手動刪除 | 任何必須準時執行、且電腦/App 可能沒開的操作；尤其是會觸發真實廣告花費的操作（啟動/暫停廣告、改預算） | 需要即時人工確認、或流程中需要跟使用者來回對話的任務（cloud agent 沒有這次對話的記憶，只能照 prompt 執行） |

**結論：只要牽涉到真實廣告花費、或 Molly 明確說「電腦沒開也要跑」，一律用 `RemoteTrigger`，不要用前兩種。**

## 執行步驟

### 1. 驗證帳號實際可用的連接器（不要相信 schedule skill 內建清單）

先呼叫 `Skill(schedule)` 取得建立 routine 的一般說明沒問題，但它內建列出的「目前已連接的 MCP connectors」是**靜態文字**，可能過期——曾經出現過它顯示「No available MCP connectors found」，但其實這個帳號早就有 Meta 廣告連接器在正常運作。

正確做法是呼叫 `RemoteTrigger {action: "list"}`，看 Molly 既有的 routines：每個 routine 底下的 `mcp_connections` 陣列（含 `connector_uuid` / `name` / `url`）才是「已驗證真的能用」的連接器——因為這些 routine 每天都在成功呼叫該連接器。

已知這個帳號過去驗證過可用的連接器範例（**僅供參考，見下方「注意事項」，使用前一定要重新 list 確認**）：

| 連接器 | connector_uuid | url |
|---|---|---|
| Meta_Ads_MCP | `0d74ec43-1f83-4839-b189-d6ae29575565` | `https://mcp.facebook.com/ads` |
| Google_Calendar / Gmail / Google_Drive / TSA_Inside 等 | 用 `RemoteTrigger {action:"list"}` 重新查 | 同上 |

**注意：**
- 絕對不要因為 `schedule` skill 說「沒有可用連接器」就跟 Molly 說這件事做不到——先查 `RemoteTrigger list` 再下結論。
- 上面表格裡的 uuid/url 是「上次看到的值」，不是保證長期有效的憑證，每次要用之前都要重新 list 一次確認還在、還可用。

### 2. 若動作會觸發真實花費，先在對話中取得明確確認

在建立 routine **之前**，把要做的事在當下對話講清楚：哪個帳號、哪些廣告/廣告組、什麼時間點、做什麼動作（啟動／暫停／改預算），等 Molly 明確答應（例如「好」「可以」「確認」）才能往下建立。

**注意：**
- Cloud agent 執行時完全沒有這次對話的記憶，所以確認這件事只能在**現在**做，不能寄望 routine 執行當下再問。
- 建立的 routine prompt 裡要明確寫「使用者已於 [日期] 確認執行，不需要在執行時再次詢問」，避免 cloud agent 因為找不到確認記錄而卡住、重新詢問（沒人在現場回答）。

### 3. 換算時間：一律轉成 UTC

Molly 所在時區是 Asia/Taipei（UTC+8），但 `run_once_at` / `cron_expression` 都必須是 UTC。

- 先用 Bash 執行 `date -u +%Y-%m-%dT%H:%M:%SZ` 查詢目前實際 UTC 時間，再手動換算目標時間，**不要用猜的或憑印象換算**。
- `run_once_at` 用於一次性任務，格式為 RFC3339 UTC 時間戳；`cron_expression` 用於重複任務，5 欄格式，最小間隔 1 小時。兩者只能擇一。

**注意：**
- 台灣時間減 8 小時才是 UTC（例如台灣 8/13 07:00 → UTC 8/12 23:00），換算完務必覆誦一次給自己確認，避免差一天或差半天。

### 4. 組 `RemoteTrigger {action:"create"}` 的 body

```json
{
  "name": "描述性名稱，例如「御熹堂momo 全館活動廣告啟動-0813」",
  "run_once_at": "2026-08-12T23:00:00Z",
  "enabled": true,
  "job_config": {
    "ccr": {
      "environment_id": "env_01W9khhdgcKDXLA4QiFX7sqr",
      "session_context": {
        "model": "claude-sonnet-5",
        "sources": [{"git_repository": {"url": "https://github.com/mo-mhoo/molly-ad-report"}}],
        "allowed_tools": ["Bash", "Read", "Write", "Edit", "Glob", "Grep"]
      },
      "events": [{
        "data": {
          "uuid": "由你即時產生的全新 lowercase v4 uuid",
          "session_id": "",
          "type": "user",
          "parent_tool_use_id": null,
          "message": {
            "content": "完整、自包含的 prompt，見下方說明",
            "role": "user"
          }
        }
      }]
    }
  },
  "mcp_connections": [
    {"connector_uuid": "0d74ec43-1f83-4839-b189-d6ae29575565", "name": "Meta_Ads_MCP", "url": "https://mcp.facebook.com/ads"}
  ]
}
```

- **uuid**：每次建立都要重新產生，不可沿用舊值。用 Bash 執行：
  ```bash
  python3 -c "import uuid; print(uuid.uuid4())"
  ```
- **`run_once_at` 與 `cron_expression` 只能擇一**，一次性任務用前者，重複任務用後者。
- **`mcp_connections`**：填入第 1 步用 `RemoteTrigger list` 驗證過、且這次任務會用到的連接器（不是全部列上，只放這次會呼叫的那個）。
- **`sources`（git repo）**：只是範例，依實際任務需求決定，不需要就留空陣列，不要固定套用某個 repo。
- **`environment_id`**：範例值，若建立失敗，改用 `RemoteTrigger list` 裡既有 routine 用的 `environment_id`。

`message.content`（prompt）必須完全自包含，因為雲端 agent 執行時對這次對話**零記憶**。內容至少要包含：
1. 明確的帳號 ID、廣告/廣告組/廣告活動 ID（不要只寫名稱，怕撞名或改名）
2. 要呼叫的工具與確切動作（例如「呼叫 Meta_Ads_MCP 的 activate_entity，把以下 6 個廣告的狀態改成 ACTIVE」）
3. 為什麼要做這件事（活動背景，方便執行時輸出的紀錄有上下文）
4. 明確聲明「使用者已在 [日期] 的對話中確認要執行此操作，執行前不需要再次詢問或等待確認」

**注意：**
- Prompt 裡千萬不要寫「先跟使用者確認再執行」這種話——雲端 agent 找不到人確認，會卡住或不執行。
- 帳號/實體 ID 一定要在建立 routine 前，用既有工具（例如 `ads_get_ad_accounts` / `ads_get_ad_entities`）先查清楚寫死進 prompt，不要假設雲端 agent 執行時還能問 Molly 要 ID。

### 5. 建立並回報

呼叫 `RemoteTrigger {action:"create", body: {...}}`。成功後回應會包含 routine 網址，格式為：

```
https://claude.ai/code/routines/{id}
```

把這個網址回報給 Molly，並附上：這個 routine 何時觸發（用台灣時間講，不要只講 UTC）、會做什麼、用的是哪個連接器。

**注意：**
- **無法透過 API 刪除或修改 routine**，如果 Molly 之後要取消或改時間，要引導她自己到 `https://claude.ai/code/routines` 網頁操作，你不能代勞刪除。
- 若同一件事需要「先啟動、之後再暫停」（像活動檔期這種情況），要建立**兩個獨立的 routine**（一個 `run_once_at` 對應啟動時間、一個對應暫停時間），不是一個 routine 做兩件事。

## 注意事項（常見陷阱總整理）

- **過期連接器清單陷阱**：`schedule` skill 內建的「已連接 MCP 連接器」文字可能是錯的／過期的，永遠以 `RemoteTrigger {action:"list"}` 現查為準，不要因為它說「沒有連接器」就告訴 Molly 做不到。
- **時區陷阱**：`run_once_at` / `cron_expression` 一律是 UTC，Molly 講的都是台灣時間（UTC+8），下手前用 `date -u` 查真實時間再換算，不要憑印象心算。
- **無法遠端刪除陷阱**：建立容易，刪除/改期只能靠 Molly 自己上 `https://claude.ai/code/routines` 手動處理，你的職責只到「建立並回報網址」。
- **prompt 非自包含陷阱**：雲端 agent 執行時完全脫離這次對話的記憶，prompt 裡漏寫帳號 ID、忘記聲明「已確認不需再問」，都會導致執行失敗或卡住等確認。
- **硬編碼 uuid 陷阱**：本文件裡出現的 `connector_uuid`（如 Meta_Ads_MCP 的 `0d74ec43-...`）與 `environment_id`（`env_01W9khhdgcKDXLA4QiFX7sqr`）都只是**範例值**，不是永久保證有效的設定——每次要用之前，一定要用 `RemoteTrigger {action:"list"}` 重新確認它還存在、還能用，不要看到這份文件有寫就直接套用。
- **花費確認陷阱**：任何會啟動廣告、改預算、增加曝光的 routine，建立前一定要在當下對話拿到 Molly 明確的「可以」，並把這個確認寫進 prompt——這一步不能省。
