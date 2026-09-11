---
name: feedback-creative-report-template
description: 帶圖片的 Meta 素材成效 HTML 報告製作模板與流程 — TSA/毛孩時代等帳戶素材分析可重複套用
metadata: 
  node_type: memory
  type: feedback
  originSessionId: eb41322c-25af-4a66-b91f-2571b66b60eb
  modified: 2026-08-06T01:57:59.980Z
---

用戶在 2026-07-23 針對毛孩官網潔牙粉素材分析，明確要求記住這套「整理素材」的模板和邏輯，供之後同類報告重複使用。

**資料抓取流程：**
1. 用 `ads_get_ad_entities`（level=ad）以產品關鍵字 filter ad.name（CONTAIN），配合 [[project_ads_optimization_training]] 和 [[feedback_atl_btl_testing_logic]] 的 TSA 帳戶規則，拆出 ATL / BTL 兩組數據。
2. 對想放圖的素材，用同一批 ad.id 查 `creative_id`，再用 `ads_get_creatives`（creative_ids + fields: name/body/title/image_url/thumbnail_url/call_to_action_type）拿到圖片網址與文案。
3. 圖片網址是 Facebook CDN 簽名連結，Artifact 的 CSP 會擋外部圖片請求，**必須下載後轉 base64 內嵌**，不能直接 hotlink。

**圖片處理（避免檔案爆量）：**
- 用 `curl -sL -A "Mozilla/5.0"` 下載原圖到 scratchpad。
- 用 Python PIL 縮小到長邊 640px、JPEG quality 72、`optimize=True` 再壓縮，可把單張 400-500KB 壓到 60-70KB，11 張圖整體 base64 才不會把 HTML 撐到誇張大小。
- **絕對不要把巨大的 base64 字串讀進對話 context**：寫一個 Python script 直接讀 data URI 檔案、用字串替換塞進 HTML 模板裡的 `{{IMG_xx}}` placeholder，全程用 Bash 跑完，不用 Read 工具打開。

**報告排版邏輯：**
- 先寫好帶 placeholder 的 HTML 模板（Write 工具），再用 Bash/Python 做字串替換產出最終檔案，不要一次把整包 base64 塞進 Write 的 content 參數。
- 排序依 [[feedback_atl_btl_testing_logic]] 的兩階段漏斗：主排序看 ATL 互動 CTR（篩選指標），每張卡片再附上該素材是否已升級 BTL、以及 BTL 實際 ROAS/購買數 作為第二層驗證訊號。
- 卡片用語意色（good/warn/bad/neutral）標示 BTL 驗證狀態，語意色需與品牌強調色分開，不能共用。
- 這類報告屬於「內部分析報告」定位，走乾淨的 dashboard/report 排版（清楚的 stat tile + 排名卡片列表），不用做成行銷落地頁那種大 hero 編輯排版。

**交付格式：**
- 若用戶要上傳內部伺服器或分享給同事，圖片內嵌的單一自足 HTML 檔案本身就不需要額外資料夾。
- 這台機器的 git-bash 沒有 `zip` 指令，打包用 PowerShell 的 `Compress-Archive`（先檢查目標 zip 是否存在，存在就先刪除再壓，避免 Compress-Archive 因檔案已存在而報錯）。

**⚠️ 打包 HTML 給用戶下載/上傳前必檢查的雷（2026-07-23 踩過一次）：**
- Artifact 工具吃的 HTML 片段（只有 `<title>`+`<style>`+內容，沒有 `<!DOCTYPE html>`/`<head>`/`<meta charset>`）是給 Artifact 自動包一層骨架用的，**不能直接拿去 zip 給用戶當獨立檔案**。用戶直接雙擊在瀏覽器開啟時，沒有 `<meta charset="UTF-8">` 瀏覽器會猜錯編碼，中文全部變亂碼。
- 只要是「打包成 zip / 給用戶下載 / 上傳到其他地方」這種會脫離 Artifact 渲染環境、由用戶自己直接開啟的 HTML 交付，**產出前一定要確認檔案本身是完整獨立文件**：`<!DOCTYPE html>` + `<html lang="zh-Hant">` + `<head><meta charset="UTF-8">...<title>...</title><style>...</style></head>` + `<body>...</body></html>`，不能只有 Artifact 用的片段。
- 用戶明確要求「以後打包 html 要特別注意」——之後任何要離開 Artifact 環境交付的 HTML（zip、寄信附件、上傳伺服器等），出手前先自查有沒有 doctype/head/charset，不用等用戶回報亂碼才發現。

**⚠️ 補充：本機開沒事，但上傳到第三方網頁託管服務（例如用戶用的 page.7889.tw 上傳工具）還是亂碼**
- 原因：有些第三方上傳/託管服務會用自己的 Content-Type header 或內部解析邏輯決定編碼，不一定會乖乖讀 HTML 裡的 `<meta charset="UTF-8">`。瀏覽器判斷編碼優先順序是 BOM > HTTP header charset > meta charset，所以 header 蓋過 meta 時就會亂碼，即使本機直接開檔案完全正常。
- 解法：**存檔時用 UTF-8 with BOM**（Python: `open(path, 'w', encoding='utf-8-sig')`），在檔案最前面加上 `EF BB BF` 三個 byte 的 BOM 標記，讓沒有明確宣告 charset 的第三方服務也能正確偵測出 UTF-8。這對本機瀏覽器開啟無副作用。
- **之後只要 HTML 是要上傳到外部平台/CMS/簡報工具的，預設就用 utf-8-sig 存檔**，不用等用戶回報「本地端沒事，上傳就壞」才補。
- **已驗證有效**：用戶把加了 BOM 的版本重新上傳到 page.7889.tw 後回報「成功了」，中文正常顯示，這個解法可信賴，之後遇到同類「本機沒事、上傳外部平台就亂碼」直接套用。

**⚠️ 聚合單位一定是「素材」，不是 ad（2026-08-06 被糾正）**
- 同一支素材常同時掛在多個 ad set / campaign 底下（例：御熹堂「血管不是塞滿才危險」同時跑 ATL(PV)、ATL(Interact)、BTL ASC，是 3 個不同的 ad id + 3 個不同 creative_id）。
- **不可以把每個 ad 各列一行當成不同素材**。正確做法：以素材名稱（ad.name）分組，把花費／曝光／點擊／連結點擊／購買／收益先加總，再用總數相除算 CTR／CPC／CPA／ROAS。
- 但 ATL 與 BTL 仍要分開呈現（不同投放目的用不同指標，見 [[feedback_atl_btl_testing_logic]]），所以一張素材卡＝上半 ATL 區塊＋下半 BTL 區塊，底下再附「投放明細」小字列出各 ad set 數字保留追溯性。
- 觸及／頻次跨組會重複計算，聚合後不要呈現，或明講未聚合。
- 對照組的「素材數」也要用唯一素材名稱去算，不要報 ad 數（會灌水）。

**⚠️ 拿素材縮圖：boosted post（object_type=STATUS）走廣告預覽，不要用 thumbnail_url**
- `ads_get_creatives` 的 `thumbnail_url` 對 STATUS 類型回傳的是**粉專頭像 64x64**，不同素材還會拿到同一張圖，完全不能用。`image_url` / `image_hash` 也是空的。
- Graph API 直接打 `/{object_story_id}?fields=attachments` 會被擋：`(#10) requires pages_read_engagement`，META_ACCESS_TOKEN 沒有這個權限。
- **可行解法**：`GET /{ad_id}/previews?ad_format=MOBILE_FEED_STANDARD` → 取回的 iframe `src` 是簽名 URL，**不需登入就能直接 fetch**（約 730KB HTML）→ 把 HTML 的 `\/` 還原成 `/` 後，用 regex 抓 `https://scontent...` 且路徑含 `/t39.30808-6/` 的網址（`/t39.30808-1/` 是粉專頭像要排除）→ 即為真實貼文圖，預設 320x320，多圖貼文可抓到前 4 張。
- 抓到後照原本流程 PIL 壓縮（thumbnail 340、JPEG q74、optimize）轉 base64；4 支素材×4 張約 550KB base64，Artifact 吃得下。

**How to apply:** 之後只要是「毛孩時代 / TSA 集團素材成效＋要看圖」的請求，直接套用這套抓資料→**依素材名稱聚合**→縮圖壓縮→模板替換→打包的完整流程，不用每次重新設計。
