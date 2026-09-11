---
name: meta-ad-video-img-upload
description: 把「還沒進到 Meta 裡」的全新素材（影片、動圖、單圖，來源是 Google Drive 連結，不是已發布的貼文或既有廣告）從進稿追蹤表一路做成 Meta 廣告——上傳素材、視情況建立精選集（Canvas），交給 meta-ad-copy 既有的巡檢/啟動流程收尾。當使用者說「上傳這支影片/這張圖」「這支素材幫我上稿」「這支做精選集」「這支素材連結是資料夾/Drive連結，幫我建廣告」「進稿表這一列還沒上稿，處理一下」，或貼出試算表某一列內容且格式欄是「影片」「動圖」「單圖」「多圖」「輪播」、已上稿=FALSE 時觸發。跟 meta-ad-copy 是互補關係：meta-ad-copy 處理「素材已經在 Meta 裡（貼文/既有廣告）」的複製上稿，這個 skill 專門處理「素材完全沒進過 Meta、只有 Drive 連結」的從零上傳，兩者判斷帳號/adset/巡檢啟動的邏輯共用，不要用 meta-ad-copy 硬套原始檔案上傳，也不要在這裡重新設計巡檢/啟動邏輯。
---

# Meta 素材上傳（Drive → Meta 廣告）

## 你的身份與分工

你是 Molly 的廣告上稿助理，專門處理進稿追蹤表裡「格式＝影片／動圖／單圖／多圖／輪播、已上稿＝FALSE」但「可上稿＝TRUE」的列——這些素材檔案本身還在 Google Drive，Meta 帳號裡完全沒有對應的素材/廣告，跟 [[meta-ad-copy]] 處理的「素材已經存在 Meta（貼文/既有廣告）」是完全不同的起點。

這個 skill 只負責**前半段**：讀表→判斷帳號→抓 Drive 檔案→上傳素材→（視情況）建精選集 Canvas→拿到 `creative_id`。拿到 `creative_id` 之後，**直接沿用 meta-ad-copy 的第5步以後流程**（建 PAUSED 廣告→`check_new_ad_launch.py` 巡檢→依結果啟動或排走期）——這段邏輯只維護一份，不要在這裡重新設計，執行方式跟 meta-ad-copy 完全一樣：

```bash
cd "C:\AI小摸" && python check_new_ad_launch.py <ad_id>
```

這是寫入類操作，**建立廣告前一定要先把計畫給 Molly 看過、等她明確說要執行才動手**（跟 [[feedback_meta_ads_write_safety]] 一致）。

**這個 skill 原本只做影片（2026-08-31 建立），單圖上稿驗證成功後 Molly 明確要求擴大範圍涵蓋單圖**——常態、量大的單圖上稿如果她想繼續用既有的 Apps Script 流程（例如御熹堂/毛孩時代那套「素材連結回填工具」），那是她的選擇，不用主動搶著做；但只要她開口要這個 skill 處理單圖，就直接照下面的流程做，不用反問「這不是該用 Apps Script 嗎」。

---

## 完整案例走一遍（御熹堂_CPAS_蝦皮，影片＋精選集，真實跑過）

下面每一步括號裡標了對應的詳細規則在第幾步，只是先串成一條故事線，讓人先抓到全貌，細節還是要看對應那一步：

1. 佇列表裡看到一列：蝦皮_素材進稿、格式=影片、素材連結是 Drive 單一檔案連結、目標網址是 `tsa1.cc` 短連結（第1步）
2. 目標網址／原始網址帶 `shopee`，判斷帳號 = 御熹堂_CPAS_蝦皮 `1479378256009304`（第2步，速查表已有）
3. 素材連結是單一檔案（不是資料夾），用 Drive 連接器 `download_file_content` 下載成本地檔案——**不用 `gdown`**，這份表通常是公司內部限定分享，匿名下載會失敗（第3步）
4. 用 `facebook-business` SDK 的 `AdAccount.create_ad_video(params={'source': 本地路徑})` 上傳，拿到 `video_id`——**不是用 `file_url`**，Drive 連結 Meta 抓不到（第4步 Video）
5. 輪詢 `AdVideo.api_get(fields=['status'])` 直到 `video_status == 'ready'`，再抓縮圖 `get_thumbnails()`（第4步 Video）
6. 帳號是 CPAS 通路 → 預設走精選集，不是一般 video creative（第4步「兩條路共用的判斷」）
7. 這一列「帶品」欄是空的、目標網址導的是全站活動頁（不是單一商品）→ 精選集商品格用**動態選擇**，不手動綁 `retailer_item_ids`（第4步「精選集商品綁定判斷」）
8. 查目標草稿 adset（`120236798368350016`）的 `promoted_object.product_set_id`，拿到 `1632828977697061`（第4步 Video 步驟1）
9. 換一組**粉專層級的 Page Access Token**（`.env` 的 `META_PAGE_ACCESS_TOKEN`，跟廣告帳號用的 `META_ACCESS_TOKEN` 是不同組）——因為接下來的 Canvas 端點只認粉專權限的 token，用廣告帳號 token 會被拒（前置需求）
10. 用這組 page token 直接呼叫 Graph API（**不是** Meta Ads MCP 的 `ads_create_creative`，那個工具不支援這個格式），依序建 `canvas_video`（影片封面）、`canvas_button`（CTA）、`canvas_footer`（掛按鈕）、`canvas_product_set`（掛步驟8查到的 product_set_id）四個元素（第4步 Video 步驟2）
11. 把前3個元素（影片＋商品格＋footer）組成一個 Canvas 文件：`POST /{page_id}/canvases`（第4步 Video 步驟3）
12. 建 Ad Creative：`object_story_spec.link_data.link` 指向 `https://fb.com/canvas_doc/{canvas_id}`（不是一般的 `video_data`），`link_data.picture` 放縮圖網址（第4步 Video 步驟4）
13. 建廣告，放進草稿 adset（`120236798368350016`），狀態 PAUSED（第6步情境B，因為當時 Molly 說先放草稿）
14. 回報帳號＋廣告名稱給 Molly（第6步情境B）

---

## 前置需求（`.env`）

| 變數 | 用途 |
|---|---|
| `META_ACCESS_TOKEN` | 廣告帳號層級操作（上傳素材、建一般 creative、建廣告）都用這組，既有變數，不用新增 |
| `META_PAGE_ACCESS_TOKEN` | **只有建精選集(Canvas)才需要**——`canvas_elements`／`canvases` 這兩個端點硬性要求粉專層級的 Page Access Token，`META_ACCESS_TOKEN` 沒有粉專權限會直接被拒（`(#210) A page access token is required`）。取得方式：任何一組已在企業管理平台（Business Manager）被授權粉專權限的 System User token 都可以；如果 `.env` 沒有這個變數、而這一列要建精選集，先請 Molly 提供並存進 `.env`，不要自己生成或猜測。 |

如果只是建一般廣告（非精選集），完全不需要 `META_PAGE_ACCESS_TOKEN`。

---

## 已知資源速查表（2026-08-31 實測，換 session 直接用，不用重查）

**御熹堂**（頁面 page_id `110903708668924`，目錄 catalog_id `780424856974162`）：

| 通路 | account_id | 草稿/素材庫 adset（PAUSED，建廣告先放這裡） |
|---|---|---|
| CPAS_蝦皮 | `1479378256009304` | `120236798368350016`（Ad素材庫｜活動｜混和商品） |
| 官網 | `800743741924152` | `120236267569670735`（Ad素材庫｜活動1，**已滿150支**，滿了就用 `120253537789950735`／Ad素材庫｜活動2，是複製前者出來的） |
| CPAS_momo | `7609150239116011` | `120245270544160507`（Ad素材庫｜活動2，2026-09-01首次驗證，Molly指定「都先建立在」這裡，不分品項），product_set_id=`405238042213877`（"All Products" 動態全商品集） |

CPAS 精選集常用的 product_set_id：`1632828977697061`（「主打活動品(排除贈品/加購)」，蝦皮草稿 adset `120236798368350016` 綁的就是這個）——**只適用於這個 adset**，換別的 adset 一定要重查 `promoted_object.product_set_id`，不要照抄這個值。

**御熹堂_CPAS_momo 商品目錄陷阱（2026-09-01 實測）**：這個廣告帳號背後有兩份名稱相似的momo目錄，SKU內容重複但屬於不同catalog_id：
- `1811609289712017`（【momoCPAS】TSA 000_御熹堂_202507，新版）
- `730633932514830`（【momoCPAS】騰勢股份有限公司2023_御熹堂_202403，舊版）——**這份才是「Ad素材庫｜活動2」adset 的 `promoted_object.product_set_id`(`405238042213877`) 實際掛的目錄**，查retailer_item_ids要用這份，不是新版那份（雖然兩份目錄的retailer_id剛好完全重複，用哪份查都查得到，但概念上要記得對齊的是promoted_object指到的那份，換帳號/換adset時不能預設兩份一定同步）。

御熹堂 momo「入組」商品 retailer_id 對照表（用 `ads_catalog_list_products` 對 `1811609289712017` 或 `730633932514830` 皆可查到，momo品號=momoshop網址的i_code，跟retailer_id不是同一組數字）：

| 品項 | 1入 | 3入 | 5入 | 6入 | 9入 | 12入 |
|---|---|---|---|---|---|---|
| 益固醇 紅麴納豆Q10 | 13271998 | 13271999 | — | 13272000 | 14208793 | — |
| 舒密妃 蔓越莓益生菌 | 11075892 | 11080207 | 11080208 | — | 11080209 | — |
| 黃金頂級90%魚油 | 11086797 | 11086788 | 11086789 | — | 11086790 | 12300435 |
| 益健步 UC-II+葡萄糖胺 | 13617370 | 13617368 | — | 13617369 | — | — |

**御熹堂粉專的 `META_PAGE_ACCESS_TOKEN` 其實是 System User token，不是真正的 Page Access Token**——直接拿去打 `/{page_id}/photos`、`/canvas_elements` 會報 `(#200) Unpublished posts must be posted to a page as the page itself`。每次用之前要先換一次真正的 page token：
```
GET /{page_id}?fields=access_token&access_token={.env的META_PAGE_ACCESS_TOKEN}
```
換出來的 `access_token` 才是能用在 canvas 相關端點的 page token（不用存回 `.env`，這組是短效交換結果，隨用隨換即可）。

**御熹堂_CPAS_momo 建 creative 一樣會撞到 App 開發模式 subcode 1885183**（原本以為只有毛孩時代才會，2026-09-01 這次御熹堂帳號也踩到）——`/adcreatives` 這一步要改用上面換出來的 page token 呼叫，不能用 `META_ACCESS_TOKEN`；`/ads` 建廣告那一步用一般的 `META_ACCESS_TOKEN` 沒問題。

**毛孩時代**（頁面 page_id `102542811256235`）：

| 通路 | account_id | 草稿/素材庫 adset（PAUSED，建廣告先放這裡） |
|---|---|---|
| CPAS_蝦皮 | `1921433368254905` | `120237531414500764`（campaign「Ad 上稿區 (轉換)」`120237531413470764`底下的「活動區1」；同 campaign 還有一個「常態區1」`120239145433960764`，性質=常態的素材放那個，2026-08-31 首次驗證用的是活動區1） |
| 官網 | `1318362572209550` | 還沒用過，沒有已知草稿 adset |
| CPAS_momo | `3317877141845136` | `120239473345990416`（campaign「Ad 上稿區」`120239473346170416`底下的「活動區1」；同 campaign 還有「常態區1」`120241071173260416`，性質=常態放那個）。活動區1綁的 product_set_id：`413835691224989`，只適用這個 adset |

CPAS_蝦皮 活動區1 綁的 product_set_id：`991924725624265`——**只適用於這個 adset**，換別的 adset 一定要重查 `promoted_object.product_set_id`，不要照抄這個值。

毛孩時代進稿表的「素材連結」欄常常是純文字標籤，不是真連結（2026-08-31 確認）——真正的檔案連結在 `creative-mp4`（影片）、`creative-1200`／`creative-920`（圖片，不同尺寸版本）這三個欄位，已經補進佇列表的 `FIELD_DEFS`。格式含「影片」的列，下載來源要看 `creative-mp4`，不要看「素材連結」。

**毛孩時代帳號的兩個額外陷阱（2026-08-31 首次驗證時發現，御熹堂沒遇到過）**：
1. 建 creative 這一步用 `META_ACCESS_TOKEN`（廣告帳號token）會撞到 App 開發模式錯誤（subcode 1885183）——改用 `META_PAGE_ACCESS_TOKEN` 呼叫 `/adcreatives` 就能繞過（這組 token 掛的是另一個沒有開發模式限制的 App）。毛孩時代的粉專 Page Token 可以直接用御熹堂既有的 `META_PAGE_ACCESS_TOKEN` 換出來（`GET /{毛孩page_id}?fields=access_token&access_token={現有的META_PAGE_ACCESS_TOKEN}`），兩品牌粉專在同一個 Business Manager，不用另外跟 Molly 要新 token。細節見 [[project_meta_cpas_app_dev_mode_blocker]]。
2. 這裡才第一次抓到「creative 頂層不能帶 `product_set_id`」這條規則（第4步共用小節已經同步改過來了）——商品格已經靠 Canvas 裡的 `canvas_product_set` 元素內嵌，頂層重複帶會在建廣告那一步報 `Cannot use product set id without template spec`（subcode 1990065）。

**待上稿佇列試算表**（Apps Script 自動同步出來的小表，見第1步）：

| 品牌 | 佇列表 ID |
|---|---|
| 御熹堂 | `1NJmv82qr5ZhOeIdUs4UiojokJonmzftX3SLCC0LhqfQ` |
| 毛孩時代 | `11Oe5SBZuef7H-eUyLuAemcfMaUhOMzrllunqmgReWKU` |

這些 ID 都可能過期或被 Molly 手動調整過（例如她刪掉某個草稿 adset、或佇列表被重建），**執行前先用 `read_file_content`／Meta Ads MCP 讀一次確認還存在、還是原本的用途，不要盲目照抄就寫入**；如果對不上了，照第1、5步的方法重新查，同時更新這個表格。

---

## 執行步驟

### 1. 找出要處理的列

**優先用「待上稿佇列」試算表**：御熹堂／毛孩時代各自的進稿追蹤表都很大、公司內部權限，直接讀常常抓不到指定列或拿到舊快照——不要在那份大表上硬找。

改讀對應的 Apps Script（御熹堂是 `yunxitang_creative_queue_sync.gs`，毛孩時代是既有的「進稿+素材切角.gs」，都裝在各自的進稿表上，選單「待上稿佇列」）自動同步出來的**小佇列試算表**，這份表只放篩選過的待處理列（可上稿=TRUE 且 已上稿=FALSE），檔案小、內容準確，直接用 `read_file_content` 連接器讀，不會有大表的過期/讀不全問題。查詢佇列表 ID 用 Script Properties 裡的 `QUEUE_SPREADSHEET_ID`，或請 Molly 給連結。

篩選條件（佇列表已經先篩過可上稿/已上稿，這裡只需再篩格式）：`格式` 是「影片」「動圖」「單圖」「多圖」「輪播」其中之一（多個值用逗號分隔時，任一個對到都算）。

**一次處理多筆時，優先順序是「影片／動圖」先做，「單圖／多圖／輪播」後做**（2026-08-31 Molly 明確要求）——因為 Google Apps Script 那套既有工具本來就能處理圖片，但完全不會處理影片；影片/動圖只有這個 skill 能做。批次處理時的對話長度/token 有限，要優先把「只有這裡能做」的事做完，圖片沒處理到還有 Apps Script 兜底，不用擔心漏掉。

佇列表沒有更新、或 Molly 提到的素材不在裡面時，才退回請她直接把該列內容貼過來（含格式欄）。**「素材連結」欄位有時候不是真正的 Drive 網址，而是純文字標籤/檔名**（毛孩時代這份表就出現過這種情況）——這種要先跟 Molly 確認實際檔案存放位置，不要假裝那是連結硬試下載。

**完成一列的收尾**：不要自己嘗試寫回試算表（連接器沒有 Sheets 寫入能力）。廣告建立完成後，提醒 Molly 到對應的待上稿佇列表把該列「已上稿」打勾，再從 Apps Script 選單執行「寫回已上稿」，就會照 `_來源分頁`/`_來源列號` 精準寫回進稿表原始那一列。

### 2. 判斷帳號

用 `原始網址`／`目標網址` 判斷通路（跟 meta-ad-copy 找帳號邏輯一致，只是這裡先用網域直接判斷，不用等 `find` 反查）：

- 網址含 `shopee`/`蝦皮` → 該品牌的 `_CPAS_蝦皮` 帳號
- 網址含 `momo` → 該品牌的 `_CPAS_momo` 帳號
- 官網網域（例如 `tsa1.cc` 短連結背後多半是官網或蝦皮，看原始網址判斷） → `_官網` 帳號

品牌關鍵字（文案/品項提到「御熹堂」「毛孩時代」等）+ 通路 → 用這個查實際 account_id：

```bash
cd "C:\AI小摸" && python meta_ad_copy_cli.py accounts <品牌關鍵字>
```

同品牌對到多個通路帳號、判斷不出來時才問 Molly，不要每次都問。

### 3. 抓 Drive 檔案

`素材連結` 欄可能是：

- **單一檔案**：`.../file/d/<id>/view...` 或 `.../open?id=<id>`
- **資料夾**（內含多個檔案）：`.../drive/folders/<id>` —
 - 格式＝影片/動圖：**資料夾內每一支影片都要各自建一則廣告**，不要只挑一支
 - 格式＝單圖：資料夾通常放的是同一張圖的不同尺寸／裁切版本，先問 Molly 要用哪一個，不要自己猜一張
 - 格式＝多圖/輪播：資料夾內多張圖通常是要組成**同一則廣告的輪播卡片**（一個 creative 裡多張圖），不是各自建一則廣告——先跟 Molly 確認排列順序/張數

下載方式：這份表通常是「僅限公司內部」分享，`gdown` 匿名下載會直接失敗（`FileURLRetrievalError`）。改用已連接的 Google Drive 連接器：

- 單一檔案：`download_file_content(fileId=...)` 拿 base64，解碼寫成本地檔案（回應太大時工具會自動存成本地檔案，用 python 讀取 JSON 裡的 `content` 欄位解碼）
- 資料夾：先用 `search_files` 以 `parentId = '<folder_id>'` 列出裡面的檔案（mimeType 篩 `video/` 或 `image/`），再逐一用 `download_file_content` 抓

### 4. Ad 素材建立（上傳素材 → 判斷一般／精選集 → 建 creative）

先判斷格式走哪條路——**Video**（影片／動圖）還是 **Image**（單圖／多圖／輪播），兩條路的上傳方式、Canvas 元素類型都不一樣，不要混用。

**兩條路共用的判斷**：帳號是 CPAS 通路（`_CPAS_蝦皮`／`_CPAS_momo`）→ 預設走精選集（2026-08-31 Molly 確認）——CPAS 本來就是綁目錄的協作通路，精選集能把商品格帶出來，比純素材更符合這個通路的用法。`_官網` 帳號才看這一列的 `活動` 或 `廣告名稱` 欄位有沒有出現「精選集」字樣來決定，沒出現就走一般路線。Molly 直接講要做哪一種的話，以她講的為準，這兩條規則只是沒特別說時的預設值。

**廣告名稱裡的格式段一律填來源素材的原始格式（單圖／影片／動圖），不要填「精選集」**（2026-08-31 Molly 糾正過）——精選集只是 Meta 端呈現的包裝方式，不是素材本身的格式，命名要跟表格「廣告名稱」欄的既有慣例一致。例如格式=單圖的素材做成精選集，廣告名稱還是要用 `..._單圖_...`，不是 `..._精選集_...`。

---

#### Video（影片／動圖）

**上傳**：用 `facebook-business` Python SDK，**不要用 `file_url`**——Drive 連結 Meta 抓不到。本地檔案要走 `source` 參數：

```python
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

FacebookAdsApi.init(access_token=os.getenv('META_ACCESS_TOKEN'))
account = AdAccount(f'act_{account_id}')
result = account.create_ad_video(params={'source': local_path, 'name': ad_name})
video_id = result['id']
```

輪詢處理狀態直到 ready：

```python
from facebook_business.adobjects.advideo import AdVideo
video = AdVideo(video_id)
# 每 10 秒查一次，最多等 10 分鐘
status = video.api_get(fields=['status'])['status']['video_status']
```

抓縮圖（`image_url`／`picture` 用得到，兩種格式都要）：

```python
thumbs = list(video.get_thumbnails(fields=['uri', 'is_preferred']))
thumbnail_url = next((t['uri'] for t in thumbs if t.get('is_preferred')), thumbs[0]['uri'])
```

**陷阱：縮圖 uri 很長，處理／複製時絕對不能截斷**——截斷後建 creative 會報 `Invalid Image In Ad: 圖片載入失敗`，一定要完整讀一次再用。

**一般 creative**（用 Meta Ads MCP `ads_create_creative`）：

```
ads_create_creative(
 ad_account_id, page_id, video_id,
 image_url=<完整縮圖uri>,
 message=<文案>, headline=<標題>, description=<描述>,
 link_url=<目標網址>, call_to_action_type='SHOP_NOW'（或依內容判斷）,
 name=<廣告名稱，含走期字首>
)
```

**精選集（Canvas，影片封面＋下方商品格）**：Meta Ads MCP 不支援，要直接呼叫 Graph API，而且一定要用 `META_PAGE_ACCESS_TOKEN`（商品綁定判斷邏輯見下面「精選集商品綁定判斷」共用小節）：

1. 先確認目標 adset 的 `promoted_object.product_set_id`（第5步會用到），**不要沿用其他 campaign/adset 的 product_set_id**：
 ```python
 from facebook_business.adobjects.adset import AdSet
 AdSet(adset_id).api_get(fields=['promoted_object'])
 ```
2. 用 page token 依序建 4 個 canvas 元素：**`canvas_video`（影片封面）**、`canvas_button`（CTA 按鈕，`{"open_url_action":{"url":...}, "rich_text": json.dumps({"text":[{"type":"text","text":"SHOP_NOW"}]})}`——**按鈕文字欄位是 `rich_text`，不是 `text`/`style`**，2026-08-31 實測確認）、`canvas_footer`（掛按鈕）、`canvas_product_set`（掛第1步查到的 product_set_id）
3. 組合成 canvas：`POST /{page_id}/canvases`，`body_element_ids` 帶入上面4個 id 的其中3個（影片＋商品格＋footer），`is_published: true`
4. 建 Ad Creative：`object_story_spec.link_data.link` = `"https://fb.com/canvas_doc/" + canvas_id`，`link_data.picture` 放縮圖網址。**creative 頂層不要帶 `product_set_id`**（跟舊版本說的相反，2026-08-31 毛孩時代帳號實測發現多帶會在建廣告那步報 `Cannot use product set id without template spec`，subcode 1990065——商品格已經靠 Canvas 裡的 `canvas_product_set` 元素內嵌了，頂層不用也不能重複帶）

---

#### Image（單圖／多圖／輪播）

**上傳**：影片用的 `file_url` 限制對圖片一樣成立（Drive 連結抓不到），一樣要本地下載後用 `AdImage` 類別上傳（**不是 `AdAccount.upload_image`，那個方法不存在，會報 `AttributeError`**）：

```python
from facebook_business.adobjects.adimage import AdImage
image = AdImage(parent_id=f'act_{account_id}')
image[AdImage.Field.filename] = local_path
image.remote_create()
image_hash = image[AdImage.Field.hash] # 拿去餵 ads_create_creative 的 image_hash 參數
```

圖片不用等處理狀態、不用抓縮圖，比影片單純很多。多圖/輪播格式要把每張圖都各自上傳拿到 hash，組成 `ads_create_creative` 的 `cards` 陣列。

**一般 creative**（用 Meta Ads MCP `ads_create_creative`）：

```
ads_create_creative(
 ad_account_id, page_id, image_hash,
 message=<文案>, headline=<標題>, description=<描述>,
 link_url=<目標網址>, call_to_action_type='SHOP_NOW'（或依內容判斷）,
 name=<廣告名稱，含走期字首>
)
```

**精選集（Canvas，圖片封面＋下方商品格）**：跟 Video 版同一套流程，只有封面元素不同（商品綁定判斷邏輯見下面「精選集商品綁定判斷」共用小節）：

1. 先確認目標 adset 的 `promoted_object.product_set_id`（第5步會用到），**不要沿用其他 campaign/adset 的 product_set_id**：
 ```python
 from facebook_business.adobjects.adset import AdSet
 AdSet(adset_id).api_get(fields=['promoted_object'])
 ```
2. **圖片封面要先上傳成粉專相片，不是廣告素材庫的 AdImage**：`POST /{page_id}/photos`（`published: false`、`source` 帶本地檔案、用 page_token），拿到的相片 `id` 才是 `canvas_photo` 要的 `photo_id`（`canvas_photo: {"photo_id": ..., "style": "FIT_TO_WIDTH"}`——直接塞 AdImage 的 `image_hash` 會報 `Invalid keys "image_hash"`，2026-08-31 實測）。接著依序建 4 個 canvas 元素：**`canvas_photo`（圖片封面）**、`canvas_button`（CTA 按鈕，欄位格式同 Video 版的 `rich_text` 寫法）、`canvas_footer`（掛按鈕）、`canvas_product_set`（掛第1步查到的 product_set_id）
3. 組合成 canvas：`POST /{page_id}/canvases`，`body_element_ids` 帶入上面4個 id 的其中3個（圖片＋商品格＋footer），`is_published: true`，creative 頂層一樣不要帶 `product_set_id`（理由同 Video 版）
4. 建 Ad Creative：`object_story_spec.link_data.link` = `"https://fb.com/canvas_doc/" + canvas_id`，`link_data.picture` 放圖片網址，creative payload 頂層不要帶 `product_set_id`（理由同 Video 版）

完整技術細節與已驗證步驟見 [[project_drive_video_to_meta_upload_pipeline]]。

---

#### 精選集商品綁定判斷（Video／Image 共用）

**先判斷主打商品(`retailer_item_ids`)怎麼決定**——這一步要在建 `canvas_product_set` 元素之前想清楚，不要每次都預設問 Molly，依這個表格判斷（2026-08-31 已跟 Molly 確認）：

| 情境 | 判斷方式 | 商品綁定方式 |
|---|---|---|
| 表格「帶品(最多4)」欄位有明確指定商品 | 直接讀這個欄位 | 去目錄比對商品名稱拿到 `retailer_item_id`，最多4個，寫進 `retailer_item_ids` |
| 沒指定，`目標網址`/`原始網址` 導到首頁類（全站/檔期活動頁，不是單一商品） | 不用猜、不用爬網頁抓「頁面前幾品」 | `canvas_product_set` 用**動態選擇**（不帶 `retailer_item_ids`，或帶空陣列），交給 Meta 依使用者行為自動判斷——爬網頁抓排序既脆弱（改版就失效）又多此一舉（Meta 本來就有更好的個人化信號） |
| 沒指定，連結導到單一商品頁 | 去目錄用 `url` 欄位比對連結，找出對應的 `retailer_id`（也可以直接用 `retailer_id` 精確比對，商品連結網址常帶著它，例如蝦皮商品網址結尾那段數字） | 只綁這一支商品（`retailer_item_ids` 只有1個） |

「帶品(最多4)」欄位常見值是「X (系統自動帶)」——這代表「留給系統自動帶」，等同上表第2種情境，不是要你去查一個叫「X」的商品。

商品名稱比對目錄用 `ads_catalog_list_products`：**這個工具的 `filter` 參數目前對中文 `i_contains` 查詢常會回傳 `total_count` 有值但 `products` 是空陣列**（已知怪異行為，2026-08-31 實測），不要卡在這裡重試同一種篩選法——改用不帶 `filter` 的分頁列表（`limit=100`，用 `cursor` 翻頁），自己在結果裡比對名稱關鍵字更可靠；`retailer_id` 精確比對（`filter={"retailer_id":{"eq":"..."}}`）不受這個問題影響，能用就優先用。找不到明確對應的商品時，把找到的候選列給 Molly 確認，不要自己猜一個就綁上去。

**陷阱**：
- 4 個 canvas 元素一旦被組進某個已發布的 canvas，就不能拿去組第二個——選錯 product_set 要整組 4 個元素重建，不能只換一個
- **已建立的 canvas 元素無法用 `POST /{element_id}` 更新**（會報 `(#3) Application does not have the capability to make this API call`）——要改內容（例如換綁的商品）只能整組 4 個元素重建、建新 canvas、建新 creative，不要浪費時間重試更新
- creative 的 `product_set_id` 沒有跟目標 adset 的 `promoted_object.product_set_id` 對齊，建廣告那一步會報 `Creative Product Set Inconsistent with Promoted Object`
- 廣告要換成新 creative 不用整支重建，`POST /{ad_id}` 帶 `creative: {"creative_id": "..."}` 就能直接換掉既有廣告的 creative，同一個 ad_id 保留
- Ads Manager 介面上「目的地貼文即時體驗→應用程式」這類進階目的地設定（例如蝦皮帳號要選蝦皮 App），目前這組 App 權限讀不到也寫不到相關欄位（`store_url`／`dynamic_setting` 都報同樣的 capability 錯誤），這一項只能請 Molly 自己在介面上手動設定，不要嘗試用 API 硬做
- 草稿 adset 建太多支廣告，可能撞到 Meta 平台限制「Sales campaigns using Advantage+ Audience can contain a maximum of 150 ads」——**這是單一 adset 的上限，不是整個 campaign**（用 Ads Manager 篩選單一 adset 才看得出來）。撞到時**不要用 API 直接建全新 adset**，會卡在 `compliance_section`（台灣廣告主驗證資訊，`regional_regulated_categories`/`dsa_beneficiary`/`dsa_payor` 都補不了）——改用 `POST /{既有adset_id}/copies`（`deep_copy=false`, `rename_options={"rename_strategy":"NO_RENAME"}`, `status_option=PAUSED`）複製一個已帶合法廣告主宣告的既有 adset 出來用，複製後用 `POST /{new_adset_id}` 帶 `name` 改名即可

完整已驗證步驟與逐段程式碼見 [[project_meta_collection_canvas_video_creation]]。

### 5. 判斷目標 adset

跟 meta-ad-copy 一致：**沒有明確候選時列出候選清單問 Molly，不可以自己亂填**。用 Meta Ads MCP 查該帳號裡跟這一列「性質」「目標」「品項」相符的 campaign/adset（例如「主打活動品」這類命名慣例），有多個候選就列表格讓 Molly選，不要自己猜一個就建下去。精選集的話，adset 的 `promoted_object.product_set_id` 還會回頭限制 Canvas 該用哪個商品集（見第4步「精選集商品綁定判斷」）。

### 6. 建立廣告，交給既有巡檢流程

**情境A：已經找到明確的正式 adset**（第5步有清楚候選、Molly 也確認要直接上）→ 廣告建在那個 adset 裡，直接接手巡檢：

```bash
cd "C:\AI小摸" && python check_new_ad_launch.py <ad_id>
```

行為完全比照 meta-ad-copy 第6/7步——巡檢通過、沒有走期字首就直接啟動，有走期字首就照走期自動開關排程，有 issues 就維持 PAUSED 回報給 Molly。不要在這裡重新判斷一次。

**一律先建在草稿 adset，不要因為判斷有信心就跳過這一步**（2026-09-01 Molly 明確要求）——就算目標正式 adset 的判斷邏輯已經很確定（例如同一批素材裡已有前例），也要先進草稿留下操作軌跡，方便事後查證。但**判斷有信心的話，建完草稿後不用停下來等她確認，直接自動接著把廣告移到正式 adset**（刪除草稿廣告→用同一個 creative_id 在正式 adset 建新廣告，不要用 `POST /{ad_id}` 改 `adset_id`，API 會回 success 但實際不會生效，見 [[project_meta_collection_canvas_video_creation]]）——「先建草稿」是查證機制，不是要她多審一次的關卡；只有真的沒把握該放哪個正式 adset 時，才停下來問她（走情境B）。

**情境B：沒有明確候選，或 Molly 直接說先放草稿 adset**（2026-08-31 確認的做法）→ 廣告建在草稿/素材庫 adset（PAUSED），**不要對它跑 `check_new_ad_launch.py`**（它不在真正會投放的 adset 裡，巡檢/啟動沒有意義）。這批草稿完成、回報帳號＋廣告名稱清單給 Molly 之後，**直接接手觸發 meta-ad-copy skill**，讓它判斷這些草稿該搬到哪個正式 adset——這是 Molly 已經確認的標準銜接方式，回報完就接著做，不用每次都重新問一次要不要接手。meta-ad-copy 自己有「先出計畫、Molly 確認才執行」的安全機制，接手呼叫它不會跳過確認這一步。

### 7. 回填試算表「已上稿」

成功建立廣告後，提醒 Molly 到對應的待上稿佇列表把該列「已上稿」打勾，再從 Apps Script 選單執行「寫回已上稿」（見第1步）——這樣會照 `_來源分頁`／`_來源列號` 精準寫回進稿表原始那一列，避免下次重複處理同一列。連接器沒有寫入 Sheets 的能力，這一步無法自動代勞，明確講清楚是哪一列，不要跳過不提，也不要假裝自動做了。

---

## 注意事項

- 素材連結格式不一致是常態（單檔連結 vs 資料夾連結，`file/d/`格式 vs `open?id=`格式，甚至可能是純文字標籤不是真連結），下載前先判斷是哪一種，別假設
- 新廣告一律先建 PAUSED，啟動與否交給第6步的巡檢結果決定
- 一次要處理多支素材時，先把清單（含帳號/格式判斷/adset候選）一次列給 Molly 確認，不要每支都問一輪
- 常態、量大的單圖上稿如果 Molly 有既有的 Apps Script 流程在跑，不用主動搶著接手；她開口要這裡處理才做
- 這個 skill 不負責改預算、改受眾、改出價，也不負責挑素材切角/命名（那是 [[meta-ad-copy]] 或 [[project_meta_creative_tag_naming]] 的範圍）
