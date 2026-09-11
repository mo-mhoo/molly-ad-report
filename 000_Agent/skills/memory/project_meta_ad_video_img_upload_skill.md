---
name: project-meta-ad-video-img-upload-skill
description: meta-ad-video-img-upload skill：從Drive原始檔案(影片/動圖/單圖)一路建到Meta廣告(含精選集)，跟meta-ad-copy是互補分工，不是取代
metadata:
  node_type: memory
  type: project
  originSessionId: bc9f80f0-27dc-40fe-b7b0-88b95c2160ba
  modified: 2026-09-07T06:37:39.514Z
---

2026-08-31 新建的 skill，檔案位置 `C:\Users\Molly Ho\.claude\skills\meta-ad-video-img-upload\SKILL.md`。命名演進：`meta-video-upload`（Molly 覺得看不出跟廣告相關）→ `meta-video-ad-upload`（跟 `meta-ad-copy`／`meta-ad-launch-check` 命名一致）→ 單圖上稿測試成功後擴大範圍涵蓋單圖，改成 `meta-asset-upload`（不再限定影片）→ Molly 要求名稱加入好辨識的元素，改成 `meta-ad-video-img-upload`（ad/video/img 都直接放進名字）。

**跟 [[project_meta_ad_launch_check_skill]]／meta-ad-copy 的分工**：meta-ad-copy 處理「素材已經在 Meta 裡（貼文/既有廣告）」的複製上稿；這個 skill 專門處理「素材完全沒進過 Meta、只有 Drive 連結」的從零上傳——讀進稿追蹤表→判斷帳號→抓 Drive 檔案→上傳素材→（視情況）建精選集 Canvas→拿到 creative_id，之後直接交給 meta-ad-copy 既有的巡檢/啟動流程收尾，不重新設計那段判斷邏輯。

技術細節（Drive下載、SDK上傳影片/圖片、Canvas API、product_set對齊、已知API限制）都寫在 skill 本文，逐段驗證過的程式碼另外存在 [[project_drive_video_to_meta_upload_pipeline]] 和 [[project_meta_collection_canvas_video_creation]]。

**格式判斷規則（2026-08-31 確認）**：CPAS 通路帳號（`_CPAS_蝦皮`／`_CPAS_momo`）預設一律走精選集（Canvas），因為 CPAS 本來就是綁目錄的協作通路，精選集能把商品格帶出來，比純影片/單圖更符合這個通路的用法；`_官網` 帳號才看表格「活動」或「廣告名稱」欄位有沒有出現「精選集」字樣來決定要不要走精選集，沒出現就走一般路線。Molly 有明講要哪一種就以她講的為準，這兩條只是沒特別說時的預設值。

**單圖也涵蓋，但有優先順序（2026-08-31 確認）**：一次要處理多筆時，**影片/動圖優先於單圖/多圖/輪播**——因為 Google Apps Script 那套既有工具（御熹堂/毛孩時代各自的「素材連結回填工具」）本來就能處理圖片上稿，但完全不會處理影片；批次處理時對話長度有限，要優先做「只有這個 skill 能做」的事，圖片沒處理到還有 Apps Script 兜底。常態、量大的單圖上稿如果 Molly 有既有 Apps Script 流程在跑，這個 skill 不用主動搶著接手，她開口要處理才做。

**讀取來源已解決（2026-08-31）**：御熹堂裝了 `yunxitang_creative_queue_sync.gs`（進稿表本身的 Apps Script，選單「待上稿佇列」）自動同步出一份小的「御熹堂_待上稿佇列」試算表（ID: `1NJmv82qr5ZhOeIdUs4UiojokJonmzftX3SLCC0LhqfQ`），毛孩時代則是既有的「進稿+素材切角.gs」（同一份試算表：`毛孩_進稿`，ID `16l6pLAauIUksO7eHv9ZOJ7XEKpe89kH3g2Mpvb6AF_o`）擴充了同樣的 `FIELD_DEFS`（補上格式/活動/料號/廣告名稱等欄位）跟新增的 `writeBackPosted()` 函式，佇列表 ID: `11Oe5SBZuef7H-eUyLuAemcfMaUhOMzrllunqmgReWKU`。兩邊都只放可上稿=TRUE 且已上稿=FALSE 的列，用 `read_file_content` 連接器讀這份小表完全準確，不再需要請 Molly 手動貼列內容。完成上稿後，請 Molly 在佇列表勾選「已上稿」、執行 Apps Script 選單「寫回已上稿」，會精準寫回進稿表原始列。

**已解決：毛孩時代的真實素材連結欄位（2026-08-31 確認）**——「素材連結」欄很多時候是純文字標籤/檔名（例如「2.豆腐砂」「901」「下單抽」），不是真正的 Drive 網址；**實際檔案連結在另外三個欄位：`creative-1200`（圖片1200px版本）、`creative-920`（圖片920px版本）、`creative-mp4`（影片檔案，格式=影片/動圖時要下載這個）**。已把這三個加進 `FIELD_DEFS`。判斷邏輯：格式含「影片」的列，下載來源看 `creative-mp4` 欄，不要看「素材連結」欄。

**重大陷阱（2026-08-31）：「已上稿」欄位可能是公式欄，不能用 `setValue()` 硬寫**——御熹堂進稿表既有的 `CreativeLinks.gs` 明確註明「已上稿」是自動判斷的公式欄（特地排除在它自己的 `MANUAL_HEADERS` 之外），不是人工填的欄位。一開始寫的 `writeBackPosted()` 用 `sourceSheet.getRange(...).setValue(true)` 直接寫入「已上稿」欄，會蓋掉該儲存格原本的公式，導致那一列以後不會再自動更新——**已改成不寫入分頁、只列出清單提醒人工確認**。往後任何要「寫回進稿表」的功能，動手寫入前一定要先確認目標欄位是不是公式欄（可以問 Molly，或看該分頁本身有沒有類似 `CreativeLinks.gs` 那種排除公式欄的既有邏輯可以參考），不能假設欄位都是人工可寫的。

**多檔案 Apps Script 專案的 `onOpen` 慣例**：御熹堂進稿表的 Apps Script 專案裡已經有 `CreativeLinks.gs`（素材工具）、`UtmLinks.gs`（UTM工具）等別人維護的既有檔案，同一個專案裡只能有一個 `onOpen()` 生效（多個會互相蓋掉，不會報錯但選單會消失）。既有的正確模式是：只有 `CreativeLinks.gs` 定義 `onOpen()`，其他檔案改成一般函式（例如 `UTM_buildMenu(ui)`），由 `onOpen()` 用 `if (typeof X_buildMenu === 'function') X_buildMenu(ui);` 依序呼叫。新增功能一律照這個模式做（例如這裡新增的 `QUEUE_buildMenu(ui)`），不要自己在新檔案裡另外寫一個 `onOpen()`，會蓋掉別人的選單。

**御熹堂_CPAS_momo（`7609150239116011`）已找到草稿adset（2026-08-31 首次使用，7支活動廣告全部驗證成功）**：`Ad素材庫｜活動2`（adset ID `120245270544160507`，屬於「Ad素材庫」campaign `120225655563600507`，PAUSED，Advantage+ 18-65全台受眾），綁定的 `promoted_object.product_set_id` 是 `405238042213877`（目錄「All Products」，50支商品，無篩選規則，可涵蓋任何單一商品的 `retailer_item_ids` 綁定）。這個 adset ID 是 Molly 直接提供的，不是自動查出來的——skill 本文（第4/5步）原本標註這個帳號「還沒用過，沒有已知草稿adset」，下次遇到同帳號可以直接沿用這個 adset，不用再問。

**御熹堂_官網（`800743741924152`）常態單圖草稿adset已找到（2026-09-07首次驗證）**：草稿/素材庫campaign「Ad 素材庫」(`120210717941760735`)底下有「常態」(`120210717941790735`)跟「常態2」(`120237823971530735`)兩個adset——2026年起新的常態單圖/影片都放在**常態2**（常態原始那個adset近期沒再用，都是2025年以前的舊料），跟skill本文資源速查表只記錄了這個帳號「活動1/活動2」是同一種缺口，下次可以直接補進去。

**御熹堂_官網也會踩到App開發模式類阻擋，但這次擋在上傳圖片這一步（2026-09-07）**：用一般的`META_ACCESS_TOKEN`打`/act_{account}/adimages`（不管是SDK的`AdImage.remote_create()`還是直接呼叫Graph API）會報`(#100) subcode 33`「Object with ID ... does not exist, cannot be loaded due to missing permissions, or does not support this operation」——GET這個帳號完全正常，只有POST圖片會擋。解法跟既有的creative端點阻擋([[project_meta_cpas_app_dev_mode_blocker]])一樣：換成粉專Page Token（`GET /{page_id}?fields=access_token&access_token={.env的META_PAGE_ACCESS_TOKEN}`換出來的那組）呼叫`/adimages`就正常。建creative那步這次用`ads_create_creative` MCP工具直接成功，沒有再踩到端點阻擋。

**御熹堂_官網有一個「流量ATL(PV)｜新客｜測試包(202607)」campaign（`120252070803830735`）專門測試不同興趣TA**：底下adset命名都是「新客｜{興趣}｜排除購買名單」，2026-09-07查到的成效快照（CTR排序，ATL用CTR判斷）：LAL會員5-10%(6.76%) > 健康飲食興趣(5.56%) > Broad(4.12%) > 美容養顏興趣(4.02%) >> 戶外運動族群(1.33%，已PAUSED) > 料理網購咖啡興趣(1.13%，已PAUSED)。這份排名是特定時間點快照，之後會變動，但可以當作「常態單圖找不到明確TA候選時」的參考起點——內容主題關鍵字對應的興趣不一定是最佳選擇（例如UC2素材提到「久站/運動量大」，直覺會選戶外運動族群，但這組實際成效是最差、已被關掉，改選健康飲食興趣這種泛健康保健受眾反而CTR更好）。

**重大教訓：「鄭鈞云醫師」掛名的常態單圖，很多時候貼文本身已經存在（未發布dark post），不是純Drive素材（2026-09-07）**——這次御熹堂官網UC2單圖一開始判斷成「素材只有Drive連結、完全沒進過Meta」直接走這個skill從零上傳建creative，結果Molly糾正：這篇她已經用鄭醫師粉專(`1978357732385294`)發過（is_published:false的dark post），應該用`object_story_id`引用既有貼文建廣告，不是重新上傳圖片建全新creative。**判斷依據**：命名裡帶「鄭鈞云醫師/...」的常態單圖/影片，只要Molly説「這篇我有過」或類似語氣，優先假設貼文已存在於她粉專，應該去該粉專的Graph API直接try `{page_id}_{post_id}`（post id從Molly那邊拿，`/posts`或`/feed`就算加`include_hidden=true`也搜不到未發布貼文，沒有找到能列出全部未發布貼文的端點/權限，只能請她給連結或post id）確認後用`ads_create_creative`的`object_story_id`參數建（不能跟`image_hash`/`message`等欄位並用），這其實是[[meta-ad-copy]]的範圍（素材已在Meta），不是這個skill的範圍——下次遇到同類命名素材，**先反問或確認是否已有既有貼文**，不要預設一定是純Drive上傳。

**API建廣告一定要手動補`tracking_specs`，Ads Manager手動建的話是自動帶的（2026-09-07）**：這個帳號常態單圖廣告(不管走哪個adset/campaign)都會固定帶：pixel `196186853297082`的`offsite_conversion`、一組固定`conversion_id`("24073927592256144")的`onsite_conversion`、一組固定11個`conversion_id`陣列的`onsite_conversion`（帳號層級的custom conversions清單，換素材不用換這組值）、以及對應到creative本身`object_story_id`那個貼文的`post_engagement`/`link_click`/`post_interaction_gross`三組（page+post id要跟creative的post一致）。用`ads_create_ad`/`ads_create_creative` MCP工具或原始Graph API直接建，這組`tracking_specs`不會自動帶，要手動組進`ads_create_ad`的`tracking_specs`參數，不然就是「追蹤事件沒設定」——查這組固定值的方法：抓同一個adset裡任一支既有廣告(`ads_get_ad_entities`或直接GET ad的`tracking_specs`欄位)當模板照抄，只換post/page id。另外`ads_update_entity`不能拿來換既有廣告的creative（會報「creative immutable」），要換creative只能建一支新ad換掉舊的，舊的直接PAUSE留著就好，不用刪。

**御熹堂_官網常態單圖也要同時鋪多個ATL新客campaign，不是只鋪一個（2026-09-07）**——這支UC2單圖一開始只鋪了「流量ATL(PV)｜新客｜測試包(202607)」，Molly事後追加「interact也複製到合適ad set」，才又鋪進「流量ATL(Interact)｜新客」(`120236312952980735`)。這帳號的ATL新客campaign其實還有更多變體（Con-Clicks／Clicks／ATC／Reach，見campaign清單），跟[[project_maohai_official_campaign_structure]]記錄的毛孩時代「BTL舊客/ASC/ATL新客Interact三種campaign同時鋪」是同類型陷阱——**常態單圖上稿完一個ATL campaign後，不要預設只要鋪這一個，主動確認是否也要鋪其他ATL新客campaign**，不用每次等她講「XX也複製」才反應。每個campaign底下的候選adset命名/興趣清單都不一樣（PV測試包是「新客｜{興趣}｜排除購買名單」六選一，Interact是「新客｜{熟女/醫美/膽固醇/Broad}」），要各自重新用實際CTR數據判斷，不能假設兩邊命名對得起來、也不能照搬同一個adset選擇邏輯。

相關：[[project_drive_video_to_meta_upload_pipeline]]、[[project_meta_collection_canvas_video_creation]]、[[project_meta_ad_launch_check_skill]]、[[feedback_sheets_emoji_mojibake]]、[[project_meta_cpas_app_dev_mode_blocker]]、[[meta-ad-copy]]、[[project_maohai_official_campaign_structure]]
