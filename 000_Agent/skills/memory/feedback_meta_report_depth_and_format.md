---
name: feedback-meta-report-depth-and-format
description: Meta廣告成效報告的深度與格式標準— ATL絕不用ROAS評估、素材要有縮圖、素材要按格式/主題/信任元素分類分析、視覺化報告用Google簡報16:9尺寸
metadata:
  node_type: memory
  type: feedback
  originSessionId: e87f2e7f-7ca5-405d-8c54-4635a1cc2a12
  modified: 2026-08-12T02:00:23.749Z
---

2026-08-12，整理毛孩時代官網 7月 Meta 成效 artifact，第一版被用戶點名「沒有先想過再動手很不成熟」，具體指出四個問題，第二版重做後才過關。記錄下來避免重蹈覆轍。

**問題1：ATL 週趨勢圖混用了 ROAS 折線。** 這其實違反了已存在的 [[feedback_atl_btl_testing_logic]]（ATL用CTR/頻次/CPM，不用購買/ROAS）——代表我當下沒有主動去檢查記憶庫就動手做圖表，只是憑印象處理。

**Why:** ATL 目的是觸及/曝光，不是轉單，用 ROAS 評估等於用錯誤的尺量錯誤的目的，結論會誤導決策者以為 ATL「效果差」而砍掉本來就該長期投放的觸及預算。

**問題2：檔期(campaign calendar)沒有跟指標並排對照。** 用戶指名參考官網月報的日趨勢圖畫法——長條(業績/花費)+折線(轉換率/ROAS)+下方檔期時間軸(色塊區間)三者對齊呈現，而不是抽象的週別bar chart。

**問題3：素材分析太淺、且完全沒有縮圖。** 用戶原話：「素材是meta成敗的靈魂，你歸納的重點卻很少」「官網文、比較文、獸醫、影片、品類、圖片、試吃...等等都是元素可以歸納分析」「也沒有列出素材縮圖代表你在做meta成效報告沒有去讀skill」。

**How to apply（下次做 Meta 成效報告前，動手前先做這幾件事）：**
1. **先讀 `tsa-meta-ads-operations` skill 全文，並主動比對 [[feedback_atl_btl_testing_logic]] 和 [[feedback_meta_report_creative_level]]**，不要單憑記憶動手；這兩則已經寫明 ATL 只能用 CTR/CPM/頻次，絕對不能拿 ROAS/CPA 評估 ATL 素材或活動。
2. **素材分類要拆到「格式」「訴求主題」「信任元素」三個維度**，用 ad 名稱關鍵字比對歸類（原生圖比較文/十大素材/AI單圖/AI多圖比較文/影片/活動單圖/KOC-KOL；皮膚/關節/腸胃/貓砂/魚油/離胺酸/葉黃素/肉泥/免疫力/化毛等；獸醫背書/KOL-KOC/十大素材權威感），再交叉算各維度的 CTR/CPM（ATL側）或 ROAS/CPA（BTL側），做成「資源配置效率排行」（例如：某訴求主題BTL花費占比過低=幾乎只做觸及沒做轉單），比單純列 Top N 花費排名有價值得多。
3. **素材縮圖要放實際圖片，不能只有文字。** 流程：先用 `ads_get_ad_entities`(level=ad) 找出目標 ad id → 用該 ad id 查 `creative_id`(ad層級欄位) → 用 `ads_get_creatives`(creative_ids=[...], fields=["image_url","thumbnail_url","video_id"]) 拿到圖片網址 → curl 下載到 scratchpad → 用 PIL 縮圖壓縮(約360px寬, JPEG quality~70)並轉 base64 → 以 data URI 內嵌進 artifact 的 `<img>`（Artifact CSP 擋外部圖片，不能直接掛 CDN 網址）。範例流程與踩雷紀錄見本次 session。
4. **意外發現的QA價值：下載縮圖時發現素材命名與實際創意內容不符**（一支叫「全犬飼料」的廣告，實際掛載的創意是「貓飼料推薦」文章）——抓縮圖不只是為了好看，也是交叉驗證素材數據是否可信的手段，發現類似落差要主動點出來，不要略過。
5. **視覺化的成效報告用 Google 簡報比例（16:9，作者畫布1280x720）**，用 CSS `aspect-ratio` + JS `transform:scale()` 做響應式縮放，搭配上一頁/下一頁鍵盤與按鈕導覽，而不是預設的長捲軸報告版型；純文字/數據導向的報告（無需視覺化）才維持原本的長捲軸格式。

相關：[[feedback_atl_btl_testing_logic]]、[[feedback_meta_report_creative_level]]、[[project_meta_insights_report]]
