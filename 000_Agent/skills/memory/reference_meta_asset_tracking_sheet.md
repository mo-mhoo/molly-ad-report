---
name: reference-meta-asset-tracking-sheet
description: 御熹堂 Meta 廣告素材/進稿追蹤表的 Google Sheet 連結，含品項對應的 Drive 圖檔連結欄位
metadata: 
  node_type: memory
  type: reference
  originSessionId: b45bea3a-6113-436b-8a81-5ce9d3d10e59
  modified: 2026-08-14T03:19:35.987Z
---

御熹堂的 Meta 廣告素材管理／進稿追蹤表：
https://docs.google.com/spreadsheets/d/16nQN5DFG_wPUec03rgIl0f2kHyCtfKfb7dksN9d2uOc/edit

這份表是私人權限（gviz CSV 公開端點會 401），要透過已連接的 Google Drive/Sheets 連接器讀取（`read_file_content`／`download_file_content`），不能直接 curl。

內容包含多個分頁：專案 SOP 待辦清單、全年活動檔期日曆（滿額贈/滿額折/滿額抽，依日期橫向排列）、每日電商營收與廣告費追蹤，以及最關鍵的**素材上稿記錄表**——每列有「品項／圖檔連結（Drive）／主標題／文案／素材名稱」，可以直接查到某個品項過去用過哪些 Drive 圖檔，比在 Drive 裡盲搜品項關鍵字準確。

檔案很大（讀出來 ~45 萬字），用 `read_file_content` 讀出來會存成本地檔案，要用 Python/grep 過濾關鍵字，不要整份讀進 context。

**已知限制（2026-08-14 確認）：`read_file_content` 對這份多分頁大檔案只會匯出摘要分頁，不含明細分頁「官網_素材進稿」（Molly 截圖裡「素材切角」那份表就在這個分頁）；`download_file_content` 匯出整份 xlsx 又會因檔案太大被系統擋掉（"File too large for export"）。這兩個連接器工具都沒有「只抓單一分頁」的參數。遇到需要讀「官網_素材進稿」這類明細分頁時，不要重複嘗試整份檔案硬讀——請 Molly 把該分頁複製成獨立新試算表（右鍵分頁籤→複製到→新試算表，權限維持公司內部不公開），用新 fileId 讀，或請她直接貼分頁截圖（辨識完的文字要先給她核對過，見 [[feedback_text_source_over_image_ocr]]）。**

相關：[[feedback_drive_asset_search_by_flight_dates]]、[[feedback_spreadsheet_tools]]、[[feedback_text_source_over_image_ocr]]、[[project_meta_creative_tag_naming]]
