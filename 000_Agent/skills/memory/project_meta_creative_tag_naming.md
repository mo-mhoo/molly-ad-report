---
name: project-meta-creative-tag-naming
description: 御熹堂 Meta 素材「素材切角」欄位標準化命名專案，2026-08-14 定案樣板與詞庫，已做成 meta-creative-tag-naming skill
metadata:
  type: project
  originSessionId: 6c70015e-7f4e-4e64-83d0-8b456a462ed6
  modified: 2026-08-14T03:20:04.667Z
---

Molly 想把「素材進稿追蹤表」（見 [[reference_meta_asset_tracking_sheet]]）裡自由輸入的「素材切角」欄位改成有規則的命名，方便之後分析歸納重點。2026-08-14 跟 Molly 一起定案，已做成 [[meta-creative-tag-naming]] skill，之後直接呼叫該 skill 即可，不用重講一次規則。

**命名樣板：** `素材切角 = [訴求角度]_[信任元素]`，串進廣告名稱時沿用帳戶原本就在用的格式 `[走期]_[常態|活動|KOL]_[格式]_[品項]_[訴求角度]_[信任元素]_[料號]`——這套格式是從御熹堂官網帳號（800743741924152）近30天實際素材命名反推出來的，不是憑空發明。

**Why:** 原本切角欄位是自由輸入（例如「健字號苦瓜價購590x鄭醫師」），同一個代言人會被打成不同寫法（鄭醫師/鄭醫生/鄭均云醫師），拆解分析時變成碎片標籤，看不出真正的成效歸因。

**How to apply:** 之後只要 Molly 提到「素材切角」「素材命名」，直接觸發 skill，不用在對話裡重新討論維度或格式。

**已知的技術限制：** 這份追蹤表的明細分頁「官網_素材進稿」沒辦法直接用 Drive 連接器整份讀出（見 [[reference_meta_asset_tracking_sheet]] 的限制說明），需要 Molly 配合複製分頁或提供截圖。

相關：[[feedback_text_source_over_image_ocr]]、[[feedback_meta_report_depth_and_format]]（素材要拆格式/訴求主題/信任元素三維度分析的既有原則）
