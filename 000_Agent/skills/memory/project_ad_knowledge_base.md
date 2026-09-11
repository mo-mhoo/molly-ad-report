---
name: project-ad-knowledge-base
description: 廣告知識庫（knowledge/ 資料夾）長期累積計畫，Meta/Google Ads 官方文件整理成結構化筆記，含互動式索引 Artifact
metadata: 
  node_type: memory
  type: project
  originSessionId: c915f99a-5c98-4c1b-acff-5d9576cffadc
  modified: 2026-08-21T00:40:08.334Z
---

2026-08-21 開始建置「廣告知識庫」：把 Meta/Google 官方訓練簡報、產品說明會 PDF 整理成結構化 Markdown 筆記，存在專案 `C:\AI小摸\knowledge\` 資料夾（依平台分子資料夾，目前只有 `meta-ads/`）。

**Why**：使用者長期會陸續丟更多廣告平台文件進來，希望建立可搜尋、可引用的知識庫，且要能跨電腦同步——因此選擇存在專案 git repo 裡（可 commit + push），而不是存進本機 memory（memory 不會跨電腦同步）。

**結構慣例**：
- `knowledge/README.md` 是總索引，列出所有筆記連結
- 每份筆記開頭標註「來源文件」與「時效性狀態」，因為廣告平台功能更新快（很多是 Beta/限時測試方案）
- 若同一來源 PDF 涵蓋多主題，會拆成多份筆記，並在各筆記結尾互相連結相關筆記
- 時效性用三級分類：常青（方法論不過期）／混合（框架穩定但功能細節會變）／Beta 或限時（需之後重新核對）

**互動式索引 Artifact**：發布在 https://claude.ai/code/artifact/ccfa7446-949f-4057-bfd8-b39da433ae04 （標題「廣告知識庫地圖」），用樹狀結構呈現 `knowledge/` 底下的資料夾/筆記，依來源文件分組，每篇筆記標示時效性狀態徽章。使用者反應正面（「好酷喔」），確認可保留的設計方向：
- 搜尋框即時過濾（比純長條列表好找）
- 資料夾可點擊收合/展開
- 筆記卡片點擊展開重點摘要（不用跳轉去看完整 md）
- 狀態標籤可點擊篩選

**How to apply**：之後每次在 `knowledge/` 加新筆記，記得同步更新這個 Artifact（republish 同一個 URL，不要開新的），並維持同樣的樹狀+搜尋+收合互動模式，除非使用者要求改版型。

## 相關筆記
- [[feedback_recurring_prompt_to_skill]] — 若知識庫查詢變成重複性操作，考慮做成 skill
