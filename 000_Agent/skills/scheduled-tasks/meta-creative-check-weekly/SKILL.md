---
name: meta-creative-check-weekly
description: 每週一早上9點巡檢 Meta 廣告素材成效(依 ATL/BTL 分開判斷)，輸出建議暫停/觀察清單
---

執行 meta-creative-check skill(用 Skill 工具呼叫 `meta-creative-check`)，對預設範圍（`daily_meta_report.py` 的 `ACCOUNTS`：御熹堂、毛孩時代，含各自 CPAS 通路帳號）跑一次素材成效巡檢。

實際做法是跑 `cd "C:\AI小摸" && python check_creative_fatigue.py`（不加帳號參數＝查全部），該腳本已經正確依 ad_id 分開評估 ATL/BTL(不會把同一素材名稱跨 ATL/BTL 的花費混在一起算)，直接沿用它的判斷邏輯即可，不用自己重新彙總。

比較區間：昨日 vs 過去7日均值 vs 過去14日均值（腳本內建邏輯）。

輸出格式比照 meta-creative-check skill 的 SKILL.md：每個帳戶先列 BTL 轉換漏斗 DoD 快照，再列素材層 🚨/⚠️ 標記表格，正常素材只列筆數不逐支攤開，結尾一句摘要。

輸出完整結果直接留在這個 session 裡，**不要自動加 `--send` 推播到 Google Chat，也不要執行任何暫停/關閉動作**——那些都要等 Molly 自己回來看過、明確確認後才能動手（跟 google-ads-atl-review-weekly 同一個原則：只診斷不動手）。

如果查詢過程中遇到帳號/憑證問題，照 skill 裡的指示誠實列出待確認事項，不要編數字。