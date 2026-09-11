---
name: feedback-meta-ads-write-safety
description: Meta Marketing API 串接（首盛等專案）寫入類操作一律先列出來給用戶確認，不自動刪除
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 0ec05879-055b-42c8-a13e-e97362d473c9
  modified: 2026-07-31T01:13:37.967Z
---

串接 Meta Marketing API 時（新增廣告活動、調預算、刪除等），任何會影響真實花費或刪除既有物件的動作，一律先把要做的內容列出來給用戶看，等明確確認才送出 API 呼叫。不主動呼叫任何刪除（campaign/adset/ad/預算排程）的 API，除非用戶在對話中明確要求且確認過。

**Why:** 首盛專案（2026-07-27）設定 System User token 時，用戶主動要求「確保資安風險、不要誤刪資料」，明確表態擔心 `business_management` 這種較廣的權限範圍。用戶最後選擇不勾 `business_management`，只用 `ads_read` + `ads_management`，因為讀報表、建廣告/調預算都走 `act_<帳號ID>` 層級的 API，不需要商家資產管理層級的權限。

**How to apply:** 
- Token 只存 `.env`，不寫進程式碼、不推上 git（同 [[feedback-security]]）。
- 生成 System User token 時預設只勾 `ads_read` + `ads_management`，不主動建議勾 `business_management`，除非有具體功能非用不可才回來加。
- 任何寫入/刪除類 API 呼叫都要先跟用戶確認內容再執行，這條規則適用於首盛、molly-ad-report 等所有涉及真實廣告帳號寫入的專案。
- **範圍不限於改廣告本身**：推播到 Google Chat 這類「送到團隊看得到的地方」的動作也算，同樣要先讓用戶看過文字結果、確認要送才推播（2026-07-30，`meta-creative-check` skill：腳本預設只印文字不推播，加 `--send` 旗標才會送，且 SKILL.md 明講不要自己直接一次到位加 `--send`）。之後任何 skill 只要會把資料送到用戶以外的人看得到的地方（Chat、Email、共用文件等），都比照這個「先文字、後確認、再推播」的兩段式流程。
