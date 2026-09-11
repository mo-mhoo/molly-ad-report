---
name: feedback-security
description: 不得讀取或顯示 .env、secrets.toml 等憑證檔案的內容
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 82398407-5cd0-47c7-a3d0-dca0f83779d2
---

絕對不要讀取 `.env`、`.streamlit/secrets.toml` 或任何憑證檔案的內容並顯示在對話中。

**Why:** 曾在確認 META_ACCESS_TOKEN 是否存在時，直接把整個 .env 明文印出，導致 API Key 洩漏在對話記錄裡。後續又在 secrets.toml 寫入明文 Token，同樣暴露在對話中。用戶需要 rotate 所有 Key，造成不必要的麻煩。

**How to apply:** 若需確認環境變數是否存在，用 `python -c "import os; print(bool(os.getenv('VAR_NAME')))"` 只印出 True/False，不印值。永遠不要 Read、cat、顯示任何含憑證的檔案內容。
