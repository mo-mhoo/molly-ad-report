# Meta Access Token 換新流程

> 每 60 天需更新一次，到期前 app 會顯示黃色警告橫幅提醒。

## 需要的資訊

- **App ID**：`1535543074833417`（tsa000(M) 應用程式編號）
- **App Secret**：Meta for Developers → tsa000(M) → Settings → Basic → App Secret → 點「顯示」複製
- **短期 token**：每次從 Graph API Explorer 重新產生

---

## Step 1：產生短期 token

1. 前往 https://developers.facebook.com/tools/explorer/
2. 右側選 App：**tsa000(M)**，用戶或粉絲專頁：**用戶權杖**
3. 權限勾選：`ads_management`、`ads_read`
4. 按 **Generate Access Token**，授權後複製 token

---

## Step 2：換成 60 天 long-lived token（PowerShell）

```powershell
$short_token = "EAA...短期token完整貼這裡"
$secret = "你的App Secret"
$result = Invoke-RestMethod "https://graph.facebook.com/oauth/access_token?grant_type=fb_exchange_token&client_id=1535543074833417&client_secret=$secret&fb_exchange_token=$short_token"
$result.access_token | Out-File "token.txt"
```

- 四行一起貼到 PowerShell 執行
- 開 `token.txt` 複製裡面的完整 token

---

## Step 3：更新 Streamlit Cloud Secrets

1. Streamlit Cloud → app → **Settings → Secrets**
2. 找到 `meta_token = "..."` 那行
3. 把引號內換成新 token（**一定要在同一行，不能換行**）
4. 存檔，app 自動重啟

---

## 常見錯誤

| 錯誤訊息 | 原因 | 解法 |
|---|---|---|
| `Invalid Client ID` | client_id 填錯 | 用 App ID `1535543074833417`，不是 Secret |
| `Error validating client secret` | App Secret 貼錯 | 重新從 Settings → Basic 複製 |
| `could not be decrypted` | token 被截斷或含特殊字元 | 用 `$short_token` 變數方式傳入 |
| app 顯示 API 錯誤 | Secrets 裡 token 換行 | 確認 `meta_token = "EAA..."` 在同一行 |
