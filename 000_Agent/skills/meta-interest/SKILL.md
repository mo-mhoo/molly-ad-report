---
name: meta-interest
description: 搜尋 Meta 廣告可用的興趣標籤（interest targeting）。當用戶說「搜尋 Meta 興趣標籤」「查 Meta 受眾標籤」「幫我找 Meta interest」「/meta-interest」「Meta 有哪些興趣標籤」「找孕婦/媽媽/育兒等受眾標籤」時觸發。直接呼叫 Meta Graph API 查詢，回傳標籤名稱與受眾規模，不需要瀏覽器或外部工具。
---

# Meta 興趣標籤查詢

## 你的任務

用戶想知道 Meta 廣告後台有哪些可用的興趣標籤（interest targeting），以便在廣告組合中設定受眾。

## 執行步驟

### 1. 取得關鍵字

從用戶訊息中擷取要搜尋的關鍵字。若未提供，詢問：「請問要搜尋什麼關鍵字？」

### 2. 呼叫 Meta Graph API

用 Bash 工具執行以下 Python 程式碼（直接執行，不要存成檔案）：

```python
import os, requests

TOKEN = os.getenv("META_ACCESS_TOKEN", "")
if not TOKEN:
    print("❌ 找不到 META_ACCESS_TOKEN，請在 .env 或環境變數中設定")
    exit(1)

keywords = __KEYWORDS__  # 替換為實際關鍵字清單，例如 ["pregnancy", "new mom"]

for kw in keywords:
    resp = requests.get(
        "https://graph.facebook.com/v21.0/search",
        params={
            "type": "adinterest",
            "q": kw,
            "limit": 10,
            "locale": "zh_TW",
            "access_token": TOKEN,
        },
        timeout=10,
    )
    data = resp.json().get("data", [])
    print(f"\n=== {kw} ===")
    for item in data:
        name = item.get("name", "")
        low = item.get("audience_size_lower_bound", 0)
        high = item.get("audience_size_upper_bound", 0)
        size = f"{low/1_000_000:.1f}M ~ {high/1_000_000:.1f}M" if low >= 1_000_000 else f"{low:,} ~ {high:,}" if low else "N/A"
        print(f"  {name:<40} 受眾：{size}")
```

**注意：**
- 英文關鍵字通常比中文結果更多，建議中英文各搜一次
- `META_ACCESS_TOKEN` 從環境變數讀取，不要在對話中顯示 Token 值

### 3. 整理輸出

用表格格式呈現結果：

| 標籤名稱 | 受眾規模 | 備註 |
|---|---|---|
| ... | ... | ... |

若結果中有明顯不相關的標籤（例如搜尋「孕婦」卻出現音樂類別），略過不顯示。

### 4. 補充建議

結果顯示後，根據標籤類型補充：
- 受眾規模太小（< 100萬）：建議組合多個標籤擴大規模
- 找不到精確標籤：建議改用「生命事件 → 即將成為父母」或 Lookalike 受眾

## 常用搜尋詞對照

| 目標受眾 | 建議搜尋詞 |
|---|---|
| 孕婦 | pregnancy, expecting parents, baby shower |
| 新手媽媽 | new mom, parenting, new parents |
| 已婚婦女 | married women, wife |
| 育兒家長 | parenting, childcare, baby products |
| 母嬰商品 | maternity, baby shower, Mothercare |
