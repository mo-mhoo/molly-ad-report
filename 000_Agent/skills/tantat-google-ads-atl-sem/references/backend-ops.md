# Google Ads 後台操作限制（不讀會浪費大量時間）

2026-08-13 一整個 session 實測歸納。**這些不是偶發，是穩定重現的。**

## 一、分工原則

| 誰做 | 做什麼 |
|---|---|
| Claude | 讀報表、撈清單、算數字、貼上內容、**貼前逐字核對**、**存檔後驗證** |
| 人 | 按下最後那顆「儲存」／確認鍵；點開下拉選單裡的項目 |

**不要硬要自動點「儲存」。** 已經發生過「點偏 → 觸發左側選單 → 使用者已貼好的 63 行被清空」的倒退，白做一次。

## 二、點得動 vs 點不動

| 元件 | 自動化點擊 |
|---|---|
| 工具列按鈕（＋、搜尋、篩選圖示、展開） | ✅ 可以（用 ref 點最穩） |
| 表格內的列選取 checkbox | ✅ 可以（用 ref） |
| **下拉選單裡的項目**（編輯 → 移除／暫停、廣告群組選單、篩選器選項） | ❌ **點不動**，只會把選單關掉 |
| **面板底部的「儲存」鈕** | ❌ 座標換算與截圖對不上，容易點到別的東西 |
| 對話框（modal）裡的「儲存」 | ⚠️ 視窗在前景時，用座標點**可以**成功 |

### 已知唯一穩定的存檔手法

**真實 Tab 鍵把焦點移到「儲存」，再送真實 Enter。**

```
1. JS 把內容寫進 textarea
2. textarea.focus()
3. 送真實 Tab 鍵，每按一次用 JS 讀 document.activeElement 確認落點
4. 焦點停在「儲存」時，送真實 Enter
```

活動層級排除關鍵字面板實測：textarea → Tab ×2 → 焦點在「儲存」→ Enter，成功存入 24 筆。

⚠️ 但**不是每個面板的 Tab 順序都會經過儲存鈕**。關鍵字面板的 Tab 會跑進右側「取得關鍵字提案」，到不了儲存 → 這種就交給人按。
⚠️ JS 的 `element.click()` / `dispatchEvent(MouseEvent)` 對 Google Ads 的按鈕**完全無效**，不要試。

## 三、視窗狀態決定一切

```js
JSON.stringify({vis: document.visibilityState, w: innerWidth, h: innerHeight})
```

| 狀態 | 症狀 | 解法 |
|---|---|---|
| `visible` | 正常 | — |
| `hidden`（視窗在背景） | 截圖逾時、真實點擊送不進去；JS 仍可執行 | 請人把 Chrome 點回最前面 |
| `innerWidth = 0`（視窗最小化） | 截圖直接報 CDP 參數錯誤 | 請人還原視窗 |

**長時間操作前先請人保持視窗在前景、中間不要切走。** 這是本次 session 最大的時間殺手，卡了七次。

## 四、browser_batch 的座標時序陷阱

**batch 內的座標是以「batch 開始前」的畫面為準。**
所以「同一個 batch 內先捲動、再點擊」會點到捲動前的位置。

而跨 batch 也不保險——頁面常在兩個 batch 之間自動捲回頂端。

**結論：需要捲動才看得到的按鈕，交給人按。**

## 五、虛擬捲動：表格一次只給約 14 列

`document.body.innerText` 一次只拿得到約 14 列，即使頁尾寫著「共 887 列」。

要拿完整清單得邊捲邊累積：

```js
window.__k = {};
window.__g = function () {
  Array.from(document.querySelectorAll('[role="row"]')).forEach(function (r) {
    var s = r.innerText.replace(/\n/g, ' ');
    if (/比對/.test(s) && !/help_outline/.test(s)) window.__k[s] = 1;
  });
  return Object.keys(window.__k).length;
};
window.__g();
```

然後用**真實滑鼠捲動**（`scroll` 動作，一次 6 格），每捲一次呼叫 `window.__g()`，直到數字不再增加。
JS 直接設 `scrollTop` 通常**不會**觸發虛擬捲動的載入，不要用。

## 六、中文輸入

`computer.type` 打不進 Google Ads 的輸入框。用 JS 原生 setter：

```js
var el = document.querySelector('textarea');
el.focus();
var s = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value').set;
s.call(el, '第一行\n第二行');
el.dispatchEvent(new Event('input', { bubbles: true }));
el.dispatchEvent(new Event('change', { bubbles: true }));
```

## 七、其他坑

- **篩選器清不掉**：套上去的篩選器（尤其帳戶儲存檢視帶的）常常按 X／清除都沒反應，會一路跟著換頁。用「清除篩選器」連結，或請人手動按。
- **「Turn off ad blockers」是誤導**：這段字串恆常存在於 DOM，**不代表真的偵測到 ad blocker**。實測 Ahrefs SEO Toolbar 與 AdBlock 都關著時它照樣在。不要據此誤判。
  （但交接文件另有記載：Ahrefs SEO Toolbar **開著**時確實會拖慢後台，操作前建議停用。）
- **分頁會無預警被重建**，tabId 失效。收到「Tab no longer exists」就重新取 context，不是錯誤。
- **左側選單滑過去會展開**，蓋住表格左側，導致 x ≈ 280 附近的點擊打歪。用 ref 點可避開。
- **變更記錄的日期範圍改不動**（下拉選了沒套用），想用它驗證當天的異動常常失敗。改用「筆數前後對照」驗證比較可靠。

## 八、驗證的標準做法

不要靠「看起來成功了」。用**筆數前後對照**：

```js
document.body.innerText.match(/第\s*\d+\s*到\s*\d+\s*列[^\n]*/)[0]
// → "第 1 到 85 列 (共 85 列)"
```

貼上前先記下舊筆數，存檔後重載再數一次，差值要等於新增的數量。
關鍵字還要多驗一項：**逐列掃描比對類型**，確認沒有被系統改成別的類型。

```js
var all = Object.keys(window.__k);
JSON.stringify({
  total: all.length,
  nonPhrase: all.filter(function (s) { return !/詞組比對/.test(s); }).length
});
```
