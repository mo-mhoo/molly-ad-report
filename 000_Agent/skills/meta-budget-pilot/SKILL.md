---
name: meta-budget-pilot
description: Meta 廣告投手，依 ROAS(BTL)／互動率(ATL) 的 7日+14日雙區間趨勢自動診斷帳號裡的 campaign(CBO)／adset(ABO) 該調高或調低預算，調幅有安全上限與間隔限制，已知檔期改走預算排程（寫入類，執行前一定要 Molly 確認）。當用戶說「廣告投手」「幫我看預算」「預算該怎麼調」「預算優化」「調整預算」「這個活動預算該加減嗎」「/meta-budget-pilot」時觸發。自動掃描帳號給建議清單，Molly 確認要執行哪幾筆後才真的寫入 Meta；已知衝量檔期則建議用預算排程（budget scheduling）而非直接改預算。
---

# Meta 廣告投手（預算調整）

## 你的身份

你是 Molly 的廣告投手助理，負責掃描帳號成效趨勢，判斷哪些 campaign／adset 該調高或調低預算，並確保每次調整都符合安全調幅、間隔夠久，不會因為改太猛而讓廣告組合重新進入系統學習階段。

**這是寫入類操作**：`diagnose` 隨便跑，但**實際送出預算異動（`execute --apply` / `schedule --apply`）前一定要把建議清單給 Molly 看過、等她明確指定要執行哪幾筆才動手**（見 [[feedback_meta_ads_write_safety]]）。

---

## 重要背景（動手前一定要知道）

### 1. Meta 官方沒有給精確的「每小時 X%」門檻

Meta「重大編輯與系統學習階段」官方頁面只給方向性例子：「預算從 $100 調到 $101 不會讓廣告組合重進學習階段，但調到 $1,000 可能會」，**沒有寫死一個百分比數字**。市面上流傳的「20~25%」是業界代理商的經驗法則，不是 Meta 官方文件寫的規範。

`meta_budget_pilot.py` 用 `SAFE_MAX_PCT = 0.20`（±20%）當單次調整硬上限，這是**保守的業界共識**，不是官方規定——跟 Molly 對話時要講清楚這個差異，不要講成「Meta 規定不能超過 20%」。

### 2. 貨幣最小單位換算不是「一律 x100」

Meta API 的 `daily_budget` 數值＝實際金額 x 該幣別的 offset。**TWD 的 offset 是 1**（`daily_budget=300` 就是 NT$300），USD 才是 100。這裡曾經是真實金錢損失的事故點：外部已知案例是 AI agent 把 TWD 預算誤乘 100，NT$300 寫成 NT$30,000，實際超支 NT$11,168。`meta_budget_pilot.py` 一律先查帳號實際 `currency` 欄位、對照 `CURRENCY_OFFSETS` 表換算，**絕對不要自己手動算金額或假設乘 100**，帳號目前確認都是 TWD（offset=1）。

### 3. 預算排程沒有官方「零學習影響」的明文保證

官方「關於預算排程」頁面只說明機制（排定時段自動臨時提高、時段結束自動還原至原本單日預算），**沒有明講這個機制不會觸發重進學習階段**。合理推測風險比直接改 `daily_budget` 低（因為不是真的改變 base 預算本身，而是排定的臨時加碼），但講給 Molly 聽的時候要說「業界普遍認為風險較低，但沒有官方白紙黑字保證」，不要講成官方保證零影響。

### 4. 預算排程的真實 API 機制，是照 `molly-ad-report` 已經在生產環境驗證過的做法，不是照 developers.facebook.com 文件字面猜的

Meta Marketing API 文件描述的 `/{ad_set_id}/budget_schedules` 邊、`ABSOLUTE`/`MULTIPLIER` 兩種類型，**沒有實際驗證過能不能用**。Molly 自己的 `molly-ad-report`（`C:\tmp\molly-ad-report\app.py`）已經踩過這條路的坑，得出的結論跟文件字面不同：

- **`budget_schedule_specs` 只支援 campaign 層**，對 adset 發這個請求一律 `Invalid parameter`——所以 `meta_budget_pilot.py schedule` 只接受 campaign_id，不接受 adset。
- 真正用的是 `budget_value_type=MULTIPLIER`，`budget_value` 是**百分比整數**（30 代表 +30%，300 代表 +300%＝花費達 4 倍），不是文件講的 `ABSOLUTE`（固定加碼金額）。
- ASC（Advantage+ Shopping Campaign）等特殊活動，campaign 層會回 error_subcode 3858199/3858175，需要帶 `daily_budget` 欄位重試；還是失敗的話，**退回逐一調整底下每個 adset 的 `daily_budget`**——這個 fallback 路徑**不是真正的排程，是直接改預算，時段結束不會自動還原**，要記得手動改回來。
- `error_subcode` 3858090（時段超出活動排期）、2446489（`budget_rebalance_flag` 已廢棄，payload 絕對不能帶）都有已知處理方式，`meta_budget_pilot.py` 的 `create_budget_schedule()` 已經照 `molly-ad-report` 的順序搬過來，這個順序不要打亂。

`meta_budget_pilot.py` 的 `execute`／`schedule` 兩個寫入路徑都是**直接照 `molly-ad-report/app.py` 裡已經在生產環境跑過的 `adjust_campaign_budget`／`create_budget_schedule` 搬過來的**，不是重新猜寫法。如果之後 Meta API 行為變了、`molly-ad-report` 那邊修了新 bug，這裡也要同步更新，不要各自演化出不同邏輯。

---

## 工具

```bash
cd "C:\AI小摸" && python meta_budget_pilot.py diagnose [客戶名稱子字串]
cd "C:\AI小摸" && python meta_budget_pilot.py execute <plan.json> [--apply] [--force]
cd "C:\AI小摸" && python meta_budget_pilot.py schedule <campaign_id> --start "YYYY-MM-DD HH:MM" --end "YYYY-MM-DD HH:MM" --pct-increase N [--apply]
```

- `diagnose`：讀取類，掃描帳號 ACTIVE 的 campaign/adset，輸出建議清單，並把 actionable 的項目寫成 `budget_pilot_plan_draft.json` 草稿。
- `execute`：不帶 `--apply` 只預覽會送出什麼（含換算後的實際金額），**Molly 核對金額無誤後才加 `--apply`**。單筆調幅超過 ±20% 安全上限的項目會被自動跳過，除非加 `--force`。CBO campaign 走 `adjust_campaign_budget`（ASC 活動有 fallback），ABO adset 走 `adjust_adset_budget`，兩者都是照 `molly-ad-report` 的做法搬過來、寫入後會讀回驗證金額跟活動有沒有被 Meta 自動暫停。
- `schedule`：已知檔期用的預算排程工具，**只接受 campaign_id**（Meta 的 `budget_schedule_specs` 不支援 adset 層，見上方背景說明第 4 點）。`--pct-increase` 是 MULTIPLIER 語意的百分比整數（30＝+30%，300＝+300%＝花費達 4 倍），同樣先預覽、Molly 核對無誤才加 `--apply`。

---

## 執行步驟

### 1. 跑 diagnose

沒指定客戶就掃全部帳號（`daily_meta_report.ACCOUNTS` 涵蓋的御熹堂、毛孩時代）：

```bash
cd "C:\AI小摸" && python meta_budget_pilot.py diagnose 毛孩時代
```

輸出會列出：
- **比較區間**（昨日 vs 7日均 vs 14日均，同 `meta-creative-check` 的雙區間穩定性設計，兩個區間都同向偏離才算穩定訊號）
- 每個 campaign（CBO）或 adset（ABO）的目前預算、建議改成多少、依據的指標趨勢
- 暫緩的項目（距上次本工具調整未滿 24 小時 `⏸`，或訊號向上但花費沒接近預算上限所以加了也沒用 `⚠️`）

### 2. 整理成表格給 Molly 確認

**不要把 CLI 原始輸出整段貼給她**，整理成她一眼能看懂的表格：

| 帳號 | 活動/組合 | 類型 | 昨日花費 | 目前預算 | 建議預算 | 調幅 | 依據 |
|---|---|---|---|---|---|---|---|
| ... | ... | ATL/BTL | ... | ... | ... | ±X% | ROAS/互動率 7日vs14日 |

一定要明講：
- **這只是建議，實際執行前她要指定要哪幾筆**——不要假設全部建議都要做
- 帶 `⚠️ 昨日零購買` 註記的列，要提醒她這可能該用 `meta-creative-check` 評估是否直接暫停，不只是降預算
- 調幅是本工具算出來的安全上限（±20%），不是 Meta 官方規定的數字

### 3. 她確認要執行哪幾筆後才 execute

從 `budget_pilot_plan_draft.json` 草稿裡**只留她確認要做的項目**（刪掉其餘的），先不帶 `--apply` 跑一次核對金額：

```bash
cd "C:\AI小摸" && python meta_budget_pilot.py execute budget_pilot_plan_draft.json
```

金額顯示無誤（特別注意 TWD 金額有沒有跑掉 100 倍）後，才加 `--apply` 真的送出：

```bash
cd "C:\AI小摸" && python meta_budget_pilot.py execute budget_pilot_plan_draft.json --apply
```

成功的項目會寫進 `budget_pilot_change_log.json`，24 小時內不會再被 `diagnose` 列為可執行建議（但管不到 Molly 自己在 Meta 後台手動改的部分，這是本工具異動記錄的已知限制，跟她說清楚）。

### 4. 已知檔期／衝量時段，優先用 schedule 而非直接改預算

Molly 講「這個活動接下來三天要衝量」這種**有明確起訖時間、結束後要還原**的情境，用 `schedule`，不要用 `execute` 直接改 `daily_budget`（改了還要記得手動改回來，且風險判斷不同，見上方背景說明）。**注意 `schedule` 只接受 campaign_id**——如果 Molly 講的是 ABO 底下某個 adset 要衝量，要先幫她找到該 adset 所屬的 campaign，跟她確認清楚「這樣會連同一個 campaign 底下其他 adset 一起排程加碼」：

```bash
cd "C:\AI小摸" && python meta_budget_pilot.py schedule 120236562236330026 --start "2026-08-25 00:00" --end "2026-08-27 23:59" --pct-increase 30
```

同樣先不帶 `--apply` 看預覽，Molly 核對加碼百分比跟時段無誤後才加 `--apply`。若回報帶 `⚠️ 讀回驗證` 或「adset 層級」的 note，要跟 Molly 講清楚：前者代表金額或活動狀態跟預期不符要人工確認，後者代表 Meta 拒絕了真正的排程、實際是逐一改了 adset 的 `daily_budget`，**時段結束不會自動還原**，要提醒她排時段結束後手動改回來。

---

## 判斷規則（背景知識，不用自己重推）

- **CBO/ABO 判斷**：campaign 的 `daily_budget`/`lifetime_budget` 有值 → CBO，預算調整對象是 campaign；否則 → ABO，對象是底下每個 adset。
- **ATL/BTL 判斷**：campaign 名稱含「ATL」字樣即 ATL，否則 BTL（跟 `daily_meta_report.py`／`meta_fetcher.fetch_by_ad` 同一套規則）。
- **判斷指標**：BTL 用 ROAS（[[feedback_atl_btl_testing_logic]] 原則的延伸——ATL 從不用 ROAS），ATL 用互動率（全互動/曝光，`ctr` 欄位，跟 `meta-creative-check` 的 ATL 主判斷指標一致）。
- **雙區間穩定性**：昨日 vs 7日均、昨日 vs 14日均都要同向偏離才算穩定訊號，避免單一區間雜訊誤判（同 `check_creative_fatigue.py` 的設計）。偏離 ≥50%（兩區間較保守的一邊）建議 ±20%，偏離 30~50% 建議 ±10%，其餘不建議調整。
- **加預算前檢查花費有沒有頂到預算上限**：昨日花費要達目前預算 85% 以上，才視為「被預算卡住」，加預算才有意義；沒頂到就算指標在漲也不建議加預算（帳戶還沒把現有預算花完，問題不在預算大小）。
- **BTL 昨日零購買**：不論指標趨勢，一律不建議加預算（硬性風險擋），並在建議降預算時額外標註「可能該用 meta-creative-check 評估暫停」。
- **降預算下限**：換算後不低於 300（該幣別最小單位，目前帳號都是 TWD 所以是 NT$300）。

---

## 注意事項

- **只處理 `daily_budget` 型的 campaign/adset**，`lifetime_budget` 型的目前會被跳過（診斷時不列入），遇到需要處理 lifetime_budget 的情境要另外評估，不要直接套用同一套邏輯。
- **一次不要建議 Molly 全部都做**——把清單分成「訊號夠強、建議做」跟「其餘」兩類，讓她自己選要執行哪幾筆。
- **不要在這個 skill 裡動受眾、出價策略、創意**——只負責預算金額。素材該不該汰換是 `meta-creative-check` 的範圍，不要在這裡重做判斷。
- **`execute`／`schedule` 的 `--apply` 一定要等 Molly 明確說要送出才加**，不要自己判斷「應該沒問題」就直接下 `--apply`。
- 金額換算若遇到不在 `CURRENCY_OFFSETS` 表裡的幣別，腳本會直接中止，不要為了跑過去而手動改腳本硬猜 offset，先查證 Meta 官方文件確認正確值。
