---
name: kaohsiung-events-weekly-update
description: 每週一早上 9 點更新「高雄 2026 整年度活動雷達」Artifact 的場次資料
---

你要更新使用者的一個私人 Artifact 網頁：「高雄 2026 整年度活動雷達」，網址是
https://claude.ai/code/artifact/95db93a3-3d94-4a84-9be2-e7fc9af05187

## 背景
使用者下班常常要從高雄市區南下，只要高雄巨蛋、世運主場館（國家體育場）等大場館有演唱會/大型活動就會塞車，所以做了這個頁面追蹤全年場次，尤其在意週五、六、日。頁面設計是「橫向月份卡片牆」：從當月排到12月，每個月一欄，卡片顯示日期、場館、活動名；週五六日的卡片用橘色底標出；已經結束的場次會被頁面上的 JS 用「今天」日期自動判斷並收進頁面下方一個預設收合的「已結束場次」區塊——**這個過去/未來的判斷是頁面載入時用 JavaScript 的 new Date() 自動算的，不需要你手動搬動資料**，你只需要維護 events 陣列本身的內容（新增場次、把 TBD 補成確定日期），不用管哪些該顯示成已結束。

**另外使用者特別要求標示韓星場次**：卡司含韓國藝人的場次會有一個藍色「🇰🇷 韓星」標籤（用 `kr: true` 欄位控制）。使用者明確說過「不要幻覺」——這個標記務必只根據查證到的公開卡司資訊認定，確定卡司包含韓籍藝人／韓國團體才標 `kr: true`；不確定、卡司未公布、或你不確定藝人國籍時，寧可不標也不要用猜的。混合卡司的音樂節（例如高雄啤酒節）如果主要/標題卡司是韓籍藝人可以標，但如果只是眾多台灣藝人中夾雜一位不確定的名字就不要標。

## 你要做的事
1. 搜尋以下場館 2026 年（如果現在已經接近年底，也搜尋 2027 年初）**新公布**的大型活動／演唱會，逐一確認：
   - 高雄巨蛋
   - 世運主場館（高雄國家體育場）
   - 高雄流行音樂中心（海音館）
   - 夢時代（含高雄啤酒節、跨年晚會等）
   - 高雄展覽館（大型商展）
   上次同步時常常漏掉「高雄流行音樂中心」的場次，這次也要單獨搜一次這個場館名稱，不要只搜巨蛋跟世運。也要留意有些演出是連續多天（例如三天），第一輪搜尋常常只抓到其中一天，要交叉確認完整日期範圍。

2. 特別確認這兩個目前列為「日期未定」的項目是否已公布正式日期：
   - AAA 亞洲明星盛典頒獎典禮（世運主場館，傳聞落在年底，kr: true）
   - 游鴻明 高雄巨蛋演唱會（確定8月開唱，日期未公布，非韓星）
   - 高雄跨年晚會（12/31，卡司未公布——若公布卡司，確認是否有韓籍藝人再決定要不要補上 kr: true）
   若已公布日期，把它從 tbdItems 陣列移到 events 陣列（給正確日期），並移除 tbd 標記。

3. 找到新場次就加進 `events` 陣列（照現有格式：date, 可選 end 或 multi, name, sub, venue, type, 以及查證後決定要不要加 kr: true）；找到新的未定日期項目就加進 `tbdItems`（month 用 0-index，完全不知道月份用 null，一樣要查證卡司再決定 kr）。不要刪除任何已經存在的場次資料（即使已經過去也保留，過去場次是使用者刻意想保留對照用的），也不要移除或亂改別人已經標好的 kr 欄位，除非你查到證據顯示原本標錯了。

4. 更新 header 裡「資料整理於 2026-07-22」那行的日期文字，改成你實際跑這個任務的日期。

5. 保持整體 HTML／CSS／JS 結構、配色（deep navy header + 橘色 accent、看板風格、藍色 🇰🇷 韓星標籤）、版面（橫向月曆卡片牆 + 收合的已結束區塊）完全不變，只動資料內容跟資料日期戳記。

## 技術做法
下面是目前這個 Artifact 的完整原始碼（上一次同步的版本）。請把它寫到你自己這次執行環境的一個暫存檔案（例如你的 scratchpad 目錄下建立 kaohsiung_weekend_events.html），依照上面 1–4 點更新內容後，用 Artifact 工具發布，並且**務必帶上 url 參數指向上面那個既有網址**（因為每次排程執行都是全新 session，不帶 url 的話會產生一個新網址，使用者收藏的連結就失效了）。favicon 用 🚦，description 可以寫「高雄各大場館未來場次橫向月曆快覽，韓星場次特別標記，方便下班南下前快速避開塞車。」。

現有原始碼：

```html
<title>高雄週末活動雷達</title>
<style>
  :root {
    --bg: #F2F3F0;
    --surface: #FFFFFF;
    --board: #1A2130;
    --board-line: #333C4E;
    --ink: #1A2130;
    --ink-soft: #5B6472;
    --ink-faint: #8992A1;
    --accent: #D9691F;
    --accent-ink: #7A3C0F;
    --accent-soft: #FBE7D4;
    --line: #DEE2E1;
    --chip-neutral: #E7E9E5;
    --chip-neutral-ink: #5B6472;
    --kr: #2C6E8C;
    --kr-soft: #E1EDF3;
    --radius: 3px;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #10141C;
      --surface: #1A2130;
      --board: #0C0F16;
      --board-line: #2B3345;
      --ink: #E9EBEF;
      --ink-soft: #9AA3B4;
      --ink-faint: #6B7484;
      --accent: #F0954C;
      --accent-ink: #FBD9B6;
      --accent-soft: #3A2A18;
      --line: #262D3B;
      --chip-neutral: #232B3A;
      --chip-neutral-ink: #9AA3B4;
      --kr: #6FB8DA;
      --kr-soft: #16303B;
    }
  }
  :root[data-theme="dark"] {
    --bg: #10141C;
    --surface: #1A2130;
    --board: #0C0F16;
    --board-line: #2B3345;
    --ink: #E9EBEF;
    --ink-soft: #9AA3B4;
    --ink-faint: #6B7484;
    --accent: #F0954C;
    --accent-ink: #FBD9B6;
    --accent-soft: #3A2A18;
    --line: #262D3B;
    --chip-neutral: #232B3A;
    --chip-neutral-ink: #9AA3B4;
    --kr: #6FB8DA;
    --kr-soft: #16303B;
  }
  :root[data-theme="light"] {
    --bg: #F2F3F0;
    --surface: #FFFFFF;
    --board: #1A2130;
    --board-line: #333C4E;
    --ink: #1A2130;
    --ink-soft: #5B6472;
    --ink-faint: #8992A1;
    --accent: #D9691F;
    --accent-ink: #7A3C0F;
    --accent-soft: #FBE7D4;
    --line: #DEE2E1;
    --chip-neutral: #E7E9E5;
    --chip-neutral-ink: #5B6472;
    --kr: #2C6E8C;
    --kr-soft: #E1EDF3;
  }

  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: "PingFang TC", "Hiragino Sans TC", "Microsoft JhengHei", "Noto Sans TC", system-ui, sans-serif;
    -webkit-font-smoothing: antialiased;
  }
  .wrap {
    max-width: 720px;
    margin: 0 auto;
    padding: 0 0 4rem;
  }

  /* ---- Header board ---- */
  header {
    background: var(--board);
    color: #EDEFF3;
    padding: 1.5rem 1.25rem 1.35rem;
    border-bottom: 3px solid var(--accent);
  }
  .eyebrow {
    font-size: 0.72rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--accent);
    font-weight: 700;
    margin: 0 0 0.4rem;
  }
  h1 {
    font-size: 1.5rem;
    line-height: 1.3;
    margin: 0 0 0.55rem;
    font-weight: 800;
    text-wrap: balance;
    letter-spacing: 0.01em;
  }
  .subline {
    font-size: 0.85rem;
    color: #AEB4C2;
    margin: 0;
    line-height: 1.6;
  }
  .updated {
    margin-top: 0.9rem;
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.72rem;
    color: #8890A0;
    font-variant-numeric: tabular-nums;
  }
  .updated .dot {
    width: 5px; height: 5px; border-radius: 50%;
    background: #4CAF7D;
    flex: none;
  }

  /* ---- Hero / next alert ---- */
  .hero {
    margin: 1.1rem 1.25rem 0;
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    overflow: hidden;
  }
  .hero-top {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    padding: 0.85rem 1rem 0.7rem;
    border-bottom: 1px dashed var(--line);
  }
  .hero-label {
    font-size: 0.72rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-weight: 700;
    color: var(--accent-ink);
    background: var(--accent-soft);
    padding: 0.2rem 0.5rem;
    border-radius: 2px;
  }
  .hero-countdown {
    font-family: "SF Mono", "Roboto Mono", Consolas, monospace;
    font-weight: 700;
    font-size: 0.95rem;
    color: var(--accent);
    font-variant-numeric: tabular-nums;
  }
  .hero-body {
    padding: 0.9rem 1rem 1.05rem;
  }
  .hero-name {
    font-size: 1.15rem;
    font-weight: 800;
    margin: 0 0 0.3rem;
    text-wrap: balance;
  }
  .hero-meta {
    font-size: 0.83rem;
    color: var(--ink-soft);
    margin: 0;
    line-height: 1.55;
  }

  /* ---- Section heading ---- */
  .board { margin: 1.5rem 0 0; }
  .board-heading {
    font-size: 0.72rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    font-weight: 700;
    color: var(--ink-faint);
    margin: 0 1.25rem 0.4rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .board-heading::after {
    content: "";
    flex: 1;
    height: 1px;
    background: var(--line);
  }
  .scroll-hint {
    font-size: 0.68rem;
    color: var(--ink-faint);
    text-align: right;
    margin: 0 1.25rem 0.55rem;
  }

  /* ---- Month grid (primary view) ---- */
  .month-grid-wrap {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    scroll-snap-type: x proximity;
    padding: 0 1.25rem 0.3rem;
  }
  .month-grid {
    display: flex;
    gap: 0.6rem;
    width: max-content;
  }
  .month-col {
    flex: 0 0 158px;
    display: flex;
    flex-direction: column;
    scroll-snap-align: start;
  }
  .month-col-header {
    background: var(--board);
    color: #EDEFF3;
    font-size: 0.85rem;
    font-weight: 800;
    text-align: center;
    padding: 0.45rem 0.3rem;
    border-radius: 4px 4px 0 0;
    font-variant-numeric: tabular-nums;
  }
  .month-col-header.current { box-shadow: inset 0 -3px 0 var(--accent); }
  .month-col-header .badge {
    display: block;
    font-size: 0.6rem;
    font-weight: 700;
    color: var(--accent);
    margin-top: 0.15rem;
    letter-spacing: 0.05em;
  }
  .month-col-body {
    flex: 1;
    background: var(--surface);
    border: 1px solid var(--line);
    border-top: none;
    border-radius: 0 0 4px 4px;
    padding: 0.5rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    min-height: 68px;
  }
  .month-empty {
    font-size: 0.7rem;
    color: var(--ink-faint);
    text-align: center;
    padding: 0.4rem 0;
  }

  .event-card {
    border: 1px solid var(--line);
    border-radius: 3px;
    padding: 0.45rem 0.5rem 0.5rem;
    background: var(--bg);
  }
  .event-card.weekend { border-color: var(--accent); background: var(--accent-soft); }
  .event-card.tbd { border-style: dashed; }
  .event-card .ec-date {
    font-family: "SF Mono", "Roboto Mono", Consolas, monospace;
    font-size: 0.66rem;
    font-weight: 700;
    color: var(--ink-soft);
    font-variant-numeric: tabular-nums;
  }
  .event-card.weekend .ec-date { color: var(--accent-ink); }
  .event-card .ec-name {
    font-size: 0.79rem;
    font-weight: 700;
    margin: 0.15rem 0 0.15rem;
    line-height: 1.32;
    text-wrap: balance;
  }
  .event-card .ec-venue { font-size: 0.67rem; color: var(--ink-faint); }
  .event-card .ec-tbd {
    display: inline-block;
    margin-top: 0.3rem;
    font-size: 0.6rem;
    font-weight: 700;
    color: var(--ink-faint);
    background: var(--chip-neutral);
    padding: 0.05rem 0.35rem;
    border-radius: 2px;
  }
  .event-card .ec-kr {
    display: inline-block;
    margin-top: 0.3rem;
    margin-right: 0.3rem;
    font-size: 0.6rem;
    font-weight: 700;
    color: var(--kr);
    background: var(--kr-soft);
    padding: 0.05rem 0.35rem;
    border-radius: 2px;
  }

  /* ---- Past events (collapsed) ---- */
  .past-details { margin: 1.7rem 1.25rem 0; border-top: 1px solid var(--line); }
  .past-details summary {
    cursor: pointer;
    font-size: 0.78rem;
    font-weight: 700;
    color: var(--ink-soft);
    padding: 0.8rem 0 0.6rem;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }
  .past-details summary::-webkit-details-marker { display: none; }
  .past-details summary::before {
    content: "▸";
    font-size: 0.65rem;
    color: var(--ink-faint);
    display: inline-block;
    transition: transform 0.15s ease;
  }
  .past-details[open] summary::before { transform: rotate(90deg); }

  .month-divider {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.7rem 0 0.4rem;
  }
  .month-divider:first-child { padding-top: 0; }
  .month-divider span {
    font-size: 0.72rem;
    font-weight: 800;
    color: var(--ink-soft);
    font-variant-numeric: tabular-nums;
  }
  .month-divider::after { content: ""; flex: 1; height: 1px; background: var(--line); }

  .row {
    display: grid;
    grid-template-columns: 52px 1fr;
    gap: 0.85rem;
    padding: 0.65rem 0;
    border-bottom: 1px solid var(--line);
    opacity: 0.55;
  }
  .row:last-child { border-bottom: none; }
  .row .name { text-decoration: line-through; text-decoration-color: var(--ink-faint); text-decoration-thickness: 1px; }

  .datechip {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background: var(--chip-neutral);
    color: var(--ink-faint);
    border-radius: 4px;
    padding: 0.3rem 0 0.25rem;
    font-family: "SF Mono", "Roboto Mono", Consolas, monospace;
  }
  .datechip .num { font-size: 0.95rem; font-weight: 700; line-height: 1.1; font-variant-numeric: tabular-nums; }
  .datechip .wd { font-size: 0.64rem; font-weight: 700; margin-top: 0.1rem; }

  .rowbody { min-width: 0; }
  .rowbody .top { display: flex; align-items: center; gap: 0.4rem; flex-wrap: wrap; }
  .rowbody .name { font-size: 0.9rem; font-weight: 700; margin: 0; }
  .tag {
    font-size: 0.63rem;
    font-weight: 700;
    padding: 0.06rem 0.35rem;
    border-radius: 2px;
    background: var(--chip-neutral);
    color: var(--ink-soft);
    white-space: nowrap;
  }
  .rowbody .venue { font-size: 0.74rem; color: var(--ink-faint); margin: 0.2rem 0 0; }

  /* ---- Footnote ---- */
  .foot {
    margin: 1.6rem 1.25rem 0;
    padding-top: 1rem;
    border-top: 1px solid var(--line);
    font-size: 0.76rem;
    color: var(--ink-faint);
    line-height: 1.7;
  }
  .foot p { margin: 0 0 0.6rem; }

  @media (prefers-reduced-motion: no-preference) {
    .hero { transition: background-color 0.2s ease; }
  }
</style>

<div class="wrap">
  <header>
    <p class="eyebrow">Kaohsiung 2026 Events Radar</p>
    <h1>高雄 2026 整年度活動雷達</h1>
    <p class="subline">下班南下前先看一眼 — 高雄巨蛋、世運主場館、高流、夢時代等大場館未來場次，橘色底 = 週五六日，🇰🇷 韓星 = 卡司含韓國藝人。</p>
    <div class="updated"><span class="dot"></span>資料整理於 2026-07-22 · 每週更新一次</div>
  </header>

  <div class="hero" id="hero">
    <div class="hero-top">
      <span class="hero-label">最近警報</span>
      <span class="hero-countdown" id="hero-countdown">—</span>
    </div>
    <div class="hero-body">
      <p class="hero-name" id="hero-name">載入中…</p>
      <p class="hero-meta" id="hero-meta"></p>
    </div>
  </div>

  <div class="board">
    <p class="board-heading">未來場次快覽</p>
    <p class="scroll-hint">← 左右滑動看更多月份 →</p>
    <div class="month-grid-wrap">
      <div class="month-grid" id="grid"></div>
    </div>
  </div>

  <details class="past-details">
    <summary>已結束場次（<span id="past-count">0</span> 場，僅供對照）</summary>
    <div id="past-list"></div>
  </details>

  <div class="foot">
    <p>場館位置：<strong>高雄巨蛋</strong>鄰近三多商圈／博愛路一帶；<strong>世運主場館</strong>鄰近左營高鐵站／翠華路、國道一號左營端；<strong>高雄流行音樂中心</strong>在苓雅區真愛碼頭／中山路一帶；<strong>夢時代</strong>周邊活動日輕軌／道路易管制。活動日這些路段容易回堵。</p>
    <p>🇰🇷 韓星標記依公開卡司資訊認定，只標示已公布卡司、確定有韓籍藝人演出的場次；卡司尚未公布的（如跨年晚會）先不標，等公布後再確認補上，避免亂猜。</p>
    <p>資料來源：各活動官方售票頁與新聞整理，實際場次以官方公告為準，之後每週會視最新公布再更新這頁。</p>
  </div>
</div>

<script>
(function () {
  var events = [
    { date: '2026-01-31', name: 'Calum Scott', sub: '個人演唱會', venue: '高雄巨蛋', type: '演唱會' },
    { date: '2026-04-25', multi: ['2026-04-25', '2026-04-26'], name: 'SEVENTEEN CxM', sub: '小分隊世巡高雄場', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-05-09', multi: ['2026-05-09', '2026-05-10'], name: 'EXO', sub: '世界巡演高雄場', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-05-30', name: 'K-SPARK IN KAOHSIUNG', sub: 'G-Dragon、太妍、FTIsland、DPR Ian、KiiKiii 拼盤', venue: '世運主場館', type: '演唱會', kr: true },
    { date: '2026-06-27', multi: ['2026-06-27', '2026-06-28'], name: 'ITZY', sub: '世界巡演高雄場', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-07-03', end: '2026-07-05', name: '高雄啤酒音樂節', sub: '周子瑜、Rain、BTOB、RIIZE、藝聲等韓星登台', venue: '夢時代', type: '音樂節', kr: true },
    { date: '2026-07-04', multi: ['2026-07-04', '2026-07-05'], name: 'MAMAMOO《4WARD》', sub: '世界巡演高雄場', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-07-11', name: 'NMIXX', sub: '演唱會', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-07-18', multi: ['2026-07-18', '2026-07-19'], name: 'EXO', sub: '追加場次', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-07-24', multi: ['2026-07-24', '2026-07-25', '2026-07-26'], name: 'DxS《SERENADE ON STAGE》', sub: 'SEVENTEEN 小分隊，連續三天', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-07-25', name: 'DONGHAE 東海《ALIVE》', sub: 'Super Junior 東海個人巡演加場', venue: '高雄流行音樂中心', type: '演唱會', kr: true },
    { date: '2026-08-08', name: 'K-WAVE INFINITY', sub: 'HIGHLIGHT、Apink、CRAVITY、NEWBEAT、FLARE U', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-09-11', end: '2026-09-14', name: '高雄國際建材大展', sub: '商用展覽，連假期間人潮＋車潮多', venue: '高雄展覽館', type: '展覽' },
    { date: '2026-09-19', name: 'Post Malone《BIG ASS Stadium World Tour》', sub: '特別嘉賓 Don Toliver', venue: '世運主場館', type: '演唱會' },
    { date: '2026-09-26', name: 'TREASURE', sub: '《THE STAGE 2026 NEW WAV》世界巡演高雄場', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-10-24', name: 'TWS', sub: '《24/7:FOR:YOU》巡演高雄場', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-11-19', multi: ['2026-11-19', '2026-11-21', '2026-11-22'], name: 'BTS《ARIRANG》', sub: '連續三場世界巡演', venue: '世運主場館', type: '演唱會', kr: true },
    { date: '2026-12-12', name: '滅火器《ON FIRE DAY》', sub: '樂團專場', venue: '高雄巨蛋', type: '演唱會' },
    { date: '2026-12-19', name: 'T.O.P', sub: '個人演唱會', venue: '高雄巨蛋', type: '演唱會', kr: true },
    { date: '2026-12-31', name: '高雄跨年晚會', sub: '卡司與煙火場次待官方公布', venue: '夢時代／義大世界', type: '跨年活動', tbdNote: true }
  ];

  // month is 0-indexed (0 = Jan); used for undated items only, null = month unknown
  var tbdItems = [
    { name: 'AAA 亞洲明星盛典頒獎典禮', venue: '世運主場館', note: '傳聞落在年底，官方尚未公布正式演出日', month: null, kr: true },
    { name: '游鴻明 高雄巨蛋演唱會', venue: '高雄巨蛋', note: '確定 8 月開唱，確切日期未公布', month: 7 }
  ];

  var monthNames = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'];
  var wd = ['日', '一', '二', '三', '四', '五', '六'];
  function isWeekend(d) { var n = d.getDay(); return n === 0 || n === 5 || n === 6; }
  function parseLocal(s) { var p = s.split('-'); return new Date(+p[0], +p[1] - 1, +p[2]); }
  function fmtMD(d) { return (d.getMonth() + 1) + '/' + d.getDate(); }

  var today = new Date();
  today.setHours(0, 0, 0, 0);
  var todayMonth = today.getMonth();
  var todayYear = today.getFullYear();

  // classify events
  var future = [];
  var past = [];
  events.forEach(function (ev) {
    var primary = parseLocal(ev.date);
    var effectiveEnd = ev.end ? parseLocal(ev.end) : (ev.multi ? parseLocal(ev.multi[ev.multi.length - 1]) : primary);
    var isPast = effectiveEnd < today;
    var diff = Math.round((primary - today) / 86400000);
    var rec = { ev: ev, primary: primary, diff: diff, isPast: isPast };
    (isPast ? past : future).push(rec);
  });

  // ---- Hero: nearest upcoming ----
  var nearest = null;
  future.forEach(function (r) {
    if (r.diff >= 0 && (nearest === null || r.diff < nearest.diff)) {
      nearest = { diff: r.diff, name: r.ev.name, sub: r.ev.sub, venue: r.ev.venue, date: r.primary };
    }
  });
  var heroName = document.getElementById('hero-name');
  var heroMeta = document.getElementById('hero-meta');
  var heroCountdown = document.getElementById('hero-countdown');
  if (nearest) {
    heroName.textContent = nearest.name + '　·　' + nearest.venue;
    heroMeta.textContent = nearest.sub + '　（' + (nearest.date.getMonth() + 1) + '/' + nearest.date.getDate() + ' 週' + wd[nearest.date.getDay()] + '）';
    heroCountdown.textContent = nearest.diff === 0 ? '就是今天' : nearest.diff + ' 天後';
  } else {
    heroName.textContent = '目前追蹤清單內沒有即將到來的場次';
    heroMeta.textContent = '會在每週更新時補上新公布的場次';
    heroCountdown.textContent = '';
  }

  // ---- Month grid: current month through December ----
  var gridEl = document.getElementById('grid');
  for (var m = todayMonth; m <= 11; m++) {
    var col = document.createElement('div');
    col.className = 'month-col';

    var header = document.createElement('div');
    header.className = 'month-col-header' + (m === todayMonth ? ' current' : '');
    header.innerHTML = todayYear + ' ' + monthNames[m] + (m === todayMonth ? '<span class="badge">本月</span>' : '');
    col.appendChild(header);

    var body = document.createElement('div');
    body.className = 'month-col-body';

    var monthFuture = future.filter(function (r) { return r.primary.getMonth() === m; });
    monthFuture.sort(function (a, b) { return a.primary - b.primary; });
    monthFuture.forEach(function (r) {
      body.appendChild(buildCard(r.ev, r.primary));
    });

    tbdItems.filter(function (t) { return t.month === m; }).forEach(function (t) {
      body.appendChild(buildTbdCard(t));
    });

    if (!body.children.length) {
      var empty = document.createElement('div');
      empty.className = 'month-empty';
      empty.textContent = '目前沒有場次';
      body.appendChild(empty);
    }

    col.appendChild(body);
    gridEl.appendChild(col);
  }

  // trailing "date unknown" column
  var undated = tbdItems.filter(function (t) { return t.month === null; });
  if (undated.length) {
    var ucol = document.createElement('div');
    ucol.className = 'month-col';
    var uheader = document.createElement('div');
    uheader.className = 'month-col-header';
    uheader.textContent = '日期未定';
    ucol.appendChild(uheader);
    var ubody = document.createElement('div');
    ubody.className = 'month-col-body';
    undated.forEach(function (t) { ubody.appendChild(buildTbdCard(t)); });
    ucol.appendChild(ubody);
    gridEl.appendChild(ucol);
  }

  function buildCard(ev, primary) {
    var card = document.createElement('div');
    card.className = 'event-card' + (isWeekend(primary) ? ' weekend' : '');
    var dateStr;
    if (ev.multi) {
      dateStr = ev.multi.map(function (ds) { var d = parseLocal(ds); return fmtMD(d) + '(' + wd[d.getDay()] + ')'; }).join(' ');
    } else if (ev.end) {
      dateStr = fmtMD(primary) + '–' + fmtMD(parseLocal(ev.end));
    } else {
      dateStr = fmtMD(primary) + '(' + wd[primary.getDay()] + ')';
    }
    var html = '<div class="ec-date">' + dateStr + '</div>';
    html += '<div class="ec-name">' + ev.name + '</div>';
    html += '<div class="ec-venue">📍 ' + ev.venue + '</div>';
    if (ev.kr) html += '<span class="ec-kr">🇰🇷 韓星</span>';
    if (ev.tbdNote) html += '<span class="ec-tbd">卡司未公布</span>';
    card.innerHTML = html;
    return card;
  }

  function buildTbdCard(t) {
    var card = document.createElement('div');
    card.className = 'event-card tbd';
    var html = '<div class="ec-name">' + t.name + '</div>';
    html += '<div class="ec-venue">📍 ' + t.venue + '</div>';
    if (t.kr) html += '<span class="ec-kr">🇰🇷 韓星</span>';
    html += '<span class="ec-tbd">' + t.note + '</span>';
    card.innerHTML = html;
    return card;
  }

  // ---- Past events (collapsed) ----
  var pastListEl = document.getElementById('past-list');
  document.getElementById('past-count').textContent = past.length;
  var lastMonth = null;
  past.sort(function (a, b) { return a.primary - b.primary; });
  past.forEach(function (r) {
    var ev = r.ev, primary = r.primary;
    var monthKey = primary.getFullYear() + '-' + primary.getMonth();
    if (monthKey !== lastMonth) {
      var divider = document.createElement('div');
      divider.className = 'month-divider';
      divider.innerHTML = '<span>' + primary.getFullYear() + ' 年 ' + monthNames[primary.getMonth()] + '</span>';
      pastListEl.appendChild(divider);
      lastMonth = monthKey;
    }

    var row = document.createElement('div');
    row.className = 'row';

    var chip = document.createElement('div');
    chip.className = 'datechip';
    if (ev.end) {
      chip.innerHTML = '<span class="num">' + (primary.getMonth() + 1) + '.' + primary.getDate() + '</span><span class="wd">起</span>';
    } else {
      chip.innerHTML = '<span class="num">' + primary.getDate() + '</span><span class="wd">週' + wd[primary.getDay()] + '</span>';
    }

    var body2 = document.createElement('div');
    body2.className = 'rowbody';
    var top = document.createElement('div');
    top.className = 'top';
    var name = document.createElement('p');
    name.className = 'name';
    name.textContent = ev.name;
    var tag = document.createElement('span');
    tag.className = 'tag';
    tag.textContent = ev.type;
    top.appendChild(name);
    top.appendChild(tag);
    if (ev.kr) {
      var krTag = document.createElement('span');
      krTag.className = 'tag';
      krTag.textContent = '🇰🇷 韓星';
      top.appendChild(krTag);
    }
    var venue = document.createElement('p');
    venue.className = 'venue';
    venue.textContent = '📍 ' + ev.venue;
    body2.appendChild(top);
    body2.appendChild(venue);

    row.appendChild(chip);
    row.appendChild(body2);
    pastListEl.appendChild(row);
  });
})();
</script>
```

## 完成後
不用跟使用者長篇報告，執行完就結束即可（使用者會收到排程完成通知）。如果這次完全沒有找到任何新資訊（沒有新場次、TBD 也還沒公布），還是要更新「資料整理於」那個日期戳記再重新發布，讓使用者知道這週有確認過。