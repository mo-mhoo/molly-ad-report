#!/usr/bin/env python3
"""推播上稿/巡檢結果到 Google Chat，用 Cards v2 卡片格式（跟其他工具推播到同一個
「廣告軍團-Meta 上傳素材」空間的視覺樣式對齊：粗體標題行 + 分隔線 + 內文）。

**同一支素材（帳號＋廣告名稱＋建立方式都相同）鋪進多個 adset 時，會聚合成同一張卡片**
——標題只出現一次，底下依序列出每個 adset 的 placement（adset/campaign/狀態/ad_id），
placement 之間用細分隔線隔開，不要讓同一支素材因為鋪了 4 個 adset 就重複出現 4 張長得
一樣的卡片（2026-09-11 Molly 看過實際推播結果後要求的）。

預設讀 .env 的 GOOGLE_CHAT_WEBHOOK_AD_UPLOAD（跟每日廣告日報用的 GOOGLE_CHAT_WEBHOOK 是不同變數）。

輸入是一個 JSON 檔（--file），內容是一個陣列，每個元素是「一支廣告在一個 adset 的
placement」：
[
  {
    "content_type": "活動",              // 或 "常態" / 巡檢類訊息可省略
    "account_name": "御熹堂_官網",
    "ad_name": "[~9/15]20260911_活動_單圖_...",
    "build_method": "post id 建立",       // 或 "整支複製" / "重用既有creative_id"
    "adset_name": "新客｜LAL高價值會員(10%)排除名單",
    "campaign_name": "轉換BTL(CPA)｜新客｜活動",
    "status": "已啟動(ACTIVE)",           // 或 "PAUSED" / "已啟動(PENDING_REVIEW)"
    "ad_id": "120253706605730735",
    "extra_note": ""                     // 選填，例如巡檢 warnings 內容
  },
  ...
]

`content_type` + `account_name` + `ad_name` + `build_method` 相同的元素，會自動合併成
同一張卡片（依輸入順序分組，同一支素材的 placement 不用特地排在一起，但建議排在一起
比較好讀）。

也支援單純文字卡片（只有 "text" 欄位的元素），用於巡檢摘要這類整批的總結訊息，不參與
聚合。

預設 dry-run：只印出將建立的卡片內容，不會真的送出。加 --send 才會實際 POST。
從不把 webhook URL 本身印到 stdout。
"""
import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
ENV_PATH = PROJECT_ROOT / ".env"

DEFAULT_WEBHOOK_VAR = "GOOGLE_CHAT_WEBHOOK_AD_UPLOAD"


def load_webhook(var_name: str) -> str:
    if os.environ.get(var_name):
        return os.environ[var_name]
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{var_name}="):
                return line.split("=", 1)[1].strip()
    return ""


def group_entries(entries: list) -> list:
    """把 text 卡片跟 ad placement 卡片分開處理；ad placement 依
    (content_type, account_name, ad_name, build_method) 分組，同組合併成一個 group。
    回傳一個 list，元素是 {"text": ...} 或 {"header": {...}, "placements": [...]}。
    """
    groups = []
    index_by_key = {}
    for entry in entries:
        if "text" in entry and len(entry) == 1:
            groups.append({"text": entry["text"]})
            continue
        key = (
            entry.get("content_type", ""),
            entry.get("account_name", ""),
            entry.get("ad_name", ""),
            entry.get("build_method", ""),
        )
        if key not in index_by_key:
            index_by_key[key] = len(groups)
            groups.append({
                "header": {
                    "content_type": entry.get("content_type", ""),
                    "account_name": entry.get("account_name", ""),
                    "ad_name": entry.get("ad_name", ""),
                    "build_method": entry.get("build_method", ""),
                },
                "placements": [],
            })
        groups[index_by_key[key]]["placements"].append(entry)
    return groups


def header_lines(header: dict, bold: bool) -> list:
    """一行小字「廣告內容｜類型」+ 一行粗體「帳號｜廣告名稱｜建立方式：...」，
    對齊 2026-09-11 Molly 貼的參考截圖（另一個 skill 推播到同一空間的樣式）。"""
    wrap = (lambda s: f"<b>{s}</b>") if bold else (lambda s: s)
    lines = []
    if header.get("content_type"):
        lines.append(f"廣告內容｜{header['content_type']}")
    bold_parts = [p for p in [header.get("account_name", ""), header.get("ad_name", "")] if p]
    if header.get("build_method"):
        bold_parts.append(f"建立方式：{header['build_method']}")
    if bold_parts:
        lines.append(wrap("｜".join(bold_parts)))
    return lines


def placement_lines(entry: dict) -> list:
    lines = []
    if entry.get("adset_name"):
        lines.append(f"→ {entry['adset_name']}")
    if entry.get("campaign_name"):
        lines.append(f"Campaign：{entry['campaign_name']}")
    if entry.get("status"):
        label = entry.get("status_label", "狀態")
        lines.append(f"{label}：{entry['status']}")
    if entry.get("ad_id"):
        lines.append(f"ad_id：{entry['ad_id']}")
    if entry.get("extra_note"):
        lines.append(entry["extra_note"])
    return lines


def build_card(group: dict, index: int) -> dict:
    if "text" in group:
        return {"cardId": f"text-{index}", "card": {"sections": [{"widgets": [{"textParagraph": {"text": group["text"]}}]}]}}

    widgets = []
    hlines = header_lines(group["header"], bold=True)
    if hlines:
        widgets.append({"textParagraph": {"text": "<br>".join(hlines)}})

    placements = group["placements"]
    for i, entry in enumerate(placements):
        plines = placement_lines(entry)
        if not plines:
            continue
        widgets.append({"divider": {}})
        widgets.append({"textParagraph": {"text": "<br>".join(plines)}})

    first_ad_id = placements[0].get("ad_id", index) if placements else index
    return {"cardId": f"group-{index}-{first_ad_id}", "card": {"sections": [{"widgets": widgets}]}}


def build_payload(entries: list) -> dict:
    groups = group_entries(entries)
    cards = [build_card(g, i) for i, g in enumerate(groups)]
    return {"cardsV2": cards}


def render_preview(entries: list) -> str:
    groups = group_entries(entries)
    blocks = []
    for group in groups:
        if "text" in group:
            blocks.append(group["text"])
            continue
        lines = header_lines(group["header"], bold=False)
        lines.append("")
        for i, entry in enumerate(group["placements"]):
            if i > 0:
                lines.append("- - -")
            lines.extend(placement_lines(entry))
        blocks.append("\n".join(lines))
    return "\n---\n".join(blocks)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="JSON 檔路徑（陣列，見本檔說明）")
    parser.add_argument("--send", action="store_true", help="實際送出；不加這個旗標只會 dry-run 印出預覽")
    parser.add_argument("--webhook-var", default=DEFAULT_WEBHOOK_VAR, help=f"要讀取的 .env 變數名稱（預設 {DEFAULT_WEBHOOK_VAR}）")
    args = parser.parse_args()

    entries = json.loads(Path(args.file).read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        print("錯誤：--file 內容必須是一個 JSON 陣列。", file=sys.stderr)
        return 1

    groups = group_entries(entries)

    if not args.send:
        print("=== DRY RUN（未送出，加 --send 才會實際推播）===")
        print(render_preview(entries))
        print(f"\n({len(entries)} 筆 placement 聚合成 {len(groups)} 張卡片)")
        return 0

    webhook = load_webhook(args.webhook_var)
    if not webhook:
        print(f"錯誤：找不到 {args.webhook_var}（.env 或環境變數皆無值），已中止，未送出。", file=sys.stderr)
        return 1

    import requests

    payload = build_payload(entries)
    resp = requests.post(webhook, json=payload, timeout=15)
    if resp.status_code == 200:
        print(f"已推播到 Google Chat（{len(entries)} 筆 placement 聚合成 {len(groups)} 張卡片）。")
        return 0
    print(f"推播失敗：HTTP {resp.status_code} — {resp.text[:300]}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
