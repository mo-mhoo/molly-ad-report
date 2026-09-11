#!/usr/bin/env python3
"""推播上稿/巡檢結果到 Google Chat，用 Cards v2 卡片格式（跟其他工具推播到同一個
「廣告軍團-Meta 上傳素材」空間的視覺樣式對齊：粗體標題行 + 分隔線 + 內文）。

預設讀 .env 的 GOOGLE_CHAT_WEBHOOK_AD_UPLOAD（跟每日廣告日報用的 GOOGLE_CHAT_WEBHOOK 是不同變數）。

輸入是一個 JSON 檔（--file），內容是一個陣列，每個元素是一張卡片要顯示的欄位：
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

也支援單純文字卡片（沒有上面那些欄位、只有 "text" 欄位的元素），用於巡檢摘要這類不是
「一支廣告一張卡」的訊息。

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


def build_ad_card(entry: dict, index: int) -> dict:
    header_lines = []
    if entry.get("content_type"):
        header_lines.append(f"<b>廣告內容｜{entry['content_type']}</b>")
    if entry.get("account_name") or entry.get("ad_name"):
        header_lines.append(f"<b>{entry.get('account_name', '')}｜{entry.get('ad_name', '')}</b>")
    if entry.get("build_method"):
        header_lines.append(f"<b>建立方式：{entry['build_method']}</b>")

    detail_lines = []
    if entry.get("adset_name"):
        detail_lines.append(f"→ {entry['adset_name']}")
    if entry.get("campaign_name"):
        detail_lines.append(f"Campaign：{entry['campaign_name']}")
    if entry.get("status"):
        detail_lines.append(f"狀態：{entry['status']}")
    if entry.get("ad_id"):
        detail_lines.append(f"ad_id：{entry['ad_id']}")
    if entry.get("extra_note"):
        detail_lines.append(entry["extra_note"])

    widgets = []
    if header_lines:
        widgets.append({"textParagraph": {"text": "<br>".join(header_lines)}})
    if header_lines and detail_lines:
        widgets.append({"divider": {}})
    if detail_lines:
        widgets.append({"textParagraph": {"text": "<br>".join(detail_lines)}})

    return {"cardId": f"entry-{index}-{entry.get('ad_id', index)}", "card": {"sections": [{"widgets": widgets}]}}


def build_text_card(entry: dict, index: int) -> dict:
    return {
        "cardId": f"text-{index}",
        "card": {"sections": [{"widgets": [{"textParagraph": {"text": entry["text"]}}]}]},
    }


def build_payload(entries: list) -> dict:
    cards = []
    for i, entry in enumerate(entries):
        if "text" in entry and len(entry) == 1:
            cards.append(build_text_card(entry, i))
        else:
            cards.append(build_ad_card(entry, i))
    return {"cardsV2": cards}


def render_preview(entries: list) -> str:
    lines = []
    for entry in entries:
        if "text" in entry and len(entry) == 1:
            lines.append(entry["text"])
        else:
            block = []
            if entry.get("content_type"):
                block.append(f"廣告內容｜{entry['content_type']}")
            if entry.get("account_name") or entry.get("ad_name"):
                block.append(f"{entry.get('account_name', '')}｜{entry.get('ad_name', '')}")
            if entry.get("build_method"):
                block.append(f"建立方式：{entry['build_method']}")
            block.append("")
            if entry.get("adset_name"):
                block.append(f"→ {entry['adset_name']}")
            if entry.get("campaign_name"):
                block.append(f"Campaign：{entry['campaign_name']}")
            if entry.get("status"):
                block.append(f"狀態：{entry['status']}")
            if entry.get("ad_id"):
                block.append(f"ad_id：{entry['ad_id']}")
            if entry.get("extra_note"):
                block.append(entry["extra_note"])
            lines.append("\n".join(block))
    return "\n---\n".join(lines)


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

    if not args.send:
        print("=== DRY RUN（未送出，加 --send 才會實際推播）===")
        print(render_preview(entries))
        print(f"\n(將建立 {len(entries)} 張卡片)")
        return 0

    webhook = load_webhook(args.webhook_var)
    if not webhook:
        print(f"錯誤：找不到 {args.webhook_var}（.env 或環境變數皆無值），已中止，未送出。", file=sys.stderr)
        return 1

    import requests

    payload = build_payload(entries)
    resp = requests.post(webhook, json=payload, timeout=15)
    if resp.status_code == 200:
        print(f"已推播到 Google Chat（{len(entries)} 張卡片）。")
        return 0
    print(f"推播失敗：HTTP {resp.status_code} — {resp.text[:300]}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
