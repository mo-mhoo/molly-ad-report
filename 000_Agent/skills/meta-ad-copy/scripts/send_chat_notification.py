#!/usr/bin/env python3
"""推播 meta-ad-copy 上稿結果到 Google Chat（預設讀 .env 的 GOOGLE_CHAT_WEBHOOK_AD_UPLOAD，
這是「廣告軍團-Meta 上傳素材」空間專屬的 webhook，跟每日廣告日報用的 GOOGLE_CHAT_WEBHOOK 是不同變數）。

預設 dry-run：只印出將發送的文字，不會真的送出。
加 --send 才會實際 POST 到 webhook。訊息內容一律用 --file 傳入（避免命令列跳脫問題），
從不把 webhook URL 本身印到 stdout。
"""
import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="要推播的訊息文字檔路徑（純文字，Google Chat 支援基本 *粗體* 語法）")
    parser.add_argument("--send", action="store_true", help="實際送出；不加這個旗標只會 dry-run 印出訊息")
    parser.add_argument("--webhook-var", default=DEFAULT_WEBHOOK_VAR, help=f"要讀取的 .env 變數名稱（預設 {DEFAULT_WEBHOOK_VAR}）")
    args = parser.parse_args()

    message = Path(args.file).read_text(encoding="utf-8")

    if not args.send:
        print("=== DRY RUN（未送出，加 --send 才會實際推播）===")
        print(message)
        return 0

    webhook = load_webhook(args.webhook_var)
    if not webhook:
        print(f"錯誤：找不到 {args.webhook_var}（.env 或環境變數皆無值），已中止，未送出。", file=sys.stderr)
        return 1

    import requests

    resp = requests.post(webhook, json={"text": message}, timeout=15)
    if resp.status_code == 200:
        print("已推播到 Google Chat。")
        return 0
    print(f"推播失敗：HTTP {resp.status_code} — {resp.text[:300]}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
