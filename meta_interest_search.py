"""
Meta 廣告興趣標籤搜尋工具
用法：python meta_interest_search.py
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"


def search_interests(query: str, limit: int = 20) -> list[dict]:
    """搜尋興趣標籤"""
    resp = requests.get(
        f"{BASE_URL}/search",
        params={
            "type": "adinterest",
            "q": query,
            "limit": limit,
            "locale": "zh_TW",
            "access_token": ACCESS_TOKEN,
        },
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def browse_interests(category_id: str = None) -> list[dict]:
    """瀏覽興趣分類（不輸入 category_id 則列出頂層分類）"""
    params = {
        "type": "adinterestcategory",
        "access_token": ACCESS_TOKEN,
    }
    if category_id:
        params["interest_list"] = f"[{category_id}]"

    resp = requests.get(f"{BASE_URL}/search", params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])


def get_interest_suggestions(interest_ids: list[str]) -> list[dict]:
    """根據已選標籤，取得 Meta 推薦的相關標籤"""
    resp = requests.get(
        f"{BASE_URL}/search",
        params={
            "type": "adinterestsuggestion",
            "interest_list": str(interest_ids).replace("'", '"'),
            "access_token": ACCESS_TOKEN,
        },
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def print_results(results: list[dict]):
    if not results:
        print("  （無結果）")
        return
    print(f"  {'名稱':<35} {'受眾規模':>15}  ID")
    print("  " + "-" * 70)
    for item in results:
        name = item.get("name", "")
        audience = item.get("audience_size_lower_bound", 0)
        audience_upper = item.get("audience_size_upper_bound", 0)
        item_id = item.get("id", "")
        path = " > ".join(item.get("path", []))
        size_str = f"{audience:,}–{audience_upper:,}" if audience else "N/A"
        print(f"  {name:<35} {size_str:>20}  {item_id}")
        if path:
            print(f"    路徑：{path}")


def main():
    if not ACCESS_TOKEN:
        print("❌ 找不到 META_ACCESS_TOKEN，請在 .env 檔案中設定：")
        print("   META_ACCESS_TOKEN=your_token_here")
        return

    print("=" * 60)
    print("  Meta 廣告興趣標籤搜尋工具")
    print("=" * 60)
    print("指令：")
    print("  s <關鍵字>   搜尋興趣標籤（支援中英文）")
    print("  r <ID>       根據標籤 ID 取得推薦相關標籤")
    print("  q            離開")
    print()

    collected_ids = []

    while True:
        try:
            raw = input(">> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not raw:
            continue

        if raw.lower() == "q":
            break

        parts = raw.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if cmd == "s":
            if not arg:
                print("請輸入關鍵字，例如：s 懷孕")
                continue
            print(f"\n搜尋「{arg}」中...")
            try:
                results = search_interests(arg)
                print_results(results)
                # 自動收集 ID 供後續推薦使用
                collected_ids = [r["id"] for r in results if "id" in r]
                if collected_ids:
                    print(f"\n  已收集 {len(collected_ids)} 個標籤 ID，輸入 r 取得推薦標籤")
            except requests.HTTPError as e:
                print(f"❌ API 錯誤：{e.response.text}")

        elif cmd == "r":
            ids_to_use = [arg] if arg else collected_ids[:5]
            if not ids_to_use:
                print("請先搜尋標籤，或輸入 r <ID>")
                continue
            print(f"\n取得推薦標籤（基於 {len(ids_to_use)} 個標籤）...")
            try:
                results = get_interest_suggestions(ids_to_use)
                print_results(results)
            except requests.HTTPError as e:
                print(f"❌ API 錯誤：{e.response.text}")

        else:
            # 直接當搜尋關鍵字
            print(f"\n搜尋「{raw}」中...")
            try:
                results = search_interests(raw)
                print_results(results)
                collected_ids = [r["id"] for r in results if "id" in r]
            except requests.HTTPError as e:
                print(f"❌ API 錯誤：{e.response.text}")

        print()


if __name__ == "__main__":
    main()
