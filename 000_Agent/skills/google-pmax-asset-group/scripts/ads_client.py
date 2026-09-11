# -*- coding: utf-8 -*-
"""
共用的 Google Ads client 啟動 + mutate 輔助工具。
所有 google-pmax-asset-group skill 底下的腳本都應該 import 這個檔案，
不要每次重新寫一份 client 設定邏輯（憑證路徑、id 清理、合併 mutate 這幾個坑都在這裡處理掉了）。

用法：
    from ads_client import get_client, clean_id, combined_mutate, weighted_len

    client = get_client()
    customer_id = clean_id("8669832537")
"""
import os
import re
import sys
import unicodedata

# 這個檔案假設 .env 在專案根目錄 C:\AI小摸\.env。
# load_dotenv() 預設用「呼叫腳本所在資料夾」往上找，不是用執行時的 cwd，
# 所以腳本放在 scratchpad 或其他資料夾時常常讀不到 .env，這裡明確指定路徑。
ENV_PATH = r"C:\AI小摸\.env"


def clean_id(v: str) -> str:
    """customer_id / login_customer_id 傳給 GoogleAdsClient 前一定要先清掉破折號/空白，
    不然會報 'invalid login customer id'。"""
    return re.sub(r"[^0-9]", "", v or "")


def get_client(login_customer_id: str | None = None):
    """建立 GoogleAdsClient。login_customer_id 預設用 .env 裡的 GOOGLE_ADS_LOGIN_CUSTOMER_ID
    （通常是騰勢 MCC 4195240594），要用別的 MCC 再自己傳參數覆蓋。"""
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_PATH)
    from google.ads.googleads.client import GoogleAdsClient

    lcid = login_customer_id or os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "4195240594")
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "use_proto_plus": True,
        "login_customer_id": clean_id(lcid),
    }
    return GoogleAdsClient.load_from_dict(config)


def weighted_len(text: str) -> int:
    """Google Ads 文字素材（標題/長標題/說明/Sitelink/Callout）的字數上限，
    全形字元（含大多數中文、全形標點）算 2、半形字元算 1。不能用 len() 直接算，
    中文句子的 len() 會嚴重低估實際佔用的字數上限。"""
    total = 0
    for ch in text:
        w = unicodedata.east_asian_width(ch)
        total += 2 if w in ("W", "F") else 1
    return total


def check_limit(text: str, limit: int, label: str = "") -> bool:
    """回傳是否在上限內；超過時印出警告方便腳本裡直接擋下來，不要等 API 報錯才發現。"""
    n = weighted_len(text)
    ok = n <= limit
    if not ok:
        print(f"[WARN] {label or text!r} 權重字數 {n} 超過上限 {limit}：{text!r}", file=sys.stderr)
    return ok


def new_mutate_operation(client, kind: str, op):
    """包一個 MutateOperation。kind 是 'asset_group' / 'asset' / 'asset_group_asset' /
    'campaign_asset' 之類的 oneof 欄位名稱（對應到 XxxOperation 的類型）。"""
    mo = client.get_type("MutateOperation")
    setattr(mo, f"{kind}_operation", op)
    return mo


def combined_mutate(client, customer_id: str, mutate_ops: list, validate_only: bool = True):
    """建立 asset_group / asset / asset_group_asset / campaign_asset 等多種操作時，
    一定要包在同一個 GoogleAdsService.Mutate 呼叫裡（不能拆成各自 service 的 mutate_xxx 呼叫），
    否則操作之間用來互相參照的暫時性負數 ID 沒辦法跨呼叫使用，會報 'Resource was not found'。

    validate_only=True 時只驗證不寫入，確認沒問題後才用 validate_only=False 正式送出
    （建議腳本用一個 --apply 之類的命令列參數切換，預設保持 validate_only=True 比較安全）。
    """
    ga_service = client.get_service("GoogleAdsService")
    req = client.get_type("MutateGoogleAdsRequest")
    req.customer_id = customer_id
    req.mutate_operations.extend(mutate_ops)
    req.validate_only = validate_only
    return ga_service.mutate(request=req)


def write_utf8(path: str, lines: list):
    """Windows 終端機是 cp950，中文字串直接 print 到 console 會亂碼（資料本身沒錯，
    只是顯示壞掉），查詢結果一律寫成 UTF-8 檔案，再用 Read 工具讀回來確認內容，
    不要靠 console 輸出判斷對不對。"""
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
