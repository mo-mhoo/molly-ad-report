# -*- coding: utf-8 -*-
"""
共用的 Google Ads client 啟動工具（唯讀查詢用，不含 mutate）。
跟 google-pmax-asset-group/scripts/ads_client.py 是同一套帳密來源，
但這個 skill 是純讀取的覆盤工具，不需要 combined_mutate 那些寫入輔助函式。

用法：
    from atl_client import get_client, clean_id
    client = get_client()
    customer_id = clean_id("1014276621")
"""
import os
import re

ENV_PATH = r"C:\AI小摸\.env"


def clean_id(v: str) -> str:
    """customer_id / login_customer_id 傳給 GoogleAdsClient 前一定要先清掉破折號/空白。"""
    return re.sub(r"[^0-9]", "", v or "")


def get_client(login_customer_id: str | None = None):
    """建立 GoogleAdsClient。login_customer_id 預設用 .env 裡的 GOOGLE_ADS_LOGIN_CUSTOMER_ID
    （騰勢 MCC 4195240594）。"""
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


def run_query(client, customer_id: str, query: str) -> list:
    from google.ads.googleads.errors import GoogleAdsException

    service = client.get_service("GoogleAdsService")
    try:
        stream = service.search_stream(customer_id=clean_id(customer_id), query=query)
        rows = []
        for batch in stream:
            rows.extend(batch.results)
        return rows
    except GoogleAdsException as e:
        detail = e.failure.errors[0].message if e.failure.errors else str(e)
        raise RuntimeError(f"Google Ads API 錯誤：{detail}")


def write_utf8(path: str, lines: list):
    """Windows 終端機是 cp950，中文字串直接印到 console 會亂碼，查詢結果一律先寫成
    UTF-8 檔案再用 Read 工具讀回來確認，不要靠 console 輸出判斷對不對。"""
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
