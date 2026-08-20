"""
Google Ads API Fetcher
抓取廣告數據並轉換成 analyze_by_dimension 工具所需的 rows 格式。
介面與 meta_fetcher.py 對齊（fetch_dimension / _FETCHERS），方便 app.py 共用同一套分析流程。

支援維度：廣告活動、廣告群組、關鍵字（僅搜尋活動）、日期、週、月

憑證取得方式（需先手動完成，見 README 或詢問 Claude）：
  1. Developer Token — Google Ads 帳戶 > 工具與設定 > API 中心
  2. OAuth Client ID/Secret — Google Cloud Console，建立「Desktop app」類型的 OAuth 用戶端
  3. Refresh Token — 執行 get_google_ads_refresh_token.py 一次性產生
"""

import re
from collections import defaultdict
from datetime import date, timedelta
from typing import Literal, Optional

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

DimensionKey = Literal["廣告活動", "廣告群組", "關鍵字", "日期", "週", "月"]

BASE_METRIC_FIELDS = [
    "metrics.cost_micros",
    "metrics.impressions",
    "metrics.clicks",
    "metrics.conversions",
    "metrics.conversions_value",
]


# ── Client / query helpers ───────────────────────────────────────────────────

def _client(
    developer_token: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    login_customer_id: str = "",
) -> GoogleAdsClient:
    config = {
        "developer_token": developer_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "use_proto_plus": True,
    }
    if login_customer_id:
        config["login_customer_id"] = _clean_id(login_customer_id)
    return GoogleAdsClient.load_from_dict(config)


def _clean_id(customer_id: str) -> str:
    """Strip dashes/spaces — Google Ads API wants a bare 10-digit string."""
    return re.sub(r"[^0-9]", "", customer_id or "")


def _run_query(client: GoogleAdsClient, customer_id: str, query: str) -> list:
    service = client.get_service("GoogleAdsService")
    try:
        stream = service.search_stream(customer_id=_clean_id(customer_id), query=query)
        rows = []
        for batch in stream:
            rows.extend(batch.results)
        return rows
    except GoogleAdsException as e:
        detail = e.failure.errors[0].message if e.failure.errors else str(e)
        raise RuntimeError(f"Google Ads API 錯誤：{detail}")


def _row_metrics(row) -> dict:
    m = row.metrics
    return {
        "cost_micros": m.cost_micros,
        "impressions": m.impressions,
        "clicks": m.clicks,
        "conversions": m.conversions,
        "conversions_value": m.conversions_value,
    }


# ── Row building / aggregation (mirrors meta_fetcher.py) ────────────────────

def _build_row(label: str, m: dict) -> dict:
    spend = float(m.get("cost_micros") or 0) / 1_000_000
    impressions = float(m.get("impressions") or 0)
    clicks = float(m.get("clicks") or 0)
    conversions = float(m.get("conversions") or 0)
    revenue = float(m.get("conversions_value") or 0)

    row: dict = {
        "label": label,
        "spend": round(spend),
        "revenue": round(revenue),
        "roas": round(revenue / spend, 2) if spend > 0 else 0,
        "conversions": round(conversions, 2),
        "clicks": int(clicks),
        "impressions": int(impressions),
        "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
        "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
        "cpm": round(spend / impressions * 1000, 2) if impressions > 0 else 0,
        "cvr_click_to_conversion": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
    }
    if spend > 0 and conversions > 0:
        row["cpa"] = round(spend / conversions)
        row["aov"] = round(revenue / conversions)
    return row


def _aggregate(label: str, metric_dicts: list[dict]) -> dict:
    totals: dict = defaultdict(float)
    for m in metric_dicts:
        for k in ("cost_micros", "impressions", "clicks", "conversions", "conversions_value"):
            totals[k] += float(m.get(k) or 0)
    return _build_row(label, totals)


def _bucket_by_label(rows_with_metrics: list[tuple[str, dict]]) -> list[dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for label, m in rows_with_metrics:
        buckets[label].append(m)
    return [_aggregate(label, ms) for label, ms in buckets.items()]


# ── Dimension fetchers ────────────────────────────────────────────────────────

def fetch_by_campaign(
    customer_id: str, client: GoogleAdsClient, date_from: date, date_to: date
) -> list[dict]:
    query = f"""
        SELECT campaign.name, {", ".join(BASE_METRIC_FIELDS)}
        FROM campaign
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
    """
    rows = _run_query(client, customer_id, query)
    pairs = [(r.campaign.name, _row_metrics(r)) for r in rows]
    return _bucket_by_label(pairs)


def fetch_by_ad_group(
    customer_id: str, client: GoogleAdsClient, date_from: date, date_to: date
) -> list[dict]:
    query = f"""
        SELECT campaign.name, ad_group.name, {", ".join(BASE_METRIC_FIELDS)}
        FROM ad_group
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND ad_group.status != 'REMOVED'
    """
    rows = _run_query(client, customer_id, query)
    pairs = [
        (f"{r.campaign.name} / {r.ad_group.name}", _row_metrics(r)) for r in rows
    ]
    return _bucket_by_label(pairs)


def fetch_by_keyword(
    customer_id: str, client: GoogleAdsClient, date_from: date, date_to: date
) -> list[dict]:
    """僅涵蓋搜尋廣告活動的關鍵字成效。"""
    query = f"""
        SELECT ad_group_criterion.keyword.text, campaign.name, {", ".join(BASE_METRIC_FIELDS)}
        FROM keyword_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.advertising_channel_type = 'SEARCH'
          AND ad_group_criterion.status != 'REMOVED'
    """
    rows = _run_query(client, customer_id, query)
    pairs = [
        (r.ad_group_criterion.keyword.text, _row_metrics(r)) for r in rows
    ]
    return _bucket_by_label(pairs)


def _fetch_daily(
    customer_id: str, client: GoogleAdsClient, date_from: date, date_to: date
) -> list[tuple[date, dict]]:
    query = f"""
        SELECT segments.date, {", ".join(BASE_METRIC_FIELDS)}
        FROM campaign
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
    """
    rows = _run_query(client, customer_id, query)
    out = []
    for r in rows:
        d = date.fromisoformat(r.segments.date)
        out.append((d, _row_metrics(r)))
    return out


def fetch_by_date(
    customer_id: str, client: GoogleAdsClient, date_from: date, date_to: date
) -> list[dict]:
    daily = _fetch_daily(customer_id, client, date_from, date_to)
    pairs = [(str(d), m) for d, m in daily]
    return _bucket_by_label(pairs)


def fetch_by_week(
    customer_id: str, client: GoogleAdsClient, date_from: date, date_to: date
) -> list[dict]:
    daily = _fetch_daily(customer_id, client, date_from, date_to)
    pairs = []
    for d, m in daily:
        week_start = d - timedelta(days=d.weekday())  # Monday
        week_end = week_start + timedelta(days=6)
        pairs.append((f"{week_start} ~ {week_end}", m))
    return _bucket_by_label(pairs)


def fetch_by_month(
    customer_id: str, client: GoogleAdsClient, date_from: date, date_to: date
) -> list[dict]:
    daily = _fetch_daily(customer_id, client, date_from, date_to)
    pairs = [(f"{d:%Y-%m}", m) for d, m in daily]
    return _bucket_by_label(pairs)


# ── Main dispatch ─────────────────────────────────────────────────────────────

_FETCHERS = {
    "廣告活動": fetch_by_campaign,
    "廣告群組": fetch_by_ad_group,
    "關鍵字":   fetch_by_keyword,
    "日期":     fetch_by_date,
    "週":       fetch_by_week,
    "月":       fetch_by_month,
}


def fetch_dimension(
    dimension: DimensionKey,
    customer_id: str,
    developer_token: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    date_from: date,
    date_to: date,
    login_customer_id: str = "",
) -> list[dict]:
    """
    公開入口。回傳依 spend 排序的 rows，可直接餵給 analyze_by_dimension 工具。
    """
    fetcher = _FETCHERS.get(dimension)
    if fetcher is None:
        raise ValueError(f"不支援的維度：{dimension}（可用：{list(_FETCHERS)}）")

    client = _client(developer_token, client_id, client_secret, refresh_token, login_customer_id)
    rows = fetcher(customer_id, client, date_from, date_to)
    return sorted(rows, key=lambda r: r.get("spend", 0), reverse=True)
