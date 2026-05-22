"""
Meta Marketing API Fetcher
抓取廣告數據並轉換成 analyze_by_dimension 工具所需的 rows 格式。

支援維度：新舊客、素材類型、媒材、廣告組合、日期、週、月、品項

命名規則假設（可在下方 NAME_SEGMENTS 調整）：
  廣告組合名稱：{新舊客} | {受眾/興趣}   例：新客 | 早午餐/速食
  廣告名稱：    {素材類型} | {品項} | ...  例：比較文 | 關節粉 | v1
"""

import json
from collections import defaultdict
from datetime import date
from typing import Literal

import requests

GRAPH_URL = "https://graph.facebook.com/v21.0"

# ── 命名規則設定（依實際廣告命名調整）────────────────────────────────────────
NAME_SEGMENTS = {
    "新舊客":   {"level": "adset", "delimiter": "|", "index": 0},
    "受眾":     {"level": "adset", "delimiter": "|", "index": 1},
    "素材類型": {"level": "ad",    "delimiter": "|", "index": 0},
    "品項":     {"level": "ad",    "delimiter": "|", "index": 1},
}

# Meta creative object_type → 中文媒材
FORMAT_MAP = {
    "VIDEO":    "影片",
    "CAROUSEL": "多圖",
    "PHOTO":    "單圖",
    "IMAGE":    "單圖",
    "LINK":     "單圖",
    "STATUS":   "其他",
    "OTHER":    "其他",
}

DimensionKey = Literal["新舊客", "素材類型", "媒材", "廣告組合", "日期", "品項", "週", "月"]

BASE_FIELDS = [
    "spend", "impressions", "clicks", "ctr", "cpc", "cpm", "frequency",
    "actions", "action_values",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _paginate(url: str, params: dict) -> list[dict]:
    """Fetch all pages from a Meta Insights endpoint."""
    results: list[dict] = []
    while url:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise RuntimeError(f"Meta API 錯誤：{body['error']['message']}")
        results.extend(body.get("data", []))
        url = body.get("paging", {}).get("next", "")
        params = {}  # next URL already contains all query params
    return results


def _base_params(access_token: str, date_from: date, date_to: date) -> dict:
    return {
        "access_token": access_token,
        "time_range": json.dumps({"since": str(date_from), "until": str(date_to)}),
        "limit": 500,
    }


def _action_maps(raw: dict) -> tuple[dict, dict]:
    counts = {a["action_type"]: float(a["value"]) for a in raw.get("actions") or []}
    values = {a["action_type"]: float(a["value"]) for a in raw.get("action_values") or []}
    return counts, values


def _build_row(label: str, raw: dict) -> dict:
    """Convert a single raw API row into the row format for analyze_by_dimension."""
    spend = float(raw.get("spend") or 0)
    clicks = float(raw.get("clicks") or 0)
    impressions = float(raw.get("impressions") or 0)
    counts, values = _action_maps(raw)

    purchases = counts.get("purchase", 0)
    add_to_cart = counts.get("add_to_cart", 0)
    revenue = values.get("purchase", 0)

    row: dict = {
        "label": label,
        "spend": round(spend),
        "revenue": round(revenue),
        "roas": round(revenue / spend, 2) if spend > 0 else 0,
        "purchases": int(purchases),
        "clicks": int(clicks),
        "ctr": round(float(raw.get("ctr") or 0), 2),
        "cpc": round(float(raw.get("cpc") or 0), 2),
        "cpm": round(float(raw.get("cpm") or 0), 2),
        "frequency": round(float(raw.get("frequency") or 0), 2),
        "cart_count": int(add_to_cart),
        "cvr_click_to_purchase": round(purchases / clicks * 100, 2) if clicks > 0 else 0,
        "cvr_click_to_cart": round(add_to_cart / clicks * 100, 2) if clicks > 0 else 0,
        "cvr_cart_to_purchase": round(purchases / add_to_cart * 100, 2) if add_to_cart > 0 else 0,
    }
    if spend > 0 and purchases > 0:
        row["cpa"] = round(spend / purchases)
        row["aov"] = round(revenue / purchases)
    if spend > 0 and add_to_cart > 0:
        row["cart_cpa"] = round(spend / add_to_cart)
    return row


def _aggregate(label: str, rows: list[dict]) -> dict:
    """
    Sum numeric metrics across multiple raw API rows, then build one output row.
    Handles CTR / CPC / CPM as derived values from totals.
    """
    spend = clicks = impressions = 0.0
    action_counts: dict = defaultdict(float)
    action_values: dict = defaultdict(float)
    freq_sum = freq_n = 0.0

    for r in rows:
        spend += float(r.get("spend") or 0)
        clicks += float(r.get("clicks") or 0)
        impressions += float(r.get("impressions") or 0)
        f = float(r.get("frequency") or 0)
        if f > 0:
            freq_sum += f
            freq_n += 1
        for a in r.get("actions") or []:
            action_counts[a["action_type"]] += float(a["value"])
        for a in r.get("action_values") or []:
            action_values[a["action_type"]] += float(a["value"])

    synthetic: dict = {
        "spend": spend,
        "clicks": clicks,
        "impressions": impressions,
        "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
        "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
        "cpm": round(spend / impressions * 1000, 2) if impressions > 0 else 0,
        "frequency": round(freq_sum / freq_n, 2) if freq_n > 0 else 0,
        "actions": [{"action_type": k, "value": v} for k, v in action_counts.items()],
        "action_values": [{"action_type": k, "value": v} for k, v in action_values.items()],
    }
    return _build_row(label, synthetic)


# ── Dimension fetchers ────────────────────────────────────────────────────────

def fetch_by_adset(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        **_base_params(access_token, date_from, date_to),
        "level": "adset",
        "fields": ",".join(BASE_FIELDS + ["adset_name"]),
    }
    return [_build_row(r["adset_name"], r) for r in _paginate(url, params)]


def fetch_by_date(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        **_base_params(access_token, date_from, date_to),
        "level": "account",
        "time_increment": "1",
        "fields": ",".join(BASE_FIELDS),
    }
    return [_build_row(r["date_start"], r) for r in _paginate(url, params)]


def fetch_by_week(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        **_base_params(access_token, date_from, date_to),
        "level": "account",
        "time_increment": "7",
        "fields": ",".join(BASE_FIELDS),
    }
    return [
        _build_row(f"{r['date_start']} ~ {r['date_stop']}", r)
        for r in _paginate(url, params)
    ]


def fetch_by_month(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        **_base_params(access_token, date_from, date_to),
        "level": "account",
        "time_increment": "monthly",
        "fields": ",".join(BASE_FIELDS),
    }
    # Use YYYY-MM as label
    return [_build_row(r["date_start"][:7], r) for r in _paginate(url, params)]


def fetch_by_media_format(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    """
    Fetch ad-level data with creative object_type.
    Meta supports nested fields: creative{object_type,effective_object_story_spec}
    """
    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        **_base_params(access_token, date_from, date_to),
        "level": "ad",
        "fields": ",".join(
            BASE_FIELDS + ["ad_id", "creative{object_type,effective_object_story_spec}"]
        ),
    }
    ad_rows = _paginate(url, params)

    buckets: dict[str, list] = defaultdict(list)
    for row in ad_rows:
        creative = row.get("creative") or {}
        obj_type = creative.get("object_type", "OTHER").upper()
        spec = creative.get("effective_object_story_spec") or {}
        link_data = spec.get("link_data") or {}
        if "child_attachments" in link_data:
            obj_type = "CAROUSEL"
        fmt = FORMAT_MAP.get(obj_type, "其他")
        buckets[fmt].append(row)

    return [_aggregate(fmt, rows) for fmt, rows in buckets.items()]


def _fetch_by_name_segment(
    account_id: str,
    access_token: str,
    date_from: date,
    date_to: date,
    level: str,
    delimiter: str,
    index: int,
) -> list[dict]:
    """
    Generic name-based grouping.
    Fetches at `level` (adset or ad) and groups rows by the Nth segment of the name.
    """
    name_field = "adset_name" if level == "adset" else "ad_name"
    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        **_base_params(access_token, date_from, date_to),
        "level": level,
        "fields": ",".join(BASE_FIELDS + [name_field]),
    }
    rows = _paginate(url, params)

    buckets: dict[str, list] = defaultdict(list)
    for row in rows:
        name = row.get(name_field, "")
        parts = [p.strip() for p in name.split(delimiter)]
        label = parts[index] if index < len(parts) else "其他"
        buckets[label].append(row)

    return [_aggregate(label, rows) for label, rows in buckets.items()]


def fetch_by_customer_type(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    cfg = NAME_SEGMENTS["新舊客"]
    return _fetch_by_name_segment(
        account_id, access_token, date_from, date_to,
        cfg["level"], cfg["delimiter"], cfg["index"],
    )


def fetch_by_creative_type(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    cfg = NAME_SEGMENTS["素材類型"]
    return _fetch_by_name_segment(
        account_id, access_token, date_from, date_to,
        cfg["level"], cfg["delimiter"], cfg["index"],
    )


def fetch_by_product(
    account_id: str, access_token: str, date_from: date, date_to: date
) -> list[dict]:
    """
    Try product_id breakdown first (動態廣告).
    Falls back to parsing the ad name by NAME_SEGMENTS["品項"] config.
    """
    url = f"{GRAPH_URL}/act_{account_id}/insights"
    params = {
        **_base_params(access_token, date_from, date_to),
        "level": "ad",
        "breakdowns": "product_id",
        "fields": ",".join(BASE_FIELDS + ["product_id"]),
    }
    try:
        rows = _paginate(url, params)
        if rows:
            buckets: dict[str, list] = defaultdict(list)
            for r in rows:
                buckets[r.get("product_id", "unknown")].append(r)
            return [_aggregate(pid, rows) for pid, rows in buckets.items()]
    except Exception:
        pass

    # Fallback: parse from ad name
    cfg = NAME_SEGMENTS["品項"]
    return _fetch_by_name_segment(
        account_id, access_token, date_from, date_to,
        cfg["level"], cfg["delimiter"], cfg["index"],
    )


# ── Main dispatch ─────────────────────────────────────────────────────────────

_FETCHERS = {
    "廣告組合": fetch_by_adset,
    "日期":     fetch_by_date,
    "週":       fetch_by_week,
    "月":       fetch_by_month,
    "媒材":     fetch_by_media_format,
    "新舊客":   fetch_by_customer_type,
    "素材類型": fetch_by_creative_type,
    "品項":     fetch_by_product,
}


def fetch_dimension(
    dimension: DimensionKey,
    account_id: str,
    access_token: str,
    date_from: date,
    date_to: date,
) -> list[dict]:
    """
    Public entry point. Returns rows sorted by spend desc.
    Each row is ready to pass as `rows` to analyze_by_dimension tool.
    """
    fetcher = _FETCHERS.get(dimension)
    if fetcher is None:
        raise ValueError(f"不支援的維度：{dimension}（可用：{list(_FETCHERS)}）")
    rows = fetcher(account_id, access_token, date_from, date_to)
    return sorted(rows, key=lambda r: r.get("spend", 0), reverse=True)
