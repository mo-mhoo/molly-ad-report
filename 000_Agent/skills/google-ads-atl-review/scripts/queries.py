# -*- coding: utf-8 -*-
"""
ATL 覆盤專用查詢函式。純讀取，不含任何 mutate 操作。

跟專案根目錄的 google_ads_fetcher.py 不重複——那支只有「用預設 conversions 欄位
的整體維度拆解」，這裡補的是覆盤方法論需要、但那支沒有的部分：
  - 依 campaign 名稱過濾 ATL 活動
  - 依指定 conversion_action_name 過濾「真實購買」轉換數（避免多個轉換動作疊加灌水）
  - search_term_view 零轉換花費清單（所有比對類型，不只廣泛比對）
  - keyword 三層 status 交叉查詢（campaign / ad_group / ad_group_criterion）
"""
from collections import defaultdict
from datetime import date

from atl_client import run_query


def _micros(v) -> float:
    return float(v or 0) / 1_000_000


def list_atl_campaigns(client, customer_id: str, name_contains: str = "ATL") -> list[dict]:
    """列出名稱含指定字串（預設 ATL）的搜尋型活動。回傳 id/name/status。"""
    query = """
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """
    rows = run_query(client, customer_id, query)
    out = []
    for r in rows:
        if name_contains in r.campaign.name:
            out.append({
                "id": r.campaign.id,
                "name": r.campaign.name,
                "status": r.campaign.status.name,
                "channel_type": r.campaign.advertising_channel_type.name,
            })
    return out


def list_conversion_actions(
    client, customer_id: str, date_from: date, date_to: date, campaign_ids: list[int] | None = None
) -> list[dict]:
    """列出期間內各轉換動作名稱的轉換數/轉換價值，依價值排序。
    用來確認哪個是「真實購買」動作（見 project_google_ads_integration 的 conversions 灌水陷阱），
    第一次查某個帳號時務必先跑這個，不要假設欄位名稱跟毛孩時代一樣。"""
    where_campaign = ""
    if campaign_ids:
        ids = ",".join(str(i) for i in campaign_ids)
        where_campaign = f" AND campaign.id IN ({ids})"
    query = f"""
        SELECT segments.conversion_action_name, metrics.conversions, metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          {where_campaign}
    """
    rows = run_query(client, customer_id, query)
    totals: dict[str, dict] = defaultdict(lambda: {"conversions": 0.0, "conversions_value": 0.0})
    for r in rows:
        name = r.segments.conversion_action_name or "(未命名)"
        totals[name]["conversions"] += r.metrics.conversions
        totals[name]["conversions_value"] += r.metrics.conversions_value
    out = [
        {"conversion_action_name": k, "conversions": round(v["conversions"], 2),
         "conversions_value": round(v["conversions_value"])}
        for k, v in totals.items()
    ]
    return sorted(out, key=lambda x: x["conversions_value"], reverse=True)


def campaign_trend(
    client, customer_id: str, date_from: date, date_to: date,
    campaign_ids: list[int] | None = None, conversion_action_name: str | None = None,
) -> dict:
    """ATL 活動彙總花費/轉換數/轉換價值/ROAS。
    conversion_action_name 有給就只算該動作的轉換數（真實購買口徑）；
    沒給就用預設 conversions（可能疊加多個轉換動作，只適合看花費/CTR/CPC 這些跟轉換數無關的指標）。"""
    where_campaign = ""
    if campaign_ids:
        ids = ",".join(str(i) for i in campaign_ids)
        where_campaign = f" AND campaign.id IN ({ids})"

    # GAQL 不允許 segments.conversion_action_name 跟 clicks/cost_micros/impressions 同查詢
    # （PROHIBITED_SEGMENT_WITH_METRIC_IN_SELECT_OR_WHERE_CLAUSE）——這幾個指標不受轉換動作
    # 切分影響，拆成獨立查詢；真實購買轉換數/轉換價值另外用 conversion_action_name 過濾查。
    spend_query = f"""
        SELECT metrics.cost_micros, metrics.impressions, metrics.clicks
        FROM campaign
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          {where_campaign}
    """
    spend_rows = run_query(client, customer_id, spend_query)
    spend = sum(_micros(r.metrics.cost_micros) for r in spend_rows)
    impressions = sum(r.metrics.impressions for r in spend_rows)
    clicks = sum(r.metrics.clicks for r in spend_rows)

    if conversion_action_name:
        conv_query = f"""
            SELECT segments.conversion_action_name, metrics.conversions, metrics.conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND segments.conversion_action_name = '{conversion_action_name}'
              AND campaign.status != 'REMOVED'
              {where_campaign}
        """
    else:
        conv_query = f"""
            SELECT metrics.conversions, metrics.conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.status != 'REMOVED'
              {where_campaign}
        """
    conv_rows = run_query(client, customer_id, conv_query)
    conversions = sum(r.metrics.conversions for r in conv_rows)
    revenue = sum(r.metrics.conversions_value for r in conv_rows)
    return {
        "date_from": str(date_from),
        "date_to": str(date_to),
        "spend": round(spend),
        "impressions": int(impressions),
        "clicks": int(clicks),
        "conversions": round(conversions, 2),
        "conversions_value": round(revenue),
        "roas": round(revenue / spend, 2) if spend > 0 else 0,
        "cpa": round(spend / conversions) if conversions > 0 else None,
        "conversion_action_filter": conversion_action_name or "(預設，可能疊加多個轉換動作)",
    }


def search_terms_zero_conversion(
    client, customer_id: str, campaign_ids: list[int], date_from: date, date_to: date,
    min_spend: float = 0,
) -> list[dict]:
    """search_term_view 報表，涵蓋所有比對類型（廣泛/詞組/完全都要查，不能只查廣泛比對——
    見 feedback_google_ads_atl_keyword_optimization 的教訓）。依花費排序，只回傳 conversions=0 的列。"""
    if not campaign_ids:
        return []
    ids = ",".join(str(i) for i in campaign_ids)
    query = f"""
        SELECT campaign.name, ad_group.name, search_term_view.search_term,
               segments.keyword.info.text, segments.keyword.info.match_type,
               metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value
        FROM search_term_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.id IN ({ids})
    """
    rows = run_query(client, customer_id, query)
    out = []
    for r in rows:
        spend = _micros(r.metrics.cost_micros)
        if r.metrics.conversions > 0 or spend < min_spend:
            continue
        out.append({
            "campaign": r.campaign.name,
            "ad_group": r.ad_group.name,
            "search_term": r.search_term_view.search_term,
            "matched_keyword": r.segments.keyword.info.text,
            "match_type": r.segments.keyword.info.match_type.name,
            "clicks": r.metrics.clicks,
            "spend": round(spend),
        })
    return sorted(out, key=lambda x: x["spend"], reverse=True)


def keyword_revenue_lookup(
    client, customer_id: str, campaign_ids: list[int], date_from: date, date_to: date,
) -> dict[str, dict]:
    """既有關鍵字（keyword_view，非 search term）本身的花費/轉換數/轉換價值，用關鍵字文字當 key。
    用來做「無意圖字尾但關鍵字本身有實際營收」的交叉核對——見 feedback 裡魚油/全貓飼料的教訓，
    不能只看字尾就一刀切，要看這個字有沒有貢獻營收。"""
    if not campaign_ids:
        return {}
    ids = ",".join(str(i) for i in campaign_ids)
    query = f"""
        SELECT ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
               metrics.cost_micros, metrics.conversions, metrics.conversions_value
        FROM keyword_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.id IN ({ids})
          AND ad_group_criterion.status != 'REMOVED'
    """
    rows = run_query(client, customer_id, query)
    out: dict[str, dict] = defaultdict(lambda: {"spend": 0.0, "conversions": 0.0, "conversions_value": 0.0})
    for r in rows:
        key = r.ad_group_criterion.keyword.text
        out[key]["spend"] += _micros(r.metrics.cost_micros)
        out[key]["conversions"] += r.metrics.conversions
        out[key]["conversions_value"] += r.metrics.conversions_value
    return {k: {"spend": round(v["spend"]), "conversions": round(v["conversions"], 2),
                "conversions_value": round(v["conversions_value"])} for k, v in out.items()}


def keyword_status(
    client, customer_id: str, campaign_ids: list[int] | None = None,
    keyword_texts: list[str] | None = None,
) -> list[dict]:
    """關鍵字三層 status 交叉查詢：campaign.status / ad_group.status / ad_group_criterion.status。
    campaign 整個 PAUSED 時，裡面關鍵字即使顯示 ENABLED 也不會真的花錢——查證關鍵字有沒有在
    生效前務必三層都看，不要只看關鍵字自己的 status（見 project_google_ads_integration 的教訓）。
    也用這個查驗證批次調整是否真的生效，不要相信 Google Ads UI 的「變更記錄」頁面（有索引延遲）。"""
    where_clauses = ["ad_group_criterion.type = 'KEYWORD'"]
    if campaign_ids:
        ids = ",".join(str(i) for i in campaign_ids)
        where_clauses.append(f"campaign.id IN ({ids})")
    if keyword_texts:
        escaped = ",".join(f"'{k}'" for k in keyword_texts)
        where_clauses.append(f"ad_group_criterion.keyword.text IN ({escaped})")
    where = " AND ".join(where_clauses)
    query = f"""
        SELECT campaign.name, campaign.status, ad_group.name, ad_group.status,
               ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
               ad_group_criterion.status
        FROM ad_group_criterion
        WHERE {where}
    """
    rows = run_query(client, customer_id, query)
    return [{
        "campaign": r.campaign.name,
        "campaign_status": r.campaign.status.name,
        "ad_group": r.ad_group.name,
        "ad_group_status": r.ad_group.status.name,
        "keyword": r.ad_group_criterion.keyword.text,
        "match_type": r.ad_group_criterion.keyword.match_type.name,
        "criterion_status": r.ad_group_criterion.status.name,
    } for r in rows]
