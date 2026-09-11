# -*- coding: utf-8 -*-
"""
常用查詢的輔助函式。全部走 search_stream，回傳 list of dict，方便直接印或寫檔。
GAQL 查連結表（asset_group_asset / campaign_asset）預設都會排除 REMOVED 狀態——
這個帳號的歷史教訓是：查詢沒篩狀態，會把已經移除的連結也列出來，
誤判成「移除操作沒生效」，浪費一輪排查時間。
"""
from ads_client import get_client, clean_id


def list_pmax_campaigns(client, customer_id: str) -> list[dict]:
    """列出這個帳號底下所有 Performance Max 活動。"""
    ga_service = client.get_service("GoogleAdsService")
    q = """
    SELECT campaign.id, campaign.name, campaign.status
    FROM campaign
    WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
    ORDER BY campaign.name
    """
    out = []
    for batch in ga_service.search_stream(customer_id=customer_id, query=q):
        for row in batch.results:
            out.append({"id": row.campaign.id, "name": row.campaign.name, "status": row.campaign.status.name})
    return out


def list_asset_groups(client, customer_id: str, campaign_id: str, include_removed: bool = False) -> list[dict]:
    """列出某個 PMax 活動底下的資產群組。命名慣例是 `[~到期日]起始日-活動名稱`，
    可以從這裡抓現有命名規律，決定新群組怎麼取名。"""
    ga_service = client.get_service("GoogleAdsService")
    status_clause = "" if include_removed else "AND asset_group.status != 'REMOVED'"
    q = f"""
    SELECT asset_group.id, asset_group.name, asset_group.status, asset_group.final_urls
    FROM asset_group
    WHERE asset_group.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
    {status_clause}
    ORDER BY asset_group.name
    """
    out = []
    for batch in ga_service.search_stream(customer_id=customer_id, query=q):
        for row in batch.results:
            g = row.asset_group
            out.append({
                "id": g.id, "name": g.name, "status": g.status.name,
                "final_urls": list(g.final_urls),
            })
    return out


# 借用既有素材時容易漏掉的欄位——YOUTUBE_VIDEO 常常被忘記，
# 只查圖片/Logo/品牌名會讓人誤以為「這個群組沒有可借用的影片」。
ALL_REUSABLE_FIELD_TYPES = (
    "MARKETING_IMAGE", "SQUARE_MARKETING_IMAGE", "PORTRAIT_MARKETING_IMAGE",
    "LOGO", "LANDSCAPE_LOGO", "BUSINESS_NAME", "YOUTUBE_VIDEO",
)


def list_asset_group_assets(client, customer_id: str, asset_group_id: str,
                             field_types: tuple = ALL_REUSABLE_FIELD_TYPES) -> list[dict]:
    """列出某個資產群組目前生效中（非 REMOVED）的素材，預設涵蓋所有可借用類型
    （包含 YOUTUBE_VIDEO）。用來源群組的結果决定新群組要借用哪些既有素材。"""
    ga_service = client.get_service("GoogleAdsService")
    types_clause = ", ".join(f"'{t}'" for t in field_types)
    q = f"""
    SELECT asset_group_asset.resource_name, asset_group_asset.field_type, asset_group_asset.status,
      asset.resource_name, asset.name, asset.type,
      asset.image_asset.full_size.width_pixels, asset.image_asset.full_size.height_pixels,
      asset.youtube_video_asset.youtube_video_id, asset.youtube_video_asset.youtube_video_title
    FROM asset_group_asset
    WHERE asset_group_asset.asset_group = 'customers/{customer_id}/assetGroups/{asset_group_id}'
      AND asset_group_asset.field_type IN ({types_clause})
      AND asset_group_asset.status != 'REMOVED'
    """
    out = []
    for batch in ga_service.search_stream(customer_id=customer_id, query=q):
        for row in batch.results:
            aga, a = row.asset_group_asset, row.asset
            out.append({
                "link_resource_name": aga.resource_name,
                "field_type": aga.field_type.name,
                "asset_resource_name": a.resource_name,
                "asset_name": a.name,
                "asset_type": a.type_.name,
                "image_size": f"{a.image_asset.full_size.width_pixels}x{a.image_asset.full_size.height_pixels}" if a.image_asset.full_size.width_pixels else "",
                "youtube_video_id": a.youtube_video_asset.youtube_video_id,
                "youtube_video_title": a.youtube_video_asset.youtube_video_title,
            })
    return out


def count_asset_group_assets(client, customer_id: str, asset_group_id: str) -> dict:
    """統計某個資產群組目前每種 field_type 各有幾個素材（狀態已篩 != REMOVED），
    寫完 mutate 之後用這個做 verify，回報給使用者確認最終數量。"""
    ga_service = client.get_service("GoogleAdsService")
    q = f"""
    SELECT asset_group_asset.field_type
    FROM asset_group_asset
    WHERE asset_group_asset.asset_group = 'customers/{customer_id}/assetGroups/{asset_group_id}'
      AND asset_group_asset.status != 'REMOVED'
    """
    from collections import Counter
    counts = Counter()
    for batch in ga_service.search_stream(customer_id=customer_id, query=q):
        for row in batch.results:
            counts[row.asset_group_asset.field_type.name] += 1
    return dict(counts)


def list_campaign_assets(client, customer_id: str, campaign_id: str,
                          field_types: tuple = ("SITELINK", "CALLOUT", "PROMOTION")) -> list[dict]:
    """列出活動層級的額外資訊（Sitelink/Callout/Promotion 等）。這些會套用到整個
    PMax 活動下所有資產群組，不是單一資產群組專屬——動手新增前務必先看一次現況，
    避免跟既有的重複或衝突。"""
    ga_service = client.get_service("GoogleAdsService")
    types_clause = ", ".join(f"'{t}'" for t in field_types)
    q = f"""
    SELECT campaign_asset.field_type, campaign_asset.status,
      asset.callout_asset.callout_text, asset.sitelink_asset.link_text,
      asset.promotion_asset.promotion_target
    FROM campaign_asset
    WHERE campaign_asset.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
      AND campaign_asset.field_type IN ({types_clause})
      AND campaign_asset.status != 'REMOVED'
    """
    out = []
    for batch in ga_service.search_stream(customer_id=customer_id, query=q):
        for row in batch.results:
            ca, a = row.campaign_asset, row.asset
            text = a.callout_asset.callout_text or a.sitelink_asset.link_text or a.promotion_asset.promotion_target
            out.append({"field_type": ca.field_type.name, "status": ca.status.name, "text": text})
    return out


if __name__ == "__main__":
    import sys
    client = get_client()
    customer_id = clean_id(sys.argv[1]) if len(sys.argv) > 1 else clean_id("8669832537")
    for c in list_pmax_campaigns(client, customer_id):
        print(c)
