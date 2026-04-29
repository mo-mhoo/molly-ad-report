import streamlit as st
import pandas as pd
import requests
import json
from datetime import date, timedelta
from pathlib import Path

st.set_page_config(page_title="廣告週報產生器", page_icon="📊", layout="wide")

REPORT_DIR = Path("/Users/a111111/Downloads/TSA/Report/")
CONFIG_FILE = Path(__file__).parent / "config.json"

CLIENT_PREFIX = {"毛孩時代": "毛孩", "御熹堂": "御熹堂"}
CHANNEL_KEYWORD = {"官網": "官網", "momo": "mo", "蝦皮": "蝦皮"}

META_NUMERIC_COLS = [
    "花費金額 (TWD)", "購買次數", "購買轉換值",
    "連結點擊次數", "曝光次數", "加到購物車次數",
    "開始結帳次數", "網站連結頁面瀏覽次數",
]

# ── Config ────────────────────────────────────────────────

def load_config():
    try:
        if "meta_token" in st.secrets:
            cfg = {
                "meta_token": st.secrets.get("meta_token", ""),
                "meta_account_id": st.secrets.get("meta_account_id", ""),
            }
            if "meta_accounts" in st.secrets:
                cfg["meta_accounts"] = [dict(a) for a in st.secrets["meta_accounts"]]
            return cfg
    except Exception:
        pass
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return json.load(f)
    return {"meta_token": "", "meta_account_id": ""}

def save_config(cfg):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception:
        pass

# ── Meta API ──────────────────────────────────────────────

ACTION_TYPES = {
    "general": {
        "purchase":  "purchase",
        "purchase_value": "purchase",
        "add_to_cart": "add_to_cart",
        "checkout": "initiate_checkout",
        "page_view": "landing_page_view",
    },
    "cpas": {
        "purchase":  "onsite_conversion.purchase",
        "purchase_value": "onsite_conversion.purchase",
        "add_to_cart": "onsite_conversion.add_to_cart",
        "checkout": "onsite_conversion.initiated_checkout",
        "page_view": "landing_page_view",
    },
}

def _fetch_raw_actions(access_token, ad_account_id, since, until):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"
    cpas_fields = (
        "campaign_name,spend,actions,action_values,"
        "purchase_roas,website_purchase_roas,"
        "omni_purchase,omni_add_to_cart,"
        "catalog_segment_value,catalog_segment_actions"
    )
    params = {
        "level": "campaign",
        "fields": cpas_fields,
        "time_range": json.dumps({"since": str(since), "until": str(until)}),
        "access_token": access_token,
        "limit": 500,
    }
    try:
        data = requests.get(url, params=params, timeout=15).json()
        all_types = {}
        for item in data.get("data", []):
            for a in item.get("actions", []):
                atype = a["action_type"]
                all_types[atype] = all_types.get(atype, 0) + float(a["value"])
            for a in item.get("action_values", []):
                atype = f"[value] {a['action_type']}"
                all_types[atype] = all_types.get(atype, 0) + float(a["value"])
            for field in ["purchase_roas","website_purchase_roas","omni_purchase",
                          "omni_add_to_cart","catalog_segment_value","catalog_segment_actions"]:
                if item.get(field) not in (None, "", []):
                    all_types[f"[field] {field}"] = str(item.get(field))
        return all_types
    except Exception as e:
        return {"error": str(e)}

def fetch_meta_insights(access_token, ad_account_id, since, until, account_type="general"):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"

    if account_type == "cpas":
        fields = (
            "campaign_name,spend,impressions,inline_link_clicks,"
            "actions,action_values,"
            "catalog_segment_actions,catalog_segment_value"
        )
    else:
        fields = "campaign_name,spend,impressions,inline_link_clicks,actions,action_values"

    params = {
        "level": "campaign",
        "fields": fields,
        "time_range": json.dumps({"since": str(since), "until": str(until)}),
        "access_token": access_token,
        "limit": 500,
    }
    resp = requests.get(url, params=params, timeout=30)
    data = resp.json()
    if "error" in data:
        raise Exception(data["error"].get("message", str(data["error"])))

    def get_action(lst, atype):
        for a in (lst or []):
            if a.get("action_type") == atype:
                return float(a["value"])
        return 0.0

    def safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    rows = []
    for item in data.get("data", []):
        actions = item.get("actions", [])
        action_values = item.get("action_values", [])

        if account_type == "cpas":
            catalog_actions = item.get("catalog_segment_actions", [])
            catalog_values  = item.get("catalog_segment_value", [])
            purchases    = get_action(catalog_actions, "purchase")
            purchase_val = get_action(catalog_values,  "purchase")
            add_to_cart  = get_action(catalog_actions, "add_to_cart")
            checkout     = get_action(catalog_actions, "initiate_checkout")
        else:
            purchases     = get_action(actions, "purchase")
            purchase_val  = get_action(action_values, "purchase")
            add_to_cart   = get_action(actions, "add_to_cart")
            checkout      = get_action(actions, "initiate_checkout")

        rows.append({
            "行銷活動名稱": item.get("campaign_name", ""),
            "花費金額 (TWD)": safe_float(item.get("spend")),
            "曝光次數": safe_float(item.get("impressions")),
            "連結點擊次數": safe_float(item.get("inline_link_clicks")),
            "購買次數": purchases,
            "購買轉換值": purchase_val,
            "加到購物車次數": add_to_cart,
            "開始結帳次數": checkout,
            "網站連結頁面瀏覽次數": get_action(actions, "landing_page_view"),
        })
    empty_cols = ["行銷活動名稱"] + META_NUMERIC_COLS
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=empty_cols)

# ── CSV 讀取 ──────────────────────────────────────────────

def load_meta_csv(filepath):
    df = pd.read_csv(str(filepath), encoding="utf-8", thousands=",")
    for col in META_NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def load_google_csv(filepath):
    with open(str(filepath), encoding="utf-16") as f:
        lines = f.readlines()
    header_idx = next(
        (i for i, l in enumerate(lines) if "費用" in l and "點擊" in l), None
    )
    if header_idx is None:
        return pd.DataFrame()
    header = [c.strip() for c in lines[header_idx].split("\t")]
    rows = []
    for line in lines[header_idx + 1:]:
        parts = [c.strip() for c in line.split("\t")]
        if len(parts) < 3 or parts[0].startswith("總計") or parts[0] == "":
            continue
        rows.append(parts[: len(header)])
    df = pd.DataFrame(rows, columns=header)
    for col in ["費用", "轉換", "轉換價值", "點擊", "曝光"]:
        if col in df.columns:
            df[col] = df[col].str.replace(",", "").str.replace('"', "")
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

# ── 指標計算 ─────────────────────────────────────────────

def classify_type(name):
    if pd.isna(name):
        return "Other"
    s = str(name)
    if "ATL" in s or "流量" in s:
        return "ATL"
    if "BTL" in s or "轉換" in s:
        return "BTL"
    return "Other"

def calc_meta_metrics(df):
    df = df.copy()
    df["_type"] = df["行銷活動名稱"].apply(classify_type)
    result = {}
    for t in ["ATL", "BTL"]:
        sub = df[df["_type"] == t]
        spend = sub["花費金額 (TWD)"].sum()
        clicks = sub["連結點擊次數"].sum()
        impr = sub["曝光次數"].sum()
        revenue = sub["購買轉換值"].sum()
        purchases = sub["購買次數"].sum()
        atc = sub["加到購物車次數"].sum()
        result[t] = {
            "花費": spend,
            "CPC": spend / clicks if clicks > 0 else 0,
            "CTR": clicks / impr * 100 if impr > 0 else 0,
            "ROAS": revenue / spend if spend > 0 else 0,
            "廣告收益": revenue,
            "CPA": spend / purchases if purchases > 0 else 0,
            "AOV": revenue / purchases if purchases > 0 else 0,
            "購買次數": purchases,
            "加購次數": atc,
            "購物車成本": spend / atc if atc > 0 else 0,
            "點擊到成交率": purchases / clicks * 100 if clicks > 0 else 0,
            "點擊到購物車率": atc / clicks * 100 if clicks > 0 else 0,
            "購物車到成交率": purchases / atc * 100 if atc > 0 else 0,
        }
    return result

def calc_google_metrics(df):
    if df.empty:
        return {}
    spend = df["費用"].sum() if "費用" in df.columns else 0
    revenue = df["轉換價值"].sum() if "轉換價值" in df.columns else 0
    conversions = df["轉換"].sum() if "轉換" in df.columns else 0
    clicks = df["點擊"].sum() if "點擊" in df.columns else 0
    impr = df["曝光"].sum() if "曝光" in df.columns else 0
    return {
        "花費": spend,
        "ROAS": revenue / spend if spend > 0 else 0,
        "轉換": conversions,
        "CPA": spend / conversions if conversions > 0 else 0,
        "CTR": clicks / impr * 100 if impr > 0 else 0,
        "CPC": spend / clicks if clicks > 0 else 0,
    }

def pct_change(curr, prev):
    if prev == 0 or prev is None:
        return None
    return (curr - prev) / abs(prev) * 100

def fmt_val(v, style="currency"):
    if v == 0:
        return "$0"
    if style == "currency":
        return f"${v:,.0f}"
    if style == "roas":
        return f"{v:.2f}"
    if style == "pct":
        return f"{v:.2f}%"
    return f"{v:,.0f}"

def fmt_change(v, higher_is_better=True):
    if v is None:
        return "-"
    good = (v >= 0 and higher_is_better) or (v < 0 and not higher_is_better)
    sign = "+" if v >= 0 else ""
    label = f"{sign}{v:.1f}%"
    return f'<span style="color:{"#16a34a" if good else "#dc2626"}">{label}</span>'

def build_table_html(curr_m, comp_m, yoy_m):
    rows_def = [
        ("ATL", "花費",    "currency", True),
        ("ATL", "CPC",     "currency", False),
        ("BTL", "花費",    "currency", True),
        ("BTL", "ROAS",    "roas",     True),
        ("BTL", "廣告收益", "currency", True),
        ("BTL", "CPA",     "currency", False),
        ("BTL", "AOV",     "currency", True),
    ]
    has_wow = comp_m is not None
    has_yoy = yoy_m is not None

    header = "<tr><th>類型</th><th>指標</th><th>實際數值</th>"
    if has_wow:
        header += "<th>WoW</th>"
    if has_yoy:
        header += "<th>YoY</th>"
    header += "</tr>"

    body = ""
    prev_type = None
    for t, metric, style, hib in rows_def:
        val = curr_m.get(t, {}).get(metric, 0)
        row = "<tr>"
        if t != prev_type:
            span = sum(1 for r in rows_def if r[0] == t)
            row += f'<td rowspan="{span}" style="font-weight:bold;background:#1e3a5f;color:white;text-align:center">{t}</td>'
            prev_type = t
        row += f"<td>{metric}</td><td>{fmt_val(val, style)}</td>"
        if has_wow:
            comp_val = comp_m.get(t, {}).get(metric, 0)
            row += f"<td>{fmt_change(pct_change(val, comp_val), hib)}</td>"
        if has_yoy:
            yoy_val = yoy_m.get(t, {}).get(metric, 0)
            row += f"<td>{fmt_change(pct_change(val, yoy_val), hib)}</td>"
        row += "</tr>"
        body += row

    return f"""
    <style>
    .ad-table {{ border-collapse:collapse; width:100%; font-size:14px; }}
    .ad-table th {{ background:#1e3a5f; color:white; padding:8px 12px; text-align:center; }}
    .ad-table td {{ border:1px solid #555; padding:8px 12px; text-align:center; color:#e8e8e8; background:#1a1a2e; }}
    .ad-table tr:nth-child(even) td {{ background:#16213e; }}
    </style>
    <table class="ad-table">{header}{body}</table>
    """

# ── 檔案偵測 ─────────────────────────────────────────────

def list_files(client, channel, platform="Meta"):
    prefix = CLIENT_PREFIX[client]
    kw = CHANNEL_KEYWORD[channel]
    files = [
        f for f in REPORT_DIR.iterdir()
        if f.suffix == ".csv"
        and prefix in f.name
        and kw in f.name
        and platform in f.name
    ]
    return sorted(files, key=lambda x: x.name, reverse=True)

# ── Claude 分析 ───────────────────────────────────────────

def build_prompt(channel, curr_m, comp_m, yoy_m, prev_actions, platform="Meta"):
    btl = curr_m.get("BTL", {})
    atl = curr_m.get("ATL", {})

    def change_str(curr_dict, comp_dict, yoy_dict, key):
        parts = []
        if comp_dict:
            c = pct_change(curr_dict.get(key, 0), comp_dict.get(key, 0))
            if c is not None:
                parts.append(f"WoW {'+' if c>=0 else ''}{c:.1f}%")
        if yoy_dict:
            c = pct_change(curr_dict.get(key, 0), yoy_dict.get(key, 0))
            if c is not None:
                parts.append(f"YoY {'+' if c>=0 else ''}{c:.1f}%")
        return "、".join(parts) if parts else "無對比"

    comp_atl = comp_m.get("ATL") if comp_m else None
    comp_btl = comp_m.get("BTL") if comp_m else None
    yoy_atl  = yoy_m.get("ATL")  if yoy_m  else None
    yoy_btl  = yoy_m.get("BTL")  if yoy_m  else None

    data_summary = f"""【{channel} 本期廣告數據 - {platform}】

ATL（流量型）：
- 花費：${atl.get('花費', 0):,.0f}（{change_str(atl, comp_atl, yoy_atl, '花費')}）
- CPC：${atl.get('CPC', 0):.1f}（{change_str(atl, comp_atl, yoy_atl, 'CPC')}）

BTL（轉換型）：
- 花費：${btl.get('花費', 0):,.0f}（{change_str(btl, comp_btl, yoy_btl, '花費')}）
- ROAS：{btl.get('ROAS', 0):.2f}（{change_str(btl, comp_btl, yoy_btl, 'ROAS')}）
- 廣告收益：${btl.get('廣告收益', 0):,.0f}（{change_str(btl, comp_btl, yoy_btl, '廣告收益')}）
- CPA：${btl.get('CPA', 0):,.0f}（{change_str(btl, comp_btl, yoy_btl, 'CPA')}）
- AOV：${btl.get('AOV', 0):,.0f}（{change_str(btl, comp_btl, yoy_btl, 'AOV')}）"""

    prev_section = f"\n\n【上週行動記錄】\n{prev_actions.strip()}" if prev_actions.strip() else ""

    return f"""你是一位資深數位廣告投手，正在撰寫廣告週報。請根據以下數據，產出週報中「觀察」與「行動」兩個段落。

{data_summary}{prev_section}

請依照以下格式輸出，不需要其他說明文字：

【觀察】
• （分析 WoW/YoY 變化的可能原因，包含市場因素、素材因素、受眾因素等假設）
• ...（共 3-5 點，每點 30 字以內，語氣專業直接）

【行動】
• （基於觀察的具體優化行動，包含預算調整、素材測試、受眾優化等）
• ...（共 3-5 點，每點 30 字以內，列出具體操作）"""

# ── 預設日期 ──────────────────────────────────────────────

def last_week_range():
    today = date.today()
    last_mon = today - timedelta(days=today.weekday() + 7)
    last_sun = last_mon + timedelta(days=6)
    return last_mon, last_sun

def prev_week_range(since, until):
    delta = (until - since) + timedelta(days=1)
    return since - delta, until - delta

def yoy_range(since, until):
    return date(since.year - 1, since.month, since.day), date(until.year - 1, until.month, until.day)

# ── UI ───────────────────────────────────────────────────

st.markdown("## 📊 廣告週報產生器")
st.markdown("Meta API 直抓 或 上傳 CSV，自動計算 WoW/YoY 並生成觀察與行動建議。")
st.divider()

cfg = load_config()

def check_token_expiry(token):
    if not token:
        return
    try:
        resp = requests.get(
            "https://graph.facebook.com/debug_token",
            params={"input_token": token, "access_token": token},
            timeout=5,
        )
        data = resp.json().get("data", {})
        exp_at = data.get("expires_at", 0)
        if exp_at == 0:
            return
        exp_date = date.fromtimestamp(exp_at)
        days_left = (exp_date - date.today()).days
        if days_left <= 0:
            st.error(f"⚠️ Meta Access Token 已過期！請立即更換。")
        elif days_left <= 7:
            st.warning(f"⚠️ Meta Access Token 將於 {exp_date} 到期（剩 {days_left} 天），請盡快更換。")
    except Exception:
        pass

if cfg.get("meta_token"):
    check_token_expiry(cfg["meta_token"])

def parse_account_name(name):
    parts = name.strip().split(" ", 1)
    client  = parts[0] if len(parts) > 0 else name
    channel = parts[1] if len(parts) > 1 else ""
    return client, channel

# ── 維度解析 ─────────────────────────────────────────────

ACTIVITY_TYPES = {"常態", "全館活動", "限搶活動"}
FORMAT_KEYWORDS = ["單圖", "多圖", "比較文", "影片", "原生圖", "輪播", "動態", "IG社群"]

def parse_campaign_audience(campaign_name):
    parts = [p.strip() for p in str(campaign_name).split('｜')]
    return parts[1] if len(parts) > 1 else "未標示"

def parse_ad_dims(ad_name):
    name = str(ad_name)
    parts = [p.strip() for p in name.split('_')]
    activity_type, format_type, category = "其他", "未知", "未知"
    found_format_idx = found_activity_idx = -1
    for i, part in enumerate(parts):
        if part in ACTIVITY_TYPES:
            activity_type = part
            found_activity_idx = i
    for i, part in enumerate(parts):
        if any(kw in part for kw in FORMAT_KEYWORDS):
            format_type = part
            found_format_idx = i
            break
    ref_idx = max(found_format_idx, found_activity_idx)
    if ref_idx >= 0 and ref_idx + 1 < len(parts):
        cand = parts[ref_idx + 1]
        if not (len(cand) >= 6 and cand[:4].isdigit()):
            category = cand.split('x')[0]
    if "代言人" in name:
        creative_type = "代言人"
    elif any(kw in name for kw in ["獸醫", "醫生", "醫師", "醫"]):
        creative_type = "醫生/獸醫"
    else:
        creative_type = "一般"
    return {"活動類型": activity_type, "格式": format_type, "品類": category, "素材類型": creative_type}

def fetch_meta_ad_insights(access_token, ad_account_id, since, until, account_type="general"):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"
    if account_type == "cpas":
        fields = "campaign_name,ad_name,spend,impressions,inline_link_clicks,catalog_segment_actions,catalog_segment_value"
    else:
        fields = "campaign_name,ad_name,spend,impressions,inline_link_clicks,actions,action_values"
    params = {
        "level": "ad",
        "fields": fields,
        "time_range": json.dumps({"since": str(since), "until": str(until)}),
        "access_token": access_token,
        "limit": 500,
    }
    resp = requests.get(url, params=params, timeout=30)
    data = resp.json()
    if "error" in data:
        raise Exception(data["error"].get("message", str(data["error"])))
    def get_action(lst, atype):
        for a in (lst or []):
            if a.get("action_type") == atype:
                return float(a["value"])
        return 0.0
    def safe_float(val):
        try: return float(val)
        except: return 0.0
    rows = []
    for item in data.get("data", []):
        actions = item.get("actions", [])
        action_values = item.get("action_values", [])
        if account_type == "cpas":
            ca = item.get("catalog_segment_actions", [])
            cv = item.get("catalog_segment_value", [])
            purchases = get_action(ca, "purchase")
            purchase_val = get_action(cv, "purchase")
        else:
            purchases = get_action(actions, "purchase")
            purchase_val = get_action(action_values, "purchase")
        rows.append({
            "行銷活動名稱": item.get("campaign_name", ""),
            "廣告名稱": item.get("ad_name", ""),
            "花費": safe_float(item.get("spend")),
            "曝光": safe_float(item.get("impressions")),
            "點擊": safe_float(item.get("inline_link_clicks")),
            "購買次數": purchases,
            "購買轉換值": purchase_val,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def _agg_by_dim(df, dim_col):
    result = {}
    for val, grp in df.groupby(dim_col):
        spend = grp["花費"].sum()
        purchases = grp["購買次數"].sum()
        revenue = grp["購買轉換值"].sum()
        impr = grp["曝光"].sum()
        clicks = grp["點擊"].sum()
        result[val] = {
            "花費": spend,
            "購買次數": purchases,
            "ROAS": revenue / spend if spend > 0 else 0,
            "CPA": spend / purchases if purchases > 0 else 0,
            "CTR%": clicks / impr * 100 if impr > 0 else 0,
        }
    return result

def build_dim_table(df, dim_col, df_comp=None, df_yoy=None):
    curr = _agg_by_dim(df, dim_col)
    comp = _agg_by_dim(df_comp, dim_col) if df_comp is not None and not df_comp.empty else {}
    yoy  = _agg_by_dim(df_yoy,  dim_col) if df_yoy  is not None and not df_yoy.empty  else {}
    has_wow = bool(comp)
    has_yoy = bool(yoy)

    rows = []
    for val, m in sorted(curr.items(), key=lambda x: -x[1]["花費"]):
        c = comp.get(val, {})
        y = yoy.get(val, {})
        row = {
            dim_col:    val,
            "花費":     f"${m['花費']:,.0f}",
            "購買次數": int(m["購買次數"]),
            "ROAS":     f"{m['ROAS']:.2f}",
            "CPA":      f"${m['CPA']:,.0f}" if m["CPA"] > 0 else "-",
            "CTR%":     f"{m['CTR%']:.2f}%",
        }
        if has_wow:
            row["花費 WoW"]  = fmt_change(pct_change(m["花費"], c.get("花費", 0)),  True)  if c else "-"
            row["ROAS WoW"] = fmt_change(pct_change(m["ROAS"], c.get("ROAS", 0)),  True)  if c else "-"
            row["CPA WoW"]  = fmt_change(pct_change(m["CPA"],  c.get("CPA",  0)),  False) if c else "-"
        if has_yoy:
            row["花費 YoY"]  = fmt_change(pct_change(m["花費"], y.get("花費", 0)),  True)  if y else "-"
            row["ROAS YoY"] = fmt_change(pct_change(m["ROAS"], y.get("ROAS", 0)),  True)  if y else "-"
            row["CPA YoY"]  = fmt_change(pct_change(m["CPA"],  y.get("CPA",  0)),  False) if y else "-"
        rows.append(row)
    return pd.DataFrame(rows)

def enrich_ad_dims(df):
    df = df.copy()
    df["ATL/BTL"]  = df["行銷活動名稱"].apply(classify_type)
    df["受眾"]     = df["行銷活動名稱"].apply(parse_campaign_audience)
    _d = df["廣告名稱"].apply(parse_ad_dims)
    df["活動類型"] = _d.apply(lambda x: x["活動類型"])
    df["格式"]     = _d.apply(lambda x: x["格式"])
    df["品類"]     = _d.apply(lambda x: x["品類"])
    df["素材類型"] = _d.apply(lambda x: x["素材類型"])
    return df

with st.sidebar:
    st.header("⚙️ 設定")
    platform_sel = st.selectbox("平台", ["Meta", "Google"])

    st.divider()
    data_source = st.radio(
        "資料來源",
        ["Meta API 自動抓取", "CSV 手動上傳"],
        disabled=(platform_sel == "Google"),
        help="Google 僅支援 CSV 上傳"
    )
    if platform_sel == "Google":
        data_source = "CSV 手動上傳"

    if data_source == "Meta API 自動抓取":
        st.divider()
        st.markdown("**Meta Access Token**")
        meta_token = st.text_input(
            "Access Token",
            value=cfg.get("meta_token", ""),
            type="password",
        )
        if st.button("💾 儲存 Token"):
            cfg["meta_token"] = meta_token
            save_config(cfg)
            st.success("Token 已儲存")

        st.divider()
        st.markdown("**廣告帳戶管理**")
        accounts = cfg.get("meta_accounts", [])
        if accounts:
            def acct_label(a):
                tag = "【CPAS】" if a.get("type") == "cpas" else "【一般】"
                return f"{tag} {a['name']}"
            acct_labels = [acct_label(a) for a in accounts]
            selected_acct_idx = st.selectbox("選擇帳戶", range(len(acct_labels)), format_func=lambda i: acct_labels[i], key="acct_sel")
            selected_account_id   = accounts[selected_acct_idx]["id"]
            selected_account_type = accounts[selected_acct_idx].get("type", "general")
            client_sel, channel_sel = parse_account_name(accounts[selected_acct_idx]["name"])
        else:
            st.info("尚未新增帳戶")
            selected_account_id   = cfg.get("meta_account_id", "")
            selected_account_type = "general"
            client_sel, channel_sel = "", ""

        with st.expander("➕ 新增帳戶"):
            new_name = st.text_input("帳戶名稱（例：毛孩時代官網）", key="new_acct_name")
            new_id   = st.text_input("廣告帳戶 ID", key="new_acct_id")
            new_type = st.radio("帳戶類型", ["general（官網）", "cpas（momo/蝦皮）"], key="new_acct_type")
            if st.button("新增"):
                if new_name and new_id:
                    accounts.append({
                        "name": new_name,
                        "id": new_id.strip(),
                        "type": "cpas" if "cpas" in new_type else "general",
                    })
                    cfg["meta_accounts"] = accounts
                    save_config(cfg)
                    st.success(f"已新增：{new_name}")
                    st.rerun()

        if accounts:
            with st.expander("🗑️ 刪除帳戶"):
                del_idx = st.selectbox("選擇要刪除的帳戶", range(len(acct_labels)), format_func=lambda i: acct_labels[i], key="del_acct")
                if st.button("確認刪除", type="secondary"):
                    accounts.pop(del_idx)
                    cfg["meta_accounts"] = accounts
                    save_config(cfg)
                    st.rerun()
    else:
        meta_token = cfg.get("meta_token", "")
        selected_account_id   = cfg.get("meta_account_id", "")
        selected_account_type = "general"
        client_sel  = st.text_input("客戶名稱", value="", key="csv_client")
        channel_sel = st.text_input("渠道", value="", key="csv_channel")

# ── 資料載入 ─────────────────────────────────────────────

acct_title = f"{client_sel} × {channel_sel}" if client_sel else ""
st.subheader(f"📁 {acct_title} × {platform_sel}" if acct_title else f"📁 {platform_sel}")

df_curr = df_comp = df_yoy = None

if data_source == "Meta API 自動抓取":
    default_since, default_until = last_week_range()
    default_comp_since, default_comp_until = prev_week_range(default_since, default_until)
    default_yoy_since, default_yoy_until = yoy_range(default_since, default_until)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**本期**")
        curr_since = st.date_input("開始", default_since, key="api_curr_s")
        curr_until = st.date_input("結束", default_until, key="api_curr_e")
    with col2:
        st.markdown("**對比期（WoW）**")
        comp_since = st.date_input("開始", default_comp_since, key="api_comp_s")
        comp_until = st.date_input("結束", default_comp_until, key="api_comp_e")
        use_comp = st.checkbox("啟用 WoW 對比", value=True)
    with col3:
        st.markdown("**去年同期（YoY）**")
        yoy_since = st.date_input("開始", default_yoy_since, key="api_yoy_s")
        yoy_until = st.date_input("結束", default_yoy_until, key="api_yoy_e")
        use_yoy = st.checkbox("啟用 YoY 對比", value=False)

    if st.button("🔄 從 Meta API 抓取數據", type="primary"):
        token = cfg.get("meta_token", "")
        acct  = selected_account_id
        if not token or not acct:
            st.error("請先在側欄填入 Access Token 和廣告帳戶 ID")
        else:
            with st.spinner("正在從 Meta API 抓取..."):
                try:
                    atype = selected_account_type
                    st.session_state["selected_account_type"] = atype
                    df_curr = fetch_meta_insights(token, acct, curr_since, curr_until, atype)
                    st.session_state["df_curr"] = df_curr
                    if use_comp:
                        df_comp = fetch_meta_insights(token, acct, comp_since, comp_until, atype)
                        st.session_state["df_comp"] = df_comp
                    else:
                        st.session_state.pop("df_comp", None)
                    if use_yoy:
                        df_yoy = fetch_meta_insights(token, acct, yoy_since, yoy_until, atype)
                        st.session_state["df_yoy"] = df_yoy
                    else:
                        st.session_state.pop("df_yoy", None)
                    st.success(f"✅ 抓到 {len(df_curr)} 個行銷活動")
                    st.session_state["dim_since"] = curr_since
                    st.session_state["dim_until"] = curr_until
                    st.session_state["dim_token"] = token
                    st.session_state["dim_acct_id"] = acct
                    st.session_state["dim_acct_type"] = atype
                    st.session_state["dim_comp_since"] = comp_since if use_comp else None
                    st.session_state["dim_comp_until"] = comp_until if use_comp else None
                    st.session_state["dim_yoy_since"]  = yoy_since  if use_yoy  else None
                    st.session_state["dim_yoy_until"]  = yoy_until  if use_yoy  else None
                    st.session_state.pop("df_ads", None)
                    st.session_state.pop("df_ads_comp", None)
                    st.session_state.pop("df_ads_yoy", None)
                    raw = _fetch_raw_actions(token, acct, curr_since, curr_until)
                    if raw:
                        st.session_state["raw_actions"] = raw
                except Exception as e:
                    st.error(f"API 錯誤：{e}")

    df_curr = st.session_state.get("df_curr")
    df_comp = st.session_state.get("df_comp")
    df_yoy  = st.session_state.get("df_yoy")

else:
    load_fn = load_google_csv if platform_sel == "Google" else load_meta_csv
    available_files = list_files(client_sel, channel_sel, platform_sel) if REPORT_DIR.exists() else []
    file_names = ["（選擇檔案）"] + [f.name for f in available_files]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**本期**")
        curr_sel = st.selectbox("本期報表", file_names, key="curr")
    with col2:
        st.markdown("**對比期（WoW/MoM）**")
        comp_sel = st.selectbox("對比期報表", file_names, key="comp")
    with col3:
        st.markdown("**去年同期（YoY）**")
        yoy_sel = st.selectbox("YoY 報表", file_names, key="yoy")

    with st.expander("📂 或手動上傳 CSV"):
        upload_curr = st.file_uploader("本期 CSV", type=["csv"], key="u_curr")
        upload_comp = st.file_uploader("對比期 CSV", type=["csv"], key="u_comp")
        upload_yoy  = st.file_uploader("YoY CSV", type=["csv"], key="u_yoy")

    is_google = platform_sel == "Google"

    def get_df(sel_name, upload_file):
        if upload_file is not None:
            if is_google:
                content = upload_file.read().decode("utf-16")
                lines = content.splitlines()
                header_idx = next(
                    (i for i, l in enumerate(lines) if "費用" in l and "點擊" in l), None
                )
                if header_idx is None:
                    return pd.DataFrame()
                header = [c.strip() for c in lines[header_idx].split("\t")]
                rows = []
                for line in lines[header_idx + 1:]:
                    parts = [c.strip() for c in line.split("\t")]
                    if len(parts) < 3 or parts[0].startswith("總計"):
                        continue
                    rows.append(parts[: len(header)])
                return pd.DataFrame(rows, columns=header)
            else:
                return pd.read_csv(upload_file, encoding="utf-8", thousands=",")
        if sel_name and sel_name != "（選擇檔案）":
            return load_fn(REPORT_DIR / sel_name)
        return None

    df_curr = get_df(curr_sel if curr_sel != "（選擇檔案）" else None, upload_curr)
    df_comp = get_df(comp_sel if comp_sel != "（選擇檔案）" else None, upload_comp)
    df_yoy  = get_df(yoy_sel if yoy_sel != "（選擇檔案）" else None, upload_yoy)

# ── 指標展示 ─────────────────────────────────────────────

st.divider()

if df_curr is not None and not df_curr.empty:
    if platform_sel == "Google":
        curr_m = {"Google": calc_google_metrics(df_curr)}
        comp_m = {"Google": calc_google_metrics(df_comp)} if df_comp is not None else None
        yoy_m  = {"Google": calc_google_metrics(df_yoy)}  if df_yoy is not None else None

        st.subheader("📈 Google Ads 關鍵指標")
        g = curr_m["Google"]
        g_comp = comp_m["Google"] if comp_m else {}
        g_yoy  = yoy_m["Google"] if yoy_m else {}

        rows = []
        for metric, style, hib in [
            ("花費", "currency", True), ("ROAS", "roas", True),
            ("轉換", "count", True), ("CPA", "currency", False),
            ("CTR", "pct", True), ("CPC", "currency", False)
        ]:
            val = g.get(metric, 0)
            row = {"指標": metric, "實際數值": fmt_val(val, style)}
            if g_comp:
                row["WoW"] = fmt_change(pct_change(val, g_comp.get(metric, 0)), hib)
            if g_yoy:
                row["YoY"] = fmt_change(pct_change(val, g_yoy.get(metric, 0)), hib)
            rows.append(row)
        st.write(pd.DataFrame(rows).to_html(escape=False, index=False), unsafe_allow_html=True)

    else:
        curr_m = calc_meta_metrics(df_curr)
        comp_m = calc_meta_metrics(df_comp) if df_comp is not None else None
        yoy_m  = calc_meta_metrics(df_yoy)  if df_yoy is not None else None

        st.subheader("📈 Meta Ads 關鍵指標（ATL / BTL）")
        st.write(build_table_html(curr_m, comp_m, yoy_m), unsafe_allow_html=True)

        btl = curr_m.get("BTL", {})
        atl = curr_m.get("ATL", {})
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("ATL 花費", f"${atl.get('花費', 0):,.0f}")
        c2.metric("BTL ROAS", f"{btl.get('ROAS', 0):.2f}")
        c3.metric("BTL CPA", f"${btl.get('CPA', 0):,.0f}")
        c4.metric("BTL 購買次數", f"{btl.get('購買次數', 0):.0f}")

        if btl:
            comp_btl = comp_m.get("BTL", {}) if comp_m else {}
            yoy_btl  = yoy_m.get("BTL",  {}) if yoy_m  else {}

            def funnel_delta(curr_val, comp_val, yoy_val, higher_is_better=True):
                parts = []
                if comp_val:
                    c = pct_change(curr_val, comp_val)
                    if c is not None:
                        parts.append(f"WoW {'+' if c>=0 else ''}{c:.1f}%")
                if yoy_val:
                    c = pct_change(curr_val, yoy_val)
                    if c is not None:
                        parts.append(f"YoY {'+' if c>=0 else ''}{c:.1f}%")
                return " | ".join(parts) if parts else None

            def funnel_color(curr_val, comp_val, higher_is_better=True):
                if not comp_val:
                    return "off"
                c = pct_change(curr_val, comp_val)
                if c is None:
                    return "off"
                good = (c >= 0 and higher_is_better) or (c < 0 and not higher_is_better)
                return "normal" if good else "inverse"

            st.markdown("#### 🛒 BTL 轉換漏斗")
            r1, r2, r3, r4, r5 = st.columns(5)
            atc  = btl.get('加購次數', 0)
            cost = btl.get('購物車成本', 0)
            cr   = btl.get('點擊到成交率', 0)
            acr  = btl.get('點擊到購物車率', 0)
            c2p  = btl.get('購物車到成交率', 0)
            r1.metric("購物車次數",    f"{atc:.0f}",   delta=funnel_delta(atc,  comp_btl.get('加購次數',0),      yoy_btl.get('加購次數',0)),      delta_color=funnel_color(atc,  comp_btl.get('加購次數',0)))
            r2.metric("購物車成本",    f"${cost:,.0f}", delta=funnel_delta(cost, comp_btl.get('購物車成本',0),    yoy_btl.get('購物車成本',0),  False), delta_color=funnel_color(cost, comp_btl.get('購物車成本',0), False))
            r3.metric("點擊→成交率",   f"{cr:.2f}%",   delta=funnel_delta(cr,   comp_btl.get('點擊到成交率',0),  yoy_btl.get('點擊到成交率',0)),  delta_color=funnel_color(cr,   comp_btl.get('點擊到成交率',0)))
            r4.metric("點擊→購物車率", f"{acr:.2f}%",  delta=funnel_delta(acr,  comp_btl.get('點擊到購物車率',0),yoy_btl.get('點擊到購物車率',0)), delta_color=funnel_color(acr,  comp_btl.get('點擊到購物車率',0)))
            r5.metric("購物車→成交率", f"{c2p:.2f}%",  delta=funnel_delta(c2p,  comp_btl.get('購物車到成交率',0),yoy_btl.get('購物車到成交率',0)), delta_color=funnel_color(c2p,  comp_btl.get('購物車到成交率',0)))

    raw_actions = st.session_state.get("raw_actions")
    if raw_actions:
        with st.expander("🔍 Debug：API 回傳的所有 Action Types（找不到數據時用）"):
            df_debug = pd.DataFrame(list(raw_actions.items()), columns=["Action Type", "數值"])
            df_debug["數值"] = df_debug["數值"].astype(str)
            st.dataframe(df_debug, use_container_width=True)

    st.divider()

    st.subheader("📋 生成 Claude Prompt")
    prev_actions = st.text_area(
        "貼入上週【行動】內容（選填，有助提升分析品質）",
        height=120,
        placeholder="例如：\n• 將日預算從 $3000 調回 $8000\n• 新增 KOSE 聯慶素材進 BTL 測試",
    )

    if st.button("⚡ 生成 Prompt", type="primary"):
        prompt_text = build_prompt(
            channel_sel, curr_m, comp_m, yoy_m, prev_actions, platform_sel
        )
        st.success("✅ Prompt 已生成！複製後貼入 Claude 即可。")
        st.divider()
        st.markdown("**👇 全選複製這段 Prompt，然後點下方按鈕開啟 Claude**")
        st.code(prompt_text, language=None)
        st.link_button(
            "🤖 開啟 Claude.ai（貼上 Prompt 後送出）",
            "https://claude.ai/new",
            type="primary",
        )

    if data_source == "Meta API 自動抓取" and platform_sel == "Meta":
        st.divider()
        st.subheader("🔍 素材維度分析")

        if st.button("📊 抓取廣告層級數據", key="fetch_ad_dims"):
            token_d  = st.session_state.get("dim_token", cfg.get("meta_token", ""))
            acct_d   = st.session_state.get("dim_acct_id", selected_account_id)
            atype_d  = st.session_state.get("dim_acct_type", selected_account_type)
            since_d  = st.session_state.get("dim_since")
            until_d  = st.session_state.get("dim_until")
            if not token_d or not acct_d or since_d is None:
                st.error("請先點「從 Meta API 抓取數據」按鈕")
            else:
                with st.spinner("正在抓取廣告層級數據..."):
                    try:
                        df_ads_new = fetch_meta_ad_insights(token_d, acct_d, since_d, until_d, atype_d)
                        st.session_state["df_ads"] = df_ads_new
                        comp_s = st.session_state.get("dim_comp_since")
                        comp_u = st.session_state.get("dim_comp_until")
                        if comp_s:
                            st.session_state["df_ads_comp"] = fetch_meta_ad_insights(token_d, acct_d, comp_s, comp_u, atype_d)
                        else:
                            st.session_state.pop("df_ads_comp", None)
                        yoy_s = st.session_state.get("dim_yoy_since")
                        yoy_u = st.session_state.get("dim_yoy_until")
                        if yoy_s:
                            st.session_state["df_ads_yoy"] = fetch_meta_ad_insights(token_d, acct_d, yoy_s, yoy_u, atype_d)
                        else:
                            st.session_state.pop("df_ads_yoy", None)
                        st.success(f"✅ 抓到 {len(df_ads_new)} 個廣告")
                    except Exception as e:
                        st.error(f"API 錯誤：{e}")

        df_ads_raw  = st.session_state.get("df_ads")
        df_ads_comp = st.session_state.get("df_ads_comp")
        df_ads_yoy  = st.session_state.get("df_ads_yoy")
        if df_ads_raw is not None and not df_ads_raw.empty:
            df_ads      = enrich_ad_dims(df_ads_raw)
            df_ads_c    = enrich_ad_dims(df_ads_comp) if df_ads_comp is not None and not df_ads_comp.empty else None
            df_ads_y    = enrich_ad_dims(df_ads_yoy)  if df_ads_yoy  is not None and not df_ads_yoy.empty  else None

            with st.expander("🔽 篩選條件（選擇後自動更新所有維度表格）"):
                fc = st.columns(6)
                dim_filter_labels = [("ATL/BTL", "ATL/BTL"), ("受眾", "受眾／新舊客"), ("活動類型", "活動類型"),
                                     ("格式", "素材格式"), ("品類", "品類"), ("素材類型", "素材類型")]
                filters = {}
                for i, (dim, lbl) in enumerate(dim_filter_labels):
                    opts = sorted(df_ads[dim].dropna().unique().tolist())
                    filters[dim] = fc[i].multiselect(lbl, opts, default=[], key=f"filter_{dim}")

            def apply_filters(df):
                if df is None:
                    return None
                for dim, sel in filters.items():
                    if sel:
                        df = df[df[dim].isin(sel)]
                return df

            df_f  = apply_filters(df_ads)
            df_fc = apply_filters(df_ads_c)
            df_fy = apply_filters(df_ads_y)

            active_filters = [f"{lbl}={','.join(filters[dim])}" for dim, lbl in dim_filter_labels if filters[dim]]
            if active_filters:
                st.caption(f"篩選：{' | '.join(active_filters)}　→　{len(df_f)} 個廣告（共 {len(df_ads)} 個）")
            else:
                st.caption(f"共 {len(df_f)} 個廣告" + ("　含 WoW 對比" if df_ads_c is not None else "") + ("　含 YoY 對比" if df_ads_y is not None else ""))

            for dim_col, label in [
                ("ATL/BTL", "📊 ATL/BTL"),
                ("受眾",    "👥 受眾／新舊客"),
                ("活動類型","📅 活動類型"),
                ("格式",    "🖼️ 素材格式"),
                ("品類",    "📦 品類"),
                ("素材類型","🎭 素材類型"),
            ]:
                st.markdown(f"**{label}**")
                tbl = build_dim_table(df_f, dim_col, df_fc, df_fy)
                if not tbl.empty:
                    st.write(tbl.to_html(escape=False, index=False), unsafe_allow_html=True)
                else:
                    st.caption("無資料")
                st.markdown("")

else:
    if data_source == "Meta API 自動抓取":
        st.info("👆 設定日期範圍後，點「從 Meta API 抓取數據」按鈕")
    else:
        st.info("👆 請先選擇「本期報表」或上傳 CSV 檔案")

st.markdown("---")
st.caption("Powered by Claude Sonnet 4.6 · 毛孩時代 & 御熹堂廣告週報自動化")
