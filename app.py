import streamlit as st
import pandas as pd
import requests
import json
import math
import time
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit.components.v1 as components
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, ColumnsAutoSizeMode

# AgGrid 手機橫向滑動 CSS（注入進 iframe 內部）
AGGRID_SCROLL_CSS = {
    ".ag-root-wrapper":      {"overflow-x": "auto !important", "overflow-y": "hidden !important"},
    ".ag-root":              {"width": "max-content !important", "min-width": "100%"},
    ".ag-body-horizontal-scroll": {"display": "block !important"},
    ".ag-center-cols-viewport":   {"overflow-x": "auto !important"},
}

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
            if "account_target_roas" in st.secrets:
                cfg["account_target_roas"] = dict(st.secrets["account_target_roas"])
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
        pass  # Streamlit Cloud 環境下無法永久寫檔，設定改在 Secrets 管理

# ── Meta API ──────────────────────────────────────────────

# action type 對照表：一般帳號 vs CPAS 帳號
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
    """行銷活動層級彙總所有 action types + 嘗試 CPAS 獨立欄位，用於偵錯。"""
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"
    # 嘗試 CPAS 可能用到的獨立欄位
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
        # 彙總 actions
        for item in data.get("data", []):
            for a in item.get("actions", []):
                atype = a["action_type"]
                all_types[atype] = all_types.get(atype, 0) + float(a["value"])
            for a in item.get("action_values", []):
                atype = f"[value] {a['action_type']}"
                all_types[atype] = all_types.get(atype, 0) + float(a["value"])
            # 顯示獨立欄位是否有值
            for field in ["purchase_roas","website_purchase_roas","omni_purchase",
                          "omni_add_to_cart","catalog_segment_value","catalog_segment_actions"]:
                if item.get(field) not in (None, "", []):
                    all_types[f"[field] {field}"] = str(item.get(field))
        return all_types
    except Exception as e:
        return {"error": str(e)}

def fetch_account_reach(access_token, ad_account_id, since, until):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"
    params = {
        "level": "account",
        "fields": "reach",
        "time_range": json.dumps({"since": str(since), "until": str(until)}),
        "access_token": access_token,
    }
    try:
        data = requests.get(url, params=params, timeout=15).json()
        rows = data.get("data", [])
        return int(rows[0]["reach"]) if rows else 0
    except Exception:
        return 0

def fetch_meta_insights(access_token, ad_account_id, since, until, account_type="general"):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"

    if account_type == "cpas":
        fields = (
            "campaign_name,spend,impressions,reach,inline_link_clicks,"
            "actions,action_values,"
            "catalog_segment_actions,catalog_segment_value"
        )
    else:
        fields = "campaign_name,spend,impressions,reach,inline_link_clicks,actions,action_values"

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
            "觸及人數": safe_float(item.get("reach")),
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
        reach = sub["觸及人數"].sum() if "觸及人數" in sub.columns else 0
        revenue = sub["購買轉換值"].sum()
        purchases = sub["購買次數"].sum()
        atc = sub["加到購物車次數"].sum()
        result[t] = {
            "花費": spend,
            "點擊": clicks,
            "CPC": spend / clicks if clicks > 0 else 0,
            "CTR": clicks / impr * 100 if impr > 0 else 0,
            "ROAS": revenue / spend if spend > 0 else 0,
            "廣告收益": revenue,
            "CPA": spend / purchases if purchases > 0 else 0,
            "AOV": revenue / purchases if purchases > 0 else 0,
            "觸及人數": reach,
            "觸及成本": spend / reach * 1000 if reach > 0 else 0,
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

def _delta_str(curr_val, ref_val, style):
    if curr_val is None or ref_val is None or ref_val == 0:
        return ""
    delta = curr_val - ref_val
    direction = "增" if delta > 0 else "減"
    return f"，{direction} {fmt_val(abs(delta), style)}"

def _fmt_chg(v, higher_is_better=True, ref_val=None, ref_style=None, ref_label=None, curr_val=None):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    txt = f"{sign}{v:.1f}%"
    if ref_val is not None and ref_style is not None:
        ref_str = fmt_val(ref_val, ref_style)
        lbl = f"{ref_label}: " if ref_label else ""
        txt += f" （{lbl}{ref_str}{_delta_str(curr_val, ref_val, ref_style)}）"
    return txt

def _chg_color(v, hib, ref_val=None, ref_style=None, ref_label=None, curr_val=None):
    """根據變化率與指標方向回傳 HTML 顏色 span"""
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    txt = f"{sign}{v:.1f}%"
    is_good = (v >= 0 and hib) or (v < 0 and not hib)
    color = "#27ae60" if is_good else "#e74c3c"
    result = f'<span style="color:{color};font-weight:bold">{txt}</span>'
    if ref_val is not None and ref_style is not None:
        ref_str = fmt_val(ref_val, ref_style)
        lbl = f"{ref_label}: " if ref_label else ""
        detail = f"（{lbl}{ref_str}{_delta_str(curr_val, ref_val, ref_style)}）"
        result += f'<br class="ref-br"><span class="ref-info" style="color:#999;font-size:12px;font-weight:normal">{detail}</span>'
    return result

def build_table_html(curr_m, comp_m, mom_m, yoy_m, comp_label="前期", comp_header=None):
    rows_def = [
        ("ATL", "花費",    "currency", True),
        ("ATL", "點擊",    "count",    True),
        ("ATL", "CPC",     "currency", False),
        ("BTL", "花費",    "currency", True),
        ("BTL", "ROAS",    "roas",     True),
        ("BTL", "廣告收益", "currency", True),
        ("BTL", "CPA",     "currency", False),
        ("BTL", "AOV",     "currency", True),
    ]
    _comp_hdr = comp_header or comp_label
    cols = [_comp_hdr if comp_m else None, "MoM" if mom_m else None, "YoY" if yoy_m else None]
    chg_headers = "".join(f"<th>{c}</th>" for c in cols if c)
    header = f"<tr><th class='s1'>類型</th><th class='s2'>指標 / 數值</th>{chg_headers}</tr>"

    def _total_spend(m):
        return (m.get("ATL", {}).get("花費", 0) or 0) + (m.get("BTL", {}).get("花費", 0) or 0)
    def _total_clicks(m):
        return (m.get("ATL", {}).get("點擊", 0) or 0) + (m.get("BTL", {}).get("點擊", 0) or 0)
    def _total_roas(m):
        s = _total_spend(m)
        rev = m.get("BTL", {}).get("廣告收益", 0) or 0
        return rev / s if s > 0 else 0
    def _total_rev(m):
        return m.get("BTL", {}).get("廣告收益", 0) or 0
    def _total_reach(m):
        if m.get("_account_reach"):
            return m["_account_reach"]
        return (m.get("ATL", {}).get("觸及人數", 0) or 0) + (m.get("BTL", {}).get("觸及人數", 0) or 0)
    def _total_cpr(m):
        s = _total_spend(m); r = _total_reach(m)
        return s / r * 1000 if r > 0 else 0

    body = ""
    prev_type = None
    for t, metric, style, hib in rows_def:
        val = curr_m.get(t, {}).get(metric, 0)
        row = "<tr>"
        if t != prev_type:
            span = sum(1 for r in rows_def if r[0] == t)
            row += f'<td rowspan="{span}" class="s1" style="font-weight:700;text-align:center;vertical-align:middle">{t}</td>'
            prev_type = t
        row += f"<td class='s2'>{metric}<br><span style='font-weight:normal;color:#333;font-size:13px'>{fmt_val(val, style)}</span></td>"
        if comp_m is not None:
            _cv = comp_m.get(t,{}).get(metric,0)
            row += f"<td class='chg-cell'>{_chg_color(pct_change(val, _cv), hib, _cv, style, comp_label, curr_val=val)}</td>"
        if mom_m is not None:
            _mv = mom_m.get(t,{}).get(metric,0)
            row += f"<td class='chg-cell'>{_chg_color(pct_change(val, _mv), hib, _mv, style, '上月', curr_val=val)}</td>"
        if yoy_m is not None:
            _yv = yoy_m.get(t,{}).get(metric,0)
            row += f"<td class='chg-cell'>{_chg_color(pct_change(val, _yv), hib, _yv, style, '去年', curr_val=val)}</td>"
        row += "</tr>"
        body += row

    # 總計行：花費 > 點擊 > ROAS > 廣告收益 > 觸及成本
    total_rows = [
        ("總計", "花費",     "currency", True,  _total_spend),
        ("總計", "點擊",     "count",    True,  _total_clicks),
        ("總計", "ROAS",     "roas",     True,  _total_roas),
        ("總計", "廣告收益", "currency", True,  _total_rev),
        ("總計", "觸及成本", "currency", False, _total_cpr),
    ]
    n_total = len(total_rows)
    for i, (t, metric, style, hib, fn) in enumerate(total_rows):
        val = fn(curr_m)
        td_style = "font-weight:700;text-align:center;vertical-align:middle;border-top:2px solid #bbb"
        row = "<tr style='background:#f8f8f8'>"
        if i == 0:
            row += f'<td rowspan="{n_total}" class="s1" style="{td_style}">{t}</td>'
        row += f"<td class='s2' style='font-weight:600'>{metric}<br><span style='font-weight:normal;color:#333;font-size:13px'>{fmt_val(val, style)}</span></td>"
        if comp_m is not None:
            _cv = fn(comp_m)
            row += f"<td class='chg-cell'>{_chg_color(pct_change(val, _cv), hib, _cv, style, comp_label, curr_val=val)}</td>"
        if mom_m is not None:
            _mv = fn(mom_m)
            row += f"<td class='chg-cell'>{_chg_color(pct_change(val, _mv), hib, _mv, style, '上月', curr_val=val)}</td>"
        if yoy_m is not None:
            _yv = fn(yoy_m)
            row += f"<td class='chg-cell'>{_chg_color(pct_change(val, _yv), hib, _yv, style, '去年', curr_val=val)}</td>"
        row += "</tr>"
        body += row

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  body {{ margin:0; padding:4px; font-family:sans-serif; font-size:14px; }}
  .scroll-wrap {{ overflow-x:auto; -webkit-overflow-scrolling:touch; }}
  table {{ border-collapse:collapse; min-width:600px; width:100%; }}
  th {{ padding:8px 10px; text-align:left; border-bottom:2px solid #ccc; color:#555; font-size:12px; white-space:nowrap; }}
  td {{ padding:7px 10px; border-bottom:1px solid #e0e0e0; white-space:nowrap; }}
  .chg-cell {{ text-align:right; min-width:105px; white-space:normal; }}
  .s1 {{ position:sticky; left:0; z-index:2; background:#fff; min-width:44px; }}
  .s2 {{ position:sticky; left:44px; z-index:2; background:#fff; min-width:90px; box-shadow:2px 0 4px rgba(0,0,0,0.07); white-space:normal; }}
  tr:hover .s1, tr:hover .s2 {{ background:#f5f5f5; }}
  @media (min-width:520px) {{
    body {{ font-size:15px; }}
    th {{ padding:10px 16px; font-size:13px; }}
    td {{ padding:9px 16px; }}
    .chg-cell {{ min-width:160px; white-space:nowrap; }}
    .ref-br {{ display:none; }}
    .ref-info {{ display:inline; margin-left:5px; font-size:14px; }}
  }}
</style></head><body>
<div class="scroll-wrap"><table>{header}{body}</table></div>
</body></html>"""

def build_table_df(curr_m, comp_m, mom_m, yoy_m, comp_label="前期"):
    rows_def = [
        ("ATL", "花費",    "currency", True),
        ("ATL", "點擊",    "count",    True),
        ("ATL", "CPC",     "currency", False),
        ("BTL", "花費",    "currency", True),
        ("BTL", "ROAS",    "roas",     True),
        ("BTL", "廣告收益", "currency", True),
        ("BTL", "CPA",     "currency", False),
        ("BTL", "AOV",     "currency", True),
    ]
    def _total_spend(m):
        return (m.get("ATL", {}).get("花費", 0) or 0) + (m.get("BTL", {}).get("花費", 0) or 0)
    def _total_clicks(m):
        return (m.get("ATL", {}).get("點擊", 0) or 0) + (m.get("BTL", {}).get("點擊", 0) or 0)
    def _total_roas(m):
        s = _total_spend(m)
        rev = m.get("BTL", {}).get("廣告收益", 0) or 0
        return rev / s if s > 0 else 0
    def _total_rev(m):
        return m.get("BTL", {}).get("廣告收益", 0) or 0
    def _total_reach(m):
        if m.get("_account_reach"):
            return m["_account_reach"]
        return (m.get("ATL", {}).get("觸及人數", 0) or 0) + (m.get("BTL", {}).get("觸及人數", 0) or 0)
    def _total_cpr(m):
        s = _total_spend(m); r = _total_reach(m)
        return s / r * 1000 if r > 0 else 0

    result = []
    for t, metric, style, hib in rows_def:
        val = curr_m.get(t, {}).get(metric, 0)
        row = {"類型": t, "指標": metric, "實際數值": fmt_val(val, style)}
        if comp_m is not None:
            _cv = comp_m.get(t, {}).get(metric, 0)
            row["WoW"] = _fmt_chg(pct_change(val, _cv), hib, _cv, style, comp_label, curr_val=val)
        if mom_m is not None:
            _mv = mom_m.get(t, {}).get(metric, 0)
            row["MoM"] = _fmt_chg(pct_change(val, _mv), hib, _mv, style, "上月", curr_val=val)
        if yoy_m is not None:
            _yv = yoy_m.get(t, {}).get(metric, 0)
            row["YoY"] = _fmt_chg(pct_change(val, _yv), hib, _yv, style, "去年", curr_val=val)
        result.append(row)

    for metric, style, hib, fn in [
        ("花費",     "currency", True,  _total_spend),
        ("點擊",     "count",    True,  _total_clicks),
        ("ROAS",     "roas",     True,  _total_roas),
        ("廣告收益", "currency", True,  _total_rev),
        ("觸及成本", "currency", False, _total_cpr),
    ]:
        val = fn(curr_m)
        row = {"類型": "總計", "指標": metric, "實際數值": fmt_val(val, style)}
        if comp_m is not None:
            _cv = fn(comp_m)
            row["WoW"] = _fmt_chg(pct_change(val, _cv), hib, _cv, style, comp_label, curr_val=val)
        if mom_m is not None:
            _mv = fn(mom_m)
            row["MoM"] = _fmt_chg(pct_change(val, _mv), hib, _mv, style, "上月", curr_val=val)
        if yoy_m is not None:
            _yv = fn(yoy_m)
            row["YoY"] = _fmt_chg(pct_change(val, _yv), hib, _yv, style, "去年", curr_val=val)
        result.append(row)

    return pd.DataFrame(result)

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

def build_prompt(channel, curr_m, comp_m, mom_m, yoy_m, prev_actions, platform="Meta"):
    btl = curr_m.get("BTL", {})
    atl = curr_m.get("ATL", {})

    def change_str(curr_dict, comp_dict, mom_dict, yoy_dict, key):
        parts = []
        if comp_dict:
            c = pct_change(curr_dict.get(key, 0), comp_dict.get(key, 0))
            if c is not None:
                parts.append(f"WoW {'+' if c>=0 else ''}{c:.1f}%")
        if mom_dict:
            c = pct_change(curr_dict.get(key, 0), mom_dict.get(key, 0))
            if c is not None:
                parts.append(f"MoM {'+' if c>=0 else ''}{c:.1f}%")
        if yoy_dict:
            c = pct_change(curr_dict.get(key, 0), yoy_dict.get(key, 0))
            if c is not None:
                parts.append(f"YoY {'+' if c>=0 else ''}{c:.1f}%")
        return "、".join(parts) if parts else "無對比"

    comp_atl = comp_m.get("ATL") if comp_m else None
    comp_btl = comp_m.get("BTL") if comp_m else None
    mom_atl  = mom_m.get("ATL")  if mom_m  else None
    mom_btl  = mom_m.get("BTL")  if mom_m  else None
    yoy_atl  = yoy_m.get("ATL")  if yoy_m  else None
    yoy_btl  = yoy_m.get("BTL")  if yoy_m  else None

    data_summary = f"""【{channel} 本期廣告數據 - {platform}】

ATL（流量型）：
- 花費：${atl.get('花費', 0):,.0f}（{change_str(atl, comp_atl, mom_atl, yoy_atl, '花費')}）
- CPC：${atl.get('CPC', 0):.1f}（{change_str(atl, comp_atl, mom_atl, yoy_atl, 'CPC')}）

BTL（轉換型）：
- 花費：${btl.get('花費', 0):,.0f}（{change_str(btl, comp_btl, mom_btl, yoy_btl, '花費')}）
- ROAS：{btl.get('ROAS', 0):.2f}（{change_str(btl, comp_btl, mom_btl, yoy_btl, 'ROAS')}）
- 廣告收益：${btl.get('廣告收益', 0):,.0f}（{change_str(btl, comp_btl, mom_btl, yoy_btl, '廣告收益')}）
- CPA：${btl.get('CPA', 0):,.0f}（{change_str(btl, comp_btl, mom_btl, yoy_btl, 'CPA')}）
- AOV：${btl.get('AOV', 0):,.0f}（{change_str(btl, comp_btl, mom_btl, yoy_btl, 'AOV')}）"""

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

def mom_range(since, until):
    def sub_month(d):
        month = d.month - 1 if d.month > 1 else 12
        year = d.year if d.month > 1 else d.year - 1
        last_day = calendar.monthrange(year, month)[1]
        return date(year, month, min(d.day, last_day))
    return sub_month(since), sub_month(until)

# ── UI ───────────────────────────────────────────────────

st.markdown("## 📊 廣告週報產生器")
st.markdown("Meta API 直抓 或 上傳 CSV，自動計算 WoW/YoY 並生成觀察與行動建議。")
st.divider()

cfg = load_config()

# Token 到期提醒
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
            return  # 永不過期（系統用戶 token）
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
    """從帳戶名稱解析 client / channel，例如「毛孩時代 蝦皮」→ ('毛孩時代', '蝦皮')"""
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
        else:
            # 支援 2505｜常態｜蔓越莓 格式：｜分隔中含 ACTIVITY_TYPE
            sub = [s.strip() for s in part.split('｜')]
            for j, s in enumerate(sub):
                if s in ACTIVITY_TYPES:
                    activity_type = s
                    found_activity_idx = i
                    # 品類取 ACTIVITY_TYPE 後一個 ｜ 段（非純數字）
                    if j + 1 < len(sub) and sub[j+1] and not sub[j+1][:4].isdigit():
                        category = sub[j+1]
                    break
    for i, part in enumerate(parts):
        if any(kw in part for kw in FORMAT_KEYWORDS):
            format_type = part
            found_format_idx = i
            break
    if category == "未知":
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

def fetch_meta_ad_insights(access_token, ad_account_id, since, until, account_type="general", level="ad"):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"
    name_field = "adset_name" if level == "adset" else "ad_name"
    if account_type == "cpas":
        fields = f"campaign_name,{name_field},spend,impressions,inline_link_clicks,catalog_segment_actions,catalog_segment_value"
    else:
        fields = f"campaign_name,{name_field},spend,impressions,inline_link_clicks,actions,action_values"
    params = {
        "level": level,
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
        row_name = item.get(name_field, "")
        rows.append({
            "行銷活動名稱": item.get("campaign_name", ""),
            "廣告名稱": row_name,
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

def build_dim_table(df, dim_col, df_comp=None, df_mom=None, df_yoy=None):
    curr = _agg_by_dim(df, dim_col)
    comp = _agg_by_dim(df_comp, dim_col) if df_comp is not None and not df_comp.empty else {}
    mom  = _agg_by_dim(df_mom,  dim_col) if df_mom  is not None and not df_mom.empty  else {}
    yoy  = _agg_by_dim(df_yoy,  dim_col) if df_yoy  is not None and not df_yoy.empty  else {}
    has_wow = bool(comp)
    has_mom = bool(mom)
    has_yoy = bool(yoy)

    rows = []
    for val, m in sorted(curr.items(), key=lambda x: -x[1]["花費"]):
        c = comp.get(val, {})
        mo = mom.get(val, {})
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
        if has_mom:
            row["花費 MoM"]  = fmt_change(pct_change(m["花費"], mo.get("花費", 0)),  True)  if mo else "-"
            row["ROAS MoM"] = fmt_change(pct_change(m["ROAS"], mo.get("ROAS", 0)),  True)  if mo else "-"
            row["CPA MoM"]  = fmt_change(pct_change(m["CPA"],  mo.get("CPA",  0)),  False) if mo else "-"
        if has_yoy:
            row["花費 YoY"]  = fmt_change(pct_change(m["花費"], y.get("花費", 0)),  True)  if y else "-"
            row["ROAS YoY"] = fmt_change(pct_change(m["ROAS"], y.get("ROAS", 0)),  True)  if y else "-"
            row["CPA YoY"]  = fmt_change(pct_change(m["CPA"],  y.get("CPA",  0)),  False) if y else "-"
        rows.append(row)
    return pd.DataFrame(rows)

def fetch_campaigns_with_budget(access_token, ad_account_id):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/campaigns"
    params = {
        "fields": "id,name,status,daily_budget,lifetime_budget,budget_rebalance_flag,smart_promotion_type",
        "filtering": json.dumps([{"field": "effective_status", "operator": "IN", "value": ["ACTIVE", "PAUSED"]}]),
        "access_token": access_token,
        "limit": 200,
    }
    all_camps = []
    while url:
        resp = requests.get(url, params=params, timeout=30).json()
        if "error" in resp:
            raise Exception(resp["error"].get("message", str(resp["error"])))
        all_camps.extend(resp.get("data", []))
        url    = resp.get("paging", {}).get("next")
        params = {}   # next URL 已含所有參數，清空避免重複
    return all_camps

def _fmt_roas(v):
    """安全格式化 ROAS：None/NaN/Inf 都回傳 '—'"""
    if v is None:
        return "—"
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return "—"
        return f"{f:.1f}"
    except (TypeError, ValueError):
        return "—"

def _end_ts(s):
    """排程結束時間 → Unix timestamp（支援整數字串和 ISO 格式）"""
    v = s.get("time_end", 0)
    try:
        return int(v)
    except (ValueError, TypeError):
        return int(datetime.strptime(str(v), "%Y-%m-%dT%H:%M:%S%z").timestamp())

def parse_meta_ts(ts_val, tz_tw):
    """Meta API 回傳 Unix 整數或 ISO 字串，統一轉成台灣時間 datetime"""
    try:
        return datetime.fromtimestamp(int(ts_val), tz=tz_tw)
    except (ValueError, TypeError):
        dt = datetime.strptime(str(ts_val), "%Y-%m-%dT%H:%M:%S%z")
        return dt.astimezone(tz_tw)

def _batch_fetch_all_schedules(token, camps, now_ts):
    """Batch API 一次抓所有活動的排程（152 活動 → 4 HTTP calls），回傳 del_scheds dict。"""
    BATCH_SIZE = 50
    sched_field = "id,time_start,time_end,budget_value,budget_value_type,status"
    result = {}
    for chunk_start in range(0, len(camps), BATCH_SIZE):
        chunk = camps[chunk_start: chunk_start + BATCH_SIZE]
        batch_payload = [
            {"method": "GET",
             "relative_url": f"{c['id']}/budget_schedules?fields={sched_field}&limit=50&access_token={token}"}
            for c in chunk
        ]
        try:
            batch_resp = requests.post(
                "https://graph.facebook.com/v25.0/",
                data={"access_token": token, "batch": json.dumps(batch_payload)},
                timeout=30,
            ).json()
        except Exception:
            continue
        for c, item in zip(chunk, batch_resp):
            if not isinstance(item, dict) or item.get("code") != 200:
                continue
            try:
                body  = json.loads(item.get("body", "{}"))
                scheds = body.get("data", [])
                if scheds:
                    result[c["id"]] = {
                        "campaign": c,
                        "active":  [s for s in scheds if _end_ts(s) > now_ts],
                        "expired": [s for s in scheds if _end_ts(s) <= now_ts],
                    }
            except Exception:
                continue
    return result

def fetch_campaign_schedules(access_token, campaign_id):
    """取得單一活動的所有預算排程"""
    resp = requests.get(
        f"https://graph.facebook.com/v25.0/{campaign_id}/budget_schedules",
        params={
            "fields": "id,time_start,time_end,budget_value,budget_value_type,status",
            "access_token": access_token,
            "limit": 50,
        },
        timeout=15,
    ).json()
    if "error" in resp:
        raise Exception(resp["error"].get("message", str(resp["error"])))
    return resp.get("data", [])

def delete_budget_schedule(access_token, schedule_id):
    """刪除單一預算排程"""
    resp = requests.delete(
        f"https://graph.facebook.com/v25.0/{schedule_id}",
        params={"access_token": access_token},
        timeout=15,
    ).json()
    return resp

def update_budget_schedule(access_token, schedule_id, new_pct, campaign_id=None, time_start=None, time_end=None):
    """修改現有預算排程：POST 相同時段給 campaign，失敗不動舊排程"""
    def _to_ts(v):
        try:
            return int(v)
        except (ValueError, TypeError):
            return int(datetime.strptime(str(v), "%Y-%m-%dT%H:%M:%S%z").timestamp())

    if not time_start or not time_end:
        return {"error": {"message": "缺少排程時段資訊，請重新載入後再試"}}

    ts_start = _to_ts(time_start)
    ts_end   = _to_ts(time_end)
    now_ts   = int(datetime.now(timezone.utc).timestamp())

    if ts_end <= now_ts:
        return {"error": {"message": "排程結束時間已過，無法修改，請重新新增排程"}}

    if not campaign_id:
        return {"error": {"message": "缺少 campaign_id，無法重建排程"}}

    # 剩餘時間 < 3 小時 → Meta 建不回來，直接擋住保留舊排程
    remaining_min = int((ts_end - now_ts) / 60)
    if ts_end - now_ts < 3 * 3600:
        return {"error": {"message": f"Meta 限制：距排程結束僅剩 {remaining_min} 分鐘（需 ≥ 3 小時才能修改）。舊排程保留中。"}}

    # 剩餘時間足夠 → 先刪再建
    delete_budget_schedule(access_token, schedule_id)
    return create_budget_schedule(access_token, campaign_id, ts_start, ts_end, new_pct)

def create_budget_schedule(access_token, campaign_id, time_start, time_end, pct_increase):
    # 若開始時間已過，自動推到下一個 15 分鐘整點
    TZ_TAIPEI = timezone(timedelta(hours=8))
    now_ts = int(datetime.now(TZ_TAIPEI).timestamp())
    if int(time_start) <= now_ts:
        now_tw = datetime.now(tz=TZ_TAIPEI).replace(second=0, microsecond=0)
        rem = now_tw.minute % 15
        time_start = int((now_tw + timedelta(minutes=(15 - rem) if rem else 15)).timestamp())

    # 結束時間已過 → 拒絕，避免送給 Meta 無效參數
    if int(time_end) <= now_ts:
        return {"error": {"message": "排程結束時間已過，請重新選擇時段"}}

    # 結束時間 ≤ 調整後的開始時間 → 時段無效
    if int(time_end) <= int(time_start):
        return {"error": {"message": "排程結束時間必須晚於開始時間，請重新選擇時段"}}

    # 先刪除時段重疊的舊排程（避免堆疊）；若 API 不支援則跳過
    try:
        existing = fetch_campaign_schedules(access_token, campaign_id)
    except Exception:
        existing = []
    for s in existing:
        try:
            s_start = int(s["time_start"]) if str(s["time_start"]).isdigit() else int(datetime.strptime(str(s["time_start"]), "%Y-%m-%dT%H:%M:%S%z").timestamp())
            s_end   = int(s["time_end"])   if str(s["time_end"]).isdigit()   else int(datetime.strptime(str(s["time_end"]),   "%Y-%m-%dT%H:%M:%S%z").timestamp())
        except Exception:
            continue
        # 只要有重疊就刪
        if s_start < int(time_end) and s_end > int(time_start):
            delete_budget_schedule(access_token, s["id"])

    # MULTIPLIER = 增加百分比：300 = +300%（Meta 花費達 4x）；負數 = 減碼
    budget_value = int(pct_increase)
    spec = [{
        "time_start": int(time_start),
        "time_end": int(time_end),
        "budget_value": budget_value,
        "budget_value_type": "MULTIPLIER",
    }]
    payload = {
        "access_token": access_token,
        "budget_schedule_specs": json.dumps(spec),
    }

    # 先嘗試 campaign 層級
    result = requests.post(f"https://graph.facebook.com/v25.0/{campaign_id}", data=payload, timeout=30).json()
    if "error" not in result:
        return {"success": True, "level": "campaign"}

    # error_subcode 3858090：time_end 超出活動本身排期 → 縮短至活動結束時間重試
    if result.get("error", {}).get("error_subcode") == 3858090:
        camp_info = requests.get(
            f"https://graph.facebook.com/v25.0/{campaign_id}",
            params={"fields": "stop_time,end_time,start_time", "access_token": access_token},
            timeout=15,
        ).json()
        camp_end_str = camp_info.get("stop_time") or camp_info.get("end_time")
        print(f"[DEBUG] 3858090 camp={campaign_id} camp_info={camp_info} camp_end_str={camp_end_str} time_end={time_end}")
        if camp_end_str:
            try:
                camp_end_ts = int(datetime.strptime(camp_end_str, "%Y-%m-%dT%H:%M:%S%z").timestamp())
                print(f"[DEBUG] 3858090 camp_end_ts={camp_end_ts} time_start={time_start} time_end={time_end}")
                if camp_end_ts > int(time_start):
                    spec[0]["time_end"] = camp_end_ts
                    payload2 = {**payload, "budget_schedule_specs": json.dumps(spec)}
                    result2 = requests.post(f"https://graph.facebook.com/v25.0/{campaign_id}", data=payload2, timeout=30).json()
                    print(f"[DEBUG] 3858090 truncated retry result={result2}")
                    if "error" not in result2:
                        return {"success": True, "level": "campaign", "note": f"（排程結束時間已調整為活動結束時間）"}
                    result = result2
                else:
                    return {"error": {"message": "活動排期已結束，無法建立排程"}}
            except Exception as _e:
                print(f"[DEBUG] 3858090 parse exception: {_e}")

    # 3858090 且無法縮短 → 先帶 daily_budget 重試 campaign 層（部分活動類型需要）
    if result.get("error", {}).get("error_subcode") == 3858090:
        camp_info2 = requests.get(
            f"https://graph.facebook.com/v25.0/{campaign_id}",
            params={"fields": "daily_budget", "access_token": access_token},
            timeout=15,
        ).json()
        _db2 = camp_info2.get("daily_budget")
        if _db2:
            _payload_db = {**payload, "daily_budget": _db2}
            _r_db = requests.post(f"https://graph.facebook.com/v25.0/{campaign_id}", data=_payload_db, timeout=30).json()
            print(f"[DEBUG] 3858090+daily_budget retry result={_r_db}")
            if "error" not in _r_db:
                return {"success": True, "level": "campaign", "note": "（帶 daily_budget 成功）"}

    # 3858090 且無法縮短（無結束時間）→ 嘗試 adset 層級
    if result.get("error", {}).get("error_subcode") == 3858090:
        print(f"[DEBUG] 3858090 campaign={campaign_id} err={result.get('error')}")
        _adsets_resp = requests.get(
            f"https://graph.facebook.com/v25.0/{campaign_id}/adsets",
            params={"fields": "id,name,daily_budget,lifetime_budget", "access_token": access_token, "limit": 50},
            timeout=15,
        ).json()
        print(f"[DEBUG] 3858090 adsets_resp={_adsets_resp}")
        adsets = _adsets_resp.get("data", [])
        if adsets:
            # 偵測 CBO：所有 adset 都沒有自己的預算 → budget_rebalance_flag 活動
            all_cbo = all(not a.get("daily_budget") and not a.get("lifetime_budget") for a in adsets)
            if all_cbo:
                return {"error": {"message": "此為行銷活動預算（CBO）活動，排程 API 不支援。建議改用「快速加減碼」直接調整日預算，或在 Meta 廣告管理員設定預算排程。"}}
            ok, fail, invalid = 0, [], 0
            for a in adsets:
                r = requests.post(f"https://graph.facebook.com/v25.0/{a['id']}", data=payload, timeout=30).json()
                if "error" not in r:
                    ok += 1
                elif "Invalid parameter" in r.get("error", {}).get("message", ""):
                    invalid += 1
                else:
                    fail.append(f"{a['name']}: {r['error']['message']}")
            if ok:
                msg = f"已套用至 {ok} 個廣告組合（adset 層級）"
                if fail:
                    msg += f"，{len(fail)} 個失敗"
                return {"success": True, "level": "adset", "note": f"（{msg}）"}
            if invalid == len(adsets):
                return {"error": {"message": "此活動的廣告組合不支援排程加碼（使用 lifetime 預算）。請改用活動管理員手動調整預算。"}}
            if fail:
                return {"error": {"message": " / ".join(fail)}}
        else:
            return {"error": {"message": "找不到廣告組合，無法建立排程（此活動可能需手動調整預算）"}}

    # error_subcode 3858199 / 3858175：帶著 daily_budget 再試一次（ASC 活動需要）
    if result.get("error", {}).get("error_subcode") in (3858199, 3858175):
        camp_info = requests.get(
            f"https://graph.facebook.com/v25.0/{campaign_id}",
            params={"fields": "daily_budget", "access_token": access_token},
            timeout=15,
        ).json()
        daily_budget = camp_info.get("daily_budget")
        if daily_budget:
            payload2 = {**payload, "daily_budget": daily_budget}
            result2 = requests.post(f"https://graph.facebook.com/v25.0/{campaign_id}", data=payload2, timeout=30).json()
            if "error" not in result2:
                return {"success": True, "level": "campaign"}
            result = result2  # 用新的錯誤繼續往下

        # adset 層級 fallback
        adsets = requests.get(
            f"https://graph.facebook.com/v25.0/{campaign_id}/adsets",
            params={"fields": "id,name,daily_budget,lifetime_budget", "access_token": access_token, "limit": 50},
            timeout=15,
        ).json().get("data", [])
        if not adsets:
            return {"error": {"message": "找不到任何廣告組合"}}

        ok, fail, invalid = 0, [], 0
        for a in adsets:
            r = requests.post(f"https://graph.facebook.com/v25.0/{a['id']}", data=payload, timeout=30).json()
            if "error" not in r:
                ok += 1
            elif "Invalid parameter" in r.get("error", {}).get("message", ""):
                invalid += 1
            else:
                fail.append(f"{a['name']}: {r['error']['message']}")

        if ok:
            msg = f"已套用至 {ok} 個廣告組合（adset 層級）"
            if fail:
                msg += f"；失敗 {len(fail)} 個"
            return {"success": True, "note": msg}

        err_msg = result.get("error", {}).get("message", "")
        return {"error": {"message": err_msg or (fail[0] if fail else "預算排程設定失敗")}}

    return result

def date_to_ts(d, is_start=False):
    TZ_TAIPEI = timezone(timedelta(hours=8))
    if is_start and d <= date.today():
        now = datetime.now(tz=TZ_TAIPEI).replace(second=0, microsecond=0)
        remainder = now.minute % 15
        next_15 = now + timedelta(minutes=(15 - remainder) if remainder else 15)
        return int(next_15.timestamp())
    return int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ_TAIPEI).timestamp())

def date_hour_to_ts(d, hour_str):
    """date + 'HH:MM' → Unix timestamp（台灣時間）"""
    TZ_TAIPEI = timezone(timedelta(hours=8))
    parts = hour_str.split(":")
    h, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    return int(datetime(d.year, d.month, d.day, h, m, 0, tzinfo=TZ_TAIPEI).timestamp())

# ── 快速加減碼 & 批次上刊 API ─────────────────────────────────

def fetch_today_campaign_insights(access_token, ad_account_id, date_preset="today"):
    """抓各活動的 ROAS、訂單數、花費（支援一般及 CPAS 帳戶）"""
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/insights"
    params = {
        "fields": "campaign_id,spend,impressions,reach,inline_link_clicks,purchase_roas,actions,action_values,catalog_segment_actions,catalog_segment_value",
        "level": "campaign",
        "date_preset": date_preset,
        "access_token": access_token,
        "limit": 50,
    }
    all_rows = []
    while url:
        try:
            resp = requests.get(url, params=params, timeout=30).json()
        except Exception as e:
            raise Exception(f"Insights API 請求失敗: {e}")
        if "error" in resp:
            raise Exception(resp["error"].get("message", str(resp["error"])))
        all_rows.extend(resp.get("data", []))
        url    = resp.get("paging", {}).get("next")
        params = {}
    result = {}
    for row in all_rows:
        cid = row.get("campaign_id")
        if not cid:
            continue

        def _get_action(lst, atype):
            for a in (lst or []):
                if a.get("action_type") == atype:
                    try:
                        return float(a["value"])
                    except Exception:
                        pass
            return 0.0

        # 訂單數與購買金額：一般用 actions/action_values，CPAS 用 catalog_segment_actions/value
        orders = 0
        purchase_val = 0.0
        for a_type, v_type in [("actions", "action_values"), ("catalog_segment_actions", "catalog_segment_value")]:
            p = _get_action(row.get(a_type, []), "purchase")
            v = _get_action(row.get(v_type, []), "purchase")
            if p > 0:
                orders += int(p)
                purchase_val += v

        # ROAS：優先用 purchase_roas 欄位，否則用購買金額 / 花費計算
        roas = None
        for r in row.get("purchase_roas", []):
            try:
                roas = float(r["value"])
                break
            except Exception:
                pass
        if roas is None and purchase_val > 0:
            try:
                spend_val = float(row.get("spend", 0))
                if spend_val > 0:
                    roas = round(purchase_val / spend_val, 2)
            except Exception:
                pass

        try:
            spend = float(row.get("spend", 0))
        except Exception:
            spend = 0.0

        try:
            impressions = float(row.get("impressions", 0))
        except Exception:
            impressions = 0.0

        try:
            reach = float(row.get("reach", 0))
        except Exception:
            reach = 0.0

        try:
            link_clicks = float(row.get("inline_link_clicks", 0))
        except Exception:
            link_clicks = 0.0

        add_to_cart = 0.0
        for a_type in ["actions", "catalog_segment_actions"]:
            atc = _get_action(row.get(a_type, []), "add_to_cart")
            if atc > 0:
                add_to_cart += atc

        result[cid] = {
            "roas": roas, "orders": orders, "spend": spend, "purchase_val": purchase_val,
            "impressions": impressions, "reach": reach, "link_clicks": link_clicks, "add_to_cart": add_to_cart,
        }
    return result

def adjust_campaign_budget(access_token, campaign_id, multiplier_pct):
    camp = requests.get(
        f"https://graph.facebook.com/v25.0/{campaign_id}",
        params={"fields": "daily_budget,smart_promotion_type,objective,special_ad_categories", "access_token": access_token},
        timeout=15,
    ).json()
    daily_budget = camp.get("daily_budget")
    if not daily_budget:
        return {"error": {"message": "終身預算活動不支援直接調整"}}
    old_b = int(daily_budget)
    new_b = max(1, int(old_b * multiplier_pct / 100))

    payload = {"daily_budget": str(new_b), "access_token": access_token}

    result = requests.post(
        f"https://graph.facebook.com/v25.0/{campaign_id}",
        data=payload,
        timeout=30,
    ).json()
    if "error" not in result:
        return {"success": True, "old_budget": old_b, "new_budget": new_b}

    # ASC 活動：帶 special_ad_categories 再試一次
    if camp.get("smart_promotion_type"):
        sac = camp.get("special_ad_categories", [])
        payload2 = {**payload, "special_ad_categories": json.dumps(sac) if sac else "[]"}
        result2 = requests.post(
            f"https://graph.facebook.com/v25.0/{campaign_id}",
            data=payload2,
            timeout=30,
        ).json()
        if "error" not in result2:
            return {"success": True, "old_budget": old_b, "new_budget": new_b}
        # campaign 層失敗 → 嘗試 adset 層級
        adsets_r = requests.get(
            f"https://graph.facebook.com/v25.0/{campaign_id}/adsets",
            params={"fields": "id,name,daily_budget", "access_token": access_token, "limit": 50},
            timeout=15,
        ).json().get("data", [])
        as_ok, as_fail = 0, []
        for a in adsets_r:
            if not a.get("daily_budget"):
                continue
            a_new_b = max(1, int(int(a["daily_budget"]) * multiplier_pct / 100))
            r = requests.post(
                f"https://graph.facebook.com/v25.0/{a['id']}",
                data={"daily_budget": str(a_new_b), "access_token": access_token},
                timeout=30,
            ).json()
            if "error" not in r:
                as_ok += 1
            else:
                as_fail.append(a["name"])
        if as_ok:
            return {"success": True, "old_budget": old_b, "new_budget": new_b,
                    "note": f"（adset 層級，{as_ok} 個成功{f'，{len(as_fail)} 個失敗' if as_fail else ''}）"}
        err = result2.get("error", {})
        return {"error": {"message": f"[ASC] {err.get('message','')} (code:{err.get('code')} sub:{err.get('error_subcode')})"}}

    err = result.get("error", {})
    return {"error": {"message": f"{err.get('message','')} (code:{err.get('code')} sub:{err.get('error_subcode')})"}}


def create_ad_in_adset(access_token, ad_account_id, ad_name, adset_id, creative_id, status="PAUSED"):
    url = f"https://graph.facebook.com/v25.0/act_{ad_account_id}/ads"
    payload = {
        "access_token": access_token,
        "name": ad_name,
        "adset_id": adset_id,
        "creative": json.dumps({"creative_id": creative_id}),
        "status": status,
    }
    resp = requests.post(url, data=payload, timeout=30)
    data = resp.json()
    if "error" in data:
        raise Exception(data["error"].get("message", str(data["error"])))
    return data.get("id")

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

# 帳號快速切換：在 sidebar 渲染前套用 pending 值（不能在 widget 渲染後直接改 widget key）
if "acct_sel_pending" in st.session_state:
    st.session_state["acct_sel"] = st.session_state.pop("acct_sel_pending")

with st.sidebar:
    st.header("⚙️ 設定")

    # ── 管理員驗證 ──────────────────────────────────────────────
    _admin_pwd = st.secrets.get("admin_password", "")
    if _admin_pwd and not st.session_state.get("is_admin"):
        _pwd_input = st.text_input("管理員密碼", type="password", key="admin_pwd_input")
        if st.button("登入", key="admin_login"):
            if _pwd_input == _admin_pwd:
                st.session_state["is_admin"] = True
                st.rerun()
            else:
                st.error("密碼錯誤")
    is_admin = st.session_state.get("is_admin", True) if not _admin_pwd else st.session_state.get("is_admin", False)
    if is_admin and _admin_pwd:
        if st.button("登出", key="admin_logout"):
            st.session_state["is_admin"] = False
            st.rerun()

    # ── 廣告帳戶（最頂端）──────────────────────────────────────
    accounts = cfg.get("meta_accounts", [])
    if accounts:
        def acct_label(a):
            tag = "【CPAS】" if a.get("type") == "cpas" else "【一般】"
            return f"{tag} {a['name']}"
        acct_labels = [acct_label(a) for a in accounts]
        st.markdown("**廣告帳戶**")
        selected_acct_idx = st.selectbox("選擇帳戶", range(len(acct_labels)), format_func=lambda i: acct_labels[i], key="acct_sel", label_visibility="collapsed")
        selected_account_id   = accounts[selected_acct_idx]["id"]
        selected_account_type = accounts[selected_acct_idx].get("type", "general")
        client_sel, channel_sel = parse_account_name(accounts[selected_acct_idx]["name"])
    else:
        selected_account_id   = cfg.get("meta_account_id", "")
        selected_account_type = "general"
        client_sel, channel_sel = "", ""

    st.divider()
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
        meta_token = cfg.get("meta_token", "")
        # Token UI 只在本機（secrets 沒有 token）才顯示
        _token_in_secrets = bool(st.secrets.get("meta_token", ""))
        if is_admin:
            if not _token_in_secrets:
                st.divider()
                st.markdown("**Meta Access Token**")
                meta_token = st.text_input(
                    "Access Token",
                    value=meta_token,
                    type="password",
                )
                if st.button("💾 儲存 Token"):
                    cfg["meta_token"] = meta_token
                    save_config(cfg)
                    st.success("Token 已儲存")

            st.divider()
            st.markdown("**帳戶管理**")
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
    else:
        meta_token = cfg.get("meta_token", "")
        selected_account_id   = cfg.get("meta_account_id", "")
        selected_account_type = "general"
        client_sel  = st.text_input("客戶名稱", value="", key="csv_client")
        channel_sel = st.text_input("渠道", value="", key="csv_channel")

# ── 資料載入 ─────────────────────────────────────────────

acct_title = f"{client_sel} × {channel_sel}" if client_sel else ""
st.subheader(f"📁 {acct_title} × {platform_sel}" if acct_title else f"📁 {platform_sel}")

# 快速帳戶切換按鈕
if accounts and len(accounts) > 1:
    cur_idx = st.session_state.get("acct_sel", 0)
    btn_cols = st.columns(len(accounts))
    for i, (col, acct) in enumerate(zip(btn_cols, accounts)):
        label = acct["name"]
        if col.button(label, key=f"acct_btn_{i}",
                      type="primary" if i == cur_idx else "secondary",
                      use_container_width=True):
            st.session_state["acct_sel_pending"] = i
            st.rerun()

df_curr = df_comp = df_mom = df_yoy = None

if data_source == "Meta API 自動抓取":
    today = datetime.now(timezone(timedelta(hours=8))).date()
    preset_options = ["今日", "昨天", "過去7天", "本月至今", "自訂"]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("**本期**")
        preset = st.selectbox("快速選擇", preset_options, key="date_preset", label_visibility="collapsed")
        if preset == "今日":
            p_since = p_until = today
        elif preset == "昨天":
            p_since = p_until = today - timedelta(days=1)
        elif preset == "過去7天":
            p_since, p_until = today - timedelta(days=6), today
        elif preset == "本月至今":
            p_since, p_until = date(today.year, today.month, 1), today - timedelta(days=1)
        else:
            p_since, p_until = today - timedelta(days=6), today
        curr_since = st.date_input("開始", p_since, key=f"api_curr_s_{preset}_{today}")
        curr_until = st.date_input("結束", p_until, key=f"api_curr_e_{preset}_{today}")

    default_comp_since, default_comp_until = prev_week_range(curr_since, curr_until)
    default_mom_since,  default_mom_until  = mom_range(curr_since, curr_until)
    default_yoy_since,  default_yoy_until  = yoy_range(curr_since, curr_until)

    with col2:
        st.markdown("**對比期（WoW）**")
        comp_since = st.date_input("開始", default_comp_since, key=f"api_comp_s_{curr_since}_{curr_until}")
        comp_until = st.date_input("結束", default_comp_until, key=f"api_comp_e_{curr_since}_{curr_until}")
        use_comp = st.checkbox("啟用 WoW", value=False)
    with col3:
        st.markdown("**上月同期（MoM）**")
        mom_since = st.date_input("開始", default_mom_since, key=f"api_mom_s_{curr_since}_{curr_until}")
        mom_until = st.date_input("結束", default_mom_until, key=f"api_mom_e_{curr_since}_{curr_until}")
        use_mom = st.checkbox("啟用 MoM", value=True)
    with col4:
        st.markdown("**去年同期（YoY）**")
        yoy_since = st.date_input("開始", default_yoy_since, key=f"api_yoy_s_{curr_since}_{curr_until}")
        yoy_until = st.date_input("結束", default_yoy_until, key=f"api_yoy_e_{curr_since}_{curr_until}")
        use_yoy = st.checkbox("啟用 YoY", value=True)

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
                    st.session_state["reach_curr"] = fetch_account_reach(token, acct, curr_since, curr_until)
                    if use_comp:
                        df_comp = fetch_meta_insights(token, acct, comp_since, comp_until, atype)
                        st.session_state["df_comp"] = df_comp
                        st.session_state["reach_comp"] = fetch_account_reach(token, acct, comp_since, comp_until)
                    else:
                        st.session_state.pop("df_comp", None)
                        st.session_state.pop("reach_comp", None)
                    if use_mom:
                        df_mom = fetch_meta_insights(token, acct, mom_since, mom_until, atype)
                        st.session_state["df_mom"] = df_mom
                        st.session_state["reach_mom"] = fetch_account_reach(token, acct, mom_since, mom_until)
                    else:
                        st.session_state.pop("df_mom", None)
                        st.session_state.pop("reach_mom", None)
                    if use_yoy:
                        df_yoy = fetch_meta_insights(token, acct, yoy_since, yoy_until, atype)
                        st.session_state["df_yoy"] = df_yoy
                        st.session_state["reach_yoy"] = fetch_account_reach(token, acct, yoy_since, yoy_until)
                    else:
                        st.session_state.pop("df_yoy", None)
                        st.session_state.pop("reach_yoy", None)
                    st.success(f"✅ 抓到 {len(df_curr)} 個行銷活動")
                    st.session_state["dim_since"] = curr_since
                    st.session_state["dim_until"] = curr_until
                    st.session_state["dim_token"] = token
                    st.session_state["dim_acct_id"] = acct
                    st.session_state["dim_acct_type"] = atype
                    st.session_state["dim_comp_since"] = comp_since if use_comp else None
                    st.session_state["dim_comp_until"] = comp_until if use_comp else None
                    st.session_state["dim_mom_since"]  = mom_since  if use_mom  else None
                    st.session_state["dim_mom_until"]  = mom_until  if use_mom  else None
                    st.session_state["dim_yoy_since"]  = yoy_since  if use_yoy  else None
                    st.session_state["dim_yoy_until"]  = yoy_until  if use_yoy  else None
                    st.session_state.pop("df_ads", None)
                    st.session_state.pop("df_ads_comp", None)
                    st.session_state.pop("df_ads_mom", None)
                    st.session_state.pop("df_ads_yoy", None)
                    # Debug: 顯示所有 action types（幫助找出正確的 CPAS 欄位名稱）
                    raw = _fetch_raw_actions(token, acct, curr_since, curr_until)
                    if raw:
                        st.session_state["raw_actions"] = raw
                except Exception as e:
                    st.error(f"API 錯誤：{e}")

    df_curr = st.session_state.get("df_curr")
    df_comp = st.session_state.get("df_comp")
    df_mom  = st.session_state.get("df_mom")
    df_yoy  = st.session_state.get("df_yoy")

else:
    # CSV mode
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
        mom_m  = {"Google": calc_google_metrics(df_mom)}  if df_mom  is not None else None
        yoy_m  = {"Google": calc_google_metrics(df_yoy)}  if df_yoy  is not None else None

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
        components.html(pd.DataFrame(rows).to_html(escape=False, index=False), height=320, scrolling=False)

    else:
        curr_m = calc_meta_metrics(df_curr)
        comp_m = calc_meta_metrics(df_comp) if df_comp is not None else None
        mom_m  = calc_meta_metrics(df_mom)  if df_mom  is not None else None
        yoy_m  = calc_meta_metrics(df_yoy)  if df_yoy  is not None else None
        # 注入帳戶層級去重 reach（API 模式才有）
        for _m, _key in [(curr_m, "reach_curr"), (comp_m, "reach_comp"),
                         (mom_m,  "reach_mom"),  (yoy_m,  "reach_yoy")]:
            if _m is not None and _key in st.session_state:
                _m["_account_reach"] = st.session_state[_key]

        st.subheader("📈 Meta Ads 關鍵指標（ATL / BTL）")
        _since = st.session_state.get("dim_since")
        _until = st.session_state.get("dim_until")
        if _since and _until:
            _n = (_until - _since).days + 1
            _comp_label  = "昨天" if _n == 1 else "上週" if _n == 7 else "前期"
            _comp_header = "DoD"  if _n == 1 else "WoW"  if _n == 7 else "前期"
        else:
            _comp_label  = "前期"
            _comp_header = "前期"
        components.html(build_table_html(curr_m, comp_m, mom_m, yoy_m, comp_label=_comp_label, comp_header=_comp_header), height=660, scrolling=True)

        btl = curr_m.get("BTL", {})
        atl = curr_m.get("ATL", {})
        comp_atl = comp_m.get("ATL", {}) if comp_m else {}
        comp_btl = comp_m.get("BTL", {}) if comp_m else {}
        mom_atl  = mom_m.get("ATL",  {}) if mom_m  else {}
        mom_btl  = mom_m.get("BTL",  {}) if mom_m  else {}
        yoy_atl  = yoy_m.get("ATL",  {}) if yoy_m  else {}
        yoy_btl  = yoy_m.get("BTL",  {}) if yoy_m  else {}

        def funnel_delta(curr_val, comp_val, mom_val, yoy_val, higher_is_better=True):
            parts = []
            if comp_val:
                c = pct_change(curr_val, comp_val)
                if c is not None:
                    parts.append(f"WoW {'+' if c>=0 else ''}{c:.1f}%")
            if mom_val:
                c = pct_change(curr_val, mom_val)
                if c is not None:
                    parts.append(f"MoM {'+' if c>=0 else ''}{c:.1f}%")
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

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("ATL 花費",    f"${atl.get('花費', 0):,.0f}",
                  delta=funnel_delta(atl.get('花費',0), comp_atl.get('花費',0), mom_atl.get('花費',0), yoy_atl.get('花費',0)),
                  delta_color=funnel_color(atl.get('花費',0), comp_atl.get('花費',0)))
        c2.metric("BTL ROAS",   f"{btl.get('ROAS', 0):.2f}",
                  delta=funnel_delta(btl.get('ROAS',0), comp_btl.get('ROAS',0), mom_btl.get('ROAS',0), yoy_btl.get('ROAS',0)),
                  delta_color=funnel_color(btl.get('ROAS',0), comp_btl.get('ROAS',0)))
        c3.metric("BTL CPA",    f"${btl.get('CPA', 0):,.0f}",
                  delta=funnel_delta(btl.get('CPA',0), comp_btl.get('CPA',0), mom_btl.get('CPA',0), yoy_btl.get('CPA',0), False),
                  delta_color=funnel_color(btl.get('CPA',0), comp_btl.get('CPA',0), False))
        c4.metric("BTL 購買次數", f"{btl.get('購買次數', 0):.0f}",
                  delta=funnel_delta(btl.get('購買次數',0), comp_btl.get('購買次數',0), mom_btl.get('購買次數',0), yoy_btl.get('購買次數',0)),
                  delta_color=funnel_color(btl.get('購買次數',0), comp_btl.get('購買次數',0)))

        if btl:
            st.markdown("#### 🛒 BTL 轉換漏斗")
            r1, r2, r3, r4, r5, r6 = st.columns(6)
            atc  = btl.get('加購次數', 0)
            cost = btl.get('購物車成本', 0)
            cr   = btl.get('點擊到成交率', 0)
            acr  = btl.get('點擊到購物車率', 0)
            c2p  = btl.get('購物車到成交率', 0)
            aov  = btl.get('AOV', 0)
            r1.metric("購物車次數",    f"{atc:.0f}",    delta=funnel_delta(atc,  comp_btl.get('加購次數',0),       mom_btl.get('加購次數',0),       yoy_btl.get('加購次數',0)),       delta_color=funnel_color(atc,  comp_btl.get('加購次數',0)))
            r2.metric("購物車成本",    f"${cost:,.0f}", delta=funnel_delta(cost, comp_btl.get('購物車成本',0),     mom_btl.get('購物車成本',0),     yoy_btl.get('購物車成本',0),  False), delta_color=funnel_color(cost, comp_btl.get('購物車成本',0), False))
            r3.metric("AOV",          f"${aov:,.0f}",  delta=funnel_delta(aov,  comp_btl.get('AOV',0),           mom_btl.get('AOV',0),           yoy_btl.get('AOV',0)),           delta_color=funnel_color(aov,  comp_btl.get('AOV',0)))
            r4.metric("點擊→成交率",   f"{cr:.2f}%",    delta=funnel_delta(cr,   comp_btl.get('點擊到成交率',0),   mom_btl.get('點擊到成交率',0),   yoy_btl.get('點擊到成交率',0)),   delta_color=funnel_color(cr,   comp_btl.get('點擊到成交率',0)))
            r5.metric("點擊→購物車率", f"{acr:.2f}%",   delta=funnel_delta(acr,  comp_btl.get('點擊到購物車率',0), mom_btl.get('點擊到購物車率',0), yoy_btl.get('點擊到購物車率',0)), delta_color=funnel_color(acr,  comp_btl.get('點擊到購物車率',0)))
            r6.metric("購物車→成交率", f"{c2p:.2f}%",   delta=funnel_delta(c2p,  comp_btl.get('購物車到成交率',0), mom_btl.get('購物車到成交率',0), yoy_btl.get('購物車到成交率',0)), delta_color=funnel_color(c2p,  comp_btl.get('購物車到成交率',0)))

    # Debug：顯示 API 回傳的所有 action types
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
            channel_sel, curr_m, comp_m, mom_m, yoy_m, prev_actions, platform_sel
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
                        comp_s = st.session_state.get("dim_comp_since")
                        comp_u = st.session_state.get("dim_comp_until")
                        mom_s  = st.session_state.get("dim_mom_since")
                        mom_u  = st.session_state.get("dim_mom_until")
                        yoy_s  = st.session_state.get("dim_yoy_since")
                        yoy_u  = st.session_state.get("dim_yoy_until")
                        for lv in ("ad", "adset"):
                            sfx = "" if lv == "ad" else "_as"
                            df_new = fetch_meta_ad_insights(token_d, acct_d, since_d, until_d, atype_d, level=lv)
                            st.session_state[f"df_ads{sfx}"] = df_new
                            if comp_s:
                                st.session_state[f"df_ads{sfx}_comp"] = fetch_meta_ad_insights(token_d, acct_d, comp_s, comp_u, atype_d, level=lv)
                            else:
                                st.session_state.pop(f"df_ads{sfx}_comp", None)
                            if mom_s:
                                st.session_state[f"df_ads{sfx}_mom"] = fetch_meta_ad_insights(token_d, acct_d, mom_s, mom_u, atype_d, level=lv)
                            else:
                                st.session_state.pop(f"df_ads{sfx}_mom", None)
                            if yoy_s:
                                st.session_state[f"df_ads{sfx}_yoy"] = fetch_meta_ad_insights(token_d, acct_d, yoy_s, yoy_u, atype_d, level=lv)
                            else:
                                st.session_state.pop(f"df_ads{sfx}_yoy", None)
                        df_ads_new = st.session_state["df_ads"]
                        st.success(f"✅ 抓到 {len(df_ads_new)} 個廣告 / {len(st.session_state['df_ads_as'])} 個廣告組合")
                    except Exception as e:
                        st.error(f"API 錯誤：{e}")

        df_ads_raw  = st.session_state.get("df_ads")
        df_ads_comp = st.session_state.get("df_ads_comp")
        df_ads_mom  = st.session_state.get("df_ads_mom")
        df_ads_yoy  = st.session_state.get("df_ads_yoy")
        if df_ads_raw is not None and not df_ads_raw.empty:
            df_ads      = enrich_ad_dims(df_ads_raw)
            df_ads_c    = enrich_ad_dims(df_ads_comp) if df_ads_comp is not None and not df_ads_comp.empty else None
            df_ads_m    = enrich_ad_dims(df_ads_mom)  if df_ads_mom  is not None and not df_ads_mom.empty  else None
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
            df_fm = apply_filters(df_ads_m)
            df_fy = apply_filters(df_ads_y)

            active_filters = [f"{lbl}={','.join(filters[dim])}" for dim, lbl in dim_filter_labels if filters[dim]]
            if active_filters:
                st.caption(f"篩選：{' | '.join(active_filters)}　→　{len(df_f)} 個廣告（共 {len(df_ads)} 個）")
            else:
                extras = "".join([
                    "　含 WoW 對比" if df_ads_c is not None else "",
                    "　含 MoM 對比" if df_ads_m is not None else "",
                    "　含 YoY 對比" if df_ads_y is not None else "",
                ])
                st.caption(f"共 {len(df_f)} 個廣告{extras}")

            for dim_col, label in [
                ("ATL/BTL", "📊 ATL/BTL"),
                ("受眾",    "👥 受眾／新舊客"),
                ("活動類型","📅 活動類型"),
                ("格式",    "🖼️ 素材格式"),
                ("品類",    "📦 品類"),
                ("素材類型","🎭 素材類型"),
                ("廣告名稱","📝 廣告名稱"),
            ]:
                st.markdown(f"**{label}**")
                tbl = build_dim_table(df_f, dim_col, df_fc, df_fm, df_fy)
                if not tbl.empty:
                    components.html(tbl.to_html(escape=False, index=False), height=400, scrolling=True)
                else:
                    st.caption("無資料")
                st.markdown("")

            # ── Adset 成效 flat table ──────────────────────────────
            def _build_flat_table(df_cur, df_c, df_m, df_y, name_col="廣告名稱"):
                """將逐列數據彙總後顯示，含比較期欄位。"""
                def _agg(df):
                    if df is None or df.empty:
                        return {}
                    g = df.groupby(name_col, as_index=False).agg(
                        花費=("花費", "sum"), 購買次數=("購買次數", "sum"),
                        購買轉換值=("購買轉換值", "sum"), 曝光=("曝光", "sum"), 點擊=("點擊", "sum")
                    )
                    return {r[name_col]: r for _, r in g.iterrows()}
                cur  = _agg(df_cur)
                comp = _agg(df_c)
                mom  = _agg(df_m)
                yoy  = _agg(df_y)
                rows = []
                for name, r in sorted(cur.items(), key=lambda x: -x[1]["花費"]):
                    roas = r["購買轉換值"] / r["花費"] if r["花費"] > 0 else 0
                    cpa  = r["花費"] / r["購買次數"] if r["購買次數"] > 0 else None
                    row = {
                        name_col: name,
                        "花費": f"${r['花費']:,.0f}",
                        "購買": int(r["購買次數"]),
                        "ROAS": f"{roas:.2f}" if roas > 0 else "—",
                        "CPA": f"${cpa:,.0f}" if cpa else "—",
                    }
                    def _pct(a, b):
                        if b and b > 0:
                            v = (a - b) / b * 100
                            c = "green" if v >= 0 else "red"
                            sign = "+" if v >= 0 else ""
                            return f'<span style="color:{c}">{sign}{v:.1f}%</span>'
                        return "—"
                    if comp:
                        cr = comp.get(name, {})
                        row["花費 WoW"] = _pct(r["花費"], cr.get("花費", 0)) if cr else "—"
                        cr_roas = cr.get("購買轉換值", 0) / cr.get("花費", 1) if cr.get("花費", 0) > 0 else 0
                        row["ROAS WoW"] = _pct(roas, cr_roas) if cr else "—"
                    if mom:
                        mr = mom.get(name, {})
                        row["花費 MoM"] = _pct(r["花費"], mr.get("花費", 0)) if mr else "—"
                        mr_roas = mr.get("購買轉換值", 0) / mr.get("花費", 1) if mr.get("花費", 0) > 0 else 0
                        row["ROAS MoM"] = _pct(roas, mr_roas) if mr else "—"
                    if yoy:
                        yr = yoy.get(name, {})
                        row["花費 YoY"] = _pct(r["花費"], yr.get("花費", 0)) if yr else "—"
                        yr_roas = yr.get("購買轉換值", 0) / yr.get("花費", 1) if yr.get("花費", 0) > 0 else 0
                        row["ROAS YoY"] = _pct(roas, yr_roas) if yr else "—"
                    rows.append(row)
                return pd.DataFrame(rows)

            # adset flat table
            df_ads_as_raw = st.session_state.get("df_ads_as")
            if df_ads_as_raw is not None and not df_ads_as_raw.empty:
                st.markdown("**📋 廣告組合（Adset）成效**")
                df_as_c = st.session_state.get("df_ads_as_comp")
                df_as_m = st.session_state.get("df_ads_as_mom")
                df_as_y = st.session_state.get("df_ads_as_yoy")
                tbl_as = _build_flat_table(df_ads_as_raw, df_as_c, df_as_m, df_as_y)
                if not tbl_as.empty:
                    components.html(tbl_as.to_html(escape=False, index=False), height=500, scrolling=True)

            # ad flat table
            st.markdown("**📝 廣告（Ad）成效**")
            tbl_ad = _build_flat_table(df_f, df_fc, df_fm, df_fy)
            if not tbl_ad.empty:
                components.html(tbl_ad.to_html(escape=False, index=False), height=600, scrolling=True)

else:
    if data_source == "Meta API 自動抓取":
        st.info("👆 設定日期範圍後，點「從 Meta API 抓取數據」按鈕")
    else:
        st.info("👆 請先選擇「本期報表」或上傳 CSV 檔案")

def _do_load_campaigns(token, acct, force=False):
    """載入活動、成效、今日排程，全部存入 session_state。兩個 tab 共用。
    force=False 時若快取未過期（30 分鐘內）直接跳過，節省 API 配額。"""
    CACHE_TTL = 1800  # 30 分鐘
    last_load = st.session_state.get("_load_ts", 0)
    if not force and st.session_state.get("campaigns") and (time.time() - last_load < CACHE_TTL):
        st.session_state["_load_msg"] = f"✅ 使用快取資料（{int((time.time()-last_load)//60)} 分鐘前載入）"
        return
    TZ_TAIPEI = timezone(timedelta(hours=8))

    def _sched_tw_date(s):
        v = s.get("time_start", 0)
        try:
            ts = int(v)
        except (ValueError, TypeError):
            ts = int(datetime.strptime(str(v), "%Y-%m-%dT%H:%M:%S%z").timestamp())
        return (datetime.utcfromtimestamp(ts) + timedelta(hours=8)).date()

    with st.spinner("載入中（含 7 天成效與今日排程）..."):
        try:
            with ThreadPoolExecutor(max_workers=3) as ex:
                f_camps  = ex.submit(fetch_campaigns_with_budget, token, acct)
                f_ins    = ex.submit(fetch_today_campaign_insights, token, acct, "today")
                f_ins_7d = ex.submit(fetch_today_campaign_insights, token, acct, "last_7d")
                camps      = f_camps.result()
                ins_today  = f_ins.result()
                ins_7d_raw = f_ins_7d.result()
            st.session_state["campaigns"]         = camps
            st.session_state["sched_insights"]    = ins_today
            st.session_state["sched_insights_7d"] = ins_7d_raw

            _active_camps = [c for c in camps if c.get("status") == "ACTIVE"]
            _now_ts  = datetime.now(timezone.utc).timestamp()
            today_scheds = {}
            _errors      = []
            _rate_limited = 0

            time.sleep(1)  # 避免和前面平行請求同時搶 rate limit
            # Batch API：47 個請求合成 1 次 HTTP，避免 rate limit
            BATCH_SIZE = 50
            sched_field = "id,time_start,time_end,budget_value,budget_value_type,status"
            for chunk_start in range(0, len(_active_camps), BATCH_SIZE):
                chunk = _active_camps[chunk_start: chunk_start + BATCH_SIZE]
                batch_payload = [
                    {"method": "GET",
                     "relative_url": f"{c['id']}/budget_schedules?fields={sched_field}&limit=50"}
                    for c in chunk
                ]
                try:
                    batch_resp = requests.post(
                        "https://graph.facebook.com/v25.0/",
                        data={"access_token": token, "batch": json.dumps(batch_payload)},
                        timeout=30,
                    ).json()
                    # rate limit → 等 3 秒重試一次
                    if isinstance(batch_resp, list) and batch_resp and isinstance(batch_resp[0], dict):
                        first_body = json.loads(batch_resp[0].get("body", "{}")) if isinstance(batch_resp[0].get("body"), str) else {}
                        if "User request limit" in first_body.get("error", {}).get("message", ""):
                            time.sleep(3)
                            batch_resp = requests.post(
                                "https://graph.facebook.com/v25.0/",
                                data={"access_token": token, "batch": json.dumps(batch_payload)},
                                timeout=30,
                            ).json()
                except Exception as e:
                    _errors.append(f"Batch 請求失敗: {e}")
                    continue
                for c, item in zip(chunk, batch_resp):
                    if not isinstance(item, dict):
                        continue
                    if item.get("code") != 200:
                        try:
                            body = json.loads(item.get("body", "{}"))
                            msg  = body.get("error", {}).get("message", f"HTTP {item.get('code')}")
                        except Exception:
                            msg = f"HTTP {item.get('code')}"
                        # rate limit 不計入「失敗」，但仍計數，避免排程資料默默消失而不自知
                        if "rate limit" in msg.lower():
                            _rate_limited += 1
                        else:
                            _errors.append(msg)
                        continue
                    try:
                        body   = json.loads(item.get("body", "{}"))
                        scheds = body.get("data", [])
                        valid  = [s for s in scheds if _end_ts(s) > _now_ts]
                        if valid:
                            best = min(valid, key=lambda s: _end_ts(s))
                            bv   = int(best.get("budget_value", 100))
                            today_scheds[c["id"]] = {
                                "tag": f"+{bv}%" if bv >= 0 else f"{bv}%",
                                "schedule_id": best["id"],
                                "budget_value": bv,
                                "time_start": best.get("time_start"),
                                "time_end": best.get("time_end"),
                            }
                    except Exception as fe:
                        _errors.append(str(fe))
            st.session_state["today_scheds"] = today_scheds
            st.session_state["_load_ts"]     = time.time()
            _err_preview = f"｜⚠️{len(_errors)} 筆失敗：{_errors[0][:80]}" if _errors else ""
            _rl_preview  = f"｜⏳{_rate_limited} 筆遭 rate limit 跳過，排程資料可能不完整，請稍後按「強制重整」" if _rate_limited else ""
            st.session_state["_load_msg"] = (
                f"✅ {len(camps)} 活動 / {len(_active_camps)} ACTIVE → 今日排程={len(today_scheds)}{_err_preview}{_rl_preview}"
            )
        except Exception as e:
            st.session_state["_load_msg"] = f"❌ 載入錯誤：{e}"


if data_source == "Meta API 自動抓取" and platform_sel == "Meta":
    st.divider()
    st.subheader("📅 預算排程")

    tab_new, tab_mod, tab_del = st.tabs(["➕ 新增排程", "✏️ 修改今日排程", "🗑️ 刪除排程"])

    # ── Tab 1：新增排程（含查看今日排程）──────────────────────────
    with tab_new:
        _lb1, _lb2 = st.columns([3, 1])
        if _lb1.button("🔄 載入活動與成效", key="load_campaigns", use_container_width=True):
            _token = cfg.get("meta_token", "")
            _acct  = selected_account_id
            if not _token or not _acct:
                st.error("請先設定 Token 和帳戶")
            else:
                _do_load_campaigns(_token, _acct, force=False)
        if _lb2.button("強制重整", key="load_campaigns_force", use_container_width=True):
            _token = cfg.get("meta_token", "")
            _acct  = selected_account_id
            if not _token or not _acct:
                st.error("請先設定 Token 和帳戶")
            else:
                _do_load_campaigns(_token, _acct, force=True)

        if "_load_msg" in st.session_state:
            st.caption(st.session_state.get("_load_msg", ""))

        campaigns       = st.session_state.get("campaigns", [])
        sched_insights  = st.session_state.get("sched_insights", {})
        sched_ins_7d    = st.session_state.get("sched_insights_7d", {})
        today_scheds    = st.session_state.get("today_scheds", {})

        if campaigns:
            # 從 session_state 取值（UI 元件在表格下方，此處僅讀取供建表格用）
            sched_pct = st.session_state.get("sched_pct", 20)
            sched_dir = st.session_state.get("sched_dir", "加碼 ⬆️")
            sched_actual_pct = sched_pct if "加碼" in sched_dir else -sched_pct

            # ── 排程時段設定（2 列排列，手機不擠）
            HOURS = [f"{h:02d}:00" for h in range(24)] + ["23:45"]
            st.markdown("**排程時段**")

            # 快速加整天（日期範圍）
            with st.expander("⚡ 快速加整天（日期範圍）", expanded=False):
                qc1, qc2 = st.columns(2)
                with qc1:
                    quick_s = st.date_input("從", datetime.now(timezone(timedelta(hours=8))).date(), key="quick_s")
                with qc2:
                    quick_e = st.date_input("到", datetime.now(timezone(timedelta(hours=8))).date(), key="quick_e")
                if st.button("＋ 批次加整天（每天 00:00–23:45）", key="add_quick_days", use_container_width=True):
                    if quick_e < quick_s:
                        st.error("結束日期不能早於開始日期")
                    else:
                        slots = st.session_state.get("sched_slots", [])
                        added = 0
                        d = quick_s
                        while d <= quick_e:
                            ts_s = date_hour_to_ts(d, "00:00")
                            ts_e = date_hour_to_ts(d, "23:45")
                            if not any(s["_ts_start"] == ts_s and s["_ts_end"] == ts_e for s in slots):
                                slots.append({
                                    "開始": f"{d} 00:00",
                                    "結束": f"{d} 23:45",
                                    "_ts_start": ts_s,
                                    "_ts_end":   ts_e,
                                })
                                added += 1
                            d += timedelta(days=1)
                        st.session_state["sched_slots"] = slots
                        st.success(f"已加入 {added} 個整天時段")
                        st.rerun()

            # 自訂時段
            with st.expander("🕐 自訂時段", expanded=False):
                ts1, ts2 = st.columns(2)
                with ts1:
                    slot_s_date = st.date_input("開始日期", datetime.now(timezone(timedelta(hours=8))).date(), key="slot_s_date")
                with ts2:
                    slot_s_hour = st.selectbox("開始時間", HOURS, index=0, key="slot_s_hour")
                ts3, ts4 = st.columns(2)
                with ts3:
                    slot_e_date = st.date_input("結束日期", datetime.now(timezone(timedelta(hours=8))).date(), key="slot_e_date")
                with ts4:
                    slot_e_hour = st.selectbox("結束時間", HOURS, index=len(HOURS)-1, key="slot_e_hour")
                if st.button("＋ 加入批次清單", key="add_slot", use_container_width=True):
                    ts_s = date_hour_to_ts(slot_s_date, slot_s_hour)
                    ts_e = date_hour_to_ts(slot_e_date, slot_e_hour)
                    if ts_s >= ts_e:
                        st.error("結束時間必須晚於開始時間")
                    else:
                        slots = st.session_state.get("sched_slots", [])
                        if not any(s["_ts_start"] == ts_s and s["_ts_end"] == ts_e for s in slots):
                            slots.append({
                                "開始": f"{slot_s_date} {slot_s_hour}",
                                "結束": f"{slot_e_date} {slot_e_hour}",
                                "_ts_start": ts_s,
                                "_ts_end":   ts_e,
                            })
                            st.session_state["sched_slots"] = slots
                            st.rerun()

            # 批次時段清單（選用）
            sched_slots = st.session_state.get("sched_slots", [])
            sel_slot_rows = []
            if sched_slots:
                st.caption("批次時段（Shift 多選後可刪除；勾選的時段才會套用）")
                df_slots = pd.DataFrame(sched_slots)
                gb_s = GridOptionsBuilder.from_dataframe(df_slots)
                gb_s.configure_selection(selection_mode="multiple", use_checkbox=True)
                gb_s.configure_column("開始", checkboxSelection=True, headerCheckboxSelection=True, width=200)
                gb_s.configure_column("結束", width=200)
                gb_s.configure_column("_ts_start", hide=True)
                gb_s.configure_column("_ts_end",   hide=True)
                slot_resp = AgGrid(
                    df_slots,
                    gridOptions=gb_s.build(),
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
                    height=min(200, 48 + 36 * len(sched_slots)),
                    theme="streamlit",
                    fit_columns_on_grid_load=True,
                    key="slots_aggrid",
                )
                _sr = slot_resp.get("selected_rows")
                if _sr is None or (isinstance(_sr, pd.DataFrame) and _sr.empty):
                    sel_slot_rows = []
                elif isinstance(_sr, pd.DataFrame):
                    sel_slot_rows = _sr.to_dict("records")
                else:
                    sel_slot_rows = list(_sr)

                db1, db2 = st.columns([1.5, 6])
                with db1:
                    if st.button("🗑 刪除選取", key="del_slots") and sel_slot_rows:
                        sel_keys = {(r["_ts_start"], r["_ts_end"]) for r in sel_slot_rows}
                        st.session_state["sched_slots"] = [s for s in sched_slots if (s["_ts_start"], s["_ts_end"]) not in sel_keys]
                        st.rerun()
                with db2:
                    if st.button("清除全部", key="clear_slots"):
                        st.session_state["sched_slots"] = []
                        st.rerun()

            st.divider()

            # ── 建立表格資料
            show_paused = st.checkbox("顯示暫停的活動 ⏸", value=False, key="show_paused")
            rows, camp_id_list = [], []
            for c in campaigns:
                if not c.get("daily_budget"):
                    continue
                if c["status"] != "ACTIVE" and not show_paused:
                    continue
                ins    = sched_insights.get(c["id"], {})
                ins_7d = sched_ins_7d.get(c["id"], {})
                daily_b      = int(c["daily_budget"])
                _ts_entry    = today_scheds.get(c["id"])
                sched_tag    = _ts_entry["tag"] if isinstance(_ts_entry, dict) else (_ts_entry or "—")
                _existing_pct = _ts_entry["budget_value"] if isinstance(_ts_entry, dict) else None
                projected    = 0  # 在套用 sel_state 後重算
                spend_today  = round(ins.get("spend", 0))
                orders_today = ins.get("orders", 0)
                pv_today     = ins.get("purchase_val", 0)
                cpa_today    = round(spend_today / orders_today) if orders_today > 0 else None
                lc  = ins.get("link_clicks", 0)
                imp = ins.get("impressions", 0)
                rc  = ins.get("reach", 0)
                atc = ins.get("add_to_cart", 0)
                cvr      = f"{orders_today / lc * 100:.1f}%" if lc > 0 else None
                atc_rate = f"{atc / lc * 100:.1f}%"          if lc > 0 else None
                ctr      = f"{lc / imp * 100:.2f}%"           if imp > 0 else None
                cpc      = round(spend_today / lc)            if lc > 0 else None
                cpm_reach = round(spend_today / rc * 1000)    if rc > 0 else None
                rows.append({
                    "選取":     False,
                    "狀":       "🟢" if c["status"] == "ACTIVE" else "⏸",
                    "活動名稱": c["name"],
                    "日預算":   daily_b,
                    "今日花費": spend_today,
                    "今日ROAS": ins.get("roas"),
                    "7天ROAS":  ins_7d.get("roas"),
                    "今日排程": sched_tag,
                    "排程後預算": 0,
                    "_existing_pct": _existing_pct,
                    "今日購買": orders_today,
                    "今日CPA":  cpa_today,
                    "轉換價值": round(pv_today) if pv_today else None,
                    "CVR":      cvr,
                    "加車率":   atc_rate,
                    "CTR":      ctr,
                    "CPC":      cpc,
                    "觸及成本": cpm_reach,
                })
                camp_id_list.append(c["id"])

            # 排序：🟢有花費 → 🟢無花費 → ⏸；有花費層內按今日ROAS desc（None 最後），再按 7天ROAS desc
            def _sort_key(pair):
                row = pair[0]
                return (
                    row["狀"] == "⏸",
                    row["今日花費"] == 0,
                    row["今日ROAS"] is None,
                    -(row["今日ROAS"] or 0),
                    row["7天ROAS"] is None,
                    -(row["7天ROAS"] or 0),
                )
            combined = sorted(zip(rows, camp_id_list), key=_sort_key)
            rows, camp_id_list = (list(z) for z in zip(*combined)) if combined else ([], [])

            # ── 目標 ROAS 設定（每帳號獨立）
            _roas_cfg_key = f"target_roas_{selected_account_id}"
            _stored_roas  = float(cfg.get("account_target_roas", {}).get(selected_account_id, 4.0))
            if _roas_cfg_key not in st.session_state:
                st.session_state[_roas_cfg_key] = _stored_roas
            _tr_col, _ = st.columns([2, 5])
            target_roas = _tr_col.number_input(
                "目標 ROAS（用於建議加碼）", min_value=0.1, step=0.5,
                key=_roas_cfg_key,
            )
            if target_roas != _stored_roas:
                cfg.setdefault("account_target_roas", {})[selected_account_id] = target_roas
                save_config(cfg)
            _now_tw = datetime.now(tz=timezone(timedelta(hours=8)))
            _hour_frac = _now_tw.hour + _now_tw.minute / 60
            _expected_pace = _hour_frac / 24  # 當前時間應達成進度

            # ── 調整幅度 & 方向
            pc1, pc2 = st.columns(2)
            with pc1:
                sched_pct = st.number_input("調整幅度 (%)", min_value=1, max_value=10000, value=int(sched_pct), step=5, key="sched_pct")
            with pc2:
                sched_dir = st.radio("方向", ["加碼 ⬆️", "減碼 ⬇️"], key="sched_dir", horizontal=True)
            sched_actual_pct = sched_pct if "加碼" in sched_dir else -sched_pct

            # ── 快速選取按鈕
            qb1, qb2, qb3 = st.columns(3)
            if qb1.button("全選", key="sel_all", use_container_width=True):
                st.session_state["sched_sel"] = set(range(len(camp_id_list)))
                st.session_state["_sched_btn_set"] = True
                st.rerun()
            if qb2.button("取消全選", key="sel_none", use_container_width=True):
                st.session_state["sched_sel"] = set()
                st.session_state["_sched_btn_set"] = True
                st.rerun()
            if qb3.button("選🟢有花費", key="sel_spend", use_container_width=True):
                st.session_state["sched_sel"] = {i for i, r in enumerate(rows) if r["今日花費"] > 0}
                st.session_state["_sched_btn_set"] = True
                st.rerun()

            proj_col = "排程後預算"
            sel_indices = st.session_state.get("sched_sel", set())
            for i, row in enumerate(rows):
                is_sel  = i in sel_indices
                daily_b = row["日預算"]
                eff_pct = sched_actual_pct if is_sel else (row["_existing_pct"] if row["_existing_pct"] is not None else sched_actual_pct)
                row[proj_col]    = f"${round(daily_b * (1 + eff_pct / 100))}"

                # 建議加碼%（Phase 1：ROAS tier + 花費進度比）
                _roas_raw = row["今日ROAS"]
                _pace = (row["今日花費"] / daily_b / _expected_pace
                         ) if (_expected_pace > 0 and daily_b > 0) else 0
                if _roas_raw and _roas_raw > 0:
                    _r = _roas_raw / target_roas
                    if _r >= 5:
                        _sug = "🚀 +200~800%"
                    elif _r >= 3:
                        _sug = "🟢 +100~200%"
                    elif _r >= 1.5:
                        _sug = "🟢 +50~100%"
                    elif _pace >= 1.0:
                        _sug = "🟡 +20%"
                    else:
                        _sug = "⏸ 觀望"
                else:
                    _sug = "🟡 +20%" if _pace >= 1.0 else "— 無資料"
                row["建議加碼"] = _sug

                row["今日ROAS"]  = _fmt_roas(row["今日ROAS"])
                row["7天ROAS"]   = _fmt_roas(row["7天ROAS"])

            disp_cols_sched = ["建議加碼", "狀", "活動名稱", "日預算", "今日花費", "今日ROAS",
                               "7天ROAS", "今日排程", proj_col, "今日購買", "今日CPA", "轉換價值",
                               "CVR", "加車率", "CTR", "CPC", "觸及成本"]
            df_sched = pd.DataFrame(rows)
            sched_event = st.dataframe(
                df_sched[disp_cols_sched],
                use_container_width=True,
                hide_index=True,
                height=min(420, 50 + 40 * len(rows)),
                column_config={
                    "建議加碼":  st.column_config.TextColumn("建議加碼", width=110),
                    "狀":        st.column_config.TextColumn("狀",       width=40),
                    "活動名稱":  st.column_config.TextColumn("活動名稱", width=160),
                    "日預算":    st.column_config.NumberColumn("日預算",   width=80),
                    "今日花費":  st.column_config.NumberColumn("今日花費", width=80),
                    "今日ROAS":  st.column_config.TextColumn("今日ROAS", width=75),
                    "7天ROAS":   st.column_config.TextColumn("7天ROAS",  width=70),
                    "今日排程":  st.column_config.TextColumn("今日排程", width=80),
                    proj_col:    st.column_config.TextColumn(proj_col,   width=110),
                    "今日購買":  st.column_config.NumberColumn("今日購買", width=70),
                    "今日CPA":   st.column_config.TextColumn("今日CPA",  width=70),
                    "轉換價值":  st.column_config.TextColumn("轉換價值", width=80),
                    "CVR":       st.column_config.TextColumn("CVR",      width=70),
                    "加車率":    st.column_config.TextColumn("加車率",    width=70),
                    "CTR":       st.column_config.TextColumn("CTR",      width=70),
                    "CPC":       st.column_config.NumberColumn("CPC",    width=65),
                    "觸及成本":  st.column_config.NumberColumn("觸及成本", width=80),
                },
                on_select="rerun",
                selection_mode="multi-row",
                key=f"sched_df_{st.session_state.get('sched_sel_v', 0)}",
            )
            # 同步選取狀態（原生多選，支援 Shift+Click）
            # 若是按鈕觸發的 rerun，跳過同步以免 dataframe 空 state 覆蓋按鈕設定
            new_sel = set(sched_event.selection.rows)
            if st.session_state.pop("_sched_btn_set", False):
                sel_indices = st.session_state.get("sched_sel", set())
            elif new_sel != sel_indices:
                st.session_state["sched_sel"] = new_sel
                sel_indices = new_sel

            sel_indices = {i for i in sel_indices if i < len(camp_id_list)}
            selected_camp_ids   = [camp_id_list[i] for i in sorted(sel_indices) if i < len(camp_id_list)]
            selected_camp_names = [rows[i]["活動名稱"] for i in sorted(sel_indices)]

            n_camps = len(selected_camp_ids)
            pct_sign = f"+{sched_actual_pct}%" if sched_actual_pct > 0 else f"{sched_actual_pct}%"

            if n_camps > 0:
                # 決定要套用的時段：有勾選批次清單 → 用清單；否則用上方單一時段
                if sel_slot_rows:
                    slots_to_apply = [{"開始": r["開始"], "結束": r["結束"],
                                       "_ts_start": int(r["_ts_start"]), "_ts_end": int(r["_ts_end"])}
                                      for r in sel_slot_rows]
                    slot_desc = f"**{len(slots_to_apply)}** 個時段"
                else:
                    ts_s = date_hour_to_ts(slot_s_date, slot_s_hour)
                    ts_e = date_hour_to_ts(slot_e_date, slot_e_hour)
                    slots_to_apply = [{"開始": f"{slot_s_date} {slot_s_hour}",
                                       "結束": f"{slot_e_date} {slot_e_hour}",
                                       "_ts_start": ts_s, "_ts_end": ts_e}]
                    slot_desc = f"`{slot_s_date} {slot_s_hour} → {slot_e_date} {slot_e_hour}`"

                dir_label = f"提升 {sched_pct}%" if sched_actual_pct > 0 else f"降低 {sched_pct}%"
                total = n_camps * len(slots_to_apply)
                st.info(f"已選 **{n_camps}** 個活動 × {slot_desc} = **{total}** 筆排程，預算{dir_label}")

                if st.button("✅ 確認建立排程", type="primary", key="confirm_sched"):
                    _token = cfg.get("meta_token", "")
                    _camp_map = {c["id"]: c for c in campaigns}
                    for slot in slots_to_apply:
                        slot_label = f"{slot['開始']}→{slot['結束']}"
                        for cid, cname in zip(selected_camp_ids, selected_camp_names):
                            # 只有 Advantage+ Shopping（SHOPPING）才不支援預算排程
                            if _camp_map.get(cid, {}).get("smart_promotion_type") == "SHOPPING":
                                st.warning(f"⚠️ {cname}：ASC 活動不支援預算排程，已跳過")
                                continue
                            try:
                                result = create_budget_schedule(_token, cid, slot["_ts_start"], slot["_ts_end"], sched_actual_pct)
                            except requests.exceptions.RequestException as e:
                                result = {"error": {"message": f"連線逾時或失敗：{e}"}}
                            if result.get("success") or result.get("id"):
                                note = result.get("note", "")
                                label = f"【{slot_label}】" if len(slots_to_apply) > 1 else ""
                                st.success(f"✅ {cname}{label}{note if note else '排程建立成功'}")
                                st.session_state.setdefault("today_scheds", {})[cid] = {
                                    "tag": pct_sign,
                                    "schedule_id": result.get("id", ""),
                                    "budget_value": sched_actual_pct,
                                }
                            else:
                                _err = result.get("error", {})
                                err_msg = _err.get("message", str(result))
                                err_sub = _err.get("error_subcode", "")
                                err_usr = _err.get("error_user_msg", "")
                                detail = " | ".join(filter(None, [err_usr, f"subcode:{err_sub}" if err_sub else ""]))
                                st.error(f"❌ {cname}【{slot_label}】{err_msg}" + (f"（{detail}）" if detail else ""))
        else:
            st.info("請先點「載入活動與成效」")

    # ── Tab 2：修改今日排程 ────────────────────────────────────────
    with tab_mod:
        today_scheds_mod = st.session_state.get("today_scheds", {})
        campaigns_mod    = st.session_state.get("campaigns", [])

        _mod_token = cfg.get("meta_token", "")
        _mod_acct  = selected_account_id
        if not campaigns_mod or not today_scheds_mod:
            _mb1, _mb2 = st.columns([3, 1])
            if _mb1.button("🔄 載入活動與今日排程", key="load_mod", use_container_width=True):
                if not _mod_token or not _mod_acct:
                    st.error("請先設定 Token 和帳戶")
                else:
                    _do_load_campaigns(_mod_token, _mod_acct, force=False)
                    st.rerun()
            if _mb2.button("強制重整", key="load_mod_force", use_container_width=True):
                if not _mod_token or not _mod_acct:
                    st.error("請先設定 Token 和帳戶")
                else:
                    _do_load_campaigns(_mod_token, _mod_acct, force=True)
                    st.rerun()

        if "_load_msg" in st.session_state:
            st.caption(st.session_state.pop("_load_msg"))

        today_scheds_mod = st.session_state.get("today_scheds", {})
        campaigns_mod    = st.session_state.get("campaigns", [])
        mod_camps = [c for c in campaigns_mod
                     if isinstance(today_scheds_mod.get(c["id"]), dict)]

        if not campaigns_mod:
            st.info("請點上方按鈕載入資料")
        elif not mod_camps:
            st.info("今日尚無已建立的排程")
        else:
                st.caption(f"共 {len(mod_camps)} 個活動今日有排程，可直接修改幅度")

                mod_new_pct = st.number_input(
                    "新幅度 (%，正數 = 加碼，負數 = 減碼)",
                    min_value=-99, max_value=10000, value=20, step=5,
                    key="mod_new_pct",
                )

                mod_ins    = st.session_state.get("sched_insights", {})
                mod_ins_7d = st.session_state.get("sched_insights_7d", {})

                mod_sign    = f"+{mod_new_pct}%" if mod_new_pct >= 0 else f"{mod_new_pct}%"
                proj_col_m  = f"修改後預算（{mod_sign}）"

                mod_rows, mod_id_list = [], []
                for c in mod_camps:
                    entry    = today_scheds_mod[c["id"]]
                    ins      = mod_ins.get(c["id"], {})
                    ins_7d   = mod_ins_7d.get(c["id"], {})
                    daily_b  = int(c.get("daily_budget", 0))
                    spend    = round(ins.get("spend", 0))
                    orders   = ins.get("orders", 0)
                    pv       = ins.get("purchase_val", 0)
                    cpa      = round(spend / orders) if orders > 0 else None
                    projected = round(daily_b * (1 + mod_new_pct / 100))
                    mod_rows.append({
                        "選取":       False,
                        "活動名稱":   c["name"],
                        "日預算":     daily_b,
                        "今日花費":   spend,
                        "今日ROAS":   _fmt_roas(ins.get("roas")),
                        "7天ROAS":    _fmt_roas(ins_7d.get("roas")),
                        "今日排程":   entry["tag"],
                        proj_col_m:   f"${projected}",
                        "今日購買":   orders,
                        "今日CPA":    cpa,
                        "轉換價值":   round(pv) if pv else None,
                    })
                    mod_id_list.append(c["id"])

                mc1, mc2 = st.columns(2)
                if mc1.button("全選", key="mod_sel_all", use_container_width=True):
                    st.session_state["mod_sel"] = set(range(len(mod_id_list)))
                    st.session_state["_mod_btn_set"] = True
                    st.rerun()
                if mc2.button("取消全選", key="mod_sel_none", use_container_width=True):
                    st.session_state["mod_sel"] = set()
                    st.session_state["_mod_btn_set"] = True
                    st.rerun()

                # 移除選取欄，改用原生多選
                for row in mod_rows:
                    row.pop("選取", None)

                mod_sel_indices = st.session_state.get("mod_sel", set())
                mod_event = st.dataframe(
                    pd.DataFrame(mod_rows),
                    use_container_width=True,
                    hide_index=True,
                    height=min(420, 50 + 40 * len(mod_rows)),
                    column_config={
                        "活動名稱":  st.column_config.TextColumn("活動名稱", width=160),
                        "日預算":    st.column_config.NumberColumn("日預算",  width=75),
                        "今日花費":  st.column_config.NumberColumn("今日花費", width=75),
                        "今日ROAS":  st.column_config.TextColumn("今日ROAS", width=75),
                        "7天ROAS":   st.column_config.TextColumn("7天ROAS",  width=70),
                        "今日排程":  st.column_config.TextColumn("今日排程", width=80),
                        proj_col_m:  st.column_config.TextColumn(proj_col_m, width=120),
                        "今日購買":  st.column_config.NumberColumn("今日購買", width=65),
                        "今日CPA":   st.column_config.NumberColumn("今日CPA",  width=70),
                        "轉換價值":  st.column_config.NumberColumn("轉換價值", width=75),
                    },
                    on_select="rerun",
                    selection_mode="multi-row",
                    key="mod_df",
                )
                new_mod_sel = set(mod_event.selection.rows)
                if st.session_state.pop("_mod_btn_set", False):
                    mod_sel_indices = st.session_state.get("mod_sel", set())
                elif new_mod_sel != mod_sel_indices:
                    st.session_state["mod_sel"] = new_mod_sel
                    mod_sel_indices = new_mod_sel

                selected_mod_ids = [mod_id_list[i] for i in sorted(mod_sel_indices) if i < len(mod_id_list)]

                if selected_mod_ids:
                    new_sign = f"+{mod_new_pct}%" if mod_new_pct >= 0 else f"{mod_new_pct}%"
                    st.info(f"已選 **{len(selected_mod_ids)}** 個活動，將排程幅度改為 **{new_sign}**")

                    if st.button("✅ 確認修改排程", type="primary", key="confirm_mod"):
                        _token = cfg.get("meta_token", "")
                        ok, fail = 0, 0
                        for cid in selected_mod_ids:
                            entry  = today_scheds_mod[cid]
                            sched_id = entry["schedule_id"]
                            cname  = next((c["name"] for c in mod_camps if c["id"] == cid), cid)
                            result = update_budget_schedule(
                                _token, sched_id, mod_new_pct,
                                campaign_id=cid,
                                time_start=entry.get("time_start"),
                                time_end=entry.get("time_end"),
                            )
                            if "error" not in result:
                                st.success(f"✅ {cname} 已更新為 {new_sign}")
                                st.session_state["today_scheds"][cid] = {
                                    "tag": new_sign,
                                    "schedule_id": sched_id,
                                    "budget_value": mod_new_pct,
                                }
                                ok += 1
                            else:
                                _e = result.get("error", {})
                                _emsg = _e.get("message", str(result))
                                _esub = _e.get("error_subcode", "")
                                _eusr = _e.get("error_user_msg", "")
                                _detail = " | ".join(filter(None, [_eusr, f"subcode:{_esub}" if _esub else ""]))
                                st.error(f"❌ {cname}：{_emsg}" + (f"（{_detail}）" if _detail else ""))
                                fail += 1
                        if ok:
                            st.session_state["mod_sel"] = set()
                            st.rerun()

    # ── Tab 3：刪除排程 ─────────────────────────────────────────
    with tab_del:
        if st.button("🔄 載入排程", key="load_del_scheds"):
            _token = cfg.get("meta_token", "")
            _acct  = selected_account_id
            if not _token or not _acct:
                st.error("請先設定 Token 和帳戶")
            else:
                with st.spinner("載入中..."):
                    try:
                        camps = fetch_campaigns_with_budget(_token, _acct)
                        now_ts = datetime.now(timezone.utc).timestamp()
                        del_scheds = _batch_fetch_all_schedules(_token, camps, now_ts)
                        st.session_state["del_scheds"] = del_scheds
                    except Exception as e:
                        st.error(f"錯誤：{e}")

        del_scheds = st.session_state.get("del_scheds", {})
        if del_scheds:
            tz_tw = timezone(timedelta(hours=8))
            _token = cfg.get("meta_token", "")

            # 統計過期排程數
            all_expired = [(cid, s) for cid, data in del_scheds.items() for s in data["expired"]]
            if all_expired:
                exp_count = len(all_expired)
                st.warning(f"共有 **{exp_count}** 筆過期排程")
                if st.button(f"🗑️ 一鍵刪除所有過期排程（{exp_count} 筆）", type="primary", key="del_all_expired"):
                    ok, fail = 0, 0
                    for _, s in all_expired:
                        res = delete_budget_schedule(_token, s["id"])
                        if res.get("success"):
                            ok += 1
                        else:
                            fail += 1
                    st.success(f"✅ 已刪除 {ok} 筆" + (f"，失敗 {fail} 筆" if fail else ""))
                    camps2 = fetch_campaigns_with_budget(_token, selected_account_id)
                    st.session_state["del_scheds"] = _batch_fetch_all_schedules(
                        _token, camps2, datetime.now(timezone.utc).timestamp()
                    )
                    st.rerun()
            else:
                st.success("✅ 沒有過期排程")

            st.divider()

            # 各活動排程列表
            for cid, data in del_scheds.items():
                c = data["campaign"]
                active_list  = data["active"]
                expired_list = data["expired"]
                if not active_list and not expired_list:
                    continue
                icon = "🟢" if c["status"] == "ACTIVE" else "⏸"
                label = f"{icon} {c['name']}（進行中 {len(active_list)} / 過期 {len(expired_list)}）"
                with st.expander(label):
                    for s in active_list + expired_list:
                        t_start = parse_meta_ts(s["time_start"], tz_tw).strftime("%Y/%m/%d %H:%M")
                        t_end   = parse_meta_ts(s["time_end"],   tz_tw).strftime("%Y/%m/%d %H:%M")
                        bv = int(s.get("budget_value", 100))
                        pct_str = f"+{bv}%" if bv >= 0 else f"{bv}%"
                        now_ts2 = datetime.now(timezone.utc).timestamp()
                        is_active = _end_ts(s) > now_ts2
                        badge = "🟡 進行中" if is_active else "⬜ 已結束"
                        col_info, col_btn = st.columns([6, 1])
                        col_info.write(f"{badge}　**{t_start} ～ {t_end}**　`{pct_str}`")
                        if col_btn.button("刪除", key=f"del_sched_{s['id']}"):
                            res = delete_budget_schedule(_token, s["id"])
                            if res.get("success"):
                                st.success("已刪除")
                                camps2 = fetch_campaigns_with_budget(_token, selected_account_id)
                                st.session_state["del_scheds"] = _batch_fetch_all_schedules(
                                    _token, camps2, datetime.now(timezone.utc).timestamp()
                                )
                                st.rerun()
                            else:
                                st.error(f"刪除失敗：{res}")
        elif "del_scheds" in st.session_state:
            st.info("目前沒有任何排程")
        else:
            st.info("請先點「載入排程」")


if data_source == "Meta API 自動抓取" and platform_sel == "Meta":

    # ── 快速加減碼 ──────────────────────────────────────────────
    st.divider()
    st.subheader("⚡ 快速加減碼")
    st.caption("直接修改日預算（立即生效，非排程）")

    if st.button("🔄 載入／重新整理", key="load_camps_adj"):
        _token = cfg.get("meta_token", "")
        _acct  = selected_account_id
        if not _token or not _acct:
            st.error("請先設定 Token 和帳戶")
        else:
            with st.spinner("載入中..."):
                try:
                    with ThreadPoolExecutor(max_workers=3) as ex:
                        f_camps  = ex.submit(fetch_campaigns_with_budget, _token, _acct)
                        f_ins    = ex.submit(fetch_today_campaign_insights, _token, _acct)
                        f_ins_7d = ex.submit(fetch_today_campaign_insights, _token, _acct, "last_7d")
                        st.session_state["adj_campaigns"]   = f_camps.result()
                        st.session_state["adj_insights"]    = f_ins.result()
                        st.session_state["adj_insights_7d"] = f_ins_7d.result()
                except Exception as e:
                    st.error(f"錯誤：{e}")

    adj_campaigns   = st.session_state.get("adj_campaigns", [])
    adj_insights    = st.session_state.get("adj_insights", {})
    adj_insights_7d = st.session_state.get("adj_insights_7d", {})
    if adj_campaigns:
        _token = cfg.get("meta_token", "")

        show_paused_adj = st.checkbox("顯示暫停的活動 ⏸", value=False, key="show_paused_adj")

        adj_rows, adj_id_list = [], []
        for c in adj_campaigns:
            if not c.get("daily_budget"):
                continue
            if c["status"] != "ACTIVE" and not show_paused_adj:
                continue
            ins    = adj_insights.get(c["id"], {})
            ins_7d = adj_insights_7d.get(c["id"], {})
            spend_today  = round(ins.get("spend", 0))
            orders_today = ins.get("orders", 0)
            pv_today     = ins.get("purchase_val", 0)
            cpa_today    = round(spend_today / orders_today) if orders_today > 0 else None
            lc   = ins.get("link_clicks", 0)
            imp  = ins.get("impressions", 0)
            rc   = ins.get("reach", 0)
            atc  = ins.get("add_to_cart", 0)
            cvr_a      = f"{orders_today / lc * 100:.1f}%" if lc > 0 else None
            atc_rate_a = f"{atc / lc * 100:.1f}%"          if lc > 0 else None
            ctr_a      = f"{lc / imp * 100:.2f}%"           if imp > 0 else None
            cpc_a      = round(spend_today / lc)            if lc > 0 else None
            cpm_reach_a = round(spend_today / rc * 1000)    if rc > 0 else None
            adj_rows.append({
                "狀":       "🟢" if c["status"] == "ACTIVE" else "⏸",
                "活動名稱": c["name"],
                "日預算":   int(c["daily_budget"]),
                "今日花費": spend_today,
                "今日ROAS": ins.get("roas"),
                "7天ROAS":  ins_7d.get("roas"),
                "今日購買": orders_today,
                "今日CPA":  cpa_today,
                "轉換價值": round(pv_today) if pv_today else None,
                "CVR":      cvr_a,
                "加車率":   atc_rate_a,
                "CTR":      ctr_a,
                "CPC":      cpc_a,
                "觸及成本": cpm_reach_a,
            })
            adj_id_list.append(c["id"])

        if not adj_rows:
            st.info("沒有符合條件的活動")
        else:
            # 排序：⏸ 最後；同層 🟢有花費 > 🟢無花費；有花費按今日ROAS desc，再7天ROAS desc
            def _adj_sort_key(pair):
                row = pair[0]
                return (
                    row["狀"] == "⏸",
                    row["今日花費"] == 0,
                    row["今日ROAS"] is None,
                    -(row["今日ROAS"] or 0),
                    row["7天ROAS"] is None,
                    -(row["7天ROAS"] or 0),
                )
            combined_adj = sorted(zip(adj_rows, adj_id_list), key=_adj_sort_key)
            adj_rows, adj_id_list = (list(z) for z in zip(*combined_adj)) if combined_adj else ([], [])

            # 格式化欄位（ROAS 轉字串）
            for row in adj_rows:
                row["今日ROAS"] = _fmt_roas(row["今日ROAS"])
                row["7天ROAS"]  = _fmt_roas(row["7天ROAS"])
                row["今日CPA"]  = f"${row['今日CPA']:,}"    if row["今日CPA"]  is not None else "—"
                row["轉換價值"] = f"${row['轉換價值']:,}"   if row["轉換價值"] is not None else "—"

            df_adj = pd.DataFrame(adj_rows)
            display_cols = ["狀", "活動名稱", "日預算", "今日花費", "今日ROAS", "7天ROAS", "今日購買", "今日CPA", "轉換價值"]

            # 快速選取按鈕
            ab1, ab2, ab3 = st.columns(3)
            def _reset_adj_sel(new_sel):
                v = st.session_state.get("adj_sel_v", 0)
                old_key = f"adj_df_{v}"
                if old_key in st.session_state:
                    del st.session_state[old_key]
                st.session_state["adj_sel"] = new_sel
                st.session_state["adj_sel_v"] = v + 1
                st.session_state["_adj_btn_set"] = True
                st.rerun()

            if ab1.button("全選", key="adj_all", use_container_width=True):
                _reset_adj_sel(set(range(len(adj_id_list))))
            if ab2.button("取消全選", key="adj_none", use_container_width=True):
                _reset_adj_sel(set())
            if ab3.button("選🟢有花費", key="adj_spend", use_container_width=True):
                _reset_adj_sel({i for i, r in enumerate(adj_rows) if r["今日花費"] > 0})

            adj_sel_indices = st.session_state.get("adj_sel", set())
            if not isinstance(adj_sel_indices, set):
                adj_sel_indices = set()

            display_cols = ["狀", "活動名稱", "日預算", "今日花費", "今日ROAS",
                            "7天ROAS", "今日購買", "今日CPA", "轉換價值",
                            "CVR", "加車率", "CTR", "CPC", "觸及成本"]
            df_adj = pd.DataFrame(adj_rows)
            adj_event = st.dataframe(
                df_adj[display_cols],
                use_container_width=True,
                hide_index=True,
                height=min(420, 50 + 40 * len(adj_rows)),
                column_config={
                    "狀":        st.column_config.TextColumn("狀",       width=40),
                    "活動名稱":  st.column_config.TextColumn("活動名稱", width=160),
                    "日預算":    st.column_config.NumberColumn("日預算",   width=80),
                    "今日花費":  st.column_config.NumberColumn("今日花費", width=80),
                    "今日ROAS":  st.column_config.TextColumn("今日ROAS", width=75),
                    "7天ROAS":   st.column_config.TextColumn("7天ROAS",  width=70),
                    "今日購買":  st.column_config.NumberColumn("今日購買", width=70),
                    "今日CPA":   st.column_config.TextColumn("今日CPA",  width=70),
                    "轉換價值":  st.column_config.TextColumn("轉換價值", width=80),
                    "CVR":       st.column_config.TextColumn("CVR",      width=70),
                    "加車率":    st.column_config.TextColumn("加車率",    width=70),
                    "CTR":       st.column_config.TextColumn("CTR",      width=70),
                    "CPC":       st.column_config.NumberColumn("CPC",    width=65),
                    "觸及成本":  st.column_config.NumberColumn("觸及成本", width=80),
                },
                on_select="rerun",
                selection_mode="multi-row",
                key=f"adj_df_{st.session_state.get('adj_sel_v', 0)}",
            )
            new_adj_sel = set(adj_event.selection.rows)
            if st.session_state.pop("_adj_btn_set", False):
                adj_sel_indices = st.session_state.get("adj_sel", set())
            elif new_adj_sel != adj_sel_indices:
                st.session_state["adj_sel"] = new_adj_sel
                adj_sel_indices = new_adj_sel

            adj_sel_indices = {i for i in adj_sel_indices if i < len(adj_id_list)}
            selected_adj_ids = {adj_id_list[i] for i in adj_sel_indices}
            sel_camps = [c for c in adj_campaigns
                         if c["id"] in selected_adj_ids and c.get("daily_budget")]
            if sel_camps:
                st.markdown(f"**已選 {len(sel_camps)} 個活動**")

                # 快捷幅度按鈕（立即套用）
                PRESETS = [("−25%", -25), ("−10%", -10), ("+10%", 10), ("+25%", 25), ("+50%", 50)]
                pc = st.columns(5)
                for i, (label, pct) in enumerate(PRESETS):
                    if pc[i].button(label, key=f"adj_preset_{pct}", use_container_width=True):
                        for camp in sel_camps:
                            res = adjust_campaign_budget(_token, camp["id"], 100 + pct)
                            if res.get("success"):
                                st.success(f"✅ {camp['name']}：${res['old_budget']:,} → ${res['new_budget']:,}（{label}）")
                            else:
                                st.error(f"❌ {camp['name']}：{res.get('error', {}).get('message', str(res))}")
                        st.session_state["adj_campaigns"]   = fetch_campaigns_with_budget(_token, selected_account_id)
                        st.session_state["adj_insights"]    = fetch_today_campaign_insights(_token, selected_account_id)
                        st.session_state["adj_insights_7d"] = fetch_today_campaign_insights(_token, selected_account_id, "last_7d")
                        st.rerun()

                # 自訂幅度 + 確認
                ca1, ca2 = st.columns([3, 2])
                with ca1:
                    adj_pct = st.number_input("調整幅度 (%)", min_value=1, max_value=10000, value=20, step=5, key="adj_pct_input")
                with ca2:
                    adj_dir = st.radio("方向", ["加碼 ⬆️", "減碼 ⬇️"], horizontal=True, key="adj_dir")
                final_pct = adj_pct if "加碼" in adj_dir else -int(adj_pct)
                sign = "+" if final_pct >= 0 else ""
                if st.button(f"{'⬆️' if final_pct >= 0 else '⬇️'} 確認套用 {sign}{final_pct}% 到 {len(sel_camps)} 個活動",
                             type="primary", use_container_width=True, key="adj_confirm"):
                    for camp in sel_camps:
                        res = adjust_campaign_budget(_token, camp["id"], 100 + final_pct)
                        if res.get("success"):
                            st.success(f"✅ {camp['name']}：${res['old_budget']:,} → ${res['new_budget']:,}")
                        else:
                            st.error(f"❌ {camp['name']}：{res.get('error', {}).get('message', str(res))}")
                    st.session_state["adj_campaigns"]   = fetch_campaigns_with_budget(_token, selected_account_id)
                    st.session_state["adj_insights"]    = fetch_today_campaign_insights(_token, selected_account_id)
                    st.session_state["adj_insights_7d"] = fetch_today_campaign_insights(_token, selected_account_id, "last_7d")
                    st.rerun()
            else:
                st.info("請在上方表格勾選要調整的活動")
    else:
        st.info("請先點「載入／重新整理」")


st.markdown("---")
st.caption("Powered by Claude Sonnet 4.6 · 毛孩時代 & 御熹堂廣告週報自動化")
