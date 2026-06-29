"""
數位廣告 AI Agent — Streamlit Web UI
"""

import json
import os
from typing import Any, Optional

import anthropic
import requests
import streamlit as st
from dotenv import load_dotenv

import meta_fetcher

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="數位廣告 AI Agent",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Claude client ─────────────────────────────────────────────────────────────

@st.cache_resource
def get_client() -> anthropic.Anthropic:
    try:
        api_key = st.secrets.get("ANTHROPIC_API_KEY", None)
    except Exception:
        api_key = None
    api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        st.error("請設定 ANTHROPIC_API_KEY（Streamlit Secrets 或 .env）")
        st.stop()
    return anthropic.Anthropic(api_key=api_key)


client = get_client()
MODEL = "claude-opus-4-7"

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """你是一位專業的數位廣告 AI 顧問，深耕 Facebook、Google、Instagram、TikTok、LINE 等平台的廣告策略。

你的核心能力：
1. **廣告文案生成** — 根據產品、受眾、平台生成高轉換率廣告文案與 CTA
2. **廣告投放分析** — 解讀 CTR、ROAS、CPC、CPM 等指標，提供具體優化行動
3. **受眾分析** — 協助定義目標受眾特徵，建立精準的受眾分群策略
4. **A/B 測試建議** — 根據現有數據設計有統計意義的測試方案
5. **市場案例分享** — 分享同業成功廣告策略，提供可直接複製的框架

工作原則：
- 給出具體可執行的建議，避免空泛回答
- 數據導向思考，先診斷問題再給方向
- 考量各平台受眾習慣與廣告格式差異
- 廣告文案需符合各平台政策規範
- 使用繁體中文回覆

遇到需要進行分析、生成文案或查找案例時，主動使用對應工具完成任務。"""

# ── Tool schemas ──────────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "generate_ad_copy",
        "description": "根據產品資訊、目標受眾、投放平台和語調，生成多個廣告文案變體（主標題、描述、CTA）",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "產品或服務名稱"},
                "product_description": {"type": "string", "description": "產品核心價值與差異化特色"},
                "target_audience": {"type": "string", "description": "目標受眾描述（年齡、職業、興趣、痛點等）"},
                "platform": {
                    "type": "string",
                    "enum": ["Facebook", "Instagram", "Google", "TikTok", "LINE"],
                    "description": "投放平台",
                },
                "tone": {
                    "type": "string",
                    "enum": ["專業", "親切", "幽默", "緊迫", "情感"],
                    "description": "廣告語調",
                },
                "goal": {
                    "type": "string",
                    "enum": ["品牌知名度", "導流", "轉換", "再行銷"],
                    "description": "廣告目標",
                },
                "variants": {"type": "integer", "description": "要生成的文案變體數量（預設 3）", "default": 3},
            },
            "required": ["product_name", "target_audience", "platform", "goal"],
        },
    },
    {
        "name": "analyze_ad_performance",
        "description": "分析廣告投放數據，識別績效問題，並提供優先級排序的優化建議",
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_name": {"type": "string", "description": "廣告活動名稱"},
                "platform": {"type": "string", "description": "投放平台"},
                "campaign_goal": {"type": "string", "description": "活動目標"},
                "metrics": {
                    "type": "object",
                    "description": "廣告指標數據",
                    "properties": {
                        "impressions": {"type": "number"},
                        "clicks": {"type": "number"},
                        "ctr": {"type": "number", "description": "點擊率 (%)"},
                        "spend": {"type": "number"},
                        "conversions": {"type": "number", "description": "購買次數"},
                        "revenue": {"type": "number"},
                        "cpc": {"type": "number"},
                        "cpm": {"type": "number"},
                        "roas": {"type": "number"},
                        "cpa": {"type": "number"},
                        "conversion_rate": {"type": "number"},
                        "cvr_click_to_purchase": {"type": "number", "description": "CVR 點>買 (%)"},
                        "cvr_click_to_cart": {"type": "number", "description": "CVR 點>車 (%)"},
                        "cvr_cart_to_purchase": {"type": "number", "description": "CVR 車>買 (%)"},
                        "cart_count": {"type": "number", "description": "購物車次數"},
                        "cart_cpa": {"type": "number", "description": "購物車 CPA"},
                    },
                },
                "industry_benchmarks": {"type": "object", "description": "同業基準值（選填）"},
            },
            "required": ["platform", "metrics", "campaign_goal"],
        },
    },
    {
        "name": "analyze_audience",
        "description": "分析目標受眾特徵，生成受眾分群策略與各分群的觸及建議",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_category": {"type": "string", "description": "產品類別"},
                "expansion_goal": {"type": "string", "description": "受眾擴展目標"},
                "current_customers": {
                    "type": "object",
                    "description": "現有客戶特徵（選填）",
                    "properties": {
                        "age_range": {"type": "string"},
                        "gender": {"type": "string"},
                        "location": {"type": "string"},
                        "interests": {"type": "array", "items": {"type": "string"}},
                        "behaviors": {"type": "array", "items": {"type": "string"}},
                    },
                },
                "budget_level": {
                    "type": "string",
                    "enum": ["低（月預算 < 3萬）", "中（月預算 3-10萬）", "高（月預算 > 10萬）"],
                    "description": "預算等級",
                },
            },
            "required": ["product_category", "expansion_goal"],
        },
    },
    {
        "name": "suggest_ab_tests",
        "description": "根據現有廣告數據和瓶頸，設計有統計意義的 A/B 測試方案",
        "input_schema": {
            "type": "object",
            "properties": {
                "ad_type": {
                    "type": "string",
                    "enum": ["圖片廣告", "影片廣告", "輪播廣告", "搜尋廣告", "動態廣告"],
                    "description": "廣告類型",
                },
                "platform": {"type": "string", "description": "投放平台"},
                "bottleneck": {
                    "type": "string",
                    "enum": ["點擊率低", "轉換率低", "ROAS 不達標", "受眾觸及不足", "廣告疲乏"],
                    "description": "目前主要瓶頸",
                },
                "current_performance": {
                    "type": "object",
                    "description": "目前廣告表現數據（選填）",
                    "properties": {
                        "ctr": {"type": "number"},
                        "conversion_rate": {"type": "number"},
                        "roas": {"type": "number"},
                    },
                },
                "num_suggestions": {"type": "integer", "description": "建議測試數量（預設 3）", "default": 3},
            },
            "required": ["ad_type", "platform", "bottleneck"],
        },
    },
    {
        "name": "get_market_cases",
        "description": "根據產業類別和行銷目標，提供成功廣告案例分析與可直接複製的策略框架",
        "input_schema": {
            "type": "object",
            "properties": {
                "industry": {"type": "string", "description": "產業類別"},
                "marketing_goal": {
                    "type": "string",
                    "enum": ["品牌建立", "用戶獲取", "提升 ROAS", "降低 CPA", "擴大市佔"],
                    "description": "行銷目標",
                },
                "platform": {"type": "string", "description": "指定平台（選填）"},
                "company_size": {
                    "type": "string",
                    "enum": ["新創/中小企業", "成長期企業", "大型企業"],
                    "description": "企業規模",
                },
            },
            "required": ["industry", "marketing_goal"],
        },
    },
    {
        "name": "analyze_by_dimension",
        "description": (
            "分析廣告數據按單一維度拆分的表現差異，支援維度：新舊客、素材類型、媒材、日期、品項。"
            "每列包含花費、收益、ROAS、CPA、購買次數、CVR點>買、CVR點>車、CVR車>買、購物車次數、購物車CPA、點擊、CTR、CPC、CPM。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dimension": {
                    "type": "string",
                    "enum": ["新舊客", "素材類型", "媒材", "日期", "品項", "廣告組合", "週", "月"],
                    "description": "分析維度",
                },
                "rows": {
                    "type": "array",
                    "description": "各維度值的數據列",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string", "description": "維度值，如：新客、比較文、影片、2026-04-20、關節粉"},
                            "spend": {"type": "number", "description": "花費"},
                            "revenue": {"type": "number", "description": "收益"},
                            "roas": {"type": "number", "description": "ROAS"},
                            "cpa": {"type": "number", "description": "CPA"},
                            "purchases": {"type": "number", "description": "購買次數"},
                            "cvr_click_to_purchase": {"type": "number", "description": "CVR 點>買 (%)"},
                            "cvr_click_to_cart": {"type": "number", "description": "CVR 點>車 (%)"},
                            "cvr_cart_to_purchase": {"type": "number", "description": "CVR 車>買 (%)"},
                            "cart_count": {"type": "number", "description": "購物車次數"},
                            "cart_cpa": {"type": "number", "description": "購物車 CPA"},
                            "clicks": {"type": "number", "description": "點擊"},
                            "ctr": {"type": "number", "description": "CTR (%)"},
                            "cpc": {"type": "number", "description": "CPC"},
                            "cpm": {"type": "number", "description": "CPM"},
                            "aov": {"type": "number", "description": "平均客單價 AOV"},
                            "frequency": {"type": "number", "description": "頻次"},
                            "cvr": {"type": "number", "description": "整體 CVR (%)，週/月維度使用"},
                        },
                        "required": ["label"],
                    },
                },
                "campaign_goal": {"type": "string", "description": "廣告目標，如：提升 ROAS、降低 CPA"},
                "platform": {"type": "string", "description": "投放平台（選填）"},
            },
            "required": ["dimension", "rows", "campaign_goal"],
        },
    },
]

TOOL_LABELS = {
    "generate_ad_copy": "廣告文案生成",
    "analyze_ad_performance": "廣告績效分析",
    "analyze_audience": "受眾分群分析",
    "suggest_ab_tests": "A/B 測試建議",
    "get_market_cases": "市場案例查詢",
    "analyze_by_dimension": "維度拆解分析",
}

TOOL_ICONS = {
    "generate_ad_copy": "✍️",
    "analyze_ad_performance": "📊",
    "analyze_audience": "👥",
    "suggest_ab_tests": "🔬",
    "get_market_cases": "📚",
    "analyze_by_dimension": "🔍",
}

# ── Tool implementations ──────────────────────────────────────────────────────

def _call_claude(prompt: str, max_tokens: int = 2000) -> str:
    resp = client.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text


def _generate_ad_copy(
    product_name: str,
    target_audience: str,
    platform: str,
    goal: str,
    product_description: str = "",
    tone: str = "親切",
    variants: int = 3,
) -> str:
    platform_hints = {
        "Facebook": "主標題 ≤ 40 字，描述 ≤ 125 字，善用社群互動感",
        "Instagram": "主標題簡潔有力，視覺描述強，善用 emoji，hashtag 置於最後",
        "Google": "主標題 ≤ 30 字（搜尋廣告 3 個標題），描述 ≤ 90 字，包含關鍵字",
        "TikTok": "開場前 3 秒要抓眼球，口語化、年輕化，強調娛樂性",
        "LINE": "訊息感強，友善親近，CTA 明確，考量行動端閱讀習慣",
    }
    hint = platform_hints.get(platform, "")
    return _call_claude(
        f"""請為以下廣告需求生成 {variants} 個文案變體：

產品：{product_name}
產品描述：{product_description or "（未提供，請根據產品名稱合理推測）"}
目標受眾：{target_audience}
投放平台：{platform}（{hint}）
廣告目標：{goal}
語調：{tone}

每個變體請包含：
1. **主標題（Headline）**
2. **副標題/描述（Description）**
3. **行動呼籲（CTA）**
4. **創意策略說明**（一行，說明這個變體的差異化角度）

以清楚的 Markdown 格式輸出，每個變體用 --- 分隔。""",
        max_tokens=2500,
    )


def _analyze_ad_performance(
    platform: str,
    metrics: dict,
    campaign_goal: str,
    campaign_name: str = "廣告活動",
    industry_benchmarks: Optional[dict] = None,
) -> str:
    m = dict(metrics)
    if m.get("clicks") and m.get("impressions") and not m.get("ctr"):
        m["ctr"] = round(m["clicks"] / m["impressions"] * 100, 2)
    if m.get("revenue") and m.get("spend") and m["spend"] > 0 and not m.get("roas"):
        m["roas"] = round(m["revenue"] / m["spend"], 2)
    if m.get("spend") and m.get("conversions") and m["conversions"] > 0:
        m["cpa"] = round(m["spend"] / m["conversions"], 2)
    if m.get("clicks") and m.get("conversions") and not m.get("conversion_rate"):
        m["conversion_rate"] = round(m["conversions"] / m["clicks"] * 100, 2)

    benchmarks_section = ""
    if industry_benchmarks:
        benchmarks_section = f"\n**同業基準：** {json.dumps(industry_benchmarks, ensure_ascii=False)}"

    return _call_claude(
        f"""分析此廣告活動並給出優化方向：

**活動：** {campaign_name}｜**平台：** {platform}｜**目標：** {campaign_goal}

**指標數據：**
{json.dumps(m, ensure_ascii=False, indent=2)}{benchmarks_section}

請輸出：
## 績效診斷
用表格列出各指標：指標名稱 | 數值 | 評級（✅好 / ⚠️普通 / ❌需改善）| 說明

## 問題識別
列出 2-3 個最關鍵問題（按優先順序）

## 優化行動建議
針對每個問題給出具體行動步驟，並估計預期改善幅度

## 本週優先行動
最重要的一個立即執行項目""",
        max_tokens=2000,
    )


def _analyze_audience(
    product_category: str,
    expansion_goal: str,
    current_customers: Optional[dict] = None,
    budget_level: str = "中（月預算 3-10萬）",
) -> str:
    customers_section = ""
    if current_customers:
        customers_section = f"\n**現有客戶特徵：**\n{json.dumps(current_customers, ensure_ascii=False, indent=2)}"

    return _call_claude(
        f"""為以下情境建立受眾分群策略：

**產品類別：** {product_category}
**擴展目標：** {expansion_goal}
**預算等級：** {budget_level}{customers_section}

請輸出：
## 受眾分群建議（3-4 個）
每個分群包含：
- **分群名稱與畫像描述**
- 預估規模與購買潛力
- 推薦觸及管道與廣告格式
- 適合的廣告訊息角度與痛點

## 優先開發順序
說明建議從哪個分群切入，以及原因

## 受眾排除建議
建議排除哪些受眾以提升廣告效率

## 相似受眾策略
如何利用現有客戶建立 Lookalike 受眾（含比例建議）""",
        max_tokens=2000,
    )


def _suggest_ab_tests(
    ad_type: str,
    platform: str,
    bottleneck: str,
    current_performance: Optional[dict] = None,
    num_suggestions: int = 3,
) -> str:
    perf_section = ""
    if current_performance:
        perf_section = f"\n**當前表現：** {json.dumps(current_performance, ensure_ascii=False)}"

    return _call_claude(
        f"""為以下廣告設計 A/B 測試方案：

**廣告類型：** {ad_type}｜**平台：** {platform}｜**主要瓶頸：** {bottleneck}{perf_section}

請設計 {num_suggestions} 個測試方案，按優先順序排列，每個包含：

### 測試 N：[測試名稱]
- **測試假設：** 改變 X 可以解決 Y，因為…
- **控制組：** 現況描述
- **測試組：** 具體改變的內容
- **單一測試變數：** 只改動一個要素（確保純粹性）
- **主要評估指標** 與次要指標
- **所需樣本量：** 估計需要多少曝光/點擊達到統計顯著
- **預期改善幅度：** 如果假設正確
- **風險評估：** 可能的負面影響""",
        max_tokens=2500,
    )


def _get_market_cases(
    industry: str,
    marketing_goal: str,
    platform: Optional[str] = None,
    company_size: str = "成長期企業",
) -> str:
    platform_context = f"，聚焦 {platform} 平台" if platform else "，跨平台案例"
    return _call_claude(
        f"""分享 {industry} 產業{platform_context}在「{marketing_goal}」的成功廣告案例：

**企業規模：** {company_size}

請提供 2-3 個案例，每個包含：

### 案例 N：[品牌/活動名稱]
- **背景與挑戰：** 企業面臨的情境與問題
- **策略核心：** 採用的主要廣告策略
- **執行重點：** 文案角度、受眾設定、出價策略、創意格式等關鍵細節
- **成效結果：** 具體數字或相對改善幅度
- **可複製的做法：** 其他品牌可以直接借鑑的 3 個具體動作

---

## 成功共同因素
整理以上案例的共同關鍵成功要素

## 給 {company_size} 的特別建議
考量規模與資源限制，最適合優先採用的策略""",
        max_tokens=2500,
    )


def _analyze_by_dimension(
    dimension: str,
    rows: list[dict],
    campaign_goal: str,
    platform: str = "Facebook",
) -> str:
    rows_text = json.dumps(rows, ensure_ascii=False, indent=2)
    total_spend = sum(r.get("spend", 0) for r in rows)

    is_time_dim = dimension in ("日期", "週", "月")
    comparison_cols = (
        f"{dimension} | 花費 | ROAS | CPA | CVR | AOV | 點>車% | 車>買% | 頻次 | 趨勢"
        if is_time_dim
        else f"{dimension} | 花費 | 花費占比 | ROAS | CPA | CVR點>買 | CTR | CPM | 評級（🟢/🟡/🔴）"
    )
    trend_section = (
        "\n## 趨勢判讀\n說明 ROAS、CPA、CVR 的走向，指出轉折點與原因推測"
        if is_time_dim
        else ""
    )

    return _call_claude(
        f"""分析以下廣告數據按【{dimension}】維度的表現差異：

**平台：** {platform}｜**目標：** {campaign_goal}｜**總花費：** ${total_spend:,.0f}

**數據（各維度值）：**
{rows_text}

請輸出：
## {dimension} 績效比較表
表格欄位：{comparison_cols}
{trend_section}
## 關鍵發現
列出 3 個最重要的發現（表現差距、異常值、隱藏機會）

## 預算重配建議
根據 ROAS 與 CVR 數據，具體說明哪個維度值應加碼、哪個應縮減，附上建議比例（時間維度則改為節奏建議）

## 深挖方向
針對表現最好與最差的維度值，建議下一步要怎麼進一步測試或優化

## 立即行動
最值得本週執行的 1 個調整（具體到操作步驟）""",
        max_tokens=2000,
    )


TOOL_MAP = {
    "generate_ad_copy": _generate_ad_copy,
    "analyze_ad_performance": _analyze_ad_performance,
    "analyze_audience": _analyze_audience,
    "suggest_ab_tests": _suggest_ab_tests,
    "get_market_cases": _get_market_cases,
    "analyze_by_dimension": _analyze_by_dimension,
}


def execute_tool(tool_name: str, tool_input: dict[str, Any]) -> str:
    fn = TOOL_MAP.get(tool_name)
    if fn is None:
        return f"⚠️ 未知工具：{tool_name}"
    try:
        return fn(**tool_input)
    except Exception as e:
        return f"⚠️ 工具執行失敗：{e}"


# ── Demo queries ──────────────────────────────────────────────────────────────

DEMO_QUERIES = [
    "我在賣一款健身 App，目標是 25-35 歲上班族女性，想在 Instagram 投廣告提升下載量，幫我生成 3 個廣告文案，語調要親切",
    "我的 Facebook 電商廣告 CTR 1.2%，花費 80000 元，轉換 200 次，營收 280000 元，目標是提升 ROAS，請分析並給優化建議",
    "我是賣 B2B SaaS 工具的，想在 Google 做廣告開發新客，幫我分析目標受眾並建立分群策略，月預算約 15 萬",
    "我的 Facebook 圖片廣告 CTR 很低，只有 0.5%，幫我設計 A/B 測試方案",
    "分享電商品牌在 Facebook 提升 ROAS 的成功案例，我們是成長期企業",
]

TOOL_DESCRIPTIONS = {
    "generate_ad_copy": "根據產品與受眾生成多平台高轉換廣告文案",
    "analyze_ad_performance": "解讀 CTR/ROAS/CPC 等指標並給具體優化方向",
    "analyze_audience": "定義目標受眾，建立精準分群與觸及策略",
    "suggest_ab_tests": "設計有統計意義的 A/B 測試假設與方案",
    "get_market_cases": "分享同業成功廣告案例與可複製策略框架",
    "analyze_by_dimension": "按新舊客、素材、媒材、廣告組合、品項、週/月拆解表現差異",
}

# ── Session state ─────────────────────────────────────────────────────────────

if "api_messages" not in st.session_state:
    st.session_state.api_messages = []

# Meta credentials (persist across reruns, cleared on browser close)
if "meta_token" not in st.session_state:
    try:
        st.session_state.meta_token = st.secrets.get("META_ACCESS_TOKEN", "") or ""
    except Exception:
        st.session_state.meta_token = os.getenv("META_ACCESS_TOKEN", "")
if "meta_account_id" not in st.session_state:
    try:
        st.session_state.meta_account_id = st.secrets.get("META_AD_ACCOUNT_ID", "") or ""
    except Exception:
        st.session_state.meta_account_id = os.getenv("META_AD_ACCOUNT_ID", "")

# display_history: list of dicts with key "kind"
# kind == "user":      {"kind": "user", "content": str}
# kind == "assistant": {"kind": "assistant", "content": str}
# kind == "tool":      {"kind": "tool", "tool_name": str, "input": dict, "result": str}
if "display_history" not in st.session_state:
    st.session_state.display_history = []

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🚀 數位廣告 AI Agent")
    st.caption("由 Claude claude-opus-4-7 驅動 · 五大廣告功能")
    st.divider()

    if st.button("🗑️ 清除對話記錄", use_container_width=True, type="secondary"):
        st.session_state.api_messages = []
        st.session_state.display_history = []
        st.rerun()

    st.markdown("### 💡 快速示例")
    for i, q in enumerate(DEMO_QUERIES, 1):
        label = f"{i}. {q[:35]}…"
        if st.button(label, key=f"demo_{i}", use_container_width=True):
            st.session_state.pending_input = q
            st.rerun()

    st.divider()

    # ── Meta API 數據抓取 ─────────────────────────────────────────────────────
    with st.expander("📥 從 Meta 抓數據", expanded=False):
        st.caption("填入憑證後，一鍵抓取指定維度數據並送去 AI 分析")

        meta_token = st.text_input(
            "Access Token",
            value=st.session_state.meta_token,
            type="password",
            placeholder="EAA...",
            key="meta_token_input",
        )
        meta_account = st.text_input(
            "Ad Account ID",
            value=st.session_state.meta_account_id,
            placeholder="123456789（不含 act_ 前綴）",
            key="meta_account_input",
        )

        col_l, col_r = st.columns(2)
        with col_l:
            date_from = st.date_input("開始日期", key="meta_date_from")
        with col_r:
            date_to = st.date_input("結束日期", key="meta_date_to")

        dimension = st.selectbox(
            "分析維度",
            options=list(meta_fetcher._FETCHERS.keys()),
            key="meta_dimension",
        )

        campaign_goal = st.text_input(
            "廣告目標",
            value="提升 ROAS",
            placeholder="例：提升 ROAS、降低 CPA",
            key="meta_goal",
        )

        if st.button("🚀 抓取並分析", use_container_width=True, type="primary"):
            if not meta_token or not meta_account:
                st.error("請填入 Access Token 與 Ad Account ID")
            elif date_from > date_to:
                st.error("開始日期不能晚於結束日期")
            else:
                # Persist credentials in session
                st.session_state.meta_token = meta_token
                st.session_state.meta_account_id = meta_account

                with st.spinner(f"正在從 Meta API 抓取【{dimension}】數據…"):
                    try:
                        rows = meta_fetcher.fetch_dimension(
                            dimension=dimension,
                            account_id=meta_account,
                            access_token=meta_token,
                            date_from=date_from,
                            date_to=date_to,
                        )
                        if not rows:
                            st.warning("沒有抓到數據，請確認日期範圍與帳戶 ID")
                        else:
                            msg = (
                                f"以下是從 Meta API 抓取的【{dimension}】維度數據"
                                f"（{date_from} ~ {date_to}），請幫我深度分析：\n\n"
                                f"```json\n{json.dumps(rows, ensure_ascii=False, indent=2)}\n```"
                            )
                            st.session_state.pending_input = msg
                            st.rerun()
                    except Exception as e:
                        st.error(f"抓取失敗：{e}")

    st.divider()
    st.markdown("### 🔧 可用功能")
    for name, label in TOOL_LABELS.items():
        icon = TOOL_ICONS[name]
        st.markdown(f"**{icon} {label}**")
        st.caption(TOOL_DESCRIPTIONS[name])

# ── Interest search helper ────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def search_meta_interests(query: str, token: str, limit: int = 20) -> list[dict]:
    resp = requests.get(
        "https://graph.facebook.com/v21.0/search",
        params={
            "type": "adinterest",
            "q": query,
            "limit": limit,
            "locale": "zh_TW",
            "access_token": token,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


@st.cache_data(ttl=300, show_spinner=False)
def get_meta_interest_suggestions(interest_ids: tuple[str, ...], token: str) -> list[dict]:
    import json as _json
    resp = requests.get(
        "https://graph.facebook.com/v21.0/search",
        params={
            "type": "adinterestsuggestion",
            "interest_list": _json.dumps(list(interest_ids)),
            "access_token": token,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


# ── Main tabs ─────────────────────────────────────────────────────────────────

st.title("🚀 數位廣告工具箱")
tab_agent, tab_interest = st.tabs(["💬 廣告 AI 顧問", "🎯 興趣標籤查詢"])

# ══════════════════════════════════════════════════════════════════════════════
# Tab 1 — 廣告 AI 顧問
# ══════════════════════════════════════════════════════════════════════════════

with tab_agent:
    st.caption("輸入廣告問題，AI 自動選用工具，給出具體可執行建議")
    st.divider()

    for item in st.session_state.display_history:
        kind = item["kind"]
        if kind == "user":
            with st.chat_message("user"):
                st.write(item["content"])
        elif kind == "assistant":
            with st.chat_message("assistant"):
                st.markdown(item["content"])
        elif kind == "tool":
            icon = TOOL_ICONS.get(item["tool_name"], "🔧")
            label = TOOL_LABELS.get(item["tool_name"], item["tool_name"])
            with st.expander(f"{icon} {label} — 點擊展開結果", expanded=False):
                with st.container():
                    st.markdown("**輸入參數**")
                    st.json(item["input"])
                st.divider()
                st.markdown(item["result"])

    user_input: Optional[str] = st.chat_input("例如：幫我生成 Instagram 廣告文案、分析 Facebook 廣告數據…")

    if "pending_input" in st.session_state:
        user_input = st.session_state.pop("pending_input")

    if user_input:
        with st.chat_message("user"):
            st.write(user_input)

        st.session_state.display_history.append({"kind": "user", "content": user_input})
        st.session_state.api_messages.append({"role": "user", "content": user_input})

        messages: list[dict] = list(st.session_state.api_messages)

        while True:
            assistant_bubble = st.chat_message("assistant")
            text_placeholder = assistant_bubble.empty()
            stream_text = ""

            with client.messages.stream(
                model=MODEL,
                max_tokens=4096,
                thinking={"type": "adaptive"},
                system=[
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                tools=TOOLS,
                messages=messages,
            ) as stream:
                for chunk in stream.text_stream:
                    stream_text += chunk
                    text_placeholder.markdown(stream_text + "▌")
                response = stream.get_final_message()

            if stream_text:
                text_placeholder.markdown(stream_text)
            else:
                text_placeholder.empty()

            if response.stop_reason == "end_turn":
                messages.append({"role": "assistant", "content": response.content})
                if stream_text:
                    st.session_state.display_history.append(
                        {"kind": "assistant", "content": stream_text}
                    )
                break

            if response.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                if stream_text:
                    st.session_state.display_history.append(
                        {"kind": "assistant", "content": stream_text}
                    )

                tool_results = []
                for block in response.content:
                    if block.type != "tool_use":
                        continue

                    label = TOOL_LABELS.get(block.name, block.name)
                    icon = TOOL_ICONS.get(block.name, "🔧")

                    with st.status(f"{icon} {label}…", expanded=True) as status:
                        st.markdown("**輸入參數**")
                        st.json(block.input)
                        result = execute_tool(block.name, block.input)
                        status.update(
                            label=f"{icon} {label} 完成",
                            state="complete",
                            expanded=False,
                        )

                    st.session_state.display_history.append(
                        {
                            "kind": "tool",
                            "tool_name": block.name,
                            "input": block.input,
                            "result": result,
                        }
                    )

                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        }
                    )

                messages.append({"role": "user", "content": tool_results})
            else:
                break

        st.session_state.api_messages = messages

# ══════════════════════════════════════════════════════════════════════════════
# Tab 2 — 興趣標籤查詢
# ══════════════════════════════════════════════════════════════════════════════

with tab_interest:
    st.caption("直接查詢 Meta 可用的興趣標籤，支援關鍵字搜尋與推薦標籤")

    # Token 設定
    interest_token = (
        st.session_state.get("meta_token")
        or st.secrets.get("META_ACCESS_TOKEN", "")
        or os.getenv("META_ACCESS_TOKEN", "")
    )
    if not interest_token:
        st.warning("請先在側邊欄填入 Meta Access Token，或在 Streamlit Secrets 設定 META_ACCESS_TOKEN")
        st.stop()

    # 快速選詞
    QUICK_KEYWORDS = ["pregnancy", "new mom", "parenting", "baby shower", "breastfeeding", "married women", "postpartum", "育兒", "媽媽", "孕婦"]
    st.markdown("**快速選詞**")
    cols = st.columns(5)
    for i, kw in enumerate(QUICK_KEYWORDS):
        if cols[i % 5].button(kw, key=f"quick_{kw}", use_container_width=True):
            st.session_state.interest_query = kw

    st.divider()

    # 搜尋框
    query = st.text_input(
        "輸入關鍵字（中英文皆可，英文結果通常更多）",
        value=st.session_state.get("interest_query", ""),
        placeholder="例：pregnancy、媽媽、parenting…",
        key="interest_query_input",
    )

    col_search, col_suggest = st.columns([1, 1])
    do_search = col_search.button("🔍 搜尋標籤", type="primary", use_container_width=True)
    do_suggest = col_suggest.button("✨ 推薦相關標籤", use_container_width=True,
                                    help="根據已加入的標籤取得 Meta 推薦")

    # 已選標籤
    if "selected_interests" not in st.session_state:
        st.session_state.selected_interests = []  # list of {"id": str, "name": str}

    # 執行搜尋
    if do_search and query:
        with st.spinner(f"搜尋「{query}」中…"):
            try:
                results = search_meta_interests(query, interest_token)
                st.session_state.interest_results = results
                st.session_state.interest_query = query
            except Exception as e:
                st.error(f"API 錯誤：{e}")
                st.session_state.interest_results = []

    # 執行推薦
    if do_suggest:
        if not st.session_state.selected_interests:
            st.warning("請先加入至少一個標籤再取得推薦")
        else:
            ids = tuple(item["id"] for item in st.session_state.selected_interests)
            with st.spinner("取得推薦標籤中…"):
                try:
                    results = get_meta_interest_suggestions(ids, interest_token)
                    st.session_state.interest_results = results
                except Exception as e:
                    st.error(f"API 錯誤：{e}")

    # 顯示搜尋結果
    if st.session_state.get("interest_results"):
        results = st.session_state.interest_results
        st.markdown(f"#### 搜尋結果（{len(results)} 筆）")

        selected_ids = {item["id"] for item in st.session_state.selected_interests}

        for item in results:
            item_id = item.get("id", "")
            name = item.get("name", "")
            low = item.get("audience_size_lower_bound", 0)
            high = item.get("audience_size_upper_bound", 0)
            size_str = f"{low / 1_000_000:.1f}M ~ {high / 1_000_000:.1f}M" if low >= 1_000_000 else f"{low:,} ~ {high:,}" if low else "N/A"
            path = " › ".join(item.get("path", []))

            col_name, col_size, col_btn = st.columns([4, 3, 1])
            col_name.markdown(f"**{name}**{'  \n`' + path + '`' if path else ''}")
            col_size.markdown(f"👥 {size_str}")
            already_added = item_id in selected_ids
            if col_btn.button(
                "✓" if already_added else "＋",
                key=f"add_{item_id}",
                disabled=already_added,
                use_container_width=True,
            ):
                st.session_state.selected_interests.append({"id": item_id, "name": name})
                st.rerun()

    # 已選標籤清單
    st.divider()
    st.markdown("#### 已選標籤清單")
    if not st.session_state.selected_interests:
        st.caption("尚未加入任何標籤，點擊上方結果的 ＋ 加入")
    else:
        tag_cols = st.columns(4)
        to_remove = None
        for i, item in enumerate(st.session_state.selected_interests):
            if tag_cols[i % 4].button(f"✕ {item['name']}", key=f"rm_{item['id']}_{i}", use_container_width=True):
                to_remove = i
        if to_remove is not None:
            st.session_state.selected_interests.pop(to_remove)
            st.rerun()

        st.markdown("")
        names_text = "\n".join(f"• {item['name']}" for item in st.session_state.selected_interests)
        st.text_area("複製標籤名稱", value=names_text, height=120, key="copy_area")

        if st.button("🗑️ 清空清單", type="secondary"):
            st.session_state.selected_interests = []
            st.rerun()
