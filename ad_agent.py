"""
數位廣告 AI Agent
使用 Claude API + Tool Use，支援五大廣告功能
"""

import json
import os
from typing import Any, Optional

import anthropic
from dotenv import load_dotenv

load_dotenv()

client = anthropic.Anthropic()
MODEL = "claude-opus-4-7"

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

TOOLS: list[dict] = [
    {
        "name": "generate_ad_copy",
        "description": "根據產品資訊、目標受眾、投放平台和語調，生成多個廣告文案變體（主標題、描述、CTA）",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {
                    "type": "string",
                    "description": "產品或服務名稱"
                },
                "product_description": {
                    "type": "string",
                    "description": "產品核心價值與差異化特色"
                },
                "target_audience": {
                    "type": "string",
                    "description": "目標受眾描述（年齡、職業、興趣、痛點等）"
                },
                "platform": {
                    "type": "string",
                    "enum": ["Facebook", "Instagram", "Google", "TikTok", "LINE"],
                    "description": "投放平台"
                },
                "tone": {
                    "type": "string",
                    "enum": ["專業", "親切", "幽默", "緊迫", "情感"],
                    "description": "廣告語調"
                },
                "goal": {
                    "type": "string",
                    "enum": ["品牌知名度", "導流", "轉換", "再行銷"],
                    "description": "廣告目標"
                },
                "variants": {
                    "type": "integer",
                    "description": "要生成的文案變體數量（預設 3）",
                    "default": 3
                }
            },
            "required": ["product_name", "target_audience", "platform", "goal"]
        }
    },
    {
        "name": "analyze_ad_performance",
        "description": "分析廣告投放數據，識別績效問題，並提供優先級排序的優化建議",
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_name": {
                    "type": "string",
                    "description": "廣告活動名稱"
                },
                "platform": {
                    "type": "string",
                    "description": "投放平台"
                },
                "campaign_goal": {
                    "type": "string",
                    "description": "活動目標（例如：提升 ROAS、降低 CPA、增加品牌曝光）"
                },
                "metrics": {
                    "type": "object",
                    "description": "廣告指標數據",
                    "properties": {
                        "impressions": {"type": "number", "description": "曝光次數"},
                        "clicks": {"type": "number", "description": "點擊次數"},
                        "ctr": {"type": "number", "description": "點擊率 (%)"},
                        "spend": {"type": "number", "description": "花費金額（元）"},
                        "conversions": {"type": "number", "description": "轉換次數"},
                        "revenue": {"type": "number", "description": "帶來營收（元）"},
                        "cpc": {"type": "number", "description": "每次點擊費用"},
                        "cpm": {"type": "number", "description": "每千次曝光費用"},
                        "roas": {"type": "number", "description": "廣告投資報酬率"},
                        "conversion_rate": {"type": "number", "description": "轉換率 (%)"}
                    }
                },
                "industry_benchmarks": {
                    "type": "object",
                    "description": "同業基準值（選填，有助於比較分析）"
                }
            },
            "required": ["platform", "metrics", "campaign_goal"]
        }
    },
    {
        "name": "analyze_audience",
        "description": "分析目標受眾特徵，生成受眾分群策略與各分群的觸及建議",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_category": {
                    "type": "string",
                    "description": "產品類別（例如：健身 App、SaaS 工具、美妝保養品）"
                },
                "expansion_goal": {
                    "type": "string",
                    "description": "受眾擴展目標（例如：新客開發、提高 LTV、再行銷流失客）"
                },
                "current_customers": {
                    "type": "object",
                    "description": "現有客戶特徵（選填）",
                    "properties": {
                        "age_range": {"type": "string", "description": "年齡區間，例如：25-40"},
                        "gender": {"type": "string", "description": "性別分布，例如：女性 70%"},
                        "location": {"type": "string", "description": "地理位置"},
                        "interests": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "興趣標籤列表"
                        },
                        "behaviors": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "行為特徵列表"
                        }
                    }
                },
                "budget_level": {
                    "type": "string",
                    "enum": ["低（月預算 < 3萬）", "中（月預算 3-10萬）", "高（月預算 > 10萬）"],
                    "description": "預算等級"
                }
            },
            "required": ["product_category", "expansion_goal"]
        }
    },
    {
        "name": "suggest_ab_tests",
        "description": "根據現有廣告數據和瓶頸，設計有統計意義的 A/B 測試方案，包含假設、測試變數與評估指標",
        "input_schema": {
            "type": "object",
            "properties": {
                "ad_type": {
                    "type": "string",
                    "enum": ["圖片廣告", "影片廣告", "輪播廣告", "搜尋廣告", "動態廣告"],
                    "description": "廣告類型"
                },
                "platform": {
                    "type": "string",
                    "description": "投放平台"
                },
                "bottleneck": {
                    "type": "string",
                    "enum": ["點擊率低", "轉換率低", "ROAS 不達標", "受眾觸及不足", "廣告疲乏"],
                    "description": "目前主要瓶頸"
                },
                "current_performance": {
                    "type": "object",
                    "description": "目前廣告表現數據（選填）",
                    "properties": {
                        "ctr": {"type": "number", "description": "點擊率 (%)"},
                        "conversion_rate": {"type": "number", "description": "轉換率 (%)"},
                        "roas": {"type": "number", "description": "廣告投資報酬率"}
                    }
                },
                "num_suggestions": {
                    "type": "integer",
                    "description": "建議測試數量（預設 3）",
                    "default": 3
                }
            },
            "required": ["ad_type", "platform", "bottleneck"]
        }
    },
    {
        "name": "get_market_cases",
        "description": "根據產業類別和行銷目標，提供成功廣告案例分析與可直接複製的策略框架",
        "input_schema": {
            "type": "object",
            "properties": {
                "industry": {
                    "type": "string",
                    "description": "產業類別，例如：電商、SaaS、餐飲、教育、金融、美妝保養"
                },
                "marketing_goal": {
                    "type": "string",
                    "enum": ["品牌建立", "用戶獲取", "提升 ROAS", "降低 CPA", "擴大市佔"],
                    "description": "行銷目標"
                },
                "platform": {
                    "type": "string",
                    "description": "指定平台（選填，不填則跨平台）"
                },
                "company_size": {
                    "type": "string",
                    "enum": ["新創/中小企業", "成長期企業", "大型企業"],
                    "description": "企業規模"
                }
            },
            "required": ["industry", "marketing_goal"]
        }
    }
]


# ── Tool implementations ──────────────────────────────────────────────────────

def _call_claude(prompt: str, max_tokens: int = 2000) -> str:
    """Helper to call Claude for tool sub-tasks."""
    resp = client.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}]
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

    return _call_claude(f"""請為以下廣告需求生成 {variants} 個文案變體：

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

以清楚的 Markdown 格式輸出，每個變體用 --- 分隔。""", max_tokens=2500)


def _analyze_ad_performance(
    platform: str,
    metrics: dict,
    campaign_goal: str,
    campaign_name: str = "廣告活動",
    industry_benchmarks: Optional[dict] = None,
) -> str:
    # Auto-calculate derived metrics when possible
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

    return _call_claude(f"""分析此廣告活動並給出優化方向：

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
最重要的一個立即執行項目""", max_tokens=2000)


def _analyze_audience(
    product_category: str,
    expansion_goal: str,
    current_customers: Optional[dict] = None,
    budget_level: str = "中（月預算 3-10萬）",
) -> str:
    customers_section = ""
    if current_customers:
        customers_section = f"\n**現有客戶特徵：**\n{json.dumps(current_customers, ensure_ascii=False, indent=2)}"

    return _call_claude(f"""為以下情境建立受眾分群策略：

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
如何利用現有客戶建立 Lookalike 受眾（含比例建議）""", max_tokens=2000)


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

    return _call_claude(f"""為以下廣告設計 A/B 測試方案：

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
- **風險評估：** 可能的負面影響""", max_tokens=2500)


def _get_market_cases(
    industry: str,
    marketing_goal: str,
    platform: Optional[str] = None,
    company_size: str = "成長期企業",
) -> str:
    platform_context = f"，聚焦 {platform} 平台" if platform else "，跨平台案例"

    return _call_claude(f"""分享 {industry} 產業{platform_context}在「{marketing_goal}」的成功廣告案例：

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
考量規模與資源限制，最適合優先採用的策略""", max_tokens=2500)


# ── Tool dispatcher ───────────────────────────────────────────────────────────

TOOL_MAP = {
    "generate_ad_copy": _generate_ad_copy,
    "analyze_ad_performance": _analyze_ad_performance,
    "analyze_audience": _analyze_audience,
    "suggest_ab_tests": _suggest_ab_tests,
    "get_market_cases": _get_market_cases,
}

TOOL_LABELS = {
    "generate_ad_copy": "廣告文案生成",
    "analyze_ad_performance": "廣告績效分析",
    "analyze_audience": "受眾分群分析",
    "suggest_ab_tests": "A/B 測試建議",
    "get_market_cases": "市場案例查詢",
}


def execute_tool(tool_name: str, tool_input: dict[str, Any]) -> str:
    fn = TOOL_MAP.get(tool_name)
    if fn is None:
        return f"⚠️ 未知工具：{tool_name}"
    try:
        return fn(**tool_input)
    except Exception as e:
        return f"⚠️ 工具執行失敗：{e}"


# ── Agent loop ────────────────────────────────────────────────────────────────

def run_agent(user_message: str) -> None:
    """Run one turn of the advertising AI agent."""
    print(f"\n{'─'*60}")
    print(f"🧑 用戶：{user_message}")
    print(f"{'─'*60}\n")

    messages: list[dict] = [{"role": "user", "content": user_message}]

    while True:
        print("🤖 AI 顧問：", end="", flush=True)

        with client.messages.stream(
            model=MODEL,
            max_tokens=4096,
            thinking={"type": "adaptive"},
            system=[{
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},  # cache stable system prompt
            }],
            tools=TOOLS,
            messages=messages,
        ) as stream:
            for text in stream.text_stream:
                print(text, end="", flush=True)
            response = stream.get_final_message()

        print()  # newline after streamed content

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason != "tool_use":
            break

        # Append assistant turn (includes tool_use blocks)
        messages.append({"role": "assistant", "content": response.content})

        # Execute each tool call
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue

            label = TOOL_LABELS.get(block.name, block.name)
            print(f"\n  🔧 使用工具：{label}")
            print(f"     參數：{json.dumps(block.input, ensure_ascii=False, indent=5)}\n")

            result = execute_tool(block.name, block.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result,
            })
            print(f"  ✅ {label} 完成\n")

        messages.append({"role": "user", "content": tool_results})

    print(f"\n{'─'*60}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

DEMO_QUERIES = [
    "我在賣一款健身 App，目標是 25-35 歲上班族女性，想在 Instagram 投廣告提升下載量，幫我生成 3 個廣告文案，語調要親切",
    "我的 Facebook 電商廣告 CTR 1.2%，花費 80000 元，轉換 200 次，營收 280000 元，目標是提升 ROAS，請幫我分析並給優化建議",
    "我是賣 B2B SaaS 工具的，想在 Google 做廣告開發新客，幫我分析目標受眾並建立分群策略，月預算約 15 萬",
    "我的 Facebook 圖片廣告 CTR 很低，只有 0.5%，幫我設計 A/B 測試方案",
    "分享電商品牌在 Facebook 提升 ROAS 的成功案例，我們是成長期企業",
]


def main() -> None:
    print("=" * 60)
    print("  🚀 數位廣告 AI Agent")
    print("  功能：文案生成 | 投放分析 | 受眾分析 | A/B 測試 | 市場案例")
    print("=" * 60)
    print("\n💡 示例問題（輸入數字 1-5 快速體驗，或直接輸入問題）：")
    for i, q in enumerate(DEMO_QUERIES, 1):
        print(f"  {i}. {q[:55]}…")
    print("\n  輸入 quit 結束\n")

    while True:
        try:
            raw = input("你：").strip()

            if not raw:
                continue

            if raw.lower() in ("quit", "exit", "退出", "q"):
                print("\n感謝使用數位廣告 AI Agent，再見！👋")
                break

            # Allow shortcut: "1" ~ "5" to use demo queries
            if raw in ("1", "2", "3", "4", "5"):
                user_input = DEMO_QUERIES[int(raw) - 1]
                print(f"（示例問題 {raw}）\n")
            else:
                user_input = raw

            run_agent(user_input)

        except KeyboardInterrupt:
            print("\n\n感謝使用，再見！👋")
            break
        except anthropic.APIError as e:
            print(f"\n⚠️ API 錯誤：{e}\n")


if __name__ == "__main__":
    main()
