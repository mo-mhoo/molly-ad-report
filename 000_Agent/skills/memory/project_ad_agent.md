---
name: project_ad_agent
description: 數位廣告 AI Agent 專案 — 架構、功能與檔案位置
type: project
originSessionId: fb099a82-d33b-4671-9f6d-47e9b7b949f7
---
**專案位置：** C:\AI小摸\ad_agent.py

**架構：** Python + Claude API (claude-opus-4-7) + Tool Use 手動迴圈

**五大工具：**
1. `generate_ad_copy` — 廣告文案生成（支援 FB/IG/Google/TikTok/LINE）
2. `analyze_ad_performance` — 廣告投放分析（自動計算 ROAS/CPA/CTR）
3. `analyze_audience` — 受眾分群分析
4. `suggest_ab_tests` — A/B 測試方案設計
5. `get_market_cases` — 市場成功案例分享

**技術細節：**
- Adaptive thinking 已啟用
- System prompt 使用 prompt caching（ephemeral）
- 使用 streaming 回應
- 工具內部再呼叫 Claude 完成實際生成任務

**Why:** 使用者希望打造能協助廣告投放決策的 AI 顧問工具
