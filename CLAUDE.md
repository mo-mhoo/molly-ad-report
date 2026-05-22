# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 環境設定

```bash
pip install -r requirements.txt
cp .env.example .env  # 填入 ANTHROPIC_API_KEY
```

## 執行方式

```bash
python ad_agent.py
```

在提示符號輸入問題，或輸入 `1`–`5` 執行內建示範查詢，輸入 `quit` 結束。

## 架構說明

本專案是單一檔案的 CLI Agent（`ad_agent.py`），基於 Anthropic Python SDK，使用 Tool Use 與串流輸出。

**Agent 主迴圈**（`run_agent`）：以 `while True` 將使用者輸入傳送給 Claude。若回應的 `stop_reason == "tool_use"`，則將每個工具呼叫分派給 `execute_tool`，並將結果以 `tool_result` 角色附加回對話後繼續；收到 `end_turn` 或其他停止原因時結束。

**工具為兩層設計**：`TOOLS` 中宣告的五個工具（Claude 看到的 JSON Schema）透過 `TOOL_MAP` 一對一對應至私有 Python 函式（`_generate_ad_copy`、`_analyze_ad_performance` 等）。每個實作函式組合提示詞後呼叫 `_call_claude`——一個非串流的巢狀 Claude API 呼叫——因此工具執行本身也是一次 Claude API 呼叫。

**模型設定**：使用 `claude-opus-4-7`，開啟 `thinking: {"type": "adaptive"}`，並對系統提示詞套用 ephemeral cache 以降低多輪對話成本。

**無狀態持久化**——每次呼叫 `run_agent` 都是獨立對話。
