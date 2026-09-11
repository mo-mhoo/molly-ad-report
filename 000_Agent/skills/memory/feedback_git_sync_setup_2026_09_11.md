---
name: feedback-git-sync-setup-2026-09-11
description: Molly-AI Agent專案的memory/skill系統已修好跨裝置git同步,不用再懷疑「換裝置一錯再錯」是記憶寫法問題
metadata:
  node_type: memory
  type: feedback
---

2026-09-11 之前，`000_Agent/`（記憶＋skills）從建立以來**從來沒有被 commit 進 git**，等於這台 Mac 上的所有記憶更正，永遠不會同步到其他裝置——這才是 Molly 問「怎麼記憶 md 檔才不會換裝置一錯再錯」的真正原因，不是記憶檔案的寫法有問題。

**已修復（2026-09-11）**：
1. `000_Agent/`、`100_Todo/`、`200_Reference/`、`CLAUDE.md`、`.gitignore`、`.env.example` 已第一次 commit + push 上 GitHub（`mo-mhoo/molly-ad-report`，commit `0fd554f`）。
2. Git remote 原本內嵌一組明碼 GitHub Personal Access Token（`ghp_...`），造成 `git push` 在這個環境（non-interactive shell）直接失敗（`could not read Password... Device not configured`）。已改用 `gh auth login`（GitHub CLI，OAuth 網頁授權，token 存在系統 keyring 不是明碼）解決，並用 `git remote set-url origin https://github.com/mo-mhoo/molly-ad-report.git`（不含 token 的乾淨網址）+ `gh auth setup-git` 讓 git push/pull 自動用 gh 的憑證。
3. **舊的那組明碼 token（`ghp_` 開頭，原本嵌在 remote URL 裡那組）還沒被撤銷**，只是不再寫在 remote URL 裡了——Molly 需要自己去 GitHub Settings → Developer settings → Personal access tokens 撤銷掉這組舊的，才算徹底處理完。（這則記憶本身第一版曾誤把完整 token 字串寫進來，被 GitHub push protection 擋下才發現、已改寫成不含實際字串的版本——連寫「這裡曾經有個 token 外洩」這件事本身都要小心別又把 token 抄一次進 git。）

**How to apply（之後每次改完記憶/skill都要做）**：
- 改完 `000_Agent/skills/memory/*.md` 或任何 skill 檔案後，**要主動 `git add` + `git commit` + `git push`**，不要等 Molly 問才想起來（比照 [[feedback_push_after_edit]] 對 app.py 的做法，同一個原則套用到這裡）。
- 另一台裝置（例如 Windows 那台）要記得 `git pull` 才會拿到這邊的更正，反之亦然。
- 如果 push 又卡住報 `could not read Password`，代表 remote URL 又被寫回帶 token 的格式，或 `gh auth status` 顯示沒登入，先跑 `gh auth status` 確認，過期了就重新 `gh auth login`。
- 這個修復**只解決 git 同步**，不等於 Claude Code 內建的「自動記憶」系統（`~/.claude/projects/.../memory/`）會跨裝置同步——那一層本來就是純本機的，不受這次修復影響，這個專案真正在用的記憶是 `000_Agent/skills/memory/`，不是內建自動記憶。

另外查到一個目前沒處理的旁支：Google Drive 上有一份 2026-09-10 的 `~/.claude/` 完整快照備份（flat file 命名如 `claude-skills__memory__xxx.md`），裡面有幾個這個專案 `000_Agent/skills/memory/MEMORY.md` 索引裡沒有的記憶檔名（例如 `project_pmax_flight_autopause.md`、`feedback_mh_report_compile_full_after_organic.md`），看命名可能屬於 [[project_mh_ai_agent_setup]] 那個獨立的 MH 專案、也可能是這個專案漏收的，還�没有比對清楚，之後有空要花時間對一次，不要假設兩邊已經一致。
