---
name: project-master-branch-orphan-2026-09-11
description: GitHub上molly-ad-report repo有main跟master兩個分支,master是沒人在用的舊版本,內容跟main衝突,先不合併不處理
metadata:
  node_type: memory
  type: project
---

2026-09-11 修 git 同步問題時發現：`mo-mhoo/molly-ad-report` 這個 repo 在 GitHub 上除了目前這台 Mac 一直在用的 `main` 分支，還有一個 `master` 分支，兩邊在 `dc50f2b`（`Merge branch 'main'...`）之後就分頭發展，從沒再合併過。

**master 分支內容**：只有 6 個 commit，碰的是 main 完全沒有的檔案（`ad_agent.py`、`ad_agent_gemini.py`、`meta_batch_upload.py`、`meta_fetcher.py`、`knowledge/` 底下一批 Meta 廣告知識庫文件），而且 `.env.example`／`CLAUDE.md`／`app.py`／`requirements.txt` 都跟 main 現在的內容衝突（試著 `git merge` 會報 conflict，已經 `git merge --abort` 沒有真的合併）。master 版的 `.env.example` 是單一 Facebook 帳號、單一 Meta 廣告帳號 ID 的簡化設定（`ANTHROPIC_API_KEY`／`META_AD_ACCOUNT_ID`／`META_PAGE_ID`），看起來像是這個專案更早期、比較陽春的方向，但問過 Molly，她自己也認不出這是不是她建的早期版本（「我不知道」）。

**已確認的安全邊界**：master 從來沒有動過 `000_Agent/skills/memory/`，所以這個分支分裂**不是**造成今天記憶不同步問題的原因——[[feedback_git_sync_setup_2026_09_11]] 記錄的「000_Agent 從沒 commit 過」才是真正原因，這兩個問題是各自獨立的。

**目前處理方式：先不動它**——不合併、不刪除 master 分支，避免在不清楚內容取捨的情況下亂動可能還在用的程式碼（尤其 app.py 是實際跑在 Streamlit Cloud 上的檔案，衝突內容需要人工判斷該留哪一版，不是我能單方面猜的）。

**How to apply**：
- 之後如果又在 GitHub 上看到 `master` 分支相關的東西（例如 PR 建議、其他 session 提到），先假設是這個已知的孤兒分支，不用當新問題重新排查一次。
- 如果 Molly 想查清楚 master 到底是什麼（例如去問其他曾經碰過這個 repo 的人、或去 Streamlit Cloud 部署設定確認實際上線的是哪個分支），查清楚之後回來更新這則記憶，記錄最終怎麼處理（合併／刪除／保留當備份）。
- 在查清楚之前，**不要自己決定合併或刪除 master**，這牽涉到可能還在用的程式碼版本取捨，需要 Molly 明確拍板。
