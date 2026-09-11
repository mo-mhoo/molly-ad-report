---
name: project-maohai-official-campaign-structure
description: "毛孩時代_官網帳號(1318362572209550)的 campaign 結構分散在三種平行邏輯,新素材上稿要同時鋪三種"
metadata: 
  node_type: memory
  type: project
  originSessionId: 66910583-5c1e-4bd4-a6b9-7cad5c066d82
  modified: 2026-08-19T02:39:07.581Z
---

毛孩時代_官網帳號(act_1318362572209550)裡,同一支新素材理論上要同時鋪到三種平行邏輯的 campaign,彼此命名習慣不統一,[[meta-ad-copy]] skill 的自動比對(`find_candidate_campaigns`,只抓 campaign 名稱最後一段關鍵字)只能抓到第一種:

1. **BTL 舊客**:`轉換BTL｜舊客｜貓` / `轉換BTL｜舊客｜狗` — 依動物種類分,adset 名稱如「舊客｜貓｜排除PUR(30)」。這組可以被自動比對抓到。
2. **ASC 成分 bucket**:`轉換BTL｜ASC｜化毛/情緒/葉黃素/腎臟/肉泥/免疫力`(campaign id 120234621211530026)— 單一 campaign 底下依成分細分 adset:ASC｜化毛、ASC｜情緒粉、ASC｜葉黃素、ASC｜腎臟、ASC｜肉泥、ASC｜免疫力。因為 campaign 名稱最後一段是整串成分列表,不會被自動比對抓到,要手動搜 campaign 名稱含目標成分關鍵字去找。
3. **ATL 新客 Interact**:`流量ATL(Interact)｜新客`(campaign id 120235887157880026)— 依動物種類/主題分 adset:新貼文｜貓、新貼文｜狗、新貼文｜情緒粉/免疫力、新貼文｜魚油、新貼文｜腸胃、新貼文｜關節、新貼文｜官網文、新貼文｜比較文、新貼文｜活動、新貼文｜新品/新內容。同樣不會被自動比對抓到(campaign 名稱裡沒有貓/狗/成分字樣可直接比對)。

**Why:** 2026-08-12 幫腎臟粉三支新素材上稿時,自動 plan 只抓到 BTL 舊客這組,ASC 跟 ATL 都要我手動搜 campaign 名稱才找到,Molly 反問「campaign 有這麼難找?」。

**How to apply:** 之後在這個帳號跑 [[meta-ad-copy]] 上稿流程,分析階段除了照 skill 正常跑 `plan` 拿到 BTL 舊客那組,還要主動:
- 用素材名稱裡的成分關鍵字(如「腎臟」「葉黃素」)去搜 ASC bucket campaign(120234621211530026)底下對應的 adset
- 依動物種類搜 ATL新客 campaign(120235887157880026)底下對應的「新貼文｜貓」或「新貼文｜狗」adset(若素材主題另外對得上情緒粉/魚油/腸胃/關節等 adset 則優先用主題,沒對上才退回貓/狗)

把三份候選一起列給 Molly 確認,不用她自己想起來要問,除非她主動說只要上其中一種。

**補充(2026-08-19,狗狗日x迷客夏全館活動素材上稿)：**
- 「全館活動」(非動物/成分專屬)主題的素材,ATL 新客 Interact 那邊固定對應到 `新貼文｜活動` adset(id 120236259363790026),過去 27 次全館活動素材裡 25 次都上這裡,比動物種類判斷更準。
- BTL 舊客那組如果比對到兩個以上候選 campaign/adset(例如「轉換BTL｜RT｜活動」vs「轉換BTL｜舊客｜狗」),可以拉近 30 天各候選 adset 的 ROAS/CPA/頻次來輔助判斷,不是只能靠關鍵字比對長度硬選——頻次過高(如 6.8+)的 adset 可以用主題相關的第二個 adset 分流,不用單壓一個。
- `新貼文｜活動` adset 曾經因為排程 end_time 過期(帳號裡有些舊 adset 會設 end_time)導致「無法新增廣告」,而且**延長 end_time 本身可能因為 adset 裡混了失效素材(如已刪除的 Reel)而連帶失敗**,要先處理掉壞素材才能改期。之後上稿前可以先查一下這個 adset 的 end_time 是否過期,提早抓到這個坑。
