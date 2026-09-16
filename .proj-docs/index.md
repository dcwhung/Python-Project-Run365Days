# 項目文件索引 — Run365Days

**最後更新**：2026-09-16（**documentation-only lane S-063…S-078 完成**：12 條 open suggestion 一次清，一條一個 commit，production 行為零改動（`api/schema.py` / `api/service.py` 剝 docstring + comment 後 AST 同 `ea820aa` 相同）；Python **418 tests**、前端 141 tests / 25 files、`api/service.py` coverage 99% → **100%**、total 95%；ruff + SDL + eslint + typecheck 全綠。其中 S-074 部分推翻 reviewer —— `year` 實測掟得出 list-row budget refusal。尚餘 S-077（user decision）同 S-079（留待下次 bump）)

> 本目錄為 ai-dev-team 產出文件嘅 Single Source of Truth。
> 每次有新文件輸出，必須喺此更新條目同頂部日期。
> 項目自身嘅技術文件（architecture / data-pipeline / deployment / roadmap / CHANGELOG）留喺 `docs/`，此處只索引唔重複。

---

## 項目速覽

| 項目 | 內容 |
|---|---|
| 名稱 | Run365Days（`dcwhung/Python-Project-Run365Days`） |
| 版本 | v3.1.1 |
| Stack | Python 3.10+ package + Flask/Strawberry GraphQL API + React 19 / Vite 7 / TS 5.9 |
| 部署 | Vercel（API mode）＋ GitHub Pages（static mode） |
| Default branch | `master`；**整合同部署 branch 係 `develop`**（master 現時落後 54 個 commit，只行 lint/test，唔 build 唔 deploy） |
| 部署 branch 核實（2026-09-14） | `github-pages` environment ✅ 仍然只收 `develop`（Actions run #34 嘅 Deploy job conclusion=success）；Vercel production branch ✅ 仍然係 `develop`（repo 讀唔到，由 owner 喺 Vercel dashboard 核實） |
| 可見性 | Public |

---

## 審計報告（`audits/`）

| 日期 | 文件 | 範圍 | 結論摘要 |
|---|---|---|---|
| 2026-09-13 | [`2026-09-13_22-42_audit_full-codebase.md`](audits/2026-09-13_22-42_audit_full-codebase.md) | 完整 codebase（Python / API / React / 測試 / CI / 依賴 / 安全） | 46 條 finding（4 🔴 / 31 🟡 / 11 🟢）；代碼質量 68/100；推薦漸進修復，不建議重建 |

---

## 待辦追蹤

> Ticket 狀態嘅 SSoT 係 [`tickets.md`](tickets.md)，呢度只係鳥瞰。

| 優先級 | 項目 | Finding | 狀態 |
|---|---|---|---|
| P0 | 4 條 Critical（AU-001 ~ AU-004） | AU-001 ~ AU-004 | ✅ 已修（PR #11 merge 入 `develop`） |
| P1 | `Activity.track` 單次請求成本冇 bound（rows 同 round trips 兩個維度） | AU-047 | ✅ **完成** —— review pass 91/100、QA pass 0 Critical、static JSON byte-identical |
| P1 | `ActivityView` 將 track 載入失敗誤報成「Activity not found.」 | CUI-0016 | ✅ **Done**（v3.1.0，`88c5c6f` `b84b7b7`）—— 視覺 gate 揾到錯誤訊息吐成個序列化 ClientError，返工修好 |
| P2 | Client 輸入錯誤一律以 `ValueError` + 完整 traceback 記入 ERROR log | S-018 | pending（AU-047 review 分拆） |
| P2 | 測試常數 / docstring 嘅 claim 收窄 | W-015, S-019 ~ S-022 | ✅ 已修（`9cd1125` `5ff91d3` `3ce2bd5`），S-022 嘅理由本身要重寫 → W-016 |
| P1 | **claim discipline 嘅結構修復**：headline 量度數字由散文變成 assert | W-017 | ✅ **Done** `57317e9`（AU-047 五次重複嘅共同成因，已驗證三種漂移都會紅） |
| P2 | `_page_field` docstring 斷言 alias fan-out 去唔到 field cap（實測去到）+ 該路徑零測試覆蓋 | W-016, S-024 ~ S-026 | ✅ **Done** `b127a7e` `bab3858` |
| P3 | `MAX_PAGE_SIZE` assertion 理由減弱；ruff 將 `graphql` 排入 first-party | S-027, S-028 | pending |
| P2 | 非 track 嘅 `year` fan-out 係 post-fix 最貴合法請求（330 SQL / 1.58 s） | S-023 | pending（AU-047 之前已存在，範圍外） |
| P1 | epoch-ms path 語義錯 8 小時（含捏造測試常數） | AU-048 | ✅ **Done**（v3.1.0，`6fcb3cc`）—— QA 重 parse 1,095 個 raw 檔證實已 export 數據零影響 |
| P1 | ruff 加 `ANN`+`S`、CI 加 `api/`、前端加 prettier、加 coverage gate | AU-010 ~ AU-012, AU-033 | pending |
| P1 | 常數統一、weight 兩個 bug、security headers、依賴版本上限 | AU-006 ~ AU-008, AU-031, AU-032 | pending |
| P2 | `create_app()` 預設仍然開 GraphiQL | AU-049 | pending |
| P2 | `{ activities { track } }` 仍然 2N round-trip（AU-047 分拆） | AU-050 | ✅ **Done**（v3.1.0，`2a043f7`）—— 但 batch predicate 係 O(N²)，見 CUI-0019 |
| P2 | a11y、logging、`dashboard` 改名、死碼清理、文件 drift、前端結構對齊 | AU-005, AU-013, AU-019, AU-023 ~ AU-029, AU-035 | pending |
| P3 | 前端依賴 major upgrade、隱私加固 | AU-030, AU-034 | pending |
| P1 | **AU-050 嘅 batch predicate 係 O(batch²)**：真實 export 上 field cap 嗰個 fan-out 慢咗 2.37×（67 → 158.7 ms），365 條 8.9×。前端唔發呢條 query，最壞合法形狀仍然 38× 喺 15 s 之內 | CUI-0019 | pending 🟠 |
| P2 | docstring wall clock 喺 600-row fixture 量卻用嚟論證 production（134,041 row）餘裕 | CUI-0020 | pending |
| P3 | api / static 取樣 rounding 差一個 index（pre-existing，今日不可達） | CUI-0021 | pending |
| P3 | 空 activity list → 頁面永遠「Loading…」；per-region pending 三態；gate 嘅「預期會紅」要寫白 | S-030, S-037, S-036 | pending |
| — | Review items（C/W/S-NNN）同 CUI-NNNN 逐條狀態 | — | 見 [`tickets.md`](tickets.md) |

---

## 其他文件

| 類型 | 位置 | 狀態 |
|---|---|---|
| Ticket registry（AU / C-W-S / CUI 全部） | [`tickets.md`](tickets.md) | ✅ 現行 SSoT |
| Code review 報告（2026-09-16 CUI-0029 批次 Round 1：CUI-0029 / 0030 / 0031 / 0033 / 0034）| [`reviews/2026-09-16_review_CUI-0029_batch.md`](reviews/2026-09-16_review_CUI-0029_batch.md)（**86/100 warn**，0 🔴 / 0 🟡 / 7 🟢（S-073…S-079）；hard gates 6/6 pass；417 py / 141 fe tests、coverage 95%、SDL in sync；每個 lane 講法獨立重做：票上 Option A 實測**唔 work**（bare `GraphQLError` 一樣出 9 個絕對路徑 frame）、INFO-over-WARNING 重現（bare deployment INFO 0 bytes / WARNING 541 bytes）、`process_errors` 冇吞任何錯、CUI-0033(a) 用重建 `else` 分支比對 **365 activity / 134,041 行 track 逐點零差異**、CUI-0034 gate 兩個方向連 `__pycache__` 清空重做；`npm audit --omit=dev` = **0 vulnerabilities**（14 條全屬 dev chain）；warn 完全嚟自 documentation suggestion 債，非本批代碼質素）| ✅ 已建立 |
| Code review 報告（2026-09-16 Round 2 batch：CUI-0025 / CUI-0028 / CUI-0032 + S-053 / S-058 / S-059 / S-060）| [`reviews/2026-09-16_review_CUI-0025_batch.md`](reviews/2026-09-16_review_CUI-0025_batch.md)（**88/100 warn**，0 🔴 / 1 🟡 / 7 🟢；hard gates 10/10 pass；CUI-0025 五個決定三個 mutant 一對一驗過；**S-060 裁決 lane 推翻 briefing 係啱嘅**，cold cache 只貴 3–8%、query 只掂 10/2865 版，上一輪 S-060 premise 正式更正為錯；W-029 = CUI-0028 一句假「實測」，票上原 repro 其實會撞）<br>⚠️ **§W-029 嘅成因歸因同 `python -B` 補救已被 Round 3 delta 證偽 —— 以 [`reviews/2026-09-16_review_CUI-0025_delta.md`](reviews/2026-09-16_review_CUI-0025_delta.md) §3 為準**| ✅ 已建立（部分 superseded）|
| Code review 報告（2026-09-16 Round 3 delta：W-029 / S-067，重新評分整批）| [`reviews/2026-09-16_review_CUI-0025_delta.md`](reviews/2026-09-16_review_CUI-0025_delta.md)（**90/100 pass**，0 🔴 / 0 🟡 / 4 🟢；hard gates 10/10 pass；88 → 90 = W-029 +5、S-067 +1、新 S-068…S-071 各 −1；`-B` 同 stale-`.pyc` 兩個 Round 2 講法被 developer 更正，reviewer 獨立重做後**兩個更正都採納**；production 行為 AST 證明零改動）| ✅ 已建立 |
| Code review 報告（2026-09-16 CUI-0027 批次，2 輪）| [`reviews/2026-09-16_review_CUI-0027_batch.md`](reviews/2026-09-16_review_CUI-0027_batch.md)（84/100 **warn**，0 🔴 / 2 🟡 / 6 🟢）<br>[`reviews/2026-09-16_review_CUI-0027_delta.md`](reviews/2026-09-16_review_CUI-0027_delta.md)（**95/100 pass**，0 🔴 / 0 🟡 / 3 🟢；W-027 / W-028 mutation 獨立重做；S-057 更正咗上一輪一個事實錯誤；production 行為 AST 證明零改動）| ✅ 已建立 |
| Code review 報告（2026-09-15 batch，955 行，3 輪）| [`reviews/2026-09-15_review_cui0016-au048-au050_batch.md`](reviews/2026-09-15_review_cui0016-au048-au050_batch.md)（89 warn → 99 pass → 97 pass）| ✅ 已建立 |
| QA 報告（2026-09-16 CUI-0027 batch，496 行）| [`qa/2026-09-16_qa_CUI-0027_batch.md`](qa/2026-09-16_qa_CUI-0027_batch.md)（✅ pass、0 Critical、hard gates 8/8、382 pytest / 126 vitest / coverage 95%；真 client call path + 兩個 build mode + 六類 edge case + static↔api 逐點比對 + export 等價性；新開 CUI-0029 / CUI-0030 / CUI-0031；CUI-0022 同 CUI-0024 建議 completed，CUI-0027 要先補 DoD bookkeeping）| ✅ 已建立 |
| QA 報告（2026-09-16 low-cost wave 1 batch，724 行）| [`qa/2026-09-16_qa_low-cost-wave1_batch.md`](qa/2026-09-16_qa_low-cost-wave1_batch.md)（✅ pass、0 Critical、363 tests、coverage 94.71%；export→SQLite→static JSON→GraphQL 全鏈只差 `generated_at`；CUI-0020 唔可以標 completed）| ✅ 已建立 |
| QA 報告（2026-09-15 batch，708 行）| [`qa/2026-09-15_qa_cui0016-au048-au050_batch.md`](qa/2026-09-15_qa_cui0016-au048-au050_batch.md)（0 Critical、coverage 99%、369/370 static 檔 byte-identical）| ✅ 已建立 |
| Code review 報告 | [`reviews/2026-09-13_review_p0-batch.md`](reviews/2026-09-13_review_p0-batch.md)（84/100 pass）<br>[`reviews/2026-09-14_review_au-047.md`](reviews/2026-09-14_review_au-047.md)（66/100 **fail**，1 🔴）<br>[`reviews/2026-09-14_review_au-047_round2.md`](reviews/2026-09-14_review_au-047_round2.md)（91/100 **pass**，0 🔴）<br>[`reviews/2026-09-14_review_au-047_round3.md`](reviews/2026-09-14_review_au-047_round3.md)（87/100 **warn**，cleanup 輪驗證） | ✅ 已建立 |
| QA 報告 | [`qa/2026-09-14_qa_p0-batch.md`](qa/2026-09-14_qa_p0-batch.md)（0 Critical）<br>[`qa/2026-09-14_qa_au-047.md`](qa/2026-09-14_qa_au-047.md)（0 Critical / 1 Major / 2 Minor，369/370 byte-identical） | ✅ 已建立 |
| CUI ticket 檔案 | `.tickets/pending/0001-0200/` | ✅ CUI-0001 ~ 0018 |
| 項目 `CLAUDE.md` | repo root | ✅ 2026-09-14 建立（項目速覽 / layout / 指令 / branch 同部署 / conventions / 已知陷阱 / 文件 SSoT） |
| Functional spec | `.proj-docs/specs/` | ❌ 未建立 |
| Technical spec | `.proj-docs/specs/` | ❌ 未建立 |
| Implementation plan | `.proj-docs/plans/` | ❌ 未建立 |
| Session log | [`.claude/session-logs/2026-09-14_14-55.md`](../.claude/session-logs/2026-09-14_14-55.md) | ✅ 2026-09-14（AU-047 + CLAUDE.md） |
| Session log | [`.claude/session-logs/2026-09-15_15-30.md`](../.claude/session-logs/2026-09-15_15-30.md) | ✅ 2026-09-15（v3.1.0：3 lane 並行 + 3 輪 review + QA）<br>**含 §HANDOVER —— 下一個 session 由呢度開始** |

---

## 項目自身文件（`docs/`，非本索引產出）

| 文件 | 用途 | 準確度（2026-09-13 審計） |
|---|---|---|
| `docs/architecture.md` | 架構說明 | ⚠️ 5 處 drift（見 AU-035） |
| `docs/data-pipeline.md` | 資料流程 | ⚠️ 1 處直接錯誤（`:103` BMI 常數來源） |
| `docs/deployment.md` | 部署指南 | ✅ 準確，`:41-48` 踩坑表同 git log 完全對得上 |
| `docs/roadmap.md` | 路線圖 | 未核對 |
| `docs/CHANGELOG.md` | 版本紀錄 | ✅ 準確 |
