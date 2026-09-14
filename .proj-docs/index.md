# 項目文件索引 — Run365Days

**最後更新**：2026-09-14（AU-047 + CLAUDE.md）

> 本目錄為 ai-dev-team 產出文件嘅 Single Source of Truth。
> 每次有新文件輸出，必須喺此更新條目同頂部日期。
> 項目自身嘅技術文件（architecture / data-pipeline / deployment / roadmap / CHANGELOG）留喺 `docs/`，此處只索引唔重複。

---

## 項目速覽

| 項目 | 內容 |
|---|---|
| 名稱 | Run365Days（`dcwhung/Python-Project-Run365Days`） |
| 版本 | v3.0.0 |
| Stack | Python 3.10+ package + Flask/Strawberry GraphQL API + React 19 / Vite 7 / TS 5.9 |
| 部署 | Vercel（API mode）＋ GitHub Pages（static mode） |
| Default branch | `master`；**整合同部署 branch 係 `develop`**（master 現時落後 54 個 commit，只行 lint/test，唔 build 唔 deploy） |
| 部署 branch 核實（2026-09-14） | `github-pages` environment ✅ 仍然只收 `develop`（Actions run #34 嘅 Deploy job success）；Vercel production branch ⚠️ 讀唔到（設定只存喺 Vercel dashboard，agent proxy 亦封咗 vercel.app），要人手核對 |
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
| P1 | `Activity.track` 單次請求資料量冇 bound | AU-047 | 🔧 本次 session 處理中 |
| P1 | epoch-ms path 語義錯 8 小時（含捏造測試常數） | AU-048 | pending |
| P1 | ruff 加 `ANN`+`S`、CI 加 `api/`、前端加 prettier、加 coverage gate | AU-010 ~ AU-012, AU-033 | pending |
| P1 | 常數統一、weight 兩個 bug、security headers、依賴版本上限 | AU-006 ~ AU-008, AU-031, AU-032 | pending |
| P2 | `create_app()` 預設仍然開 GraphiQL | AU-049 | pending |
| P2 | `{ activities { track } }` 仍然 2N round-trip（AU-047 分拆） | AU-050 | pending |
| P2 | a11y、logging、`dashboard` 改名、死碼清理、文件 drift、前端結構對齊 | AU-005, AU-013, AU-019, AU-023 ~ AU-029, AU-035 | pending |
| P3 | 前端依賴 major upgrade、隱私加固 | AU-030, AU-034 | pending |
| — | Review items（C/W/S-NNN）同 CUI-NNNN 逐條狀態 | — | 見 [`tickets.md`](tickets.md) |

---

## 其他文件

| 類型 | 位置 | 狀態 |
|---|---|---|
| Ticket registry（AU / C-W-S / CUI 全部） | [`tickets.md`](tickets.md) | ✅ 現行 SSoT |
| Code review 報告 | [`reviews/2026-09-13_review_p0-batch.md`](reviews/2026-09-13_review_p0-batch.md) | ✅ 已建立 |
| QA 報告 | [`qa/2026-09-14_qa_p0-batch.md`](qa/2026-09-14_qa_p0-batch.md) | ✅ 已建立（0 Critical，regression 通過） |
| CUI ticket 檔案 | `.tickets/pending/0001-0200/` | ✅ CUI-0001 ~ 0015 |
| 項目 `CLAUDE.md` | repo root | ✅ 2026-09-14 建立（項目速覽 / layout / 指令 / branch 同部署 / conventions / 已知陷阱 / 文件 SSoT） |
| Functional spec | `.proj-docs/specs/` | ❌ 未建立 |
| Technical spec | `.proj-docs/specs/` | ❌ 未建立 |
| Implementation plan | `.proj-docs/plans/` | ❌ 未建立 |
| Session log | `.claude/session-logs/` | ❌ 未建立 |

---

## 項目自身文件（`docs/`，非本索引產出）

| 文件 | 用途 | 準確度（2026-09-13 審計） |
|---|---|---|
| `docs/architecture.md` | 架構說明 | ⚠️ 5 處 drift（見 AU-035） |
| `docs/data-pipeline.md` | 資料流程 | ⚠️ 1 處直接錯誤（`:103` BMI 常數來源） |
| `docs/deployment.md` | 部署指南 | ✅ 準確，`:41-48` 踩坑表同 git log 完全對得上 |
| `docs/roadmap.md` | 路線圖 | 未核對 |
| `docs/CHANGELOG.md` | 版本紀錄 | ✅ 準確 |
