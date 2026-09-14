# 項目文件索引 — Run365Days

**最後更新**：2026-09-13 22:42

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
| Default branch | `master`（整合 branch 為 `develop`，現已落後 3 個 commit） |
| 可見性 | Public |

---

## 審計報告（`audits/`）

| 日期 | 文件 | 範圍 | 結論摘要 |
|---|---|---|---|
| 2026-09-13 | [`2026-09-13_22-42_audit_full-codebase.md`](audits/2026-09-13_22-42_audit_full-codebase.md) | 完整 codebase（Python / API / React / 測試 / CI / 依賴 / 安全） | 46 條 finding（4 🔴 / 31 🟡 / 11 🟢）；代碼質量 68/100；推薦漸進修復，不建議重建 |

---

## 待辦追蹤（源自最新審計）

| 優先級 | 項目 | Finding |
|---|---|---|
| P0 | CI 只喺 `develop` 觸發，default branch `master` 零 gate | AU-004 |
| P0 | Parser 零測試 + 吞 `AttributeError` + 無 zero-record guard → 可靜默 deploy 空站 | AU-002 |
| P0 | `parse_datetime()` 回傳 LMT `+07:37` | AU-003 |
| P0 | GraphQL 零 query cost 控制 + production GraphiQL | AU-001 |
| P1 | ruff 加 `ANN`+`S`、CI 加 `api/`、前端加 prettier、加 coverage gate | AU-010 ~ AU-012, AU-033 |
| P1 | 常數統一、weight 兩個 bug、security headers、依賴版本上限 | AU-006 ~ AU-008, AU-031, AU-032 |
| P2 | a11y、logging、`dashboard` 改名、死碼清理、文件 drift、前端結構對齊 | AU-005, AU-013, AU-019, AU-023 ~ AU-029, AU-035 |
| P3 | 前端依賴 major upgrade、隱私加固 | AU-030, AU-034 |

---

## 其他文件

| 類型 | 位置 | 狀態 |
|---|---|---|
| Functional spec | `.proj-docs/specs/` | ❌ 未建立 |
| Technical spec | `.proj-docs/specs/` | ❌ 未建立 |
| Implementation plan | `.proj-docs/plans/` | ❌ 未建立 |
| QA 報告 | `.proj-docs/qa/` | ❌ 未建立 |
| Session log | `.claude/session-logs/` | ❌ 未建立 |
| 項目 `CLAUDE.md` | repo root | ❌ 未建立（建議補，令 ai-dev-team 每次 session 唔使重新掃描） |

---

## 項目自身文件（`docs/`，非本索引產出）

| 文件 | 用途 | 準確度（2026-09-13 審計） |
|---|---|---|
| `docs/architecture.md` | 架構說明 | ⚠️ 5 處 drift（見 AU-035） |
| `docs/data-pipeline.md` | 資料流程 | ⚠️ 1 處直接錯誤（`:103` BMI 常數來源） |
| `docs/deployment.md` | 部署指南 | ✅ 準確，`:41-48` 踩坑表同 git log 完全對得上 |
| `docs/roadmap.md` | 路線圖 | 未核對 |
| `docs/CHANGELOG.md` | 版本紀錄 | ✅ 準確 |
