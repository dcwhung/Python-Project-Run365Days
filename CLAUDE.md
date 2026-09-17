# CLAUDE.md — Run365Days

> 項目專屬設定，覆蓋 ai-dev-team 全局規則。
> 每次 session 開始先讀呢個檔案，避免重新掃描整個 codebase。
> 內容有變（stack / branch 策略 / 部署）必須同步更新。

---

## 1. 項目速覽

| 項目 | 內容 |
|---|---|
| 名稱 | Run365Days（`dcwhung/Python-Project-Run365Days`） |
| 版本 | v3.2.0 |
| 用途 | 個人跑步分析：Garmin activity 解析、天氣關聯、體重追蹤、dashboard |
| Python | 3.10+（package 名 `run365days`，src-layout，`package-dir = { "run365days" = "src" }`） |
| Backend | Flask 3 + Strawberry GraphQL（code-first），read-only 行喺 SQLite export 之上 |
| Storage | SQLite via SQLAlchemy 2.0 typed ORM，build 時生成 |
| Frontend | React 19 + TS 5.9 + Vite 7、TanStack Query + graphql-request + GraphQL Codegen、Tailwind CSS 4 |
| Quality | ruff（lint + format + isort + pydocstyle Google）、pytest、pre-commit |
| 部署 | Vercel（api mode）＋ GitHub Pages（static mode） |
| 可見性 | Public |

---

## 2. Repository 佈局（只列需要知嘅）

```
src/                    Python package，imported as `run365days`
  activities/           Activity / TrackPoint models、parsers/（tcx, gpx, kml）、metrics
  weather/              weather models、collectors/（hko_daily, hourly, warnings）
  weight/               體重解析同 summary
  dashboard/            builder（共用計算）、stats（年度 aggregate）
  common/               config（所有 path 同常數）、geo、time、numeric
  export/               models（ORM）、records、sqlite、static_json
  api/                  app、db、schema（Strawberry）、service（純 SQLAlchemy，回 dict）
  cli/                  process_activities、collect_weather、export_data、export_schema
tests/                  pytest，一個 feature module 一個檔案
frontend/               React dashboard；src/data（api + static source）、src/views、src/app
data/processed/         run365.db、static/*.json（git-ignored）
docs/                   architecture、data-pipeline、deployment、roadmap、CHANGELOG
.proj-docs/             ai-dev-team 產出（audit / review / qa / tickets / index）
.tickets/               CUI-NNNN ticket 檔案
legacy/                 2022 舊 script，只作參考，唔好改
```

### 分層鐵律

`src/api/service.py` **唔可以** import Strawberry。Service 層只回 plain dict（export record shape），
由 `schema.py` 映射落 Strawberry type。咁樣先可以用臨時 DB 直接測 service，唔使經 GraphQL。

---

## 3. 常用指令

```bash
# Python（本 repo 喺 remote session 要用 venv：系統 pip 同 python 唔同 prefix，
# 而且 debian 裝咗嘅 blinker 冇 RECORD file，會令 pip install -e . 失敗）
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest tests -q          # 測試
.venv/bin/ruff check src tests               # lint
.venv/bin/ruff format --check src tests      # format check
.venv/bin/run365-schema --check frontend/schema.graphql   # SDL 同 frontend 同步

# Frontend
cd frontend
npm ci
npm audit --omit=dev       # CI gate：只審 runtime 依賴（dev 樹嗰啲 advisory 係 pre-existing，見 CUI-0036）
npm run lint
npm run typecheck          # 會先跑 codegen
npx vitest run
npm run build              # api mode
npm run build:static       # static mode
```

CI 跑嘅就係上面呢批（見 `.github/workflows/pages.yml`）。改動後本地行過先 push。

---

## 4. Branch 同部署（唔好搞錯）

| 項目 | 值 |
|---|---|
| Default branch | `master` |
| **整合 branch（所有 feature / fix 由此開，PR 亦 merge 返此）** | **`develop`** |
| CI 觸發 | push / PR 去 `develop` 同 `master` |
| **Build + Deploy（GitHub Pages）** | **只喺 `refs/heads/develop`** |
| `github-pages` environment | deployment branch rule 只容許 `develop` |
| **Vercel production branch** | **`develop`**（設定喺 Vercel dashboard → Settings → Git，repo 入面讀唔到，改 branch 佈局後要人手核對） |
| `master` | 只行 lint / test，唔 build 唔 deploy；靠 release merge 前進 |

> ⚠️ 呢個 repo **唔係** 「master = production」。`develop` 先係 deploy 源頭（AU-004 / W-011 嘅決定）。
> Branch policy hook 會 hard block 喺 `master` / `develop` 直接 commit。

Branch 命名：`[feature|fix|refactor|chore]/[scope]/[TICKET]_[描述]`，One-Task-One-Branch。
Commit：Conventional Commits（`feat` / `fix` / `refactor` / `chore` / `docs` / `test` / `ci` / `style`），
慣例係 `fix: AU-047 | 一句描述`。

---

## 5. Coding conventions（項目層，覆蓋全局）

| 規則 | 值 |
|---|---|
| line-length | 100（ruff，非 Prettier 嘅 printWidth） |
| quote style | double（`ruff format`，**唔係** 全局規則嘅 singleQuote） |
| docstring | Google convention，ruff `D` 已開；`src/api/schema.py` 豁免 `D101`/`D102`（GraphQL type 用 SDL description） |
| type hints | 用 `X | None`（`from __future__ import annotations` 已喺多數 module） |
| 常數 | 全部 module-level `UPPER_SNAKE_CASE` 加 docstring；`src/common/config.py` 收納所有 path 同共用常數 |
| 測試 | pytest，測試名即文件（`tests/*` 豁免 `D`）；TDD Red-Green-Refactor |
| 前端 | ESLint + `tsc -b`；GraphQL document 一定要過 codegen，SDL 改動要跑 `run365-schema` 更新 `frontend/schema.graphql` |

---

## 6. 已知陷阱（實測得出，唔好再踩）

| 陷阱 | 內容 |
|---|---|
| `PYTHONTZPATH` 測試 | `zoneinfo` 喺 import 時讀，測試入面改 `os.environ` 唔生效，會寫出永遠綠嘅測試。要三路齊斬：`sys.modules["tzdata"] = None` + `zoneinfo.reset_tzpath(to=[])` + `ZoneInfo.clear_cache()` |
| TDD Red 階段 | 唔好 import 未存在嘅 symbol，會變 collection Error 而唔係 assertion Fail（睇落紅其實壞咗）。先 assert 行為，Green 之後先收緊 exception type |
| `inf` | pandas 兩個有限值相加可以變 `inf`，只出一個 RuntimeWarning；`int(inf)` 會掟 `OverflowError`。數值喺交俾 `int()` / `timedelta()` 前要 `ensure_finite()` |
| NOT NULL 欄位 | `activities.distance_km` / `duration_sec` 喺 SQLite 係 NOT NULL，唔可以將非有限值映射成 `None`（static writer 收，SQLite 拒），要降級成數值 |
| Vercel function 偵測 | 唯一被認嘅 file name 係 `api/graphql.py`，而且 `app = _build_app()` 必須係 top-level statement，唔可以入 try block |
| Vercel 依賴 | Vercel 由 `pyproject.toml` 裝但**唔裝 optional extras**，所以 Flask / Strawberry 係 core dependency |
| `tzdata` | 無條件宣告，唔加 platform marker（slim / distroless container 一樣冇 `/usr/share/zoneinfo`） |
| 環境錯誤 | `MissingTimeZoneDataError` 刻意繼承 `RuntimeError`，令 `parse_all` 任何 except 分支都食唔到 |
| `graphql` 名字遮蔽（只係 tooling 層）| `[tool.ruff] src` 包含 `api/`，而 Vercel 只認 `api/graphql.py` ⇒ ruff 將 bare `from graphql import ...` 當成本 repo 自己嘅 module，迫佢入 first-party block（`src/api/schema.py`、`tests/test_api.py`）。已用 `known-third-party = ["graphql"]` 釘死；移走呢個 setting 兩個檔案即刻 `I001` 紅。⚠️ **唔好為咗閃開遮蔽而改 entry 名** —— `fd252a3` 改做 `api/index.py`、`e506531` 改做 `api/graphql_api.py`，兩個都被 Vercel 個 functions pattern 拒，最後 `5aa1b2f` 改返。而且 `5aa1b2f` 查明**真正嘅 crash 由頭到尾係欠 Flask 依賴**（`12c31fe` 修），遮蔽從來冇喺 runtime 咬過人 ——兩個 deploy cycle 蝕喺一個誤診度。遮蔽只喺 import-sorting 層有影響，喺嗰層解 |
| Mutation testing 用 stale `.pyc` | 改完源碼即刻重跑，可以行到**舊** bytecode（mtime 係秒精度，size 又啱 ⇒ cache 判定為有效），令 mutant 假綠。`python -B` **救唔到**（佢擋寫唔擋讀），而且單一次觀察**判定唔到**成因（stale `.pyc` 定 `sys.modules` 已 import 都解釋得晒）。要開嘅係**方法**唔係一個補救指令：in-process `setattr` 落 mutant → assert 個 mutant 真係生效咗（唔好假設）→ 用一個獨立 oracle 驗結果 |
| GraphQL nullability widening，`tsc` 天生捉唔到 | non-null → nullable 嘅 widening，凡係落喺一個已經寫咗 `??` / `?.` 嘅取值點，`tsc` 一聲都唔會出。CUI-0018 (b) 把 `track` 由 `[TrackPoint!]!` 改成 `[TrackPoint!]`，codegen 照樣產出 `| null`（`frontend/src/gql/graphql.ts:318`），但 `npm run typecheck` 實測捉到 **0 處** —— 因為 `frontend/src/data/api/source.ts:77` 原本就有個為「activity 唔存在」而寫嘅 `?? []`，照單全收吞晒。**唔係 `tsc` 盲**：同一行剷走 `?? []` 即刻 `TS2322`，明文寫住 `... | null | undefined`，所以個 coalesce 正正就係塊遮眼布。**Nullability widening 嘅 checklist 係後端測試**：同一個改動 type-only 倒返轉頭（resolver annotation 去返 `list[TrackPoint]`、清 `__pycache__`、regenerate SDL 確認變咗 `[TrackPoint!]!`），`tests/test_api.py` 實測紅 **5 條**（4 條驗 wire 上 `track is None`，1 條驗 SDL type）。前端 codegen 綠唔代表冇嘢郁 |

---

## 7. 文件 SSoT

| 內容 | 位置 |
|---|---|
| ai-dev-team 文件索引 | `.proj-docs/index.md`（每次輸出後更新頂部日期） |
| Ticket registry（AU-NNN / C-W-S-NNN / CUI-NNNN） | `.proj-docs/tickets.md` |
| CUI ticket 檔案 | `.tickets/pending/0001-0200/` |
| 審計報告 | `.proj-docs/audits/` |
| Review / QA 報告 | `.proj-docs/reviews/`、`.proj-docs/qa/` |
| 項目技術文件 | `docs/architecture.md`、`docs/data-pipeline.md`、`docs/deployment.md`、`docs/roadmap.md`、`docs/CHANGELOG.md` |

編號全局唯一、永不重用；完成嘅保留紀錄只改狀態。

---

## 8. 語言

回覆用繁體中文（廣東話書面語）。代碼、identifier、技術術語、commit message 用英文。
