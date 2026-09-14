# 架構審計報告

**日期**：2026-09-13 22:42
**項目**：Run365Days（`dcwhung/Python-Project-Run365Days`）
**審計員**：Architect Agent + Code Reviewer Agent（ai-dev-team）
**審計範圍**：完整 codebase（Python package、GraphQL API、React frontend、測試、CI/CD、依賴、安全配置）
**審計基準 commit**：`2ff659e`（branch `claude/stoic-ritchie-rkynjn`）

---

## 項目概覽

| 項目 | 內容 |
|---|---|
| 類型 | 個人數據分析 Web App（Python 分析 package + GraphQL API + React dashboard） |
| 狀態 | **Active**（v3.0.0，2026-09-10 最後 push） |
| 代碼規模 | Python 41 檔 / 3,091 行（`src/` + `api/`）；TS/TSX 87 檔 / 5,282 行；測試 Python 12 檔 1,009 行 + Frontend 19 檔 |
| Legacy | `legacy/` 1,948 行（reference-only，已排除於 lint 與測試之外） |
| 最後活躍 | 2026-09-10（`2ff659e`） |
| 可見性 | **Public** repo |
| Default branch | **`master`** |
| 部署 | Vercel（Flask + GraphQL）＋ GitHub Pages（static mode） |

### 三次重建歷程

v1（2022 腳本集）→ v2（測試化 package + static dashboard + CI）→ v3（React dashboard 雙模式 + Python GraphQL API）。`legacy/` 保留 v1 腳本作參考。

---

## Tech Stack

| 層級 | 技術 | 版本 | 備注 |
|---|---|---|---|
| Language (BE) | Python | `>=3.10`（CI 用 3.12） | `.python-version` = 3.12 |
| Language (FE) | TypeScript | `~5.9.2` | `strict` + `noUnusedLocals` + `verbatimModuleSyntax` |
| Parsing | `xml.etree.ElementTree`, BeautifulSoup4, lxml | 無版本約束 | TCX / GPX / KML |
| 數值 | numpy, pandas | 無版本約束 | 僅 export / weight 層需要 |
| ORM | SQLAlchemy | `>=2.0` | 唯一有下限約束嘅其中之一 |
| API | Flask + Strawberry GraphQL | `>=3` / `>=0.250` | read-only，無 Mutation |
| Frontend | React 19 + Vite 7 + React Router 7 | `^19.1.0` / `^7.1.0` / `^7.9.0` | |
| Data fetch | TanStack Query 5 + graphql-request 7 | `^5.90.0` / `^7.2.0` | |
| Charts | Chart.js 4 + react-chartjs-2 5 | `^4.5.1` / `^5.3.1` | |
| Styling | Tailwind CSS 4（`@theme` token） | `^4.1.13` | |
| DB | SQLite（build-time 產生，`mode=ro` 讀取） | — | 非持久化，每次 build 重建 |
| Infra | Vercel Functions（512MB / 15s）+ GitHub Pages | — | |
| 測試 | pytest（Python）、Vitest + Testing Library（FE） | `>=7` / `^3.2.4` | |
| Lint | ruff（`E,W,F,I,UP,B,SIM,N,D`）、ESLint 9 + typescript-eslint | `>=0.5` / `^9.36.0` | 前端**無 formatter** |

---

## 目錄結構

```
src/                    Python package（import 名 run365days，見 pyproject package-dir）
├── common/             config（所有路徑）、geo（haversine）、time  ← base layer，零內部依賴
├── activities/         Activity/TrackPoint dataclass + TCX/GPX/KML parser + MET metrics
├── weather/            HKO / freemeteo collector + dataclass
├── weight/             WeightRecord + 文字 log 解析
├── dashboard/          builder（共用純計算）+ stats（年度聚合）  ← 實為 shared kernel，名不副實
├── export/             records（單一 IR）→ models（SQLAlchemy）→ sqlite / static_json writer
├── api/                app（Flask factory）、schema、service、db
└── cli/                4 個 console script（純 wiring）
api/graphql.py          Vercel serverless entry（89 行 bootstrap，零業務邏輯）
frontend/src/           React app（app / views / components / data / lib）
tests/                  12 個測試檔 + conftest.py
legacy/                 v1 腳本，reference-only
data/raw/               committed 原始數據（1,109 檔 / 184MB）
data/processed/         gitignored，build time 產生
docs/                   architecture / data-pipeline / deployment / roadmap / CHANGELOG
```

---

## 架構分析

### 整體架構模式

**Feature-organised**（`docs/architecture.md:16` 明確聲明），但內部有清晰且方向正確嘅隱含分層：

```
common  ──►  activities / weather / weight     (feature)
                    │
                    ▼
              dashboard (builder + stats)       ← 實為 shared kernel
                    │
                    ▼
                 export (records → models → writers)
                    │
                    ▼
                   api (service → schema → app)
                    │
                    ▼
                   cli (wiring)
```

### 三個做得正確嘅核心決定

1. **單一 intermediate representation** — `ExportRecords`（`src/export/records.py:41`）由 `build_records()` 一次建成，`write_sqlite`（`sqlite.py:60`）同 `write_static_json`（`static_json.py:35`）純序列化。SQLite 同 static JSON **結構上唔可能唔一致**。`TRACK_COLUMNS`（`records.py:23`）係欄位次序嘅唯一真相，被 4 處共用。
2. **雙部署模式嘅 seam 開喺正確位置** — Python 側**零** deployment-specific 分支（同一支 CLI，兩個 flag）；Frontend 側 seam 只有 10 行（`frontend/src/data/source.ts:7-10`），由 `VITE_DATA_MODE` 揀實作，View 層完全唔知自己喺邊個 mode 跑。
3. **用測試釘死架構邊界** — `tests/test_api_imports.py:9` 以 subprocess 斷言 `run365days.api.*` import 後 `sys.modules` 唔含 pandas / numpy / lxml / bs4 / requests，守住 serverless cold-start 預算。罕見而正確。

### 依賴關係

**冇循環依賴，冇下層 import 上層**（逐條 grep `src/` + `api/` 全部 import 驗證）。最大檔案 267 行，冇 god object。

`docs/architecture.md:19-28` 嘅依賴表漏咗兩條實際存在嘅合法向下依賴：`src/api/db.py:12`（→ `export.sqlite`）、`src/api/service.py:11`（→ `dashboard.builder`）。

---

## API 結構

Flask factory（`src/api/app.py:28`）掛 `_SessionView`（`app.py:15`），每 request 開一個 SQLAlchemy session，`response.call_on_close(session.close)` 收尾。Resolver 由 `info.context["session"]` 攞 session 並委派落 `service` / `stats`，**schema 層完全冇 SQLAlchemy** — 耦合度控制得好。

| Query / field | 說明 | Resolver | 委派 | 認證 |
|---|---|---|---|---|
| `meta` | 匯出 metadata | `schema.py:204` | `service.meta` | 無（public） |
| `activities(fromDate, toDate, minKm, hasGps)` | 跑步列表 | `schema.py:211` | `service.activities` | 無 |
| `activity(id)` | 單次跑步 | `schema.py:225` | `service.activity` | 無 |
| `Activity.track(points)` | GPS 軌跡（降採樣） | `schema.py:70` nested | `service.track` | 無 |
| `weight(fromDate, toDate)` | 每日磅重 | `schema.py:229` | `service.weight` | 無 |
| `weather(fromDate, toDate)` | HKO 每日天氣 | `schema.py:236` | `service.daily_weather` | 無 |
| `warnings(fromDate, toDate)` | HKO 警告信號 | `schema.py:243` | `service.warnings` | 無 |
| `year(year)` | 年度聚合（totals / monthly / weekly / dailyDistance / trainingLoad / PB） | `schema.py:250` | `service.activities` + `dashboard.stats` ×6 | 無 |

無 Mutation（read-only，設計如此）。`run365-schema --check` 喺 CI（`pages.yml:49`）釘死 `frontend/schema.graphql` 同 Python schema 一致 — 跨語言 contract gate，做得好。

---

## 數據庫結構

SQLite（`data/processed/run365.db`，每次 build 重建，運行時 `mode=ro`）。ORM 定義於 `src/export/models.py`。

| Table | 用途 | 主要欄位 |
|---|---|---|
| `activities` | 每次跑步摘要 | id, date, distance_km, duration, pace, calories, cadence, ascent, avg_temp |
| `track_points` | GPS 軌跡點（每跑步 600 點，`export_data.py:27`） | activity_id, seq, lat, lon, alt, speed, temp, time |
| `weight` | 每日磅重 | date, lbs, kg, bmi, day_number |
| `weather_daily` | HKO 每日 | date, temp, humidity, wind, rain |
| `weather_warnings` | HKO 警告信號 | date, signal |
| `meta` | 匯出 metadata | generated_at, counts |

### 資料流

```
data/raw/garmin/{tcx,gpx,kml}/*  (各 365) ─► activities.parsers ─► Activity[] ─┐
data/raw/weight/2021_daily_weight.txt     ─► weight.analysis ─► WeightRecord[] ─┤
data/raw/weather/*.json                   ─► load_jsonl ─► hourly/daily/warning ─┤
                                                                                 ▼
                                                       export.records.build_records()
                                                              │              │
                                            write_sqlite ◄────┘              └──► write_static_json
                                                    │                                  │
                                          data/processed/run365.db          data/processed/static/
                                                    │                                  │
                                            Vercel API mode                  GitHub Pages static mode
```

---

## 發現問題

> 編號規則：`AU-NNN` 依嚴重度排序。來源標註 `[A]` = Architect、`[R]` = Code Reviewer、`[M]` = Main agent（基建 / 依賴 / 安全層）。

### 🔴 Critical（必須處理）

#### [AU-001] GraphQL 端點零 query cost 控制，production 開住 GraphiQL `[A]`

- **位置**：`src/api/schema.py:267`、`api/graphql.py:82`、`src/api/service.py:56,85`
- **描述**：`strawberry.Schema(query=Query)` 冇任何 extension、`validation_rules` 或 depth / complexity limit（grep `src/api/` 零 hit）。同時：
  1. 全部 list resolver（`activities` / `weight` / `weather` / `warnings`）**冇分頁** — 無 `limit` / `offset` / cursor
  2. `Activity.track` 係 **N+1** — `schema.py:70-72` 每個 activity 各發一次 query；`service.py:92` 先把該 activity 全部已存 track point（每跑步 600 點）materialize 成 ORM object，之後先喺 Python 降採樣（即使 `points=1` 都照讀晒）
  3. `api/graphql.py:82` 硬編 `graphiql=True`，production 一樣開（連帶 introspection）
- **影響**：`{ activities { track(points: 999999) } }` 一句 = 365 次 query × 600 行 ≈ **219,000 個 ORM object**，喺 512MB / 15 秒上限（`vercel.json`）嘅 function 入面。呢個係公開端點，GraphiQL 仲幫手寫埋條 query。目前 frontend 只喺單一 activity 查 track（`frontend/src/data/api/queries.ts:128-144`），所以係**潛伏而非已觸發**。
- **建議**：(a) `Query.activities` 加 `limit` 並設預設上限；(b) 加 Strawberry `QueryDepthLimiter` / `MaxTokensLimiter`；(c) `track` 喺 SQL 層做 stride 篩選（`WHERE seq % n = 0`）而非讀晒先降採樣；(d) `graphiql` 改由環境變數控制。

#### [AU-002] Ingestion path 靜默失敗鏈 — 三個 parser 零測試 + 吞 `AttributeError` + 無 zero-record guard `[R]`

- **位置**：`src/activities/parsers/base.py:51`、`tcx.py`、`gpx.py`、`kml.py`、`src/cli/export_data.py:52`
- **描述**：
  ```python
  with contextlib.suppress(ValueError, AttributeError, ET.ParseError):
      activities.append(self.parse(fp))
  ```
  - `grep -rln "TCXParser\|GPXParser\|KMLParser" tests/` → **零 match**。三個 parser 嘅 `parse()` body 完全冇測試（coverage：tcx 28%、gpx 30%、kml 24%，miss 行 = 整個 method body）
  - parser 滿佈無 guard 嘅 `.find(...).text` 鏈，例如 `tcx.py:89 float(trkpt.find("ns:AltitudeMeters", _NS).text)` — TCX schema 入面 `AltitudeMeters` 係 optional，室內跑或舊 device 冇呢個 tag 就 raise `AttributeError`
  - 呢個 `AttributeError` 正正被 L51 suppress，所以「schema 唔啱」同「呢條 activity 應該 skip」分唔開，而且**零 log**
  - 下游 `export_data.py:52` 攞到 `tcx = []` 之後**冇 zero-record guard**，照樣 `write_static_json()` 寫一份空 dashboard，**exit 0**
- **影響**：CI 嘅 `run365-export --skip-db --static-dir frontend/public/data` 會**綠燈 deploy 一個空站**。呢個係整份審計唯一會造成「靜默 ship 錯誤數據」嘅完整鏈條。
- **建議**：(a) 收窄 suppress 至 `ValueError` + `ET.ParseError`，`AttributeError` 改為明確 `ParseError` 並 log；(b) `export_data.main()` 加「parsed 0 activities → `sys.exit(1)`」；(c) 每個 parser 至少一個 golden-file test + 一個 malformed-input test。

#### [AU-003] `parse_datetime()` 回傳 LMT `+07:37` 而非 `+08:00` `[R]`

- **位置**：`src/common/time.py:37,39`
- **描述**：`pytz` 時區物件直接塞入 `.replace(tzinfo=tz)` 會攞到 1904 年前嘅 LMT 定義（香港 +7:37）。實跑證據：
  ```
  naive local -> 2021-10-17 06:10:56+07:37   utcoffset: 7:37:00   ← 應為 +08:00
  epoch ms    -> 2021-10-17 06:10:56+07:37
  ISO Z       -> 2021-10-17 06:10:56+08:00   ← 走 .astimezone(tz)，正確
  ```
  同一個 function 四條 input path 有兩個唔同 offset，跨 path 比較差 **23 分鐘**。`tests/test_common_time.py:29-33` 只 assert `hour/minute/second`，**從來冇 assert `utcoffset()`**，所以 89% coverage 之下測試全綠。
- **影響**：現時被遮住（parser 攞到 datetime 後即刻 `.strftime()` 掉走 tzinfo），但 `activity_time_range()` 同任何未來 `astimezone()` 會即刻中招。
- **建議**：改用 `zoneinfo.ZoneInfo`（標準庫，`replace(tzinfo=)` 語義正確）或 `tz.localize(naive_dt)`。同時 `datetime.utcfromtimestamp` 喺 CI 用嘅 Python 3.12 已 deprecated，改 `datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz)` 一次修兩樣。測試補 `utcoffset()` assert。

#### [AU-004] CI 只喺 `develop` 觸發，但 default branch 係 `master` — 入 master 嘅 PR 零 CI gate `[M]`

- **位置**：`.github/workflows/pages.yml:4-7`
- **描述**：
  ```yaml
  on:
    push:
      branches: [develop]
    pull_request:
      branches: [develop]
  ```
  GitHub API 確認 `default_branch = "master"`。`origin/develop` 存在但落後 `master` 3 個 commit（內容差異只有 `.claude/settings.json`，無 source drift）。
- **影響**：所有以 `master` 為 base 嘅 PR **完全唔會跑 lint / test / typecheck / schema-drift check**。PR #10（`chore/claude/ai-dev-team-plugin` → `master`）就係喺零 CI 之下 merge 咗。README 嘅 CI badge 亦指住 `?branch=develop`，反映嘅唔係 default branch 狀態。
- **建議**：二擇其一 —
  - (a) 保持 `develop` 為整合 branch：README badge 維持現狀，但補一個 `on: pull_request: branches: [master]` 嘅 lint-test job，令 release PR 都有 gate；
  - (b) 廢除 `develop`，全部改用 `master`：`pages.yml` 同 `tag-release.yml`（`ref` 預設值 `develop`）一併改。
  建議 (b) — 現時 `develop` 已無獨立內容，維持兩條 branch 只係徒增 drift 風險。

---

### 🟡 Warning（建議處理）

#### [AU-005] `dashboard` package 名不副實，實為 shared kernel `[A]`

- **位置**：`src/dashboard/__init__.py:1`、`src/dashboard/builder.py:3-6`、`src/__init__.py:8`
- **描述**：Python 側已無 dashboard（React app 喺 `frontend/`）。`builder.py` 現為 export 同 api **共用**嘅純計算層（`export/records.py:12-20` import 7 個名、`api/service.py:11` import `downsample`）。`builder.py:3-6` 自己 docstring 都寫明「the payload itself is gone」。
- **影響**：新人睇 package 樹會以為 `dashboard/` 係 presentation 層，實際佢係 `export` 同 `api` 之間最核心嘅共用領域邏輯，個名令依賴方向睇落似倒轉。
- **建議**：改名 `src/analytics/`（或 `src/domain/`），`builder.py` → `runs.py`。成本只有 4 個 import site（`export/records.py:12`、`api/service.py:11`、`api/schema.py:17`、`cli/export_data.py:21`）。

#### [AU-006] `build_dataframe()` 靜默無視 `height_cm`，BMI 前後不一致 `[R]`

- **位置**：`src/weight/analysis.py:68` vs `:120`
- **描述**：`parse_weight_file` 用參數 `height_cm`，`build_dataframe` 寫死 `_HEIGHT_CM_DEFAULT`。實跑：
  ```
  parse_weight_file(..., height_cm=180) -> record.bmi = 21.69
  build_dataframe(同一批 record)        -> df BMI    = 24.32   ← 用返 170
  ```
  呢兩個 function 就係 pipeline 前後兩步。
- **建議**：`build_dataframe(records, year=None, height_cm=config.BODY_HEIGHT_CM)`，或直接用 `record.bmi` 唔好重算。

#### [AU-007] `day_number` 係「檔案行號」而非「第幾次磅」`[R]`

- **位置**：`src/weight/analysis.py:53,71`
- **描述**：`for day_number, line in enumerate(f, start=1)` — 檔案有 header 雜訊時偏移。實跑：input 首行為 header → `day_numbers: [2, 3]`，但 `dates: ['2021-01-01', '2021-01-02']`。`tests/test_weight_analysis.py:44-48` 有測「skip 唔 match 嘅行」但只 assert `len(records) == 1`，**冇 assert `day_number`**。
- **建議**：`enumerate` 移去 `records.append` 位置（`day_number=len(records) + 1`），或 rename 做 `source_line`。

#### [AU-008] 同一 domain constant 散落 5 處，值仲唔一致 `[R]` + `[A]`

- **位置**：見下表
- **描述**：

  | 概念 | 位置 | 值 |
  |---|---|---|
  | 身高預設 | `src/common/config.py:46 BODY_HEIGHT_CM` | 170.0（**零 import**） |
  | 身高預設 | `src/weight/analysis.py:21 _HEIGHT_CM_DEFAULT` | 170.0（實際被用） |
  | 身高預設 | `frontend/src/lib/prefs.ts:18 heightCm` | 170 |
  | lbs→kg | `src/weight/analysis.py:67,117` | **0.454**（inline ×2） |
  | lbs→kg | `frontend/src/lib/prefs.ts:22 LBS_TO_KG` | **0.45359237** |
  | lbs→kg | `frontend/src/views/weight/model.ts:15` | **0.45359237**（inline，冇用上面個 const） |
  | track point 上限 | `src/dashboard/builder.py:17` / `src/api/schema.py:20` | 150 |
  | track point 上限 | `src/cli/export_data.py:27` / `frontend/src/data/api/source.ts:20` / `ActivityView.tsx:17` | 600 |

- **影響**：lbs→kg 兩邊差 0.00040763 → 70kg 身位差約 **0.09 kg**。API 出嘅 `weightKg` / `bmi`（Python ×0.454）同前端自己算嘅唔會對得上。`docs/data-pipeline.md:103` 更白紙黑字話 BMI「derived using `config.BODY_HEIGHT_CM`」— **直接錯**，改咗 `config.py:46` BMI 完全唔會變，亦冇任何測試會 fail。
- **建議**：Python 側改用 `0.45359237` 並抽入 `config.py`；`analysis.py` import `config.BODY_HEIGHT_CM`（刪 `_HEIGHT_CM_DEFAULT`）；前端 `model.ts:15` 改用 `LBS_TO_KG`；統一由 API 出 BMI 或統一前端算。

#### [AU-009] 年度統計邏輯跨語言重複，只靠人手抄嘅 fixture 保證一致 `[A]`

- **位置**：`src/dashboard/stats.py`（222 行）vs `frontend/src/data/stats.ts`（202 行）
- **描述**：TS 版自稱「a line-for-line port」。一致性唯一保障係兩邊各自測試釘住相同數字（`frontend/src/test/fixtures.ts:25` comment：「Same four runs as tests/test_dashboard_stats.py」），但兩組 fixture **分別用兩種語言人手打**，唔係共用檔案；CI 亦冇 job 比較兩者輸出。同樣情況見於 `downsample`（`builder.py:41` vs `frontend/src/lib/downsample.ts`）。
- **影響**：改 `stats.py` 一條公式而唔同步改 TS，兩個 mode 會靜靜地顯示唔同數字，**兩邊測試都照樣綠燈**。呢個係雙部署模式唯一嘅真實隱藏耦合。
- **建議**：共用 fixture 抽成 golden JSON（`tests/fixtures/stats_golden.json`），Python 同 Vitest 讀同一份；CI 加 job 比對同一輸入嘅輸出。

#### [AU-010] CI 唔 lint `api/`，但 `pyproject.toml` 同 pre-commit 都當佢要 lint `[A]` + `[R]`

- **位置**：`.github/workflows/pages.yml:40,43` vs `pyproject.toml [tool.ruff]`
- **描述**：CI 跑 `ruff check src tests` / `ruff format --check src tests` — **冇 `api`**。但 `pyproject.toml` 設 `src = ["src", "tests", "api"]`，且專登為 `"api/*"` 寫咗 `["D"]` per-file-ignore，證明原意要 lint。`.pre-commit-config.yaml` 亦冇排除 `api/`。
- **影響**：`api/graphql.py`（Vercel 唯一 entry point、歷史上出過 6 次部署事故）係全 repo 唯一冇 CI lint gate 嘅 production 檔案。
- **建議**：`pages.yml:40,43` 改 `ruff check src tests api` / `ruff format --check src tests api`。

#### [AU-011] Lint 規則集偏鬆 — 本報告一半 finding 係「開多兩條 rule 就抓到」`[R]`

- **位置**：`pyproject.toml [tool.ruff.lint] select`、`frontend/eslint.config.js`
- **描述**：

  | 缺失 rule / plugin | 抓唔到嘅 finding |
  |---|---|
  | ruff `ANN`（flake8-annotations） | AU-020（20 個缺 annotation） |
  | ruff `S`（flake8-bandit）`S113` | AU-014（`requests.get` 無 timeout） |
  | ruff `W1514` / `PLW1514` | AU-015（`open()` 無 encoding） |
  | `eslint-plugin-jsx-a11y` | AU-018（鍵盤導航） |
  | prettier / `max-len` | AU-012（637 字元行） |
  | `max-lines-per-function` | AU-021（146 行 component） |
  | `id-length` / `camelcase` | AU-022（`L`/`R`/`BOT`/`W`/`u`） |
  | `tseslint.configs.recommendedTypeChecked` | 型別層面 unsafe 操作 |

- **影響**：lint 綠燈唔代表冇問題，只代表**配到嗰幾條**冇問題。
- **建議**：ruff 加 `ANN` + `S`；ESLint 加 `jsx-a11y` + `max-len` + `max-lines-per-function`。分階段開，先 warn 後 error。

#### [AU-012] 前端零 formatter，行長失控（最長 637 字元）`[R]`

- **位置**：`frontend/package.json`（無 prettier）、無 `.prettierrc`、`eslint.config.js` 無 `max-len`、`.pre-commit-config.yaml` 無 JS/TS hook
- **描述**：23 個檔案有 >120 字元嘅行 —
  ```
  WeatherView.tsx      : 17 行 >120, 最長 637
  PerformanceView.tsx  : 18 行 >120, 最長 609
  WeightView.tsx       : 16 行 >120, 最長 541
  LoadView.tsx         : 22 行 >120, 最長 441
  ```
  Python 側同一問題有兩層防護（CI `ruff format --check` + pre-commit hook），前端側**零層**。
- **影響**：`WeightView.tsx:66` 一行塞晒 Chart.js data + options + scales + 兩個 tooltip callback → diff review 同 git blame 失效，亦係「檔案睇落只有 80 行」呢個假象嘅成因。
- **建議**：加 prettier（`printWidth: 100`）+ CI `npx prettier --check .` + pre-commit hook。
- **註**：Code Reviewer 原評 🔴 Critical。本報告下調為 🟡 — 理由：呢個係**流程 gate 缺失**而非運行時缺陷，唔會造成錯誤輸出；但佢係 AU-021 / AU-022 難以被發現嘅根因，優先級仍然高。

#### [AU-013] 全 codebase 零 logging，靜默失敗完全無 observability `[R]`

- **位置**：`src/`、`api/` 全域（`grep -rn "import logging\|logger\." src api` → 零 match）
- **描述**：三處靜默丟數據，冇一處留痕 — `parsers/base.py:51`（suppress 整個 activity）、`weather/collectors/hko_daily.py:45-46`（`except Exception: continue`，成個月天氣消失）、`weight/analysis.py:62-63`（skip 唔 match 嘅行）。
- **建議**：加 `logging.getLogger(__name__)`，上述三點改 `logger.warning(...)` 帶 file/line context。

#### [AU-014] 5 個 `requests.get()` 全部冇 `timeout=` `[R]`

- **位置**：`src/weather/collectors/hourly.py:39`、`warnings.py:17,41`、`hko_daily.py:32,42`
- **描述**：socket 預設無 timeout，可無限期 hang。`hko_daily.fetch_year()` 最壞會發 13 個 request，全部可卡死，CLI 冇進度提示。
- **建議**：`requests.get(url, timeout=(5, 30))`，值抽做 constant。

#### [AU-015] 4 個 `open()` 全部冇 `encoding=` `[R]`

- **位置**：`src/weight/analysis.py:52`、`src/cli/process_activities.py:41`、`src/cli/collect_weather.py:28`、`src/dashboard/builder.py:124`
- **描述**：依賴 `locale.getpreferredencoding()`。天氣數據含 `Temperature (°C)` 等非 ASCII（見 `tests/conftest.py:79`），non-UTF-8 locale 或 Windows 上會 `UnicodeDecodeError`。CI 上 Linux 剛好 UTF-8 所以測唔到。
- **建議**：全部加 `encoding="utf-8"`。

#### [AU-016] `metrics.py` 三個 public function 冇除零保護 `[R]`

- **位置**：`src/activities/metrics.py:47,53,59`
- **描述**：實跑 `pace_min_per_km("00:30:00", 0.0)` / `pace_min_per_mile(...)` / `speed_mph("00:00:00", 5.0)` 全部 `ZeroDivisionError`。`distance_km=0`（室內 / GPS 失效）同 `duration=00:00:00` 都係真實 Garmin 數據。同 codebase 嘅 `common/time.py:52 pace_str()` 就有 guard — 兩套處理。該 module coverage 100%，但測試只覆蓋 happy path。
- **建議**：加 guard 並補 edge case 測試。（若採納 AU-023 刪 module，本條自動消失。）

#### [AU-017] `seconds_to_hhmmss()` docstring 講 `HH:MM:SS`，實際格式唔係 `[R]`

- **位置**：`src/common/time.py:42-44`
- **描述**：`3661s -> '1:01:01'`（無 zero-pad）、`90000s -> '1 day, 1:00:00'`（超 24 小時爆格式）、`-10s -> '-1 day, 23:59:50'`。`pace_str()` 同樣問題（1km 用 30 小時 → `'1 day, 6:00:00'`）。`str(timedelta)` 唔係 duration 格式化工具。
- **建議**：自行 format `f"{h:02d}:{m:02d}:{s:02d}"`，並修正 docstring。

#### [AU-018] Static data source 永久快取失敗嘅 fetch `[R]`

- **位置**：`frontend/src/data/static/source.ts:36-41`
- **描述**：`if (!cache.has(url)) cache.set(url, fetcher(url));` — rejected promise 亦留喺 map，之後每次 `load()` 攞返同一個 rejection。TanStack `retry: 1`（`DataProvider.tsx:25`）retry 嗰次即刻攞到同一個 rejection，用戶要 full reload 先救得返。
- **建議**：`fetcher(url).catch(err => { cache.delete(url); throw err; })`。

#### [AU-019] 可點擊嘅 `<tr>` / `<li>` 冇鍵盤支援 `[R]`

- **位置**：`frontend/src/components/DataTable.tsx:31-36`、`views/overview/RecentActivities.tsx:39-43`、`views/overview/PersonalBestList.tsx:22-25`
- **描述**：`<tr onClick={...}>` 冇 `tabIndex` / `onKeyDown` / `role`。呢三個係去 activity 詳情頁嘅主要導航入口，**鍵盤用戶完全去唔到**。
- **正面對照**：`views/overview/Heatmap.tsx:59-68` 用咗 `<button type="button" aria-label={text}>`，完全正確 — 團隊識做，只係唔一致。根因係冇 `eslint-plugin-jsx-a11y`（見 AU-011）。
- **建議**：照 `Heatmap.tsx` 嘅做法改，並加 a11y plugin 防止回歸。

#### [AU-020] 20 / 108 個 Python function 缺 type annotation `[R]`

- **位置**：public function 缺 return type 者：`src/common/time.py:59 activity_time_range()`；其餘 19 個為 private helper / Flask route / WSGI callable
- **描述**：`_safe(value: str)` 冇 return type、`_to_float(value)` 冇 param type 呢類最影響可讀性（caller 睇唔出會唔會收到 `None`）。ruff 冇 select `ANN` 所以查唔到（見 AU-011）。

#### [AU-021] 函數 / 組件過長 `[R]`

- **Python 14 個 >30 行**：`kml.py:28 parse()` **96 行**（一個 method 做四件事：XML 導航、BeautifulSoup HTML table 解析、track point 抽取、pandas lap 聚合）、`tcx.py:29` 87、`gpx.py:30` 80、`weight/analysis.py:24` 55、`export/records.py:82` 49 …
- **TS 13 個 >50 行**：`RouteMap.tsx:9` **146 行**（同時做 canvas 繪圖、投影計算、resize 監聽、pointer hit-testing、imperative DOM 寫入）、`SeriesChart.tsx:13` 133、`ActivityView.tsx:26` 125 …
- **註**：配合 AU-012（行長 >600），實際複雜度遠高於行數所示。
- **建議**：優先拆 `kml.py:parse()` 同 `RouteMap.tsx`；其餘可隨 touch 逐步處理。

#### [AU-022] 前端命名縮寫違規 `[R]`

- **位置**：`SeriesChart.tsx:6-10`（`L` / `R` / `TOP` / `BOT` / `HEIGHT`）、`DiamondGrid.tsx:6`（`LABEL_W`）、`WeatherView.tsx:13`（`COND_COLORS`）、`WeightView.tsx:22`（`const W = weight.data`）、`WeightView.tsx:18`（`weightUnit: u`，之後喺 25 處用單字母 `u`）
- **建議**：`PADDING_LEFT` / `PADDING_RIGHT` / `PADDING_BOTTOM`、`LABEL_WIDTH`、`CONDITION_COLORS`、`weightEntries`、`weightUnit`。

#### [AU-023] `src/activities/metrics.py` 整個 module 喺 production 冇消費者 `[A]`

- **位置**：`src/activities/metrics.py`（80 行）
- **描述**：唯一 import 佢嘅係 `tests/test_activities_metrics.py:3`（8 個測試）。MET / kcal 計算冇用武之地 — TCX 直接提供 device calories（`export/records.py:120` 用 `activity.calories`）。但 `src/__init__.py:5` 同 `docs/architecture.md:22` 都仲當佢係 `activities` 嘅正式職責宣傳。
- **建議**：或接入（為 GPX-only 冇 calories 嘅跑步做估算），或移去 `legacy/`。唔好留喺 `src/` 扮 production API。

#### [AU-024] 重複 helper — 6 個數值轉換 + 1 個逐字重複 function `[R]` + `[A]`

- **位置**：

  | Helper | 位置 | 說明 |
  |---|---|---|
  | `_to_float` | `dashboard/builder.py:34` / `export/records.py:75` / `weather/collectors/hko_daily.py:12`（`_safe`） / `hourly.py:89`（`_safe_float`） | **四份逐字相同** |
  | `_num` vs `_round` | `builder.py:22` vs `records.py:64` | 同功能，NaN 檢測方式唔同（`math.isnan` vs `value != value`） |
  | `_hhmmss_to_sec` | `activities/metrics.py:79-80` | 同 `common/time.py:47-49` **逐字相同** |

- **影響**：兩份 NaN 處理邏輯可各自演化，而 SQLite writer 同 static JSON writer（`static_json.py:32` 用 `allow_nan=False` 會直接 raise）對 NaN 容忍度已經唔一致。
- **建議**：收歸 `src/common/numeric.py`；`metrics.py` 直接 import `common.time.hhmmss_to_seconds`。

#### [AU-025] Design token 三份 source of truth `[R]`

- **位置**：`frontend/src/index.css:4-18`（Tailwind v4 `@theme`）vs `components/charts/theme.ts` vs 各 view 嘅 `rgba()` 硬寫 vs `RouteMap.tsx` 硬寫
- **描述**：9 個 token（`--color-accent` `#4f8ef7`、`--color-accent2` `#34d399`、`--color-warn` `#f59e0b`、`--color-danger` `#f87171`、`--color-border`、`--color-surface`、`--color-surface2`、`--color-text`、`--color-muted`）同時硬寫喺 TS。改 palette 要改 **4 個地方**。
- **註**：Chart.js / canvas 2D 確實食唔到 CSS variable，需要一份 TS 鏡像 — 但目前係三份。
- **建議**：`theme.ts` 用 `getComputedStyle(document.documentElement).getPropertyValue('--color-accent')` 讀返 CSS 真值（或最少令 `theme.ts` 成為唯一鏡像）；`RouteMap.tsx` 同所有 `rgba()` 改為 import `COLORS` + `withAlpha(color, a)` helper。

#### [AU-026] `WarningSprite.tsx` — 整個 component 係 inline SVG，30+ 個硬寫 hex `[R]`

- **位置**：`frontend/src/components/WarningSprite.tsx:1-28`
- **描述**：24 個 `<symbol>`，每個一行 130–331 字元，內含 `#e2e8f0` / `#f5b800` / `#e53935` / `#facc15` 等硬寫色值。SVG sprite sheet 本身係合法 pattern（比每個 icon 一檔更慳 request），但 (a) 顏色完全繞過 token；(b) 一行一 symbol 令 diff 不可讀。
- **建議**：fill 改為引用 `COLORS` 或 `currentColor` + CSS class，或搬去 `src/assets/svg/warnings.svg` 用 `?react` import。

#### [AU-027] 26 處 module-scope business constant 留喺 `.tsx` `[R]`

- **位置**：`KpiCard.tsx:1 ACCENTS`、`PersonalBestList.tsx:7 BAR`、`Heatmap.tsx:8-10 CELL/GAP/LEVEL_CLASS`、`RecentActivities.tsx:8 RECENT_COUNT`、`RouteMap.tsx:3 HOVER_RADIUS_PX`、`SeriesChart.tsx:6-10`、`ActivityView.tsx:17,19 TRACK_POINTS/SPECS`、`DiamondGrid.tsx:6-7 LABEL_W/GAP`、`ActivitiesView.tsx:17 COLUMNS`、`WeatherView.tsx:13 COND_COLORS`、`SettingsView.tsx:9 input`
- **註**：`GAP` 喺 `Heatmap.tsx:9` 同 `DiamondGrid.tsx:7` 各定義一次（同值 4）。
- **建議**：抽去 `src/constants/ui/dimensions.ts`、`constants/ui/colors.ts`、`constants/domain/*.ts`。

#### [AU-028] `src/components/` 6 個頂層 `.tsx`，未跟 `<type-group>/<name>/` 結構 `[R]`

- **位置**：`Card.tsx`、`KpiCard.tsx`、`DataTable.tsx`、`WeatherTag.tsx`、`WarningIcons.tsx`、`WarningSprite.tsx`
- **註**：Code Reviewer 明確標註 —「呢啲係 11–48 行嘅細 component，扁平結構喺呢個規模係合理工程判斷，唔係疏忽」。本報告同意：呢條應作**convention 對齊決策**處理，唔係缺陷。要改就一次過改晒，唔好半新半舊。

#### [AU-029] `weight/analysis.py` 一半 public function 冇 production 消費者 `[A]`

- **位置**：`analysis.py:81 build_dataframe`、`:131 monthly_summary`、`:149 weekday_summary`、`:167 describe_weight`（四個都喺 `__all__`）
- **描述**：production 只用 `parse_weight_file`（`cli/export_data.py:25`）。`build_dataframe` 有測試，另外三個連測試都冇。呢啲係**唯一**令 `weight` package 需要 pandas + numpy 嘅代碼。
- **建議**：確認冇 notebook / ad-hoc 用途後刪走；`parse_weight_file` 本身唔需要 pandas。

#### [AU-030] 前端依賴 14 個已知漏洞（12 high / 2 moderate）`[M]`

- **位置**：`frontend/package-lock.json`
- **描述**：`npm audit --package-lock-only` 實測結果 —

  | 套件 | 嚴重度 | 漏洞 | 路徑 |
  |---|---|---|---|
  | `lodash <=4.17.23` | **high** | GHSA-r5fr-rjxr-66jc（`_.template` code injection）、GHSA-f23m-r3pf-42rh（`_.unset`/`_.omit` prototype pollution） | ← `@graphql-codegen/plugin-helpers` ← `client-preset` |
  | `@vitest/mocker 2.1.0–4.1.10` | moderate | GHSA-82fw-gwwq-j7x9（path traversal / arbitrary file read） | ← `vitest` |

- **風險評估**：**兩者都係 devDependency**，唔會打包入 production bundle。實際風險限於 build / test 環境（惡意 schema 或 test fixture）。修復需 breaking upgrade（`vitest@5`、`@graphql-codegen/client-preset@6.2.0`）。
- **建議**：排入下一個維護窗口做 major upgrade，唔急於 hotfix。同時 CI 加 `npm audit --audit-level=high` 作為**非阻塞**告警。
- **對照**：Python 側 `pip-audit`（10 個 runtime dep）實測 **No known vulnerabilities**。

#### [AU-031] 完全冇 HTTP security headers；CORS 未明示 `[M]`

- **位置**：`vercel.json` `headers` 段、`src/api/app.py`、`api/graphql.py`
- **描述**：`vercel.json` 只為 `/assets/(.*)` 設 `Cache-Control`。**零** CSP / HSTS / X-Frame-Options / X-Content-Type-Options / Referrer-Policy / Permissions-Policy。`grep -riE "cors|access-control"` 喺 `api/` `src/` `vercel.json` 全部零 hit — 即係採用瀏覽器預設 same-origin（前端同 API 同域，功能上冇問題），但呢個係**隱含**而非明示嘅決定。
- **影響**：站點可被任意 iframe 嵌入（clickjacking）；無 nosniff；配合 AU-001 嘅開放 GraphiQL，攻擊面比必要大。
- **建議**：`vercel.json` 加一段 `"source": "/(.*)"` 嘅 security headers（至少 `X-Frame-Options: DENY`、`X-Content-Type-Options: nosniff`、`Referrer-Policy: strict-origin-when-cross-origin`、`Strict-Transport-Security`）。CSP 因為 Chart.js / canvas 需要驗證，可後置。

#### [AU-032] Python 依賴零版本上限，build 不可重現 `[M]`

- **位置**：`pyproject.toml [project] dependencies`
- **描述**：10 個 runtime dep 中 8 個完全無版本約束（`numpy` / `pandas` / `python-dateutil` / `pytz` / `requests` / `beautifulsoup4` / `lxml`），只有 `sqlalchemy>=2.0` / `flask>=3` / `strawberry-graphql>=0.250` 有下限，**全部無上限**。`requirements.txt` 內容只有 `.`（安裝本 package）。
- **影響**：Vercel 同 GitHub Pages 每次 build 都解析最新版。pandas 3.0 或 numpy 2.x 嘅 breaking change 會令**歷史 commit 都 build 唔返**，而且失敗會出現喺 deploy 而非 PR（因 AU-004，PR 根本冇 CI）。
- **建議**：加 upper bound（如 `pandas>=2,<3`），或為 CI / 部署維護一份 `requirements.lock`（`pip-compile`）。
- **對照**：前端側有 `package-lock.json` + `npm ci`，已鎖定 — 兩側標準唔一致。

#### [AU-033] 冇 coverage gate，Python 實測 71% < DoD 要求嘅 80% `[R]`

- **位置**：`pyproject.toml [project.optional-dependencies] dev`、`frontend/package.json` devDependencies、`pages.yml`
- **描述**：`dev` extras 只有 `pytest, ruff, pre-commit`，**無 `pytest-cov`**；前端無 `@vitest/coverage-v8`；CI 兩個 job 都冇 coverage step 或 threshold。Code Reviewer 需自行安裝先量到數。
- **實測**：Python **71%**（1,276 stmts / 374 miss）；Frontend **91.18% stmt / 86.72% branch / 70.82% func**。
- **建議**：加 `pytest-cov` + `@vitest/coverage-v8`，CI 先設 **70% 保底**再逐步升到 80% — 一開就 80% 會即刻紅。

#### [AU-034] 隱私 — public repo 含個人 GPS 軌跡，git history 仍有完整 Garmin account export `[M]`

- **位置**：`data/`（1,109 個 tracked 檔 / 184MB）、git history
- **描述**：Repo 為 **public**。`data/raw/garmin/{tcx,gpx,kml}/` 各 365 個檔含完整 GPS 軌跡（可推斷住址、日常路線、作息時間）。History 最大 blob 包括 `Garmin/fa4684ae-…_1/DI_CONNECT/…/UploadedFiles_0-_Part1.zip`（12MB）同 `Garmin/run365_tcx_data.json`（12MB）。
- **現狀評估**：呢個係**已知並已文件化嘅決定** — README `## Privacy` 明確講「earlier tags still contain the full export (profile, device and consent records) because the history has not been rewritten」，LICENSE 末段亦將 `data/` 排除於 MIT 授權外。所以唔算疏忽。
- **仍建議考慮**：(a) GPS 軌跡起訖點做半徑模糊化（住址推斷風險最高）；(b) 若唔需要保留 v1/v2 tag 嘅完整 export，用 `git filter-repo` 清走 history 中嘅 `Garmin/` 目錄；(c) `.vercelignore` 補上 `data/raw/garmin/sleep_*.json`、`daily_summary_*.json`、`summarized_activities.json`、`data/raw/weight/activities.xlsx` — 呢批 `docs/data-pipeline.md:66-68` 標明「Not yet consumed」嘅個人健康數據，每次部署都上傳去 Vercel build（雖然 `excludeFiles` 阻止咗入 function bundle）。

#### [AU-035] 文件與代碼 drift（8 處）`[A]`

| 位置 | 文件講 | 實際 |
|---|---|---|
| `docs/data-pipeline.md:103` | BMI 用 `config.BODY_HEIGHT_CM` | **錯** — 實際用 `analysis.py:21 _HEIGHT_CM_DEFAULT`（見 AU-008） |
| `docs/architecture.md:104` | file I/O 限於 `load_jsonl` 同 `write_data_js` | `write_data_js` 全 repo 不存在 |
| `docs/architecture.md:28` | 「**Three** console scripts」 | `pyproject.toml` 定義 **4** 個 |
| `docs/architecture.md:5` | 「single JSON payload that a static web dashboard renders」 | v2 講法，同 `:71-89` 自己描述嘅雙 mode 矛盾 |
| `docs/architecture.md:138` | 「(93 tests)」 | 實測 **90** 個 |
| `docs/architecture.md:19-28` | api 依賴 `export.models`, `dashboard.stats` | 仲有 `export.sqlite`、`dashboard.builder` |
| `src/__init__.py:8` | dashboard「builds the payload for the static web dashboard」 | payload 已消失 |
| `src/dashboard/__init__.py:1` | 同上 | stale |

**已核對一致者**：`docs/data-pipeline.md:6-28` 嘅目錄樹同 `data/` 實際內容完全對得上；`docs/deployment.md:41-48` 嘅部署踩坑表同 git log（`126032b`…`2f1a1da` 連續 10 個 `fix(deploy)`）完全對得上 — 呢啲決定係有代價換返嚟，唔應亂改。

---

### 🟢 Observation（可改善）

- **[AU-036]** `src/common/config.py` 有 4 個零 import 常數：`SUMMARIZED_ACTIVITIES_JSON:23`、`SUN_MOON_JSON:34`、`DAILY_WEIGHT_JSON:39`、`BODY_HEIGHT_CM:46`。config 係「唯一知道 data 喺邊」嘅 module，入面有死路徑會令人以為呢啲檔案有人讀。`[A]`
- **[AU-037]** `src/weather/models.py:72 SunMoon` dataclass 冇對應 collector（原始 scraper 停留喺 `legacy/03_GetSunMoonRiseSetHistory.py`）。`docs/data-pipeline.md:83` 仍將佢列為正式 source，line 90 先補「currently unused」。`[A]`
- **[AU-038]** `dashboard/builder.py:70-84 merge_temperature(tcx_points, gpx_points)` 第一個參數完全冇用，docstring line 75 自認「Unused; kept so the signature reads as a merge」— 為咗讀落順眼保留死參數係倒轉咗。改名 `temperatures_by_time(gpx_points)` 即可。`[A]`
- **[AU-039]** `src/common/geo.py:45` 喺 `total_track_distance()` 函數體內 `import pandas as pd` — 應係為 API cold-start 而設（`test_api_imports.py` 守住），但**冇 comment 解釋 WHY**，下個人好易「順手」搬去檔案頂而打爛。順帶：用 `DataFrame.apply()` row-wise 算 haversine 係最慢寫法，numpy 向量化可快一個數量級。`[A]` `[R]`
- **[AU-040]** `run365-activities` 產出嘅 `data/processed/activities_{tcx,gpx,kml}.jsonl` **冇下游消費者** — `cli/export_data.py:52-54` 重新 parse 原始目錄而非讀 jsonl。即兩條平行 parse path；KML parser（123 行）唯一用途就係餵一個冇人讀嘅檔案。`docs/data-pipeline.md:116` 標為「ad-hoc analysis」，算誠實。`[A]`
- **[AU-041]** `export/static_json.py:42-43 shutil.rmtree()` 無 guard。CI 傳 `--static-dir frontend/public/data`，打錯做 `frontend/public` 就會刪走成個 public 目錄。建議加「目標存在但唔含 `meta.json` → 拒絕」sanity check。`[R]`
- **[AU-042]** `frontend/src/app/router.tsx:73` — `element: <Navigate to={landingPath()} replace />` 喺 module 載入時求值一次。用戶喺 Settings 改咗 landing view 後，同一 SPA session 內返 `/` 仍去舊 view，要 full reload。改成讀 `getPrefs()` 嘅細 component 即可。`[R]`
- **[AU-043]** `frontend/src/App.tsx` 擺喺 `src/` 根，但其測試喺 `src/app/App.test.tsx`，其餘 app 層檔案（`Layout.tsx` / `router.tsx` / `views.ts`）全部喺 `src/app/`。搬入 `src/app/` 即統一。`[A]`
- **[AU-044]** 根 `.gitignore` 缺 `.coverage` / `htmlcov/`（`frontend/.gitignore` 反而完整）。加 coverage gate（AU-033）之前順手補。`[R]` `[M]`
- **[AU-045]** `legacy/`（1,948 行，含 830 行 `legacy/test.py`）完全喺質量體系外 — pre-commit exclude、CI 唔掂、無測試。作為 reference-only 合理，但 `legacy/test.py` 呢個檔名令 `testpaths = ["tests"]` 成為唯一防線。建議喺 `legacy/README.md` 明確標註或 rename。`[R]`
- **[AU-046]** 其他小項：`builder.py:141 best_gap = 10**9` magic sentinel（用 `math.inf` 更清楚）；`api/schema.py:~183 def activity(self, info, id: ...)` shadow builtin `id`；`common/time.py:70 start.replace(minute=0, second=0)` 冇清 `microsecond`；`tests/test_cli_process_activities.py:41 import pytest` 放喺 function 內；`frontend/src/views/activity/usePlayback.ts:43` magic `1000`；`data/hooks.ts:32,41` non-null assertion `id!`；`RouteMap.tsx:98-100` imperative 改 React 管理嘅 DOM。`[R]`

---

## 值得保留嘅良好實踐

呢部分同 findings 同等重要 — 以下屬**唔應該喺重構中被破壞**嘅設計：

| 項目 | 位置 | 為何重要 |
|---|---|---|
| 單一 `ExportRecords` IR | `src/export/records.py:41` | SQLite 同 static JSON 結構上唔可能唔一致 |
| `DataSource` interface（10 行 seam） | `frontend/src/data/source.ts:7-10` | 雙部署模式只差一個環境變數，Python 側零條件分支 |
| Import 邊界測試 | `tests/test_api_imports.py:9` | 用 subprocess 釘死 serverless cold-start 依賴預算 |
| GraphQL schema drift gate | `pages.yml:49` `run365-schema --check` | 跨語言 contract，好過大部分項目 |
| `conftest.py` fixture 設計 | `tests/conftest.py`（126 行） | 有意注入 unsorted（L55）、重複 warning（L103）、空 signal（L107）、非數值降雨 `Trace`（L68）、無 GPS 活動（L49） |
| TS 嚴格度 | `tsconfig.app.json` | `strict` + `noUnusedLocals` + `noUnusedParameters` + `verbatimModuleSyntax`；全 codebase **零 `any`、零 `@ts-ignore`、零 `console.*`**（實 grep 確認） |
| Docstring 質素 | 全 Python codebase | Google convention 全覆蓋，多處寫咗真正嘅 WHY（如 `api/graphql.py` 解釋 Vercel 限制、`builder.py` 解釋模組歷史） |
| Provider 缺失明確 throw | `frontend/src/data/context.ts:8` | 唔係靜默回 undefined |
| 穩定 array identity | `ActivityView.tsx:152 const EMPTY: number[] = []` | 避免每 frame 重建 effect — 細節功夫 |
| DB 唯讀 + 資源管理 | `src/api/db.py` | `mode=ro`、`session_scope`、`engine.dispose()` 正確 |
| SQL 注入防護 | 全 API 層 | 全部 SQLAlchemy ORM 參數化，零 raw SQL |

---

## 技術債評估

| 類別 | 程度 | 描述 | 預計處理成本 |
|---|---|---|---|
| 代碼質量 | **中** | 外殼優秀（lint 綠、type 綠、177 test 綠、docstring 認真），但 6 個重複 helper、常數散落 5 處值不一、14 個 Python function + 13 個 component 超長 | 3–5 日 |
| 測試覆蓋 | **高** | Python 71% < 80%；三個 parser（產品資料入口）零測試；`src/weather/` 整個 sub-package 0%；三個「100% coverage 但 bug 漏網」實例證明測試偏 happy path | 4–6 日 |
| 文件完整性 | **中** | `docs/` 五份文件質素高且大部分準確，但 8 處 drift，其中 `data-pipeline.md:103` 係直接錯誤 | 0.5–1 日 |
| 安全性 | **中** | 無注入、無 secret、DB 唯讀、import 邊界受控。但 GraphQL 零 cost 控制 + production GraphiQL + 零 security headers + 5 個無 timeout 嘅 outbound HTTP | 2–3 日 |
| 性能 | **低–中** | downsample、`selectinload`、static memoise、chart 數據前置計算都做咗。扣分集中喺 `Activity.track` N+1 同 `service.track()` 讀晒先降採樣 | 1–2 日 |
| 依賴更新 | **中** | Python 零 CVE 但**零版本上限**（不可重現 build）；前端 14 個漏洞但全部 devDependency，修復需 breaking upgrade | 1–2 日 |
| CI / 流程 | **高** | **default branch 零 CI gate**（AU-004）；CI 漏 lint production `api/`；前端零 formatter；無 coverage gate；lint 規則集偏鬆 | 1–2 日 |

---

## 代碼質量評分

Code Reviewer 給出 **68 / 100**：

| 維度 | 得分 | 主要扣分 |
|---|---|---|
| 正確性 | 15 / 25 | AU-003 時區、AU-006 BMI、AU-007 day_number、AU-016 除零、AU-017 duration 格式、AU-018 快取、AU-008 跨語言常數 |
| 安全性 | 15 / 20 | AU-001（GraphiQL + 無 depth limit + N+1）、AU-014 無 timeout、`/api/health` 回傳 filesystem path |
| 可維護性 | 14 / 20 | AU-024 重複 helper、AU-008 常數散落、AU-021 超長函數、AU-025 顏色三份、AU-027 TSX 常數、AU-013 零 logging |
| 測試覆蓋 | 9 / 15 | AU-002 parser 零測試、`weather/` 0%、71% < 80% |
| 性能 | 7 / 10 | AU-001 N+1、`total_track_distance` 用 `DataFrame.apply` |
| 代碼風格 | 8 / 10 | AU-012 前端零 formatter、AU-010 CI 漏 api/、AU-011 規則集偏鬆 |

**一句總結**：呢個 codebase 嘅**外殼**做得相當好，但**核心 ingestion path（三個 parser + 整個 weather collector）係零測試 + 吞 exception + 零 logging**，加上 `export_data.main()` 冇 zero-record guard —— parser 一 regress，pipeline 會靜靜雞出一個空 dashboard 然後 CI 亮綠燈 deploy。而 `parse_datetime` 嘅 `+07:37` bug 喺 89% coverage 之下完全冇被發現。兩點合埋反映真正問題唔係「唔夠測試」，而係**測試同 lint 都繞開咗風險最高嗰幾個檔案**。

---

## 建議方向

### 方案 A：漸進修復現有系統 ✅ 推薦

- **適用情況**：架構本身健康（零循環依賴、無 god object、抽象正確），問題集中喺**測試盲點**同**流程 gate**，兩者都可以增量處理而唔需要動架構。
- **預計工作量**：P0 約 3–4 日，P1 約 5–7 日，P2 隨手處理。
- **優點**：保留三個做得正確嘅核心設計（單一 IR、雙部署 seam、import 邊界測試）；每步都可獨立驗證；風險低。
- **缺點**：`dashboard` 改名（AU-005）同前端結構對齊（AU-028）呢類「一次過做至有意義」嘅項目需要專門窗口。

### 方案 B：重建

- **適用情況**：不適用。
- **理由**：無循環依賴、無 god object、最大檔案 267 行、抽象層次正確、177 個測試綠燈、docstring 完整、CI 有 schema drift gate。重建會丟失三次演進累積嘅部署知識（`docs/deployment.md:41-48` 記錄嘅 10 個 `fix(deploy)` 係用失敗 build 換返嚟嘅）。

### 推薦

**方案 A**。呢個項目唔係 rescue case，係一個**質素高於同等規模個人項目平均水平**、但有幾個明確缺口嘅 active codebase。優先修「會靜默出錯」同「冇 gate 攔截」嗰批，其餘隨 touch 處理。

---

## 下一步建議

| 優先級 | 行動 | 對應 finding | 負責 Agent | 預計工作量 |
|---|---|---|---|---|
| **P0** | 修 CI 觸發 branch，令 default branch 嘅 PR 有 gate | AU-004 | DevOps | 0.5 日 |
| **P0** | Parser 測試 + 收窄 `contextlib.suppress` + `export_data` zero-record guard | AU-002 | Backend | 1.5 日 |
| **P0** | `parse_datetime` 改 `zoneinfo` + 補 `utcoffset()` assert（順帶修 3.12 deprecation） | AU-003 | Backend | 0.5 日 |
| **P0** | GraphQL 加 depth/complexity limit + `activities` 分頁 + GraphiQL 改環境變數控制 | AU-001 | Backend | 1 日 |
| **P1** | ruff 加 `ANN` + `S`；CI 加 `api/` — 一個 config 改動關掉三條 finding | AU-010, AU-011, AU-014, AU-015, AU-020 | DevOps | 0.5 日 |
| **P1** | Weight 兩個 bug + 對應 assert | AU-006, AU-007 | Backend | 0.5 日 |
| **P1** | 前端加 prettier + CI check + pre-commit hook | AU-012 | Frontend | 0.5 日 |
| **P1** | 加 coverage gate（先 70% 保底） | AU-033, AU-044 | DevOps | 0.5 日 |
| **P1** | 常數統一（lbs→kg `0.45359237`、身高、track point limit） | AU-008 | Backend + Frontend | 1 日 |
| **P1** | `vercel.json` 加 security headers | AU-031 | DevOps | 0.5 日 |
| **P1** | Python 依賴加 upper bound 或 lock file | AU-032 | DevOps | 0.5 日 |
| **P2** | a11y 修 3 處鍵盤導航 + 加 `eslint-plugin-jsx-a11y` | AU-019 | Frontend | 0.5 日 |
| **P2** | 加 logging；helper 收歸 `common/numeric.py` | AU-013, AU-024 | Backend | 1 日 |
| **P2** | `dashboard` → `analytics` 改名（4 個 import site） | AU-005 | Backend | 0.5 日 |
| **P2** | 死碼處理：`metrics.py`、`SunMoon`、4 個死 config 常數、`weight` 三個無消費者 function | AU-023, AU-029, AU-036, AU-037 | Backend | 1 日 |
| **P2** | 修正 8 處文件 drift | AU-035 | PM | 0.5 日 |
| **P2** | 前端結構對齊（design token 收斂、TSX 常數外移、命名、組件拆分） | AU-025, AU-026, AU-027, AU-028, AU-022, AU-021 | Frontend | 3 日（建議一次過） |
| **P2** | 統計邏輯 golden-file 比對（Python ↔ TS） | AU-009 | Backend + Frontend | 1 日 |
| **P3** | 前端依賴 major upgrade（vitest 5、graphql-codegen 6） | AU-030 | DevOps | 1 日 |
| **P3** | 隱私加固（GPS 起訖模糊化 / history 清理 / `.vercelignore` 補漏） | AU-034 | 需用戶決策 | 依選項而定 |

---

## 審計方法與可信度說明

| 項目 | 說明 |
|---|---|
| 掃描方式 | Architect（架構層）+ Code Reviewer（代碼層）平行獨立掃描，Main agent 負責基建 / 依賴 / 安全層並合併仲裁 |
| 實際執行嘅驗證 | `pytest`（90 passed）、`pytest --cov`（71%）、`vitest run`（87 passed）、vitest coverage（91.18%）、`ruff check` / `ruff format --check`（全綠）、`eslint .`（exit 0）、`tsc -b`（pass）、`pip-audit`（0 CVE）、`npm audit`（14 vulns）、GitHub API（default branch）、`git ls-remote` / `rev-list`（branch 狀態） |
| 未能驗證者 | 無。所有數字均為實測，冇估算 |
| 環境註記 | 容器原無 pytest / numpy / node_modules。Code Reviewer 安裝咗 `run365days[dev]`、`pytest-cov`、`frontend/node_modules`、`@vitest/coverage-v8`（`--no-save`）以取得真實數字。`pyproject.toml` / `package.json` **未被修改**，`git status` 已確認 clean，`.coverage` artifact 已刪除 |
| 本次審計未修改任何 source code | ✅ 唯一寫入為本報告及 `.proj-docs/index.md` |

---

**報告完**
