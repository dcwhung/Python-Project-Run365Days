# Batch Review — 2026-09-16 — CUI-0029 / 0033 / 0034 / 0030 / 0031


## 整體 verdict

- 涵蓋 commit：`ccf7609`(0031) `bbe87b6`(0030) `d68a52a`+`87693d4`(0029) `4cce9d7`+`1fa9309`(0033) `39298b2`(0034)
- Delta：`a494513..HEAD`，20 files，+1427 / −299
- **Status：⚠️ warn（86/100）** — 0 🔴 / 0 🟡 / 7 🟢（S-073 … S-079）
- Hard gates **6/6 pass**。5 張票**全部逐張 pass**；warn 純粹來自累積嘅 documentation suggestion 債（7 條舊 open + 7 條新開），**無一條阻 merge 或 QA**

### Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| Lint | `ruff check src tests` / `eslint .` | ✅ pass（All checks passed / 無輸出）|
| Format | `ruff format --check src tests` | ✅ pass（58 files already formatted）|
| Type check | `npm run typecheck`（codegen + `tsc -b`）| ✅ pass |
| Tests (py) | `pytest tests -q` | ✅ **417 passed**（預期 417）|
| Tests (fe) | `npm test` | ✅ **141 passed / 25 files**（預期 141 / 25）|
| SDL 同步 | `run365-schema --check frontend/schema.graphql` | ✅ up to date |
| Coverage | `pytest --cov=src` | ✅ **95% total**；`api/schema.py` **100%**、`api/service.py` **99%**（唯一漏行 `service.py:382`）|
| Security scan | `npm audit --prefix frontend` | ✅ pass — 見下 |

**`npm audit`（本 repo 首次執行）**：14 vulnerabilities（12 high / 2 moderate / **0 critical**）。
逐條追源：全部係 **devDependency 傳遞依賴** —— `@graphql-codegen/*` → `lodash`（2 條 high 根因，GHSA-r5fr-rjxr-66jc / GHSA-f23m-r3pf-42rh）同 `vitest` → `@vitest/mocker`（GHSA-82fw-gwwq-j7x9，moderate）。
決定性核實：**`npm audit --omit=dev` → `found 0 vulnerabilities`**（prod 16 個依賴、dev 619 個）。
本批 **零新增依賴**（`package.json` / `package-lock.json` / `pyproject.toml` 喺 delta 入面完全冇改動），所以「新增依賴無 Critical/High CVE」呢個 gate = **pass**。build-time toolchain 嘅 12 high 屬 pre-existing，唔喺本批 scope，另行處理即可。

### 評分結果

| 維度 | 得分 | 滿分 | 備註 |
|---|---|---|---|
| 正確性 | **25** | 25 | 獨立重做全部通過；134,041 行 track 逐點比對零差異 |
| 安全性 | **20** | 20 | 淨值係**關閉**兩個洩漏面（server log 9 frames、client blob 1247 字元）；prod CVE = 0 |
| 可維護性 | **10** | 20 | 舊 open S-063/064/065/068/069/070/071 各 −1（−7）；新 S-076/077/079 各 −1（−3）|
| 測試覆蓋 | **11** | 15 | 新 S-073/074/075/078 各 −1（−4）|
| 性能 | **10** | 10 | omitted path 多一個 statement（實測 +19%），且 GraphQL 根本去唔到 |
| 代碼風格 | **10** | 10 | ruff + eslint 全清 |
| **總分** | **86** | **100** | |

**結果：⚠️ warn**（75–89）。交叉核對：上輪 90（7 條 open S），本輪 0 條舊 S 關閉 + 7 條新 S → 90 − 7 = 83。兩種算法都落喺 warn 區間，verdict 穩健。

---

## Section A — CUI-0029（最高風險，獨立重做）

### A-1 `BudgetExceededError` 機制 —— **Lane 對，張票原方案 A 錯**

我冇照單全收，自己起咗真 Flask app（`create_app` + 真 `data/processed/run365.db`）、OS-level 捕捉 stderr、跑三種 logging 狀態 × 六個 variant。**第一次嘗試用 `redirect_stderr` 係量錯嘅**（`StreamHandler` 喺建立時已綁死 `sys.stderr`，buffer 永遠係空），改用 subprocess 級捕捉後結果如下：

| logging 設定 | variant | stderr bytes | Traceback | 絕對路徑 frame |
|---|---|---|---|---|
| **bare**（乜都唔設，`lastResort`）| **HEAD** | **0** | **0** | **0** |
| bare | mutant：淨 `GraphQLError`（＝張票方案 A）| 4369 | 2 | **18**（每個 document **9**）|
| bare | mutant：`REFUSAL_LOG_LEVEL = ERROR` | 541 | 0 | 0 |
| bare | mutant：`REFUSAL_LOG_LEVEL = WARNING` | **541** | 0 | 0 |
| bare | mutant：`process_errors` 唔分類 | 541 | 0 | 0 |
| handler（root 掛 StreamHandler，唔設 level）| HEAD | **0** | 0 | 0 |
| debug（operator 主動調低）| HEAD | **593** | **0** | 0 |

三項獨立確認：
1. **張票寫嘅修復方法確實係錯嘅。** 單純換成 `GraphQLError` 之後 traceback 一個都冇少 —— 實測 9 frames/document，同票上量度數字**完全吻合**。真正生效嘅係 `__init__` 由 `Info` 填 `nodes` + `path`。
2. **client 側逐字不變**：`message` 同 `errors[0]` keys（`locations` / `message` / `path`）喺 HEAD 同 mutant 之間一模一樣。
3. **唔係喺源頭掉咗**：`cfg=debug` 之下 HEAD 仍然出 593 bytes、0 traceback —— 即「預設靜咗，operator 調低就全部見到」呢個 claim 成立。

我另外殺埋 `nodes` mutant：去 `nodes=` 之後 client `errors[0]` keys 變成 `["message","path"]`，**`locations` 消失**。docstring 講「`nodes` 唔可以慳」係準確嘅。

### A-2 INFO vs WARNING —— **Lane 個決定性推理成立，我獨立重現**

Lane 話揀 INFO 而唔係 WARNING 嘅決定性理由係「`logging` 預設 threshold 係 WARNING，所以乜都冇設定嘅 deployment 會整個掉咗呢啲 record」。

**裁決：成立，而且係本批最紮實嘅一個判斷。** 上表 `cfg=bare` / `cfg=handler` 兩行同時顯示：
- `REFUSAL_LOG_LEVEL = INFO`（HEAD）→ **0 bytes**
- `REFUSAL_LOG_LEVEL = WARNING`（mutant）→ **541 bytes**

即係話揀 WARNING 嘅話，Vercel 預設之下**每個被拒 request 仍然寫一行**，影響 2（log 量放大，998 token / request、免認證）只會由「10-frame traceback」縮到「一行」，**唔會歸零**。揀 INFO 先至真係歸零。呢個唔係口味問題，係量得出嚟嘅。

### A-3 `process_errors` 冇吞走任何嘢 —— **核實通過**

我用真 Flask app 逐類 error 打一次，喺 `strawberry.execution` 掛 capture handler（DEBUG）：

| 情境 | client errors | log level | `exc_info` |
|---|---|---|---|
| validation（`{ nosuchfield }`）| ✅ 有 | **ERROR** | None（本來就冇）|
| syntax error | ✅ 有 | **ERROR** | None |
| `Int` coercion（`points: 2.5`）| ✅ 有 | **ERROR** | None |
| depth limiter（2 條 error）| ✅ 有 | **ERROR** ×2 | None |
| **resolver 爆錯**（monkeypatch `service.meta`）| ✅ 有 | **ERROR** | **`RuntimeError`** ✅ |
| 成功 query（control）| — | 冇任何 record | — |

真 error **全部保留 ERROR + traceback**，`process_errors` 冇食走任何唔屬於 budget 拒絕嘅 error。
`mutant：process_errors 唔分類`（全部交返 `super()`）之下 refusal 回到 ERROR、stderr 541 bytes —— classifier 係有作用嘅，唔係 dead code。

### A-4 **刻意唔加 `original_error is None` guard —— 我同意 Lane 嘅判斷**

我喺 `process_errors` 入面裝 spy，睇真實 refusal 到底帶咩：

```
{"type": "BudgetExceededError", "original_error": "None", "path": ["e"],              "has_nodes": true}
{"type": "BudgetExceededError", "original_error": "None", "path": ["a", 10, "track"], "has_nodes": true}
```

第二行係**由 list item 入面（`path` 有 index 10）掟出嚟**嘅 refusal —— 即係連 nested resolver 呢個最似會被 `located_error` 包多層嘅情況，`original_error` 都係 `None`。原因結構性：constructor 一定填 `path`，而 `located_error` 對「已帶 path」嘅 error 原樣放行。

**裁決：唔加 guard 係啱嘅。** 加咗會係一行永遠行唔到嘅 code，冇 mutant 殺得到，反而令讀者以為存在一條我搵唔到嘅路徑。Lane 嘅理由（「今日恆真，冇 mutant 殺得到」）我獨立驗證屬實。

### A-5 **兩個 test infrastructure 修正 —— 兩個都核實成立**

1. **`caplog.at_level` 由 ERROR 改 DEBUG**：必要。降 level 之後 record 係 INFO，留喺 `at_level(ERROR)` 就根本捕捉唔到，`assert records` 會紅。而且測試而家係**讀出** level（`record.levelno == REFUSAL_LOG_LEVEL`）再加 `REFUSAL_LOG_LEVEL < logging.ERROR` 封頂 —— 冇第二句嘅話，`REFUSAL_LOG_LEVEL = ERROR` 呢個 mutant 會自己滿足第一句。呢個 bounding assertion 係必要嘅，我實測 mutant 確實靠佢死。
2. **`_unbounded_schema()` 用 `type(schema)(...)` 而唔係 `strawberry.Schema(...)`**：必要，而且係本批最容易被人 review 漏嘅一項。原本嗰條 unseeded 測試起緊 plain `strawberry.Schema`，**根本冇行過 `RefusalAwareSchema`** —— 綠嘅係 Strawberry 自己個 default。用 `type(schema)` 而唔係 import class name 去釘「同 production 同一個 class」亦係啱嘅取捨。

### A-6 ⚠️ `info._raw_info.field_nodes` 私有 API 耦合 —— **風險可接受，tripwire 夠用（但有一點要收緊）**

核實事實：
- Strawberry **0.327.7**，`pyproject.toml` pin 係 `strawberry-graphql>=0.250`（**冇上界**）
- `Info` 嘅 public 屬性：`context / field_name / get_argument_definition / input_extensions / operation / path / python_name / query / return_type / root_value / schema / selected_fields / variable_values` —— **確實冇 `field_nodes`**。docstring 講「no public accessor」屬實，`_raw_info` 係唯一路徑。

Tripwire mutation：我把 `_raw_info` 改名成 `_renamed_in_a_future_release`（模擬 Strawberry rename），結果：

```
message: "'Info' object has no attribute '_renamed_in_a_future_release'"
still_says_budget_exhausted: false   ← 測試喺呢度紅
has_locations: true                  ← 仍然 true
has_path:      true                  ← 仍然 true
```

**裁決：tripwire 有效（測試會紅），但佢紅嘅原因唔係 docstring 暗示嗰個。** 真正捉到 rename 嘅係 `assert "budget exhausted" in error["message"]`；`locations` / `path` 兩句喺 rename 之下**仍然 pass**（graphql-core 為 `AttributeError` 自己重建咗兩者）。而條測試名叫 `..._still_tells_the_client_where_it_happened`，讀落似係靠 location assertion 把關。→ **S-075**。

整體風險可接受：CI 每次 push 去 develop / master 都會跑 Python suite，而依賴無上界即係新版 Strawberry 一入嚟就會即刻紅 —— 呢個正正係想要嘅行為。

---

## Section B — CUI-0033(a)（方案 C）

### B-1 常數搬遷 —— 全部核實

- `MAX_TRACK_POINTS` 由 `schema.py` 搬去 `service.py`（值 **1000 不變**），`schema.py` 改成 `MAX_TRACK_POINTS = service.MAX_TRACK_POINTS`
- `from run365days.api.schema import MAX_TRACK_POINTS` **仍然 work**，而且 `via_schema is service.MAX_TRACK_POINTS` → **True**（同一 object，唔係 copy）
- 分層鐵律守住：`service.py` 冇 import Strawberry（AST diff 確認新增 import 只有常數本身）

### B-2 ⭐ CUI-0034 gate 喺搬遷之後**仲 work** —— 雙向獨立重做

按你嘅陷阱提示，兩個方向都**先清 `__pycache__`**（`-B` 冇用），而且 1000→999 係同 byte 長度。

**方向 1（TS → 紅）**：改 `source.ts` 常數做 999 →
```
Tests  5 failed | 136 passed (141)
  × refuses points: 0 ...                              （字面 1000）
  × refuses points above the maximum api mode allows    （字面 1000）
  × refuses a non-integer points ...                    （字面 1000）
  × refuses an illegal points before the fetch ...       （字面 1000，CUI-0033(c) 新加）
  × agrees with the ceiling the generated SDL publishes  ← 新 gate
```
（commit message 講「four red」，而家係 5 —— CUI-0034 個 branch 早過 CUI-0033(c) 開，量度當時只有 4 條。唔算 finding。）

**方向 2（Python → 紅，即係搬遷後最需要驗嘅一條）**：改 `service.py` 常數做 999 →
- `run365-schema --check` → **exit 1**（`frontend/schema.graphql is out of date`）✅ 搬遷冇整穿
- 跟住**照 regenerate SDL**（即開發者清紅嘅捷徑路徑）→ `--check` 變綠、SDL 寫住 `(1-999)`
- 呢個正正係舊 run 完結、全綠收工嘅位 —— 而家 `npm test` → **`× agrees with the ceiling the generated SDL publishes` / `expected 999 to be 1000`** ✅

**額外發現（對 gate 有利）**：方向 2 之下 Python suite 亦有 **2 條紅**（`test_a_cheap_track_is_refused_once_the_points_budget_is_spent`、`test_batched_tracks_match_the_reference_downsampler[1000]`）。CUI-0033(a) 個 cap 順帶令 Python 側都夾住咗呢個常數。即係話 Python → SDL → TS 呢條鏈而家**兩重把關**，比 CUI-0034 commit message 當時講嘅更穩。

全部 mutation **已還原**：`git status --porcelain` 同 `git diff --stat` 皆空，三個檔案 md5 同 mutation 前 backup 完全一致（`1ce783bf…` / `6d1e7226…` / `1ee1328e…`）。

### B-3 ⭐ api mode：read-every-row branch 消失 —— **逐點一致，134,041 行零差異**

我按 diff 逐字重建被刪嗰條 `else` branch（`select(TrackPoint).where(activity_id.in_(wanted))` + `order_by(activity_id, seq)`），對**真實 export 每一條 track** 同 `service.tracks()` 逐 row / 逐欄比對：

```json
{"activities": 365, "activities_compared": 365, "rows_compared": 134041,
 "max_stored_rows": 600, "min_stored_rows": 250,
 "over_MAX_TRACK_POINTS": [], "mismatches": [], "IDENTICAL": true}
```

**今日零行為改變 —— 獨立核實成立。** 最長 stored track = 600 行（＝ `DEFAULT_POINT_LIMIT`），無一條超過 1000，全部原封不動回晒。機制上亦成立：`_sample_filter` 對 `total <= points` 嘅 track 行 `activity_id.in_(whole)`，同舊 branch 嘅 predicate 同 ordering 完全等價。

**omitted path 多咗一個 grouped COUNT 嘅代價（實測，60 條 track 一批）**：

| | statements/call | seconds/call |
|---|---|---|
| 舊 plain scan | 1 | 0.2401 |
| 新 capped | **2** | 0.2849（**1.19×**）|

代價真實但輕微，而且**GraphQL 根本去唔到呢條路**：SDL 係 `track(points: Int! = 150)`（NonNull + default），`src/` 入面唯一 caller `schema.py:759` 永遠傳一個經 `_track_points`（拒絕 0）charge 過嘅 `points`。所以今日只有直接叫 `service.tracks()` 嘅 Python caller（即測試）付呢個錢。註釋講「one grouped COUNT more expensively」準確。

### B-4 static mode 同殘留描述

- `source.ts:147` `const limit = points === undefined ? MAX_TRACK_POINTS : checkPoints(points);` 之後**無條件** `downsample(rows, limit)` ✅
- **殘留描述準確。** 我核實：`frontend/src/data/api/source.ts:57` `async track(id, points = DEFAULT_TRACK_POINTS)`，而 `DEFAULT_TRACK_POINTS = 600`（TS 側常數，`source.ts:20`）。GraphQL 本身確實冇 omitted state（`points: Int! = 150` 有 default），client 見到嘅係 600。所以「`--points 1250` 之下 1000（static）vs 600（api）」**逐字成立**，而且票上點名個成因（「經 GraphQL 根本冇 omitted 呢個狀態」）亦準確。呢個係用戶已知悉、明確唔關閉嘅決定，我唔 challenge。

---

## Section C — CUI-0030

- **`readableError()` byte-identical 冇改** ✅：`git diff a494513..HEAD -- frontend/src/lib/errors.ts` **完全空**
- **`grep -rn "error\.message" frontend/src --include=*.tsx | grep -v test` → 零命中** ✅（全 repo 唯一 `error.message` 喺 `lib/errors.ts:27`，即 `readableError` 自己嘅 fallback）
- **覆蓋完整性（我行多一步）**：掃晒全部 `isError` branch，總共 **6 條會插值 error**，其中 `ActivityView.tsx` 兩條**本批之前已經**用緊 `readableError`；另外四條（Overview ×2、Year ×1、Activities ×1）就係本批關閉嘅。餘下四個 view（Load / Performance / Weight / Weather）用固定句子、零插值、無洩漏面。→ **本批關閉咗最後四個缺口，冇遺漏**

### 抽驗 Lane 兩個更正 —— **兩個都成立**

我起真 Flask app（port 5099）＋真 `graphql-request` client，打一個真正觸發 list row budget 嘅 document：

```json
{"raw_length": 911, "leaks_query_document": true, "leaks_variables": true,
 "leaks_variable_values": true,
 "readableError_output": "list row budget exhausted: one request may read at most 4000 rows",
 "readableError_length": 65, "readable_leaks_anything": false}
```

- **`leaks variables = true` —— 確認，票上寫 false 係錯嘅。** 唔單止 `"variables"` 個 key，連**變數值本身**（`2021-12-31`、`hasGps`）都出現喺 `message` 入面
- **blob 長度**：我量到 911（我個 document 嘅 selection set 縮短咗）；Lane 量到 1247 用嘅係完整 `ActivitiesQuery`。**票上 601 明顯低估**，Lane 修正方向正確。`readableError` 出 **65 字元**，同 Lane 數字**完全吻合**
- `ActivitiesQuery` 真係帶 **四個** 變數（`$fromDate $toDate $minKm $hasGps`，`queries.ts:114`）✅ fixture 註釋嗰句屬實

### ⭐ `UI_VISUAL_CONFIRMATION_REQUIRED: true` —— **裁決：實質已滿足，可以豁免**

四點理由，全部我自己核實：

1. **零 className / layout / 結構改動。** 三個 `.tsx` 嘅 diff 逐行就係：一行 `import { readableError } from "@/lib/errors";` ＋ 一至兩個 call-site swap。`<p className="text-danger">`、前綴句子（"Could not load activities: "）全部逐字不變
2. **改動方向係單向減少渲染文字**（~1247 → 65 字元），冇引入任何新 layout 要人肉睇
3. **四個 error state 全部喺 jsdom 真渲染**，而且 assertion 係**兩面**嘅：正面 `toHaveTextContent(BUDGET_MESSAGE)`，反面 `not.toContain('{"response"')` / 文件片段 / `'"variables"'` —— 呢個反面 assertion 先係真正釘住「blob 唔上頁」嘅嘢
4. **Design Origin 合法**：`bbe87b6` commit message 第二行 `Design Origin: baseline: ActivityView's existing readableError() handling`，係五種合法 origin 之一，而且實際 delta（import + call-site）完全喺 baseline 描述嘅範圍內，無意外 layout 改動

截圖喺呢度加唔到任何 text assertion 未 hold 住嘅資訊。**Gate 判定：滿足。**

---

## Section D — CUI-0031（docs-only）

**做法裁決：認同。** 「局部主語（`tests/` / `frontend/src/` 係 inventory）＋ 指向目錄做 SSoT ＋ 只列 areas 唔列 files」係啱嘅收斂方向 —— 佢令呢頁**結構上冇嘢會 fall out of sync**，而唔係靠人記得更新。重用 CUI-0024 為 `pages.yml` 立嘅同一句寫法（`docs/architecture.md:149` 有原句）亦係好事：同一個 repo 用同一個 idiom 處理同一類問題。

**逐句核實佢寫嘅每一樣嘢都對應真實存在嘅測試：**

| 新寫嘅 claim | 對應測試 | ✓ |
|---|---|---|
| TCX / GPX / KML parsers | `tests/test_activities_parsers.py` | ✅ |
| HKO daily / hourly / warning collectors | `tests/test_weather_collectors.py` | ✅ |
| 三者共享嘅 raw-row contract | `tests/test_weather_models.py` + `test_export_records.py` | ✅ |
| year statistics | `tests/test_dashboard_stats.py` | ✅ |
| CLI serialiser | `tests/test_cli_process_activities.py::test_numpy_scalars_are_serialised` | ✅ |
| GraphQL API / Flask test client / 臨時 DB | `tests/test_api.py` | ✅ |
| track downsampling | `frontend/src/lib/downsample.test.ts` | ✅ |
| GraphQL error mapping | `frontend/src/lib/errors.test.ts` | ✅ |
| activity series builder + activity view 本身 | `views/activity/series.test.ts` + `ActivityView.test.tsx` | ✅ |
| all **nine** views | `frontend/src/views/` 實數 **9** 個 view 目錄 | ✅ |

**「undated-last-line quirk」**：確認佢住喺 `tests/test_dashboard_builder.py`，**唔係** `test_weight_analysis.py`（全 repo 只有前者命中 `undated`）。

**但呢個唔構成 finding** —— 而且正正係呢次改寫嘅設計初衷：新句子講嘅係「`tests/` 呢個目錄入面**被行使到嘅 area**」，從頭到尾冇 claim 任何 area 住喺邊個檔案。舊版本亦一樣。Lane 喺自己嘅筆記入面主動指出呢一點（quirk 實際住喺 builder 測試度），係誠實嘅自我披露，唔係一個要扣分嘅不準確。

Scope 核實：`ccf7609` 只掂 `README.md` / `docs/architecture.md` / ticket 三個檔 → **docs-only 屬實**。

---

## Section E — CUI-0034

已喺 B-2 完整記錄（雙向重做、兩個方向都清 `__pycache__`、1000→999 同 byte 長度）。額外核實 `39298b2` 只掂 `source.test.ts` + ticket → **test-only 屬實**，零 production 改動。

`?raw` import 而唔係 `fs`、以及先 assert `(1-N)` match 非 null 再比對 —— 兩個做法都係啱嘅，我確認若 `(1-N)` 措辭消失，測試會出具名 failure 而唔係靜靜比對空值。

---

## 橫向核實

### 測試數

| | 前 | 後 | Δ | 核實 |
|---|---|---|---|---|
| Python | 402 | **417** | +15 | ✅ 逐條點算：0033(a) 3、`Int` scalar param 2、unknown-id 2、refusal-not-fault param 2、unseeded param 3、resolver-fault 1、refusal-locations param 2 = **15** |
| Frontend | 131 | **141** | +10 | ✅ ActivitiesView 2、OverviewView 3、YearView 2、source.test.ts 3 = **10** |

**冇任何測試被刪**：`git diff a494513..HEAD -- tests/ | grep "^-.*def test_"` 零命中；frontend `grep "^-\s*\(it\|test\)("` 零命中。

### Production 行為改動（AST 核實）

我對 `schema.py` / `service.py` 做咗**剝 docstring + 剝註釋之後嘅 AST diff**（`ast.unparse` 後 unified diff）。結果係**剛好且只有**票面描述嗰啲，零隱藏改動：

- `schema.py`（35 行）：3 個 import；`MAX_TRACK_POINTS` 改成 re-export；`BudgetExceededError` class；三處 `ValueError` → `BudgetExceededError(info, …)`（**三句 message 逐字不變**）；`REFUSAL_LOG_LEVEL` / `EXECUTION_LOGGER` / `RefusalAwareSchema`；`build_schema` 改用新 class
- `service.py`（18 行）：加常數；`tracks()` 兩條 branch 收窄成一條（`sample = min(points, MAX_TRACK_POINTS) if points else MAX_TRACK_POINTS`）

其餘三張票（0030 UI 文字、0031 docs、0034 test）逐 commit file scope 已核實，**零 production 行為改動**。

### 用戶已拍板嘅決定 —— **全部無被動過**

| 決定 | 核實 |
|---|---|
| `MAX_LIST_ROWS_PER_REQUEST = 4000` | ✅ `schema.py:289`，delta 內無改動 |
| `MAX_PAGE_SIZE = 1000` / `MAX_TRACK_FIELDS_PER_REQUEST = 64` / `MAX_TRACK_POINTS_PER_REQUEST = 10000` / `MAX_QUERY_DEPTH = 5` / `MAX_QUERY_TOKENS = 1000` / `DEFAULT_TRACK_POINTS = 150` | ✅ 全部原值 |
| CUI-0027 / CUI-0025 兩個 breaking change | ✅ 無回退（budget 拒絕語意、`points: 0` 拒絕語意皆不變，message 逐字不變）|
| alias 軸記錄但不處理 | ✅ 無新增 alias 處理 |
| CUI-0033(a) 方案 C | ✅ 實作就係方案 C，唔係 B |
| CUI-0029 log level 順手做 | ✅ 範圍就係 log level，冇順手改第二樣 |

`MAX_TRACK_POINTS` 只係**搬屋**（值 1000 不變、同一 object），唔算改動用戶決定。

### Secret / 絕對路徑 / model identifier

掃描 delta 全部 `+` 行（`sk-*` / `api_key` / `secret` / `token=` / `password` / `/home/user` / `/Users/` / `claude-*` / `gpt-4` / `opus` / `sonnet`）：
唯一命中係 **`.tickets/in-progress/0001-0200/CUI-0029.md` 入面引用嘅 pre-fix traceback**（4 行 `/home/user/…`）—— 嗰啲正正係本票要消滅嘅證據本身，喺 ticket 記錄入面留底係恰當嘅，**唔喺任何 production code / 測試 / docs**。無 secret、無 model identifier。✅

### 工作區狀態

最終確認（mutation 全部還原後重跑）：`git status --porcelain` 空、`git diff --stat` 空、417 py passed、ruff 全清、SDL up to date、eslint 清、141 fe passed。**我冇執行任何 git merge / push / commit，冇改任何 production 代碼。**

---

## 🟢 Suggestions（S-073 … S-079）

> ID 起點核實：掃 `.proj-docs/` 實際最高係 **C-002 / W-029 / S-072**（S-072 係上輪嗰個唔計分嘅 reviewer artefact，`2026-09-16_review_CUI-0025_delta.md:491`），所以由 **S-073** 起，無撞號。

### ⚠️ 關於 S-073 / S-074 嘅嚴重度取捨（透明交代）

兩條都係**被當成實測結果寫出嚟、但實際唔成立**嘅註釋。我有認真考慮過 evaluate 做 🟡 Warning —— 本 repo 有 **W-029「withdraw a false measurement from CUI-0028's record」** 呢個先例。

**我最終定為 🟢 Suggestion，理由：** W-029 針對嘅係**ticket record**（其他 agent 當事實讀嘅交付物）；呢兩條係**測試檔內嘅註釋**，而 rubric 嘅 Warning 清單冇「註釋準確性」呢一項，Suggestion 清單就明確涵蓋註釋。兩條都**唔令任何測試失效**、唔可能造成 production bug。
**若 main agent 按 W-029 先例判定應為 Warning，score 會由 86 跌到 76 —— 仍然係 warn 區間，next_action 不變。** 資料齊備，覆核權交返俾你。

---

### S-073 ｜ CUI-0033 ｜ `tests/test_api.py` `OVER_CAP_LENGTH` docstring 引用咗一個唔成立嘅量度

**位置**：`/home/user/Python-Project-Run365Days/tests/test_api.py:173-175`

```
``1250`` 已經喺 :data:`VARIED_TRACK_LENGTHS` -- put there for the sampler, not for this --
and is what a real ``run365-export --points 1200`` produces
```

**描述**：`--points 1200` 產生唔到 1250 行。`src/export/records.py:230` 係 `records.tracks[...] = downsample(track_rows(act, temps), point_limit)`，即 `--points N` 硬封頂 N 行。我查真 DB：**全 365 條 activity 只有一條**（`7264441638`）raw `num_points` 超過 1200，而佢**啱啱好係 1250**。所以 `--points 1200` 會將佢降到 **1200**，唔係 1250；要 1250 行必須 `--points >= 1250`。

**佐證**：張票自己 §(a) 寫得啱 ——「`--points 1250` 就會寫 1250 行」；QA 原始量度亦係 `--points 1200` → **1200 行**（票上表格：activity `7264441638` 1200 行）。即係 docstring 同佢自己張票**互相矛盾**。

**影響**：零功能影響（1200 一樣超過 1000 個 cap，測試意圖依然達到）。純粹係一句以「實測」姿態出現嘅假 statement。

**連帶**：`frontend/src/data/static/source.test.ts:26` 講 `run365-export --points 1250` 係「the measurement that opened the ticket」—— `--points 1250` 產生 1250 行呢個 claim **本身係啱嘅**，但佢唔係開票嗰個量度（嗰個係 `--points 1200`）。同源問題，一併修。

**方案 A**：改成 `is what a real ``run365-export --points 1250`` produces`（一個字），同時 `source.test.ts` 嗰句改成 "what `--points 1250` writes; the ticket measured `--points 1200` → 1200 rows"。
**方案 B**：整句拆走對 `--points` 嘅引用，只留「longer than the ceiling」呢個真正重要嘅性質。
**推薦 A** —— `--points` 呢個入口係整張票嘅成因，值得留住，只係要講啱個數。

---

### S-074 ｜ CUI-0030 ｜ Year fixture 嘅「Captured from a real run」provenance 唔成立

**位置**：`/home/user/Python-Project-Run365Days/frontend/src/views/overview/OverviewView.test.tsx:31-35` 同 `frontend/src/views/year/YearView.test.tsx:29-33`

**描述**：兩個 fixture 嘅 docstring 寫「A `graphql-request` ClientError **exactly as it arrives** … **Captured from a real run against the Flask API** — the real blob measured 1247 characters」，但 fixture 砌出嚟嗰個 request 係：

```js
query: "query Year($fromDate: Date, $toDate: Date) { year { year totals { runs } } }",
variables: { fromDate: "2021-01-01", toDate: "2021-12-31" },
```

我核實兩點都唔成立：
1. **真嘅 `YearQuery` 冇變數**：`frontend/src/data/api/queries.ts:46` 係 `query Year {`（零參數）。所以呢個 `$fromDate/$toDate` + `variables` 係砌出嚟嘅，唔係 captured
2. **`year` 根本掟唔出 list row budget refusal**：我打 `{ a: year{year} b: year{year} c: year{year} }` 去真 API → `{"data":{"a":{"year":2021},"b":...,"c":...}}`，**零 error**。`year` 唔 charge list row budget，所以 fixture 模擬嘅 `path: ["year"]` + budget message 呢個組合，API 產生唔到
3. 同一個 **1247** 數字被三個檔案引用，但三個檔案模擬三個**唔同嘅 document** —— 最多只有 `ActivitiesView` 嗰個（真係量過 `ActivitiesQuery`）可以係啱

**影響**：**零** —— 測試本身完全成立，assertion 唔依賴 fixture 係真嘅（佢 assert 嘅係「`readableError` 有冇被接落去」，任何 blob-shaped error 都測得到）。問題純粹係 provenance claim。反而係誤導性嘅：下一個人寫 `year` 相關測試時可能照抄呢個 shape。

**方案 A**：改成誠實描述 —— "Shaped like a `graphql-request` ClientError; the `ActivitiesQuery` blob was measured at 1247 characters against the real API, and this stands in for the same shape on a `year` failure."（保留真實量度，講清邊個 document 量過）
**方案 B**：三個檔案抽一個共用 `test/fixtures` helper，註釋只寫一次，順帶消滅三份 copy。
**推薦 A** 做即時修正，**B** 記入下一個 cleanup lane（三個 fixture 而家有大量重複，係獨立嘅 DRY 議題）。

---

### S-075 ｜ CUI-0029 ｜ `_raw_info` tripwire 指住正確嘅測試，但唔係佢入面真正把關嗰句

**位置**：`/home/user/Python-Project-Run365Days/src/api/schema.py` — `BudgetExceededError.__init__` 註釋

> If this attribute is ever renamed, `test_a_budget_refusal_still_tells_the_client_where_it_happened` is what goes red.

**描述**：條測試確實會紅（我實測 rename 咗 `_raw_info`），但紅嘅係 `assert "budget exhausted" in error["message"]`。`assert error["locations"]` 同 `assert error["path"]` 喺 rename 之下**仍然 pass**（graphql-core 會為結果嗰個 `AttributeError` 重建 locations 同 path）。而條測試個名同佢另外兩句 assertion 都係講 location 嘅，所以將來有人覺得「message 嗰句同其他 budget 測試重複」而剪走佢，呢個被點名嘅 tripwire 就會靜靜咁失效。

**方案 A**：註釋講明係邊句 —— "…is what goes red, on its `message` assertion specifically: `locations` and `path` survive the rename because graphql-core rebuilds them around the resulting `AttributeError`."
**方案 B**：另開一條專測 private-API 耦合嘅測試（如 `assert hasattr(info_type, "_raw_info")` 式 contract test），令 tripwire 唔再寄生喺一條講 client 體驗嘅測試身上。
**推薦 A**（一句話，零新測試）；B 可考慮但會多一條低訊息量嘅測試。

---

### S-076 ｜ CUI-0029 ｜ `docs/deployment.md` 冇講 operator 點樣睇返啲 refusal

**位置**：`/home/user/Python-Project-Run365Days/docs/deployment.md:55`

**描述**：該頁寫「The runtime log (Vercel project, Logs tab) was the only place the Flask error was visible」。本批之後，budget refusal **預設完全唔會出現喺嗰個 log**（正是 CUI-0029 想要嘅效果），但冇任何文件講 operator 想調 budget、或者想捉有人狂 hammer 呢個免認證 endpoint 嗰陣點樣開返。`REFUSAL_LOG_LEVEL` 個 docstring 有講「opts in by lowering the level」，但 docstring 唔係 operator 睇嘅地方。

**方案 A**：`docs/deployment.md` 加一行：「Budget refusals log at INFO to `strawberry.execution`; they are below `logging`'s default threshold, so set the logger to INFO to see them.」
**方案 B**：加一個 env var（如 `RUN365_REFUSAL_LOG_LEVEL`）令佢可以唔改 code 就開。
**推薦 A** —— B 係多一個 config 面，而呢個需求罕見，standard logging config 已經做到。

---

### S-077 ｜ CUI-0029 ｜ `from graphql import GraphQLError` 睇落擺錯 block，其實係被 ruff 逼出嚟

**位置**：`/home/user/Python-Project-Run365Days/src/api/schema.py:25`（喺 first-party block、`run365days` 之上）

**描述**：`graphql-core` 係第三方，但呢個 import 坐咗喺 first-party block。**開發者冇做錯** —— 我實測將佢搬去第三方 block（`strawberry` 隔籬），`ruff check` 即刻報 **I001 Import block is un-sorted or un-formatted**，而且 ruff 嘅 autofix 建議正正就係搬返落去 first-party block。

成因：`pyproject.toml:80` `src = ["src", "tests", "api"]`，而本 repo 有一個 Vercel 強制命名嘅 `api/graphql.py` → ruff 將 `graphql` 當成 first-party module。

值得留底係因為**呢個 repo 曾經為咗呢件事蝕過一個 deploy cycle**（`fd252a3 fix(deploy): rename the Vercel entry to api/index.py to avoid shadowing graphql-core`，之後又要因為 Vercel 只認呢個名而改返 `5aa1b2f`）。而本批係 `run365days` 第一次出現**直接** import 呢個被遮蔽嘅名。

**風險核實：inert，唔係 Critical。** 我模擬 `sys.path.insert(0, "api")` 呢個 shadow 生效嘅情境 → 爆 `ModuleNotFoundError: No module named 'graphql.version'; 'graphql' is not a package`，**而且係喺 `strawberry/utils/__init__.py` 第一行爆**，即係話呢個情境喺本批之前**一樣會炸**（strawberry 本身就 import graphql）。v3.1.1 已部署且正常，證明 Vercel 冇將 `api/` 排喺 site-packages 前面。

**方案 A**：import 上面加一行註釋：「`graphql-core`, not first-party — ruff sorts it here because `src` includes `api/`, where Vercel's mandated `api/graphql.py` shadows the name.」
**方案 B**：`[tool.ruff.lint.isort]` 加 `known-third-party = ["graphql"]`，令分類同現實一致。
**推薦 B**（治本，一行 config，而且令將來任何 `graphql` import 都自動擺啱），**同時做 A** 喺 `CLAUDE.md` §6 已知陷阱表加一行（嗰度已經有兩條關於 `api/graphql.py` 嘅陷阱，但無一條提及佢遮蔽 `graphql-core` 個 import 名）。

---

### S-078 ｜ CUI-0033 ｜ `service.py` 唯一漏覆蓋嗰行係 `tracks()` 嘅空批次早返

**位置**：`/home/user/Python-Project-Run365Days/src/api/service.py:382`（`if not wanted: return found`）

**描述**：`api/service.py` coverage 99%，唯一 miss 就係呢行。本批啱啱重寫咗佢下面成段 `tracks()`，而 `service.tracks(session, [])` 係一行測試。`schema.py` 已經 100%，補呢行即全模組 100%。

**方案 A**：加 `assert service.tracks(session, []) == {}`（一行，可以搭喺現有 batching 測試度）。
**方案 B**：唔理 —— pre-existing、而且 `_charge_track_field` 保證咗 schema 唔會傳空 list 落嚟。
**推薦 A** —— 成本一行，而且個 early return 而家係**唯一**一條唔行 grouped COUNT 嘅路，值得釘住。

---

### S-079 ｜ 發布流程 ｜ `v3.1.1` tag 之後嘅兩個行為改動未入 CHANGELOG

**位置**：`/home/user/Python-Project-Run365Days/docs/CHANGELOG.md`

**描述**：`v3.1.1` **已經 tag 咗**（`git tag` 見 `v3.1.1`），而 `[3.1.1] - 2026-09-16 - develop` 嗰段係喺 `a0ea9d4 chore: bump version to 3.1.1` 寫嘅，開頭講「Nine tickets … **Two behaviour changes**」。本批喺 tag 之後喺 develop 加咗**多兩個 production 行為改動**（CUI-0029 log level、CUI-0033(a) output cap）同一個用戶可見文字改動（CUI-0030），而檔案**冇 `[Unreleased]` section**，雖然佢開宗明義寫住 follows Keep a Changelog。

**唔算本批缺失**：`git log -- docs/CHANGELOG.md` 顯示本 repo 嘅慣例係**bump 嗰陣一次過寫**，唔係逐票寫；之前九張票亦係咁。所以本批五張票唔掂 CHANGELOG 係跟足慣例。

**記低係因為**：CUI-0029 個 log level 改動係 operator 需要知道嘅嘢（見 S-076），下次 bump 唔好靠 `git log` 重建。

**方案 A**：下次 bump 前開 `## [Unreleased]`，將呢五張票（尤其兩個行為改動）寫入。
**方案 B**：而家即刻開 `[Unreleased]` section 順手寫，之後 bump 改個 heading 就算。
**推薦 A**，交返 main agent 喺 release step 處理，唔阻本批。

---

## ✅ 做得好嘅地方（跨 fix）

1. **⭐ CUI-0029 用實測推翻咗自己張票嘅建議方案，而且推翻得啱。** 我獨立重做確認：方案 A 原樣（換 `GraphQLError`）**一個 frame 都冇少**，9 frames 照出。能夠喺「票上寫住推薦方案 A」之下真係去度、度到相反結果、再去搵真正機制（`located_error` 對已帶 `path` 嘅 error 原樣放行），係本批最高價值嗰部分。
2. **⭐ INFO vs WARNING 嘅理由係量出嚟而唔係講出嚟。** 我兩個 mutant 並排跑（`cfg=bare`：INFO → 0 bytes、WARNING → 541 bytes）完全重現佢個論證。呢種「揀 A 唔揀 B 因為 B 喺實際部署條件下做唔到承諾嘅嘢」嘅決策質素，比大部分 code review 見到嘅高一皮。
3. **⭐ 自己揾返兩個 test infrastructure 問題。** 尤其 `_unbounded_schema()` 嗰個 —— 原本條 unseeded 測試起緊 plain `strawberry.Schema`，**根本冇行過本 fix 嘅 class**，係一條「綠住但量緊 Strawberry 自己 default」嘅假綠測試。呢種 bug 極難喺 review 捉到，靠寫嗰個人自己警覺。用 `type(schema)` 而唔係 import class name 去釘「同 production 同一個 class」亦係啱嘅取捨。
4. **⭐ 所有新測試都同「明確問 `MAX_TRACK_POINTS`」嘅結果逐點比對，唔淨係 assert 長度。** `rows[:1000]` / `rows.slice(0, 1000)` 一樣係 1000 行、一樣過長度 assertion，但回嘅係 track 頭段而唔係平均取樣。用 `sec == 1249`（truncation 會係 999）做 oracle 係乾淨利落嘅 disambiguator。
5. **`REFUSAL_LOG_LEVEL < logging.ERROR` 呢句 bounding assertion。** 冇佢嘅話 `REFUSAL_LOG_LEVEL = ERROR` 呢個 mutant 會自己滿足 `record.levelno == REFUSAL_LOG_LEVEL`，成條測試變「level 讀得出」而唔係「level 係對嘅」。同樣道理見於 `not.toContain('{"response"')` 同 `assert "points must be between" not in message` —— **反面 assertion 貫穿成批**，係本批嘅 signature。
6. **`expect(fetcher).not.toHaveBeenCalled()`**：帶住「非法參數唔使一個 round trip」呢個真實性質嘅，係呢句，唔係個 throw。
7. **搬常數而唔係抄常數。** `MAX_TRACK_POINTS` 由 `schema.py` 搬去 `service.py` 再 re-export，正面對抗分層鐵律同「第四份 copy」（S-062/S-065）兩個約束，而且**搬完即刻實測 CUI-0034 個 gate 仲 work** —— 我獨立雙向重做，兩個方向都紅。
8. **CUI-0034 個 SDL gate 係「link」唔係「第四份 copy」**：由 `Activity.track` 嘅 description 讀返個數出嚟，而且先 assert `(1-N)` match 非 null 先比對，措辭一變就出具名 failure 而唔係靜靜比對空值。
9. **`EXECUTION_LOGGER = StrawberryLogger.logger` 攞 attribute 而唔係抄字串** `"strawberry.execution"` —— Strawberry 一搬就 import 時大聲爆，抄字串就會靜靜咁令 refusal 流去另一個 stream。細節但啱。
10. **CUI-0031 揀咗結構性解法。** 「指向目錄做 SSoT」令呢頁**冇嘢會 fall out of sync**，而唔係靠人記得更新；重用 CUI-0024 為 `pages.yml` 立嘅同一句寫法令同一 repo 用同一 idiom 處理同一類問題。
11. **CUI-0030 `readableError()` 一個 byte 都冇改。** 見到四個 view 有問題而唔去「順手改埋個 helper」，係啱嘅 scope 紀律 —— `ActivityView` 已經依賴緊佢。
12. **Ticket 記錄咗兩件冇人問嘅嘢**：CUI-0033 主動記低 (b)(c) 會互相抵銷（未知 id 只喺 `Int!` 載得起嗰啲值先食到非法 points），並開測試釘住；CUI-0029 主動記低 `process_errors` 見到嘅係成個 operation 所有 error，所以「認一種、順手掃埋其餘」係 budget 測試睇唔到嘅一行錯 —— 再開 `test_an_unexpected_resolver_error_…` 專門守住。呢種「揾到一個票冇問嘅洞就記低再釘住」嘅習慣，係本批質素高嘅根本原因。

---

## 修正優先順序

| # | ID | 內容 | 阻 merge？ | 成本 | 建議 lane |
|---|---|---|---|---|---|
| 1 | **S-073** | `--points 1200` → `1250`，改個數（+ `source.test.ts` 同源一句）| ❌ | 一個字 | docs cleanup |
| 2 | **S-074** | Year fixture provenance 改成誠實描述 | ❌ | 2 句 ×2 檔 | docs cleanup |
| 3 | **S-077** | `known-third-party = ["graphql"]` + `CLAUDE.md` §6 加一行 | ❌ | 2 行 | docs cleanup |
| 4 | **S-075** | tripwire 註釋點名 `message` assertion | ❌ | 1 句 | docs cleanup |
| 5 | **S-076** | `deployment.md` 加 refusal log level 一行 | ❌ | 1 行 | docs cleanup |
| 6 | **S-078** | `tracks(session, [])` 一行測試 | ❌ | 1 行 | 同上 |
| 7 | **S-079** | `[Unreleased]` CHANGELOG section | ❌ | release step | main agent |
| — | 舊 open | S-063 / S-064 / S-065 / S-068 / S-069 / S-070 / S-071 | ❌ | 全部係一兩行 docstring | **建議同一個 lane 一次清 12 條** |

**修正規範**：每個 review item 一個獨立 commit，`fix: S-0NN | <一句描述>`，唔可合併。

**修訂後完整代碼**：本批**零 🔴 / 零 🟡**，七條 Suggestion 全部係一至兩行嘅註釋 / config / 單行測試，逐條方案已喺上面寫明確切位置同替換文字，唔需要另附完整檔案。

---

## 給 main agent 嘅判斷建議

`status=warn` 純粹由**累積嘅 documentation suggestion 債**驅動（7 條舊 open 未清 + 7 條新開 = 12 條），**唔係**由本批代碼質素驅動。本批本身：0 Critical、0 Warning、6/6 hard gates pass、5 張票逐張 pass、production 行為改動 AST 證實剛好等於票面描述、134,041 行 track 逐點零差異。

12 條全部係一至兩行嘅 docstring / 註釋 / config 修改。建議開**一個 documentation-only lane 一次過清晒**，之後 re-review 應該直上 ~99。清完之前，功能上**冇任何嘢阻住 merge 或者行 QA**。

```handoff-receipt
protocol: 1
status: warn
score: 86/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "95%"
next_action: invoke_developer
next_agent: backend-developer
branch: "claude/ai-dev-team-start-05jie2"
context: "Batch review of CUI-0029/0033/0034/0030/0031 (a494513..HEAD): 6/6 hard gates pass (417 py + 141 fe tests, ruff clean, SDL in sync, coverage 95% with api/schema.py 100% and api/service.py 99%, npm audit run for the first time -- 14 dev-only vulns, 0 in prod, 0 new deps). 0 Critical, 0 Warning, 7 new Suggestions S-073..S-079, all one-to-two-line documentation/config/test edits. Score 86 = warn, driven entirely by accumulated suggestion debt (7 inherited open S-063/064/065/068/069/070/071 plus 7 new), not by this batch's code. Every lane claim independently redone, none taken on trust: Option A as the ticket wrote it really does NOT work (bare GraphQLError still emits 9 absolute-path frames per refusal, measured on a real Flask app with OS-level stderr capture); the INFO-over-WARNING reasoning is upheld and reproduced (bare deployment: INFO 0 bytes, WARNING 541 bytes, so WARNING would leave one line per refused request instead of zero); process_errors swallows nothing (validation, syntax, Int coercion, depth limiter and a monkeypatched resolver fault all keep ERROR, the fault keeping its RuntimeError exc_info); the omitted original_error guard is correctly omitted (spy shows original_error None even on a nested list-item refusal at path [a,10,track]); both test-infrastructure fixes are necessary, especially _unbounded_schema() -- the old unseeded test built a plain strawberry.Schema and so never exercised the fix at all. CUI-0033(a) proven behaviour-neutral by rebuilding the deleted else-branch and comparing 134,041 track rows across all 365 activities point for point: zero mismatches, max stored 600, none over 1000; omitted path costs 1 extra statement (+19% on a 60-track batch) and is unreachable from GraphQL (SDL is points: Int! = 150). CUI-0034 gate redone in BOTH directions with __pycache__ cleared: TS 1000->999 gives 5 red; Python 999 + regenerating the SDL (the exact escape hatch) leaves the new SDL test red, so the constant's move to service.py did not break the gate -- and CUI-0033(a) now adds a second Python-side tripwire. CUI-0030: readableError() byte-identical, zero error.message hits in tsx, all six interpolating error branches now routed (four closed here, ActivityView's two already were); leaks-variables=true confirmed against a live Flask API (variable VALUES leak too, ticket's false was wrong), blob ~911-1247 chars vs 65 after readableError. UI_VISUAL_CONFIRMATION judged satisfied: zero className/layout/structure change, legal Design Origin 'baseline:' in the commit, text strictly reduced, four error states pinned with both positive and negative assertions. CUI-0031 verified sentence by sentence against real test files (nine view dirs confirmed; the undated quirk does live in test_dashboard_builder.py, which the area-level wording deliberately does not contradict). Test arithmetic checked: 402->417 (+15) and 131->141 (+10), nothing deleted. AST diff with docstrings and comments stripped confirms production change is exactly and only the two tickets' described surface. All user-ratified decisions intact (MAX_LIST_ROWS_PER_REQUEST 4000, CUI-0027/0025 breaking changes, alias axis, option C, log level scope); MAX_TRACK_POINTS only moved, same value, same object. No secrets, no model identifiers; the only absolute paths are the pre-fix traceback quoted as evidence inside CUI-0029.md. Highest existing IDs rescanned: C-002/W-029/S-072, so new IDs start at S-073, no collision. Two findings (S-073 false --points 1200->1250 claim, S-074 false 'captured from a real run' provenance on the Year fixtures) were weighed against the W-029 precedent for Warning and graded Suggestion because they are test-file comments rather than ticket records and break nothing; grading them Warning instead would give 76, still warn, same next_action. IMPORTANT: the review report file was NOT written -- my harness forbids subagents writing report .md files -- the full report is in my handback message and must be persisted verbatim to .proj-docs/reviews/2026-09-16_review_CUI-0029_batch.md, with .proj-docs/index.md updated. No git operations performed, no project code modified; every mutation restored and verified (git status and git diff both empty, md5sums match pre-mutation backups, full gate suite re-run green afterwards). Recommend one documentation-only lane clearing all 12 open suggestions at once; nothing blocks merge or QA on functional grounds."
blockers:
  - "S-073 (CUI-0033) tests/test_api.py:173-175 -- OVER_CAP_LENGTH docstring says 1250 is what `run365-export --points 1200` produces; export downsamples to the limit (src/export/records.py:230) and the single over-cap activity 7264441638 holds exactly 1250 raw samples, so --points 1200 writes 1200. The ticket's own section (a) states this correctly. Same-source wording in frontend/src/data/static/source.test.ts:26 calls --points 1250 'the measurement that opened the ticket' when that measurement was --points 1200."
  - "S-074 (CUI-0030) OverviewView.test.tsx:31-35 and YearView.test.tsx:29-33 -- fixtures claim 'Captured from a real run against the Flask API', but the real YearQuery (frontend/src/data/api/queries.ts:46) takes no variables, and `year` cannot raise a list-row-budget refusal at all (three aliased year fields return data with no error). The same 1247-character figure is quoted for three different documents. Tests are sound; only the provenance claim is false."
  - "S-075 (CUI-0029) src/api/schema.py BudgetExceededError.__init__ -- the named tripwire test does go red on a _raw_info rename, but via its `budget exhausted` message assertion; the locations and path assertions it is named for survive the rename because graphql-core rebuilds them around the resulting AttributeError."
  - "S-076 (CUI-0029) docs/deployment.md:55 -- budget refusals are now below logging's default threshold and invisible in the Vercel log by default; no operator-facing doc says to lower strawberry.execution to INFO to see them."
  - "S-077 (CUI-0029) src/api/schema.py:25 -- `from graphql import GraphQLError` sits in the first-party block; verified this is FORCED (moving it to third-party fails ruff I001) because [tool.ruff] src includes api/ and Vercel mandates api/graphql.py, which shadows graphql-core's name. Inert in production (the shadow would already break strawberry/utils/__init__.py). Suggest known-third-party = [\"graphql\"] plus a CLAUDE.md section 6 trap-table line; the repo already lost a deploy cycle to this shadowing (fd252a3)."
  - "S-078 (CUI-0033) src/api/service.py:382 -- the empty-batch early return is the module's only uncovered statement (99%); tracks(session, []) is a one-line test and would take the module to 100%."
  - "S-079 (release) docs/CHANGELOG.md -- v3.1.1 is already tagged and two production behaviour changes (CUI-0029 log level, CUI-0033(a) output cap) plus one user-visible text change have landed on develop since; no [Unreleased] section exists although the file states it follows Keep a Changelog. Consistent with the project's write-at-bump-time convention, so not a defect in this batch -- flagged for the next bump."
  - "Inherited and still open, counted as open per the brief and scored -1 each: S-063, S-064, S-065, S-068, S-069, S-070, S-071. All one-to-two-line docstring edits; recommend clearing all 12 suggestions in a single documentation-only lane."
```
