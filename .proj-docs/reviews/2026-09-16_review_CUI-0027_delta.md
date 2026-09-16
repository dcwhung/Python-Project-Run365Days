# Delta Re-Review — 2026-09-16 — CUI-0027 / CUI-0022 / CUI-0024

**審閱者**：Code Reviewer（獨立角色，冇參與呢批代碼嘅撰寫）
**Branch**：`claude/ai-dev-team-start-05jie2`
**上一輪**：[`2026-09-16_review_CUI-0027_batch.md`](2026-09-16_review_CUI-0027_batch.md) — 84/100 ⚠️ warn（0 🔴 / 2 🟡 / 6 🟢）
**Delta**：`18368b8..304aff6` — 2 files, +109 / −27（`src/api/schema.py`、`tests/test_api.py`）

| Finding | Commit | 內容 | 我嘅裁決 |
|---|---|---|---|
| **W-027** + S-052 | `19dede8` | parametrize 兩條 fail-closed test 涵蓋 `activities` / `year`；`_track_rows` → `_rows_at(result, path)` | ✅ **closed** — 兩個 mutation 獨立重做，聲稱逐條成立 |
| **W-028** | `d603706` | 補收費**規則** + 補齊五個 field 實測數 | ✅ **closed** — 規則獨立推過 11 個 field，全中；數字抽驗 5 個 |
| S-057 | `d1be47c` | `year` 嘅 500 係 proxy 唔係 ceiling | ✅ **closed**，而且**更正咗上一輪 review 一個事實錯誤**（見 §4） |
| S-054 | `34f10df` | 決定**唔收緊** row gate，改為記錄 slack | ✅ **closed** — 唔做嘅理由獨立驗證成立 |
| S-052 | `19dede8` | `_TrackBudget` 舊名註釋 | ✅ closed |
| S-053 | — | 摺入 Round 2（CUI-0025） | ✅ **判斷成立** — 確認唔改 SDL 做唔到（見 §6） |
| S-055 | — | 被拒 request 留 traceback（pre-existing） | ⏳ 仍然 open，仍然未開票 |
| S-056 | `18368b8`（delta 之前） | ticket 執行紀錄 | ✅ closed（`CUI-0029` 嗰半被用戶「不開票」決定覆蓋） |

---

## 整體 verdict

**Status：✅ pass（95 / 100，0 Critical，0 Warning，3 Suggestion）**

兩個 Warning 都真正修好咗，而且**唔係靠我讀 diff 相信佢**：W-027 嗰兩個 mutation 我獨立重做，
數字（4 fail / 378 pass、51 fail / 331 pass）同 developer 報嘅**逐個對上**；
S-054 嗰個 75% undercharge mutant 我亦重做，連「exact count 只會係第 5 條紅」呢半邊都用
探針量咗出嚟（3,650 rows，`<=` 照綠）。S-057 更有意思 —— **developer 唔單止修咗，仲更正咗
上一輪 review 一個事實錯誤**，我用 identity map 核實佢係啱嘅，我個前任係錯嘅（§4）。

Production 行為**零改動，而且係結構性證明唔係讀出嚟嘅**：`src/api/schema.py` 前後兩版
剝走所有 bare string-literal statement 之後 **AST 完全相同**，module-level 常數零變動。
即係話成個 `src/` delta 係純 docstring，SDL byte-identical。

三個新 Suggestion 全部係**文件精度**，零行為風險，而且**唔改變任何已 close 嘅結論**：
最重要嗰個（S-058）係 `activity(id:)` 嗰段「90 rows / ~0.05 s / some 300x」量嘅其實係
**id 揀唔中嗰條路**；真實 id 之下係 93–101 ms、148–161×、最多 810 行、每 field 兩條 statement。
「唔收費係啱嘅」呢個結論完全企得住（148× 仍然離 15 s 十萬八千里），出事嘅係個數字。

---

## Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| **Lint（Python）** | `.venv/bin/ruff check src tests` | ✅ pass — All checks passed! |
| **Format（Python）** | `.venv/bin/ruff format --check src tests` | ✅ pass — 58 files already formatted |
| **Tests（Python）** | `.venv/bin/python -m pytest tests -q` | ✅ pass — **382 passed** in 47.39 s（重跑 49.21 s 一樣） |
| **SDL 同步** | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ pass — is up to date |
| **Lint（前端）** | `npm run lint` | ✅ pass |
| **Type check（前端）** | `npm run typecheck`（先跑 codegen） | ✅ pass — Generate to src/gql/ SUCCESS |
| **Tests（前端）** | `npm test` | ✅ pass — **126 passed / 22 files** |
| **Coverage（核心邏輯）** | `pytest tests/test_api.py --cov=run365days.api --cov-report=term-missing` | ✅ pass — `src/api/schema.py` **307 stmts / 0 miss / 100%**（99% → 100%，478–481 唔再係 miss）|
| **No Critical** | — | ✅ pass — 0 |
| **Security scan** | delta 零新增依賴，純 docstring + test | ✅ n/a |

> Coverage 核實：`src/api/__init__.py 100%`、`app.py 100%`、`db.py 100%`、**`schema.py 100%`**、
> `service.py 99%`（唯一 miss `service.py:348`，**pre-existing，唔喺本 delta**）。
> 上一輪嗰個唯一 miss（478–481，即 `_charge_list_rows` 嘅 `except KeyError` 分支）已經清零 ✅

---

## 評分結果

| 維度 | 得分 | 滿分 | 相對上一輪 | 備注 |
|------|------|------|---|------|
| 正確性 | 24 | 25 | 24 → 24 | S-057 修好（+1）；新開 S-058（−1） |
| 安全性 | 19 | 20 | 19 → 19 | S-055 仍然 open（pre-existing，仍未開票） |
| 可維護性 | 17 | 20 | 12 → 17 | W-028（+5）、S-052（+1）、S-056（+1）；S-053 仍 open（−1）、新開 S-059 / S-060（−2） |
| 測試覆蓋 | 15 | 15 | 9 → 15 | W-027（+5）、S-054（+1）；coverage 100%，兩個 mutation 證明新 test 有牙 |
| 性能 | 10 | 10 | 10 → 10 | delta 零行為改動，AST 證明 |
| 代碼風格 | 10 | 10 | 10 → 10 | ruff 全綠 |
| **總分** | **95** | **100** | **84 → 95** | |

**結果：✅ pass**（hard_gates 全 pass + ≥ 90 分 + 0 Critical）

### 84 → 95 點解郁咁多

`+11` 入面 **`+10` 係兩個 Warning 各自值嘅分**（W-027 −5 喺得 15 分嘅測試覆蓋度、W-028 −5 喺可維護性），
兩個都真正 close 咗。餘下 `+4` 係三個 Suggestion（S-052 / S-054 / S-056）close，
再 `−3` 係今輪新開嘅三個 Suggestion。冇任何一分係靠「唔再查」攞返嚟 ——
上一輪 close 咗嘅嘢（效能、等價性、budget 覆蓋面）我按指示冇重審，
所以佢哋喺兩輪之間**分數貢獻不變**，郁嘅完全係兩個 Warning 同 Suggestion 嘅增減。

---

# 1. W-027 — 新 test 到底有冇牙

> ⚠️ 本節四個 mutation 全部係我自己重做嘅，做完即刻還原，見文末「驗證用臨時改動聲明」。

## 1.1 Mutation A — fail-open mutant（developer 聲稱嗰個）

將 `_charge_list_rows` 嘅 `try / except KeyError → RuntimeError` 換成靜靜 fallback：

```python
rows_left = info.context.get(LIST_ROWS_KEY, MAX_LIST_ROWS_PER_REQUEST) - rows
```

```
4 failed, 378 passed in 50.25s

FAILED tests/test_api.py::test_a_budget_fails_closed_when_the_schema_has_no_budget_extension[activities]
FAILED tests/test_api.py::test_a_budget_fails_closed_when_the_schema_has_no_budget_extension[year]
FAILED tests/test_api.py::test_a_budget_fails_closed_when_the_context_cannot_be_seeded[activities]
FAILED tests/test_api.py::test_a_budget_fails_closed_when_the_context_cannot_be_seeded[year]
```

✅ **同 developer 報嘅一模一樣**：378 條 pre-existing test 全部照綠，**只有 4 條新 param 捉到**。
呢個就係 W-027 原本講嗰個缺口 —— 冇呢四條，list budget 可以靜靜變返 unbounded 而冇人知。

### Failure 係 AssertionError 定 collection Error —— **四條全部 AssertionError**

```
E  AssertionError: assert None
E   +  where None = ExecutionResult(data={'activities': [{'id': 'r0'}, … {'id': 'r364'}]},
                                    errors=None, extensions={}).errors
   tests/test_api.py:1356: AssertionError

E  assert 'not seeded' in "'mappingproxy' object does not support item assignment"
   tests/test_api.py:1373: AssertionError
```

- 冇 extension 嗰兩條：紅喺 `assert result.errors`，而 `data` 入面**真係坐住 365 行
  unbounded 服務出嚟嘅 activity** —— 即係 mutant 確實 fail-open 咗，test 確實捉到。
- Frozen context 嗰兩條：紅喺 `assert NOT_SEEDED in message`。呢條**先係關鍵** ——
  mutant 之下呢個 case 仍然有 error，只不過係 `'mappingproxy' object does not support
  item assignment`。如果當初圖方便只 assert `result.errors`，呢兩條會**照綠**。
  `NOT_SEEDED` 呢個常數同佢個 docstring（「Naming the message is what tells those two
  apart」）唔係擺設，係呢兩條 param 唯一嘅牙。✅

> 順帶：`tests/test_api.py` 完全冇改過 `src/` 嘅 import 形狀，所以唔會出 CLAUDE.md §6
> 講嗰種「collection Error 扮紅」。四條全部行到 assertion 先紅。

## 1.2 Mutation B — 刪走 seeding line（cross-check）

喺 `_RequestBudgets.on_operation` 刪走 `context[LIST_ROWS_KEY] = MAX_LIST_ROWS_PER_REQUEST`：

```
51 failed, 331 passed in 48.45s

$ pytest …fails_closed… -q
6 passed in 1.48s
```

✅ **又係一模一樣**：51 條其他 test 紅，**6 條 fail-closed param 全部綠**。
呢個 cross-check 嘅意義係證明新 test 量緊嘅係「冇 budget 嗰陣有冇拒絕」，
唔係「budget 有冇 seed」—— 兩個 mutation 方向相反，紅嘅集合零重疊，
即係呢對 test 同其餘 376 條**互補而唔係重複**。

## 1.3 Mutation C — 我自己加嘅：`_rows_at` 重構有冇整鈍咗原本兩條 track test

Developer 冇做呢個，但呢個正正係「重構有冇改變語義」嘅直接答案。
將 `_charge_track_field` 一樣改成 fail-open：

```
FAILED tests/test_api.py::test_a_budget_fails_closed_when_the_schema_has_no_budget_extension[track]
FAILED tests/test_api.py::test_a_budget_fails_closed_when_the_context_cannot_be_seeded[track]
2 failed, 4 passed in 1.54s
```

✅ **track 兩條 param 照樣紅**，而且 `data` 入面坐住 10 個真實服務出嚟嘅 track point
（`{'activity': {'track': [{'sec': 0}, {'sec': 67}, …]}}`）。
**重構冇拎走原本兩條 test 任何一隻牙**，兩個 budget 各自獨立武裝。

## 1.4 `_rows_at(result, path)` 對唔對 —— 兩種 propagation 形狀實測

我用探針打印三個 document 喺兩個 unseeded 場景下嘅實際 response：

```
NOEXT  track       data={'activity': None}  data_is_none=False  rows_at=[]  errpath=['activity','track']
NOEXT  activities  data=None                data_is_none=True   rows_at=[]  errpath=['activities']
NOEXT  year        data=None                data_is_none=True   rows_at=[]  errpath=['year']
FROZEN track       data={'activity': None}  data_is_none=False  rows_at=[]  errpath=['activity','track']
FROZEN activities  data=None                data_is_none=True   rows_at=[]  errpath=['activities']
FROZEN year        data=None                data_is_none=True   rows_at=[]  errpath=['year']
```

**兩種形狀真係唔同，而 commit message 講嘅方向係啱嘅**：被拒嘅 list field（non-null type）
null 走成個 response；被拒嘅 `track` 只 null 佢 parent `activity`，`data` 仍然係
`{'activity': None}`。

### ⚠️ 順帶更正上一輪 review 一個實作錯誤

上一輪報告「修訂後代碼」段落建議三個 case 一律 `assert result.data is None`。
**如果照抄，track 嗰兩條會即刻紅**（`{'activity': None} is None` → False）。
Developer 冇照抄，改為抽 `_rows_at(result, path)`，**呢個先係啱嘅一般化** ——
佢係唯一一個喺兩種 propagation 形狀之下都成立嘅斷言形狀。
呢個唔係 finding，係要記低嘅**上一輪錯、今輪啱**。

### 語義等價性

`_track_rows(r)` = `((r.data or {}).get("activity") or {}).get("track") or []`
`_rows_at(r, ("activity","track"))` 逐步 fold 出同一條式（每步 `or {}`，最後 `or []`）。
對 `data ∈ {None, {"activity": None}, {"activity": {"track": [...]}}}` 三種輸入
結果逐個相同，Mutation C 亦從行為面確認咗。✅

---

# 2. W-028 — 條規則寫得啱唔啱

## 2.1 用條規則自己推 11 個 root field

> **A root field pays if and only if the rows it materialises grow with a window** — one the
> client names (`limit`) or one the data implies (`year`'s calendar year). A field that
> materialises a fixed number of rows however large the export grows does not pay, however
> much SQL it issues to get there.

先確認係咪真係 11 個：`awk '/^type Query/,/^}/' frontend/schema.graphql` 數出 **11 個** field ✅
（`meta`、`activities`、`activitiesCount`、`activity`、`weight`、`weightCount`、
`weather`、`weatherCount`、`warnings`、`warningsCount`、`year`）。

我**唔睇代碼收唔收費**，淨係用條規則推，然後先對返 `grep -n "_charge_list_rows" src/api/schema.py`：

| # | Field | 規則推出嚟 | 理由（規則哪一句） | 實際 | 對唔對 |
|---|---|---|---|---|---|
| 1 | `activities` | **收** | client names `limit` | `_charge_list_rows(info, limit)` :973 | ✅ |
| 2 | `weight` | **收** | client names `limit` | :1006 | ✅ |
| 3 | `weather` | **收** | client names `limit` | :1028 | ✅ |
| 4 | `warnings` | **收** | client names `limit` | :1050 | ✅ |
| 5 | `year` | **收** | data implies calendar year | `_charge_list_rows(info, DEFAULT_PAGE_SIZE)` :1072 | ✅ |
| 6 | `activity(id:)` | 唔收 | fixed rows，唔隨 export 大 | 唔收 | ✅ |
| 7 | `activitiesCount` | 唔收 | 返一行；「however much SQL it issues to get there」明文覆蓋全表 scan | 唔收 | ✅ |
| 8 | `weightCount` | 唔收 | 同上 | 唔收 | ✅ |
| 9 | `weatherCount` | 唔收 | 同上 | 唔收 | ✅ |
| 10 | `warningsCount` | 唔收 | 同上 | 唔收 | ✅ |
| 11 | `meta` | 唔收 | 固定 header，唔隨 export 大 | 唔收 | ✅ |

**11 / 11 一致。冇任何 field 規則話要收但實際冇收，亦冇反過來。**

### 條規則點解真係企得住（唔只係啱數）

我特登試過拗佢：`activitiesCount(fromDate:, toDate:, minKm:, hasGps:)` **client 明明 names 咗
一個 window**（日期範圍）。如果條規則係 keyed 喺「有冇 window argument」，佢就會推錯 ——
推出「要收」而實際唔收。但條規則 keyed 喺 **the rows it materialises**，
而 `activitiesCount` 無論 window 幾闊都 materialise 零 ORM row（實測：`rows={}`，1 條 statement）。
**條規則揀啱咗自變數**，而且第二段 prose 明文點穿咗呢點：

> it is *rows materialised* that this budget bounds

呢個就係 W-028 原本要嘅嘢 —— 下一個加 root field 嘅人唔使靠類比，有條可以套嘅規則。
**呢個修法我認為做得比上一輪建議嘅（方案 A + B）好**：上一輪建議係「規則 + 逐個列」，
developer 交嘅係「規則 + 點解第二子句先係重點 + 逐個列 + 每個有數」。

## 2.2 抽驗 developer 自己量嘅五個數 —— 我獨立重量

Production export（`data/processed/run365.db`：Activity 365 / Meta 2 / WeatherWarning 461 /
Weight 365 / ActivityWarning join rows 382）。Alias 上限用 binary search 搵，唔係抄。
每個 shape 跑 5 次取 min，warm page cache。

| Shape | widest（我 search 出嚟） | Docstring 聲稱 | 我實測 | 餘裕 | 判斷 |
|---|---|---|---|---|---|
| `activitiesCount` | **332** ✅ | ~83 ms / some 180x | **83.9 ms** | **178.8×** | ✅ 準到離譜 |
| `weightCount` | **332** ✅ | （落喺 77–86 ms 區間） | 82.3 ms | 182.3× | ✅ |
| `weatherCount` | **332** ✅ | （同上） | 79.9 ms | 187.7× | ✅ |
| `warningsCount` | **332** ✅ | （同上） | 85.3 ms | 175.9× | ✅ |
| `meta` | **166** ✅ | ~46 ms / some 320x | 47.2 ms | 317.5× | ✅（差 2.6%，噪音） |
| `activity(id:)` | **90** ✅ | 90 rows / ~0.05 s / some 300x | **見 S-058** | — | ⚠️ |

- 「332 aliased counts read **77–86 ms**」：我實測區間 **79.9–85.3 ms**，**完整落喺聲稱區間入面** ✅
- 五個 widest（332 / 332 / 332 / 332 / 166）我自己 binary search 全部重現 ✅
- 唯一一個唔企得住嘅係 `activity(id:)` 嗰組數 → **S-058**

---

# 3. W-028 — cold cache 嘅處理啱唔啱

Commit message 尾段：

> A first, cold-cache pass read activitiesCount at 151 ms / 99x; the figures above are the
> warm steady state, which is what the surrounding prose reports.

**Developer 主動記低咗呢件事，而唔係扮冇發生過 —— 呢點值得講明係做得好。**
而佢揀 warm 亦有一個真理由：呢個 docstring 入面其餘所有數（4000 嘅校準、`activity(id:)`、
alias 軸嘅 824 ms）都係 warm 讀數，**撈一個 cold 數入一張 warm 表會令成張表冇得比**，
比漏咗更誤導。呢個取捨我同意。

**但「記喺 commit message」唔等於記低咗。** 呢個 repo 嘅明文文化係
「docstring 就係契約」（AU-047 兩輪 finding、W-028 本身都係呢件事），
而 commit message 唔係契約 —— 六個月後讀 `MAX_LIST_ROWS_PER_REQUEST` docstring 嗰個人
睇唔到呢句 caveat。

**而且呢個 repo 有一個令 cold 讀數特別 relevant 嘅部署事實**：呢個 API 行喺
**Vercel function 之上、打一個 SQLite 檔**。Serverless invocation 冷啟嗰陣
page cache 就係凍嘅 —— 即係話 **99× 嗰個讀數對 production 嚟講唔係 artefact，
反而可能比 180× 更有代表性**。一個讀者攞 180× 去度新 field 嘅預算，
可能會樂觀咗一倍。

→ 判斷：**處理方向啱（揀 warm 保持可比性），但收尾差一句**。列為 **S-060**，唔係 Warning ——
因為即使 cold，99× 都仍然離 15 s function limit 兩個數量級，
「唔值得收費」呢個結論完全唔郁。

---

# 4. S-057 — developer 更正咗上一輪 review 一個事實錯誤，**佢係啱嘅**

## 上一輪寫咗乜

> 另外 `year` 每次仲讀約 384 行 **weight**，呢批完全冇收費。
> —— `2026-09-16_review_CUI-0027_batch.md` § S-057

## Developer 點反駁

> the ~384 uncharged rows S-057 attributes to weight are not weight: instrumenting the
> identity map shows 382 ActivityWarning rows plus the 2 Meta rows, and an `activities` page
> pulls the same ~382 through the same selectinload.

## 我獨立核實 —— 用 `loaded_as_persistent` event 數實際 materialise 咗嘅 ORM object

```
1x year { year }                  stmts= 3  {'Meta': 2, 'Activity': 365, 'ActivityWarning': 382}
1x year (wide selection)          stmts= 3  {'Meta': 2, 'Activity': 365, 'ActivityWarning': 382}
1x activities(limit: 500)         stmts= 2  {            'Activity': 365, 'ActivityWarning': 382}
1x weight(limit: 500)             stmts= 1  {'WeightEntry': 365}
1x meta                           stmts= 1  {'Meta': 2}
```

**Developer 完全正確，上一輪報告錯**：

1. `{ year { year } }` materialise 嘅係 **`Meta` 2 + `Activity` 365 + `ActivityWarning` 382**。
   **`WeightEntry` 一行都冇。** 382 + 2 = 384 —— 上一輪應該係見到「384 行非 activity」
   就當咗係 weight，冇分類。
2. `activities(limit: 500)` 經**同一個** `selectinload(Activity.warnings)` 一樣拉 **382 行**
   `ActivityWarning`。所以呢 382 行**唔係 `year` 嘅 quirk，係成個 budget 嘅性質** ——
   正如 docstring 而家寫嘅：「so they are not a `year` quirk」。
3. `weight` 係完全獨立一條路（`WeightEntry: 365`，1 條 statement），`year` 掂都冇掂過。

> 📌 **對上一輪報告嘅正式更正**：`2026-09-16_review_CUI-0027_batch.md` § S-057 入面
> 「`year` 每次仲讀約 384 行 weight」**係錯嘅**，正確係 382 行 `ActivityWarning` + 2 行 `Meta`，
> 而且同量級嘅 warning 行 `activities` 一頁一樣會拉。呢句錯**冇影響當時嘅結論**
> （「呢批冇收費」本身仍然啱），但個歸因錯咗。

## Proxy 校準 —— 我重量

```
8x year                 (charged 4000)  stmts=24  {'Meta': 16, 'Activity': 2920, 'ActivityWarning': 3056}  146.1 ms
8x activities(limit:500)(charged 4000)  stmts=16  {            'Activity': 2920, 'ActivityWarning': 3056}  147.3 ms
```

| 聲稱（docstring） | 我實測 | 判斷 |
|---|---|---|
| 兩者都 spend the whole 4000 | ✅ 兩者都啱啱好 8 個 field × 500 | ✅ |
| 兩者都 materialise **2,920** activity rows | **2,920 / 2,920** | ✅ 一模一樣 |
| 145 ms against 151 ms，「within some 4%」 | **146.1 vs 147.3 ms = 差 0.8%** | ✅ **聲稱保守咗** |
| 「The ~380 warning rows … identical ~380 an `activities` page pulls」 | **3,056 / 3,056**（每 field 382） | ✅ 逐行相同 |

Docstring 講「to within some 4%」，我量到 0.8% —— **又係報得比實際差，方向啱**。

`year` 額外嗰 2 行 `Meta`（8× 就 16 行）係 `activities` 冇嘅，數量級可以忽略，
docstring 亦冇聲稱過佢哋唔存在。

---

# 5. S-054 — 唔做嘅取捨成唔成立

## 5.1 Developer 嘅 mutation 我重做

`_charge_list_rows` 改成只收 75% `limit`（`rows * 3 // 4`）：

```
4 failed, 378 passed in 51.98s

FAILED tests/test_api.py::test_the_widest_list_fan_out_is_refused_after_the_budget
FAILED tests/test_api.py::test_the_row_budget_is_the_boundary          ← 精確嗰條
FAILED tests/test_api.py::test_a_refused_page_stops_the_operation_where_it_stands
FAILED tests/test_api.py::test_year_pays_a_page_of_the_row_budget
```

✅ **同聲稱一模一樣**：4 條紅，`test_the_row_budget_is_the_boundary` 喺入面。

## 5.2 「exact count 只會係第 5 條紅」—— developer 冇量，我量咗

我喺嗰條 test 入面插咗個探針打印實際 row 數：

```
clean tree      rows_loaded = 2920   → 2920 <= 4000  ✅ 綠
75% mutant      rows_loaded = 3650   → 3650 <= 4000  ✅ 照綠
```

- Clean 嗰個 **2,920** 同註釋寫嘅完全對上，slack = 1 − 2920/4000 = **27.0%**，
  註釋寫「about 27%」✅
- Mutant 之下實際讀 **3,650** 行（因為收費由 500 跌到 375，affordable page 由 8 變 10）。
  `<=` 照綠，但 `== 2920` **會紅**。

→ **聲稱嘅兩半都成立**：exact count 確實會係**第 5 條**紅，唔係唯一一條。

## 5.3 我嘅判斷 —— **取捨成立，同意唔收緊**

- 呢條 test 問嘅問題（「有冇嘢綁住呢個 shape」）同 exact count 問嘅（「綁得準唔準」）
  唔同。後者已經由 `test_the_row_budget_is_the_boundary` 用 4000 served / 4001 refused
  **精確**答咗，而且係用常數而唔係 fixture size 答嘅。
- 收緊到 2,920 = 將一條 budget test **綁死喺 `year_db` 有幾多個 activity**。
  fixture 改大細（好正常嘅事）就要改 test，而且改嗰個人**唔會知**呢個數字原本量緊乜。
  呢個成本係實在嘅，換返嚟嘅係一個已經有四重覆蓋嘅 bug 嘅第五重。
- 註釋寫得**誠實過頭都唔為過**：連「15x the bound, not 1.3x」都寫埋，
  即係明講咗呢條 test 嘅解析度去到邊。呢個正正係 S-054 方案 B 要嘅嘢。

唯一可以再挑剔嘅：段註釋 13 行，比佢守住嗰句 assertion 長好多。但呢個 module
一路都係咁（`MAX_LIST_ROWS_PER_REQUEST` 本身 60 行 docstring 守一個 `4000`），
**同 module 慣例一致，唔算 finding**。

---

# 6. S-053 摺入 Round 2 —— 判斷合唔合理

## 6.1 係咪真係唔改 SDL 就做唔到 —— ✅ 確認

`LIST_ROWS_NOTE` 係一個 module 常數，直接 **concat 落五個 field 嘅 `description=`**：

```
src/api/schema.py:958   activities  … + LIST_ROWS_NOTE
src/api/schema.py:996   weight      … + LIST_ROWS_NOTE
src/api/schema.py:1018  weather     … + LIST_ROWS_NOTE
src/api/schema.py:1040  warnings    … + LIST_ROWS_NOTE
src/api/schema.py:1066  year        … + LIST_ROWS_NOTE      ← S-053 講嗰個
```

而佢**已經出咗街**：

```
$ grep -c "One request may open at most 4000 rows of pages in total" frontend/schema.graphql
5
```

S-053 兩個方案（A：`year` 唔貼，自己寫一句；B：拆兩半，`year` 只貼前半）
**兩個都必然改到 `year` 嘅 published description**，即 `frontend/schema.graphql` 一定要
regenerate，否則 `run365-schema --check` 就會紅。
**Developer 話「改佢會令 SDL 變」係事實陳述，唔係推搪。** ✅

## 6.2 摺入 CUI-0025 合唔合理 —— ✅ 合理，而且配得幾靚

CUI-0025 個標題本身就係：

> `points: 0` 喺兩個 data mode 契約唔一致，而且 **`points` / `limit` 嘅範圍冇寫入 SDL**

即係 CUI-0025 本來就要：(a) 動 `src/api/schema.py` 嘅 field description，
(b) regenerate `frontend/schema.graphql`，(c) 重新審一次 list field 嘅 description 措辭。
**S-053 三樣嘢完全重疊**，分開做等於將同一份 SDL regenerate 兩次、
叫 review 同 QA 睇兩次同一個 diff。摺入係嚴格更優。

> 📌 **交接提示**：做 CUI-0025 嗰陣記得 S-053 個實質問題係
> 「`LIST_ROWS_NOTE` 尾句 `charged on \`limit\` as asked for` 貼咗落一個**冇 `limit`** 嘅 field」。
> 上一輪推薦方案 B（拆兩半：共用嗰句 + 只適用於有 `limit` 嗰句），我維持呢個推薦。

---

# 7. 橫向檢查

## 7.1 測試數 378 → 382 —— ✅ 數啱，零測試被刪

```
$ git diff 18368b8..HEAD -- tests | grep -E "^[+-].*def test_"
-def test_track_fails_closed_when_the_schema_has_no_budget_extension(year_session):
+def test_a_budget_fails_closed_when_the_schema_has_no_budget_extension(
-def test_track_fails_closed_when_the_context_cannot_be_seeded(year_session):
+def test_a_budget_fails_closed_when_the_context_cannot_be_seeded(year_session, document, path):
```

**兩條 `-` 都各自有一條對應嘅 `+`，即係改名唔係刪除。** 冇任何 test function 淨係被刪走。

| 項 | 數 |
|---|---|
| 上一輪 | 378 |
| `test_track_fails_closed_when_the_schema_has_no_budget_extension` 改名 + parametrize ×3 | −1 +3 |
| `test_track_fails_closed_when_the_context_cannot_be_seeded` 改名 + parametrize ×3 | −1 +3 |
| **淨** | **+4** |

378 + 4 = **382** ✅ 同 `pytest --collect-only` 嘅 `382 tests collected` 完全對上。
`pytest --collect-only -q | grep -c fails_closed` = **6**（2 function × 3 param）✅

## 7.2 Production 行為有冇改動 —— ✅ **零，AST 結構性證明**

唔係讀 diff 得出，係比對 AST：將 `src/api/schema.py` 前後兩版**所有 bare string-literal
expression statement**（function/class/module docstring **同埋** attribute docstring）剝走，
再 `ast.dump(annotate_fields=True)` 逐字比：

```
AST with ALL bare string statements stripped -- IDENTICAL: True
module-level constants changed: NONE
```

即係話：
- **冇任何 executable statement 變過**（冇加、冇刪、冇改）
- **冇任何 module-level 常數值變過**（包括 `MAX_LIST_ROWS_PER_REQUEST`、`LIST_ROWS_NOTE`、
  `COUNT_DESCRIPTION` 等每一個 description 字串）
- 成個 `src/` delta **100% 係 docstring**

（第一次只剝 body[0] 嘅時候，唯一走出嚟嘅差異就係 `MAX_LIST_ROWS_PER_REQUEST = 4000`
下面嗰個 attribute docstring —— 即係話連「唯一嘅差異」都係 docstring 本身。）

所以上一輪 close 咗嘅效能結論、等價性結論（24 document / 21 個 SHA-256 一致）
**唔需要重做亦唔可能失效**，我按指示冇重做。

## 7.3 SDL byte-identical —— ✅ 核實

```
$ git diff --stat 18368b8..HEAD -- frontend/schema.graphql
（空）
$ .venv/bin/run365-schema --check frontend/schema.graphql
frontend/schema.graphql is up to date
```

Developer 聲稱成立 ✅（而且由 §7.2 嘅「module-level constants changed: NONE」
獨立佐證 —— description 字串一個都冇郁，SDL 冇得變）。

## 7.4 Scope —— ✅ 冇越界

```
$ git diff --stat 18368b8..HEAD
 src/api/schema.py | 58 ++++++-----
 tests/test_api.py | 78 +++++++-------
 2 files changed, 109 insertions(+), 27 deletions(-)

$ git diff --stat 18368b8..HEAD -- frontend/ docs/ .github/ README.md pyproject.toml
（空）
```

`docs/`、`.github/`、`frontend/`、`pyproject.toml`、`.tickets/` 全部零改動。
四個 commit 各自只掂自己該掂嘅嘢。

## 7.5 Commit 規範 —— ✅ 合規

| Commit | Format | Review item | 判斷 |
|---|---|---|---|
| `19dede8` | `fix: W-027 \| parametrize the fail-closed budget tests over the list charge too` | W-027 + S-052 | ✅ 上一輪**明文批准**咗 S-052 併入 W-027（同一段代碼），commit message 亦有交代 |
| `d603706` | `fix: W-028 \| give the charging list a rule, and make it cover all eleven root fields` | W-028 | ✅ |
| `d1be47c` | `fix: S-057 \| write down that year's 500-row charge is a proxy, not a ceiling` | S-057 | ✅ |
| `34f10df` | `fix: S-054 \| write down the row gate's fixture slack, and keep the` `<=` | S-054 | ✅ |

一個 review item 一個 commit，格式 `fix: <ID> | <描述>` 全中，無夾帶。
四個 commit message 都**主動報咗自己驗證嘅方法同數字** —— 呢點下面再講。

## 7.6 Secret / 絕對路徑 / model identifier —— ✅ 全清

```
$ git diff 18368b8..HEAD | grep -E "^\+" \
    | grep -iE "/home/|/Users/|api[_-]?key|secret|token *= *['\"]|password|claude-(opus|sonnet|haiku)|gpt-|sk-"
（零命中）
```

Commit trailer 嘅 `Co-Authored-By: Claude` / `Claude-Session` 係規定格式，
而且**冇帶具體 model identifier**，冇洩漏。

---

# 8. 發現（3 × 🟢 Suggestion，0 Critical，0 Warning）

> 三個全部係 **文件精度**，零行為風險。三個都**唔推翻任何已 close 嘅結論** ——
> 「呢啲 field 唔收費係啱嘅」喺三個 finding 之後仍然完全成立。

## 🟢 S-058｜`activity(id:)` 嗰組數量緊嘅係「id 揀唔中」嗰條路，唔係真實路徑

- **位置**：`src/api/schema.py`，`MAX_LIST_ROWS_PER_REQUEST` docstring 第 3 個 bullet，
  同埋本 delta **新寫**嗰句結語
- **描述**：個 bullet 講
  > `activity(id:)` does not pay. **It reads one row by primary key**, and `MAX_QUERY_TOKENS`
  > admits at most 90 of them: **90 rows and ~0.05 s** on the export, **some 300x** inside the
  > 15 s function

  但 `service.activity()` 係 `session.get(Activity, id, options=[selectinload(Activity.warnings)])` ——
  **一條 PK get 之外仲有一條 selectinload**，所以每個 field **兩條 statement**，
  materialise **1 + N 行**（N = 嗰個 activity 有幾多個 warning）。

- **我實測 90 個 aliased `activity(id:)`，換唔同 id**：

  ```
  id 揀唔中（"__missing__"）   48.7 ms  307.9x  stmts= 90  rows=0
  真實 id / 零 warning         93.0 ms  161.2x  stmts=180  Activity:90
  90 個唔同真實 id（最真實）   94.4 ms  158.9x  stmts=180  Activity:90  ActivityWarning: 74
  真實 id / 3 個 warning       98.0 ms  153.1x  stmts=180  Activity:90  ActivityWarning:270
  真實 id / 8 個 warning（最多）101.5 ms 147.7x  stmts=180  Activity:90  ActivityWarning:720
  ```

  **「90 rows / ~0.05 s / some 300x」三個數，只有 id 揀唔中嗰行同時對得上。**
  真實 id 之下係 **93–101 ms（~2 倍）、148–161×（~一半）、90–810 行（最多 9 倍）**。

  W-028 commit message 講「The same harness reproduces the documented 90 for `activity(id:)`
  **at ~47 ms**」—— 47 ms 正正就係 miss 嗰條路，所以 developer 係「成功重現咗一個
  本身就量錯咗嘅數」，唔係自己量錯。

- **點解今輪先提**：呢段字大部分係 pre-existing，但**本 delta 新寫嘅結語**將佢變成一條算術：

  > A request may hold that many rows, **plus up to 90 single ones**, plus the fixed-size reads
  > that do not pay.

  呢句係叫讀者攞 4000 加 90 去估成個 request 嘅 row 曝露面。實際係 4000 + 最多 810 +
  332（166 個 `meta` × 2）。W-028 個修法嘅賣點就係「exhaustive rather than illustrative」，
  所以呢條算術要準先對得住佢自己。

- **影響**：**純文件精度，零風險。** 148× 仍然離 15 s function limit 極遠，
  「唔收費」呢個判斷完全冇變。出事嘅係下一個攞呢個數做基準嘅人。
- **方案 A（推薦）**：三個數同時更正，同時講明點解會有兩個讀數：
  > `activity(id:)` does not pay. It reads one row by primary key **and its warnings through a
  > second `selectinload`, so two statements and 1 + N rows a field**. `MAX_QUERY_TOKENS` admits
  > at most 90 of them: **90 distinct ids measure ~95 ms and 164 rows on the export, some 160x
  > inside the 15 s function; the dearest single id repeated 90 times reads 810 rows at ~100 ms**
  > — so charging it would buy noise.

  Trade-off：bullet 長咗兩行，但同其餘四個 bullet 一樣係「規則 + 實測數」。
- **方案 B**：只改結語，唔動 bullet ——「plus up to 90 single ones **and the warnings they pull**」。
  Trade-off：平，但個 bullet 本身仲係留住「one row by primary key」呢句唔準嘅話。
- **推薦：方案 A**。呢個 module 一路唔接受「大概啱」嘅數，W-028 修完之後更加唔應該。

## 🟢 S-059｜`meta` 寫成「one-row export header」，實際係 **2 行**

- **位置**：`src/api/schema.py`，`MAX_LIST_ROWS_PER_REQUEST` docstring，本 delta 新增嘅第 4 個 bullet
- **描述**：新 bullet 寫
  > A count is one scan returning one row, and **`meta` is the one-row export header**

  但 `service.meta()` 係 `{m.key: m.value for m in session.scalars(select(models.Meta))}` ——
  一個 **key / value 表**，讀晒全表。實測：

  ```
  export 入面 Meta 表 = 2 行（year / generated_at）
  1x meta      stmts=1   {'Meta': 2}
  166x meta    stmts=166 {'Meta': 332}
  ```

- **點解值得提**：本 delta 條規則**明文 keyed 喺 rows materialised**
  （「it is *rows materialised* that this budget bounds」），
  所以喺同一段 docstring 入面講錯 row 數，係喺佢自己個自變數上面出錯。
  上一輪報告建議嘅字係「**`meta` returns two**」—— **上一輪嗰個數係啱嘅**，
  重寫嗰陣走樣咗。
- **影響**：**零**。2 仍然係 fixed，仍然唔隨 export 大，規則推出嚟仍然係「唔收費」。
  純粹係一個唔準嘅數字。
- **方案 A（推薦）**：`` `meta` is the two-row key/value export header ``。一個字。
- **方案 B**：寫「returns a fixed handful of rows」，唔講具體數。
  Trade-off：唔會錯，但同呢段其他 bullet「每個數都寫死」嘅風格唔一致。
- **推薦：方案 A**。

## 🟢 S-060｜warm / cold 嘅 caveat 只活喺 commit message，同埋「the dearest is」唔可重現

- **位置**：`src/api/schema.py`，`MAX_LIST_ROWS_PER_REQUEST` docstring 第 4 個 bullet
- **描述（兩件相關嘅小事）**：
  1. **Cold cache**：docstring 報 `180x` / `320x`，全部係 warm steady state。
     Developer 自己量到 cold 嗰次係 **151 ms / 99×**（差唔多兩倍），
     **只寫咗喺 commit message**。詳細判斷見 §3 —— 方向啱（保持成張表可比），
     但呢個 API **行喺 Vercel function 之上打一個 SQLite 檔**，cold page cache
     係 serverless 冷啟嘅常態，所以 99× 對 production 嚟講唔係 artefact。
  2. **「the dearest is `activitiesCount`, ~83 ms」**：我實測四個 count 係
     `activitiesCount 83.9 / weightCount 82.3 / weatherCount 79.9 / warningsCount 85.3`，
     **最貴嗰個係 `warningsCount` 唔係 `activitiesCount`**。上一輪量到嘅次序又係第三個
     （83.0 / 77.2 / — / 75.4）。四個數相差 < 7%，**邊個係「the dearest」逐次跑都唔同** ——
     呢個最高級形容詞冇可重現性。個區間（`77-86 ms`）就完全企得住，兩輪量度都落喺入面。
- **影響**：**零行為風險。** 兩件事都只影響下一個攞呢啲數做基準嘅人。
- **方案 A（推薦）**：bullet 尾加一句 + 刪走個最高級形容詞：
  > 332 aliased counts read 77-86 ms on the export **(the four are within 7% of each other, so
  > which is dearest is noise)** … **These are warm-cache readings, as every figure in this
  > docstring is; a first cold pass roughly doubles them, to some 99x, which is still two orders
  > inside the function.**

  Trade-off：兩句字，換返嚟 docstring 自己交代埋佢自己嘅量度條件。
- **方案 B**：只刪最高級形容詞，cold caveat 留喺 commit message。
  Trade-off：最平，但 §3 講嘅 Vercel 論點就冇人接住。
- **推薦：方案 A**。呢個 module 一路都係「數字 + 佢喺咩條件下量」兩樣齊
  （`MAX_LIST_ROWS_PER_REQUEST` 尾段就特登寫低「None of these readings is asserted, because a
  wall time in CI buys a flaky test」），補呢句先似佢自己。

---

# 9. 上一輪 finding 結算

| ID | 上一輪推薦 | 今輪狀態 | 我嘅核實 |
|---|---|---|---|
| **W-027** 🟡 | 方案 B（parametrize track 兩條） | ✅ **closed** | 照方案 B 做；兩個 mutation 獨立重做，數字全對；我再加第三個 mutation 證明 track 嗰邊冇被整鈍 |
| **W-028** 🟡 | 方案 A + B（規則 + 補齊實測數） | ✅ **closed** | A + B 都做咗，仲多咗「第二子句點解係重點」一段；規則獨立推 11 / 11 全中；五個數抽驗全部落喺聲稱區間 |
| S-052 🟢 | 跟 W-027 一齊做 | ✅ closed | `grep -n "_TrackBudget" src tests` 零命中（`.proj-docs/` 歷史紀錄保留，正確） |
| S-053 🟢 | 方案 B（拆兩半） | ⏳ **摺入 Round 2（CUI-0025）** | ✅ 判斷成立 —— §6 證明唔改 SDL 做唔到，而 CUI-0025 本來就要改 SDL description |
| S-054 🟢 | 方案 B（註釋講明鬆位） | ✅ **closed** | 照方案 B；mutant 重做 4 紅、探針證明 exact 會係第 5 紅；27.0% slack 數字核實 |
| S-055 🟢 | 方案 B（獨立開票，連 `track` 一齊） | ⏳ **仍然 open，仍然未開票** | pre-existing 行為，本 delta 完全冇掂。繼續帶落去 |
| S-056 🟢 | 方案 A（補 ticket + 開 CUI-0029） | ✅ **closed** | `18368b8` 補齊咗執行紀錄、三個用戶決定、DoD 18.2× 豁免留痕。**CUI-0029 嗰半冇做係啱嘅** —— 用戶決定明文係「記錄但不處理、**不開票**」，ticket 已如實記低 |
| S-057 🟢 | 方案 A（補一句前提） | ✅ **closed** + **更正咗上一輪一個事實錯誤** | §4：`year` 讀嘅係 382 `ActivityWarning` + 2 `Meta`，**唔係 384 行 weight**。Developer 啱，上一輪報告錯 |

---

# 10. ✅ 做得好嘅地方

1. **每個 commit 自己帶住反證，而唔係帶住結論。**
   四個 commit message 入面三個報咗 mutation 或者 instrumentation 嘅結果
   （`.get(KEY, MAX)` 的 378/4、75% undercharge 的 4 紅、identity map 的 382 + 2）。
   我逐個重做，**四個數字冇一個對唔上**。呢個唔係「聲稱」，係可以 replay 嘅證據。

2. **冇照抄 reviewer 建議嘅代碼，而且係對嘅。**
   上一輪「修訂後代碼」段建議三個 case 一律 `assert result.data is None` —— 照抄會令
   track 兩條即刻紅（track 被拒只 null parent，`data` 仍然係 `{'activity': None}`）。
   Developer 改為抽 `_rows_at(result, path)`，**呢個先係唯一喺兩種 propagation 形狀下
   都成立嘅斷言形狀**，而且 commit message 明文講低咗呢個分別。
   **Review 建議唔係規格，對嘅時候先跟** —— 呢次示範得好。

3. **敢反駁 reviewer，而且用數據反駁。**
   S-057 個 commit 專門開一段「One correction to the review while confirming it」，
   用 identity map 指出上一輪將 `ActivityWarning` 當咗係 weight。我核實佢係啱嘅。
   **一個肯反駁 review 嘅 lane 比一個照單全收嘅有用好多**，前提係佢帶住量度 —— 佢帶咗。

4. **W-028 交出嘅嘢比要求多。**
   上一輪要「規則 + 補齊數字」。Developer 加咗一段專門講「第二子句先係例子教唔到嗰句」，
   而呢段正正就係我試圖拗個規則嗰陣（`activitiesCount` 有 date window）攔住我嘅嗰句。
   **佢預咗下一個人會點誤解。**

5. **「唔做」都用 mutation 證明。**
   S-054 決定唔收緊，但冇停喺「我覺得唔值得」，而係量咗「收緊會捉到啲乜、依家係咪已經捉到」。
   一個 **won't-fix 帶住反證**，係我見過最健康嘅 won't-fix 形狀。

6. **主動記低對自己不利嘅讀數。**
   Cold cache 嗰次 151 ms / 99× 冇被靜靜咁丟掉，寫咗落 commit message 同埋
   講明點解 commit 嘅係 warm。S-060 講嘅係「呢句應該搬上 docstring」，
   **唔係「佢瞞咗」** —— 佢冇瞞。

7. **Delta 乾淨到可以用 AST 證明。**
   `src/` 剝走所有 string literal statement 之後 AST 完全相同、module 常數零變動。
   即係 reviewer 可以用一條機械化檢查代替「我睇過覺得冇改到行為」。
   **一個做成咁嘅 delta，令 re-review 成本低一個數量級。**

---

# 11. 修正優先順序

| 順序 | ID | 嚴重度 | 內容 | 建議方案 | 成本 |
|---|---|---|---|---|---|
| 1 | **S-058** | 🟢 | `activity(id:)` 三個數量緊 miss path；新結語「plus up to 90 single ones」偏低最多 9 倍 | 方案 A（三個數 + selectinload 一齊更正） | ~15 分鐘 |
| 2 | **S-060** | 🟢 | cold caveat 只喺 commit message；「the dearest is」唔可重現 | 方案 A（補一句 + 刪最高級形容詞） | ~5 分鐘 |
| 3 | **S-059** | 🟢 | `meta` 寫「one-row」實際 2 行 | 方案 A（一個字） | ~1 分鐘 |
| — | S-053 | 🟢 | `LIST_ROWS_NOTE` 貼咗落冇 `limit` 嘅 `year` | **Round 2 / CUI-0025**（已決定，唔喺本輪） | — |
| — | S-055 | 🟢 | 被拒 request 留 full traceback（pre-existing） | 獨立開票，連 `track` 一齊 | 開票即可 |

> **三個新 Suggestion 全部喺同一個 docstring（`MAX_LIST_ROWS_PER_REQUEST`）入面，
> 而且全部係純文字。合共 ~20 分鐘，零行為風險，唔會令任何已驗結論失效。**
> Commit 規範照舊：一個 review item 一個 commit，`fix: <ID> | <描述>`。
> 三個都喺同一段 docstring，但**唔好合併** —— 三個係三件唔同嘅事（錯數字 / 錯數字 / 缺 caveat），
> 分開先追蹤到。

---

# 12. 修訂後代碼

> 本輪 **0 Critical、0 Warning**，冇任何嘢需要即時改先可以用。
> 以下係三個 Suggestion 嘅建議實作，由 developer 執行（Reviewer 唔改代碼）。
> 三段全部喺 `src/api/schema.py` 嘅 `MAX_LIST_ROWS_PER_REQUEST` docstring 之內。

## S-058 — `activity(id:)` bullet + 結語

```python
# ── 取代現有第 3 個 bullet ────────────────────────────────────────────────
* ``activity(id:)`` does not pay. It reads one row by primary key and its
  warnings through a second ``selectinload``, so two statements and 1 + N rows
  a field rather than the one row an earlier draft of this list claimed.
  :data:`MAX_QUERY_TOKENS` admits at most 90 of them, and 90 *distinct* ids --
  the realistic shape -- measure ~95 ms and 164 rows on the export, some 160x
  inside the 15 s function. Repeating the single dearest id 90 times is the
  ceiling, at 810 rows and ~100 ms; 90 ids that match nothing is the floor, at
  ~47 ms and no rows at all, which is the reading the "~0.05 s, some 300x"
  this bullet used to carry had actually measured (CUI-0027 S-058). Charging
  it would buy noise at any of those three readings;

# ── 取代現有結語 ──────────────────────────────────────────────────────────
So the bound this constant states is on *windowed* reads. A request may hold
that many rows, plus up to 90 single ones and the warnings they pull -- 810
rows at the worst id -- plus the fixed-size reads that do not pay.
```

## S-059 — `meta` 嘅行數

```python
# ── 第 4 個 bullet 入面 ───────────────────────────────────────────────────
-  A count is one scan returning one row, and ``meta`` is the one-row export
-  header; neither widens with a window, so neither has a window to charge.
+  A count is one scan returning one row, and ``meta`` is the two-row
+  key/value export header (``year`` and ``generated_at``); neither widens
+  with a window, so neither has a window to charge.
```

## S-060 — 量度條件 + 刪走唔可重現嘅最高級形容詞

```python
# ── 第 4 個 bullet 尾段 ───────────────────────────────────────────────────
-  bounds that: at the widest each admits, 332 aliased counts read 77-86 ms on
-  the export (the dearest is ``activitiesCount``, ~83 ms, some 180x inside the
-  function) and 166 aliased ``meta`` reads ~46 ms, some 320x. That is the
-  ``activity(id:)`` order of magnitude above, and charging them would buy the
-  same noise.
+  bounds that: at the widest each admits, 332 aliased counts read 77-86 ms on
+  the export -- the four land within 7% of each other, so which one is dearest
+  is run-to-run noise rather than a fact about the fields -- and 166 aliased
+  ``meta`` reads ~46 ms, some 320x. That is the ``activity(id:)`` order of
+  magnitude above, and charging them would buy the same noise.
+
+  Every reading in this docstring is taken with the page cache warm, and says
+  so here rather than in a commit message nobody reads next to the number. A
+  first cold pass roughly doubles them -- ``activitiesCount`` measured 151 ms,
+  some 99x -- which matters more here than the word "cold" suggests, because
+  this schema is served by a Vercel function over a SQLite file and a cold
+  invocation is the ordinary case, not the artefact. 99x is still two orders
+  inside the function, so the conclusion does not move (CUI-0027 S-060).
```

---

# 13. 整體建議

1. **呢批嘢可以收。** 兩個 Warning 真正 close，hard gate 全綠，coverage 100%，
   production 行為 AST 證明零改動，SDL byte-identical。三個新 Suggestion 加埋 ~20 分鐘，
   而且**唔係 blocker**：佢哋一個都唔改變任何行為，亦唔改變任何已驗結論。
   照 `sw-post-review-handoff` Protocol 1，`status=pass` 之後應該行 QA。

2. **三個新 Suggestion 建議喺 Round 2 一齊執，唔好另開一輪。**
   S-053 已經摺咗入 CUI-0025（要動同一個檔嘅 description），
   S-058 / S-059 / S-060 三個都喺 `MAX_LIST_ROWS_PER_REQUEST` 個 docstring 入面。
   四件事夾埋係一個 docstring pass，做完跑一次 `run365-schema --check` 就知有冇掂到 SDL
   （S-058/059/060 應該冇，S-053 一定有）。

3. **`activity(id:)` 呢個教訓值得帶走（比個 finding 本身重要）。**
   一個「量度 miss path 當成量度 hit path」嘅錯，喺呢個 repo 已經過咗
   **兩輪 review + 一輪 developer 重現** 都冇人發現，因為三次都用同一種 harness。
   S-058 個修法入面我特登建議寫低「呢個數原本量緊乜」，
   就係想令下一個人唔使再撞一次。以後量 `activity(id:)` 類型嘅 field，
   **先確認個 id 真係 resolve 到**（`stmts` 係 2×N 而唔係 1×N 就係訊號）。

4. **S-055 已經跨咗三輪仲未開票。** 佢係 pre-existing、低風險、明確唔屬 CUI-0027，
   但「等有真實 log 噪音先處理」呢個決定本身冇留痕喺任何 ticket 度。
   建議連 `track` 一齊開一張（`.proj-docs/tickets.md` 註明現存最高 CUI-0028，
   下一個係 CUI-0029 —— 留意 CUI-0029 曾經被建議畀 alias 軸但用戶決定不開票，
   所以個號仲喺度，冇被用走）。

5. **`.proj-docs/index.md` 頂部日期已經 stale**（仲停留喺 CUI-0019 / 365 tests 嗰陣，
   未反映今日兩份 CUI-0027 報告同 382 tests）。CLAUDE.md §7 要求「每次輸出後更新頂部日期」。
   我已經加咗本報告同上一輪報告嘅條目並更新頂部日期；
   如果 main agent 有自己嘅 registry 流程，以 main agent 為準。

---

# 14. 驗證用臨時改動聲明

為咗獨立重做 developer 嘅聲稱，我對 working tree 做過以下臨時改動，**全部已還原**：

| # | 改動 | 目的 | 還原方式 |
|---|---|---|---|
| 1 | `_charge_list_rows` 嘅 `try/except KeyError` → `.get(LIST_ROWS_KEY, MAX_…)` | 重做 developer 嘅 fail-open mutant（§1.1） | 由 scratchpad backup 覆蓋 |
| 2 | 刪走 `_RequestBudgets.on_operation` 嘅 `context[LIST_ROWS_KEY] = …` | 重做 cross-check mutant（§1.2） | 同上 |
| 3 | `_charge_track_field` 嘅 `try/except KeyError` → `.get(…)` | **我自己加嘅** track fail-open mutant（§1.3） | 同上 |
| 4 | `rows` → `rows * 3 // 4` | 重做 75% undercharge mutant（§5.1） | 同上 |
| 5 | 喺 `test_the_widest_list_fan_out_reads_no_more_rows_than_the_budget` 插 `print` 探針 | 量實際 row 數（§5.2） | 由 backup 覆蓋 `tests/test_api.py` |
| 6 | 臨時 `tests/test_zz_probe.py`（探 propagation 形狀，§1.4） | 打印三個 document 嘅 `data` / `errors[0].path` | `rm` |

**還原後核實**：

```
$ git status --porcelain
（空）
$ git diff --stat
（空）
$ md5sum src/api/schema.py tests/test_api.py
d103ce8aaa3223e703dae5c52424b1d9  src/api/schema.py     （同 backup 一致）
edf63e2cbb6c4be789e764a18dae043c  tests/test_api.py     （同 backup 一致）
$ .venv/bin/python -m pytest tests -q
382 passed in 49.21s
$ .venv/bin/ruff check src tests
All checks passed!
$ .venv/bin/run365-schema --check frontend/schema.graphql
frontend/schema.graphql is up to date
```

**✅ Working tree 完全乾淨，`git diff --stat` 為空，HEAD（`304aff6`）未變。**
Reviewer 全程冇執行過任何 `git merge` / `git push` / `git commit` / `git checkout`。
所有量度同 mutation script 寫喺 scratchpad，唔喺 repo 入面。
所有效能量度用 `data/processed/run365.db`（git-ignored 嘅 production export），冇改過佢。
本報告同 `.proj-docs/index.md` 嘅條目係本次唯一留低嘅檔案改動。

---

```handoff-receipt
protocol: 1
agent: code-reviewer
status: pass
score: 95/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "100% (src/api/schema.py, 307 stmts / 0 miss), 99% (run365days.api overall; sole miss service.py:348, pre-existing)"
  no_critical: pass
  security_scan: n/a
critical: 0
warning: 0
suggestion: 3
report: .proj-docs/reviews/2026-09-16_review_CUI-0027_delta.md
tickets: CUI-0027, CUI-0022, CUI-0024
next_action: invoke_qa
next_agent: quality-assurance
branch: "claude/ai-dev-team-start-05jie2"
context: "84 → 95。W-027 / W-028 兩個 Warning 真正 close：W-027 嘅 fail-open mutant（4 fail / 378 pass，全部 AssertionError）同 seeding-removal cross-check（51 fail / 331 pass，6 條 fail-closed 全綠）我獨立重做，數字逐個對上；我再加第三個 mutation 證明 _rows_at 重構冇整鈍原本兩條 track test。W-028 條規則我獨立推 11 個 root field，11/11 同實際收費一致，五個實測數抽驗全部落喺聲稱區間。S-054 嘅 won't-fix 取捨用 mutant + 探針驗證成立（clean 2920 / mutant 3650，exact count 確實只會係第 5 紅）。S-057 更正上一輪 review 嘅事實錯誤（year 讀嘅係 382 ActivityWarning + 2 Meta，唔係 384 行 weight）—— 用 identity map 核實 developer 啱、上一輪錯，已喺報告 §4 正式記低。Production 行為零改動：AST 剝走所有 string literal statement 後完全相同、module 常數零變動、SDL byte-identical。382 tests（+4 = 2 個 test 改名後各 parametrize ×3），零測試被刪。"
blockers: []
follow_ups:
  - "S-058 🟢: activity(id:) 嗰組「90 rows / ~0.05 s / some 300x」量緊嘅係 id 揀唔中嗰條路（48.7 ms / 0 rows）；真實 id 之下係 93–101 ms、148–161×、90–810 行、每 field 兩條 statement（selectinload warnings）。本 delta 新寫嘅結語「plus up to 90 single ones」因而偏低最多 9 倍。零風險（148× 仍然極遠離 15 s），純文件精度。"
  - "S-059 🟢: 新 bullet 寫 `meta` 係「the one-row export header」，實際係 2 行 key/value（實測 Meta: 2；166 alias = 332 行）。上一輪建議嘅字（returns two）先係啱。零風險。"
  - "S-060 🟢: cold-cache caveat（151 ms / 99×）只寫咗喺 commit message 唔喺 docstring —— 呢個 API 行喺 Vercel function 打 SQLite 檔，cold 係常態；另外「the dearest is activitiesCount」唔可重現（我量到 warningsCount 85.3 > activitiesCount 83.9，四個相差 <7%）。零風險。"
deferred:
  - "S-053（LIST_ROWS_NOTE 貼咗落冇 limit 嘅 year）→ Round 2 / CUI-0025。已核實成立：LIST_ROWS_NOTE 喺 frontend/schema.graphql 出現 5 次，任何改法都要 regenerate SDL，而 CUI-0025 本身就係『points / limit 範圍冇寫入 SDL』，同一個檔同一次 regen。"
  - "S-055（被拒 request 留 full traceback）仍然 open、仍然未開票。Pre-existing，連 track budget 一齊，建議獨立開票（下一個編號 CUI-0029）。"
notes: "三個新 Suggestion 全部喺 MAX_LIST_ROWS_PER_REQUEST 同一個 docstring 入面、全部純文字、合共約 20 分鐘，建議連 S-053 一齊喺 Round 2 一個 docstring pass 執完；唔係 blocker，DoD（≥90 分 + 0 Critical）已達成，可以直接行 QA。Working tree 已核實乾淨（git diff --stat 空、md5 同 backup 一致、382 passed），Reviewer 全程零 git 寫操作。"
```
