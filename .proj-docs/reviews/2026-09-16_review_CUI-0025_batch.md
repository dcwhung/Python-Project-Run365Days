# Batch Review — 2026-09-16 — Round 2（CUI-0025 / CUI-0028 / CUI-0032 + S-053 / S-058 / S-059 / S-060）

**審閱者**：Code Reviewer（獨立角色，冇參與呢批代碼嘅編寫）
**範圍**：`git diff 74f0de3..HEAD`，7 個 commit、3 張 ticket、4 個 review item
**Branch**：`claude/ai-dev-team-start-05jie2`
**報告**：`.proj-docs/reviews/2026-09-16_review_CUI-0025_batch.md`

> ⚠️ 本報告入面**每一個**量度都係我自己由零重做過至少一次，冇一個係照抄 commit message
> 或者 ticket 執行備註。凡係我重做唔到、或者做出唔同結果嘅，都喺下面明文講咗。
> 所有為驗證而做嘅臨時改動已還原，見文末「驗證用臨時改動聲明」。

---

## 整體 verdict

### 涵蓋 commit

| Commit | Item | 性質 |
|---|---|---|
| `b06d28e` | CUI-0028 | test-only（3 條新測試） |
| `72dfcb5` | CUI-0025 | **唯一有行為改動**（static mode `track(points)` breaking change）+ SDL description |
| `4bd28b9` | S-053 | SDL 措辭（`LIST_ROWS_NOTE` 拆兩半） |
| `b16a317` | CUI-0032 | `pyproject.toml` dev extra + `[tool.coverage.run]` |
| `3b5c0d2` | S-058 | docstring 數字更正（`activity(id:)`） |
| `36c7cf4` | S-059 | docstring 數字更正（`meta` 兩行） |
| `af83fd6` | S-060 | docstring 更正（cold/warm、唔可重現嘅最高級形容詞） |

### Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| Lint（Python） | `.venv/bin/ruff check src tests` | ✅ **pass** — All checks passed! |
| Format（Python） | `.venv/bin/ruff format --check src tests` | ✅ **pass** — 58 files already formatted |
| Lint（前端） | `npm run lint`（eslint） | ✅ **pass** — exit 0，零 output |
| Type check | `npm run typecheck`（codegen + `tsc -b`） | ✅ **pass** — exit 0 |
| SDL 同步 | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ **pass** — is up to date |
| Tests（Python） | `.venv/bin/python -m pytest tests -q` | ✅ **pass** — **402 passed** in 52.6 s |
| Tests（前端） | `npm test` | ✅ **pass** — **131 passed / 22 files** |
| Coverage | `.venv/bin/python -m pytest tests -q --cov` | ✅ **pass** — **95% TOTAL**（1632 statements、82 miss） |
| No Critical | 本報告 | ✅ **pass** — **0 🔴** |
| Security scan | `pip-audit -r`（`pytest-cov==7.1.0` / `coverage==7.16.1` / `pluggy==1.6.0`） | ✅ **pass** — **No known vulnerabilities found**（我喺獨立 scratch venv 自己裝 pip-audit 2.10.1 重跑） |

**Hard gates 10/10 pass。**

### 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|------|------|------|------|
| 正確性 | 24 | 25 | 零行為缺陷；五個決定逐個 mutation 驗過有牙。S-061（`--points` 一改就重新出現 mode 分歧）−1 |
| 安全性 | 20 | 20 | CUI-0025 **收窄**咗攻擊面（static mode 唔再接受 `0`）；新依賴零 CVE、零 GPL、Vercel 兩條路我自己重驗過 |
| 可維護性 | 11 | 20 | **W-029 −5**（一個假量度寫入永久紀錄）；S-062 / S-063 / S-064 / S-067 各 −1 |
| 測試覆蓋 | 13 | 15 | 95%，三組新測試全部 mutation-proven。S-065 / S-066 各 −1 |
| 性能 | 10 | 10 | 零行為改動；docstring 入面嘅性能聲稱我自己重量過，結論全部企得住 |
| 代碼風格 | 10 | 10 | ruff / eslint / tsc 全綠 |
| **總分** | **88** | **100** | |

**結果：⚠️ warn**（hard_gates 全 pass + 0 Critical，但 75–89 分）

### Status 決定

```
hard_gates 10/10 pass ✅
Critical = 0          ✅
score = 88            → 75–89 → status = warn
```

**→ `status=warn`，`next_action=invoke_developer`。**

要 developer 做嘅嘢係具體而且平嘅（見 §9 修正優先順序）：
**W-029 更正一個寫錯咗嘅紀錄**，加 **CUI-0028 DoD 第 4 項**（本來就未做）。
其餘七個 Suggestion 唔阻結案。

> 📌 **本輪唔係「發現 bug」。** 七個 commit 全部零行為缺陷，五個決定全部企得住。
> 扣分集中喺「記錄嘅準確性」—— 而呢個 repo 嘅明文文化就係「docstring / ticket 就係契約」，
> 所以呢類問題喺呢個項目度唔係小事。

---

# Section A — CUI-0025（`72dfcb5`）唯一有行為改動嗰個

**Commit**：`72dfcb5` `fix: CUI-0025 | bound the points contract on both sides and put the ranges in the SDL`
**改動**：`frontend/src/data/static/source.ts`（production）、`source.test.ts`、`src/api/schema.py`、`tests/test_api.py`、`frontend/schema.graphql`

## A.1 五個決定，逐個裁決

### 決定 1 — `points: 0` 喺 static mode 拋錯 ✅ 企得住

api 側 `_track_points`（`src/api/schema.py:818`）：

```python
if not 1 <= points <= MAX_TRACK_POINTS:
    raise ValueError(f"points must be between 1 and {MAX_TRACK_POINTS}, got {points}")
```

static 側新 `checkPoints`（`frontend/src/data/static/source.ts:51`）行為一致。
**唔掂 api mode** —— 我核實過 `git diff` 入面 `_track_points` 一行都冇改。✅

### 決定 2 — `undefined` 仍然回全部，而且 guard 測 `=== undefined` ✅ **關鍵，而且真係測到**

呢個係本票最容易寫錯嘅一點，我**用 mutation 直接證明**咗個 guard 唔止係寫喺度，
而係有測試釘住。將 `points === undefined ? undefined : checkPoints(points)`
換成 falsy 寫法 `points ? checkPoints(points) : undefined`：

```
===== guard on falsiness instead of === undefined =====
 × static source > refuses points: 0 the way api mode does, rather than returning the whole track
      Tests  1 failed | 9 passed (10)
```

**精準一條紅，而且正正係 `points: 0` 嗰條。** 呢個係我見過最乾淨嘅 mutation 結果之一 ——
mutant 同 test 一對一。再做兩個對照：

| Mutation | 結果 |
|---|---|
| 完全還原 pre-fix（`return points ? downsample(rows, points) : rows`） | **3 條紅**（0 / 上界 / 非整數） |
| `points ? checkPoints(points) : undefined`（falsy guard） | **1 條紅**（正正係 `points: 0`） |
| 刪走 `!Number.isInteger(points)` 一臂 | **1 條紅**（正正係非整數嗰條） |

三個 mutant 各自對應唔同嘅測試，**冇一個係靠總數矇中**。✅

### 決定 3 — 錯誤形態逐字相同 ✅ 企得住

| | 表達式 | `points=0` 實際輸出 |
|---|---|---|
| Python | `f"points must be between 1 and {MAX_TRACK_POINTS}, got {points}"` | `points must be between 1 and 1000, got 0` |
| TS | `` `points must be between 1 and ${MAX_TRACK_POINTS}, got ${points}` `` | `points must be between 1 and 1000, got 0` |

兩邊測試都 assert 成句（`tests/test_api.py` 嗰條用 f-string 夾 `MAX_TRACK_POINTS`；
`source.test.ts` 嗰條寫死成句），**唔係淨係 assert 個數字**。✅

### 決定 4 — static mode 都守 `MAX_TRACK_POINTS = 1000` ✅ 企得住

今日不可達（export 封頂 600 行，我核實 `DEFAULT_POINT_LIMIT = 600`
喺 `src/cli/export_data.py:27`，而真 DB 嘅 track 長度全部喺 **250–600** 之間、123 個 distinct 值）。
但係「兩個 mode 一個契約」呢句要成立就必須守。理由成立。✅

### 決定 5 —— 佢自己加嘅「非整數都拒絕」：**我認為合理，而且冇引入新風險**

**超出票嘅範圍？** 嚴格嚟講係。但兩點令我接受：

1. **佢自己喺 ticket 上明文標示咗**：`.tickets/in-progress/0001-0200/CUI-0025.md`
   寫住「額外第五個決定（票上冇問，但同屬「`points` 契約」）」。
   **申報咗嘅 scope 擴張同偷偷塞入去係兩件事**，呢點做得啱。
2. 佢確實同「`points` 契約」同一主題，唔係夾帶無關改動。

**理由企唔企得住？** 企得住，而且我自己核實過源頭。`frontend/src/lib/downsample.ts`
自己個 docstring 本來就寫明：

> A non-integer `limit` is outside the claim -- Python raises on `range(2.5)`
> and on `range(NaN)` where this quietly returns 2 items and `[]`

即係話**冇呢個 guard 嘅話，static mode 會靜靜哋答一條 api mode 根本問唔出口嘅問題**
（SDL 係 `Int!`）。呢個正正係本票要關嘅「同一個 call 兩個 mode 兩個答案」。

**有冇引入新風險？** 我逐個 edge value 查過：

| 輸入 | 舊行為 | 新行為 | 判斷 |
|---|---|---|---|
| `2.5` | 靜靜回 2 件 | 拋 `points must be between 1 and 1000, got 2.5` | ✅ 改善 |
| `NaN` | 靜靜回 `[]` | 拋 `... got NaN` | ✅ 改善（本來係最惡嘅一種：空數組睇落好似「冇 track」） |
| `Infinity` | 回全部 | 拋 `... got Infinity` | ✅ 改善 |
| `-0` | 回全部（falsy） | 拋 `... got 0` | ⚠️ 訊息寫 `got 0` 而唔係 `got -0`，純粹 cosmetic |

**新拋錯路徑有冇人接？** 有。`ActivityView.tsx:164` 已經有
`<TrackError message={readableError(track.error)} onRetry={...} />`，
而 static source 嘅 `defaultFetcher` 本來就會喺 HTTP 失敗時 throw ——
即係話「`track` 會 reject」呢條路徑**喺呢個 commit 之前已經存在而且有 UI 接住**，
唔係新開嘅未處理路徑。✅

**結論：決定 5 接受。** 唯一可以講嘅係佢令 static mode 拒絕嘅值**多過** api mode ——
但多出嗰批（非整數 / NaN / Infinity）係 api mode 根本收唔到嘅值（`Int!`），
所以唔可能造成反方向嘅 mode 分歧。

## A.2 SDL `(1-1000)` 兩處，同實際常數對得上 ✅

```
src/api/schema.py:31   MAX_TRACK_POINTS = 1000
src/api/schema.py:281  MAX_PAGE_SIZE    = 1000
```

兩個 description 都係 f-string 內插常數（`f"... (1-{MAX_TRACK_POINTS}) ..."`、
`f" The window is `limit` (1-{MAX_PAGE_SIZE}) rows ..."`），**唔係寫死 `1000`**，
所以常數一改 SDL 自動跟。我 parse 咗生成嘅 SDL 核實：

```
activities   '4000' count=1 '(1-1000)' count=1
weight       '4000' count=1 '(1-1000)' count=1
weather      '4000' count=1 '(1-1000)' count=1
warnings     '4000' count=1 '(1-1000)' count=1
year         '4000' count=1 '(1-1000)' count=0   ← 啱，year 冇 limit
Activity.track                '(1-1000)' ✅
```

## A.3 前端五條 document 一條都唔受影響 —— **獨立核實 ✅**

我冇信 commit message，自己行咗三步：

1. **`frontend/src/data/api/queries.ts` 入面所有 document**：`Meta` / `Year` / `Activities` /
   `Activity` / `Track` / `Weight` / `Weather` / `Warnings`。**得 `TrackQuery` 掂 `points`**，
   而且係 `query Track($id: ID!, $points: Int!)` —— **NonNull**，即係前端永遠唔會送 `undefined`。
2. **`useTrack` 嘅 caller**：`grep -rn "useTrack" frontend/src` 得兩處 ——
   `ActivityView.tsx:34` 嘅 `useTrack(current, TRACK_POINTS)`，同埋一個 `vi.fn()` mock。
   `ActivityView.tsx:18` 寫住 `export const TRACK_POINTS = 600`。
3. **600 過唔過到新 guard**：`Number.isInteger(600) && 600 >= 1 && 600 <= 1000` → 過。

**今日唯一一個真 caller 送嘅係整數字面量 600，新 guard 對佢完全透明。** ✅

## A.4 兩個 mode 嘅取樣結果仍然一致（CUI-0021 冇被打破）✅

`checkPoints` 係**純 gate**：過到就原值返回，`downsample(rows, limit)` 收到嘅
同改動前一模一樣。合法值嘅取樣路徑**零改動** —— 唯一改變係非法值由「靜靜答」變成「拋錯」。
我讀過 diff 逐行確認冇掂 `downsample` / `_even_positions` / `_track_rows`。

## A.5 `points: 1` 語義不變（CUI-0004）✅

新測試直接釘住：

```ts
it("returns the last row alone for points: 1, as both modes do", async () => {
  expect((await src.track("a", 1)).map((p) => p.sec)).toEqual([24]);
});
```

`downsample` 嘅 `limit < 2 → [items[n-1]]` 分支冇改，`checkPoints(1)` 放行。✅

## A.6 Lane D 嗰個「差啲 ship 咗嘅 false pass」—— **我重現咗，而且證實真係修好** ✅

呢個係本輪做得最好嘅一件事，值得記低完整證據：

```
--- 對住「從來冇講過範圍」嘅舊 description ---
bare  str(MAX_TRACK_POINTS) in OLD : True    ← ❌ FALSE PASS
paren (1-1000)              in OLD : False   ← ✅ 正確地 fail
```

原因就係佢講嘅：`MAX_TRACK_POINTS_PER_REQUEST = 10000`，個舊 description 講
「totalling 10000 points」，而 `"1000" in "10000"` 係 `True`。
**一條 assert `str(MAX_TRACK_POINTS) in description` 嘅測試，會對住一個完全冇寫範圍嘅
description 照綠。** 改成 `(1-1000)` 連括號之後，同一個對照即刻 `False`。修好咗。✅

## A.7 CUI-0025 評分

| 維度 | 得分 | 備注 |
|---|---|---|
| 正確性 | 24/25 | 五個決定全部企得住；S-061 −1 |
| 安全性 | 20/20 | 收窄攻擊面 |
| 可維護性 | 18/20 | S-062 / S-063 各 −1 |
| 測試覆蓋 | 14/15 | 三個 mutant 一對一，質素高；S-065 −1 |
| 性能 | 10/10 | — |
| 代碼風格 | 10/10 | — |
| **小計** | **96/100** | ✅ **pass** |

---

# Section B — CUI-0028（`b06d28e`）test-only

**Commit**：`b06d28e` `test: CUI-0028 | pin the sample key separator with tests that go red`
**改動**：`tests/test_api.py` + ticket file。**production code 零改動**（`git diff --name-only` 核實）。

## B.1 三條新測試真係有牙 —— ✅ 我自己重做兩個 mutation

```
===== separator -> "0" =====
FAILED test_the_sample_key_separator_cannot_occur_in_a_position
FAILED test_a_batch_of_variable_length_ids_thins_each_track_independently[3]
FAILED ... [5] [7] [10] [21] [37]
7 failed, 1 passed

===== separator -> "" =====   （同上，7 failed）
```

**全部係 `AssertionError`，唔係 collection error** ——
即係「睇落紅其實壞咗」嗰個陷阱（CLAUDE.md §6）冇踩中。✅
每一個 parametrized `points` 都紅，唔係得一兩個矇中。✅

> ⚠️ 做呢個 mutation 嗰陣要小心：`SAMPLE_KEY_SEPARATOR = "0"` 同 `= ":"` **byte 長度一樣**，
> 如果喺同一秒內改檔再 import，Python 會用返 stale `__pycache__`（pyc 失效靠 mtime 秒級 + size），
> 結果係「量咗個冇 mutate 過嘅常數」。我第一次就係咁中招，加 `-B` + 清 `__pycache__` 之後先有真數。
> **呢個極可能就係 W-029 嗰個假量度嘅成因。**

## B.2 第三條測試「量 bound parameter 而唔係量 mapping」—— ✅ 做法正確

**我同意呢個做法，而且理由比 commit message 寫得更強**：

`tracks()` 回嘅 mapping 係 `{activity_id: rows}`，**keyed by id**。
重複 id 摺唔摺，個 mapping 都係一樣 —— 所以個 mapping 根本**冇能力**觀測 dedupe。
Dedupe 真正買到嘅係 bound parameter 數量，而呢樣嘢**有一個真實嘅硬天花板**
（SQLite bound parameter ceiling，同一個檔案已經有
`test_the_widest_batch_stays_under_sqlites_bound_parameter_ceiling` 守住）。
**即係話呢條測試量嘅唔係一個抽象指標，而係一個會 raise 嘅資源。** ✅

我自己重做咗呢個 mutation（刪走 `dict.fromkeys`）：

```
1 failed, 401 passed in 48.36s
FAILED test_duplicate_ids_in_a_batch_collapse_to_one_entry
AssertionError: a repeated id is paying for itself again
assert [27, 146] == [9, 50]
```

**`[27, 146]` vs `[9, 50]`，同聲稱一字不差，而且真係全套測試入面唯一一條紅。** ✅

唯一小保留：`assert repeated_cost == distinct_cost` 係對住 exact parameter count 嘅
精確相等，query shape 一改就要跟住改。但隔離兩行嘅
`test_the_widest_batch_stays_under_sqlites_bound_parameter_ceiling` 用同一種寫法，
**同 codebase 既有做法一致**，唔另外開 finding。

## B.3 ⚠️ Lane 聲稱「票上原本嘅重現形狀唔會撞」—— **呢個聲稱係錯嘅 → W-029**

呢個係本輪唯一一個我重做唔到、而且做出**相反結果**嘅聲稱，詳見 **§8 W-029**。

## B.4 DoD 第 4 項 —— ✅ 確認係唯一未做嗰項

| # | DoD | 狀態 | 我嘅核實 |
|---|---|---|---|
| 1 | 加測試，`"0"` / `""` 會紅 | ✅ | 兩個 mutation 我自己重做，7 failed |
| 2 | 加測試，覆蓋變長 id batch | ✅ | `COLLIDING_IDS = ("1","01","5","05","50","500")`，寬度 1/2/3 |
| 3 | 兩條新測試經 mutation 驗過 | ✅ | 同 #1 |
| 4 | `SAMPLE_KEY_SEPARATOR` docstring 指返測試名 | ❌ **未做** | `src/api/service.py:195-215` 我逐行讀過，只提 `_sample_key`，**冇任何測試名** |
| 5 | `pytest` 全綠、ruff clean | ✅ | 402 passed、ruff 全綠 |

**第 4 項確實係唯一未做嘅一項，而且 lane 嘅理由成立**：
`git diff --name-only 74f0de3..HEAD` 顯示 `src/api/service.py` **完全冇出現**，
即係 lane 真係守住咗「唔掂 production code」嘅 scope 限制。
**申報咗、冇偷做 —— 呢點係啱嘅做法**，但 DoD 未剔齊，CUI-0028 唔可以標 completed（見 S-067）。

## B.5 CUI-0028 評分

| 維度 | 得分 | 備注 |
|---|---|---|
| 正確性 | 25/25 | test-only，零行為風險 |
| 安全性 | 20/20 | — |
| 可維護性 | 14/20 | **W-029 −5**、S-067 −1 |
| 測試覆蓋 | 15/15 | 三條測試質素高，全部 mutation-proven |
| 性能 | 10/10 | — |
| 代碼風格 | 10/10 | — |
| **小計** | **94/100** | ⚠️ **warn**（因為有 Warning） |

---

# Section C — CUI-0032（`b16a317`）dev extra

## C.1 Vercel 前提 —— ✅ 兩條路我自己重驗

**唔係照抄 CLAUDE.md，我自己開檔睇：**

```
requirements.txt:
  # Vercel installs this for the api/ Python function. ...
  .                          ← bare dot，冇 [dev]

scripts/vercel-build.sh:
  18: VENV="${TMPDIR:-/tmp}/run365-build-venv"     ← checkout 之外
  23: uv pip install --quiet "."                   ← bare dot
  25: python3 -m pip install ... "."               ← bare dot

vercel.json:
  "excludeFiles": "{data/raw,frontend,legacy,docs,tests,scripts,.github,.venv-build}/**"
```

**三層防護，全部確認。** `pytest-cov` 入 `[dev]` **零機會**入到 serverless function。✅
（順帶：`requirements.txt` 個 comment 自己都寫明咗呢件事，可讀性好。）

## C.2 依賴安全 / License —— ✅ 自己重跑

我喺 scratchpad 開咗個獨立 venv 裝 `pip-audit 2.10.1`：

```
$ pip-audit -r newdeps.txt   (pytest-cov==7.1.0, coverage==7.16.1, pluggy==1.6.0)
No known vulnerabilities found
```

License（由 installed metadata 讀，唔係靠記）：

| 套件 | 版本 | License | 來源 |
|---|---|---|---|
| `pytest-cov` | 7.1.0 | **MIT** | classifier `License :: OSI Approved :: MIT License`（`License` 欄位係空，要讀 classifier） |
| `coverage` | 7.16.1 | **Apache-2.0** | metadata `License` |
| `pluggy` | 1.6.0 | **MIT** | metadata `License`（本來就係 pytest 嘅依賴，唔算真新增） |

**零 GPL、零 CVE。** ✅ 唯一真正新增嘅傳遞依賴係 `coverage`（Apache-2.0）。

## C.3 `[tool.coverage.run] source = ["src"]`、刻意唔加 `omit` / `fail_under` —— ✅ 我同意

**聲稱嘅效果我實測過**：

```
$ pytest tests/test_activities_metrics.py -q --cov       → TOTAL 1632 statements
$ pytest tests/test_activities_metrics.py -q --cov=src   → TOTAL 1632 statements
```

**兩者分母相同 ✅** —— 呢個正正係加呢三行想解決嘅問題（兩個 agent 各自量到唔同分母），
而且真係解決咗。

**唔加 `omit` / `branch` / `fail_under` 嘅理由，我逐個裁決：**

| 決定 | 理由 | 我嘅判斷 |
|---|---|---|
| 唔加 `omit` | 會令 `.proj-docs/` 已記錄嘅 coverage 數字同新量嘅對唔上 | ✅ **同意**。而且 `omit` 每一條 path 本身都要有理由，唔應該喺一張「補依賴」嘅票入面順手加 |
| 唔加 `branch = true` | 同上 | ✅ **同意**。branch coverage 資訊量確實更高，但**靜靜哋開會令每一個歷史數字失效**，唔應該喺呢張票做。`pyproject.toml` 個 comment 已經寫低咗呢個 deferral，下一個人唔使重新諗一次 —— 做得好 |
| 唔加 `fail_under` / 唔加 CI coverage step | 「門檻係另一個會 drift 嘅寫死數字（CUI-0024 / CUI-0031 嘅教訓）」 | ✅ **同意**，而且同票上傾向一致。我核實過 `.github/workflows/pages.yml` 本票**完全冇改** |

> 補充：本 review 流程自己有個「coverage ≥ 80%」嘅 hard gate（實測 95%），
> 呢個係 **review 流程嘅 gate**，唔係 CI 嘅 gate，同上面嘅決定唔衝突。

**一個順帶發現**：`source = ["src"]` 令 `api/graphql.py`（Vercel 入口，喺 repo root 唔喺 `src/`）
永久唔入分母 —— 見 **S-066**。

## C.4 CUI-0032 評分

| 維度 | 得分 | 備注 |
|---|---|---|
| 正確性 | 25/25 | — |
| 安全性 | 20/20 | 零 CVE、零 GPL、Vercel 三層核實 |
| 可維護性 | 20/20 | 三個決定都寫低咗理由，pyproject comment 交代得清楚 |
| 測試覆蓋 | 14/15 | S-066 −1 |
| 性能 | 10/10 | — |
| 代碼風格 | 10/10 | — |
| **小計** | **99/100** | ✅ **pass** |

---

# Section D — S-053（`4bd28b9`）

## D.1 措辭改動本身 ✅

`LIST_ROWS_NOTE` 拆成兩半：

| 常數 | 內容 | 邊個 field 帶 |
|---|---|---|
| `LIST_ROWS_NOTE` | 總 budget（4000 rows，`counted across every field in it that opens one`） | **五個**：`activities` / `weight` / `weather` / `warnings` / `year` |
| `PAGE_WINDOW_NOTE` | `limit` (1-1000) + `offset` + **「charged on `limit`」** | **四個**（`year` 冇） |

`year` 而家只帶第一句 —— **啱**，因為 `year` 冇 `limit` argument（我 parse SDL 核實
`"limit" not in year.args`）。

## D.2 「counted across every list field」→「every field in it that opens one」✅ 佢改得啱

`year` **付呢個 budget**（一個 `DEFAULT_PAGE_SIZE` page）但**唔係 list field**。
舊寫法「every list field」**少數咗佢自己**，即係話個句子涵蓋範圍**漏咗引發呢個 finding 嗰個 case**。
新寫法「every field in it that opens one（a page）」把 `year` 包返入去。
**呢個唔係換個講法，係修正一個事實錯誤。** ✅

## D.3 兩條 test 真係咁 work —— ✅ 我逐條讀過同核實

**Test 1：`year` 唔可以提 `limit`**

```python
assert "limit" not in year.args, "this test is stale: `year` grew a limit argument"
assert "`limit`" not in (year.description or "")
```

第一行係 **staleness guard** —— 如果將來 `year` 真係長出 `limit` argument，
呢條 test 唔會靜靜哋繼續綠住守一個已經唔成立嘅前提，佢會紅並且講明「this test is stale」。
**呢個做法值得表揚**，好過淨係 assert description。✅

**Test 2：五個 field 都要仍然講到 budget**

```python
@pytest.mark.parametrize("field", LIST_FIELDS + ("year",))
def test_every_field_that_spends_the_row_budget_says_so(field):
    assert str(MAX_LIST_ROWS_PER_REQUEST) in _field_descriptions()[field]
```

**呢條就係防止「修正退化成喺 `year` 刪走句嘢」嗰條。** 邏輯上成立：
如果有人為咗令 Test 1 綠而直接刪走 `year` 個 budget 句子，Test 2 嘅 `year` 參數即刻紅。
**兩條測試互相咬住，冇一條可以單獨用「刪嘢」滿足。** ✅

我實測 `_field_descriptions()` 對五個 field 都攞到非空 description，
`'4000'` 喺每個入面各出現 **1 次**（唔係 0 次，即係唔 vacuous）。✅

但**第二條測試用嘅係 bare `str(...)` substring** —— 即係 §A.6 Lane D 喺上一個 commit
啱啱修好嗰種形狀。今日冇 false pass，但係同一個 lane 喺下一個 commit 又寫返一次。→ **S-065**

## D.4 S-053 評分

| 維度 | 得分 | 備注 |
|---|---|---|
| 正確性 | 25/25 | 措辭修正，而且順手修咗一個事實錯誤 |
| 安全性 | 20/20 | — |
| 可維護性 | 20/20 | 常數拆分乾淨，兩個 docstring 都交代咗點解要拆 |
| 測試覆蓋 | 14/15 | S-065 −1 |
| 性能 | 10/10 | — |
| 代碼風格 | 10/10 | — |
| **小計** | **99/100** | ✅ **pass** |

---

# Section E — S-058 / S-059（`3b5c0d2` / `36c7cf4`）數字更正

## E.1 S-058 抽驗 —— 所有數字我自己重量，**核心數字全中**

**方法**：每個 document 一個 fresh process（`.venv/bin/python`，逐個 subprocess），
5 次 run，statement 數用 SQLAlchemy `before_cursor_execute` event 數。

| 聲稱 | 我實測 | 判定 |
|---|---|---|
| 90 aliased `activity(id:)`，真 id：**91.6–95.9 ms** | min 91.3 / median 94.2 ms（5 runs） | ✅ **中** |
| 真 id：**180 statements** | **180** | ✅ **一字不差** |
| Miss path：**48–50 ms** | min 47.3 / median 48.7 ms | ✅ **中** |
| Miss path：**90 statements**、0 rows | **90** | ✅ **一字不差** |
| alias 數取決於 selection：`{ id }` **90** | **90** | ✅ |
| 3 scalars **76** | **76** | ✅ |
| full `ActivityFields` **30** | **30** | ✅ |
| rows 唔係 90 而係 **164**（90 + **74** warning links） | `SELECT COUNT(*) FROM activity_warnings WHERE activity_id IN (首 90 個)` → **74**。90 + 74 = **164** | ✅ **一字不差** |
| ceiling **810**（8 warnings 一日） | `MAX(COUNT(*) GROUP BY activity_id)` → **8**；90 + 90×8 = **810** | ✅ **一字不差** |

**S-058 嘅每一個數字我都重現到。** 上一輪講「~0.05 s / 300x 係量錯 path」——
我確認咗：miss path 48.7 ms、90 statements（一半），hit path 94.2 ms、180 statements。
**`selectinload` 喺 miss 嗰陣唔 fire，所以 statement 數啱啱一半。** 佢講嘅機制完全正確。✅

⚠️ 兩個措辭問題見 **S-063**（「At that widest」綁錯 document）同 **S-064**（dispatch 歸因）。

## E.2 S-059 —— ✅ 佢改得啱，而且講法比舊嗰個準確

**`meta` 表真係兩行，我自己 query 真 DB：**

```
meta rows: 2
meta: [('year', '2021'), ('generated_at', '2026-09-16T11:08:55')]
```

**「one-row export header」究竟描述緊乜？** 佢嘅判斷係「描述緊 GraphQL object 唔係個 read」——
**呢個講法啱**，而且我核實咗機制：`service.meta` 用 `select(models.Meta)` 讀**成張表**
（key/value 兩行），然後摺成**一個** GraphQL `Meta` object。
所以「one row」講嘅係 output shape，而個 bullet 嘅上文下理係**數 materialised rows**
（緊接住就係「332 rows rather than 166」嘅算式）。**喺呢個 context 入面「one row」就係錯。**

佢改成「the export header -- two rows, not one: the table is key/value and holds
`year` and `generated_at`, which `service.meta` reads whole with `select(models.Meta)`
and folds into one object」—— **把兩件事分開講清楚，係嚴格嘅改善。** ✅

佢個附帶論點我亦都認同：呢個 bullet 係「後來新 field 攞嚟類比」嘅 exhaustive list，
「one row」會令人以為個 test 係「一行」，但真正個 test 係「fixed（唔隨 window 變闊）」。
**呢個區別係真嘅，而且係呢個 docstring 嘅全部重點。**

其餘 S-059 數字抽驗：

| 聲稱 | 我實測 | 判定 |
|---|---|---|
| `meta { year }` widest = **166** | **166** | ✅ |
| 加多一個 subfield 跌到 **142** | `meta { year generatedAt }` → **142** | ✅ **一字不差** |
| 166 aliased `meta` **~47 ms**、one statement per field | median **46.7 ms**、**166 statements** | ✅ |
| 332 aliased counts widest = **332** | **332** | ✅ |

## E.3 S-058 / S-059 評分

| 維度 | 得分 | 備注 |
|---|---|---|
| 正確性 | 25/25 | 數字全部重現到 |
| 安全性 | 20/20 | — |
| 可維護性 | 18/20 | S-063 / S-064 各 −1 |
| 測試覆蓋 | 15/15 | 明文講咗「None of these is asserted; a wall time in CI buys a flaky test」—— 啱 |
| 性能 | 10/10 | — |
| 代碼風格 | 10/10 | — |
| **小計** | **98/100** | ✅ **pass** |

---

# Section F — S-060（`af83fd6`）：**Lane 推翻 main agent briefing，我裁決佢啱**

## F.0 裁決結論（先講）

| 問題 | 我嘅裁決 |
|---|---|
| 佢個方法企唔企得住？ | ✅ **企得住，而且 `mincore` 嗰步唔係多餘，係必要嘅** |
| 「呢啲 document 唔係 I/O bound」啱唔啱？ | ✅ **啱，而且我用第三種方法獨立證實咗** |
| 改為寫「cold process 成本」係咪更準確？ | ✅ **係，而且係大幅更準確** |
| 上一輪 S-060 嘅 premise 錯咗？ | ✅ **錯咗。本報告正式記錄更正（見 §F.5）** |

**Lane 冇照 briefing 做係啱嘅。** 佢冇無視 briefing —— 佢去驗咗個 briefing 嘅前提，
發現前提唔成立，然後**記錄實測結果而唔係記錄被交代嘅結論**。呢個係正確嘅專業判斷。

## F.1 方法：`mincore` 核實 resident pages 呢步夠唔夠？—— ✅ 夠，而且係必要

**`posix_fadvise(POSIX_FADV_DONTNEED)` 係一個 *advice*，唔係一個保證。**
佢只會丟走 clean、冇其他人 reference 住嘅 page；有 dirty page、有其他 fd map 住、
或者 kernel 唔想丟嘅時候，佢可以**完全唔做嘢而且唔報錯**。

**即係話「我 call 咗 DONTNEED」同「個 cache 真係空咗」係兩件唔同嘅事**，
而中間嗰道 gap 正正係「一個永遠綠嘅測試」嗰種 gap。
**加 `mincore` 就係把「我相信」變成「我核實」** —— 呢一步唔係 over-engineering，
係令呢個量度由「聲稱」升級成「證據」嘅唯一一步。**呢個做得非常好。** ✅

**我自己重做咗成個 method**（`posix_fadvise` + `mmap` + `libc.mincore`，
每個 field 一個 fresh process）：

```
  [mincore] 0 of 2865 pages resident before the run
activitiesCount  n=332 COLD  import=403.3ms  92.9 ms  stmts=332
  [mincore] 0 of 2865 pages resident before the run
weightCount      n=332 COLD  import=404.3ms  84.4 ms  stmts=332
  [mincore] 0 of 2865 pages resident before the run
weatherCount     n=332 COLD  import=413.7ms  90.5 ms  stmts=332
  [mincore] 0 of 2865 pages resident before the run
warningsCount    n=332 COLD  import=405.0ms  88.8 ms  stmts=332
```

**`0 of 2865` —— 同佢報嘅 `0 / 2,865` 一模一樣。** 我另外由 `PRAGMA page_count` 獨立確認
個 DB 係 **2865 × 4096 = 11.2 MiB**，即係佢講嘅「11 MB file」同 `2,865 pages` 都對得上。✅

## F.2 「唔係 I/O bound」—— ✅ 我用三個獨立角度證實

**角度 1：cold vs warm（重做佢個實驗）**

| field | 我 COLD（0/2865 resident） | 我 WARM first | cold 貴幾多 |
|---|---|---|---|
| `activitiesCount` | 92.9 ms | 89.3 ms | **+4.0%** |
| `weightCount` | 84.4 ms | 81.8 ms | **+3.2%** |
| `weatherCount` | 90.5 ms | 86.0 ms | **+5.2%** |
| `warningsCount` | 88.8 ms | 82.1 ms | **+8.2%** |

**清空 page cache 只貴 3–8%，唔係貴一倍。** 佢報 1.7%，我報 3–8% ——
數字有差異（共用 runner 嘅噪音），但**結論方向完全一致，而且差距遠細過「兩倍」**。✅

**角度 2：直接數佢掂幾多頁（呢個係我加嘅，佢冇做）**

我 drop 完 cache 之後行一次 332 aliased `activitiesCount`，再用 `mincore` 數 resident：

```
332x activitiesCount: 85.2 ms, resident pages 0 -> 10 of 2865 (40 KiB of an 11.2 MiB file)
```

**成個 query 由頭到尾只掂咗 10 版、40 KiB。** 一個 11.2 MiB 檔案入面掂 0.35%。
**呢個係「唔係 I/O bound」最直接嘅證明** —— 佢寫「a count touches a handful of pages
of an 11 MB file」，我實測係 **10 版**，「handful」呢個講法準確。✅

**角度 3：scaling**（見 S-064）——
時間同 alias 數線性，intercept ≈ 2 ms，即係話**冇乜固定成本**。

**三個角度指向同一個結論：呢啲 document 唔係 I/O bound。Lane 嘅結論正確。** ✅

## F.3 「151 ms / 99× 唔係 cold-cache 數字」—— ✅ 成立

我嘅數據直接支持：真·cold cache 只貴 3–8%，**冇可能解釋 83 → 151 ms（+82%）**。
佢講「冇 cold-cache 解釋」而唔係「我知道係乜」—— **呢個係啱嘅講法，
佢冇為咗補一個完整故事而作一個機制出嚟。** 呢種克制值得記低。✅

## F.4 改寫成「cold process 成本」係咪更準確？—— ✅ 係，大幅更準確

| | 聲稱 | 我實測 |
|---|---|---|
| import 成本 | ~330 ms | **391–426 ms**（8 次 fresh process，穩定） |
| 首次 execution 貴 | ~7% | **0%–21%**（視乎 document） |

兩個數我都量到**更高 / 更闊**（機器唔同），但**個 framing 完全正確**：

- import ~400 ms **遠大於** query 嘅 ~85 ms —— cold process 真係先係貴嗰樣 ✅
- 佢係 **per-invocation**，唔會隨 alias 數乘大 —— 所以「呢個 budget 唔 bound 佢」啱 ✅

**「a per-invocation cost the whole function pays, not something this budget bounds
or that aliasing multiplies」呢句我核實過，完全準確。** ✅

小保留：「~330 ms」同「some 7%」兩個數係全段入面**唯一冇講量度條件**嘅數
（其餘每個數都寫明咗 method）。我量到 391–426 ms / 0–21%。
唔算錯（機器差異），但同呢一段其餘部分嘅嚴謹度唔一致 —— 併入 **S-064** 講。

## F.5 📌 上一輪 S-060 premise 正式更正（記錄在案）

> **上一輪（`2026-09-16_review_CUI-0027_delta.md` §3 / §S-060）寫過：**
>
> 「呢個 API 行喺 **Vercel function 之上、打一個 SQLite 檔**。Serverless invocation
> 冷啟嗰陣 page cache 就係凍嘅 —— 即係話 **99× 嗰個讀數對 production 嚟講唔係 artefact，
> 反而可能比 180× 更有代表性**。一個讀者攞 180× 去度新 field 嘅預算，可能會樂觀咗一倍。」
>
> 建議方案 A 要求寫入 docstring：「**a first cold pass roughly doubles them, to some 99x**」

**❌ 呢個 premise 錯咗。** 更正如下：

1. **「cold page cache 令讀數大致 double」—— 錯。** 實測 cold（`mincore` 核實 0/2865 resident）
   只貴 **3–8%**。
2. **「99× 對 production 更有代表性」—— 錯。** 99× 嗰個讀數同 page cache 無關；
   佢冇 cold-cache 解釋。
3. **「讀者攞 180× 度預算會樂觀咗一倍」—— 錯。** 180× 對 warm 同 cold 都成立（±8%）。
4. **但係「serverless 冷啟有一個真實嘅額外成本」呢個直覺方向係啱嘅** ——
   只不過嗰個成本**唔喺 page cache，喺 process**（~400 ms import），
   而且**唔受呢個 budget 管、唔會被 aliasing 乘大**。

**上一輪報告嘅錯誤根源**：由「Vercel + SQLite ⇒ cold page cache 係常態」推到
「所以 cold 讀數 ≈ 2× warm」，**中間冇量過**，直接把一個 unexplained 嘅 151 ms
歸因咗俾 cold cache。**呢個正正係上一輪自己 flag S-058 嗰種錯誤
（「個數係真嘅，但量咗一條冇 client 行嘅 path」）—— 只不過今次發生喺 reviewer 身上。**

**→ Lane 唔照 briefing 做，係啱嘅。呢個係本輪最值得表揚嘅一件事。**

## F.6 S-060 評分

| 維度 | 得分 | 備注 |
|---|---|---|
| 正確性 | 25/25 | 三個角度獨立證實 |
| 安全性 | 20/20 | — |
| 可維護性 | 19/20 | S-064 −1（dispatch 歸因 + 兩個冇量度條件嘅數） |
| 測試覆蓋 | 15/15 | 明文唔 assert wall time，啱 |
| 性能 | 10/10 | — |
| 代碼風格 | 10/10 | — |
| **小計** | **99/100** | ✅ **pass** |

---

# 7. 橫向檢查

## 7.1 測試數 —— ✅ 加埋啱數，冇 test 被刪

**Python 382 → 402（+20），我逐條對數：**

| 來源 | 新測試 | 展開後 |
|---|---|---|
| CUI-0025 | `test_track_points_of_zero_is_refused...` | 1 |
| | `test_the_sdl_states_the_range_track_points_must_fall_in` | 1 |
| | `test_the_sdl_states_the_range_limit_must_fall_in` × 4 params | 4 |
| S-053 | `test_the_year_description_does_not_mention_a_limit...` | 1 |
| | `test_every_field_that_spends_the_row_budget_says_so` × 5 params | 5 |
| CUI-0028 | `test_the_sample_key_separator_cannot_occur_in_a_position` | 1 |
| | `test_a_batch_of_variable_length_ids_thins_each_track_independently` × 6 params | 6 |
| | `test_duplicate_ids_in_a_batch_collapse_to_one_entry` | 1 |
| | **合計** | **20** |

**382 + 20 = 402 ✅ 完全對得上，冇一條係「多咗出嚟」。**

**冇 test 被刪 ✅**：

```
$ comm -23 <(git show 74f0de3:tests/test_api.py | grep -oE "^def test_[a-z0-9_]+" | sort) \
           <(grep -oE "^def test_[a-z0-9_]+" tests/test_api.py | sort)
(空)
```

**舊 commit 有而新 commit 冇嘅 test function：零個。**
test function 數 89 → 97（+8，同上表三組 3+2+3 一致）。

**前端 126 → 131（+5）✅**：`source.test.ts` 嘅 `it(` 由 **5 → 10**，啱啱 +5，
`Test Files 22 passed (22)` 檔案數不變。

**各 commit message 報嘅數亦都自洽**（parallel lane 各自嘅 baseline）：
CUI-0025 報 388 = 382+6 ✅；CUI-0028 報 390 = 382+8 ✅；S-053 報 394 = 388+6 ✅。

## 7.2 同類 substring assertion 掃描 —— 見 S-065

`grep` 全 `tests/test_api.py` 嘅 `str(MAX_*) in ...`，共 **20 處**。分兩類：

- **18 處係對住 error message**（`gql_errors(...)` / `body["errors"]`）。
  Error message 短、每句得一個數，false-pass 風險低。我抽驗咗兩個最有風險嘅
  （`test_limit_above_max_page_size_is_rejected` 送 `limit: 1001`，
  `test_track_points_above_the_maximum_are_rejected` 送 `points: 1001`）——
  兩句 message 都係 `... between 1 and 1000, got 1001`，`"1000"` 唔會被 `"1001"` 包住。✅
- **1 處係對住 SDL description**：`test_every_field_that_spends_the_row_budget_says_so`
  嘅 `str(MAX_LIST_ROWS_PER_REQUEST) in _field_descriptions()[field]` → **S-065**。
- **1 處係新加嘅 `(1-...)` 形式**（已經係修正後嘅寫法）✅

**今日冇第二個 live false pass**（我實測五個 description 入面 `'4000'` 各出現剛好 1 次）。

## 7.3 SDL 改動只係 description ✅

```
$ git diff 74f0de3..HEAD -- frontend/schema.graphql | grep -cE "^[+-]\s+\w+\(.*\):|^[+-]type |^[+-]\s+\w+: "
0
```

**零行 type / field / argument / nullability 改動。** 全部 12 行改動都喺 `"""..."""`
description block 入面。`run365-schema --check` 綠，即係 SDL 同 Python 源頭同步。✅

**呢點好重要**：CUI-0025 係 breaking change，但 breaking 嘅係 **static mode 嘅 TS 函數**，
**唔係 GraphQL contract** —— api mode 嘅 wire format 一個字都冇郁。

## 7.4 Scope / secret / 絕對路徑 / model identifier ✅

| 檢查 | 結果 |
|---|---|
| Secret / API key / token / password | **零命中** |
| 絕對路徑（`/home/`、`/Users/`、`C:\`） | **零命中** |
| Model identifier（`claude-*` / `gpt-*` / `gemini-*`） | **零命中** |
| 改咗嘅檔案 | 9 個，全部喺三張 ticket 嘅預期範圍內 |
| `src/api/service.py` | **完全冇改** ✅（符合 CUI-0028 lane 嘅 scope 限制） |
| `.github/workflows/pages.yml` | **完全冇改** ✅（符合 CUI-0032 決定） |
| `legacy/` | **完全冇改** ✅ |

Commit message 入面有 `Co-Authored-By: Claude <noreply@anthropic.com>` 同
`Claude-Session:` URL —— 呢個係本 repo 既有慣例（歷史 commit 一致），唔係問題。

## 7.5 用戶已拍板嘅三個決定 —— ✅ 一個都冇被動過

| 已拍板決定 | 核實 |
|---|---|
| **4000**（`MAX_LIST_ROWS_PER_REQUEST`） | `src/api/schema.py:284` 仍然 `= 4000`，本 batch 零改動。S-053 只改咗描述佢嘅句子，**個數字本身冇郁** ✅ |
| **Breaking change**（CUI-0025 static mode 拋錯） | 照做，冇打折扣、冇加 fallback、冇加 deprecation window ✅ |
| **Alias 軸唔處理** | 全 batch 零 rate-limit / 零 alias 上限改動；S-059 / S-060 只係喺 docstring 描述 alias 軸嘅**現有**行為，**冇加任何新 enforcement** ✅ |

**我冇 challenge 呢三個決定，只核實咗佢哋冇被動過。**

---

# 8. 問題清單

## 🔴 Critical

**零。**

---

## 🟡 Warning

### W-029｜CUI-0028：「票上原本嘅重現形狀唔會撞」係一個**假量度**，而且已經寫入永久紀錄

- **位置**：
  - `.tickets/in-progress/0001-0200/CUI-0028.md`「執行備註（Lane E）」
  - `b06d28e` commit message（"No fixture had the shape..."）
  - `tests/test_api.py` → `COLLIDING_IDS` docstring
- **聲稱**：

  > **但票上寫嘅反例形狀重現唔到**：票入面嗰組 id `("1","01","10","2","20","002")`、每條 60 行、
  > `points=7`，**實測**喺 `":"` / `"0"` / `""` 三個 separator 之下輸出**完全一樣**，冇碰撞。

- **我實測（用 repo 自己個 `_write_id_track_db` helper 砌返票上原形狀，
  每個 separator 一個 fresh process、清 `__pycache__`、`-B`）**：

  ```
  ':' points=7  counts= {'1': 7, '01':  7, '10': 7, '2': 7, '20': 7, '002': 7}
  '0' points=7  counts= {'1': 7, '01': 10, '10': 7, '2': 7, '20': 7, '002': 7}   ← 撞
  ''  points=7  counts= {'1': 7, '01': 10, '10': 7, '2': 7, '20': 7, '002': 7}   ← 撞
  '5' points=7  counts= {'1': 7, '01':  7, '10': 7, '2': 7, '20': 7, '002': 7}
  ```

  **票上寫嘅形狀撞得一清二楚**：activity `"01"` 回 **10 行**而唔係 7 行 ——
  **同 QA 喺 ticket 上寫嘅一模一樣**（ticket §「個 invariant 係真嘅（反例）」表：
  `"0"` / `""` 之下 `"01"` 回 10 行）。

- **描述**：Lane E 用「實測」兩個字，推翻咗 QA 一個**正確**嘅 repro，
  然後以呢個為理由換咗 fixture。個結論（「要換 fixture」）碰巧冇造成壞結果，
  但**個理由係假嘅，而且以「實測」身分寫入咗 ticket 呢個 SSoT**。

- **點解係 Warning 而唔係 Suggestion**：
  1. 呢個唔係「數字唔夠準」（S-058 / S-059 / S-060 嗰類），係**一個可以直接證偽嘅陳述**。
  2. 佢**否定咗另一個 agent 一個正確嘅發現**。下一個讀 CUI-0028 嘅人會以為 QA 個 repro 係錯 ——
     而 QA 其實由頭到尾都啱。
  3. **成因本身係一個會再犯嘅方法學陷阱**（見下），而呢個陷阱嘅後果係
     「mutation test 睇落綠，其實冇 mutate 過」—— 即係一個**假 pass**。
     喺一個靠 mutation testing 做質量把關嘅 repo 度，呢個陷阱比嗰句錯話本身更值得記低。

- **我推斷嘅成因（我自己中過同一個招）**：
  `SAMPLE_KEY_SEPARATOR = "0"` 同 `SAMPLE_KEY_SEPARATOR = ":"` **byte 長度完全一樣**。
  CPython 嘅 `.pyc` 失效判斷用「source mtime（秒級）+ source size」。
  喺同一秒內改完檔即刻 import，**兩個條件都睇唔出分別 → 用返 stale `.pyc`**，
  即係量緊一個**根本冇 mutate 過**嘅常數。
  我第一次跑就中招（`"0"` 嗰次印出 `':'`），加 `-B` + 清 `__pycache__` 之後即刻有真結果。
  `""` 長度唔同所以會重新 compile —— 所以佢個 `""` 結果亦都應該係真嘅……
  除非佢兩個 case 用咗同一個 already-imported process。無論成因係邊個，**結果係錯嘅**。

- **影響**：
  - **零行為風險**（test-only，production code 零改動）
  - **測試本身冇問題** —— 我獨立驗過三條新測試真係有牙（§B.1），
    而且新 fixture 其實**比票上嗰個好**（多咗 0 / 1 長度嘅退化 case、長度各異）
  - **問題純粹係紀錄準確性 + 方法學**

- **方案 A（推薦）**：更正三處紀錄，**保留現有 fixture 同測試唔改**。
  - ticket 執行備註改成：「票上嗰組形狀**確實會撞**（已重現：`"01"` 回 10 行），QA 冇錯。
    另建 `COLLIDING_IDS` 係因為佢額外覆蓋 0 / 1 長度嘅退化 case 同更闊嘅寬度分佈。」
  - commit 已經 merge，喺 ticket 加一段「更正」即可，**唔好 rewrite history**
  - `COLLIDING_IDS` docstring 刪走暗示「其他形狀唔會撞」嘅措辭
    （個 worked example 本身係**啱**嘅，見下，唔使改）
  - **Trade-off**：要改三處文字，但保住 ticket 作為 SSoT 嘅可信度。

- **方案 B**：只喺本 review 報告記低更正，ticket 唔郁。
  - **Trade-off**：最平，但 `.tickets/` 就會長期帶住一句假「實測」，
    而呢個 repo 嘅 ticket 係會被後續 agent 直接引用嘅。

- **🎯 推薦：方案 A。** 另外**強烈建議**把個 pycache 陷阱加入 `CLAUDE.md` §6「已知陷阱」：

  > **常數 mutation test** | 改一個同原值**同長度**嘅 module 常數（例：`":"` → `"0"`）之後即刻
  > 再 import，CPython 會用返 stale `.pyc`（失效只睇秒級 mtime + size），
  > 結果係量咗個冇 mutate 過嘅值 —— 一個假 pass。要 `python -B` + 清 `__pycache__`，
  > 或者每次 mutation 開 fresh process 並確認 mutant 真係生效。

- **順帶確認：`COLLIDING_IDS` docstring 嗰個 worked example 係啱嘅** ✅
  佢寫「`position=10` on id `"5"` 同 `position=1` on id `"05"` … `"05"` comes back with
  10 rows instead of 7」。我起初以為錯（呢兩個 position 唔會同時係 sampled position），
  **但我諗漏咗個機制**：個 `IN` 係「**wanted key** 對 **表入面所有 row**」，
  唔係 wanted 對 wanted。實測 `points=7` 之下 `"05"` 真係回
  `[0, 1, 2, 3, 17, 33, 50, 67, 83, 100]` —— **10 行，應該 7 行，一字不差**。
  **呢個 example 完全正確，唔好改佢。**

---

## 🟢 Suggestion

### S-061｜`undefined` 分支嘅「bounded by construction」其實靠兩個獨立常數啱啱相等

- **位置**：`frontend/src/data/static/source.ts` `track()` 嘅註釋；CUI-0025 決定 2
- **描述**：決定 2 嘅理由係「the export caps a track at 600 rows, so "all of it" is bounded
  by construction」。但個 600 係 **`src/cli/export_data.py:27` 嘅 `DEFAULT_POINT_LIMIT`，
  而佢係一個 CLI argument 嘅 default**（`ap.add_argument("--points", type=int, default=DEFAULT_POINT_LIMIT)`）。
  同時 api mode 嗰邊 `frontend/src/data/api/source.ts:20` 有個獨立嘅
  `DEFAULT_TRACK_POINTS = 600`。

  **今日兩個 mode 一致，純粹因為呢兩個 600 相等。** 如果有人行
  `run365-export --points 1200`：api mode 會送 `points: 600` 攞 600 個取樣點，
  static mode 會回**全部 1200 行** —— 即係 CUI-0025 想關嘅嗰種 mode 分歧**原樣返嚟**，
  只不過換咗個入口。
- **影響**：零現存風險（default 冇人改）。純粹係「一個已修好嘅 bug 嘅復發通道」。
- **方案 A**：喺 `track()` 註釋講明呢個依賴（「bounded by `--points`, whose default
  happens to equal `DEFAULT_TRACK_POINTS`」），**零代碼改動**。
  Trade-off：最平，但下一個人仍然要自己記住。
- **方案 B**：static mode 嘅 `undefined` 分支都套 `DEFAULT_TRACK_POINTS`（即兩個 mode
  嘅 default 一樣），咁 `--points` 點改都一致。
  Trade-off：**呢個係第二個 breaking change**，而且用戶已經拍板咗「`undefined` 回全部」，
  **唔應該喺本票做**。
- **🎯 推薦：方案 A**（註釋），如果覺得值得就另開票跟進方案 B。

### S-062｜`MAX_TRACK_POINTS` 喺兩種語言各寫一次，冇測試綁住

- **位置**：`frontend/src/data/static/source.ts:39` `export const MAX_TRACK_POINTS = 1000;`
  vs `src/api/schema.py:31` `MAX_TRACK_POINTS = 1000`
- **描述**：TS 嗰個 docstring 明寫「mirroring `MAX_TRACK_POINTS` in `src/api/schema.py`」，
  但**冇任何嘢令佢哋唔同步時紅**。Python 嗰個一改，TS 靜靜哋唔跟 ——
  而「兩邊逐字同一句 error message」（決定 3）就即刻唔成立。
- **影響**：零現存風險。但 CUI-0025 整個 ticket 嘅賣點就係「一個契約」，
  而呢個契約嘅唯一副本冇 gate。
- **方案 A（推薦）**：加一條前端測試，由 `frontend/schema.graphql`
  （**已經 in-repo、已經由 `run365-schema --check` 保證同步**）parse 返
  `Activity.track` description 入面嘅 `(1-N)`，assert `N === MAX_TRACK_POINTS`。
  Trade-off：要喺前端 parse SDL 文字，但個檔案本來就喺 repo 入面，
  而且 CUI-0025 啱啱把個範圍放咗入去 —— **呢個新增嘅 SDL 文字正好令呢條測試變得可能**。
- **方案 B**：由 codegen 產生一個常數檔。
  Trade-off：最穩陣，但要改 codegen 設定，成本遠高於收益。
- **🎯 推薦：方案 A。**

### S-063｜S-058 docstring 嘅「At that widest」把 91.6–95.9 ms 綁咗落錯嘅 document

- **位置**：`src/api/schema.py:341-345`
- **描述**：

  > ... a wider one costs tokens and buys fewer fields, 76 at three scalars and **30 under the
  > full `ActivityFields`. At that widest**: ~95 ms on the export (91.6-95.9 over five runs) ...

  「At that widest」緊接住「30 under the full `ActivityFields`」，讀者自然理解成
  **30-alias 嗰個 document**。但 91.6–95.9 ms 係 **90-alias `{ id }`** 嘅數。
  我實測 30x full `ActivityFields`：**64.8–65.1 ms，60 statements** —— 明顯係另一組數。
  （`160x` 亦都對得上 90-alias：15000/95 ≈ 158；30-alias 應該係 15000/65 ≈ 230x。）
- **影響**：零行為風險。但 **S-058 本身就係為咗修正「數字綁咗落錯嘅 path」而開嘅** ——
  修正版本身又綁錯咗一次，有少少諷刺。下一段「per-field dispatch over **90** aliases」
  其實已經 disambiguate，但要讀者讀多一段先知自己頭先理解錯。
- **方案 A（推薦）**：「At that widest」改成「At the widest fan-out (90 aliases of `{ id }`)」。
  Trade-off：幾隻字，零風險。
- **方案 B**：連 30-alias 嗰個 65 ms 都補埋落去，兩個數並列。
  Trade-off：資訊更完整，但呢個 bullet 已經好長，而且 30-alias 嘅數唔係論證需要嘅。
- **🎯 推薦：方案 A。**

### S-064｜「一半係 per-field dispatch」呢個歸因，我量唔到支持證據

- **位置**：`src/api/schema.py`，S-058 段（「Roughly half the reading either way is fixed cost,
  per-field dispatch over 90 aliases **rather than anything the database does**」）
  同 S-060 段（「what the 80-odd ms buys is **332 resolver dispatches**」）
- **描述**：我量咗兩條 path 對 alias 數嘅 scaling（每點 fresh process、5 runs 取 median）：

  | aliases | miss（median / stmts） | hit（median / stmts） |
  |---|---|---|
  | 10 | 8.0 ms / 10 | 13.0 ms / 20 |
  | 30 | 18.7 ms / 30 | 32.8 ms / 60 |
  | 45 | 25.9 ms / 45 | 47.8 ms / 90 |
  | 90 | 52.2 ms / 90 | 94.5 ms / 180 |

  **兩條線都係線性，intercept ≈ 2 ms（佔 90-alias 讀數嘅 2%）。**
  即係話「fixed cost」實際上接近零，唔係一半。
  而且 **per-statement 成本兩條 path 幾乎一樣**（miss 0.58 ms/stmt、hit 0.53 ms/stmt）——
  即係話 hit 貴一倍嘅原因就係**佢行多一倍 statement**（180 vs 90），
  而唔係「一半係 dispatch」。

  同一邏輯套落 S-060：332 counts = 85 ms → **0.26 ms/statement**，
  但 `activity(id:)` 係 **0.53–0.58 ms/statement**。
  **如果成本真係由一個共通嘅 resolver dispatch 主導，每個 field 嘅成本應該相近；
  實測差成倍。** 即係話個成本主要跟住「每個 statement 做緊乜」走，唔係 dispatch。
- **影響**：**零** —— 兩段嘅**結論**都唔靠呢個歸因：
  - S-058 要講嘅係「miss 唔係平好多」→ 真正原因係「miss 仍然行 90 條（180 條之中）statement」，
    **更簡單而且啱**
  - S-060 要講嘅係「唔係 I/O bound」→ 已經由「10 / 2865 版」同「cold ≈ warm」證實（§F.2），
    完全唔需要 dispatch 呢個歸因
- **順帶**：同一段入面「~330 ms of imports」同「first execution some 7% above the second」
  係全段**唯一冇寫量度條件**嘅兩個數（其餘每個都寫咗 method）。
  我量到 **391–426 ms** 同 **0–21%**。唔算錯（機器差異），但同呢段嘅嚴謹度唔一致。
- **方案 A（推薦）**：把兩處歸因換成量得到嘅講法 ——
  S-058：「the miss still issues 90 of the 180 statements, which is why it costs about half
  rather than nothing」；S-060：「332 resolver dispatches **and their 332 statements**」。
  再幫嗰兩個數補一句量度條件。
  Trade-off：幾句字，而且令兩段同段落其餘部分嘅嚴謹度一致。
- **方案 B**：真係去量 dispatch 同 statement 各佔幾多（例如加一個唔掂 DB 嘅 resolver 做對照）。
  Trade-off：量得準，但呢兩段嘅結論**根本唔需要**呢個拆分，唔值。
- **🎯 推薦：方案 A。**

### S-065｜S-053 嘅回歸測試用返 Lane D 上一個 commit 啱啱修好嘅 bare-substring 形狀

- **位置**：`tests/test_api.py` `test_every_field_that_spends_the_row_budget_says_so`
- **描述**：`assert str(MAX_LIST_ROWS_PER_REQUEST) in _field_descriptions()[field]`
  —— 即係 `"4000" in description`。**今日冇 false pass**（我實測五個 description
  各含 `'4000'` 剛好 1 次，而且冇第二個數字包住 `4000`）。
  但呢個正正係 `72dfcb5`（上一個 commit）花咗一段 commit message 解釋點解**唔可以咁寫**嘅形狀。
  同一個 lane，隔一個 commit。
- **影響**：零現存風險，係一個**將來**可能靜靜哋失效嘅 gate。
  另外呢條測試守得好鬆：只要 description 入面有「4000」就過，
  就算成句 budget 句子退化成得返一個孤零零嘅數字都照過。
- **方案 A（推薦）**：assert 一個真係辨識到句子嘅片語，例如
  `f"at most {MAX_LIST_ROWS_PER_REQUEST} rows"`。
  Trade-off：同 wording 耦合緊少少，但呢條測試守嘅本來就係 wording。
- **方案 B**：直接 assert `LIST_ROWS_NOTE.strip() in description` ——
  即係「呢五個 field 必須帶住呢個常數」，同來源綁死。
  Trade-off：最緊，但 wording 一改就要改埋測試（其實正正係我哋想要嘅）。
- **🎯 推薦：方案 B**（同來源綁死，最能表達意圖）；方案 A 亦可接受。

### S-066｜`source = ["src"]` 令 Vercel 入口 `api/graphql.py` 永久唔入 coverage 分母

- **位置**：`pyproject.toml` `[tool.coverage.run] source = ["src"]`
- **描述**：`api/graphql.py` 喺 repo root，唔喺 `src/`，所以唔會出現喺 coverage report。
  我核實過佢**有**被測試掂到（`tests/test_api.py:47` `VERCEL_ENTRY = ... / "api" / "graphql.py"`），
  但 **coverage 永遠唔會報佢**。
  而 CLAUDE.md §6 明文記住呢個檔案有兩個好脆嘅約束
  （「唯一被認嘅 file name 係 `api/graphql.py`」、「`app = _build_app()` 必須係 top-level」）——
  **即係話全 repo 最易靜靜哋整爛 production 嘅一個檔案，正正係 coverage 睇唔到嗰個。**
- **影響**：零現存風險（本票之前根本冇 `[tool.coverage.run]`，所以唔係 regression）。
  但呢個 commit 把「唔計佢」由一個偶然變成一個**寫落 config 嘅永久決定**。
- **方案 A（推薦）**：`source = ["src", "api"]`。
  Trade-off：**會令分母由 1632 變大，即係動到 `.proj-docs/` 已記錄嘅數字** ——
  而呢個正正係本票刻意避免嘅事。所以**唔應該喺本票做**，宜另開票。
- **方案 B**：`source` 維持 `["src"]`，但喺個 comment 加一句講明 `api/graphql.py`
  係刻意唔計、同埋點解（佢由 `tests/test_api.py` 嘅 `VERCEL_ENTRY` 守住）。
  Trade-off：零數字變動，純粹令下一個人唔使自己發現。
- **🎯 推薦：方案 B**（本票內），方案 A 另開票。

### S-067｜CUI-0028 DoD 第 4 項未做 —— ticket 唔可以標 completed

- **位置**：`.tickets/in-progress/0001-0200/CUI-0028.md` DoD #4
- **描述**：`SAMPLE_KEY_SEPARATOR` docstring 要指返新測試個名（同 repo 既有做法一致）。
  未做，**而且 lane 嘅理由成立**（要改 `src/api/service.py`，超出 lane scope，
  我核實過佢真係一個字都冇掂）。
  **申報咗、冇偷做 —— 呢個做法係啱嘅**，但件事仍然未完。
- **影響**：CUI-0028 **唔可以由 in-progress 標去 completed**。
  而且 CUI-0028 整張票嘅主旨就係「呢個 invariant 講明自己承重但冇 gate」——
  而家 gate 有咗，**但個 docstring 仍然冇指去嗰個 gate**，等於話下一個人讀 docstring
  仍然唔知邊條測試守住佢。
- **方案 A（推薦）**：開一個細 commit 改 `src/api/service.py` 個 docstring，
  加一句指向 `test_the_sample_key_separator_cannot_occur_in_a_position`
  同 `test_a_batch_of_variable_length_ids_thins_each_track_independently`。
  **順手可以同 W-029 嘅更正一齊做。**
  Trade-off：掂 production 檔案（但只係 docstring，零行為改動，`ruff` 會照跑）。
- **方案 B**：另開一張 follow-up ticket，CUI-0028 先標 completed 並註明 carry-forward。
  Trade-off：多一張票；而且個 DoD 明文寫住呢項，標 completed 會令 DoD 制度貶值。
- **🎯 推薦：方案 A**（同 W-029 一個 batch 做完）。

---

# 9. 修正優先順序

| 順序 | ID | 內容 | 阻唔阻結案 | 估計成本 |
|---|---|---|---|---|
| 1 | **W-029** | 更正 CUI-0028 ticket + `COLLIDING_IDS` docstring 嘅假「實測」聲稱；把 pycache 陷阱加入 `CLAUDE.md` §6 | ⛔ **阻**（`status=warn` 由佢嚟） | 低（純文字） |
| 2 | **S-067** | `SAMPLE_KEY_SEPARATOR` docstring 指返兩條新測試（CUI-0028 DoD #4） | ⛔ **阻 CUI-0028 結案** | 低（docstring） |
| 3 | S-063 | 「At that widest」→「At the widest fan-out (90 aliases of `{ id }`)」 | 唔阻 | 極低 |
| 4 | S-064 | 兩處 dispatch 歸因改成量得到嘅講法；兩個數補量度條件 | 唔阻 | 低 |
| 5 | S-065 | S-053 回歸測試改用 `LIST_ROWS_NOTE` 綁死 | 唔阻 | 低 |
| 6 | S-062 | 加測試綁住兩種語言嘅 `MAX_TRACK_POINTS` | 唔阻 | 中 |
| 7 | S-061 | `track()` 註釋講明 `--points` 依賴 | 唔阻 | 極低 |
| 8 | S-066 | coverage comment 講明 `api/graphql.py` 刻意唔計 | 唔阻 | 極低 |

> **1 + 2 兩項可以合成一個 commit batch**（都係 CUI-0028 尾巴），估計十分鐘內搞掂。
> 做完之後本 batch 應該可以由 88 升到 ~94，轉 `pass`。

**Commit 格式**（依 `sw-ticket-management`）：每個 review item 一個獨立 commit，
`fix: W-029 | 更正 CUI-0028 執行備註嘅假實測聲稱`、
`fix: S-067 | SAMPLE_KEY_SEPARATOR docstring 指返守住佢嘅測試`，如此類推。

---

# 10. ✅ 做得好嘅地方（跨 fix 通用）

1. **🏆 S-060：唔照 briefing 做，而係去驗個 briefing 嘅前提 —— 而且佢啱。**
   Main agent 叫佢寫 cold / warm 兩組數，佢發現「cold page cache 係常態」呢個前提根本唔成立，
   於是記錄實測結果而唔係記錄被交代嘅結論。
   **我三個獨立角度重做（cold vs warm 差 3–8%、成個 query 只掂 10/2865 版、
   時間對 alias 數線性 intercept≈2ms）全部支持佢。**
   上一輪 reviewer（即係我呢個角色）由「Vercel + SQLite ⇒ cold cache 常態」
   直接推到「所以 cold ≈ 2× warm」而**冇量過** —— 呢個先係真正嘅錯誤。
   **一個 agent 肯用證據頂返上級嘅指示，係整條 pipeline 最值錢嘅行為。**

2. **🏆 `mincore` 核實 resident pages —— 呢一步係把「聲稱」變「證據」嘅分水嶺。**
   `posix_fadvise(DONTNEED)` 係 advice，可以完全唔生效而唔報錯。
   加 `mincore` 之後「我 call 咗 drop」就變成「我核實咗 0 / 2,865 版 resident」。
   **我自己重做，數字一模一樣。** 呢種「唔止做，仲要證明自己做到」嘅習慣，
   正正係本 repo 一路喺度建立嘅嘢。

3. **🏆 Lane D 自己捉到一個差啲 ship 咗嘅 false pass，並且喺 commit message 解釋咗成個機制。**
   `str(MAX_TRACK_POINTS) in description` 對住一個**完全冇寫過範圍**嘅 description
   照樣 `True`（因為 `MAX_TRACK_POINTS_PER_REQUEST = 10000` 包住 `1000`）。
   **我重現咗個 false pass，亦確認咗 `(1-1000)` 連括號真係修好。**
   捉到呢種 bug 需要主動去問「我條測試會唔會喺代碼錯嘅時候都綠」——
   呢個問題大部分人唔會問。

4. **每個決定都同時寫低「揀咗乜」同「點解唔揀另外嗰啲」。**
   CUI-0025 四個決定逐個列 trade-off；第五個決定明文標示「票上冇問」；
   CUI-0032 三個 config 決定各有理由（而且理由引返 CUI-0024 / CUI-0031 嘅教訓）。
   **Scope 擴張申報咗就唔係偷做** —— 呢個分別好重要。

5. **Mutation 同 test 一對一，唔靠總數矇。**
   CUI-0025 三個 mutant 各自對應唔同嘅測試（3 紅 / 1 紅 / 1 紅），
   CUI-0028 嘅 dedupe mutant 係全套 402 條入面**唯一**一條紅，
   數字 `[27, 146]` vs `[9, 50]` 我重做到一字不差。
   **「有測試」同「有會紅嘅測試」係兩件事，呢批 commit 全部做到第二種。**

6. **S-053 兩條測試互相咬住，堵死咗「刪嘢就綠」呢條退路。**
   而且 `assert "limit" not in year.args, "this test is stale: ..."` 呢句
   staleness guard 做得好 —— 前提唔成立嗰陣佢會紅並且講明自己過時，
   而唔係靜靜哋繼續守一個已經冇意義嘅斷言。

7. **CUI-0032 唔加 `omit` / `branch` / `fail_under` 嘅克制。**
   加落去嘅話每一個 `.proj-docs/` 已記錄嘅 coverage 數字即刻失效。
   **忍住唔順手做多啲，而且把個 deferral 寫咗落 `pyproject.toml` 個 comment 度,
   等下一個人唔使重新諗一次** —— 呢個判斷好過幾多行 config。

8. **Scope 紀律。** `src/api/service.py` 全 batch 零改動（CUI-0028 lane 明令唔准掂），
   `.github/workflows/pages.yml` 零改動（CUI-0032 決定），
   SDL 零 structural 改動。**三條界線一條都冇越過。**

---

# 11. 整體建議

1. **先做 W-029 + S-067**（合共十分鐘），再 re-review。兩項都係文字 / docstring，零行為風險。
2. **W-029 嘅 pycache 陷阱值得入 `CLAUDE.md` §6。** 呢個 repo 靠 mutation testing 把關，
   而呢個陷阱嘅後果係**假 pass** —— 同 §6 已經有嘅 `PYTHONTZPATH`
   「會寫出永遠綠嘅測試」係同一族。**呢個係本輪最有長期價值嘅產出。**
3. **S-062（兩語言常數綁定）值得另開一張 CUI 票。** CUI-0025 把「一個契約」寫入咗 SDL，
   而家終於有一個 in-repo、已同步嘅來源可以畀前端測試 parse ——
   呢個機會係呢個 commit 自己造出嚟嘅。
4. **一個觀察，唔係 finding**：本輪七個 commit 有五個係純文件 / 措辭，
   而扣分亦都全部落喺文件準確性。呢個唔係浪費 —— 呢個 repo 已經到咗
   「docstring 就係契約」嘅階段，所以文件錯就係契約錯。
   但值得留意嘅係：**當一個 docstring 長到要分四段講一個常數嘅量度史，
   佢本身已經係一個 maintainability 訊號。** `MAX_LIST_ROWS_PER_REQUEST`
   個 docstring 而家超過 80 行。將來或者值得把「量度史」搬去
   `docs/` 或者 `.proj-docs/`，docstring 只留契約。呢個唔急，亦唔係本輪嘅問題。

---

# 12. 驗證用臨時改動聲明

本次 review 為咗獨立驗證，做過以下**臨時**改動，**每一個做完即刻還原**：

| # | 檔案 | 改動 | 用途 | 還原 |
|---|---|---|---|---|
| 1 | `src/api/service.py` | `SAMPLE_KEY_SEPARATOR` → `"0"` / `""` / `"5"` | CUI-0028 mutation（共 6 次） | ✅ |
| 2 | `src/api/service.py` | 刪走 `dict.fromkeys` | dedupe mutation | ✅ |
| 3 | `frontend/src/data/static/source.ts` | guard 還原成 falsiness / falsy-guard / 刪 `Number.isInteger` | CUI-0025 三個 mutation | ✅ |

**最終狀態核實**（喺寫本報告之前做，即三組 mutation 全部還原之後）：

```
$ git status --short
（空）
$ git diff --stat
（空）
$ git diff --cached --stat
（空）
```

**已 track 嘅檔案零改動，冇留低任何臨時改動。**
（之後唯一新增嘅係本報告同 `.proj-docs/index.md` 嘅日期行，兩個都係本次 review 嘅預期產出。）
所有量度 script 寫喺 session scratchpad（`/tmp/claude-0/.../scratchpad/`），
**冇一個檔案寫入 repo**。`src/**/__pycache__` 亦已清乾淨。

> ⛔ 本 reviewer **冇執行任何 git 操作**（無 `merge` / `push` / `commit` / `branch`）。
> Git 操作由 main agent 按下面嘅 receipt 執行。

---

## Handoff receipt

```handoff-receipt
protocol: 1
status: warn
score: 88/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "95%"
next_action: invoke_developer
next_agent: backend-developer
branch: "claude/ai-dev-team-start-05jie2"
context: "Round 2 batch (CUI-0025/0028/0032 + S-053/S-058/S-059/S-060): hard gates 10/10 pass, 0 Critical, 402 Python + 131 frontend tests, coverage 95%. All five CUI-0025 decisions verified sound and mutation-proven; S-060's refusal of the briefing is correct and independently confirmed three ways. One Warning: CUI-0028's 'the ticket's repro shape does not collide' is a false measurement recorded in the ticket, the commit message and a test docstring -- the ticket's shape DOES collide ('01' returns 10 rows instead of 7 under both mutations), likely from a stale __pycache__ when mutating a same-length constant. Fix W-029 (correct the record + add the pycache trap to CLAUDE.md 6) and S-067 (CUI-0028 DoD item 4: SAMPLE_KEY_SEPARATOR docstring must name its tests, needs src/api/service.py which the lane was scoped out of), then re-review. Seven Suggestions do not block."
blockers:
  - "W-029: CUI-0028 ticket + COLLIDING_IDS docstring record a false 'measured' claim that the ticket's own repro shape does not collide; it does. Correct the record (do not change the fixture or the tests -- both are good and mutation-proven) and add the same-length-constant stale-pycache trap to CLAUDE.md section 6."
  - "S-067: CUI-0028 DoD item 4 outstanding -- SAMPLE_KEY_SEPARATOR's docstring in src/api/service.py must point at test_the_sample_key_separator_cannot_occur_in_a_position and test_a_batch_of_variable_length_ids_thins_each_track_independently. CUI-0028 cannot move to completed until this lands."
```
