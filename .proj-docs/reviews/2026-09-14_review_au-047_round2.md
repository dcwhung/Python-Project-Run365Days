# Code Review（第二輪）— AU-047 per-request track budgets

**日期**：2026-09-14
**審閱者**：Code Reviewer（獨立 subagent，未參與實作）
**範圍**：`git diff e2ac336..c42ed6a`（`830d0a0`、`6a040f1`、`c42ed6a`）—— `src/api/schema.py`、`tests/test_api.py`、`frontend/schema.graphql`
**Branch**：`claude/run365days-au047-resolver-8wg7ay`
**上一輪**：[`2026-09-14_review_au-047.md`](2026-09-14_review_au-047.md)（66/100 fail，1 🔴）

---

## 結論

| 項目 | 值 |
|---|---|
| 評分 | **91 / 100**（上一輪 66） |
| 結果 | ✅ **pass**（≥ 90、零 🔴、hard gates 全綠） |
| 🔴 Critical | **0** |
| 🟡 Warning | 1（W-015，非 blocking） |
| 🟢 Suggestion | 5（S-019 ~ S-023） |
| next_action | `merge_develop` |

---

## 一、四個 blocking item 裁決

| Item | 裁決 | 證據 |
|---|---|---|
| **C-001** budget 扣 points 唔扣 round trip | ✅ **closed** | 上一輪原封不動嗰條 flood（27 alias × `activities(limit:365){track(points:1)}`，365 × 600 points 真 DB、真 Flask test client、`before_cursor_execute` 數 SQL）：**130 SQL / 0.11-0.15 s**，三次重複一致。同一份 document 喺 runtime 將 `MAX_TRACK_FIELDS_PER_REQUEST` 改成 `1e9`（即 `830d0a0` 之前嘅行為）：**19,764 SQL / 14.68 s**。約 150× SQL、100× 時間削減。 |
| **W-012** `10000` 理據引用發唔出嘅 document | ✅ **closed** | 新 docstring 每句獨立核實。兩句被否證嘅（`0.37 s`、`batching would not have bought the headroom back`）全 repo `grep` **零命中**。 |
| **W-013** guard 註解理由錯 + 裸 `KeyError` | ✅ **closed** | 9 種 context 形狀重跑，註解同實測逐字對上；新 `RuntimeError` 訊息唔再出現 `track_points_remaining`；fail-closed 一條路都冇被削弱。 |
| **W-014** fail-closed 零測試 | ✅ **closed**（一條 pin 偏軟，見 S-019） | 5 個 runtime mutant 全部被捉；`.get()` fallback 呢個正正係 W-014 描述嘅 regression，被 `test_track_fails_closed_when_the_schema_has_no_budget_extension` 捉實。 |

---

## 二、C-001：窮舉過嘅放大路徑

Production-shaped DB（365 activities × **每一條**都有 600 track points，15.7 MB）、真 `create_app()` test client。

```
== A. C-001 as reported ==
   27 aliases, 9855 pts charged (×3)                    sql=   130   0.11-0.15s errors=1
== B. same document, field cap lifted to 1e9 (pre-830d0a0) ==
   27 aliases, field cap = 1e9                          sql= 19764  14.68s errors=0
== C. other alias widths ==
   1 / 2 / 28 / 52 aliases × activities(365){track(1)}  sql=   130   0.10-0.16s errors=1
   53 / 100 / 200 aliases                               sql=     0   0.01s  (MaxTokensLimiter)
== D. widen the page window instead ==
   activities(limit: 64 / 65 / 365 / 1000){track(1)}    sql=   130   0.10-0.12s
== E. offset paging, 30 aliases × activities(1000, offset:n)  sql=   130   0.12s
== F. 3 aliased track fields inside 每個 activity        sql=   130   0.09s
== G. nested: N × year{personalBests ×5 track}
   1/2/5/10 aliases                                     sql=  13/26/65/130
   13 aliases (= 65 tracks)                             sql=   167   0.24s errors=1  ← cap 喺 65 咬住
== H. N × activity(id: "rN"){track(1)}                   40 → sql=160；64+ → token limit 擋
== I. 52 × activities(limit:1){track(1)}（全部服務，冇 error）sql=   208   0.17s
== K. fragment + inline fragment + __typename, 3 tracks/activity  sql=   130   0.11s
== J. 最貴嘅「被服務」請求
   activities(64){track(156), 8 欄}                     sql=   130   0.50s  1.1 MB
   activities(16){track(600), 8 欄}                     sql=    34   0.39s  1.1 MB
```

**結論**：無論 alias 數、page window 闊度、nesting、fragment、`activity(id:)` 單條乘以 alias、`__typename` 混搭，track 路徑一律收斂到 **≤ 64 個 track field = 128 條 track SQL**。冇任何合法 document 超出 field cap 隱含嘅上限。被拒嘅請求同樣只燒 130 條 / 0.15 s（上一輪被拒係 20,056 / 13 s）——「refused request 一樣燒晒時間」呢個 C-001 第二半亦關閉。

不變量逐條核實：**served fields ≤ 64、charged points ≤ 10,000**，冇任何 execution order 令佢哋失效。

---

## 三、W-012：新理據逐句核實

| docstring 講嘅 | 核實結果 |
|---|---|
| `TrackQuery` 係前端唯一 select `track` 嘅 document | ✅ `queries.ts:133` 係手寫 document 入面唯一一個 `track(`（`src/gql/` 係 codegen 產物） |
| 佢淨係攞一條 activity | ✅ `query Track($id: ID!, $points: Int!) { activity(id: $id) { ... } }` |
| 兩個 call site 都傳 600 | ✅ `ActivityView.tsx:17 TRACK_POINTS = 600`、`api/source.ts:20 DEFAULT_TRACK_POINTS = 600` |
| `YearQuery` 嘅 `personalBests` 五個 field 只 spread `ActivityFields`，而後者冇 `track` | ✅ consumption = 0 |
| 「roughly 16x above real demand」 | ✅ 10000 / 600 = 16.7 |
| 「a round number chosen as a ceiling, *not* derived from what any client needs」 | ✅ 誠實版本 |
| 兩句被否證嘅結論 | ✅ 全 repo `grep` 零命中 |

`MAX_TRACK_FIELDS_PER_REQUEST` docstring 引嘅 `19,764 statements and 13 s` 係上一輪實測值，今輪重現 19,764 / 14.68 s。

---

## 四、W-013：9 種 context 形狀

```
=== schema WITH _TrackBudget ===
  no context_value (None)              rows=None  'NoneType' object is not subscriptable
  plain dict                           rows=10    -
  MappingProxyType                     rows=None  track budget was not seeded for this request, so `track` cannot be served
  plain object (attr, not mapping)     rows=None  'Ctx' object is not subscriptable
  dict pre-seeded with budget=5        rows=10    -      ← extension 覆寫，唔係旁路
  dict pre-seeded with HUGE budget     rows=10    -      ← 同上
  dict subclass that refuses writes    rows=None  read-only
  only points key seeded               rows=10    -      （extension 在場時到不了）
  only fields key seeded               rows=10    -      （同上）

=== schema WITHOUT _TrackBudget (extensions=[]) ===
  plain dict / MappingProxyType        rows=None  track budget was not seeded ...

=== 內部 key 洩漏 ===   全部 False
=== caller 塞值會唔會提高 budget ===
  6 tracks with caller-seeded budget 5 → errors=0；remaining after: fields=58 points=9994
```

註解而家講嘅理由同實測逐字對上；`Raises:` 補齊第二個成因；新 `RuntimeError` 唔洩漏 internal key；fail-closed 冇被削弱。

---

## 五、`_charge_track_field` 扣數次序

寫回**只喺兩個 check 都過之後**先發生 → charge 係 **atomic all-or-nothing**，冇任何 edge case 令 field 扣咗但 points 冇扣（或相反）。

```
10×1000 then 1×1000 (refused) then 1×1   fields_left=54  points_left=0     ← 只有 10 個 field 被扣
1×1 then 10×1000 then 1×1000 (refused)   fields_left=54  points_left=999   ← 被拒嗰個一分唔收
65 cheap tracks then 1 more              fields_left=0   points_left=9936
1 served field                           fields_left=63  points_left=9999
```

不變量成立。但 docstring 一句比代碼講得大 → **S-020**。

---

## 六、W-014 mutation test（runtime patch，**冇改過 repo 任何檔案**）

| Mutant | 紅咗嘅測試 | 結果 |
|---|---|---|
| **M1 `.get(KEY, MAX_...)` fallback**（W-014 指名嘅 regression） | `test_track_fails_closed_when_the_schema_has_no_budget_extension` | **1 failed / 297 passed** |
| M2 完全唔 enforce field cap | alias flood、field cap boundary、SQL count | 3 failed / 295 passed |
| M3 `fields_left < 0` → `< -1`（off-by-one） | 3 條 | 3 failed / 295 passed |
| M4 `on_operation` 唔 seed field key | 10+ 條 | 大面積紅 |
| M5 `MAX_TRACK_FIELDS_PER_REQUEST = 1e9` | 4 條 | 4 failed / 294 passed |

**Developer 主動申報嗰件事屬實，已獨立核實**：`MappingProxyType` 嗰條 pin 喺 M1 之下照樣綠，而綠嘅實際原因係——

```
errors=["'mappingproxy' object does not support item assignment"]
(control: plain dict under the mutant) errors=0
```

即係佢綠係因為之後寫返 read-only mapping 掟 `TypeError`，**唔係**佢註解講嘅「missing key」。判斷：值得保留（釘住一個真實不變量），但要加牙（見 S-019）。W-014 核心已由另一條 pin 真正捉實，故判 closed。

---

## 七、Non-null propagation 靠唔靠得住

砌咗一個 `activities: [Activity!]` **nullable** 嘅 schema 變體：

```
NULLABLE activities, 27 aliases: sql=  182   0.35s  errors=27
NULLABLE activities, 52 aliases: sql=  232   0.60s  errors=52
non-null activities, 27 aliases: sql=  130   0.15s  errors=1
non-null activities, 52 aliases: sql=  130   0.11s  errors=1
```

**對「安全」唔係 load-bearing**：就算將來有人將 `activities` 改成 nullable，field cap 照樣封死 track 部分（仍然 64 field / 128 條 track SQL）；多出嗰 52–102 條係每個 alias 自己嘅 list + warnings query，由 `MaxTokensLimiter` 封頂。最壞由 130 升到 232 條 / 0.60 s，量級冇變。

**但對「測試常數」係 load-bearing**，而呢點冇被釘住 → W-015。

---

## 八、新發現

### 🟡 W-015 — `MAX_SQL_PER_REQUEST = 182` 唔係 per-request 上限，但個名同測試名都咁講

**位置**：`tests/test_api.py`（`MAX_SQL_PER_REQUEST` + `test_no_request_issues_more_statements_than_the_field_cap_allows`）

推導方式本身乾淨（`64 * SQL_PER_TRACK_FIELD + 27 * SQL_PER_LIST_FIELD`，兩個 factor 都由常數嚟，改常數唔會 drift）—— **唔係** magic number。問題係**個名同測試名 claim 咗一個唔成立嘅全域性質**：

- 今日就有合法、完全被服務、零 error 嘅 document 超出佢：`52 × activities(limit: 1) { track(points: 1) }` → **208 SQL**（> 182）。
- 將 `activities` 改成 nullable 之後，同一條 27-alias flood 啱啱好 182，52-alias 去到 **232**。

`182` 只係「27 aliases × 呢個特定形狀」嘅上限（docstring 寫 "a flood of this width" 其實誠實），但常數名同測試名都讀成「任何 request 都唔會超過」。呢個正正係 W-012 同一類缺陷（claim 大過實測支持嘅範圍），只不過今次喺測試碼。

- **方案 A（推薦）**：改名 —— 常數叫 `ALIAS_FLOOD_MAX_SQL`，測試叫 `test_the_alias_flood_issues_no_more_statements_than_the_field_cap_allows`。零行為改動，claim 即刻同實測對齊。
- **方案 B**：保留名，將常數改成真上限 `MAX_TRACK_FIELDS_PER_REQUEST * 2 + MAX_LIST_ALIASES * 2`，再加測試釘住 list alias 嘅 token 上限。更完整，但引入一個要自己維護嘅 `MAX_LIST_ALIASES`（token limit 推唔出乾淨嘅閉式解）。

> ⚠️ 非 blocking。測試嘅保護力係真嘅（M2、M5 兩個 mutant 都令佢紅），只係個名誤導。

### 🟢 S-019 — `test_track_fails_closed_when_the_context_cannot_be_seeded` 過唔到自己個註解

見第六節。加一行就有牙：`assert "not seeded" in result.errors[0].message`。同一標準亦應套落 `test_track_fails_closed_when_the_schema_has_no_budget_extension` —— 兩條都係 bare `assert result.errors`，而 **S-015 喺呢條 branch 上啱啱先為咗同一個理由被修**。

### 🟢 S-020 — 「every `track` field after the one that overran it is refused in constant time」只對 field budget 成立

Field budget sticky（停喺 0，之後每次 `-1 < 0`），但 points budget 唔 sticky（overrun 之後仲剩 999 點，一個 `track(points: 500)` 會被服務）。行為冇問題（atomic charge 係啱嘅設計），句子要收窄。

### 🟢 S-021 — 「64 is the ceiling the points budget already implied … `10000 // 150` is 66」

同一句兩個數對唔上（64 ≠ 66）。建議明寫「66, rounded down to 64」。

### 🟢 S-022 — 新測試攞 field cap 當 `activities(limit:)` 用，cap 一升過 999 就靜靜哋爛

`_cheap_tracks(n)` 用 `n` 做 page limit，而 `MAX_PAGE_SIZE = 1000`。M5 mutant 就係咁撞出嚟（`limit must be between 1 and 1000, got 1000000000`）。而 `MAX_TRACK_FIELDS_PER_REQUEST` docstring **明寫**咗 AU-050 之後要調高呢個數。建議改用 alias 砌 fan-out，或加 `assert MAX_TRACK_FIELDS_PER_REQUEST < MAX_PAGE_SIZE` 明示耦合。

### 🟢 S-023 — （**AU-047 範圍外**）非 track 嘅 `year` fan-out 而家先係最貴嘅合法請求

```
110 × year{trainingLoad{ctl}}  tokens=992   sql= 330   1.58s   errors=0
100 × year{totals{runs}}       tokens=902   sql= 300   1.19s   errors=0
 40 × year{trainingLoad+weekly+dailyDistance}  sql= 120  0.62s  534 KB
```

每個 `year` alias 都重新 materialise 成年 365 條 activity 再行 full stats，track budget 完全睇唔到佢。Post-fix 嘅最壞合法請求由 track 路徑（0.50 s）轉移到呢度（1.58 s）。按 2.2× 真機係數換算約 **3.5 s**，仍然安全喺 15 s envelope 之內，而且係 AU-047 之前就存在、同呢三個 commit 無關。建議另開 ticket（cost-based complexity analysis 或 `year` 專屬 cap）。

---

## 九、✅ 做得好嘅地方

- **修法命中要害**：`MAX_TRACK_FIELDS_PER_REQUEST` 正交於 points budget，唔係將個窿縮細而係封死佢。
- **`_charge_track_field` 嘅 charge 係 atomic**：兩個 check 全過先寫回，被拒嘅 field 一分唔收，兩個 counter 永遠一致。設計上嘅正確選擇，唔係巧合。
- **W-012 改寫係真誠實**：由「引用一份發唔出嘅 document」改成「承認呢個係一個 round number」，係一個肯認錯嘅改法。
- **W-013 唔淨係改註解，仲改埋 error**：`KeyError` → `RuntimeError` + 唔洩漏 internal key，註解留低咗「為咗一個冇咁明顯嘅理由而安全」嘅 reasoning。
- **測試常數全部係推導**且有 docstring 交代出處，逐個核實過：27×365 = 9,855、28×365 = 10,220、document lexes to **515** tokens（docstring 寫 515，實測 515）。
- **`test_the_schema_does_not_batch_operations`** 將「per-request = per-operation」呢個隱含前提釘咗落去（S-016）。
- **`sql_count` fixture 聽 `Engine` class 而唔係 instance**，並喺 `finally` 移除 —— 針對 `create_app` 自己起 engine 嘅正確做法。
- **三個 commit 各自獨立綠、各自獨立 lint 綠**（喺隔離 worktree + 隔離 venv 逐個 checkout 驗過）。

---

## 十、評分

| 維度 | 得分 | 滿分 | 備註 |
|---|---|---|---|
| 正確性 | 24 | 25 | C-001 徹底關閉，counter 語意經實測證實一致；−1：S-020 |
| 安全性 | 20 | 20 | DoS 向量封死；9 種 context 全 fail-closed；無 key 洩漏 |
| 可維護性 | 18 | 20 | docstring 質素高且誠實；−2：W-015 命名誤導、S-021 |
| 測試覆蓋 | 11 | 15 | 7 條新測試、5 個 mutant 全捉、三 commit 獨立綠；−4：S-019、W-015、S-022 |
| 性能 | 10 | 10 | 19,764 → 130 SQL、14.68 s → 0.15 s |
| 代碼風格 | 10 | 10 | ruff / SDL / commit 格式全綠 |
| **總分** | **91** | **100** | 1 × 🟡(−5) + 4 × 🟢(−1)，兩種算法一致 |

---

## 十一、Hard gates（原始輸出）

```
$ .venv/bin/python -m pytest tests -q        → 298 passed in 8.56s
$ .venv/bin/ruff check src tests             → All checks passed!       exit=0
$ .venv/bin/ruff format --check src tests    → 58 files already formatted exit=0
$ .venv/bin/run365-schema --check frontend/schema.graphql → is up to date exit=0
$ cd frontend && npm ci && npm run typecheck → [SUCCESS] Generate outputs, exit=0
```

| Gate | 結果 |
|---|---|
| Lint / Format | ✅ |
| Type check（`tsc -b` + codegen） | ✅ |
| Tests | ✅ 298（baseline 291 + 7） |
| SDL sync | ✅ |
| Coverage ≥ 80% | ⚪ n/a（`pytest-cov` 未安裝，`pyproject.toml` 無 gate） |
| Security scan | ⚪ n/a（零新增 dependency） |
| **No Critical** | ✅ |

Per-commit（隔離 worktree + 隔離 venv）：

```
e2ac336  (baseline)                                     291 passed
830d0a0  fix: AU-047 | bound the round trips...         295 passed   lint clean
6a040f1  docs: AU-047 | correct the budget rationale... 295 passed   lint clean
c42ed6a  test: AU-047 | pin the fail-closed behaviour... 298 passed  lint clean
```

**未跑**：coverage 數字（`pytest-cov` 未安裝）；frontend `lint` / `vitest` / 兩個 build（交俾 QA）；真 Vercel 計時（本機 SQLite，按 2.2× 係數換算並註明）。
