# Delta Re-review — 2026-09-16 — CUI-0029 批次 documentation lane（`ea820aa..7bacce1`）

## 整體 verdict

- **審閱者**：Code Reviewer（獨立重做，唔照收 lane 任何講法）
- **範圍**：`ea820aa..fee1744`（12 條 suggestion 嘅 documentation-only lane）＋ `fee1744..7bacce1`（S-077）
- **上一輪**：86/100 ⚠️ warn（`.proj-docs/reviews/2026-09-16_review_CUI-0029_batch.md`），0 🔴 / 0 🟡 / 13 條 open suggestion
- **本輪**：**97/100 ✅ pass** —— 0 🔴 / 0 🟡 / 3 🟢（S-080 / S-081 / S-082，全部係 `.proj-docs/tickets.md` 入面嘅記錄準確性）
- **Hard gates 8/8 pass**
- 13 條計分 suggestion **全部真正清咗**（12 條 lane + S-077 main agent）；S-079 按 brief 唔計分
- **三條 developer 推翻我上一輪嘅地方：三條全部佢啱、我錯。** 我逐條獨立實測，冇一條係靠講。

---

## Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| Lint (py) | `.venv/bin/ruff check src tests` | ✅ `All checks passed!`（exit 0）|
| Format | `.venv/bin/ruff format --check src tests` | ✅ `58 files already formatted` |
| Lint (fe) | `npm run lint`（`eslint .`）| ✅ exit 0，零輸出 |
| Type check | `npm run typecheck`（codegen + `tsc -b`）| ✅ exit 0 |
| Tests (py) | `.venv/bin/python -m pytest tests -q` | ✅ **418 passed**（51.9 s）|
| Tests (fe) | **`npm test`**（codegen + `vitest run`，**唔係** `npx vitest run`）| ✅ **141 passed / 25 files**（7.97 s）|
| Coverage | `pytest tests --cov=src` | ✅ `api/schema.py` **100%**、`api/service.py` **100%**（99% → 100%）、`api/app.py` 100%、`api/db.py` 100%、**TOTAL 95%**（1652 stmt / 81 miss）|
| SDL 同步 | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ `is up to date` |
| Security scan | 依賴檔案 diff | ✅ n/a-pass —— `package.json` / `package-lock.json` / `requirements.txt` 喺 `ea820aa..7bacce1` **完全冇改**；`pyproject.toml` 只加咗 4 行 `[tool.ruff.lint.isort]` 註釋 + setting，**零新增依賴** |

全部同你手上實測值吻合（418 / 141 / 25 / 兩個 module 100% / TOTAL 95% / ruff clean / SDL up to date）。

---

## 評分結果

| 維度 | 得分 | 滿分 | 備註 |
|---|---|---|---|
| 正確性 | **25** | 25 | `src/` 剝 docstring + comment 後 AST 同 `ea820aa` 只差一個 import node 嘅位置（S-077），零行為改動；418 tests 綠 |
| 安全性 | **20** | 20 | 安全面零改動；S-075 嘅 tripwire 註釋令「rename 會令 traceback 重新洩漏」呢條路更難被剪走 |
| 可維護性 | **17** | 20 | S-080 / S-081 / S-082 各 −1（全部係 `.proj-docs/tickets.md` 記錄準確性）|
| 測試覆蓋 | **15** | 15 | S-073/074/075/078 全清；S-078 令 `api/service.py` 99% → 100%；S-065 把一個假綠 assertion 換成殺得到 mutant 嗰個 |
| 性能 | **10** | 10 | 零 production 改動 |
| 代碼風格 | **10** | 10 | ruff + eslint 全清；S-077 令 isort 分類同現實一致 |
| **總分** | **97** | **100** | |

**結果：✅ pass**（hard gates 全 pass + ≥ 90 + 0 🔴）

**交叉核對**：上輪 86 分，14 條 suggestion 扣咗 14 分（7 舊 + 7 新）。本輪 13 條關閉、S-079 按 brief 剔出計分 → 回復 14 分 = 100，再減 3 條新開 = **97**。兩種算法一致。

---

## 三條「developer 推翻 reviewer」—— 獨立裁決

### 裁決 1：S-074 —— **Lane 完全啱，我上一輪錯**

我上一輪寫「`year` cannot raise a list-row-budget refusal at all」。Lane 話呢句錯。

**實測**（真 Flask app `create_app(data/processed/run365.db)`，真 DB，逐個 document 打）：

| Document | 結果 |
|---|---|
| `{ a: year{year} b: year{year} c: year{year} }`（我上輪嗰個 probe）| ✅ 冇 error —— 3 × 500 = 1500 / 4000 |
| 8 × aliased `year` | ✅ 冇 error —— 啱啱好 4000 |
| **9 × aliased `year`** | ❌ **REFUSED**，`path=['a8']`，`locations=[{line:1,column:123}]` |
| **8 × `activities(limit:500)` + bare `year{year}`** | ❌ **REFUSED**，`path=['year']`，`locations=[{line:1,column:243}]` |

機制核實：`ea820aa` 嘅 `src/api/schema.py:1204`（HEAD 係 `:1226`）確實係 `_charge_list_rows(info, DEFAULT_PAGE_SIZE)`，喺 `year` resolver 入面，`DEFAULT_PAGE_SIZE = 500`，`MAX_LIST_ROWS_PER_REQUEST = 4000`。Lane 引嘅行號對佢自己個 base 嚟講係啱嘅。

**裁決：Lane 啱，我錯。** 我嘅錯法本身就係本 repo 嗰個缺陷類別 —— 由一次 negative observation（3 個 alias 唔 refuse）跳去一個 universal claim（「根本掟唔出」），而冇試過個 budget 邊界。

**再進一步（Lane 都冇講到嗰層）**：GraphQL error `path` 用嘅係 **response key**，所以 9 個 alias 嗰條路 refuse 喺 `path:["a8"]` 而**唔係** `["year"]`。要重現 fixture 模擬嗰個 `path:["year"]`，個 `year` field 必須**無 alias**，即第二條路。**而 lane 改咗落 fixture docstring 嗰句係啱嘅** —— 佢寫 "The API does refuse at `path: [\"year\"]` once earlier fields in the same document have spent the budget"，正正就係第二條路。改得精準。只有 registry 嗰行冇分開兩條路（見 S-081）。

### 裁決 2：S-068 —— **Lane 唔照方案 A 係啱嘅，而且係本輪最強嘅一個判斷**

Lane 唔寫真檔案 size，改為寫「關係 + 重量方法」，理由係我量到 20103、佢量到 22228，同一個數兩次 review 之間已經漂移。

**實測 A —— 佢對 10/10/9 個 provenance 嘅重建，算術上完全吻合**：

```
printf "SEP = ':'\n" | wc -c  →  10
printf "SEP = '0'\n" | wc -c  →  10
printf "SEP = ''\n"  | wc -c  →   9
```

佢寫「呢三個數係一個最小重現模組（得 `SEP = ':'` 一行）嘅 byte size」—— 一個字都唔差。宣告嗰行 `SAMPLE_KEY_SEPARATOR = ":"` = 26 / 26 / 25，亦吻合。

**實測 B —— 決定性**：

| revision | `src/api/service.py` bytes |
|---|---|
| `ea820aa` | **22228** |
| `fee1744` | **22535** |
| `7bacce1` | **22535** |

**Lane 自己嗰個 S-071 commit（`1d901b3`）令個檔案大咗 307 bytes。** 即係話：如果佢照方案 A 寫低 22228，呢個數**喺同一條 lane 入面、喺佢寫低之後三個 commit 就已經係錯嘅**。

**裁決：Lane 完全啱，我方案 A 錯。** 呢個係 W-017 一路斬緊嗰個 pattern 嘅教科書示範，而且佢揀寫嘅嗰個不變量（相等 / 相等 / 少一 byte）喺 22535 之下一樣成立，喺任何 size 都成立。**我上一輪唔應該推薦寫死讀數。**

### 裁決 3：S-078 —— **Lane 完全啱，方案 A 殺唔到 mutant**

**Mutant**：喺 `src/api/service.py` 刪走 `if not wanted:\n    return found`，清晒 `__pycache__`，用 `inspect.getsource` 確認 mutant 真係入咗 loaded module（跟 CLAUDE.md §6 嗰條 stale-`.pyc` 陷阱嘅做法）。

**兩個方向**（獨立 oracle：我自己掛 `before_cursor_execute` listener，唔用 repo fixture）：

```
MUTANT EFFECTIVE (no early return in loaded module): True
Option A assertion  service.tracks(session, [], 7) == {} : True    ← 存活，殺唔到
Lane assertion      no SQL issued                        : False   ← 殺到
statements actually issued:
  (0 params, 'SELECT track_points.activity_id, count(*) AS count_1 FROM track_points')
  (1 param,  'SELECT anon_1.activity_id, anon_1.seq, anon_1.sec, anon_1.lat, ...')
```

repo 入面條真測試亦都紅喺嗰句 parameter assertion（`assert [0, 1] == []`），而上面 `== {}` 嗰句照過。

**裁決：Lane 啱，我方案 A 錯。** 原因同佢講嘅一樣 —— 空 mapping 由上面個 comprehension 出，`== {}` 對有冇 early return 都一樣。

**而且比 Lane 講嘅更嚴重**：第一條走出去嘅 statement 係 **0 個 bound parameter** 嘅 `SELECT activity_id, count(*) FROM track_points` —— 即係成張 `track_points` 表（~134k 行）嘅無條件 grouped COUNT，唔止係「send an empty batch into SQL」。仲會順手出 `SADeprecationWarning: Invoking or_() without arguments is deprecated`。所以嗰個 early return 唔淨係慳成本，佢係擋住一個全表掃描。**S-078 條測試值得留，而且應該保住嗰句 parameter assertion。**

---

## S-077 歸因更正 —— 核實通過，你改得啱，而且我上一輪錯咗兩樣嘢

你話我上一輪寫「the repo already lost a deploy cycle to this shadowing (`fd252a3`)」係講唔準。我逐個 commit 查：

| commit | 時間 | 內容 |
|---|---|---|
| `fd252a3` | 15:40:12 | rename `api/graphql.py` → `api/index.py`「to avoid shadowing graphql-core」。Body 寫 "which **can** crash the function before the app's own error handling runs" —— **假設句，唔係觀察句** |
| `e506531` | 15:43:43 | `index.py` 被 Vercel 拒（"doesn't match any Serverless Functions inside the api directory"）→ 改 `api/graphql_api.py` |
| `12c31fe` | 15:44:26 | make Flask / Strawberry core dependencies。Body 引咗真錯誤：`could not import api/graphql.py at from flask import ...` —— **呢句 error message 講緊 `api/graphql.py`，即 rename 之前嗰個檔名**，證明真 crash 喺 rename 之前已經存在，同遮蔽無關 |
| `5aa1b2f` | 15:49:57 | 改返 `api/graphql.py`。Body：「The runtime log showed the real crash was the missing Flask dependency (fixed in the previous commit), **so the rename was unnecessary**」 |

**裁決：你個更正完全成立。** 而且我上一輪錯咗兩樣：**（1）錯歸因** —— 遮蔽從未喺 runtime 咬過人，兩個 deploy cycle 蝕喺誤診；**（2）錯數目** —— 係兩個 cycle（`fd252a3` + `e506531`），唔係一個。`CLAUDE.md` §6 同 `tickets.md:1042` 兩處都係照正確版本寫，措辭準確。

**額外證據**（`12c31fe` 嗰個 error message 講緊 pre-rename 檔名）我建議補入 §6 嗰行，因為佢係「遮蔽從未咬過人」呢句嘅**直接文件證據**，而唔係靠 `5aa1b2f` 事後嘅判斷。唔補都唔算缺陷，所以我唔開票。

---

## S-077 個 setting 有冇牙 —— 有，實測 2 errors

```
有 known-third-party = ["graphql"]：
  $ .venv/bin/ruff check --select I src tests
  All checks passed!                                    (exit 0)

移走嗰行之後：
  $ .venv/bin/ruff check --select I src tests
  I001  src/api/schema.py:9:1   Import block is un-sorted or un-formatted
  I001  tests/test_api.py:1:1   Import block is un-sorted or un-formatted
  Found 2 errors.  [*] 2 fixable with the `--fix` option.   (exit 1)
```

同你量到嘅 2 errors 一模一樣，而且 ruff 自己個 autofix 建議正正就係把 `from graphql import GraphQLError` 搬返落 first-party block —— 即係話 **setting 一移走，gate 就會主動把 code 拉返去錯嘅分類**。有牙，而且係雙向嘅。

`pyproject.toml` 嗰三行註釋（`src` 包含 `api/` → Vercel 規定 `api/graphql.py` → ruff 當佢係 first-party）措辭準確，同 `CLAUDE.md` §6 新嗰行冇矛盾。

事後已把 `pyproject.toml` 由備份還原，`md5sum -c` OK，`ruff check --select I` 回綠。

---

## Production 行為零改動 —— 我自己重做，冇信 lane 個工具

**方法**：`git ls-tree -r` 取兩個 revision 全部 `src/**/*.py`（各 **41** 個，file set 相同），逐個 `ast.parse`，用 `NodeTransformer` **剝走每一個 bare string-literal statement**，再 `ast.dump(indent=1)` 比對。

> ⚠️ 一個陷阱值得記低：**只剝 `Module/ClassDef/FunctionDef` 開頭嗰個 docstring 係唔夠嘅。** 本 repo 每個 module-level 常數下面都有一個 attribute docstring（`MAX_LIST_ROWS_PER_REQUEST = 4000` 後面嗰段），ast 唔當佢係 docstring。我第一版就係咁，出咗兩個 false positive（`schema.py` + `service.py`）。要剝晒所有 bare string Expr 先啱。

**結果：全樹得一處差異**

```
### AST DIFFERS: src/api/schema.py
   ImportFrom(module='graphql', names=[alias(name='GraphQLError')], level=0)
   —— 由 `strawberry.utils.logging` 之後，搬到 `import strawberry` 之後
RESULT: 其餘 40 個檔案（包括 S-071 改過嘅 src/api/service.py）AST 完全相同
```

**呢個 move 係 inert，我驗過**：

```
>>> assert 'graphql' not in sys.modules
>>> import strawberry
>>> 'graphql' in sys.modules
True
```

`import strawberry`（前一行）本身已經 import 咗 graphql-core，所以把 `from graphql import ...` 由第 25 行搬到第 17 行，唔可能改變 import 次序或任何 side effect。

⇒ **S-078 只加測試、S-077 只搬 import，其餘 12 條全部係 docstring / comment / 文件。核實成立。**

---

## 逐條 suggestion 核實（13 條）

> 判斷準則：唔淨係睇「有冇改」，而係睇「新寫落去嗰句係咪真」。每個新數字、新歸因我都重量過。

### S-063 ✅ 清得啱

改動：「At that widest」→ 明確點名係 90 個 `{ id }` alias 而唔係 30 個 `ActivityFields`，並加咗 "which is the cheaper document"。

**我重量**（真 DB、median of 5、statement count 另用 listener 獨立量）：

| document | statements | median | ms/stmt |
|---|---|---|---|
| 90 × `activity(hit){id}` | **180** | **93.8 ms** | 0.521 |
| 30 × `activity(hit){ActivityFields}` | **60** | **65.1 ms** | 1.085 |

⇒ 90-alias `{id}` 確實**又慢又寬**，30-alias `ActivityFields` 確實係 cheaper document。Lane 量到 95.7 / 71.2，我量到 93.8 / 65.1 —— 大小關係同量級一致。**成立。**

### S-064 ✅ 清得啱，而且係本 lane 技術上最紮實嗰條

改動：刪走「roughly half the reading either way is fixed cost, per-field dispatch over 90 aliases」，換成「The miss is half the hit **because it still issues 90 of those 180 statements**」+ 一個 10/30/45/90 sweep 嘅線性結論。

**我重做成個 sweep**（statement count 已獨立核實：hit = 2N、miss = N）：

| aliases | HIT stmt | HIT median | MISS stmt | MISS median |
|---|---|---|---|---|
| 10 | 20 | 12.3 ms | 10 | 7.5 ms |
| 30 | 60 | 32.0 ms | 30 | 16.6 ms |
| 45 | 90 | 48.9 ms | 45 | 24.9 ms |
| 90 | 180 | 93.8 ms | 90 | 46.6 ms |

最小二乘（x = statements, y = ms）：

- **HIT**：slope **0.511** ms/stmt，intercept **+2.05 ms = 93.8 ms 嘅 2.2%**
- **MISS**：slope **0.492** ms/stmt，intercept +2.40 ms（同一個 90-alias 讀數嘅 2.6%）

逐句核：

| docstring 講法 | 實測 | 裁決 |
|---|---|---|
| 「what does not scale with the statements is **a few percent** of the 90-alias reading rather than half of it」| 2.2%（vs 被推翻嗰個 ~50%）| ✅ |
| 「both paths come out linear in their statement count, **at much the same cost per statement**」| 0.511 vs 0.492，差 4% | ✅ |
| 「The miss is **half** the hit … because it still issues 90 of those 180 statements」| 46.6/93.8 = **0.497**；90/180 = 0.500 | ✅ 因果講法對到 1% 以內 |
| 「332 **counting statements** … cost per statement **about half** the `activity(id:)` readings」| 84.4 ms / 332 = **0.254** ms/stmt；0.254 / 0.511 = **0.50** | ✅（lane 量 0.256，我量 0.254）|

**被推翻嗰句確實係錯嘅**：fixed cost 係 93.8 ms 入面嘅 2.05 ms，唔係一半。**新寫嗰句全部成立。**

另外 lane 把「~330 ms of imports」同「first execution some 7% above the second」由讀數改成 order，並明寫理由（「properties of the interpreter and the machine rather than of this schema, and both moved when re-measured on another one」）—— **呢個係啱嘅取捨**，同 S-068 同一個原則：唔穩定嘅讀數唔應該扮精確。

> 📌 一個觀察（唔開票）：docstring 保留咗嘅 "~95 ms on the export (91.6-95.9 over five runs)" 呢個 range，我五次量到 90.3–94.3、median 93.8，**略低過佢個下界**。呢個數係本 lane 之前就有嘅（S-063 只改咗佢前面嗰句），唔係新寫，而且係 schema property 唔係機器 property，所以我唔當佢係缺陷 —— 但下次有人重量嗰陣會撞返，值得知道。

### S-065 ✅ 清得啱（mutation 我獨立驗過）

改動：`assert str(MAX_LIST_ROWS_PER_REQUEST) in _field_descriptions()[field]` → `assert LIST_ROWS_NOTE.strip() in ...`

**Mutation**：把 description 降級成 `'Aggregates for the exported year. Limit 4000.'`

```
OLD assertion  "4000" in description         -> True    ← SURVIVES（假綠）
NEW assertion  LIST_ROWS_NOTE.strip() in ... -> False   ← KILLS
```

**成立。** 而且註釋交代咗呢個 shape 同 `72dfcb5` 為 `(1-1000)` assertion 寫過嘅理由同源 —— 係跨 commit 嘅一致性，唔係逐條救火。

### S-068 ✅ 清得啱（見裁決 2）

### S-069 ✅ 清得啱

改動：「兩個都重現過」→「兩個候選成因，都重現到，但都證唔到當時就係佢」，並補第三個候選（同 process re-import / `sys.modules` 命中）。

**我獨立重現第三個候選**：

```
wrote SEP=':'  -> import m reads ':'
wrote SEP='0'  -> import m reads ':'    ← 舊值
wrote SEP=''   -> import m reads ':'    ← 舊值
sizes: {"':'": 10, "'0'": 10, "''": 9}
```

三個 case 全部解釋得到，**包括 stale `.pyc` 解釋唔到嗰個 `""`**（9 bytes ≠ 10，一定會重新 compile）。Lane 講法成立。

**而佢由「綁成因」改成「綁方法」（in-process `setattr` → assert mutant 生效 → 獨立 oracle）係比原文更有用嘅嘢** —— 我本輪做 S-078 同 S-075 兩個 mutation 就係照呢三步做，兩次都一次過攞到可信結果。

### S-070 ✅ 清得啱

改動：`COLLIDING_IDS` docstring 由點名一對 collision，改成講明係三對，並解釋「所以個 count 係 10 唔係 8」。

**我 brute-force 重做**（`points=7`，digit separator）：

```
id '05' picks up 3 stray rows:
   row 1 key '1005' also built by [('5', 10)]
   row 2 key '2005' also built by [('5', 20)]
   row 3 key '3005' also built by [('5', 30)]
   -> returns 7 sampled + 3 stray = 10 rows
rows returned for 05: [0, 1, 2, 3, 17, 33, 50, 67, 83, 100]
```

三對、位置 10/20/30 對 row 1/2/3、總數 10 —— **逐個數字吻合**，同 lane log 嗰個 `[0,1,2,3,17,33,50,67,83,100]` 亦一模一樣。

### S-071 ✅ 清得啱

改動：sweep 範圍由「the positions the suite's track lengths render」收窄成 `COLLIDING_TRACK_LENGTHS` + `VARIED_TRACK_LENGTHS`，並把「點解夠」搬入 docstring。

**對住代碼逐句核**（`tests/test_api.py:2598-2609`）：

```python
for total in COLLIDING_TRACK_LENGTHS + VARIED_TRACK_LENGTHS      # ← 範圍，吻合
...
assert set("".join(str(position) for position in positions)) == set("0123456789")   # ← 十個數字，吻合
```

Docstring 講嘅「the sweep is held to rendering all ten decimal digits, so a separator set to a digit the narrower set never produced cannot slip through」同代碼一字對一字。**成立。**

### S-073 ✅ 清得啱（我用真 export 重量）

改動：`OVER_CAP_LENGTH` docstring 刪走「is what a real `run365-export --points 1200` produces」，改成講清楚 `--points N` 封頂 N，1250 嚟自 raw data，並附重量方法；`source.test.ts` 同源一句一併修。

**我行咗兩次真 export 入 scratchpad**（`--db` 指去 scratchpad，冇掂 repo）：

| 指令 | `7264441638` 存低幾多行 | 第二名 |
|---|---|---|
| `run365-export --points 1200` | **1200** | `6701104700` 653 |
| `run365-export --points 1250` | **1250** | `6701104700` 653 |

⇒ 三個 claim 逐個成立：
1. 「`--points 1200` … writes 1200, not 1250」✅
2. 「1250 rows takes `--points 1250`」✅
3. 「**exactly one** activity in the export parses to more than 1000 track rows, and it holds **1250** of them」✅（第二名 653，遠低過 1000）

順帶：舊 docstring 嗰句確實係假嘅，我上一輪個 finding 成立，而 lane 改嘅方式唔止改個數，仲加咗「點樣重量」（count `track_rows` before the downsample, not the exported file）—— **呢個係比方案 A 好嘅做法**。

### S-074 ✅ 清得啱（見裁決 1）

Fixture docstring 新文（兩個檔案一致）：

> The document here is a stand-in. The real `YearQuery` takes no variables, and one `year` field spends a single page of the row budget, so this exact request could not have produced this error. **The API does refuse at `path: ["year"]` once earlier fields in the same document have spent the budget** — what it never sends is this document.

逐句核：
- 「The real `YearQuery` takes no variables」→ `frontend/src/data/api/queries.ts:45` 係 `query Year {`，零參數 ✅
- 「one `year` field spends a single page」→ `_charge_list_rows(info, DEFAULT_PAGE_SIZE)`，500 ✅
- 「this exact request could not have produced this error」→ 單一 `year` = 500/4000，唔可能 refuse ✅
- 「refuse at `path: ["year"]` once earlier fields … have spent the budget」→ 我實測 8 × `activities(limit:500)` + bare `year` 正正 refuse 喺 `path:['year']` ✅

另外 `1247` 呢個數字而家只剩喺 `ActivitiesView.test.tsx`（唯一真係量過 `ActivitiesQuery` 嗰個），Overview / Year 兩個已改成「the serialised blob」。**「同一個數字被三個唔同 document 引用」呢個問題一併解決咗。**

### S-075 ✅ 清得啱（mutation 我獨立驗過）

改動：tripwire 註釋點名係 `message` assertion，並解釋另外兩句點解守唔住。

**Rename mutant**（`info._raw_info` → `info._renamed_in_a_future_release`，清 `__pycache__`，`inspect.getsource` 確認生效）：

| | 真 refusal | rename 之後 |
|---|---|---|
| `locations` | `[{line:1, column:243}]` | `[{line:1, column:243}]` ← **一模一樣** |
| `path` | `['year']` | `['year']` ← **一模一樣** |
| `message` | `list row budget exhausted: …` | `'Info' object has no attribute '_renamed_…'` ← **只有呢個變** |

被點名嗰條測試兩個 parametrised case 都紅，而且**兩個都紅喺同一句**：

```
>       assert "budget exhausted" in error["message"]
E       assert 'budget exhausted' in "'Info' object has no attribute '_renamed_in_a_future_release'"
FAILED ...[list-rows]
FAILED ...[track-points]
```

⇒ 新註釋「on its `message` assertion specifically, and **only** on that one … there is no way to make the other two assertions carry this tripwire」**逐字成立**。

**額外**：mutant 之下 stderr 出返一個完整 traceback（含絕對路徑 frame）—— 即係 CUI-0029 關咗嗰個洩漏面會重開。呢個 tripwire 唔係裝飾。

### S-076 ✅ 清得啱（端到端驗過）

改動：`docs/deployment.md` 加「Seeing budget refusals in the log」一節，連一段可以照抄嘅 snippet。

**我用三個獨立 subprocess 驗**：

| 情境 | stderr bytes | Traceback | 內容 |
|---|---|---|---|
| 乜都唔設（＝Vercel 預設） | **0** | 0 | refusal 完全睇唔到 ✅ 同文件講法一致 |
| **照抄文件嗰段 snippet** | 397 | 0 | `INFO:strawberry.execution:list row budget exhausted: one request may read at most 4000 rows` ✅ |
| 真 fault（monkeypatch `service.meta` 掟 `RuntimeError`），**唔設 logging** | 1888 | **1** | 帶住 `boom-oracle` oracle ✅ |

⇒ 新一節每一句都成立，包括「Real faults are unaffected by this: they are logged at ERROR with their traceback whether or not the level is lowered」。**Snippet 係照抄得就 work 嘅，唔係示意。**

### S-077 ✅ 清得啱（見上面兩節）

### S-078 ✅ 清得啱（見裁決 3）

### S-079 —— 確認仍然 open，但 owner 只係一個 trigger 唔係一個角色

- `.proj-docs/tickets.md:1044` 狀態 = `pending` ✅ **仍然 open**
- `.proj-docs/tickets.md:1103`「刻意未做」表：「依本 repo『bump 時先寫』嘅慣例，留畀下次 release」
- `.proj-docs/tickets.md:1105`「未清 Suggestion 總數：1（S-079，留畀下次 bump）」
- `.proj-docs/index.md:3`「只餘 S-079 留待下次 bump」

⇒ **open ✅，而且有三個地方交叉指向。** 但嚴格講「owner」係「下次 bump / release step」—— 一個**觸發條件**，唔係一個具名角色，而 repo 入面**冇 release checklist 檔案**會喺 bump 嗰陣強制檢查（我掃過 `docs/`，`docs/CHANGELOG.md` 只寫 release branch / tag 命名慣例）。上一輪報告個修正優先順序表寫咗 owner = main agent，但嗰個表冇進 registry。

**我唔為呢點扣分**（brief 明講 S-079 唔應該再扣本輪分，而且三處交叉指向已經係高於本 repo 平均嘅記錄密度）。但**建議你 bump 前主動 grep 一次 `S-079`**，因為而家冇任何自動化會提你。

---

## 🟢 Suggestions（S-080 … S-082）

> **ID 起點核實**：掃 `.proj-docs/` 實際最高係 **C-002 / W-029 / S-079**，所以由 **S-080** 起，無撞號。
>
> **嚴重度取捨（透明交代）**：三條都係 `.proj-docs/tickets.md` 入面嘅記錄準確性，即 W-029（「withdraw a false measurement from CUI-0028's record」）嗰個先例嘅地盤。我最終全部定為 🟢 Suggestion，理由：**W-029 針對嘅係一句冇任何地方更正過嘅假量度**；呢三條入面，S-080 嗰句喺同一個檔案 54 行之後已經有明確更正，S-081 / S-082 係精度問題唔係假 claim。如果你按 W-029 先例判 S-080 為 Warning，score 由 97 跌到 93 —— **仍然 ≥ 90、仍然 pass、next_action 不變**。資料齊備，覆核權交返你。

### S-080 ｜ registry ｜ S-074 嗰行仍然原封不動載住本 lane 自己推翻咗嘅 claim

**位置**：`/home/user/Python-Project-Run365Days/.proj-docs/tickets.md:1039`

```
| **S-074** ✅ | 🟢 Suggestion | … 但真 `YearQuery`（…）**冇 variables**，
而 `year` 根本掟唔出 list-row budget refusal。… | ✅ **Done** (`07e74c5`) |
```

**描述**：「`year` 根本掟唔出 list-row budget refusal」呢句係我上一輪寫錯、而**本 lane 已經實測推翻**嘅 claim（我今輪獨立重現：9 個 alias 就 refuse）。而家佢帶住 ✅ **Done** 標記留喺 registry 嘅 suggestion 表入面，完全冇更正標記。更正只存在喺同一個檔案 **54 行之後**嘅 lane log（`:1093`，寫住「⚠️ **推翻 reviewer 一半**」）。

**點解值得講**：同一個 commit（`70af791`）入面，**S-077 嗰行（`:1042`）就有 inline ⚠️ 更正**（「⚠️ reviewer 寫『…輸過一個 deploy cycle』係**講唔準**：…」）。即係話呢條 lane 知道點樣喺 registry 表入面標記一個被推翻嘅 reviewer claim，亦真係做過一次 —— 只係 S-074 冇做。**處理唔對稱。**

**影響**：低但真實。下一個 agent 掃 registry suggestion 表（唔係掃 lane log）攞「`year` 會唔會 refuse」呢個事實，會攞到錯嘅答案，而且佢見到 ✅ Done 會以為呢句已經核實過。呢個正正就係本 lane 成個任務要斬嘅缺陷類別。

**方案 A**：照 S-077 嗰行嘅做法，喺 S-074 行尾加一句 inline 更正：「⚠️ reviewer 寫『`year` 根本掟唔出 refusal』係**錯**：`schema.py` 嘅 `year` charge `DEFAULT_PAGE_SIZE`，9 個 aliased `year` 就 refuse；真正唔成立嘅只係『真 `YearQuery` 帶 variables』同 provenance 句。詳見下方 lane log」。
**方案 B**：直接改寫嗰句，只保留真正成立嗰半（「真 `YearQuery` 冇 variables，所以 fixture 唔可能係 captured」），唔留更正痕跡。

**推薦 A** —— 同 S-077 嗰行一致，而且保住「一個 claim 曾經被推翻過」呢個資訊本身（呢個 repo 嘅 audit trail 慣例就係咁）。方案 B 會令 registry 讀落好似冇人錯過，反而失去 S-069 嗰種「候選 vs 成因」嘅誠實度。

### S-081 ｜ registry ｜ lane log 把兩個唔同嘅 refusal shape 合併咗喺同一個 `path:["year"]` 上

**位置**：`/home/user/Python-Project-Run365Days/.proj-docs/tickets.md:1093`

```
`year` **掟得出** budget refusal（`schema.py:1204` charge `DEFAULT_PAGE_SIZE`），
9 個 aliased `year` 或者 8×`activities(limit:500)` + `year{year}` 就 refuse 喺 `path:["year"]`
```

**描述**：兩條路都真係 refuse，但**只有第二條 refuse 喺 `path:["year"]`**。GraphQL error `path` 用嘅係 **response key**，所以第一條路（9 個 alias）refuse 喺最後嗰個 **alias 名**。我實測：

```
9x aliased year            : REFUSED  path=['a8']     locations=[{line:1,column:123}]
8x activities + bare year  : REFUSED  path=['year']   locations=[{line:1,column:243}]
```

句子語法上把 `path:["year"]` 掛喺兩條路上面，所以讀落會令人以為 aliased 嗰條都會出 `["year"]`。

**影響**：低。但呢句嘅**全部價值**就係佢證明 fixture 模擬嗰個 shape（`path:["year"]`）真 API 產生得到 —— 而九個 alias 嗰條路**證明唔到呢件事**。落一個唔知情嘅人手上，佢照住第一條路去重現，會攞到 `["a8"]` 然後以為記錄錯咗。

**值得講清楚**：**fixture docstring 本身寫得啱**（"once earlier fields in the same document have spent the budget"），所以呢條唔影響代碼，純粹係 registry 嗰句嘅精度。

**方案 A**：把兩條路分開講：「9 個 aliased `year` 會 refuse（`path` 係最後嗰個 alias 名）；要 refuse 喺 `path:["year"]` 就要個 `year` **無 alias**，例如 8×`activities(limit:500)` + `year{year}` —— 後者先係 fixture 模擬嗰個 shape」。
**方案 B**：刪走 9-alias 嗰個例子，只留 8×activities 嗰條（因為佢先係論證需要嗰條）。

**推薦 A** —— 9-alias 嗰個例子係最乾淨嘅「`year` 會 charge」證明（單一 field type、冇其他變數），值得留；只需要講清楚佢個 `path` 唔同。

### S-082 ｜ registry ｜ S-068 嗰行嘅 `22228` 讀數，喺同一條 lane 入面已經過期

**位置**：`/home/user/Python-Project-Run365Days/.proj-docs/tickets.md:1088`

```
| **S-068** ✅ | `77a94c8` | …實測 `src/api/service.py` 係 22228 / 22228 / 22227，宣告嗰行 26 / 26 / 25。
改成寫「關係 + 重量方法」，唔寫會過期嘅讀數 |
```

**描述**：`22228` 喺 `ea820aa` 係啱嘅。但**同一條 lane 嘅 S-071 commit（`1d901b3`）令 `src/api/service.py` 變成 22535 bytes**，所以呢個讀數喺佢寫落去之後三個 commit 就已經過期。宣告嗰行 26 / 26 / 25 冇事（同檔案長度無關）。

```
ea820aa service.py = 22228
fee1744 service.py = 22535
7bacce1 service.py = 22535
```

**影響**：極低，而且我要講清楚 **lane 喺呢件事上大體做得啱**：佢刻意冇把絕對讀數寫入 durable artefact（`CUI-0028.md` 只寫「真檔案大三個數量級」＋「關係 + 重量方法」），只留喺一個有日期 heading 嘅 lane log 度做證據。呢個 evidence-in-a-dated-log 嘅分工係啱嘅。**唯一問題係嗰句冇講明係喺邊個 revision 量嘅**，而佢就喺一行之後緊接住「唔寫會過期嘅讀數」—— 讀落有少少諷刺。

**方案 A**：加四個字：「實測 `src/api/service.py`（喺 base `ea820aa`）係 22228 / 22228 / 22227」。
**方案 B**：整句刪走絕對讀數，只留「三個 size 兩個相等、一個少一 byte」呢個不變量（同 `CUI-0028.md` 一致）。

**推薦 A** —— 個讀數作為「我真係去量過真檔案」嘅證據有價值，只係要綁返個 revision。方案 B 會令 lane log 失去「reviewer 話 20103、lane 話 22228，所以呢個數會漂」呢個論證嘅原始證據。

---

## ✅ 做得好嘅地方

1. **⭐ 三次推翻 reviewer，三次都啱，而且三次都係靠量唔係靠拗。** S-074（`year` 真係 refuse 得到）、S-068（方案 A 個數會過期）、S-078（方案 A 殺唔到 mutant）—— 我逐條獨立重做，冇一條企得住我原本嘅講法。一條 documentation lane 能夠喺三個獨立議題上壓倒 reviewer 嘅判斷，係罕見嘅質素。
2. **⭐ S-068 揀寫不變量而唔係讀數，被佢自己條 lane 即場證明係啱嘅。** `service.py` 由 22228 變 22535 就係佢自己嗰個 S-071 commit 搞出嚟。如果佢照我方案 A 寫個數，呢條 review item 會喺自己條 lane 收工之前就變成新嘅 W-017 案例。呢個係「治本 vs 治標」最乾淨嘅一個示範。
3. **⭐ S-069 由「綁成因」改成「綁方法」。** 「重現到一個機制 ≠ 當時真係發生咗呢個機制」呢句 epistemic 上係啱嘅，而佢跟住開出嚟嗰三步（in-process `setattr` → assert mutant 生效 → 獨立 oracle）**唔係抽象建議** —— 我本輪做 S-075 同 S-078 兩個 mutation 就係照呢三步做，兩次都一次過攞到可信結果，冇撞到 stale `.pyc`。呢條 suggestion 嘅產出物係一個可以重用嘅方法，唔係一段更正文字。
4. **⭐ S-078 釘 cost 唔淨係釘 value。** 「`== {}` 對 mutant 照綠，要連 SQL parameter assertion 先紅」呢個洞察，比我原本嗰個建議高一個層次。而實測仲顯示後果比佢講嘅重 —— mutant 會行一個 **0 參數嘅全表 grouped COUNT**。
5. **S-073 / S-074 / S-075 都唔止改個數，仲交代咗「點樣重量」或者「點解另外兩句守唔住」。** 例如 S-073 寫低「count `track_rows` per activity **before the downsample** rather than reading the exported file, which is already capped」—— 下一個人唔使重新踩一次我踩過嗰個坑（我第一次去讀 jsonl 就係讀錯咗欄位）。
6. **S-077 個 setting 係雙向有牙嘅。** 唔止「移走會紅」，而係「移走之後 ruff 嘅 autofix 會主動把 code 拉返去錯嘅分類」。加埋 `CLAUDE.md` §6 嗰行（連埋 mutation testing 個 stale `.pyc` 陷阱），呢個知識而家喺三個地方（config 註釋、trap table、registry）互相扶持。
7. **一條 suggestion 一個 commit，12 條 12 個 commit，冇一個夾帶。** 我逐個 commit 睇過 diff，`fix:` / `docs:` / `test:` / `chore:` prefix 用得準確（改測試用 `test:`、改 config 用 `chore:`）。
8. **Production 零改動係真嘅，而且佢自己有驗。** 41 個 `src/*.py` 得一個 import node 位置變咗。lane log 仲寫咗「另以一個 canary mutation 驗過個工具捉得到真改動」—— 即係佢知道一個「全部相同」嘅結果本身要被證偽過先可信。呢個習慣比個結果更值錢。
9. **S-074 冇順手做方案 B（抽共用 fixture helper）。** 三個 fixture 而家仲有重複，但佢守住 scope，冇喺一條 documentation lane 度做 DRY 重構。紀律啱。
10. **S-076 個 snippet 係照抄得就 work 嘅。** 我原文複製落 subprocess 行，一次過見到 refusal。好多 operator doc 寫到似示意圖，呢個唔係。

---

## 修正優先順序

| # | ID | 內容 | 阻 merge？ | 成本 | 建議 lane |
|---|---|---|---|---|---|
| 1 | **S-080** | `tickets.md:1039` S-074 行加 inline ⚠️ 更正（照 S-077 行嘅做法）| ❌ | 1 句 | 下次 docs touch |
| 2 | **S-081** | `tickets.md:1093` 把兩個 refusal shape 分開講 | ❌ | 1 句 | 同上 |
| 3 | **S-082** | `tickets.md:1088` 個 `22228` 加返 revision 限定 | ❌ | 4 個字 | 同上 |
| — | **S-079** | `docs/CHANGELOG.md` `[Unreleased]` | ❌ | release step | 下次 bump（**冇自動化會提，記得 grep**）|

**修正規範**：每個 review item 一個獨立 commit，`fix: S-0NN | <一句描述>`，唔可合併。

三條全部喺同一個檔案（`.proj-docs/tickets.md`）、全部一至四個字，**唔阻 merge、唔阻 QA**。以本 repo 嘅慣例，同下次任何 `.proj-docs` touch 一齊做就得，唔值得為佢哋單獨開一條 lane。

**修訂後完整代碼**：本輪 0 🔴 / 0 🟡，三條 Suggestion 全部係 registry 入面一至四個字嘅措辭修正，確切位置同替換文字已喺上面逐條寫明，唔需要另附完整檔案。

---

## 我做過嘅 mutation 同還原核實

| Mutation | 目的 | 還原核實 |
|---|---|---|
| `pyproject.toml` 移走 `known-third-party = ["graphql"]` | S-077 有冇牙 | `md5sum -c` OK，`ruff check --select I` 回綠 |
| `src/api/service.py` 刪走 `if not wanted: return found` | S-078 兩個方向 | `md5sum -c` OK，該測試回綠 |
| `src/api/schema.py` `info._raw_info` → `_renamed_in_a_future_release` | S-075 tripwire | `md5sum -c` OK，兩個 parametrised case 回綠 |
| 兩次 `run365-export` | S-073 | `--db` 指去 scratchpad，**冇掂 repo** |
| 每次 mutation 前後清 `__pycache__` | CLAUDE.md §6 stale `.pyc` | — |

**最終狀態**：`git log --oneline -1` = `7bacce1`；`git status --short`、`git status --porcelain -uall`、`git diff`、`git diff --cached`、`git stash list` **全部空**。三個被我改過嘅檔案 md5 同 mutation 前備份一致。**冇執行任何 git 操作，冇改動任何 project 檔案。**

Mutation 之後我重跑咗完整 `pytest tests --cov=src` —— **418 passed**，`api/schema.py` 100% / `api/service.py` 100% / TOTAL 95%，即係話所有 mutation 都真係還原乾淨。

---

## 主 agent 需要注意

1. **報告檔冇建立**（harness 禁止），請原文寫入 `.proj-docs/reviews/2026-09-16_review_CUI-0029_delta.md`，並更新 `.proj-docs/index.md` 頂部日期同新增條目。
2. **新 ID S-080 / S-081 / S-082 要入 `.proj-docs/tickets.md`**（掃過最高係 C-002 / W-029 / S-079，無撞號）。
3. **`fee1744` 同 `5d78e35` 兩個 merge 已經喺 `claude/ai-dev-team-start-05jie2` 上**，即係兩條 lane branch 已整合。receipt 嘅 `merge_develop` 指嘅係由呢個 branch 前進去 `develop`。
4. **S-079 冇任何自動化提醒**，下次 bump 前記得 `grep -rn "S-079" .proj-docs/`。

```handoff-receipt
protocol: 1
status: pass
score: 97/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "95%"
next_action: merge_develop
next_agent: quality-assurance
branch: "claude/ai-dev-team-start-05jie2"
context: "Delta re-review of ea820aa..7bacce1 (documentation-only lane clearing S-063..S-078, plus S-077's ruff known-third-party setting). 97/100 pass, up from 86 warn. 8/8 hard gates: ruff check + format clean, eslint clean, npm run typecheck clean, 418 pytest, 141 vitest across 25 files via `npm test`, run365-schema up to date, coverage api/schema.py 100% and api/service.py 100% (99%->100% from S-078) with TOTAL 95%, zero dependency changes in the delta so the security gate is n/a-pass. All 13 scored suggestions genuinely closed; S-079 (CHANGELOG [Unreleased]) confirmed still open and cross-referenced in three places, excluded from scoring per brief. Every new number and attribution the lane wrote was independently re-measured, not taken on trust. The three points where the lane overrode the previous reviewer were adjudicated against live measurement and the lane is right on all three, the reviewer wrong: (1) S-074 -- `year` DOES charge DEFAULT_PAGE_SIZE at schema.py:1204@ea820aa, so 9 aliased year fields refuse and 8x activities(limit:500) + a bare year refuses at exactly path:[\"year\"], the shape the fixture models; my prior 'cannot refuse at all' generalised from a single 3-alias probe. (2) S-068 -- refusing to write a file size was vindicated decisively: service.py was 22228 at ea820aa and 22535 at fee1744 because the lane's own S-071 commit grew it, so Option A's number would have been stale inside the same lane; the 10/10/9 figures reproduce exactly as a minimal `SEP = ':'` module. (3) S-078 -- Option A (`tracks(session, []) == {}`) SURVIVES the mutant, the lane's SQL-parameter assertion KILLS it, and the mutant is worse than the lane claimed: it issues a zero-parameter full-table grouped COUNT over track_points. The main agent's correction of my fd252a3 deploy-cycle attribution is verified against four commits and is right twice over -- the shadowing never bit at runtime (12c31fe's body quotes the pre-rename filename api/graphql.py in the real Flask error) and it cost two cycles, not one. S-077 setting confirmed to have teeth in both directions: removing it yields exactly 2 I001 errors and ruff's autofix actively pushes the import back into the first-party block. Production neutrality re-proven independently: 41 src/*.py files both revisions, all bare string-literal statements stripped (including this repo's attribute docstrings, which a naive docstring strip misses and which produced two false positives on my first attempt), leaving exactly ONE AST difference in the whole tree -- the `from graphql import GraphQLError` node moving one position earlier -- and that move is inert because `import strawberry` alone already places graphql-core in sys.modules. S-063/S-064 alias sweep re-measured: 90x{id} 93.8ms/180stmt vs 30xActivityFields 65.1ms/60stmt (so the {id} document is indeed the widest and dearest); least-squares over 10/30/45/90 gives 0.511 ms/stmt hit and 0.492 miss with a +2.05ms intercept = 2.2% of the 90-alias reading, refuting the old 'roughly half is dispatch' claim; miss/hit = 0.497 against 90/180 statements = 0.500, confirming the new causal story to within 1%; 332 counts at 0.254 ms/stmt = exactly half the hit path. S-065/S-070/S-075 mutation-verified, S-073 verified by two real exports (--points 1200 writes 1200, --points 1250 writes 1250, one activity over 1000 holding exactly 1250), S-076 verified end to end in three subprocesses (0 bytes by default, one INFO line with the doc's snippet verbatim, real faults still ERROR with traceback), S-069's third candidate reproduced, S-071 checked clause by clause against test_api.py:2598-2609. Three new Suggestions S-080/S-081/S-082, all one-to-four-word wording fixes inside .proj-docs/tickets.md: the S-074 registry row still carries verbatim the claim this lane disproved while the S-077 row in the same commit got an inline correction (asymmetric); the lane log conflates the two refusal shapes on path:[\"year\"] when only the unaliased one produces it; and the 22228 reading is quoted without naming the revision it was taken at, one line after saying not to write figures that expire. Graded Suggestion rather than Warning because a correction does exist 54 lines below in the same file -- grading S-080 a Warning under the W-029 precedent gives 93, still pass, same next_action. IMPORTANT: the review report file was NOT written -- my harness forbids subagents writing report .md files -- the full report is in this handback and must be persisted verbatim to .proj-docs/reviews/2026-09-16_review_CUI-0029_delta.md, with .proj-docs/index.md updated and S-080/S-081/S-082 registered (highest existing IDs rescanned: C-002/W-029/S-079, no collision). No git operations performed, no project file modified: every mutation (pyproject isort setting, service.py early return, schema.py _raw_info rename) was restored and md5-verified, __pycache__ cleared each time, the two test exports written to scratchpad only, and the full pytest+coverage suite re-run green afterwards; git status, git status -uall, git diff, git diff --cached and git stash list are all empty at 7bacce1. Note for the release step: S-079 has no automation behind it -- grep S-079 before the next bump."
blockers:
  - "S-080 (.proj-docs/tickets.md:1039) -- the S-074 registry row still states verbatim 'year 根本掟唔出 list-row budget refusal', a claim this lane disproved and corrected only 54 lines below in the lane log; the S-077 row in the same commit did get an inline correction, so the treatment is asymmetric. Non-blocking, one sentence."
  - "S-081 (.proj-docs/tickets.md:1093) -- '9 個 aliased year 或者 8x activities(limit:500) + year{year} 就 refuse 喺 path:[\"year\"]' attaches path:[\"year\"] to both routes; measured, the 9-alias route refuses at path:['a8'] because GraphQL error paths use the response key, and only the unaliased route yields ['year'] -- which is the shape the fixture models, so only that route carries the argument. The fixture docstring itself is correct. Non-blocking, one sentence."
  - "S-082 (.proj-docs/tickets.md:1088) -- '實測 src/api/service.py 係 22228 / 22228 / 22227' names no revision; the lane's own S-071 commit took the file to 22535, so the figure expired three commits later, one line after the row says not to write readings that expire. The durable artefact (CUI-0028.md) correctly avoids the number. Non-blocking, four words."
```