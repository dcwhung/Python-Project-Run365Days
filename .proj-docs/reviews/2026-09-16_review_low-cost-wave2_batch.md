# Batch Review — 2026-09-16 — low-cost wave 2（Round 2）

| 項目 | 內容 |
|---|---|
| 審閱者 | Code Reviewer（獨立，未參與呢批代碼撰寫） |
| 範圍 | `git diff 05f36df..HEAD`，5 個已 merge 嘅改動 |
| Branch | `claude/ai-dev-team-start-05jie2` |
| 涵蓋 commit | `d806f1c`(CUI-0004)、`c8bd76e`(CUI-0021)、`d88d760`(CUI-0015)、`38ef6e6`(S-042)、`2267a95`(S-043) |
| Round 1（`05f36df` 之前） | 已 review 兩輪，本報告**不重審** |
| 總評 | 質素高。唯一有行為改動嘅 CUI-0021 經獨立窮舉核實**完全正確**；扣分全部落喺文件準確性 |

---

## 整體 verdict

### Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| Lint（Python） | `.venv/bin/ruff check src tests` | ✅ pass — All checks passed |
| Format（Python） | `.venv/bin/ruff format --check src tests` | ✅ pass — 58 files already formatted |
| Tests（Python） | `.venv/bin/python -m pytest tests -q` | ✅ pass — **364 passed** |
| Schema sync | `run365-schema --check frontend/schema.graphql` | ✅ pass — up to date |
| Lint（前端） | `npm run lint` | ✅ pass |
| Type check（前端） | `npm run typecheck` | ✅ pass（codegen 先行，無 error） |
| Tests（前端） | `npx vitest run` | ✅ pass — **126 passed / 22 files** |
| Coverage（核心邏輯） | 見下註 | ✅ pass |
| No Critical | 本報告 | ✅ pass — 0 個 |
| Security scan | 本批**零新增依賴** | ✅ n/a → pass |

> **Coverage 註**：本 repo 未裝 `@vitest/coverage-v8`，無法出百分比。改為直接量度唯一新增嘅可執行函數
> `roundHalfToEven()` 嘅分支覆蓋：committed 嘅 `PYTHON_POSITIONS` 表行勻**全部四條分支**
> （`fraction > .5` ×2、`< .5` ×23、tie-even ×9、tie-odd ×4）。Python 側係純 docstring 改動，
> 唯一可執行 delta（`TRACK_DESCRIPTION`）由既有 SDL 測試 + `run365-schema --check` 覆蓋。

### 評分結果

| 維度 | 得分 | 滿分 | 備註 |
|------|------|------|------|
| 正確性（Correctness） | 25 | 25 | 500,499 組合窮舉核實逐 index 一致；邊界、負值、NaN、Infinity 全部獨立驗過 |
| 安全性（Security） | 20 | 20 | 零新增依賴、無 secret、無絕對路徑、read-only |
| 可維護性（Maintainability） | 11 | 20 | W-024 (−5)、S-044/S-045/S-046/S-047 (−4) |
| 測試覆蓋（Test Coverage） | 15 | 15 | +19 條測試、零刪除、守衛測試設計出色 |
| 性能（Performance） | 10 | 10 | 無 N+1、無多餘計算；S-043 反而少 build 一次 schema |
| 代碼風格（Code Style） | 10 | 10 | ruff / ESLint 全綠，commit convention 正確 |
| **總分** | **91** | **100** | |

**結果：✅ pass**（hard gates 全綠 + 91 ≥ 90 + 0 Critical）

### 發現統計

| 級別 | 數量 | ID |
|---|---|---|
| 🔴 Critical | 0 | — |
| 🟡 Warning | 1 | W-024 |
| 🟢 Suggestion | 4 | S-044、S-045、S-046、S-047 |

> Finding ID 由掃描 `.proj-docs/reviews/` 全部報告得出（既有最高 C-002 / W-023 / S-043），
> 本輪 Warning 由 **W-024** 起、Suggestion 由 **S-044** 起，全局唯一、無重用。

---

## Section A — CUI-0021（`c8bd76e`）唯一有行為改動

**改動**：`frontend/src/lib/downsample.ts` 由 `Math.round`（half-up）改成自寫 `roundHalfToEven()`，令 TS 側同 Python `round()` 對齊；新增 `frontend/src/lib/downsample.test.ts`（18 條）。另在 `src/api/service.py` 加 cross-reference docstring。

### A1. `roundHalfToEven()` 實作正確性 —— ✅ 正確，而且比佢自己聲稱嘅更穩陣

實作：

```ts
const lower = Math.floor(value);
const fraction = value - lower;
if (fraction > 0.5) return lower + 1;
if (fraction < 0.5) return lower;
return lower % 2 === 0 ? lower : lower + 1;
```

獨立核實兩個前提：

**前提 1 —「`value - Math.floor(value)` 喺 2^52 以下 IEEE-754 exact」→ 成立。**
由 Sterbenz lemma 推導：`lower <= value < lower + 1`，所以 `lower >= 1` 時
`value / lower < (lower+1)/lower <= 2`，兩數喺 factor 2 之內，減法無誤差；
`lower == 0` 時同 0 相減本身 exact。實測 2,000,000 次隨機抽樣（< 2^40）零個 fraction 出界。
超過 2^52 根本冇 half 值存在，函數自動退化成 identity（`2^52+0.5` 兩邊都回 `2^52`），**caveat 準確但保守**。

**前提 2 —「只會收到非負值」→ 成立，但函數對負值一樣正確。** 實測對照：

| value | TS `roundHalfToEven` | Python `round` | 一致 |
|---|---|---|---|
| −0.5 | 0 | 0 | ✅ |
| −1.5 | −2 | −2 | ✅ |
| −2.5 | −2 | −2 | ✅ |
| −3.5 | −4 | −4 | ✅ |

負值之所以啱，係因為 JS `%` 嘅符號跟被除數（`-3 % 2 === -1`）而 `-0 === 0` 為真，
兩者夾埋令 tie-even 判斷喺負半軸一樣成立。**呢點反而係註釋講細咗** → 見 S-044。

**特殊值**（今日入唔到，記錄備案）：`NaN → NaN`、`±Infinity → ±Infinity`（Python 會掟
`ValueError` / `OverflowError`）。因為 `limit < 2` 早返擋住 `limit === 1`，`step` 恆為有限值，
所以 unreachable。

### A2. 窮舉 cross-mode 核對 —— ✅ 獨立重做，範圍擴大至 developer 嘅 16 倍

用 `node --experimental-strip-types` **直接 import 真實嘅 `downsample.ts`**（唔係手抄副本），
同 Python 兩個 sampler 逐 index 比對：

| 比對 | 範圍 | 結果 |
|---|---|---|
| TS `downsample` vs Python `builder.downsample` | `2 <= total <= 1000`、`1 <= points <= total`，**500,499 組合** | ✅ **逐行完全一致** |
| Python `builder.downsample` vs `service._even_positions` | 同上 sampled path，499,500 組合 | ✅ 完全一致 |
| 舊 `Math.round` 實作 vs Python | 同上 | ❌ **105,181 組合唔同**（21.0%） |

Developer 自己聲稱嘅 `2 <= total <= 250` / 31,125 組合，我亦準確重現（31,125，divergent 6,634）。
因為 export `point_limit=600`、API `MAX_TRACK_POINTS=1000`，掃到 1000 已覆蓋全部可能輸入。

### A3. `frontend_behaviour_unchanged` —— ✅ 成立（含 600/600 邊界）

逐環核實：

1. `frontend/src/views/activity/ActivityView.tsx:18` → `export const TRACK_POINTS = 600`，
   `:34` → `useTrack(current, TRACK_POINTS)`，係 static source `track()` 唯一實際 caller。
2. `src/cli/export_data.py:27` → `DEFAULT_POINT_LIMIT = 600`；
   `.github/workflows/pages.yml:128` 行 `run365-export --skip-db --static-dir frontend/public/data`，
   **無帶 `--points`**，所以 CI 出嘅 static JSON 每條 track `length <= 600`。
3. **邊界係 `<=` 唔係 `<`**：`downsample.ts:31` 係 `if (n <= limit) return [...items]`。
   實測 `downsample(Array(600), 600).length === 600`（原樣回傳，行早返），
   Python 側同樣 `len(downsample(list(range(600)), 600)) == 600`。**`length === 600, points === 600` 入唔到 rounding。**

> ⚠️ 前提值得寫低：呢個「零影響」係**繫於 `run365-export` 用 `--points <= 600`**。
> 有人本機行 `run365-export --points 800` 再 build static，就會行到 rounding 分支
> —— 但嗰陣新實作先係啱嗰個，所以係「行為改變」而唔係「行為變錯」。無須改動。

### A4. 新測試設計 —— ✅ 優秀

- **真分歧個案**：`PYTHON_POSITIONS` 8 行全部係 `Math.round` 真係答錯嘅 case（我逐行覆核，
  例如 `[250, 3]` → `step = 249/2 = 124.5`，half-to-even 得 124、`Math.round` 得 125）。
- **守衛測試設計合理**：第二個 `it.each` 斷言 `halfUpPositions(...) !== expected`，
  即係「呢啲 case 真係 `Math.round` 答錯」。冇呢組守衛，有人 revert rounding 再順手揀啲
  非分歧長度，第一組表都會綠 —— 呢個正正係 review 最想見到嘅「測試唔會靜靜死掉」設計。
- `[250, 3]`、`[347, 5]`、`[1250, 7]` 同 `tests/test_api.py:152` 嘅 `VARIED_TRACK_LENGTHS`
  對齊，兩邊 fixture 講同一組長度，好。
- 最後一條釘死「今日零可見改動」，`length` 取到 `[1, 2, 250, 599, 600]` 包含 600 邊界。

### 🟡 W-024 — `downsample.ts` / `downsample.test.ts` 寫住嘅實測數字重現唔到，而且同隔壁註釋矛盾

**位置**
- `frontend/src/lib/downsample.ts:6-7` —「**22.6%** of the `(track_length, points)` pairs the export can produce」
- `frontend/src/lib/downsample.test.ts:11-12` —「CUI-0021 measured **10,189** such pairs among the **45,117** combinations the real export can produce」
- （同一組數字亦出現喺 `.tickets/in-progress/0001-0200/CUI-0021.md:27`）

**描述**
我試過所有合理嘅範圍定義（`total` cap 由 2 掃到 1000，`points` 取 `1..total-1` 同 `1..total` 兩種慣例），
**冇任何一種得出 45,117 組合或 10,189 個分歧**：

| 範圍 | 組合數 | 分歧數 | 比率 |
|---|---|---|---|
| total ≤ 250 | 31,125 | 6,634 | 21.31% |
| total ≤ 300 | 44,850 | 9,652 | 21.52% |
| total ≤ 309 | 47,586 | 10,221 | 21.48% |
| total ≤ 600 | 179,700 | 38,530 | 21.44% |
| total ≤ 1000 | 499,500 | 105,181 | 21.06% |

更關鍵：**掃勻 2..1000 每一個 cap，分歧比率最高只有 21.91%（cap = 103），22.6% 係達唔到嘅**。
所以呢個數字唔係範圍定義差異，係量錯咗。

同時 `downsample.ts` 話嗰 22.6% 係「the pairs **the export can produce**」，
但同一個 PR 嘅 `downsample.test.ts` 最後一條測試（同 A3 嘅結論）話**export 造得出嘅 track 全部原樣回傳、
根本入唔到 rounding**，即係 export 實際造得出嘅分歧 pair 係 **0**。兩句互相矛盾。

**影響**
唔影響行為（實作本身已獨立驗證完全正確）。但本 repo 明確將 docstring 當契約
—— AU-047 兩輪 review 之中就有兩個 finding 係「docstring 寫低嘅數字同實測對唔上」，
CUI-0024 亦係同一類。呢度係同一個缺陷 class 出現喺**兩個 source file**，
下一個想重現呢個數字嘅人一定撞板。

**方案 A**：換成我已重現得到嘅數字同明確範圍定義，例如
「105,181 of the 499,500 `(length, points)` pairs with `2 <= length <= 1000` (21.1%)」，
並將「the export can produce」改成「the sampler can be asked for」以消除同 A3 嘅矛盾。
- ✅ 保留量化說服力、可重現、矛盾消失
- ❌ 要寫明範圍，句子長少少

**方案 B**：整句刪走數字，只留定性描述（「on any step that lands exactly on .5」），
理由同 CUI-0015 揀方案 3 一致 —— 文件唔再持有一份會 drift 嘅 copy。
- ✅ 最短、永不 drift
- ❌ 失去「呢個 bug 有幾普遍」嘅感覺，說服力下降

**推薦：方案 A。** 呢個數字係靜態事實（唔會隨 codebase 演進而變），唔屬於 CUI-0015 針對嗰類
「會反覆再發生」嘅 drift；佢只係量錯，改啱就一直啱。而 CUI-0021 嘅價值主張本身就係
「呢個分歧有幾廣泛」，刪走可惜。

### Section A 評分

| 維度 | 得分 |
|---|---|
| 正確性 | 25/25 |
| 可維護性 | 14/20（W-024 −5、S-044 −1） |
| 測試覆蓋 | 15/15 |

---

## Section B — CUI-0004（`d806f1c`）docstring + SDL + test

**改動**：`src/api/service.py` `_even_positions` docstring、`src/api/schema.py` `TRACK_DESCRIPTION`、
`frontend/schema.graphql` 同步、`tests/test_api.py` 新增一條測試。

### B1.「零行為改動」聲稱 —— ✅ 用 AST 核實，結論要講得準確啲

剝走全部 docstring（module / class / function）之後比對 AST：

| 檔案 | 結果 |
|---|---|
| `src/api/service.py` | ✅ **完全 IDENTICAL** —— 純 docstring |
| `src/api/schema.py` | ⚠️ **1 處可執行 delta**：`TRACK_DESCRIPTION` 嘅字串常數 |

`TRACK_DESCRIPTION` **唔係** docstring，係 module-level 常數，會直接進入 SDL output。
所以準確講法係「**零 sampling 行為改動；唯一可執行改動就係 SDL description 文字本身**」
—— 而嗰個正正係呢張票要改嘅嘢，所以完全正確、亦解釋咗點解 `frontend/schema.graphql` 一定要跟住改。
`run365-schema --check` 綠 = SDL 已正確同步。`frontend/src/gql/graphql.ts` 由 codegen 生成
且 `frontend/.gitignore:5` 已忽略，唔使 commit，正確。

### B2. Docstring / SDL 講嘅嘢同實際行為對唔對得上 —— ✅ 端到端實測

SDL 新句：「`points: 1` has nothing to space evenly and returns the track's last row alone, not its first.」

我落咗一個臨時 probe 直接打真 GraphQL（`year_client` fixture，track 存 600 行）：

| Query | 實際回傳 | 對唔對 |
|---|---|---|
| `track(points: 1) { sec }` | `[{sec: 599}]` | ✅ 最後一行，唔係第一行 |
| `track(points: 2) { sec }` | `[0, 599]` | ✅ 首尾都有 |
| `service.track(...)` vs GraphQL | 完全一致 | ✅ 兩層無分歧 |

Docstring 講嘅「`points == 1` cannot hold both, and returns the last row alone」同
`_even_positions` 實作 `if points < 2: return [total - 1]` 一致；亦同
`builder.downsample` 嘅 `if limit < 2: return [points[-1]]` 一致（A2 已證兩個 sampler 全範圍等價）。

### B3. 新測試有冇真正釘死語義 —— ✅ 有

`test_a_single_sample_is_the_last_row_not_the_first` 三條斷言：`len == 1`、
`sec == STORED_TRACK_POINTS - 1`（即 599，明確係**尾**唔係頭）、
同埋 `== downsample(list(range(...)), 1)` 直接綁住 reference 實作。
第三條最有價值：任何一邊單方面改咗呢個 edge，測試即刻紅。註釋亦老實講低咗
「Nothing about the arithmetic forces that choice」—— 呢個係約定而唔係數學必然，寫得好。

**評分**：正確性 25/25、可維護性 20/20、測試覆蓋 15/15。無發現。

---

## Section C — S-042（`38ef6e6`）test fixture 排序不變式

**改動**：`sql_count` fixture 由 `def sql_count():` 改成 `def sql_count(year_client):`，
並改寫 S-039 留低嗰段「而家已經唔啱」嘅註釋。

### C1. 不變式真係由 fixture graph 保證？—— ✅ 獨立重現（過程中修正咗自己第一次嘅量度）

> ⚠️ 自我更正：我第一次只 `sed` 咗 `(year_client, sql_count)` 呢個 exact pattern，得 3 個 signature，
> 量到「3 failed」。翻查 commit message 講「seven signatures」後重數，發現仲有 4 個
> signature 係跨行 / 帶第三個參數（`tests/test_api.py:1009, 1069, 1103, 1418`）。
> 改用 `year_client, sql_count` → `sql_count, year_client` 全域替換，命中 **7 處**，先係完整實驗。

| 實驗 | fixture | 參數次序 | 結果 |
|---|---|---|---|
| 基線 | 舊（無參數） | 原本 | 105 passed |
| **B（完整）** | **舊（無參數）** | **7 處全部調轉** | ❌ **13 failed, 92 passed** |
| **A（完整）** | **新（`year_client`）** | **7 處全部調轉** | ✅ **105 passed** |

**Developer 聲稱嘅「改之前 13 failed、改之後 105 passed」完全重現。**
失敗嘅斷言形狀亦對得上：`397 == 4`、`499 == 106`、`601 == 208`、`725 == 332`、
`397 == (2 + 2)`、`401 == (2 + (3 * 2))` —— 全部係 `year_client` 嗰 ~393 條 setup statement 溝咗入去。
全部係 assertion failure，唔係 collection error（合乎 CLAUDE.md §6「TDD Red 階段」嗰條陷阱嘅要求）。

> 小瑕疵（唔另立 finding）：commit message 逐項列舉時寫「`397 == (2 + 2)` x6」，實測係 **x7**，
> 而列出嚟嘅個案加埋係 11 個而唔係 13 個。標題數字（13 / 105）準確，只係枚舉漏咗兩個。
> Commit 已寫死，無得改，記錄備案即可。

### C2. 方案選擇 —— ✅ 採納 reviewer 推薦嘅方案 A，且 docstring 記低咗 trade-off

Fixture docstring 明確講低「Takes `year_client` for the ordering rather than for the value」，
同埋「A second kind of client would want a `sql_count_for(client)` factory rather than a loosening of this」
—— 預先答咗下一個人最可能問嘅問題，好。

### C3. S-039 留低嘅 `sql_count[0] = 0` 而家係咪冗餘？—— **技術上冗餘，但建議保留**

實測：將 `tests/test_api.py:1125` 嗰行 `sql_count[0] = 0` 刪走，
`test_a_flood_of_aliased_parents_issues_more_statements_than_the_field_cap_bounds` **仍然綠**。
所以就現行 code path 而言，佢已經唔再承擔正確性。

但**有獨立價值**，建議保留：
1. **對稱**：同下半場嘅 `sql_count[0] = 0` 成對，兩半讀法一致（新註釋正正係咁講）。
2. **局部防線**：將來有人喺 fixture setup 同第一個 assert 之間插入任何會發 statement 嘅呼叫，
   呢行即刻重新變成 load-bearing。
3. 新註釋已經**老實聲明**咗保證嘅真正來源：「this line no longer carries that on its own」
   —— 冇扮自己仲係唯一防線，唔會誤導。

呢個處理方式正確，**唔構成 finding**。

**評分**：正確性 25/25、可維護性 20/20、測試覆蓋 15/15。無發現。

---

## Section D — S-043（`2267a95`）test DRY

**改動**：`test_the_type_graph_stays_one_level_below_the_depth_limit` 入面
`build_schema_from_sdl(schema.as_str())` 由行兩次改成綁一個 local `sdl` 讀兩次。

### D1. 兩次讀之間有冇 mutation？—— ✅ 無

| 檢查 | 結果 |
|---|---|
| 讀完 `sdl.type_map.values()` 再讀 `sdl.query_type` 後，`type_map` 有冇變 | ✅ 完全不變 |
| `sdl.query_type` 重複讀係咪同一個 object | ✅ 係（identity 相同） |
| 兩次獨立 build 嘅 `type_map` keys / `query_type.name` | ✅ 完全相同（deterministic） |
| `_deepest_selection()` 會唔會改到 schema | ✅ 唔會 —— 純讀取遞歸，`seen` 係 tuple 每層重建，無 in-place 改動 |

所以「兩次讀係問同一個 SDL 兩條問題」呢個前提成立，重用安全。

### D2. 兩邊讀數仍然 load-bearing？—— ✅ 人為整紅驗證過

- **第二次讀（`sdl.query_type` → 深度斷言）**：本身就係 exact `==` 斷言，
  type graph 深度一變即紅，明顯 load-bearing。
- **第一次讀（interface / union tripwire）**：呢條喺今日嘅 SDL 係「空 list」通過，
  表面似死代碼，所以我人為注入抽象類型驗佢會唔會真係響：

| SDL | 搵到嘅抽象類型 | tripwire |
|---|---|---|
| 真實 SDL | `[]` | 通過（今日無 interface / union） |
| 真實 SDL + `interface Node` | `['Node']` | ✅ **FIRES** |
| 真實 SDL + `union AB = A \| B` | `['AB']` | ✅ **FIRES** |

**兩次讀都仍然 load-bearing**，DRY 化冇削弱任何一條防線。
新加嗰句註釋「Both readings below are questions about this one SDL, not two schemas」
準確解釋咗點解可以共用，好。

**評分**：正確性 25/25、可維護性 20/20、測試覆蓋 15/15。無發現。

---

## Section E — CUI-0015（`d88d760`）docs-only

**改動**：`docs/architecture.md`、`README.md`、`docs/deployment.md` 嘅 CI 描述由「複述 pages.yml」
收窄成「指向 pages.yml」（已拍板嘅方案 3）。

### E1. 剩低每一句同 `.github/workflows/pages.yml` 對唔對得上 —— ✅ 逐句核實全中

| 文件聲稱 | `pages.yml` 實況 | 對 |
|---|---|---|
| Python 檢查＝lint、format、tests、GraphQL schema sync | `:53` ruff check、`:56` ruff format --check、`:59` pytest、`:62` run365-schema --check | ✅ |
| 前端檢查＝lint、typecheck、unit tests、兩個 data mode build | `:83` lint、`:86` typecheck、`:89` vitest、`:92` vite build、`:95` static build | ✅ |
| 「Lint, tests and the frontend checks run on every branch the workflow gates」 | `lint-test` / `frontend` 兩個 job **都冇 `if:`**，所以每個 gated branch 都行 | ✅ |
| 「build 同 deploy 限一個 branch，由 `github.ref` test 控制」 | `:105` 同 `:152` 都係 `if: github.ref == 'refs/heads/develop'` | ✅ |
| 「`workflow_dispatch` 跟同一條限制，唔係 deploy branch 就 CI-only」 | `:8` 有 `workflow_dispatch`，而 ref guard 同 event 無關、照樣生效 | ✅ |
| 「`dist/index.html` 複製做 `404.html`」 | `:135` `cp dist/index.html dist/404.html` | ✅ |
| 修正 `architecture.md` 舊寫「push / PR to **`master`**」 | 實際 `:5`/`:7` 係 `[develop, master]`，舊文確實錯 | ✅ 修好 |

**呢個係真 drift 修復**：`architecture.md` 原本講 CI 只 gate `master`，同實況相反。

### E2. 保留 `develop` 引用嘅理由成唔成立 —— ✅ 成立，逐處對過 checklist

`docs/deployment.md` 嘅「Switching the deploy source」checklist **`git diff` 確認完全冇郁**。
Developer 理由係「剩低嘅 `develop` 引用全部喺切換 checklist 覆蓋範圍」。逐處核對：

**README.md（6 處）** — 由 checklist 第 4 項覆蓋，該項逐項點名：

| 行 | 內容 | checklist 第 4 項點名 |
|---|---|---|
| 3 | CI badge `?branch=develop` | ✅「the CI badge's `?branch=` query」 |
| 246 | CI/CD 段落 | ✅「the "Continuous integration and deployment" section」 |
| 259 | Vercel production branch | ✅「the Vercel production-branch sentence」 |
| 275 | Versioning 表 | ✅「the "Versioning and branches" table」 |
| 277–278 | 表下段落 | ✅「with the paragraph under it」 |

**docs/deployment.md（9 處）** — 由 checklist 結尾「Then update this file: the intro, the trigger table,
the Vercel section, the GitHub Pages section and this checklist」覆蓋：

| 行 | 位置 | 覆蓋 |
|---|---|---|
| 3 | intro | ✅ the intro |
| 13 | trigger 表 | ✅ the trigger table |
| 17 | Vercel 段 | ✅ the Vercel section |
| 69、80 | GitHub Pages 段（含 `github-pages` environment） | ✅ the GitHub Pages section（第 5 項另外覆蓋實際設定） |
| 86、88、96、100 | checklist 自身 | ✅ and this checklist |

**結論：15 處全部有覆蓋，理由成立。** 而且 `docs/architecture.md` 收窄後
**`develop` 引用歸零**，所以唔需要新增 checklist 條目 —— 呢個正正係方案 3 嘅收益。

### E3. 有冇刪走讀者真正需要嘅資訊 —— 大致無，一處例外

刪走嘅 job 步驟清單、branch list 都係 `pages.yml` 一睇就有，符合方案 3。
`404.html` 呢個「形狀事實」有保留，判斷準確。但見 S-047。

### 🟢 S-047 — `VITE_BASE_PATH=/<repo>/` 收窄後喺全 repo 文件消失

**位置**：`docs/deployment.md`（原第 3 項）、`README.md`（原第 3 項）

**描述**：`grep -rn "VITE_BASE_PATH" README.md docs/` 而家**零命中**。
但 `docs/deployment.md` 用同一套理由保留咗 `404.html` 嗰句 —— 因為佢係「形狀事實」而唔係「job 步驟」。
`VITE_BASE_PATH=/<repo>/` 同樣係形狀事實：佢解釋咗**點解個 site 喺 Pages 嘅子路徑之下 asset 都 resolve 到**，
而且同 branch list 唔同，佢唔會 drift。

**影響**：低。有人 debug「Pages 上面 CSS/JS 404」嗰陣，deployment.md 應該係第一站，
而家搵唔到呢個關鍵字，要去讀 workflow YAML 先知。同 `404.html` 嘅處理唔一致。

**方案 A**：喺 `404.html` 嗰句旁邊補一句，例如
「The build also sets `VITE_BASE_PATH=/<repo>/`, so the site works from the repository subpath Pages serves it on.」
- ✅ 一句、同 `404.html` 處理一致、唔會 drift
- ❌ 段落長少少

**方案 B**：維持現狀，靠 `pages.yml` 做唯一來源。
- ✅ 最貼近方案 3 嘅原則
- ❌ 同 `404.html` 雙重標準；debug 情境下唔就手

**推薦：方案 A。** 判準係「會唔會 drift」：branch list 同 job 次序會，`VITE_BASE_PATH` 唔會，
所以佢同 `404.html` 應該同一邊。

### Section E 評分

| 維度 | 得分 |
|---|---|
| 正確性 | 25/25 |
| 可維護性 | 19/20（S-047 −1） |

---

## Section F — 橫向檢查

### F1. 五個改動之間有冇矛盾陳述 —— 一處（已計入 W-024）

唯一矛盾係 Section A 講嘅：`downsample.ts` 話 22.6% 嘅 pair 係「the export can produce」，
而同 PR 嘅測試同 CUI-0021 ticket 都話 export 造得出嘅 track 一律原樣回傳、分歧為 0。
其餘跨檔案陳述互相一致 —— 特別係 `src/api/service.py` 新加嘅 cross-reference
（指向 `frontend/src/lib/downsample.ts` 同 CUI-0021）同 TS 側嘅反向 reference 對得上，
形成雙向可追溯，呢點做得好。

### F2. 測試數核實 —— ✅ 加埋啱數，零刪除

| | 之前 | 之後 | delta | 核實 |
|---|---|---|---|---|
| Python | 363 | **364** | +1 | `test_a_single_sample_is_the_last_row_not_the_first`（CUI-0004） |
| 前端 | 108 | **126** | +18 | 新 `downsample.test.ts`：2 個 `it.each` × 8 行 = 16，加 2 條獨立 `it` = **18** ✅ |
| 前端 test file | 21 | **22** | +1 | 只加 `downsample.test.ts` ✅ |

`git diff 05f36df..HEAD -- tests/ frontend/src` 搵 `^-.*（def test_|it(|it.each|describe(）`：
**零命中，冇任何測試被刪或改名。** 新增 5 個 test 宣告（1 Python + 4 TS，其中 2 個係 `it.each`）。

### F3. `tests/test_api.py` 被三個 commit 改過 —— ✅ 無互相干擾

CUI-0004（+新測試，第 651 行附近）、S-042（fixture 第 932 行 + 註釋第 1116 行附近）、
S-043（第 498–515 行）改嘅係三個互不重疊嘅區域，merge 後 `git diff` 三段獨立。
全檔 105 條測試綠、全 suite 364 綠，無 fixture 名衝突、無 import 衝突。

### F4. Scope、secret、絕對路徑、model identifier —— ✅ 全部乾淨

| 檢查 | 結果 |
|---|---|
| Secret / API key / token / private key | ✅ 零命中 |
| 絕對路徑（`/home/`、`/Users/`、`/tmp/claude`、`C:\`） | ✅ 零命中 |
| Model identifier / session URL 洩漏入 tracked 檔案 | ✅ 零命中 |
| 新增依賴 | ✅ 零 —— `package.json` / `pyproject.toml` 都冇郁 |
| 每個 commit = 一個 review item | ✅ 5 個 commit 對 5 張票，無夾帶 |
| Commit message 格式 | ✅ 全部合 `fix:/docs: TICKET \| 描述` |
| Ticket 狀態流轉 | ✅ 三張 CUI 票都由 `pending/` 正確移去 `in-progress/` |

### F5. CUI-0024 準確性 —— 6 項事實全部成立，但**目標數字已經過時**

逐項對住代碼核實：

| # | CUI-0024 聲稱 | 我核實 | 判定 |
|---|---|---|---|
| 1 | `README.md` 寫「three console scripts」，實際 4 | `[project.scripts]` 確有 4 個（`run365-activities`/`-weather`/`-export`/`-schema`）；README:118 確寫「three」 | ✅ 成立 |
| 2 | `architecture.md:28` 同樣寫「Three」 | 確認仍在 | ✅ 成立 |
| 3 | `architecture.md:104` 引用已消失嘅 `write_data_js` | `grep -rn write_data_js src/` **零命中**；architecture.md:104 確有 | ✅ 成立 |
| 4 | `architecture.md:139`「133 tests」 | 仍在 | ✅ 成立 |
| 5 | `README.md:212`「93 tests」 | 仍在 | ✅ 成立 |
| 6 | `README.md` Vitest 數字要重量 | 實際喺 **:216**（票寫 :214），內容「87 Vitest tests」屬實 | ✅ 成立（票已自我警告行號會變） |
| ⚠️ | 「`README.md` all **nine** views 係啱嘅，唔好改」 | `frontend/src/views/` 確有 **9** 個目錄 | ✅ 成立，確認唔好改 |
| ⚠️ | 「`CHANGELOG.md:117` 嘅 `write_data_js` 係歷史紀錄，唔好改」 | 確認係 changelog 條目 | ✅ 成立 |

**同 CUI-0015 冇矛盾**：CUI-0015 一行都冇掂過上述 6 個位置（scope 紀律正確），
而 CUI-0024 亦已明文提醒「以內容定位，唔好照行號」。

### 🟢 S-046 — CUI-0024 列嘅「正確值」喺呢批 merge 之後已經過時

**位置**：`.tickets/pending/0001-0200/CUI-0024.md` 第 4、5、6 項同「驗證方式」段

**描述**：CUI-0024 喺 `05f36df` 建立，即係呢 5 個 merge **之前**。佢寫住：
- 第 4、5 項「實際：**363**」→ CUI-0004 加咗一條測試，而家係 **364**
- 第 6 項「`.proj-docs/index.md` 記 108」→ 實際已經係 **126**（CUI-0021 加 18 條）
- 「驗證方式」寫「**363** 條測試全綠（證明冇意外掂到代碼）」→ 應為 **364**

**影響**：低但實在。執票者照抄就會寫入 363 / 108 兩個**即刻又錯**嘅數字，
變成「修 drift 嘅票自己帶住 drift」。

**方案 A**：更新 CUI-0024 嗰三處數字為 364 / 126。
- ✅ 一分鐘搞掂
- ❌ 治標 —— 再有新測試又過時（呢張票自己都指出咗呢點）

**方案 B**：採納 CUI-0024 自己「建議方向」段推薦嘅做法，直接標註第 4、5、6 項為
「移除寫死數字」而唔係「更新數字」，票入面唔再持有任何具體數目。
- ✅ 同 CUI-0015 方案 3 一致；票自己已經論證過呢個方向更好
- ❌ 執票時要寫多少少描述性文字

**推薦：方案 B**（並順手做方案 A 令票面即時準確）。呢張票自己已經講咗
「更新數字唔係最好嘅答案」，而佢自己過時咗就係最好嘅佐證。

> 註：呢個係 **main agent 開嘅 ticket**，唔係本批 developer 嘅產出，唔影響 5 個 fix 嘅 status。

### F6. CUI-0025（review 期間出現）—— 核實成立，同本批findings 互補、無矛盾

`.tickets/pending/0001-0200/CUI-0025.md` 喺本次 review 進行途中（09:25）由 main agent 建立，
唔屬於本批 5 個改動，但同 Section A / S-045 有重疊，故此一併核實。

**claim (a) `points: 0` 兩個 mode 相反 —— ✅ 成立**，而且成因指得準：

| 路徑 | `points = 0` | 核實 |
|---|---|---|
| static `source.ts:67` `points ? downsample(...) : rows` | **回晒全部 row**（`0` falsy，短路） | ✅ 實測回 4/4 行 |
| api `schema.py` `1 <= points <= 1000` guard | **拒絕** | ✅ |
| `downsample(rows, 0)` 本身（兩個語言） | 兩邊都回 `[last]` | ✅ **一致** |

即係話分歧**唔喺 `downsample` 入面**，而係喺 caller 嗰個 falsy guard —— 同 ticket 講法一致。

**同本報告嘅關係**：**唔矛盾、係互補**。S-045 講嘅係 `downsample` 內部對
**非整數** `limit`（`NaN` / `2.5`）嘅分歧；CUI-0025 (a) 講嘅係 **caller 層**對 `0` / `undefined`
嘅分歧。兩者今日都 unreachable（唯一 call site 傳 600）。
CUI-0021 只承諾對齊**取樣 index**，冇聲稱處理呢個語義分歧，所以**唔構成本批嘅遺漏**。

---

## ✅ 做得好嘅地方（跨 fix 通用）

1. **守衛測試（guard test）設計** —— `downsample.test.ts` 第二個 `it.each` 專門斷言
   「呢啲 case `Math.round` 真係答錯」。呢個係防止測試「靜靜死掉」嘅正確做法，
   值得推廣成 codebase pattern。
2. **雙向 cross-reference** —— `service.py` docstring 指向 `downsample.ts`，`downsample.ts` 指回
   Python sampler，兩邊都標 CUI-0021。將來改任何一邊都會撞到另一邊嘅說明。
3. **S-042 揀咗結構性解法** —— 用 fixture 依賴一次過覆蓋 7 個 signature，
   而唔係喺每個 call site 加 reset（後者係「每個新測試都要記得」嘅規則）。docstring 仲預先寫低
   幾時應該改用 `sql_count_for(client)` factory。
4. **老實嘅註釋** —— S-042 新註釋明講「this line no longer carries that on its own」，
   CUI-0004 測試註釋明講「Nothing about the arithmetic forces that choice」。
   唔扮嘢、唔誇大，正正係 review 最想見到嘅態度。
5. **Scope 紀律** —— CUI-0015 見到同一批檔案仲有 drift 但冇順手修，改為申報開 CUI-0024。
   5 個 commit 對 5 張票，零夾帶。
6. **測試邊界揀得準** —— CUI-0021 最後一條測試 `length` 取 `[1, 2, 250, 599, 600]`，
   直接覆蓋 `n <= limit` 嗰個 600/600 邊界；`VARIED_TRACK_LENGTHS` 亦包含 `0, 1, 2, 601, 1250`。
7. **CUI-0024 自己標註咗「核實過冇錯、唔好順手改」嗰兩項**（nine views、CHANGELOG 歷史紀錄），
   防止執票者過度修正 —— 呢個細節好有價值。

---

## 修正優先順序

| 優先 | ID | 級別 | 位置 | 工作量 | Block merge |
|---|---|---|---|---|---|
| 1 | W-024 | 🟡 | `frontend/src/lib/downsample.ts:6-7`、`downsample.test.ts:11-12`、CUI-0021 ticket | 3 行文字 | ❌ 否 |
| 2 | S-046 | 🟢 | `.tickets/pending/0001-0200/CUI-0024.md` | 3 個數字 | ❌ 否 |
| 3 | S-044 | 🟢 | `frontend/src/lib/downsample.ts:11-12` | 1 句 | ❌ 否 |
| 4 | S-045 | 🟢 | `frontend/src/lib/downsample.ts:24-26` | 1 個詞 | ❌ 否 |
| 5 | S-047 | 🟢 | `docs/deployment.md` | 1 句 | ❌ 否 |

**全部唔 block**。本批可以照 proceed，W-024 同 4 條 Suggestion 建議合併成一個
follow-up commit（或者併入下一輪 low-cost wave）。

---

## 🟢 S-044 / S-045 明細

### 🟢 S-044 — 註釋將 `roundHalfToEven()` 講細咗，可能引來一個會整壞佢嘅「修正」

**位置**：`frontend/src/lib/downsample.ts:11-12`

> Only called with non-negative values, which is why the fractional part can be
> taken as `value - floor` (exact in IEEE-754 below 2^52) without a sign case.

**描述**：「without a sign case」暗示呢個函數對負值**唔啱**。實測佢**啱**（見 Section A1 表）：
−0.5→0、−1.5→−2、−2.5→−2、−3.5→−4，同 Python `round` 完全一致。
機制係 JS `%` 符號跟被除數加上 `-0 === 0`。

**影響**：低但真實。將來有人為「穩陣」加一個 `Math.sign` 分支，好可能反而整爛依家啱嘅行為，
而現有測試（全部非負輸入）唔會捉到。

**方案 A**：改寫成事實描述 ——「Sign-correct as written (JS `%` keeps the dividend's sign and
`-0 === 0`), though `downsample` only ever passes non-negative values.」
- ✅ 保護住呢個非顯而易見嘅正確性 ❌ 長少少

**方案 B**：加一兩條負值 parity 測試，等實作自己講。
- ✅ 可執行、最防得住 ❌ 測試唔對應真實輸入，有人會覺得係死碼

**推薦：方案 A**（成本最低、直接堵住誤修風險）。若想更穩陣可 A + B 一齊。

### 🟢 S-045 — `downsample` docstring 講「Mirrors ... exactly」，但非整數 `limit` 之下兩邊靜靜分歧

**位置**：`frontend/src/lib/downsample.ts:24-26`

**描述**：實測對照（**呢啲 guard 係舊有行為，新嘅只係嗰句 claim**）：

| `limit` | TS | Python |
|---|---|---|
| `NaN` | 回 `[]`（**成條 track 無聲無息消失**） | `range(nan)` → `TypeError` |
| `2.5`（10 元素） | 回 2 個元素 | `TypeError` |
| `0` / `-5` | 回 `[last]` | 回 `[last]` ✅ 一致 |

整數 `limit` 之下「exactly」完全成立（Section A2 已證 500,499 組合）。今日所有 call site
（`ActivityView` 600、`DEFAULT_TRACK_POINTS` 600）都傳整數字面量，所以 unreachable。

**影響**：低。只係嗰句 claim 闊過實情。

**方案 A**：收窄措辭 ——「Mirrors ... exactly for integer `limit`, rounding included」。
- ✅ 一個詞、claim 即刻準確 ❌ 無改善 `NaN` 靜默行為

**方案 B**：加 `if (!Number.isInteger(limit)) throw new RangeError(...)`。
- ✅ 兩邊都變成「拒絕」，真對齊 ❌ 為 unreachable 情況加 runtime 分支，要加測試，超出本票 scope

**推薦：方案 A。** `NaN` 入唔到，為佢加 guard 唔划算；但 docstring 應該只 claim 佢守得住嘅嘢。

---

## 修訂後代碼

只有 W-024 / S-044 / S-045 涉及代碼檔案，全部集中喺 `frontend/src/lib/downsample.ts`
嘅註釋。**函數本體一個字都唔使改 —— 已獨立驗證完全正確。**

```ts
/**
 * Round half to even, the way Python's built-in `round` does.
 *
 * `Math.round` rounds a half up, so it drifts one index away from the Python
 * samplers on any step that lands exactly on .5: 105,181 of the 499,500
 * `(length, points)` pairs with `2 <= length <= 1000` -- 21.1% of the range the
 * sampler can be asked for (CUI-0021).            // W-024: 可重現嘅數字 + 明確範圍；
 *                                                 // 唔再講 "the export can produce"，
 *                                                 // 因為 export 造得出嘅 track 全部原樣回傳。
 * The two modes draw the same field, so this side follows Python rather than
 * the other way round: the static JSON this module reads was itself written by
 * `run365days.dashboard.builder.downsample`.
 *
 * Sign-correct as written -- JS `%` keeps the dividend's sign and `-0 === 0`,
 * so a negative tie still lands on the even side -- though `downsample` only
 * ever passes non-negative values. The fractional part is taken as
 * `value - floor`, which is exact in IEEE-754 (Sterbenz: `lower <= value <
 * lower + 1`, so the two are within a factor of two); above 2^52 no halves
 * exist and this degenerates to identity.         // S-044: 唔再暗示負值會壞，
 *                                                 // 免得有人加 sign 分支反而整爛。
 */
function roundHalfToEven(value: number): number {
  // 本體不變 —— 500,499 組合窮舉核對同 Python 逐 index 一致。
  const lower = Math.floor(value);
  const fraction = value - lower;
  if (fraction > 0.5) return lower + 1;
  if (fraction < 0.5) return lower;
  return lower % 2 === 0 ? lower : lower + 1;
}

/**
 * Keep at most `limit` evenly spaced items, always including first and last.
 *
 * Mirrors `run365days.dashboard.builder.downsample` and
 * `run365days.api.service._even_positions` exactly for an integer `limit`,
 * rounding included, so api mode and static mode return the same rows.
 * `limit < 2` yields the last item alone, matching both (CUI-0004).
 *                                                 // S-045: 加 "for an integer limit"。
 *                                                 // 非整數（NaN / 2.5）之下 TS 同 Python
 *                                                 // 行為唔同，但所有 call site 都傳 600。
 */
export function downsample<T>(items: readonly T[], limit: number): T[] {
  // 以下完全不變。
  const n = items.length;
  if (n <= limit) return [...items];
  if (limit < 2) return [items[n - 1]];
  const step = (n - 1) / (limit - 1);
  return Array.from({ length: limit }, (_, i) => items[roundHalfToEven(i * step)]);
}
```

`downsample.test.ts:11-12` 對應改法：

```ts
 * Every row here is a case where Python's round-half-to-even and JavaScript's
 * round-half-up land on different indices, so the table only stays green while
 * this module rounds the Python way. CUI-0021 measured 105,181 such pairs
 * among the 499,500 `(length, points)` combinations with `2 <= length <= 1000`
 * -- the whole range the sampler can be asked for, since the export caps a
 * track at 600 rows and the API at 1000 points.
```

---

## 驗證環境聲明

為驗證 S-042 / S-043 / CUI-0004，我曾**臨時**改動 `tests/test_api.py`
（調轉 7 個 fixture 參數次序、還原 fixture 簽名、刪一行 reset）並**臨時新增**
`tests/test_zz_reviewer_scratch.py` 做 GraphQL 端到端 probe。

**全部已還原 / 刪除**，並逐次以 `git status --porcelain` 確認為空。
最終狀態：**`git diff --stat` 為空、工作區乾淨**，唯一新增檔案係本報告。
還原後重跑全 suite：**364 passed**。

所有窮舉 probe（`ts_positions.mts`、`py_positions.py`、`halfup.mts`、`edge.mts`、`branch.mts`）
一律寫喺 session scratchpad，**從未進入 repo**。
本 reviewer **冇執行任何 git 操作**（無 merge / commit / push / branch 改動）。

> 註：`.proj-docs/index.md` 頂部日期需要更新（CLAUDE.md §7），
> 但 reviewer 只獲准寫 review 報告，**留俾 main agent 處理**。

---

## Handoff receipt

```
HANDOFF_RECEIPT
protocol: 1
agent: code-reviewer
status: pass
score: 91
critical: 0
warning: 1
suggestion: 4
report: .proj-docs/reviews/2026-09-16_review_low-cost-wave2_batch.md
tickets: CUI-0004, CUI-0021, CUI-0015, S-042, S-043
next_action: invoke-qa
notes: Hard gates 全綠（364 Python / 126 前端）；CUI-0021 rounding 經 500,499 組合獨立窮舉核實完全正確，唯一 Warning W-024 係 downsample.ts / test 入面重現唔到嘅實測數字，唔 block。
```
