# Batch Review — 2026-09-17 — CUI-0047 / CUI-0049 / CUI-0044

**HEAD 我見到嘅係 `2ff659e`（陳舊，同你警告一致）。已 `git fetch origin claude/ai-dev-team-start-05jie2` 再 `git checkout 86ca2ad`，之後全程喺 `86ca2ad` 上做，最後再核實 HEAD = `86ca2ad`。**

**報告檔案：冇建立。** `.proj-docs/reviews/` 寫唔到 —— `project-boundary` hook 封鎖 scratchpad，而我唔准改 project 檔案。以下係完整報告原文，請原文寫入 `.proj-docs/reviews/2026-09-17_review_CUI-0047_batch.md`。

**Review Item ID 範圍：我用 W-060 起 / S-120 起**（另一個 reviewer 用 W-050 / S-103）。本次實際用咗 **W-060**、**S-120 – S-124**。0 個 🔴 Critical。

**我冇 dispatch 任何 workflow**，冇 commit / merge / push，冇改任何 project 檔案。臨時 mutation 全部還原並 md5 核實（見下）。

---

## 整體 verdict

| 項目 | 內容 |
|---|---|
| 涵蓋 commit | `a3bcf36`（CUI-0047）、`c20a834`（CUI-0049）、`a2a4706`（CUI-0044），經 `ca80219` / `3e31028` / `86ca2ad` 合併 |
| Diff 範圍 | `src/common/geo.py`、`tests/test_common_geo.py`、`.github/workflows/pages.yml`、`CLAUDE.md`、`docs/deployment.md`、`frontend/package.json` + `package-lock.json` |
| 整體 score | **90 / 100** |
| Status | ✅ **pass** |

### Hard Gates（全部我自己實測）

| Gate | 指令 | 結果 |
|---|---|---|
| Lint (py) | `ruff check src tests` | ✅ `All checks passed!` |
| Format (py) | `ruff format --check src tests` | ✅ `61 files already formatted` |
| Lint (fe) | `npm run lint` | ✅ exit 0 |
| Type check | `npm run typecheck`（先跑 codegen → `tsc -b`） | ✅ exit 0 |
| Tests (py) | `pytest tests -q` | ✅ **573 passed**（72.59s） |
| Tests (fe) | **`npm test`** | ✅ **144 passed / 25 files**（vitest 5.0.1） |
| Coverage | `--cov=src` | ✅ `src/common/geo.py` **100%**（26 stmt / 0 miss）、**TOTAL 96%** |
| No Critical | — | ✅ **0** |
| Security (npm) | `npm audit`（全樹） | ✅ `found 0 vulnerabilities`，**exit 0** |
| Security (py) | `pip-audit`（upgrade 後） | ✅ `No known vulnerabilities found`，**exit 0** |
| SDL 同步 | `run365-schema --check frontend/schema.graphql` | ✅ `up to date` |
| Build ×2 | `npm run build` / `npm run build:static` | ✅ 兩個都 exit 0 |
| Workflow lint | `actionlint 1.7.7` | ✅ exit 0（shellcheck 整合未啟用，環境冇裝） |

---

## 逐條裁決（十條 lane 自報講法）

### 裁決 1 — CUI-0047「取代而唔係疊加」嘅等價性 ✅ **完全成立**

我把 `77868dd:src/common/geo.py` 抽出嚟做獨立 module，同 HEAD 版本**並行跑 19 個輸入**（nan ×4 位置、`±inf` ×4、`None` ×5 組合、合法 HK 對、極點、對蹠子午線、四角）：

```
differing: 0 / 19
```

逐項核實：

| 輸入 | 舊（CUI-0008） | 新（CUI-0047） | 一致 |
|---|---|---|---|
| `nan`（任一分量） | `ValueError: non-finite coordinate: origin=…, destination=…` | **字串完全相同** | ✅ |
| `+inf` / `-inf`（任一分量） | 同上 | **字串完全相同** | ✅ |
| `None`（任一 / 多個分量） | `nan`（早於 guard return） | `nan` | ✅ |
| `None` + `nan` 混合 | `nan` | `nan` | ✅ |

機制我亦核實過：`abs(nan)` 係 `nan`、`nan <= 90.0` 係 `False`、`abs(±inf)` 過唔到 `<= 90.0`，所以範圍檢查**真係吞埋**有限性檢查，happy path 檢查數目 4 → 4，訊息一字不差。`kind` 嗰個三元式用 `all(math.isfinite(...))` 重新分類，只喺 error path 行（多 4 次 `isfinite` + 一個 generator），comment 亦明文交代咗「Only reached on the way to an exception」。**等價性成立，lane 推翻 main agent 嘅「向量化」建議係對嘅。**

### 裁決 2 — Benchmark ✅ **成立（有一處措辭要收緊）**

我自己用 `timeit`（min of 7，1,000,000 次）重量，Python 3.11.15：

| 寫法 | min | spread | vs shipped |
|---|---|---|---|
| A int chained `-90 <= lat1 <= 90 …` | **166.3 ns** | 4.3 ns | +61.4% |
| **B `abs()` + float（shipped）** | **103.1 ns** | 2.6 ns | — |
| C float chained | 130.9 ns | 4.2 ns | +27.0% |
| D `abs()` + int | 151.9 ns | 2.5 ns | +47.4% |
| E `math.isfinite` ×4（CUI-0008） | 59.2 ns | 1.1 ns | −42.5% |

- Lane 報 **170.6 ns / 102.3 ns**，我量到 **166.3 / 103.1** —— 差距遠細過兩者之間嘅 gap，而且 spread 只 2–4 ns。**「分得開，唔係 noise」成立**，code comment 嗰句「ranks the candidate guards apart by well over their own run-to-run spread」屬實。
- `abs()`+float 確係五個入面最平，int-chained 確係最貴。**code comment 嘅講法完全準確。**
- 第二個獨立量度：我量到一次 `haversine_distance` = **3609 ns**，經 `DataFrame.apply` 每 segment = **9474 ns**（lane 報 9508 ns）。Guard 佔 **2.86% of a call / 1.09% of a segment**（lane 報 1.15%）。**兩個獨立量度再次對上。**
- ⚠️ 但 lane 向 main agent 報嘅「int literal 逼 float↔int 比較貴 **40%+**」係把兩個效應撈埋咗：單獨隔離 int-literal 效應係 **+27%**（C→A）或 **+47%**（B→D），唔係一個乾淨嘅「40%+」。→ **S-124**。

### 裁決 3 — QA 表嘅 destination 錯誤 ✅ **lane 更正正確**

我獨立跑無守衛 haversine：

```
(1e20,1e20) -> (0,0)          = 2780.028158541463
(1e20,1e20) -> (-1e20,-1e20)  = 5560.056317082926   ← 啱啱好雙倍
(400,0)  -> (0,0) = 4447.797065782349
(91,0)   -> (0,0) = 10118.738324654845
(0,200)  -> (0,0) = 17791.188263129396
(1e308,1e308) -> (-1e308,-1e308) = nan  + 兩個 "invalid value encountered in sin"
```

QA 表原本寫 `lat=lon=1e20 → 5560.1` 但冇寫 destination，而頭兩行係對 `(0,0)` 量嘅 —— **同一張表撈咗兩種 destination，lane 嘅診斷一字不差**。`.tickets/pending/0001-0200/CUI-0047.md:86-88` 已明文記低更正。測試入面四個 `pre_fix_reading` 我逐個對過，**全部精確吻合**。

### 裁決 4 — 真數據零回歸 ✅ **成立；佢個方法論係啱嘅，我重做咗**

DB 喺呢個 worktree 唔存在（`data/processed/` 只有 `.gitkeep`），所以「96,489 點」我**驗唔到**，但實質嘅嘢我用 raw 檔直接掃，比 DB 更接近源頭：

| 格式 | 檔數 | 點數 | lat 範圍 | lon 範圍 | out-of-range | non-finite |
|---|---|---|---|---|---|---|
| gpx | 365 | 97,004 | 22.246345 – 22.359524 | 114.175854 – 114.267489 | **0** | **0** |
| tcx | 365 | 97,179 | 同上 | 同上 | **0** | **0** |
| kml | 365 | 193,996 | 同上 | 同上 | **0** | **0** |

距 `±90` 差 **67.640°**、距 `±180` 差 **65.733°**（lane 報 67°/66°）。**約 388k 個座標分量，0 個會被新 guard 拒。**

**關於佢個「先 assert 兩條 arm 真係唔同」嘅做法 —— 呢個做法係啱嘅，而且係本次最值得表揚嘅一點。** 一個 null result（「冇 diff」）冇 positive control 就等於冇資訊：你分唔清「真係冇回歸」同「我兩條 arm 其實係同一段 code / 實驗根本測唔到嘢」。我照做重跑：

```
POSITIVE CONTROL  old=ok (3937.41 km, 2)  new=ValueError  ARMS_DIFFER=True   ← 先 assert
REAL DATA  tracks=356  points=97003  differing_tracks=0  max_delta_km=0.0    ← 先有上面嗰行，呢行先有意義
```

**裁決：方法正確，結論成立。**

### 裁決 5 — `exclusive` mutant 只俾邊界測試殺 ✅ **成立，數字一模一樣**

我做咗**六個 mutant**（兩個方向都覆蓋），每個都跟 `CLAUDE.md` §6「stale `.pyc`」嗰條陷阱：**先清 `__pycache__` → 用獨立 oracle assert mutant 真係生效咗（唔假設）→ 先至信 pytest 結果**。

| # | Mutant | Oracle 確認 live | pytest 結果 |
|---|---|---|---|
| 1 | `<=` → `<`（exclusive） | 極點被拒 ✅ | **4 failed** — 全部係 `test_the_extremes_of_the_real_globe_are_still_accepted` |
| 2 | Guard 退回 CUI-0008 `isfinite` ×4 | `(400,0)` 被接受 ✅ | **16 failed / 557 passed**（**全套 573**） |
| 3 | lat bound `90` → `900` | — | 8 failed |
| 4 | lon bound `180` → `1800` | — | 4 failed |
| 5 | destination 唔守（只守 origin） | 只 origin 被拒 ✅ | 7 failed（CUI-0008 嗰 5 條 + CUI-0047 嗰 2 條） |
| 6 | `kind` 分類反轉（`not all` → `all`） | — | 2 failed（兩條 message 測試各一） |

- **Mutant 1 精確複製 lane 嘅結果：4 紅，全部係同一條邊界測試。** `±90` / `±180` 嘅 inclusive 有真承重 assertion。
- **Mutant 2 係反方向**：guard 真係殺得到 out-of-range，而且**全套 573 條入面冇任何一條依賴「out-of-range 被接受」** —— 16 條全部喺 `TestHaversineRejectsImpossibleCoords`，冇 collateral。
- Mutant 5 證明 destination 分支冇缺口；mutant 6 證明兩個 message 分類都有 assertion 釘住。

**還原核實**：`src/common/geo.py` md5 `17390ed55ed248914c5170cf5b76f086`（同 mutation 前一致）、`tests/test_common_geo.py` md5 `7df1e75adb39374ba631181c51e4ec2c`、`git status --short` **空**。還原後再跑全套：**573 passed**、ruff 全綠、SDL up to date。

### 裁決 6 — codegen 產出真係變咗 ⚠️ **substance 成立，但行數對唔上（見 W-060）**

「只有一個 consumer」我 grep 驗證咗（`frontend/src` 全域，三種 import 形式）：

```
src/data/api/queries.ts:1:import { graphql } from "@/gql";
```

**確係唯一一個**，而且確係淨係攞 `graphql` helper。`tsc -b` 實測 exit 0。

我另外喺 `/tmp` 開咗一份舊 toolchain（`77868dd` 嘅 `package.json` + `package-lock.json`，`npm ci`）實際跑舊 codegen 對比：

| | client-preset 4 | client-preset 6 |
|---|---|---|
| `wc -l src/gql/graphql.ts` | **351** | **75**（實際 76 行，最後一行冇 trailing newline） |
| schema-wide type（`Maybe`/`InputMaybe`/`Scalars`/`Activity`/`TrackPoint`） | 5 個都有 | **0 個**（我 grep 過，全部消失） |
| `ID` input | `Scalars['ID']['input']` = `string` | `string \| number`（`graphql.ts:30`, `:37`） |

**Schema-wide type 消失、單一 consumer、`ID` 變 `string \| number` —— 三項全部成立。** 但 `345 → 69` 對唔上 `wc -l` 量到嘅 `351 → 75`，兩邊都**剛好差 6**（= 開頭 `eslint-disable` / `Exact` / `Incremental` / `import type` 嗰 6 行 preamble）。我連跑兩次 codegen 確認輸出 deterministic（兩次都 75）。→ **W-060**。

### 裁決 7 — `lodash` 完全離開依賴樹 ✅ **成立**

```
$ npm ls lodash
run365days-dashboard@3.2.0
`-- (empty)
```

`@vitest/mocker` 亦由 3.x 升到 **5.0.1**。兩個舊 advisory 源頭都走咗。

### 裁決 8 — `npm audit` 冇印 `0 critical` ✅ **lane 更正正確**

舊樹實跑，原文最後一行：

```
14 vulnerabilities (2 moderate, 12 high)
```

**確實冇 `0 critical`** —— 票／QA 嗰個「0 critical」係人手補。Lane 嘅更正準確。順帶核實 gate 行為：**舊樹 `npm audit` exit 1**（會 block），**新樹 exit 0**。

### 裁決 9 — pip-audit，同埋佢用第三方做紅種嘅設計 ✅ **成立，而且個設計係啱嘅**

Upgrade **前**（venv pip 24.0 / setuptools 79.0.1 —— 同 lane 報嘅版本一模一樣）：

```
Found 14 known vulnerabilities in 2 packages
pip 24.0        × 12  (PYSEC-2026-1795/1796/2875/2876/196/3721)  fix: 25.3 … 26.2
setuptools 79.0.1 × 2 (PYSEC-2026-3447)                          fix: 83.0.0
Skip: run365days — Dependency not found on PyPI
exit 1
```

Upgrade **後**（pip 26.2.1 / setuptools 84.0.0）：

```
No known vulnerabilities found
exit 0        審咗 65 個 distribution
```

**14 條、兩個版本號、升到嘅版本號、65 個 distribution、`run365days` 永遠被 skip（所以唔用 `--strict`）—— 全部逐項吻合。** 每條都有 fix version，所以「upgrade 係真修唔係 suppression」成立。

**裁決佢個紅綠設計 —— 正確，而且係本次第二個值得表揚嘅方法論決定。** 我實測重現：喺 pip / setuptools **已經**升到 fixed version 之後，再 `pip install jinja2==3.1.2`：

```
jinja2 3.1.2  PYSEC-2026-1471/1472/1473/1474/1475  → exit 1
```

要點喺度：如果用 pip / setuptools 做紅種，**個 step 自己第一行 `python -m pip install --upgrade pip setuptools` 就會清咗佢** —— 咁你測緊嘅係嗰句 upgrade，唔係測緊個 gate 有冇偵測力。用一個 upgrade 掂唔到嘅第三方 package 做紅種，先至把「gate 真係識紅」同「upgrade 順手洗白咗」兩件事**分開**。呢個正正係佢自己 comment 入面「the upgrade is a fix, not a suppression」嗰句需要嘅獨立證據。**設計成立。**

我亦把 step 嘅 `run:` body 原文抽出嚟，用 `bash -e -c` 配 stub 真行（**冇 dispatch workflow**）：

| 情境 | 結果 |
|---|---|
| 全綠 | exit **0**，三句都行 |
| `pip-audit` rc=1 | exit **1**，`REACHED_END` 冇印 → step fail |
| `pip install pip-audit` rc=1 | exit **1**，**`pip-audit` 根本冇行** → fail-closed 正確 |

### 裁決 10 — `CLAUDE.md` 同 `docs/deployment.md` 嘅改動 ⚠️ **大方向啱，有一處錯、三處可收緊**

**啱嘅：**
- §6 剷走 `frontend/src/gql/graphql.ts:318` —— **必須剷**，新檔得 76 行，`:318` 已經係死 reference。改成「CUI-0049 bump 後喺 `TrackQuery` 嗰行」冇行號、更耐用。✅
- 同一條 row 保留嘅 `frontend/src/data/api/source.ts:77` 我核實過，**仍然準確**（`return activity?.track ?? [];`）。✅
- §3 `npm audit --omit=dev` → `npm audit`：同 CI 一致。✅
- 新增 client-preset 6 陷阱 row：schema-wide type 消失、單一 consumer、`ID` 變 `string | number` —— substance 全對。✅
- `docs/deployment.md` 由「一個 gate」改成「兩個 gate」：兩張票各令佢一半過時，**唔改就係錯**，改得啱。✅ 「Both audits run last in their jobs」我用 parsed YAML 核實：pip-audit 係 `lint-test` 第 8 步（共 8 步），`npm audit` 係 `frontend` 第 9 步（共 9 步）—— **兩個都真係最後一步**。✅

**有問題嘅：** 行數 `345 → 69`（W-060）、§3 本地指令同 CI 唔等價（S-120）、`deployment.md` 漏咗 pip-audit 自己個 closure（S-121）。

**有冇過度？** 呢個 repo 嘅 house style 本身就係重註釋、重理由（成個 `pages.yml` 都係咁），所以篇幅本身唔算過度。但同一套理由而家**同時存在於三個地方**（`pages.yml` comment、`docs/deployment.md`、`CLAUDE.md` §3），有 drift 風險 —— 呢個正正係 lane 自己用嚟否決 `--ignore-vuln` allowlist 嘅同一個論據。→ **S-122**。

---

## 部署鐵律核查（`CLAUDE.md` §4）

我用 parsed YAML 做結構比對（`77868dd` vs `86ca2ad`），唔靠肉眼睇 diff：

```
triggers:  push[develop,master] / pull_request[develop,master] / workflow_dispatch   — 兩邊完全相同
jobs:      lint-test, frontend, build, deploy                                        — 兩邊完全相同
build.if:    github.ref == 'refs/heads/develop'   — 冇 CHANGED 行，未被郁 ✅
deploy.if:   github.ref == 'refs/heads/develop'   — 冇 CHANGED 行，未被郁 ✅
build.needs: ['lint-test','frontend']             — 未被郁
deploy.needs: build                               — 未被郁
build / deploy 兩個 job 嘅 steps：0 差異 ✅
concurrency group / cancel-in-progress：未被郁 ✅

唯一差異：
  + lint-test  新增 'Audit Python dependencies'（最後一步）
  + frontend   'Audit runtime dependencies: npm audit --omit=dev'
             → 'Audit dependencies (whole tree, dev included): npm audit'
```

**部署鐵律完好無損。** `actionlint` 亦 exit 0。

## `pyproject.toml` 核查

```
$ git diff --stat 77868dd..86ca2ad -- pyproject.toml requirements.txt vercel.json .pre-commit-config.yaml
(空)
```

**四個都冇被碰，lane 講法成立。**

## `src/api/` 旁系 lane 冇被撞爛

`86ca2ad` 係三條 lane 嘅 merge。`src/api/` 唔喺我範圍，但我核實咗佢冇壞：全套 **573 passed**（含 `tests/test_api.py`）、`src/api/schema.py` **100%**、`src/api/service.py` **100%**、`run365-schema --check` 報 `up to date`、`ruff` 全綠。**冇撞爛。**

---

## 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|---|---|---|---|
| 正確性 | 24 | 25 | 等價性 19/19、mutation 6 個方向全殺、真數據 388k 分量零回歸；扣 S-122 |
| 安全性 | 19 | 20 | 兩個 SCA gate 一個新增一個放闊，兩個都實測綠 + fail-closed 驗過；扣 S-120 |
| 可維護性 | 13 | 20 | 註釋質素極高；扣 W-060（-5）、S-121、S-123 |
| 測試覆蓋 | 15 | 15 | `geo.py` 100%，邊界／oracle／message 三層都有承重 assertion |
| 性能 | 9 | 10 | Benchmark 兩個獨立量度互相印證；扣 S-124 |
| 代碼風格 | 10 | 10 | ruff check + format 全綠，eslint 全綠 |
| **總分** | **90** | **100** | |

**結果：✅ pass**（hard gates 全 pass + ≥ 90 + 0 Critical）

---

## 問題清單

### 🔴 Critical — 無（0 個）

### 🟡 Warning

#### W-060 — `CLAUDE.md` §6 新增嘅 codegen 行數重現唔到

**位置**：`CLAUDE.md` §6，`@graphql-codegen/client-preset 6 唔再產出 schema-wide type` 嗰 row
**描述**：寫住「`frontend/src/gql/graphql.ts` 由 **345 行**縮到 **69 行**」。我兩邊都實際跑過 codegen：

```
client-preset 4（舊 lock，/tmp 獨立 npm ci）：wc -l = 351
client-preset 6（本 HEAD，npm ci）：          wc -l = 75   （連跑兩次都係 75，deterministic）
```

兩個數**都剛好細 6**，即係 lane 用咗一個冇講明嘅計法（最大可能係扣走頭 6 行 preamble：`/* eslint-disable */`、兩對 `Internal type` 註釋/型別、`import type` 行）。Delta（−276）本身係啱嘅。

**影響**：§6 呢張表喺呢個 repo 嘅定位係「**實測得出**、唔好再踩」—— 讀者係會照住個數去核對嘅。用最顯而易見嘅指令（`wc -l`）核對即刻對唔上，會令人懷疑成張表。**特別諷刺嘅係，同一個 commit 先啱啱剷走 `graphql.ts:318` 呢個失效數字，理由一模一樣。**

**方案 A**：改成 `wc -l` 量到嘅 `351 → 75`，並註明計法。
　*優點*：讀者一行指令核到。*缺點*：行數本身仍然會隨 codegen 版本／query 數量漂移。
**方案 B**：完全剷走絕對行數，只保留「`Maybe` / `InputMaybe` / `Scalars` 同所有 schema type 唔再產出，淨低 operation / fragment type 加 `*Document`」。
　*優點*：唔會再過時，同佢啱啱對 `:318` 做嘅嘢一致。*缺點*：少咗一個「縮咗好多」嘅直觀 signal。

**推薦：B**，可以加一句「縮減約 78%」保留量感而唔綁死行號。理由：呢個 commit 自己已經示範咗 §6 入面嘅絕對數字點樣變成負債；同一條 row 唔應該即刻再種一個。

### 🟢 Suggestion

**S-120** — `CLAUDE.md` §3 嘅本地 pip-audit 指令同 CI 唔等價
位置：`CLAUDE.md` §3。文件寫 `.venv/bin/pip install pip-audit -q && .venv/bin/pip-audit --progress-spinner=off`，但 CI 個 step 第一行係 `python -m pip install --upgrade pip setuptools`。跟住文件喺一個新 `.venv`（pip 24.0）行，會見到 **14 條紅**，而 CI 係綠 —— 我實測正正就係咁。下面個註釋有解釋，所以唔算陷阱，但「跟住 §3 行就等於跟住 CI 行」呢個 §3 嘅隱含契約破咗。建議喺指令本身加返 upgrade 一行（或者明文寫「想同 CI 一致就先 upgrade」）。

**S-121** — `docs/deployment.md` 講 scope 時漏咗 pip-audit 自己個 closure
`pages.yml` 好誠實咁寫明「Installing pip-audit puts its own closure (CacheControl, cyclonedx-python-lib, requests, rich, …) in that set too… a red here can in principle come from a package only pip-audit needs」。但 `deployment.md` 只寫「the declared dependencies, the `[dev]` extras and their transitives」。偏偏 `deployment.md` 自稱係「which threshold is in force is a policy fact… written down here」。既然特登認低咗呢個成本，唔應該喺個 policy 文件度跌咗佢。

**S-122** — 兩個 audit gate 都坐喺 `deploy` 上游，但兩份文件都冇講「可以 block deploy」
`build.needs = [lint-test, frontend]`、`deploy.needs = build`（我核實過）。兩個 gate 都**預期**會因為「上游啱啱出咗個 advisory」而紅（`pages.yml` 自己明文咁講），而呢個 repo 係每次 push `develop` 就 deploy。合起嚟即係：一條同本 repo 完全無關嘅 pip advisory，可以令**所有部署**停晒，包括一個 hotfix。呢個 trade `pages.yml` 認咗、`deployment.md` 亦寫咗「stops the workflow like any other check」—— 所以係**已知而且刻意**嘅決定，我唔當佢係缺陷。但兩份文件都冇出現「deploy」呢個字，而呢個先係最痛嗰半。建議：喺 `deployment.md` 明寫一句；如果想解耦，標準做法係另開一個 `schedule:` 嘅 audit workflow，令供應鏈漂移唔再坐喺部署路徑上（**唔係** `continue-on-error`，嗰個等於閹咗個 gate）。

**S-123** — Benchmark 讀數冇 assertion 釘住，同 oracle 讀數嘅待遇唔一致
`tests/test_common_geo.py` 個 `_unguarded_haversine` docstring 明寫「Kept as an independent oracle so the readings quoted … stay bound to an assertion (**W-017**)」—— 呢個做法非常好。但 `geo.py` 嘅 guard comment 講「the readings are in the ticket with the interpreter version they were taken on」，即係 benchmark 讀數**淨係**喺 ticket 度飄，冇任何嘢釘住。同一份檔案入面兩種讀數兩種待遇。我重量過所有數都仲準，所以唔緊急；但「W-017 只應用咗一半」值得記低。

**S-124** — 「int literal 貴 40%+」撈埋咗兩個效應
Lane 向 main agent 報嘅數。隔離出嚟：int-literal 效應單獨係 **+27%**（float-chained → int-chained）或 **+47%**（`abs`+float → `abs`+int），視乎同邊個比。`abs()` vs chained 係另一個獨立效應。Code comment 本身冇寫呢個 40%，所以只係 reporting 層嘅精度問題，唔使改 code。

---

## ✅ 做得好嘅地方

1. **「先 assert 兩條 arm 真係唔同，先至信『冇 diff』」** —— 呢個係本次最有價值嘅一點。一個 null result 冇 positive control 就等於冇資訊，我照佢個做法重跑，positive control 正正響（old 答 3937 km，new 掟 ValueError），之後 356 條 track / 97,003 點 零差異先至有意義。呢個係可以 generalise 去下次每一個「我改完嘢冇回歸」宣稱嘅方法。
2. **pip-audit 紅綠驗證特登用第三方 package 做紅種。** 呢個唔係隨手揀 —— 用 pip/setuptools 做紅種會俾 step 自己第一行 upgrade 洗白，變成一個 confounded experiment。我實測重現咗：pip/setuptools 已升級之後落 `jinja2==3.1.2` 仍然 exit 1，證明個 gate 嘅偵測力同 upgrade 係兩件事。
3. **推翻 main agent 嘅「向量化」建議，改為「取代而唔係疊加」。** 唔止唔加成本，仲同時收窄咗一個真漏洞（`1e308` 有限但令 `np.sin` 溢出返 `nan`，重開 CUI-0001 嗰個「track 悄悄報短」嘅形狀），而且 error message 對 `nan`/`inf` **一字不差**向後兼容。
4. **`kind` 分類把兩種失敗分開命名。** `non-finite` 同 `out-of-range` 指向完全唔同嘅根因（檔案損壞 vs 單位錯／經緯調轉），而 `parse_all` 會 log 呢句。兩個分類都有測試釘住（mutant 6 各殺一條）。
5. **Inclusive bound 有真承重測試。** `test_the_extremes_of_the_real_globe_are_still_accepted` 唔係擺設 —— `<=` 改 `<` 即刻 4 紅，全部係佢。極點同對蹠子午線係真地方，呢條測試就係守住呢件事。
6. **主動剷走 `graphql.ts:318` 呢個死 reference。** 冇人叫佢做，但 CUI-0049 令佢失效咗，唔剷就係留低一個誤導。
7. **`--ignore-vuln` allowlist 被明確否決，而且理由引返 W-034 / S-095 兩個同類 finding。** 「a list of exceptions drifts, and the gate then reports on the list rather than on the tree」—— 呢個判斷啱，而且係從本 repo 自己嘅歷史學返嚟。
8. **`npm audit` 由 `--omit=dev` 放闊到全樹，而且個 gate 真係企得住零。** 舊樹 14 條（exit 1）、新樹 0 條（exit 0），我兩邊都實跑過。呢個比 allowlist 嚴格，而且冇嘢要維護。

---

## 修正優先順序

| 次序 | Item | 類型 | 建議處理 |
|---|---|---|---|
| 1 | W-060 | 🟡 | 下一個 commit 修（`fix: W-060 \| …`）。方案 B。 |
| 2 | S-122 | 🟢 | 值得開 ticket 討論（部署路徑耦合），唔使即刻改 code |
| 3 | S-120 | 🟢 | 順手同 W-060 一齊改 `CLAUDE.md` |
| 4 | S-121 | 🟢 | 順手改 `docs/deployment.md` |
| 5 | S-123 / S-124 | 🟢 | 記錄即可，唔急 |

> 修正時每個 review item 一個獨立 commit：`fix: W-060 | CLAUDE.md §6 codegen 行數改用 wc -l 可核實嘅寫法`。

---

## 修訂後代碼

**冇修訂代碼。** 六個 W/S 全部落喺 `CLAUDE.md` 同 `docs/deployment.md`，`src/common/geo.py`、`tests/test_common_geo.py`、`.github/workflows/pages.yml`、`frontend/package.json` **一個字都唔使改**。W-060 嘅建議改法（方案 B）：

```markdown
| `@graphql-codegen/client-preset` 6 唔再產出 schema-wide type | CUI-0049 由 client-preset 4
bump 上 6 之後，`frontend/src/gql/graphql.ts` 縮減約 78%：`Maybe` / `InputMaybe` / `Scalars`
同所有 schema type（`Activity`、`TrackPoint` …）唔再產出，淨低 operation / fragment type 加
`*Document`。今日只有 `frontend/src/data/api/queries.ts` 用 `@/gql`（而且淨係攞個 `graphql`
helper），所以 `tsc -b` 實測零錯；但如果將來想 `import type { Activity } from "@/gql"`，佢已經
唔存在，要自己喺 `src/data/types.ts` 寫。另外 `ID` input 而家係 `string | number`（舊版係
`string`） |
```
（即係只剷走 `由 345 行縮到 69 行` 六隻字，換成 `縮減約 78%`。）

---

## 環境註記

- 本 worktree 起初喺 `2ff659e`，已 fetch + checkout 到 `86ca2ad`，全程喺該 commit 上做。
- 我喺 worktree 建立咗 `.venv/`（`CLAUDE.md` §3 指定做法）同 `frontend/node_modules`、`frontend/src/gql` —— **三個都係 git-ignored**，`git status --short` 由頭到尾空。
- Mutation 用嘅臨時檔全部喺 `/tmp`（`/tmp/geo_orig_backup.py`、`/tmp/oldfe/`、`/tmp/stub/`、`/tmp/actionlint`），冇一個喺 project 入面。
- **冇 dispatch 任何 workflow**；CI 驗證全部靠 `actionlint`、parsed-YAML 結構比對、同埋把 `run:` body 原文用 `bash -e -c` + stub 真行。

```handoff-receipt
protocol: 1
status: pass
score: 90/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "96%"
next_action: merge_develop
next_agent: quality-assurance
branch: "claude/ai-dev-team-start-05jie2 @ 86ca2ad"
context: "CUI-0047 範圍檢查取代 isfinite（等價性 19/19 驗證、6 個 mutant 兩方向全殺、真數據 388k 座標分量零回歸）；CUI-0049 codegen/vitest bump 令 npm audit 全樹 0；CUI-0044 pip-audit 入 CI（upgrade 後 clean，第三方紅種驗過 fail-closed）。build/deploy 嘅 refs/heads/develop 守衛同 pyproject.toml 皆未被碰。0 Critical，1 Warning（W-060：CLAUDE.md §6 codegen 行數 345→69 重現唔到，wc -l 實測 351→75），5 Suggestion（S-120–S-124），全部落喺文件唔涉代碼。"
blockers:
  - "W-060：CLAUDE.md §6 client-preset 6 嗰 row 寫嘅 345→69 行同 wc -l 實測 351→75 對唔上（兩邊都差 6 行 preamble）；建議剷走絕對行數改寫成『縮減約 78%』"
```
