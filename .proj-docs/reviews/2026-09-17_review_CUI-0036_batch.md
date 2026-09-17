# Batch Review — 2026-09-17 — CUI-0036 / CUI-0042 / CUI-0035（CI + coverage lane）

> ⚠️ **ID 已於 merge 時重新編號**（三個 reviewer 並行，範圍要統一）：
>
> | 原 | 改為 |
> |---|---|
> | W-042 | **W-034** |
> | S-093 … S-100 | **S-092 … S-099** |
>
> 正文其餘部分一字未動。

---


**報告檔案：未建立。** 我嘅 harness 有 `project-boundary` hook，只准寫入 project folder；而任務明文禁止改任何 project 檔案，所以我冇寫 `.proj-docs/reviews/2026-09-17_review_CUI-0036_batch.md`。以下係完整報告原文，請原文寫入該路徑。

**HEAD（第一行，按要求）**：我開場見到嘅係 **`2ff659e`**（`Merge pull request #10 from dcwhung/chore/claude/ai-dev-team-plugin`）—— 一如警告，陳舊。已 `git fetch origin claude/ai-dev-team-start-05jie2` 再 `git checkout 06ad4bd`，之後全部驗證喺 **`06ad4bd`** 上做。

**Review Item ID 範圍（明文聲明）**：本報告用 **S-092 – S-100**、**W-034**（保留 W-034–W-049）、**C-011 起**（本輪冇 Critical，一個都冇用）。呢三段同另外兩位並行 reviewer 唔會撞。

---

## 整體 verdict

| 項目 | 值 |
|---|---|
| 涵蓋 commit | `80e765b`（CUI-0036）、`c8b507c`（CUI-0042）、`8758279`（CUI-0035） |
| Review base / target | `6576e56` → `06ad4bd`（四 lane merge） |
| Hard gates | **全部 pass**（下表） |
| 整體 score | **87 / 100** |
| 🔴 Critical | **0** |
| Status | **⚠️ warn** |

> 一句話：**lane 自報嘅八條推翻，我逐條獨立實測，八條全部成立，冇一條吹水。** 呢個 lane 嘅事實紀律係本 session 見過最好嘅之一 —— 佢連自己差啲寫出嚟嘅假綠測試都自己捉返出嚟，而我驗證咗嗰個捉法係真嘅有牙。唯一擋住 90 分嘅係一句**寫得太闊嘅 comment**，而嗰句 comment 偏偏係一個 security gate 嘅理由書。

---

## Hard Gates

| Gate | 指令（實跑） | 結果 |
|---|---|---|
| Lint (py) | `.venv/bin/ruff check src tests` | ✅ `All checks passed!` exit 0 |
| Lint (py, api/) | `.venv/bin/ruff check api` | ✅ exit 0（S-077 pin 仍生效） |
| Format | `.venv/bin/ruff format --check src tests` | ✅ `60 files already formatted` exit 0 |
| Lint (fe) | `npm run lint` | ✅ exit 0 |
| Type check | `npm run typecheck`（先跑 codegen） | ✅ exit 0 |
| Tests (py) | `.venv/bin/python -m pytest tests -q --cov` | ✅ **492 passed** in 160.73s |
| Tests (fe) | **`npm test`** | ✅ **141 passed / 25 files**（24.15s） |
| Coverage | 同上 | ✅ **TOTAL 1778 / 63 / 96%** |
| SDL sync | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ `frontend/schema.graphql is up to date` |
| Security scan | `cd frontend && npm audit --omit=dev` | ✅ `found 0 vulnerabilities` exit 0 |
| Build | `npm run build` | ✅ exit 0（293 modules） |
| No Critical | — | ✅ 0 |

任務書畀嘅五個數字（492 / 141 / 25 / 96% / audit 0）**逐個對得返**。

---

## 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|---|---|---|---|
| 正確性（Correctness） | 23 | 25 | S-092、S-093 各 −1；四個 workflow case 全部實跑對 |
| 安全性（Security） | 14 | 20 | **W-034 −5**（gate 理由書寫闊咗）、S-096 −1 |
| 可維護性（Maintainability） | 15 | 20 | S-094 / S-095 / S-097 / S-098 / S-100 各 −1 |
| 測試覆蓋（Test Coverage） | 15 | 15 | 滿分 —— 見下方「做得好」 |
| 性能（Performance） | 10 | 10 | 無 runtime 影響 |
| 代碼風格（Code Style） | 10 | 10 | ruff / format / 命名全合規 |
| **總分** | **87** | **100** | |

**結果：⚠️ warn**（`hard_gates` 全 pass + 75–89 + 0 Critical）

---

## 逐條裁決：lane 自報嘅八個推翻

> 全部由我獨立重跑，唔信 lane 任何一句。

### ① 「同 pip-audit 對稱」要收窄 — ✅ **成立**

```
git grep -n "pip-audit" 6576e56 -- .github/          → exit 1（零命中）
git grep -n "pip-audit" 06ad4bd -- .github/          → 唯一命中 = 本 lane 自己新加嗰句 comment
git ls-tree -r --name-only 6576e56 -- .github        → 得 2 個 workflow 檔
git grep -niE "audit|dependabot|codeql|snyk|trivy" 6576e56 -- .github/ → exit 1（零命中）
```

Base commit 嘅 `.github/` **完全冇任何依賴審計 / SCA 機制**。呢個 step 的確係本 repo **第一個**入 CI 嘅依賴審計。Lane 喺 workflow comment 寫錯咗再自己更正 —— 更正嗰版先啱。

### ② 票建議 `--audit-level=high` 冇用 — ✅ **成立**

```
npm audit                          → 14 vulnerabilities (2 moderate, 12 high)   exit 1
npm audit --audit-level=high       →                                            exit 1
npm audit --omit=dev               → found 0 vulnerabilities                    exit 0
```

嗰 12 條就係 high，`--audit-level=high` 一樣紅。票嘅建議 (1) 直接採用會即刻 park CI 喺紅。

### ③ dev 依賴數 619 vs 625 — ✅ **成立（625）**

`npm audit --json` metadata：`{prod: 16, dev: 625, optional: 82, peer: 8, peerOptional: 0, total: 640}`。任務書寫嘅 **619 係錯**，prod **16** 兩邊一致。

順帶逐條核 advisory 入口（`npm audit --json` 拆 `via`）：11 條 `@graphql-codegen/*` + 1 條 `lodash` = **12 high**；`vitest` + `@vitest/mocker` = **2 moderate**。三個 direct 入口（`@graphql-codegen/cli`、`@graphql-codegen/client-preset`、`vitest`）**全部係 devDependencies**。票嘅明細表逐行成立。

### ④ coverage TOTAL 數字 — ✅ **成立（票兩個數字都錯）**

喺 lane tip `8758279` 實跑：

| | Stmts | Miss | Cover |
|---|---:|---:|---:|
| `--cov`（src + api） | **1696** | **81** | 95% |
| src-only（同一份 data 切片） | **1652** | **81** | 95% |

票寫嘅 `1632/82` 同 `1676/105` **兩個都唔啱**，lane 量到嘅 1652/81 同 1696/81 **啱**。

### ⑤ 「TOTAL 會跌到 94%」冇發生 — ✅ **成立，而且我驗到比 lane 講嘅更實**

呢條係本輪最值得驗嘅，所以我同時驗咗**兩邊**：

喺 base `6576e56`（即**未加測試**）跑 `pytest tests -q --cov=src --cov=api`：

```
api/graphql.py     44    23    48%    45-52, 57-78, 89-90
TOTAL            1696   104    94%
418 passed
```

**票預測嘅 94% 係完全正確嘅** —— 只要分母改動單獨做，TOTAL 就係 1696/104/94%。而 lane 先補測試（`api/graphql.py` 由 23 miss → **0 miss**），令加分母只係 +44 statements / +0 miss，TOTAL 停喺 95%。

亦即係話：**票冇錯，lane 亦冇錯，兩者講緊兩個唔同次序。** Lane 喺 commit message 同 pyproject comment 嘅措辭（"which assumed the denominator change on its own"）準確咁講清楚咗呢一點，冇踩低張票。呢個係好嘅 review 態度。

補充：喺 merge `06ad4bd` 同一結論成立 —— src-only 1734/63/96%，src+api 1778/63/96%，`api/graphql.py 44 0 100%`。

### ⑥ 「全 repo 覆蓋率第二低」唔成立 — ✅ **成立（係第三低）**

Base `6576e56`，api/ 入分母：

```
src/cli/export_schema.py        20  20    0%
src/cli/collect_weather.py      33  18   45%
api/graphql.py                  44  23   48%   ← 第三低
src/cli/process_activities.py   39  17   56%
src/cli/export_data.py          48  11   77%
src/weight/analysis.py          60  12   80%
```

排名逐個對得返 lane 講嘅。

### ⑦ 差啲寫咗假綠測試 — ✅ **成立，我獨立重現咗個假綠**

我獨立造咗個 mutant：**guard 完好、package 真係 importable**（即 `_load_package_from_source` 應該早退、乜都唔做），然後同時量兩個 assertion：

```
installed run365days.__file__ = <repo>/src/__init__.py
is it src/__init__.py? -> True

MUTANT = guard intact, package IS importable (loader must NOT run)
  identity assertion (shipped):  loaded is not installed -> False   ← FAIL = 有牙
  path assertion (discarded):    True                               ← 照樣 PASS = 假綠
```

Editable install 之下 `run365days.__file__` **本身就係** `src/__init__.py`，所以單靠比 path 分辨唔到「載咗 src/」定「本來就係 src/」—— lane 講得一字不差。改用 module identity（`loaded is not installed`）係**真係有牙**嘅寫法。

而且呢對測試兩邊都釘死：`test_...registers_src...`（find_spec→None，要求新 object）同 `test_an_installed_package_is_left_alone`（要求同一 object）。刪 guard → 後者紅；guard 改成無條件 return → 前者紅。

順帶查咗污染風險：`src/__init__.py` 係**純 docstring、11 行、零 import**，所以 re-exec 冇 side effect；`monkeypatch.setitem` 亦會喺 teardown 還原。全 suite 492 綠亦印證冇洩漏。

### ⑧ 刻意唔做票建議 (3)（寫入 `docs/deployment.md`）— ✅ **判斷正確，但有部分保留**

`docs/deployment.md:87-91` 原文核實：

> "This section describes only the shape, which is what a reader needs before opening it — **a prose copy of the branch lists and job order has gone stale twice already, so it is deliberately not kept here.**"

**拒絕抄 step list 係啱嘅**，而且係有紀錄支撐嘅拒絕，唔係躲懶。

但同一 section 嘅結尾自己開咗個口：

> "Both are **shape facts rather than job steps**, which is why they are written down here."

「有一個依賴審計 gate，而佢今日嘅 threshold 係 `--omit=dev`、bump 之後要放寬」—— 呢個係 **shape fact / policy**，唔係 job-step 抄寫，啱啱好落喺該檔案自己畫嘅例外入面。所以 lane 將「唔抄 step list」呢個正確判斷，**輕微地過度延伸**到「連 threshold 政策都唔記錄」。詳見 S-094（🟢，唔阻）。

---

## 額外核實（任務書第 2–4 點）

### `npm audit --omit=dev` 真係分得開紅綠 — ✅ 我自己造咗個紅

唔止驗「全樹紅 / prod 綠」，我另外造咗一個**真正嘅 runtime 暴露**（喺 scratchpad 複製 `package.json` + `package-lock.json`，**零 repo 改動**），把一個有 advisory 嘅 package 提升入 `dependencies`：

```
npm install --package-lock-only --omit=dev lodash@4.17.21
npm audit --omit=dev
  → Lodash has Prototype Pollution Vulnerability ... GHSA-xxjr-mmjv-4gpg
  → 1 high severity vulnerability
  → exit 1
```

**個 gate 唔係 no-op** —— runtime 依賴一有 advisory 就會紅。呢個係票 §驗證方式最後一條（「故意 downgrade 嘅 branch 要紅」）嘅離線等價驗證。

### `--omit=dev` 會唔會漏走真係入 bundle 嘅嘢 — ⚠️ **會，我實測到**（→ W-034）

跑 `npm run build` 再驗 `dist/`：

```
dist/assets/index-*.js   → "modulepreload" 命中 3 次
   前兩個係 vite 個 modulepreload-polyfill 原文：
   (){const t=document.createElement("link").relList;if(t&&t.supports&&t.supports("modulepreload"))return; ...
   第三個先至係 react-dom（runtime dep）
grep -rn "modulepreload" frontend/src frontend/index.html frontend/vite.config.ts → 零命中
   ⇒ 呢段代碼唔係 app 寫嘅，係 vite package 注入嘅

dist/assets/index-*.css  21.64 kB → 全份由 tailwindcss 生成
   @layer properties{...--tw-rotate-x:initial;--tw-skew-y:initial;...}
```

`vite`、`tailwindcss`、`@tailwindcss/vite`、`@vitejs/plugin-react` **全部係 devDependencies**，但佢哋嘅產出**真係去到瀏覽器**。所以 `--omit=dev` 審嘅係「declared `dependencies` closure」，唔係「ships to a browser」。

（今日嗰 14 條唔受影響：codegen 出嘅係會被擦走嘅 TS type，vitest 乜都唔出。所以 **gate 嘅行為係啱嘅，錯嘅淨係解釋佢嘅嗰句**。）

### CUI-0042 三層靜態方法重做 — ✅ 四個 case 全部重現

**(a) actionlint**

```
actionlint --version → 1.7.7 (installed by downloading from release page, go1.23.4, linux/amd64)
actionlint -no-color -verbose tag-release.yml pages.yml
  verbose: Rule "shellcheck" was disabled: exec: "shellcheck": executable file not found in $PATH
  verbose: Rule "pyflakes" was disabled: exec: "pyflakes": executable file not found in $PATH
  verbose: Found 0 errors in 2 files
  exit 0
```

`command -v shellcheck` → 空。**Lane 嗰句 shellcheck caveat 完全誠實**，而且 actionlint 自己 verbose 輸出白紙黑字印證咗個 rule 係 disabled 而唔係 pass。我亦試過 `pip install shellcheck-py` 補返 —— 裝唔到，所以呢個 caveat 喺我度一樣成立。（bonus：`pyflakes` 都係 disabled，lane 冇提，但兩個 workflow 都冇 inline python `run:`，無關痛癢。）

**(b) Step gating（由 parsed YAML 讀出嘅原文 `if:`）**

```
actions/checkout@v4                           | None
Validate tag name                             | None
Create and push annotated tag                 | '${{ !inputs.notes_only }}'
Extract release notes from docs/CHANGELOG.md  | '${{ inputs.release || inputs.notes_only }}'
Create GitHub Release                         | '${{ inputs.release && !inputs.notes_only }}'
Update the published Release notes            | '${{ inputs.notes_only }}'
```

**(c) `Validate tag name` 個 `run:` body 原文用 `bash -e -c` 真行**（`PATH` 前面放假 VCS stub 扮 tag 有／冇）：

```
A notes_only=false / tag 唔存在  → exit 0, ''
B notes_only=false / tag 存在    → exit 1, "Tag v3.2.0 already exists on origin
                                            To correct an already-published Release's notes, re-run with notes_only: true"
C notes_only=true  / tag 存在    → exit 0, "notes_only: v3.2.0 exists; the tag will not be touched"
D notes_only=true  / tag 唔存在  → exit 1, "notes_only asked to correct v3.2.0, but no such tag exists on origin"
tag 名唔合法（兩個 mode）        → exit 1, "Tag must look like v1.2.3, got '3.2.0'"
```

合埋 (b)+(c)，四個 case 嘅實際控制流：

| Case | 行到嘅 step |
|---|---|
| **A** | checkout → Validate(0) → **Create tag** → **Extract notes** → **Create Release** → *skip* Update |
| **B** | checkout → Validate **exit 1** → 之後全部唔行 |
| **C** | checkout → Validate(0) → *skip* Create tag → **Extract notes** → *skip* Create Release → **Update notes** |
| **D** | checkout → Validate **exit 1** → 之後全部唔行 |

**同 lane 張表一字不差。** A path 同改動前逐步一致（新增嘅只係 `exists=false` 分支，喺 A 度乜都唔做）。

**(d) 「冇 dispatch 過任何 workflow」— ✅ 用 GitHub API 獨立核實**

`actions_list(list_workflow_runs, tag-release.yml)` → **`total_count: 6`**，最新一 run 係 `2026-09-16T22:21:14Z`（v3.2.0 release，`triggering_actor: dcwhung`）。**2026-09-17 零 run。** Lane 嘅 commit message 同 ticket 都主動寫明「no workflow was dispatched」，同 API 紀錄對得返 —— 冇聲稱行過。

### CUI-0035 冇打爛部署 — ✅ 四項全部確認

```
git diff --exit-code 6576e56..06ad4bd -- api/graphql.py   → exit 0（一個 byte 都冇改）
grep -n "^app = _build_app()" api/graphql.py              → 95:app = _build_app()   （top-level、unindented）
grep -n "known-third-party" pyproject.toml                → 121:known-third-party = ["graphql"]   （S-077 未動）
ruff check api                                            → All checks passed!
```

`pyproject.toml` 嘅**全部**改動（`git diff` 只計 ±行）：

```
+ 8 行 comment
- source = ["src"]
+ source = ["src", "api"]
```

即係話只掂過 `[tool.coverage.run]`，一個 `[tool.ruff]` / `[project]` / dependency 都冇郁。`api/graphql.py` 亦**一行未改**，Vercel function 偵測（CLAUDE.md §6）零風險。

---

## Section A — CUI-0036（`pages.yml` + `CLAUDE.md`，commit `80e765b`）

**改動**：`frontend` job 喺 `Install` 之後、`Lint` 之前加 `Audit runtime dependencies` step（`npm audit --omit=dev`）＋ 17 行 comment；`CLAUDE.md` §3 加一行。

**評分：84 / 100** — 主要扣喺 W-034。

### 🟡 W-034｜`pages.yml` step comment 把 `--omit=dev` 嘅覆蓋面講闊咗

**位置**：`.github/workflows/pages.yml:83-84`

```yaml
# --omit=dev audits what actually ships to a browser: the eight runtime
# dependencies in package.json, 16 packages resolved.
```

**描述**：「8 個 declared / 16 個 resolved」我核實係啱嘅。但「what actually ships to a browser」呢句唔準確 —— 上面實測顯示 vite 個 `modulepreload-polyfill` 同成份 21.64 kB Tailwind CSS 都**真係 ship 咗**，而佢哋全部嚟自 devDependencies，被 `--omit=dev` 排走咗。準確講法係：呢個 gate 審嘅係 **declared `dependencies` closure**，係「入 bundle 嘅嘢」嘅一個好 proxy，但唔係同一個 set。

**影響**：呢句 comment 正正就係將來有人決定「而家可唔可以繼續收窄」/「幾時該放寬」時會讀嘅嗰句。一句過闊嘅理由會令下一位覺得 build tool 唔使審，而 supply-chain 攻擊近年恰恰鍾意打 build tool。今日零實際影響（14 條 advisory 冇一條喺 vite / tailwind；codegen 出嘅 type 會被擦走、vitest 乜都唔出），所以呢個係**理由書**嘅問題，唔係 gate 行為嘅問題 —— 亦即係 CLAUDE.md §6 一路記錄緊嗰個「代碼啱但解釋佢點解啱嗰句寫錯」嘅缺陷類別。

**方案 A**：改寫第一句，講返實際 scope 同已知缺口 ——
```yaml
# --omit=dev audits the declared `dependencies` closure: the eight runtime
# dependencies in package.json, 16 packages resolved. That is the browser-facing
# surface minus the build tools that emit INTO the bundle -- vite ships its
# modulepreload polyfill and tailwindcss generates the whole stylesheet, and
# both are devDependencies. Widening past that means auditing the full tree.
```
*Trade-off*：純文字改動，零行為改變，但 comment 再長 4 行（呢個檔案已經好多 comment）。

**方案 B**：保留措辭，另加一個明確 allow-list comment 列出「已知會 emit 入 bundle 但唔喺 audit 範圍」嘅 devDependency（`vite` / `tailwindcss` / `@tailwindcss/vite` / `@vitejs/plugin-react`）。
*Trade-off*：更精確、將來加 build plugin 時有個 checklist 對；但係多咗一張要人手維護嘅名單，即係多一個 drift 源 —— 同本 repo（CUI-0024 / CUI-0031）嘅教訓相反。

**推薦：方案 A。** 唔引入新名單，直接修正嗰句 over-claim，同時把「缺口喺邊」寫死俾下一位。

### 🟢 S-096｜Python 側依然冇 audit gate

**位置**：`.github/workflows/pages.yml`，`lint-test` job

票建議 (1) 係「`pages.yml` 加 `npm audit`（前端）**同 `pip-audit`**（Python）」。今次只補咗前端一半，而 Python 側先係真正部署落 Vercel 嗰半（Flask / Strawberry 係 core dependency，直接入 production runtime）。Step comment 有明寫 pip-audit 從來冇入過 CI，所以**唔算隱瞞**，只係 gate 補咗一半。

**方案 A**：開 follow-up ticket，喺 `lint-test` job 加 `pip-audit`（CUI-0032 人手行過係零漏洞，所以今日加落去係綠嘅，唔會 park CI 紅）。
**方案 B**：而家順手加埋。

**推薦：方案 A。** User 明確只揀咗 `npm audit --omit=dev`（ticket §執行紀錄有記），lane 守住範圍係啱嘅；但呢個缺口要有張飛，唔好靠記性。

### 🟢 S-095｜`CLAUDE.md` §3 嗰句只講 12 high，漏咗 2 moderate

**位置**：`CLAUDE.md:68`

```
npm audit --omit=dev       # CI gate：只審 runtime 依賴（dev 樹嘅 12 high 係 pre-existing，見 CUI-0036）
```

實測全樹係 **14 條（12 high + 2 moderate）**。寫「12 high」少報咗兩條。更值得講嘅係：**同一個 lane 喺 `pyproject.toml` 嘅 comment 度啱啱先論證過「hardcoded number 會 drift」**（引 CUI-0024 / CUI-0031），然後轉個頭喺 `CLAUDE.md` hardcode 咗一個會 drift 嘅 advisory 數。

**方案 A**：改成「dev 樹嘅 14 條（12 high / 2 moderate）係 pre-existing」。
*Trade-off*：準確，但 bump 一落地即刻 stale。

**方案 B**：改成「dev 樹嘅 advisory 係 pre-existing，見 CUI-0036」，唔寫數。
*Trade-off*：少咗即時資訊量，但同 lane 自己喺 pyproject 立嘅原則一致，而且個數本來就要去 CUI-0036 睇。

**推薦：方案 B。**

### 🟢 S-098｜Audit step 擺喺 Lint 之前，紅咗會遮走其餘全部前端訊號

**位置**：`.github/workflows/pages.yml:97`（`Install` 同 `Lint` 之間）

一條 advisory 落地，`Lint` / `Typecheck` / `Unit tests` / 兩個 `Build` 全部唔會行，個 PR 得一個 audit 錯誤睇。

先講清楚我**唔**當呢個係 flakiness 問題：上面嗰個 `npm ci` 已經要 registry，registry 冧嘅話 job 喺 `npm ci` 就死咗，所以 audit step **冇新增**網絡依賴。淨係「遮走訊號」嗰半成立。

**方案 A**：把 step 移到 job 最尾（兩個 `Build` 之後）。照樣係 hard gate，但其餘訊號已經報晒先至紅。
*Trade-off*：一行次序改動，零行為改變；代價係 audit 要等成個 job 行完先知，回饋慢 1–2 分鐘。

**方案 B**：留喺原位（fail-fast on security）。
*Trade-off*：安全訊號最快，但每次 advisory 落地都要 developer 修完先見到其他嘢。

**推薦：方案 A** —— 呢個 gate 預期會因為 upstream advisory（唔關本次 PR 事）而紅，唔係因為 PR 本身，所以早 fail 嘅價值細過遮走訊號嘅代價。（主觀成分：兩個都合理，我個人傾向 A。）

---

## Section B — CUI-0042（`tag-release.yml`，commit `c8b507c`）

**改動**：加 `notes_only` boolean input（方案 A）；`Validate tag name` 分兩路；三個既有 step 改 `if:`；新增 `Update the published Release notes` step。

**評分：92 / 100** —— 本 batch 最乾淨嗰個。

採用方案 A、明確唔做方案 B，理由（「會令『tag 已存在』由錯誤變成靜默正常情況」）寫喺 step 隔籬 comment，同票嘅論證一致。`GH_TOKEN: ${{ github.token }}` 同 `TAG` 都喺 **job-level `env:`**，所以新 step 攞得到 token；`permissions: contents: write` 足夠 `gh release edit`。呢兩點我特登查咗 —— 新 step 攞唔到 token 會係一個 runtime-only 嘅 🔴，而佢冇中。

### 🟢 S-092｜`notes_only: true` + `release: false` 一樣會寫 Release

**位置**：`.github/workflows/tag-release.yml`，`Update the published Release notes` 嘅 `if: ${{ inputs.notes_only }}`

由 parsed YAML 讀出嚟嘅 gating：`release=false, notes_only=true` → `Extract release notes` 行（因為 `||`）、`Update the published Release notes` **行**。即係 dispatcher 就算 untick 咗 `release`，個 Release body 一樣會被改寫。

呢個係**刻意**嘅，header comment 明寫「`release` is ignored when notes_only is true: notes are the whole point of it」，所以唔算隱藏行為。但 `release` 個 input description 自己冇講，喺 dispatch UI 上睇唔到。

**方案 A**：維持現狀（已喺 header 記錄）。
*Trade-off*：零改動；代價係要揭到 workflow 頂先知。

**方案 B**：`if: ${{ inputs.notes_only && inputs.release }}`。
*Trade-off*：語義上更符合直覺；但會令 `notes_only` 要剔兩個掣先生效，而 `release` 預設已經係 `true`，實際只會製造「點解剔咗 notes_only 乜都冇做」嘅困惑。

**推薦：方案 A**，另外把 `release` 個 `description` 尾加「(ignored when notes_only is true)」—— 一行，喺 dispatch UI 直接見到。

### 🟢 S-093｜反向守衛驗嘅係 tag 存在，唔係 Release 存在

**位置**：`Validate tag name`，`notes_only` 分支

Step comment 講個守衛係擋「editing the notes of a release that was never cut」，但實際 assert 嘅係 `ls-remote` 見唔見到個 tag。用 `release: false` 建過 tag（今日做得到）就會出現「tag 在、Release 唔在」，嗰陣 Validate 會放行，最後死喺 `gh release edit` 嘅 "release not found"。

影響細（一樣係紅、訊息一樣清楚），但死喺一個冇為呢件事而設嘅地方，而唔係嗰個特登為佢寫嘅守衛度。

**方案 A**：`notes_only` 分支加一句 `gh release view "$TAG" >/dev/null || { echo "..."; exit 1; }`。
*Trade-off*：一行，令 comment 講嘅「mirror-image mistake」真係成立；代價係 Validate step 開始依賴 `gh`（而家只靠 VCS 二進位），本地 reproduce 嘅成本高咗少少。

**方案 B**：唔改，把 comment 收窄成「the TAG must exist」。
*Trade-off*：零風險；但個守衛就唔再係 `create` 那邊嘅完整鏡像。

**推薦：方案 A** —— `gh` 已經係同一個 job 嘅前提（`GH_TOKEN` 喺 job env），唔算新依賴。

### 🟢 S-094｜票建議 (3) 有一半其實落得到 `docs/deployment.md`

見上面 ⑧ 嘅裁決。`docs/deployment.md:87-91` 拒絕嘅係「a prose copy of the **branch lists and job order**」，而同一 section 結尾自己寫明「Both are **shape facts** rather than job steps, which is why they are written down here」。「有一個依賴審計 gate、今日 threshold 係 `--omit=dev`、bump 之後放寬」屬於 shape fact / policy。

**方案 A**：喺 `## GitHub Pages` section 加兩句，只講 gate 存在同 threshold 政策，唔列 step。
*Trade-off*：符合該檔案自己嘅例外；代價係多咗一個要同 workflow 同步嘅地方（雖然 threshold 改動頻率遠低過 step list）。

**方案 B**：維持現狀（理由寫喺 workflow step 隔籬 + `CLAUDE.md` §3）。
*Trade-off*：零 drift；但一個讀 `deployment.md` 想知「呢個 repo 有冇 SCA」嘅人會得出「冇」嘅結論。

**推薦：方案 A**，而且只寫 policy 唔寫指令。（呢條主觀成分最重 —— lane 選 B 唔算錯，我只係覺得該檔案自己開咗個口。）

---

## Section C — CUI-0035（`tests/test_api.py` + `pyproject.toml`，commit `8758279`）

**改動**：5 條新測試蓋 `api/graphql.py` 嘅 start-up failure path；`[tool.coverage.run] source = ["src", "api"]`。

**評分：88 / 100**

5 條測試我逐條讀過同單獨跑過：

```
pytest tests/test_api.py -q -k "failed_start_up or failure_report or flask_itself or registers_src or installed_package"
  → 5 passed, 192 deselected in 0.89s
```

覆蓋嘅係 `_error_app` 嘅 except 分支（`57-78`）、Flask reporting app、catch-all route（`/`、`/<path:path>`，經 `GRAPHQL_PATH` / `/` / `/api/anything` 三條路驗 vercel.json rewrite）、flask 唔 importable 時嘅 raw WSGI 第二層（`monkeypatch.setitem(sys.modules, "flask", None)` —— 呢個係令 `from flask import ...` 真係掟 ImportError 嘅正路做法），同 `_load_package_from_source` 兩邊。`api/graphql.py` 由 `44/23/48%` → `44/0/100%`，我喺 lane tip 同 merge 兩處都量到 100%。

測試係經 `_load_vercel_entry` `exec_module()` **真嗰個 shipped file**，唔係 copy —— 呢個係好設計，代表測試唔會同 `api/graphql.py` 分歧。

### 🟢 S-100｜`pyproject.toml` comment 入面嘅絕對數字喺 merge 一刻已經 stale

**位置**：`pyproject.toml:78`

```
# (S-066, CUI-0035). Adding it moved TOTAL from 1652/81/95% to 1696/81/95%
```

呢兩個數字喺 lane branch `8758279` 上**係啱嘅**（我實測 1652/81 同 1696/81，一個 digit 都冇錯）。但喺 merge `06ad4bd` 上，同批另外三條 lane 加咗 src 代碼，實際係 **1734/63/96%** → **1778/63/96%**。即係話呢個 comment 一入 develop 就已經同現實對唔上。

要講公道：呢個 drift 唔係 lane 量錯，係並行 merge 造成，lane 冇可能喺自己條 branch 上預知。所以我擺 🟢 唔擺 🟡。但同一段 comment 上面三行啱啱先寫「a threshold is one more hardcoded number that drifts out of step with the thing it describes (CUI-0024, CUI-0031)」，而跟住就 hardcode 咗兩個會 drift 嘅數 —— 呢個自我矛盾值得記低。

**方案 A**：刪走絕對數字，只保留不變嘅結論 ——「加 `api/` 之後 TOTAL 百分比唔變，因為 `api/graphql.py` 已經 0 miss；用 `pytest tests -q --cov` vs `--cov=src` 自己對」。
*Trade-off*：永不 drift、同 lane 自己立嘅原則一致；代價係失去「當時量到幾多」嘅歷史紀錄（不過嗰個紀錄喺 ticket 同 commit message 都有）。

**方案 B**：保留數字但釘死 commit：「measured at `8758279`」。
*Trade-off*：歷史可追；但讀嘅人仲係要自己 checkout 嗰個 commit 先 reproduce 到，實用價值低。

**推薦：方案 A。**

### 🟢 S-097｜5 條新測試放喺一個 3,166 行嘅檔案度

`tests/test_api.py` 喺 merge 後係 **3,166 行 / 197 個測試**。CLAUDE.md §2 講「一個 feature module 一個檔案」，而 `api/graphql.py`（Vercel entry）嚴格嚟講唔係 `src/api/` 嘅一部分 —— 佢係 deploy shim。新測試放喺既有 `_load_vercel_entry`（AU-001）隔籬係合理嘅局部決定，但成個檔案已經好難 navigate。

**方案 A**：抽 `tests/test_vercel_entry.py`，連同既有兩條 GraphiQL entry 測試同 `_load_vercel_entry` helper 一齊搬。
*Trade-off*：`test_api.py` 即刻細一大截、entry 嘅測試集中；代價係要搬 `VERCEL_ENTRY` / `GRAPHIQL_ENV` 等共用常數，而且呢個 lane 唔應該順手做（超出票範圍）。

**方案 B**：維持現狀。
*Trade-off*：零風險；檔案繼續大。

**推薦：方案 B（本 lane）+ 方案 A 開飛另做。** 呢個係 pre-existing 問題，唔應該記喺呢個 lane 數上。

---

## ✅ 做得好嘅地方（跨 fix 通用）

1. **八條自報推翻，八條成立。** 我逐條用獨立方法重跑（`git grep` / `npm audit --json` / 兩個 commit 上跑 coverage / GitHub Actions API / 自造 mutant），冇一條要打回頭。呢個 session 已經有五次下游用實測推翻上游 —— 呢條 lane 冇成為第六次。

2. **自己捉到自己個假綠，而個捉法係真嘅。** ⑦ 嗰條：editable install 之下 `run365days.__file__` 本身就係 `src/__init__.py`，path 比對分辨唔到嘢。我獨立造 mutant 重現咗「path assertion 照樣綠、identity assertion 會紅」。呢個正正係 CLAUDE.md §6 「Mutation testing 用 stale `.pyc`」嗰條所講嘅方法（斷言 mutant 真係生效、用獨立 oracle 驗），而唔係背一句補救指令。

3. **次序本身就係設計。** CUI-0035 先補測試、後改分母，令票預測嘅 94% 冇發生。而我驗到票嘅 94% 預測**本身係啱嘅**（base + api/ 未加測試 = 1696/104/94%）。Lane 冇踩低張票，只係準確咁講清楚兩者講緊唔同次序 —— 呢種措辭紀律好過「票寫錯咗」。

4. **靜態驗證做到盡而唔越界。** CUI-0042 冇 runner、冇網絡、冇 `gh`，lane 用三層方法把四個 case 追到實際輸出，而且**主動聲明** shellcheck 冇行過。我核實咗 actionlint 真係印 `Rule "shellcheck" was disabled`，亦核實咗 GitHub 上 `tag-release.yml` 由 2026-09-16 22:21 之後零 run。承認工具限制，比扮驗過有價值得多。

5. **部署面零風險。** `api/graphql.py` byte-identical、`app = _build_app()` 仍喺 top-level、`known-third-party = ["graphql"]` 未動、`pyproject.toml` 只掂 `[tool.coverage.*]`。CLAUDE.md §6 記低咗 `fd252a3` / `e506531` 兩個 deploy cycle 蝕喺改 entry 名，呢個 lane 一步都冇行埋去。

6. **方案 A vs B 嘅取捨有寫低。** CUI-0042 揀 A 唔揀 B、`--omit=dev` 唔揀 `--audit-level=high`、刻意唔做票建議 (3) —— 三個決定都附咗可以被推翻嘅理由，而唔係淨係報結果。第三個我有保留（S-094），但佢基於一句我核實過嘅檔案原文，唔係口噏噏。

---

## 修正優先順序

| # | ID | 級別 | 內容 | 建議時機 |
|---|---|---|---|---|
| 1 | W-034 | 🟡 | `pages.yml` comment 講闊咗 `--omit=dev` 覆蓋面 | **本輪修**（唯一擋住 90 分嘅） |
| 2 | S-095 | 🟢 | `CLAUDE.md` 「12 high」→ 14（或索性唔寫數） | 本輪順手 |
| 3 | S-100 | 🟢 | `pyproject.toml` comment 絕對數字已 stale | 本輪順手 |
| 4 | S-092 | 🟢 | `release` input description 補「ignored when notes_only」 | 本輪順手 |
| 5 | S-093 | 🟢 | `notes_only` 守衛加 `gh release view` | 本輪或另飛 |
| 6 | S-098 | 🟢 | Audit step 移到 frontend job 最尾 | 本輪或另飛 |
| 7 | S-094 | 🟢 | `docs/deployment.md` 補 SCA policy（唔抄 step） | 另飛 |
| 8 | S-096 | 🟢 | Python 側補 `pip-audit` gate | **另開 ticket** |

> 1–4 全部係純文字／描述改動，零行為風險，合計應該 20 分鐘內搞掂。改完重跑 `/review` 即可上 90。

---

## 修訂後代碼（W-034，唯一需要本輪改嘅）

`.github/workflows/pages.yml` —— 只改 comment 頭 4 行，`run:` 一個字都唔郁：

```yaml
      # --omit=dev audits the declared `dependencies` closure: the eight runtime
      # dependencies in package.json, 16 packages resolved. That is not quite
      # "everything the browser gets" -- vite ships its own modulepreload
      # polyfill into the bundle and tailwindcss generates the whole stylesheet,
      # and both are devDependencies. It IS the scope this gate can hold green,
      # so a red here is a real exposure rather than a backlog. (No dependency
      # audit ran in CI before this step -- the Python side's pip-audit has only
      # ever been run by hand, in CUI-0032.)
      #
      # The full tree is NOT clean and this step does not claim otherwise: as of
      # 2026-09-17 `npm audit` (dev included) reports 14 advisories -- 12 high,
      # 2 moderate, 0 critical -- every one of them reached only through
      # devDependencies (@graphql-codegen/* -> lodash, vitest -> @vitest/mocker).
      # Those emit nothing into the bundle (codegen emits types, which are
      # erased; vitest emits nothing), are build- and CI-time only, and are
      # tracked separately in CUI-0036; gating on them would park CI red on a
      # backlog nobody could clear in this change. Widen this step to the full
      # tree once that bump lands -- dropping the flag is the point, not a
      # regression.
      - name: Audit runtime dependencies
        run: npm audit --omit=dev
```

---

## 臨時改動還原（已驗證）

本次 review 冇 commit / merge / push / dispatch 任何嘢，亦冇改任何 project 檔案。

- 期間曾經 `git checkout` 過 `8758279` 同 `6576e56` 量 coverage，**已 checkout 返 `06ad4bd`**
- `git log --oneline -1` → `06ad4bd Merge the API refusal lane (CUI-0037, 0038, 0039, 0040, 0005)`
- `git status --porcelain` → **空**
- `git diff HEAD --stat` → **空**
- 最終 md5（供對數）：
  ```
  8a35fbe62e9606a04b224233095628bb  api/graphql.py
  377385f89052a36a7da23ab10accf7b4  pyproject.toml
  8eeb71ccc4c7ee402810f64cc8229c7d  CLAUDE.md
  df16384379779910931aefd2deb06ed6  tests/test_api.py
  efa10a647fc23252756651655b789dea  .github/workflows/pages.yml
  7e0b62508dd195c080c59b284e09d1e7  .github/workflows/tag-release.yml
  ```
- 「造一個會紅嘅 npm audit」係喺 scratchpad 複製出嚟嘅 `package.json` / `package-lock.json` 上做，**repo 內兩個檔案零改動**（md5 同上表 `pages.yml` 一齊核過，`frontend/` 亦喺 `git status` 空白範圍內）
- 臨時 script / 輸出全部喺 `/tmp/claude-0/-home-user/ce531b35-9dfc-5a8f-993f-c0921f38df69/scratchpad/`
- `.venv/`、`.coverage`、`frontend/node_modules/`、`frontend/dist/`、`frontend/src/gql/` 都喺 `.gitignore` 範圍內（`git status` 空白為證）；`.coverage` 已刪

```handoff-receipt
protocol: 1
status: warn
score: 87/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "96%"
next_action: invoke_developer
next_agent: backend-developer
branch: "claude/ai-dev-team-start-05jie2"
context: "CUI-0036/0042/0035 三票實現全部正確，lane 自報八條推翻逐條實測成立，0 Critical，hard gates 全綠（492 pytest / 141 vitest / 96% / audit exit 0）。唯一 Warning W-034：pages.yml 嘅 audit step comment 聲稱 --omit=dev 審「what actually ships to a browser」，但實測 vite modulepreload polyfill 同 21.64kB Tailwind CSS 都經 devDependencies 入咗 bundle；gate 行為啱，錯嘅係理由書。連同 S-092/S-095/S-100 三條純文字修正一齊改完重跑 /review 即可上 pass。"
blockers:
  - "W-034: .github/workflows/pages.yml:83-84 comment 把 --omit=dev 覆蓋面講闊咗（實測 vite 同 tailwindcss 嘅產出真係入 bundle），需改寫成 declared dependencies closure + 明寫缺口；報告有完整修訂後 comment"
```