# Batch QA — 2026-09-16 — low-cost wave 2（Round 2）

| 項目 | 內容 |
|---|---|
| 測試員 | Quality Assurance（獨立，未參與本批代碼撰寫） |
| 範圍 | 3 張 ticket（CUI-0021 / CUI-0004 / CUI-0015）+ 2 個 review item（S-042 / S-043） |
| 驗證基線 | `85dd0e2`（QA 開始時嘅 HEAD） |
| 完成時 HEAD | `4d0914f`（過程中另一條 lane merge 咗一個 **docs-only** commit，見 §7.3） |
| 涵蓋 commit | `d806f1c`(CUI-0004)、`c8bd76e`(CUI-0021)、`d88d760`(CUI-0015)、`38ef6e6`(S-042)、`2267a95`(S-043) |
| Code Review | `.proj-docs/reviews/2026-09-16_review_low-cost-wave2_batch.md`（91/100、0 Critical、pass） |
| 本報告角色 | **行為驗證同回歸**。Reviewer 已做嘅代碼審查同 500,499 組合窮舉**不重複** |
| 測試環境 | dev；**真實 export**（365 activities / 365 tracks / 134,041 track points），Python 3.11 + Node 22 |
| 總結 | ✅ **pass** —— 0 Critical、0 Major。唯一有行為改動嘅 CUI-0021 端到端驗證**完全正確**，而且我造咗對照組證明呢個測試真係有牙 |

---

## 1. 整體 verdict

### 1.1 Hard Gates

全部喺**完成時 HEAD（`4d0914f`）**重跑過一次，以下係最終結果。

| Gate | 指令 | 結果 |
|---|---|---|
| Tests（Python） | `.venv/bin/python -m pytest tests -q` | ✅ **364 passed** |
| Lint（Python） | `.venv/bin/ruff check src tests` | ✅ All checks passed |
| Format（Python） | `.venv/bin/ruff format --check src tests` | ✅ 58 files already formatted |
| Schema sync | `run365-schema --check frontend/schema.graphql` | ✅ up to date |
| Lint（前端） | `npm run lint` | ✅ 零 error |
| Typecheck（前端） | `npm run typecheck` | ✅ codegen 先行，零 error |
| Tests（前端） | `npx vitest run` | ✅ **126 passed / 22 files** |
| Coverage（核心邏輯） | `pytest --cov=src` | ✅ **94.71%**（同 Round 1 **一模一樣**，見 §7.1） |
| Build（api mode） | `npm run build` | ✅ 293 modules，2.71s |
| Build（static mode） | `npm run build:static` | ✅ 293 modules，2.65s |
| Data integrity | 跨 mode 逐點比對 | ✅ **418,471 個真實數值零分歧**（§3.2） |
| Security | 零新增依賴；introspection 已關；`points` 型別守衛 | ✅（§3.7） |
| No Critical | 本報告 | ✅ **0 個** |
| Performance | 無新增 N+1；S-043 反而少 build 一次 schema | ✅ |

### 1.2 逐項結論

| Item | Commit | 性質 | Verdict | 建議狀態 |
|---|---|---|---|---|
| **CUI-0021** | `c8bd76e` | **唯一行為改動** | ✅ pass | **completed** |
| CUI-0004 | `d806f1c` | docstring + SDL + test | ✅ pass | **completed** |
| CUI-0015 | `d88d760` | docs-only | ✅ pass | **completed** |
| S-042 | `38ef6e6` | test 結構 | ✅ pass | 已 merge，無獨立 ticket |
| S-043 | `2267a95` | test 結構 | ✅ pass | 已 merge，無獨立 ticket |

### 1.3 Edge case gap 統計

| Item | 主動補測評估 | Gap |
|---|---|---|
| CUI-0021 | **高風險**（算術 / 跨 mode）→ 強制深驗，共補 **11 組**新 edge probe | ✅ 無 gap |
| CUI-0004 | **中風險**（語義邊界）→ 補 **3 組**（含 static 側，developer 冇驗過） | ✅ 無 gap |
| CUI-0015 | **零風險**（純 prose）→ 改為逐句事實核對 + drift 面積量度 | ✅ 無 gap（不適用） |
| S-042 / S-043 | **零風險**（測試結構，reviewer 已驗 tripwire）→ 只確認全套仍綠 | ✅ 無 gap |

---

## 2. 測試方法（點解可信）

reviewer 用嘅係**合成**組合窮舉。我刻意行另一條路，用**真實 export 產物**做端到端：

```
data/raw（365 TCX + 365 GPX，真實 Garmin 檔案）
        │  run365-export
        ├─→ run365.db ─────→ create_app() ─→ Flask test client ─→ 真 GraphQL ─→ api mode 取樣
        └─→ static/*.json ─→ createStaticSource()（真 source.ts）─→ downsample.ts ─→ static mode 取樣
                                          ↓
                            逐 activity、逐 index、逐 field 比對
```

兩邊都係**真實模組**，冇手抄副本：
- api 側行 `src/api/app.py` 嘅 `create_app()`，經真 GraphQL document（唔係直接叫 `service.track()`）；
- static 側經 `vite-node` import 真嘅 `frontend/src/data/static/source.ts`，由佢自己叫 `downsample.ts`。

---

## 3. Section A — CUI-0021（`c8bd76e`）唯一有行為改動

### 3.1 端到端 build —— ✅ 兩個 mode 都成功

| Build | 指令 | 結果 |
|---|---|---|
| api mode | `npm run build` | ✅ `dist/assets/index-Bkxw5QR7.js` 658.77 kB（gzip 205.04 kB） |
| static mode | `npm run build:static` | ✅ `dist/assets/index-D9gpmetb.js` 658.73 kB（gzip 205.03 kB）+ `dist/data/` |

Static build 亦確認咗會將 export 出嘅 JSON 收入 `dist/data/`（8.0 MB）。
兩個 build 都**零 error**，只有 pre-existing 嘅 chunk-size 警告（同本批無關）。

### 3.2 真實數據跨 mode 逐點比對 —— ✅ 418,471 個數值零分歧

真實 export：`run365-export`（365 activities、365 weigh-ins、363 weather days、461 warnings）。
每個 `points` 設定都行足 **365 條 track**，逐 index 逐 field（`sec / lat / lon / elevationM /
distanceM / speedMps / cadence / tempC`，8 個 field）比對。

| `points` | 比對嘅點數 | 長度分歧 | 數值分歧 |
|---|---|---|---|
| 2 | 730 | 0 | 0 |
| 3 | 1,095 | 0 | 0 |
| 5 | 1,825 | 0 | 0 |
| 7 | 2,555 | 0 | 0 |
| 150 | 54,750 | 0 | 0 |
| 249 | 90,885 | 0 | 0 |
| 400 | 132,590 | 0 | 0 |
| **600（生產設定）** | **134,041** | **0** | **0** |
| **合計** | **418,471** | **0** | **0** |

> `points = 600` 嗰行就係 `ActivityView` 今日實際行嘅路徑：**134,041 個真實數值，api mode 同
> static mode 完全一致**。

### 3.3 對照組 —— 呢個測試真係有牙（**本報告最重要嘅一節**）

上面全綠有個陷阱：**如果 rounding 根本入唔到，就算 fix 係錯嘅都會全綠**。
所以我造咗一個對照組，證明同一套比對會捉到舊實作。

做法（**冇改動 repo 任何檔案**）：由真實 `downsample.ts` 用 `sed` 機械化改一個 call expression
寫入 scratchpad，再 import 真嘅 `mappers.ts` 行同一條路。對照組同真檔案嘅 `diff` 只有一行：

```
<   return Array.from({ length: limit }, (_, i) => items[roundHalfToEven(i * step)]);
>   return Array.from({ length: limit }, (_, i) => items[Math.round(i * step)]);
```

| `points` | 修好之後（`roundHalfToEven`） | 對照組（舊 `Math.round`） |
|---|---|---|
| 2 | 0 條 track 分歧 | 0 條 |
| **3** | **0 條** | **96 條分歧**（560 個 field 值） |
| **5** | **0 條** | **181 條分歧**（1,021 個值） |
| **7** | **0 條** | **126 條分歧**（891 個值） |
| 150 | 0 條 | 0 條（見 §8.2 —— 呢個係**假綠**） |
| **249** | **0 條** | **219 條分歧**（2,461 個值） |
| 400 / 600 | 0 條 | 0 條 |

具體個案（真實 activity，同 CUI-0021 票上舉嘅例完全一致）：

```
activity 6055376813  stored_len=250  points=3  step=(250-1)/2=124.5   <- 啱啱好 .5
   api mode（Python round）   : [0, 866, 1847]
   static mode（修好後）       : [0, 866, 1847]   ✅ 一致
   對照組（舊 Math.round）     : [0, 872, 1847]   ❌ 取咗 index 125 而唔係 124
```

**結論：`c8bd76e` 真係修好咗一個喺真實數據上可觀測嘅跨 mode 分歧，唔係空轉。**

### 3.4 「今日不可達」獨立核實 —— ✅ 成立，而且比 reviewer 講嘅更穩陣

Reviewer 嘅推論鏈我獨立重行，並且**直接量真實 export**（唔係讀 config 推論）：

| 問題 | 實測 |
|---|---|
| 真實 export 最長嘅 track 係幾多點？ | **600**（activity `7264441638`） |
| 有冇任何 track > 600？ | **冇，0 條** |
| 啱啱 600 嘅有幾多條？ | **2 條** |
| 最短？ | 250 |
| 相異長度 | **123 個**，範圍 250..600 |
| SQLite `track_points` 對得上？ | ✅ 完全一致（max 600 / min 250 / >600 = 0） |

**額外發現（reviewer 冇提）**：raw TCX 其實**有 2 條超過 600 點**（1,250 同 653）。
即係話 **Python `downsample` 喺 export 時真係行咗 rounding 分支**，將佢哋斬落 600 —— 
寫出嚟嘅 static JSON 本身就係 Python rounding 嘅產物。TS 側只係讀呢啲已經 ≤600 嘅 track，
所以行 `n <= limit` 早返。呢點令「以 Python 為準」呢個方向嘅理由**比票上寫嘅更強**。

### 3.5 產出物核實 —— ✅ 修復真係入咗 shipped bundle

唔淨止睇源碼，直接喺 minify 完嘅 static bundle 入面搵：

```js
function yD(e){const t=Math.floor(e),n=e-t;return n>.5?t+1:n<.5||t%2===0?t:t+1}
function vD(e,t){const n=e.length;if(n<=t)return[...e];if(t<2)return[e[n-1]];
                 const a=(n-1)/(t-1);return Array.from({length:t},(l,o)=>e[yD(o*a)])}
```

`vD`（= `downsample`）叫 `yD`（= `roundHalfToEven`），**唔係** `Math.round`。
minifier 將三條分支併成 `n<.5||t%2===0` 但語義等價。`n<=t` 早返亦在。
Bundle 入面其餘 57 個 `Math.round` 全部嚟自第三方（chart.js 等），同取樣無關。

### 3.6 前端渲染 —— ✅ 路線冇變

`git diff 05f36df..HEAD` 顯示本批**冇掂過任何 view / 渲染代碼**（只有 `frontend/src/lib/downsample.ts`
同新 test file）。加上 §3.2 已證 `points=600` 之下兩個 mode 嘅 point array 完全一致
（134,041 個值），而 `ActivityView.tsx:18` 嘅 `TRACK_POINTS = 600` 亦冇改動 ——
**畫出嚟嘅路線逐點相同**。

Call site 核實：`ActivityView.tsx:34` `useTrack(current, TRACK_POINTS)` 係 `track()` 唯一實際 caller；
`frontend/src/data/api/source.ts:20` 嘅 `DEFAULT_TRACK_POINTS` 亦係 600。兩個 mode 同一個數。

### 3.7 安全 / 型別守衛回歸 —— ✅

因為 `src/api/schema.py` 被本批改過（雖然只係 description），我重驗咗佢嘅守衛仍然生效：

| 檢查 | 結果 |
|---|---|
| `points: 2.5`（variable） | ✅ 拒絕 —— `Int cannot represent non-integer value: 2.5` |
| `points: 2.5`（document literal） | ✅ 拒絕 |
| `points: "3"` / `null` / `true` | ✅ 全部拒絕 |
| `points: 0` / `-1` / `1001` | ✅ `points must be between 1 and 1000` |
| Track points budget（30 × 400 = 12,000） | ✅ `track points budget exhausted: ... at most 10000` |
| GraphQL introspection | ✅ **已停用**（`__type` 被拒）—— 良好姿態 |
| 新增依賴 | ✅ 零（`package.json` / `pyproject.toml` 都冇郁） |

> 順帶記錄：parser 有一個 **1000-token document 上限**，實測**比 64-field cap 更早觸發**，
> 所以 64 個 `track` field 嘅 cap 喺單一 document 入面實際上到唔到。屬 pre-existing 設計，
> 同本批無關，唔另開票，只作備案。

---

## 4. Section B — CUI-0004（`d806f1c`）

### 4.1 真 GraphQL 語義驗證 —— ✅ 365/365 全中

Developer 同 reviewer 都只用單一 fixture 驗過。我改為**全部 365 條真實 track 逐條驗**：

| 斷言 | 結果 |
|---|---|
| `track(points: 1)` 回**一行** | ✅ 365/365 |
| 嗰行係**最後一行**（唔係第一行） | ✅ **0 failure** |
| `track(points: 2)` 回**首 + 尾** | ✅ **0 failure** |
| `points: 1` 結果 == `downsample(full, 1)` | ✅ 365/365（assert 通過） |
| `points: 2` 結果 == `downsample(full, 2)` | ✅ 365/365 |

**claim 非空洞性核實**：「最後一行而唔係第一行」呢句要有意義，前提係首尾唔同。
實測 **first == last 嘅 track = 0 條** —— 即係話呢個斷言喺**每一條真實 track 上都真係分得出首尾**，
唔係湊啱。呢點 developer / reviewer 都冇驗過。

### 4.2 Static 側同一語義 —— ✅（本報告新增，兩邊都冇驗過）

CUI-0004 只講 api 側，但 `points: 1` 喺 static mode 一樣行得到。實測全部 365 條：

```
static source.track(id, 1)  的 sec  ==  api mode 最後一行的 sec       mismatches = 0 / 365
```

**兩個 mode 喺 `points: 1` 呢個邊界亦完全一致。**

### 4.3 SDL 契約 —— ✅

| 檢查 | 結果 |
|---|---|
| `schema.as_str()` == `frontend/schema.graphql` | ✅ 完全相同 |
| `TRACK_DESCRIPTION` 逐字出現喺 SDL | ✅ |
| ...亦喺 committed `frontend/schema.graphql` | ✅ |
| 句子關鍵詞（`points: 1` / `nothing to space evenly` / `last row` / `not its first`） | ✅ 全部在 |
| `run365-schema --check` | ✅ up to date |

### 4.4 「零行為改動」—— ✅ 確認

`git diff` 核實：`src/api/service.py` 只改 docstring；`src/api/schema.py` 只改 `TRACK_DESCRIPTION`
呢個 module-level 常數（即 SDL 文字本身，正正係本票要改嘅嘢）。取樣邏輯**一行都冇郁**。

---

## 5. Section C — CUI-0015（`d88d760`）docs-only

### 5.1 逐句對返 `pages.yml` —— ✅ 全中

| 文件剩低嘅聲稱 | `pages.yml` 實況 | 對 |
|---|---|---|
| Python 檢查 = lint / format / tests / schema sync | `:54` `:57` `:60` `:63` | ✅ |
| 前端檢查 = lint / typecheck / unit tests / 兩個 mode build | `:84` `:87` `:90` `:93` `:96` | ✅ |
| 「Lint, tests and the frontend checks run on every branch the workflow gates」 | `lint-test`(`:39`) / `frontend`(`:65`) **兩個 job 都冇 `if:`** | ✅ |
| 「build 同 deploy 限一個 branch，由 `github.ref` test 控制」 | `:105` 同 `:152` 都係 `if: github.ref == 'refs/heads/develop'` | ✅ |
| 「`workflow_dispatch` 跟同一規則」 | `:8` 有；ref guard 同 event 無關 | ✅ |
| 「`dist/index.html` 複製做 `404.html`」 | `:135` | ✅ |
| 「`master` is gated by the same CI checks」 | `:5`/`:7` `[develop, master]`，兩個 job 無 `if:` | ✅ |

### 5.2 Drift 面積量度 —— ✅ 真係收窄咗

| 量度 | 結果 |
|---|---|
| `docs/architecture.md` 提及 `develop` / `master` | **0 / 0** —— 歸零，方案 3 嘅收益兌現 |
| Docs 仲有冇 job 名（`lint-test` 等）/ job 數目（「four jobs」）/ `refs/heads` | **零命中** |
| 剩低嘅 `run365-export` / `npm ci` / `ruff check` 命中 | 全部喺**本機開發**段落（教人自己點行），**唔係** CI 描述複製 |
| `docs/deployment.md:24,27` 嘅 `npm ci` | Vercel 設定，唔係 `pages.yml` |

### 5.3 切換 checklist 完整性 —— ✅ 逐項核實

`git diff` 確認 checklist **一個字都冇改**。我逐項對返 repo：

| # | Checklist 項 | 核實 |
|---|---|---|
| 1 | `pages.yml` 嘅 `push` / `pull_request` branch lists | ✅ `:5` `:7` |
| 2 | `build` / `deploy` 嘅 `if:` + `concurrency` `group` / `cancel-in-progress`，「**All four** test `refs/heads/develop`」 | ✅ **啱啱好 4 個 expression**（`:31` `:32` `:105` `:152`） |
| 3 | `tag-release.yml` 嘅 `ref` input default | ✅ `:16` `default: develop` |
| 4 | `README.md` 四個位置 | ✅ README 6 處 `develop` 全部落入呢項 |
| 5 | GitHub `github-pages` environment（repo 外） | ✅ 保留，生成唔到 |
| 6 | Vercel Production Branch（repo 外） | ✅ 保留 |
| 尾 | 「Then update this file: intro / trigger 表 / Vercel section / Pages section / this checklist」 | ✅ 涵蓋 `deployment.md` 9 處 `develop` |

**六件事四個地方全部仍然存在且仍然需要人手更新，checklist 冇失效。**

> 🔍 觀察（**唔開票**）：`pages.yml` 另有 **3 行註釋**（`:16` `:101` `:150`）都寫死
> `refs/heads/develop`，checklist 第 2 項只點名 4 個 expression 冇提註釋。
> 但嗰 3 行註釋就喺果 4 個 expression 隔籬，改嗰 4 個嘅人唔可能睇唔到；
> 而且 checklist 本身係 CUI-0015 **刻意原樣保留**嘅範圍，屬 W-011 產出。
> 影響太低，唔值得一張票，記錄備案。

### 5.4 S-047 確認

`grep -rn "VITE_BASE_PATH" README.md docs/` → **零命中**，reviewer 嘅 S-047 屬實。
已由另一條 lane 處理中，本報告不重複。

---

## 6. Section D — S-042（`38ef6e6`）/ S-043（`2267a95`）

Reviewer 已經做咗 tripwire 驗證（S-042 人為調轉 7 個 signature 得 13 failed；
S-043 人為注入 interface / union 證明兩次讀都仍然 load-bearing）。按指示**不重複**。

我只確認冇令任何測試變成空綠：

| 檢查 | 結果 |
|---|---|
| `tests/test_api.py` collect 數 | ✅ **105 tests collected**（同 reviewer 一致） |
| 全套 Python | ✅ 364 passed |
| 全套前端 | ✅ 126 passed / 22 files |
| `downsample.test.ts` 實際 test 數 | ✅ **18**（2 × `it.each` × 8 + 2 獨立，同 developer 聲稱一致） |
| 有冇測試被刪 / 改名 | ✅ 零（reviewer F2 已核，我用 per-file count 覆核） |

---

## 7. 回歸

### 7.1 Coverage —— ✅ 冇跌，**逐位相同**

```
TOTAL   1606 stmts   85 miss   95%      ->   1521/1606 = 94.7073% = 94.71%
```

同 Round 1 嘅 **94.71%** **完全一致**，零下跌。

`src/api/service.py`：103 stmts / 1 miss / **99%**，唯一未覆蓋係 **L268**
（`tracks()` 嘅 `if not wanted: return found` 空輸入護欄）。呢個同 **CUI-0023** 記錄嘅
L257 係**同一行**（CUI-0004 加咗 docstring 令行號由 257 推移到 268），性質不變：
由 GraphQL 入唔到嘅 defensive guard。**冇新增未覆蓋行。**

### 7.2 Round 1 嘅嘢有冇被整壞 —— ✅ 冇

| Round 1 產出 | 檢查 | 結果 |
|---|---|---|
| **`src/common/time.py`（CUI-0006）** | `git diff --name-only 05f36df..HEAD` | ✅ **零改動**；last commit 仍係 `8068a81 fix: CUI-0006` |
| `src/api/schema.py`（本批改過） | 逐行 diff | ✅ 只加 `TRACK_DESCRIPTION` 兩行字串，守衛邏輯零改動（§3.7 已重驗守衛仍生效） |
| `src/api/service.py`（本批改過） | 逐行 diff | ✅ 純 docstring，`_even_positions` 實作零改動 |
| `src/dashboard/builder.py` | diff | ✅ 零改動 |
| 針對性重跑 | `test_common_time.py` + `test_api.py` + `test_dashboard_builder.py` + `test_activities_parsers.py` | ✅ **233 passed** |

### 7.3 過程中 HEAD 前進 —— 已處理

QA 途中另一條 lane merge 咗 `4d0914f`。核實其影響範圍：

```
git diff --stat 85dd0e2..4d0914f
 .proj-docs/qa/2026-09-16_qa_low-cost-wave1_batch.md | 224 +++++++-
 .proj-docs/tickets.md                               |  82 ++++
 2 files changed
```

**純 `.proj-docs/` 文件，零 source / 零 test 改動**，所以我喺 `85dd0e2` 做嘅行為驗證仍然有效。
即使如此，全部 hard gates 已喺 `4d0914f` **重跑一次**（結果見 §1.1），全綠。

---

## 8. 邊界探索

### 8.1 系統性邊界掃描 —— 121 個 case

用**真實** TS `downsample`（vite-node import）對**真實** Python `builder.downsample`，
`n ∈ {0,1,2,3,250,598,599,600,601,602,700,1000,1250} × points ∈ {0,1,2,3,150,599,600,601,1000}`
加非整數 limit：

| 邊界 | Python | TS | 判定 |
|---|---|---|---|
| **`n=600, points=600`**（任務指定） | 600 行原樣 | 600 行原樣 | ✅ 一致，證實 `n <= limit` 早返 |
| **`n=601, points=600`**（任務指定，**真實數據冇**） | 600 行 | 600 行 | ✅ **完全一致** —— 呢個 case **真係入到 rounding**，而兩邊夾到 |
| `n=599, points=600` | 599 行原樣 | 同 | ✅ |
| `n=602 / 700 / 1000 / 1250, points=600` | — | — | ✅ 全部一致 |
| **`n=0`（空 track）** | `[]` | `[]` | ✅ 兩邊都唔會 crash |
| **`n=1`（單點 track）** | `[0]` | `[0]` | ✅ |
| `n=2, points=0/1` | `[1]` | `[1]` | ✅ |
| `points=1000`（API 上限） | — | — | ✅ 一致 |
| `limit=inf` | 回全部 | 回全部 | ✅ |
| **`limit=2.5`** | `TypeError` | `[0, 6]` | ⚠️ 分歧 = **S-045**（已知，已開 finding） |
| **`limit=NaN`** | `TypeError` | `[]`（**成條 track 消失**） | ⚠️ 分歧 = **S-045** |

**121 個 case 入面 119 個完全一致，僅有嘅 2 個分歧就係 reviewer 已經開咗嘅 S-045。**

而且我確認咗 **S-045 兩個 mode 都不可達**：api 側 GraphQL `Int` scalar 拒絕 `2.5`/`NaN`（§3.7 實測），
static 側唯一 caller 傳 `TRACK_POINTS = 600` 呢個常數。S-045 定為 🟢 Suggestion **恰當**。

另外 `_even_positions` vs `builder.downsample` 喺 9 個長度 × 全部合法 `points` 之下
**mismatches = 0**，兩個 Python sampler 仍然等價。

### 8.2 主動 edge case 補測（本報告新增）

| # | Edge case | 方法 | 結果 |
|---|---|---|---|
| E1 | `points=1` 喺 **static** mode（CUI-0004 只驗咗 api） | 真 `createStaticSource`，365 條 | ✅ 全部回最後一行，同 api 一致，0 mismatch |
| E2 | **Idempotency / memoisation 安全**：同一個 source instance 交錯叫 `track(id,3)` / `track(id,600)` / 再 `track(id,3)` | 真 source | ✅ 前後兩次完全相同；`load()` memoise 唔會污染 |
| E3 | **Aliasing**：改動 `track()` 回傳嘅 array 之後再讀 | 真 source | ✅ 唔會污染（`mapTrack` 每次重新 map） |
| E4 | `points=undefined` vs `points=0` | 真 source | static 兩者都回**全部**；api 拒絕 `0` → 確認 **CUI-0025 (a)** 屬實（已有票，非本批遺漏） |

### 8.3 🔍 獨立發現：「幾時會 tie」嘅判準係 **奇偶**，唔係質數 → 已開 **CUI-0026**

`.proj-docs/tickets.md`（`4d0914f`，已 merge）寫住「`limit=150` 冇分歧係因為 **149 係質數**嘅算術巧合」。
**呢個解釋唔啱。** 真正判準係 step 分母 `d = limit - 1` 嘅**奇偶**：

```
i*(n-1)/d = k + 1/2   ⟺   2*i*(n-1) = d*(2k+1)
d 奇數 → 左邊偶、右邊奇 → 永遠無解，一個 tie 都冇
d 偶數 → 有解，tie 大量出現
```

實測（`n = 601..20000` 每個長度全掃）：

| `limit` | `d` | `d` 奇偶 | ties | 反例意義 |
|---|---|---|---|---|
| 600 | 599 | 奇 | **0** | |
| 150 | 149 | 奇 | **0** | |
| **64** | **63 = 3²×7（非質數）** | **奇** | **0** | ⬅ **質數論被證偽** |
| 97 / 101 / 193 / 241 / 249 / 65 | 偶 | 偶 | 48k–110k | |

同真實 export 嘅分歧率完全跟奇偶走（偶數 limit 2/150/400/600 → 0%；
奇數 limit 3/5/7/97/193/241 → 27.6% / 51.2% / 39.0% / 83.7% / 84.6% / 82.1%），
亦同 §3.3 對照組嘅實測逐個吻合。

**兩個實用結論**：
1. `TRACK_POINTS = 600` 今日有**兩重**保護，唔止一重：(a) `n <= limit` 早返；
   (b) **就算將來 track 長過 600，`599` 係奇數，`limit=600` 一樣永遠唔會 tie**。現有文件只講咗 (a)。
2. 將來調 `TRACK_POINTS`：**偶數免疫、奇數危險**。用 `limit=150` 做 smoke test 會出**假綠**
   （另一條 lane 呢個結論**係啱嘅**，只係歸因錯咗）。

→ 已開 **CUI-0026**（🟢 Low、tech-debt、零行為改動）。

---

## 9. W-024 獨立發現 —— 我重現到嗰組數字，核心 finding 不成立

任務指明如對 W-024 嗰組數字有獨立發現要寫入報告。**我喺唔知另一條 lane 結論嘅情況下，
獨立重現咗全部三個數字。**

Reviewer 判 W-024 為「唔係範圍定義差異，**係量錯咗**」，理由係掃 `2..1000` 連續 range
最高只得 21.91%。我用**重建嘅真實 export** 試咗一個 reviewer 冇試過嘅定義 ——
**export 實際持有嘅相異 track length**：

```
export 相異 track length = 123 個，範圍 250..600
Σ(L−1) over those lengths = 45,240 − 123 = 45,117          <- 由算術直接跌出嚟
其中分歧                   = 10,189
比率                       = 22.5835%  ->  22.6%
```

**45,117 / 10,189 / 22.6% 三個數字逐個精確命中。**「the pairs the export can produce」
指嘅就係 export 真正造得出嘅長度，唔係連續 range。

**可信度保證**：我嘅 Python 對照器唔係憑空寫，而係**用真實 TS 實作驗證過**——
兩者喺 `points = 2/3/5/7/150/249` 之下預測嘅分歧 track 數
（`0 / 96 / 181 / 126 / 0 / 219`）**六個值全部逐個吻合**（§3.3 對照組實測）。

**結論：W-024 核心 finding 係 false positive，原始數字係啱嘅。**
Reviewer 第二半（「the export can produce」措辭有歧義）**成立**，屬 🟢 措辭問題。

> ℹ️ 我完成呢段之後先發現另一條 lane 喺 `4d0914f` 已經獨立得出**完全相同**嘅裁決同數字。
> 兩條 lane 用唔同路徑（佢哋算術枚舉、我行真實 TS 對照組）**互相印證**，結論可信度更高。
> 因此**唔重複開票**；但佢哋順帶寫低嘅「149 係質數」歸因有錯，已另開 **CUI-0026**（§8.3）。
> ⚠️ 同時提醒：**唔好**採納 reviewer 原本嘅方案 A（換成 `2 <= length <= 1000` 嘅 105,181 / 499,500）——
> 咁樣會將**啱嘅**數字換成一組**同真實 export 無關**嘅數字。

---

## 10. 跨 fix 互動評估

| 檢查 | 結果 |
|---|---|
| 同一 file 被多個 commit 改（`tests/test_api.py` 被 CUI-0004 / S-042 / S-043 改） | ✅ 三個區域互不重疊；105 條測試全綠，無 fixture 名衝突、無 import 衝突 |
| `src/api/service.py` 被 CUI-0004 + CUI-0021 各加一段 docstring | ✅ 兩段並存唔矛盾：一段講 `points==1` 語義，一段講 TS 側 rounding。ruff `D` 全過 |
| 新 helper / 常數 symbol collision | ✅ 唯一新 symbol 係 module-private `roundHalfToEven`，前端全域搵唔到第二個定義 |
| 雙向 cross-reference 一致性 | ✅ `service.py` → `downsample.ts`、`downsample.ts` → Python sampler，兩邊都標 CUI-0021，指向正確 |
| CUI-0004 改 SDL + CUI-0021 改 TS，兩者會唔會撞 | ✅ 唔會：一個係 SDL 文字、一個係 TS 取樣。`run365-schema --check` 綠 + 跨 mode 418,471 值零分歧同時成立 |
| Batch 整體觸發已知陷阱（CLAUDE.md §6） | ✅ 無：本批零 `zoneinfo` / 零 `inf` 路徑 / 零 NOT NULL 映射 / 零 Vercel entry 改動 |
| Coverage threshold 仍達標 | ✅ 94.71%，核心邏輯遠高於 80% |
| Commit ↔ ticket 對應 | ✅ 5 commit 對 5 個 item，無夾帶 |

---

## 11. 問題清單同 ticket

| 優先 | ID | 級別 | 內容 | Block merge |
|---|---|---|---|---|
| — | — | 🔴 Critical | **無** | — |
| — | — | 🟠 Major | **無** | — |
| 1 | **CUI-0026** | 🟢 Low | 「tie 幾時發生」歸因錯（質數 → 應為 `limit-1` 奇偶）；已 merge 喺 `tickets.md` | ❌ 否 |

其餘 reviewer 開嘅 W-024 / S-044 / S-045 / S-047 由另一條 lane 處理中，本報告不重複開票。

---

## 12. Ticket 狀態建議

| Ticket | 建議 | 理由 |
|---|---|---|
| **CUI-0004** | ➡️ **completed** | 三項改動全部驗證通過；365/365 真實 track 語義正確；SDL 同步；零行為改動確認 |
| **CUI-0021** | ➡️ **completed** | 端到端 418,471 個真實值零分歧；對照組證明修復有實效；產出物核實；邊界全過 |
| **CUI-0015** | ➡️ **completed** | 剩低每一句對返 `pages.yml` 全中；drift 面積確認收窄；切換 checklist 六項完整 |
| CUI-0026 | 新開 `pending` | 本報告 §8.3 |
| CUI-0022 / 0023 / 0024 / 0025 | **不動** | 非本次驗證範圍 |

> ⚠️ QA **冇執行任何 git 操作**。Ticket 檔案移動（`pending`/`in-progress` → `completed`）
> 同 merge 由 main agent 按 receipt 執行。

---

## 13. 環境清潔聲明

| 項目 | 狀態 |
|---|---|
| `git diff --stat` | ✅ **空** |
| `git status --short` | ✅ 只有 `?? .tickets/pending/0001-0200/CUI-0026.md`（本報告新增嘅票）同本報告 |
| 為驗證而做嘅臨時改動 | ✅ **零** —— 對照組係喺 scratchpad 由 `sed` 導出，**repo 檔案一個字都冇改過** |
| `data/processed/` | ✅ 只有 `.gitkeep`（export 寫咗去 scratchpad） |
| `frontend/public/data/`、`frontend/dist/` | ✅ 已刪除（gitignored 產出物，唔留垃圾） |
| `.coverage` | ✅ 不存在 |
| 所有 probe / 對照組 / 中間 JSON | ✅ 全部喺 session scratchpad，repo 外 |

---

## 14. 測試建議（下一步）

1. **考慮將跨 mode parity 變成常設測試**。今日 `downsample.test.ts` 已釘死 8 個具名個案，
   好；但「兩個 mode 對真實 export 逐點一致」呢個更強嘅不變式仲係靠人手 probe。
   一個細 CI step（export → 兩邊取樣 → digest 比對）成本唔高。**如果做，唔好用 `points=150`
   做 smoke value**（§8.3：偶數 limit 必然全綠，係假綠）—— 用 **奇數** 如 97 或 249。
2. **`TRACK_POINTS` 改動應該當行為改動處理**。今日「零影響」繫於 600 呢個值
   （早返 + 599 奇數雙重保護）。調細做奇數即刻 >80% 長度分歧。建議喺
   `ActivityView.tsx` 嗰個常數旁邊留一句，指向 CUI-0021 / CUI-0026。
3. **Coverage 工具**：前端未裝 `@vitest/coverage-v8`，所以前端 coverage 冇數字
   （reviewer 亦遇到同一限制）。本批靠具名個案表補足，但長遠建議補呢個 devDependency。

---

## 15. Handoff receipt

```
HANDOFF_RECEIPT
protocol: 2
agent: quality-assurance
status: pass
critical: 0
report: .proj-docs/qa/2026-09-16_qa_low-cost-wave2_batch.md
tickets_pass: CUI-0004, CUI-0021, CUI-0015
tickets_blocked: none
new_tickets: CUI-0026
next_action: invoke-devops
notes: 5 個 item 全部通過；真實 export 端到端 418,471 個值跨 mode 零分歧，並以對照組證明該測試有牙；W-024 核心 finding 獨立證實為 false positive（原始 22.6% 係啱），另發現「tie 判準係奇偶而非質數」已開 CUI-0026。
```
