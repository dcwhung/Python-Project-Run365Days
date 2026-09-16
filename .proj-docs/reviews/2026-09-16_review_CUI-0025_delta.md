# Delta Re-Review — 2026-09-16 — CUI-0025 / CUI-0028 / CUI-0032 batch（Round 3）

| 項目 | 內容 |
|---|---|
| 審閱者 | Code Reviewer（獨立角色） |
| 日期 | 2026-09-16 |
| 類型 | **Delta Re-Review**（只審 delta，重新評分整批） |
| 上一輪 | `.proj-docs/reviews/2026-09-16_review_CUI-0025_batch.md` — **88/100 ⚠️ warn**，0 🔴 / 1 🟡（W-029）/ 7 🟢 |
| Delta 範圍 | `git diff 6cb7529..HEAD` — `b03583d`（W-029）、`0fcc293`（S-067）、merge `4d7335f` |
| 改動檔案 | `.tickets/in-progress/0001-0200/CUI-0028.md`（+83/−8）、`src/api/service.py`（+13/−0）、`tests/test_api.py`（+9/−0） |
| Branch | `claude/ai-dev-team-start-05jie2`（delta 已 merge 入嚟） |
| 涵蓋 ticket | CUI-0025、CUI-0028、CUI-0032 + S-053 / S-058 / S-059 / S-060 / W-029 / S-067 |
| 新 finding | **S-068 – S-071**（4 個 🟢，0 🔴 / 0 🟡）+ 1 個唔計分嘅 reviewer 自身 artefact 問題（S-072）+ 1 條流程越界紀錄 |

---

## 總評

**W-029 同 S-067 兩項都做妥，而且做得比我要求嘅多。** Developer 冇止於「照 review 講嘅改」——
佢逐個組合重做量度，結果**推翻咗我上一輪兩個講法**，兩個更正我都獨立重做過，**兩個都成立**。

呢一點值得講清楚：上一輪個 Warning 嘅內容（「票上嗰組形狀確實會撞，QA 由頭到尾都啱」）**完全企得住**，
但我為佢寫嘅**成因歸因同補救方法各錯咗一半**。Developer 唔係辯解，係攞證據更正 reviewer ——
喺一個靠 mutation testing 把關嘅 repo 度，呢個正正係應該發生嘅事。

扣分仍然集中喺同一個地方：**紀錄嘅準確性**。而且有少少諷刺 ——
一份專門用嚟更正「數字綁咗落錯嘅 artefact」嘅紀錄，自己又綁錯咗一個數字（S-068）。

> ⚠️ **上一輪 S-061 – S-066 六個 Suggestion 未做**（main agent 決定 delta 之後再定），
> 本輪評分**當佢哋仍然 open**，相關扣分原樣保留。

---

## 1. Hard Gates

全部喺 `/home/user/Python-Project-Run365Days` 主 checkout 實跑，非引述。

| Gate | 指令 | 結果 |
|---|---|---|
| Lint（Python） | `.venv/bin/ruff check src tests` | ✅ **pass** — All checks passed!（exit 0） |
| Format（Python） | `.venv/bin/ruff format --check src tests` | ✅ **pass** — 58 files already formatted（exit 0） |
| Lint（前端） | `npm run lint` | ✅ **pass** — exit 0，零 output |
| Type check | `npm run typecheck`（codegen + `tsc -b`） | ✅ **pass** — exit 0，codegen SUCCESS |
| SDL 同步 | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ **pass** — is up to date（exit 0） |
| Tests（Python） | `.venv/bin/python -m pytest tests -q` | ✅ **pass** — **402 passed** in 49.66 s（同上一輪一樣，冇加冇減） |
| Tests（前端） | `npm test` | ✅ **pass** — **131 passed / 22 files** |
| Coverage | `pytest tests -q --cov=src` | ✅ **pass** — **95% TOTAL**（1632 statements、82 miss，同上一輪逐字一樣） |
| No Critical | 本報告 | ✅ **pass** — **0 🔴** |
| Security scan | delta 零依賴改動 | ✅ **pass** — `git diff 6cb7529..HEAD -- pyproject.toml frontend/package*.json` **空**，上一輪 `pip-audit` 結論原樣延續 |

**Hard gates 10/10 pass。**

> 📌 402 / 131 / 95% 三個數同上一輪**逐字相同**，本身就係「delta 零行為改動」嘅第一重證據。
> 第二重（AST）見 §4.6。

---

## 2. 評分結果（重新評分整批）

| 維度 | 得分 | 滿分 | 備注 |
|------|------|------|------|
| 正確性 | 24 | 25 | 零行為缺陷（AST 證實）。S-061 仍 open −1 |
| 安全性 | 20 | 20 | delta 零依賴、零攻擊面改動 |
| 可維護性 | **13** | 20 | **W-029 已修 +5**、**S-067 已修 +1**；S-062 / S-063 / S-064 仍 open 各 −1；**新 S-068 / S-069 / S-070 / S-071 各 −1** |
| 測試覆蓋 | 13 | 15 | 95%，mutation teeth 我重做過（§4.5）。S-065 / S-066 仍 open 各 −1 |
| 性能 | 10 | 10 | 零行為改動 |
| 代碼風格 | 10 | 10 | ruff / eslint / tsc 全綠 |
| **總分** | **90** | **100** | |

**結果：✅ pass**（hard_gates 10/10 + 0 Critical + ≥ 90）

### 分數點解由 88 郁到 90

```
88（Round 2）
  + 5   W-029 修妥（假量度已撤回，紀錄三處更正）           可維護性 11 → 16
  + 1   S-067 修妥（CUI-0028 DoD #4 完成）                 可維護性 16 → 17
  − 1   S-068  pyc byte size 綁咗落錯嘅檔案                 可維護性 17 → 16
  − 1   S-069  「兩個成因都重現過」≠ 兩個都真係發生過        可維護性 16 → 15
  − 1   S-070  worked example 講一對，實際三行              可維護性 15 → 14
  − 1   S-071  「the suite's track lengths」講闊咗           可維護性 14 → 13
= 90
```

**+6 / −4 = 淨 +2。** 修嗰兩項係滿分做法，但更正文本本身帶入四個新嘅細微紀錄問題 ——
全部零行為風險、全部一兩句字可以修。

> S-072（Round 2 報告仍然帶住一個已被證偽嘅補救）**唔計分**：嗰份係 reviewer 自己嘅 artefact，
> 唔應該扣 developer 嘅分。

---

## 3. ⚠️ 三個裁決

### 3(a) 「用 `python -B`」係咪錯？ → **Developer 完全正確，我上一輪寫錯。裁決：採納更正。**

**我獨立重做嘅方法**（冇睇 developer 個 script，兩套環境各做一次）：

1. **最小模組**：`mod.py` 只有一行 `SEP = ':'`。先 import 一次寫低 `.pyc`，
   再改成 `SEP = '0'`，用 `os.utime` 把**秒級 mtime 還原成原值**（模擬同一秒內改檔）。
2. **真檔案**：把 `src/` 原樣 copy 去 scratch（**repo 一個字都冇改**），
   對 copy 入面嘅 `run365days/api/service.py` 做同一套。

**最小模組結果（`':'` → `'0'`，size 10 → 10）**：

| 組合 | 讀到嘅 `SEP` | 判定 |
|---|---|---|
| A. cache 保留，plain python | `':'` | ❌ stale，量錯 |
| B. cache 保留，**`python -B`** | `':'` | ❌ **stale，`-B` 救唔到** |
| C. 清 `__pycache__` | `'0'` | ✅ mutation 生效 |

**真 `service.py` copy 結果（size 20103 → 20103）**：

| 組合 | 讀到嘅 `SAMPLE_KEY_SEPARATOR` |
|---|---|
| A. cache 保留，plain | `':'` ❌ |
| B. cache 保留，`-B` | `':'` ❌ |
| C. cache 清走 | `'0'` ✅ |

**`-B` 到底做緊乜（直接量）**：

```
清走 cache 後跑 `python -B`      → __pycache__ 存在？ False
清走 cache 後跑 plain python     → __pycache__ 存在？ True
```

**→ `-B` 只係阻止 `.pyc` 被寫入，唔阻止現有嗰個被讀取。Developer 逐字啱。**

**點解呢個更正重要**：我上一輪寫嘅補救係「**要 `python -B` + 清 `__pycache__`**」。
呢句最大嘅問題唔係多餘咗一個 flag，而係**佢令 `-B` 睇落似係嗰個起作用嘅部分**。
下一個人如果只用 `-B`（好合理，因為聽落就係「唔好用 cache」嘅意思），
佢會以為自己避開咗個陷阱 —— **實際上佢嘅 mutation 完全冇生效，而測試會全綠**。
即係話我開 W-029 嗰個「假 pass」陷阱，我自己寫嘅補救會**製造**同一個假 pass。

> 📌 **正確嘅講法**：`-B` 對呢件事**零作用**。要真正避開，得三條路：
> (1) 清 `__pycache__`；(2) `PYTHONDONTWRITEBYTECODE` **唔係**答案，`--check-hash-based-pycs always` 或
> `SOURCE_DATE_EPOCH`-style hash-based pyc 先係；(3) **最穩陣：完全唔經檔案** ——
> 用 in-process `setattr(module, "CONST", value)` 再 `assert` 個 mutant 真係生效。
> **Developer 今次用嘅正正係 (3)，所以佢份新量度由頭到尾唔經 `.pyc`。** 呢個係最佳做法。

---

### 3(b) stale-`.pyc` 解釋唔解釋到全部三個 case？ → **Developer 嘅反駁成立，但佢自己個結論亦都講得太實。**

先講可以一刀切嘅部分。

**`""` 嘅 byte size 真係唔同**（最小模組 10 → 9；真 `service.py` 20103 → 20102），
所以佢**一定會重新 compile**。我實測 `':'` → `''` 三個組合：

| 組合 | 讀到 |
|---|---|
| A. cache 保留，plain | `''` ✅ |
| B. cache 保留，`-B` | `''` ✅ |
| C. cache 清走 | `''` ✅ |

**三個組合全部量到 `''`。→ stale `.pyc` 喺結構上唔可能解釋 Lane E 個 `""` 結果。裁決：Developer 啱。**

**Developer 提出嘅替代成因我亦重現到，而且一字不差。** 我用純算術（唔掂 DB）跑四個 separator：

| separator | **wanted-vs-wanted** clash | **wanted-vs-stored-rows**（即真正嘅 `IN` 語義） |
|---|---|---|
| `":"` | none | `{'1':7,'01':7,'10':7,'2':7,'20':7,'002':7}` ✅ |
| `"0"` | **none** | `{'1':7,'01':**10**,...}` ❌ 撞 |
| `""` | **none** | `{'1':7,'01':**10**,...}` ❌ 撞 |
| `"5"` | none | 全 7 ✅ |

**淨係比 wanted key 對 wanted key，四個 separator 一律報「冇碰撞」** ——
逐字符合 Lane E 報嘅「三個 separator 之下輸出**完全一樣**，冇碰撞」。

再用 repo 自己個 `_write_id_track_db` 行足真 DB path（in-process `setattr`、每個 separator 一個 fresh
process、`assert` 過 mutant 生效）：

```
sep=':'  effective=':'  counts={'1': 7, '01':  7, '10': 7, '2': 7, '20': 7, '002': 7}
sep='0'  effective='0'  counts={'1': 7, '01': 10, '10': 7, '2': 7, '20': 7, '002': 7}
sep=''   effective=''   counts={'1': 7, '01': 10, '10': 7, '2': 7, '20': 7, '002': 7}
sep='5'  effective='5'  counts={'1': 7, '01':  7, '10': 7, '2': 7, '20': 7, '002': 7}
```

**票上嗰組形狀確實會撞，QA 由頭到尾都啱。上一輪 W-029 嘅核心結論維持不變。**

#### ⚠️ 但「兩個成因都重現過」唔等於「兩個都真係發生過」

Developer 個結論係「第 1 點三個 case 全部解釋得到」。呢個陳述**係啱嘅**。
但佢喺 commit message 同 ticket 都寫成「**Two causes** for the bad measurement, **both reproduced here**」，
讀落係「成因搵到晒」。**呢一步跨大咗，而且我搵到第三個佢哋兩個都冇諗過嘅候選。**

**候選三：同一個 process 入面 re-import（`sys.modules` 命中）。** 我實測（真 `service.py` copy）：

```
first import (source ':')        -> ':'
source edited to '0' , re-import  -> ':'      ← 冇生效
source edited to ''  , re-import  -> ':'      ← 冇生效，連 "" 都冇生效
```

**`sys.modules` 命中同樣解釋得晒三個 case**，因為 `import` 對一個已 import 嘅 module 係 no-op，
**同 byte size、同 `.pyc`、同 `-B` 完全冇關**。

而且喺**措辭吻合度**上，佢比 wanted-vs-wanted **更貼**：
Lane E 寫嘅係「輸出**完全一樣**」—— 呢句係「行咗三次攞返同一份輸出」嘅語氣，
而 wanted-vs-wanted 檢查嘅自然講法係「冇 clash」而唔係「輸出一樣」。

（順帶：`importlib.reload` **唔係**豁免 —— 佢照行 `.pyc` 失效檢查，
mtime + size 一樣時同樣會攞 stale bytecode。）

**裁決**：
- ✅ Developer 啱：**stale `.pyc` 唔可能係唯一成因**，我上一輪個歸因要撤回。
- ✅ Developer 啱：**wanted-vs-wanted 係一個完整嘅單一成因解釋。**
- ⚠️ 但 **「兩個都有份」同「就係呢兩個」兩樣都未被證立** —— 至少有三個候選機制各自都解釋得晒。
  Lane E 嘅 script 已經冇咗，呢件事**原則上唔可能再裁決**。
- 📌 **點解呢點重要**：問題問「呢個影響將來點寫防範措施」。
  **既然成因裁決唔到，防範措施就唔可以綁死喺任何一個成因身上。**
  三個候選機制得**一個**做法可以同時擋晒：

  > **量一個常數 mutation 時：(1) 用 in-process `setattr` 改 module 物件，唔好改源檔；
  > (2) 改完即刻 `assert` 個值真係變咗；(3) 拎去同一個獨立 oracle 比
  > （例如 `builder.downsample`），唔好拎自己輸入嘅另一半去比。**

  (1) 令 `.pyc` 同 `sys.modules` 兩條路徑一次過失效，(2) 令「冇生效」即刻現形，
  (3) 令 wanted-vs-wanted 呢類自我比對唔可能發生。
  **Developer 今次份新量度三項全中** —— 佢已經用啱咗做法，只係紀錄裡面嘅歸因講實咗。
  → 記作 **S-069**。

---

### 3(c) 票上 illustrative pair 唔準？ → **Developer 完全正確，機制描述逐項核實。**

**核實一：`_even_positions(60, 7)`**（直接叫 `service._even_positions`）：

```
_even_positions(60, 7) = [0, 10, 20, 30, 39, 49, 59]
5 sampled?  False        50 sampled?  False
```

✅ 票上寫嘅 `position=5, id="01"` / `position=50, id="1"` —— **兩個 position 都唔係 sampled position**。
兩條 key 砌出嚟確係都等於 `"5001"`（`"5"+"0"+"01"` 同 `"50"+"0"+"1"`），
**所以係一個成立嘅字串碰撞，但喺票上嗰個 repro 入面唔係 live 嘅嗰對。**
Developer 講「個結果（10 行）啱，但呢一對唔係實際嗰對」—— **precise，冇過度否定 QA。**

**核實二：真正嘅 stray 來源**（逐 row 追溯，`sep="0"`）：

```
'01': 10 rows; honest=[0, 10, 20, 30, 39, 49, 59]; strays at positions [1, 2, 3]
    row 1 key '1001' <- wanted key of [('1', 10)]
    row 2 key '2001' <- wanted key of [('1', 20)]
    row 3 key '3001' <- wanted key of [('1', 30)]
```

`sep=""` 同樣三對，key 分別係 `'101'` / `'201'` / `'301'`。

✅ **逐字符合 developer 嘅描述**：係 id `"1"` 自己嘅 sample 伸咗入 `"01"`，
`10/"1"` 同 `1/"01"` 都砌出 `"1001"`，`20/2`、`30/3` 亦然。
**7 個誠實 sample + 3 個 stray = 10。裁決：機制描述正確。**

**核實三：同 `COLLIDING_IDS` worked example 係咪同一個機制**（實跑 fixture）：

```
sep=':'  {'1': 0, '01': 1, '5': 7, '05':  7, '50': 7, '500': 7}
sep='0'  {'1': 0, '01': 1, '5': 7, '05': 10, '50': 7, '500': 7}
         '05' rows: [0, 1, 2, 3, 17, 33, 50, 67, 83, 100]
```

`_even_positions(101, 7) = [0, 17, 33, 50, 67, 83, 100]`，
stray 喺 position 1 / 2 / 3，由 id `"5"` 嘅 sampled position 10 / 20 / 30 拉入嚟。
✅ **同票上嗰組一模一樣嘅機制** —— developer 講「Lane E 換 fixture 之後砌嘅係同一個機制，
只係佢以為自己推翻咗票上嗰個」，**成立**。

---

## 4. 其餘核實項

### 4.1 W-029 三個更正記錄嘅地方，內容準唔準？

| 位置 | 內容 | 判定 |
|---|---|---|
| `.tickets/.../CUI-0028.md`「執行備註」 | 假聲稱兩行加咗 `~~刪除線~~` + 「→ **呢句係錯嘅，已由 W-029 撤回**」 | ✅ |
| 同上，`實際會碰撞嘅係…` 一句 | **冇**被刪除線，改成「實際會碰撞嘅**其中一對**係…」+「（呢段係啱嘅，已重新驗過）」 | ✅ **改得準** —— 呢句本身冇錯，只係唔完整，用限定詞而唔係刪除線係啱嘅處理 |
| 新增「更正（W-029）」section | 四個 separator 嘅量度表、碰撞來源表、成因、影響同處置 | ✅ 表入面每個數我都重做過，**全部對得上** |
| `b06d28e` commit message | 已 merge 兼 push，改唔到 → 改為喺 `b03583d` 嘅 message 明文更正 | ✅ **處理正確**（唔 rewrite history，符合我上一輪方案 A） |
| `tests/test_api.py` `COLLIDING_IDS` docstring | append 一段講明票上原形狀一樣會撞 | ✅ |
| 狀態歷史表 | 加一行 `in-progress → in-progress`，寫明 W-029 / S-067 | ✅ |

⚠️ 一處唔準 → **S-068**（見 §6）。

### 4.2 `COLLIDING_IDS` docstring 係咪真係「本身冇假聲稱所以冇改，只係 append」？

我逐句讀返 `6cb7529` 版本原文核實：

- 「This is the shape the rest of the suite has no fixture for…」 ✅ 真（`_write_track_db` 只會出 `r0` / `v0..v8` 呢類非十進制前綴 id）
- 「The other fixtures number their activities `r0` and `v0`..`v8`…」 ✅ 真
- 「Production ids are pure decimal but ten characters wide」 ✅ 同票上 §影響範圍 QA 嘅紀錄一致（4,069,984 次 field 比對）
- worked example ✅ 實跑證實（§3(c) 核實三）
- **全篇冇任何一句聲稱「其他形狀唔會撞」** ✅

**→ Developer 講「原 docstring 本身冇假聲稱，所以只加不改」，核實成立。**
（我上一輪寫「刪走暗示其他形狀唔會撞嘅措辭」，其實個 docstring 冇咁嘅措辭 ——
**呢個亦係我上一輪嘅一個唔準確，developer 冇照做係啱嘅。**）

⚠️ 不過個 worked example 有另一個精度問題 → **S-070**。

### 4.3 Fixture / assertion / worked example 真係一個字都冇改？

```
$ diff <(git show 6cb7529:tests/test_api.py | grep -n "^COLLIDING_IDS = ") <(grep -n ...)
identical
COLLIDING_TRACK_LENGTHS: 值完全相同（只係行號由 2249 → 2258，即 +9，全部係新 docstring 行）
test 函數 body（兩條新測試 + fixture）: diff 完全空 — "test bodies identical"
tests/test_api.py 整個 delta: 9 行，全部係 `+`，全部喺 docstring 之內，零 `-` 行
```

✅ **確認：fixture、assertion、worked example 一個字都冇改。**

### 4.4 S-067：docstring 點名兩條 test，名啱唔啱？描述準唔準？

| 檢查 | 結果 |
|---|---|
| `test_the_sample_key_separator_cannot_occur_in_a_position` 存在 | ✅ 1 個定義 |
| `test_a_batch_of_variable_length_ids_thins_each_track_independently` 存在 | ✅ 1 個定義 |
| 「until CUI-0028 nothing did — 呢個常數可以改成十進制數字或者掉走，成個 suite 照綠」 | ✅ **直接證實**：兩個 mutant 之下都係 **395 passed**，即除咗呢兩條之外**全部照綠**（見 §4.5） |
| 「the only fixture shape that can show it」 | ✅ 真：`_write_track_db` 係唯一另一個 builder，永遠帶非十進制前綴 |
| 「Every other fixture … carries either a non-decimal prefix or a fixed width, and either one makes the key injective on its own」 | ✅ 真，而且**比原 docstring 嗰句「a non-decimal prefix at a fixed width」更準**（兩個條件各自獨立足夠，唔使同時成立） |
| 「over the positions **the suite's track lengths** render」 | ⚠️ **講闊咗** → **S-071** |

### 4.5 Mutation teeth —— 我自己重做

**方法**：一個 injected pytest plugin（放喺 scratchpad，經 `PYTHONPATH` + `-p mutplug` 載入，
**repo 零改動**），喺 `pytest_configure` 用 `setattr` 改 module 物件再 `assert` 生效，
所以由頭到尾**唔經 `.pyc`**。

```
########## MUT_SEP='0' ##########
[mutant] SAMPLE_KEY_SEPARATOR = '0' (in-process setattr, no .pyc)
FAILED test_the_sample_key_separator_cannot_occur_in_a_position
FAILED test_a_batch_of_variable_length_ids_thins_each_track_independently[3]
FAILED ... [5] [7] [10] [21] [37]
7 failed, 395 passed

########## MUT_SEP='' ##########
[mutant] SAMPLE_KEY_SEPARATOR = '' (in-process setattr, no .pyc)
FAILED test_the_sample_key_separator_cannot_occur_in_a_position
FAILED test_a_batch_of_variable_length_ids_thins_each_track_independently[3]
FAILED ... [5] [7] [10] [21] [37]
7 failed, 395 passed
```

✅ **兩個 mutant 各 7 failures，各自令同樣嗰兩個 guard 紅**（property test ×1 + batch test ×6 params）。
✅ 全部係 `FAILED` 唔係 `ERROR` —— **真係 AssertionError，唔係 collection error**
（CLAUDE.md §6「TDD Red 階段」嗰個陷阱冇踩到）。
✅ 其餘 **395 條照綠**，即係「呢兩條就係全部牙齒所在」。
**Developer 嘅聲稱逐項成立。**

### 4.6 Production 行為零改動 —— AST 核實

```python
AST identical after stripping all string-literal statements: True
raw AST identical (docstrings included):                     False
```

即係話 `src/api/service.py` **剝走所有 string literal statement 之後 AST 完全一樣**，
差異**只**存在於 docstring。配合 diff（+13 行，全部喺 `SAMPLE_KEY_SEPARATOR` docstring 之內，零 `-` 行）：

✅ **確認：production 行為零改動。**（第三重證據：402 / 131 / 95% 三個數同上一輪逐字相同。）

### 4.7 CUI-0028 DoD 五項而家全部可以剔？

| # | DoD 項目 | 核實 | 判定 |
|---|---|---|---|
| 1 | separator 改成十進制數字或空字串時會紅 | `"0"` / `""` 兩個 mutant 都令 property test 紅 | ✅ |
| 2 | 覆蓋 activity id 變長嘅 batch | `COLLIDING_IDS = ("1","01","5","05","50","500")`，六個寬度唔一 | ✅ |
| 3 | mutation 驗過真係有牙 | §4.5，7 failures × 2，AssertionError | ✅ |
| 4 | docstring 指返測試名 | §4.4，兩個名都對，描述基本準確 | ✅ |
| 5 | `pytest` 全綠、`ruff check` / `format --check` clean | §1，三項 exit 0 | ✅ |

✅ **五項全部可以剔。CUI-0028 可以由 `in-progress` 移去 `completed`。**
（ticket 檔案現時仍在 `.tickets/in-progress/0001-0200/CUI-0028.md` —— 移檔係 main agent 嘅動作，唔係 reviewer 嘅。）

---

## 5. 🔴 Critical

**零。**

## 5b. 🟡 Warning

**零。W-029 已 close。**

---

## 6. 🟢 Suggestion（新）

### S-068｜「size 10 vs 10 / size 9」係一個**最小重現檔**嘅 byte size，唔係 `service.py` 嘅

- **位置**：`.tickets/in-progress/0001-0200/CUI-0028.md` §「點解 Lane E 會量錯」第 2 點；
  `b03583d` commit message
- **原文**：

  > `SAMPLE_KEY_SEPARATOR = "0"` 同 `= ":"` **byte size 一樣**（**實測 size 10 vs 10**、秒級 mtime 相同）
  > …… `""` **size 9**，會重新 compile

- **實測**：

  | 檔案 | `":"` | `"0"` | `""` |
  |---|---|---|---|
  | `src/api/service.py`（真正宣告呢個常數嘅檔案） | **20103** | **20103** | **20102** |
  | `SAMPLE_KEY_SEPARATOR = ":"` **嗰一行** | 26 | 26 | 25 |
  | 一個只有 `SEP = ':'` 嘅最小模組 | **10** | **10** | **9** |

  **10 / 10 / 9 只對得上最後一行。** 我自己個最小重現檔逐字跑出同一組 10 / 10 / 9，
  所以呢三個數幾乎肯定係由一個 toy module 量返嚟。
- **描述**：**個關係（相等 vs 少一）啱，個結論（`""` 會重新 compile）啱，
  但個數字綁咗落一個唔係討論對象嘅 artefact**，而紀錄用咗「實測」兩個字。
- **點解值得記**：**呢個正正係 W-029 本身嗰個失敗模式的重演** ——
  W-029 就係「一個量度被綁落錯嘅對象」。一份專門用嚟更正呢件事嘅紀錄，
  唔應該再犯同一招。（同 Round 2 S-063「At that widest 綁咗落錯嘅 document」同一類。）
- **影響**：**零行為風險**，純紀錄精度。但下一個人如果照住 `stat` 真檔案，
  會見到 20103 而唔係 10，然後懷疑成段分析。
- **方案 A（推薦）**：兩處各補一個限定詞 ——
  「（喺一個最小重現檔上量：10 vs 10；`src/api/service.py` 上係 20103 vs 20103 / 20102，
  關係相同）」。Trade-off：一句字，而且順手把真檔案嘅數寫低，令下一個人 `stat` 得返。
- **方案 B**：直接把 10 / 10 / 9 換成 20103 / 20103 / 20102。
  Trade-off：最短，但會失去「呢個係一個可以獨立重跑嘅最小重現」呢個有用資訊。
- **🎯 推薦：方案 A。**

### S-069｜「Two causes … both reproduced here」講實咗；至少有三個候選機制各自解釋得晒

- **位置**：`b03583d` commit message；`.tickets/.../CUI-0028.md` §「點解 Lane E 會量錯（兩個都重現過）」
- **描述**：詳細裁決見 §3(b)。扼要：
  - 「重現到一個機制」≠「呢個機制當時真係發生過」。
  - 第三個候選 —— **同一個 process 入面 re-import（`sys.modules` 命中）** ——
    我實測**同樣解釋得晒三個 case**（`'0'` 同 `''` 兩次 re-import 都仍然讀到 `':'`），
    而且同 Lane E「輸出**完全一樣**」嘅措辭**更吻合**。
  - Lane E 嘅 script 已經冇咗，呢件事**原則上裁決唔到**。
- **影響**：**零行為風險**。但問題本身係「呢個影響將來點寫防範措施」——
  **防範措施綁死喺一個未證立嘅成因上，就會漏咗另外兩個。**
- **方案 A（推薦）**：section 標題由「兩個都重現過」改成「**兩個候選成因（都重現到，但都證唔到當時就係佢）**」，
  尾段補一句方法學結論：

  > 成因裁決唔到，所以防範措施唔綁成因，綁方法：常數 mutation 一律
  > (1) in-process `setattr` 改 module 物件、唔改源檔；(2) 改完即刻 `assert` 個值真係變咗；
  > (3) 同一個獨立 oracle 比（`builder.downsample`），唔好同自己輸入嘅另一半比。

  Trade-off：三句字，而且**呢三項 developer 今次已經全部做咗** —— 只係寫低佢。
- **方案 B**：維持原文，喺本 review 報告記低就算。
  Trade-off：最平，但 ticket 係後續 agent 會直接引用嘅 SSoT，
  「成因已查明」同「成因查唔到但方法已收緊」對下一個人係兩件唔同嘅事。
- **🎯 推薦：方案 A。**

### S-070｜`COLLIDING_IDS` docstring 點名一對，但實際返多三行

- **位置**：`tests/test_api.py` `COLLIDING_IDS` docstring（**原有段落，非 delta 新增**）
- **原文**：

  > `position=10` on id `"5"` and `position=1` on id `"05"` … That pair is live in this
  > fixture: `"05"` comes back with **10 rows instead of 7**.

- **實測**：`"05"` 回 `[0, 1, 2, 3, 17, 33, 50, 67, 83, 100]` —— **三行 stray（position 1 / 2 / 3）**，
  分別由 id `"5"` 嘅 sampled position **10 / 20 / 30** 拉入嚟。
  點名嗰對只係**三對之中嘅一對**。
- **描述**：個 example 本身**完全正確**（我上一輪同今輪都驗過），
  但「That pair is live … `"05"` comes back with 10 rows」讀落好容易理解成
  「呢一對造成咗 +3」。**Developer 喺 ticket 入面正正為票上嗰組形狀補齊咗三對表格，
  但冇為 fixture 自己個 docstring 做同一件事。**
- **影響**：**零** —— 測試守嘅係逐行比對，唔靠呢句。純粹係「同一份 delta 入面兩處標準唔一致」。
- **方案 A（推薦）**：`"That pair is live in this fixture"` → `"That pair is one of three live here
  (positions 10, 20 and 30 on "5" reaching rows 1, 2 and 3 of "05"), which is why "05" comes back
  with 10 rows instead of 7."` Trade-off：一句字，而且同 ticket 入面嗰個表對齊。
- **方案 B**：保持點名一對，把結論句改成「and collisions of this kind cost `"05"` 10 rows instead of 7」，
  唔明示數量。Trade-off：最細改動，但少咗「3 對」呢個對讀者好有用嘅數。
- **🎯 推薦：方案 A。**

### S-071｜「the positions **the suite's track lengths** render」講闊過個 sweep

- **位置**：`src/api/service.py`，`SAMPLE_KEY_SEPARATOR` docstring（delta 新增）
- **描述**：個 sweep 實際只行 `COLLIDING_TRACK_LENGTHS + VARIED_TRACK_LENGTHS`。
  但 suite 仲有幾組 track length 唔喺呢兩個常數入面：
  `[PREFETCH_DB_STORED] * PREFETCH_DB_ACTIVITIES`（:1551）、`[points + 1] * activities`（:1668）、
  `BATCH_COST` 嗰組（:1891）。**「the suite's track lengths」字面上包晒佢哋，個 sweep 冇。**
- **影響**：**零** —— 嗰條測試有一句
  `assert set("".join(str(p) for p in positions)) == set("0123456789")`，
  即係**十個數字全部出現過**，所以任何單字元十進制 separator 都一定被捉到，
  漏咗嘅 length 補唔到額外保障。**係一個措辭問題，唔係一個 gate 缺口。**
- **方案 A（推薦）**：「over the positions **`COLLIDING_TRACK_LENGTHS` and `VARIED_TRACK_LENGTHS`**
  render」，順手講埋點解夠：「the sweep is held to covering all ten digits, so a narrower
  set of lengths cannot let a digit through」。
  Trade-off：兩隻字，而且把「點解呢個 sweep 夠」寫入 docstring（現時只喺測試 inline comment 入面）。
- **方案 B**：把 sweep 擴闊到涵蓋全部 fixture length。
  Trade-off：**唔值** —— digit-completeness assertion 已經令額外 length 冇邊際收益，
  反而令測試同更多 fixture 耦合。
- **🎯 推薦：方案 A。**

### S-072｜（**唔計分** — reviewer 自身 artefact）Round 2 報告仍然帶住一個已被證偽嘅補救

- **位置**：`.proj-docs/reviews/2026-09-16_review_CUI-0025_batch.md` §W-029「🎯 推薦」段
- **原文**：「…… 要 `python -B` + 清 `__pycache__`，或者每次 mutation 開 fresh process ……」
- **描述**：`-B` 嗰半**已經由 §3(a) 證偽**。Developer 已經更正咗 ticket，
  但**review 報告係 reviewer 嘅 artefact，developer 冇改（而且唔應該改）**。
  結果係 `.proj-docs/reviews/` 入面仍然有一份會誤導人嘅補救 ——
  而呢個 folder 同 `.tickets/` 一樣係後續 agent 會直接引用嘅。
- **點解唔計分**：呢個係**我自己上一輪寫錯嘅嘢**，唔應該扣 developer 分。
- **方案 A（推薦）**：喺 Round 2 報告 §W-029 嗰段加一行 superseded 指標：

  > ⚠️ **本段嘅 `-B` 建議已被證偽，見 `2026-09-16_review_CUI-0025_delta.md` §3(a)。
  > `-B` 只阻止寫入 `.pyc`，唔阻止讀取。**

  Trade-off：一行字。**但呢個係改一份已經 merge 嘅報告，屬 main agent 決定，本 agent 唔自行執行。**
- **方案 B**：唔郁，靠本份 delta 報告自己撐住。
  Trade-off：最平，但下一個人好可能只讀 batch 報告（因為個名冇「delta」），就會攞到錯嘅補救。
- **🎯 推薦：方案 A，由 main agent 決定執唔執行。**

---

## 7. ⚠️ 流程越界紀錄（非 blocker、不計分）

**Developer push 咗自己條 branch 去 origin。**

```
$ git ls-remote --heads origin | grep W-029
0fcc293a38ba99db993a1cc2f971c9320f1e81bd    refs/heads/fix/api/W-029_correct-the-collision-record
```

`git branch -r` 亦見到 `origin/fix/api/W-029_correct-the-collision-record`。

- **邊界**：lane brief 明文寫住 ❌ `git push`。
- **實際後果**：**零**。係一條 feature branch，內容已經 merge 入 `4d7335f`，
  冇掂 `develop` / `master`，冇觸發 CI deploy（CLAUDE.md §4：deploy 只喺 `refs/heads/develop`）。
- **點解仍然記低**：一條**明文**邊界被跨過。呢個 repo 嘅 branch policy hook 只 hard block
  「喺 `master` / `develop` 直接 commit」，**唔會 block push feature branch** ——
  即係話呢條邊界今日**淨係靠 agent 自律**。
  同一類越界如果發生喺一個 target 揀錯嘅 push 上，後果就唔係零。
- **建議**（給 main agent / 用戶，唔係 reviewer 執行）：
  1. 如無其他用途，可以清走 `origin/fix/api/W-029_correct-the-collision-record`；
  2. 如果想令呢條邊界唔再靠自律，`skills/sw-hooks` 可以加一個 pre-push hook，
     只准 push 去 lane brief 允許嘅 ref。**呢個係用戶決定，唔係我。**

---

## 8. ✅ 做得好嘅地方

1. **更正 reviewer 而唔係照單全收。** 兩個更正（`-B` 冇用、stale-`.pyc` 解釋唔晒）
   我逐個獨立重做，**兩個都成立**。呢個係 review 制度應該有嘅雙向性 ——
   reviewer 唔係免檢。
2. **新量度方法係三個候選成因之中唯一全部擋得住嘅。** In-process `setattr`
   + `assert` mutant 生效 + 每 separator 一個 fresh process。
   **佢用嘅方法比佢寫落去嘅歸因更可靠** —— S-069 要修嘅係文字，唔係做法。
3. **對 QA 嘅平反做得精準，冇過度補償。** 票上個 illustrative pair 確實唔準（5 / 50 都唔係 sampled），
   developer 冇為咗「QA 啱」而連錯嘅細節都一齊認 ——
   佢分開處理：結論啱、pair 唔啱、補返真正嗰三對。
4. **「原 docstring 冇假聲稱所以只加不改」係一個克制而正確嘅判斷。**
   我上一輪叫佢「刪走暗示其他形狀唔會撞嘅措辭」，
   **佢去讀返原文，發現冇咁嘅措辭，於是冇亂改。** ✅ 我核實過，佢啱。
5. **Fixture / assertion / worked example 一個字都冇改**（diff 證實），
   完全守住「更正紀錄唔等於改測試」呢條線。402 / 131 / 95% 三個數逐字不變。
6. **唔 rewrite 已 merge 嘅 history。** `b06d28e` 改唔到，就喺新 commit message 明文更正 ——
   符合方案 A，亦係正確嘅 git 衛生。
7. **Commit 粒度守規矩**：`fix: W-029 | …` 同 `docs: S-067 | …` 兩個獨立 commit，
   一個 review item 一個 commit，冇夾帶。
8. **Commit message 本身就係一份可重跑嘅實驗紀錄** —— 四個 separator 嘅表、A/B/C 三個組合、
   size 對比全部寫低。除咗 S-068 嗰個數綁錯檔案之外，**呢個係 repo 入面 commit message 應該有嘅樣**。

---

## 9. 修正優先順序

| 順序 | ID | 內容 | 阻唔阻結案 | 估計成本 |
|---|---|---|---|---|
| — | — | **W-029 / S-067：已 close** ✅ | — | — |
| 1 | **S-069** | ticket / commit 更正「兩個成因都重現過」嘅講實咗措辭，補方法學結論 | ❌ 唔阻 | 低（3 句） |
| 2 | **S-068** | `10 / 10 / 9` 補上「最小重現檔」限定詞 + 補真檔案數字 | ❌ 唔阻 | 低（1 句 ×2 處） |
| 3 | **S-072** | Round 2 報告加 superseded 指標（**main agent 決定**） | ❌ 唔阻 | 低（1 行） |
| 4 | **S-070** | `COLLIDING_IDS` docstring：一對 → 三對 | ❌ 唔阻 | 低（1 句） |
| 5 | **S-071** | `SAMPLE_KEY_SEPARATOR` docstring：sweep 範圍講返準 | ❌ 唔阻 | 低（2 隻字） |
| — | S-061 – S-066 | **上一輪遺留，仍然 open**（main agent 決定幾時做） | ❌ 唔阻 | 見 Round 2 §9 |

**十個 Suggestion 全部唔阻結案。CUI-0025 / CUI-0028 / CUI-0032 三張票喺技術上可以結。**

---

## 10. 修訂後代碼

**本輪零代碼修訂。** 四個新 Suggestion 全部係 docstring / ticket 文字，
上面每個 finding 已附方案 A 嘅確實措辭，唔重複貼整份檔案。
Production code（`src/api/service.py`）**AST 證實零行為改動**，冇嘢要改。

---

## 11. 驗證環境聲明（強制）

- 所有量度喺 `/home/user/Python-Project-Run365Days` 主 checkout 或 scratchpad 內嘅 **copy** 上進行。
- `.pyc` / `-B` 實驗**全部喺 scratchpad 嘅 `src/` copy 上做**，主 repo 嘅 `src/api/service.py` 由頭到尾冇被寫過。
- Mutation teeth 用 injected pytest plugin（`PYTHONPATH` + `-p mutplug`），**repo 零改動**。
- 收工核查：

  ```
  $ git status --porcelain     → (空)
  $ git diff --stat            → (空)
  $ git diff --cached --stat   → (空)
  ```

  ✅ **主 checkout 一個字都冇被改過，冇遺留任何臨時改動。**
- 本 agent **冇**執行任何 `git merge` / `git push` / `git commit`。

---

## 12. Handoff receipt

```handoff-receipt
protocol: 1
agent: code-reviewer
status: pass
score: 90/100
critical: 0
warning: 0
suggestion: 4
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "95%"
report: .proj-docs/reviews/2026-09-16_review_CUI-0025_delta.md
tickets: CUI-0025, CUI-0028, CUI-0032
next_action: end
next_agent: null
branch: "claude/ai-dev-team-start-05jie2"
context: "Delta re-review of b03583d (W-029) + 0fcc293 (S-067): hard gates 10/10 pass, 0 Critical, 0 Warning, score 88 -> 90 = pass. W-029 and S-067 both closed; CUI-0028's five DoD items all verifiable, ticket can move to completed. Developer corrected the reviewer twice and both corrections are upheld on independent re-measurement: python -B does NOT avoid the stale-.pyc trap (it blocks writing, not reading -- only clearing __pycache__ measured the mutant), and stale .pyc cannot explain the '' reading since that size differs and recompiles under all three lanes. Comparing wanted keys against wanted keys is a complete single cause and reproduces Lane E's wording exactly; a third mechanism neither of us named (same-process re-import, a sys.modules hit) explains all three too, so the cause is undecidable and the prevention must be method-based (in-process setattr + assert the mutant took + compare against an independent oracle) -- which is what the developer actually did. Four new Suggestions, all docstring/ticket wording, none blocking: S-068 the '10 vs 10 / 9' byte sizes belong to a minimal repro file, not service.py (20103/20103/20102); S-069 'two causes, both reproduced' overstates attribution; S-070 COLLIDING_IDS' worked example names one colliding pair but three rows arrive; S-071 'the suite's track lengths' is wider than the sweep. S-072 is not scored: the Round 2 report still carries the disproven -B remedy and should get a superseded pointer -- that is the reviewer's own artefact, main agent's call. Production behaviour zero change, AST-verified. S-061 to S-066 remain open and were scored as open. Process note, not a blocker: the developer pushed fix/api/W-029_correct-the-collision-record to origin although the lane brief forbade git push; consequence is zero (feature branch, already merged, no CI deploy) but an explicit boundary was crossed. Working tree left clean -- git status, git diff and git diff --cached all empty."
```
