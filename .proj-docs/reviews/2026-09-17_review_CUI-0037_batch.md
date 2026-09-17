# Batch Review — 2026-09-17 — CUI-0037 API refusal lane

**審閱者**：Code Reviewer（獨立，未參與實作）
**目標**：`git diff 6576e56..06ad4bd -- src/api/ tests/test_api.py`，五張票一條 lane（`b73510e` … `aed2bd2`）
**Base**：`06ad4bd`（四條 lane 嘅 merge 結果）
**環境**：Python 3.11.x、strawberry-graphql 0.327.7、graphql-core 3.2.12、Flask 3.1.3、SQLAlchemy 2.0.54、ruff 0.16.8

## 整體 verdict

- 涵蓋 commit：`b73510e`(CUI-0037) / `856a821`(CUI-0038) / `93da9d6`(CUI-0039) / `c87f518`(CUI-0040) / `2d0742c`(CUI-0005) / `aed2bd2`(docs)
- 整體 score：**92 / 100**
- Status：**✅ pass**（hard gates 全 pass、0 🔴 Critical）
- 一句總評：五個 fix 全部做到佢哋聲稱嘅嘢，而且**安全面係淨正**——4 條洩露路關晒（每條 9 frame / 6 個絕對路徑 / ~2.4KB → 0 byte），3 條應該保留嘅 ERROR + traceback 全部原封不動。lane 自報嘅 7 條推翻，我獨立重驗後 **6 條成立、1 條部分成立**，另外**自己搵到一個 lane 漏報嘅活 mutant**（見 W-030）。

### Hard Gates

| Gate | 檢測方式 | 實測 | 結果 |
|---|---|---|---|
| Lint (Python) | `ruff check src tests api` | All checks passed | ✅ pass |
| Format (Python) | `ruff format --check src tests` | 60 files already formatted | ✅ pass |
| Lint (FE) | `npm run lint` | eslint 無輸出 | ✅ pass |
| Type check | `npm run typecheck`（codegen + `tsc -b`） | codegen SUCCESS、tsc 無錯 | ✅ pass |
| Tests (Python) | `pytest tests -q` | **492 passed in 60.09s** | ✅ pass |
| Tests (FE) | **`npm test`** | **141 passed / 25 files** | ✅ pass |
| Coverage | `pytest --cov=src` | `api/schema.py` **100%**、`api/service.py` **100%**、`api/app.py` **100%**、TOTAL **96%** | ✅ pass |
| No Critical | 本次 review | 🔴 = 0 | ✅ pass |
| Security scan | `npm audit --omit=dev` | **found 0 vulnerabilities** | ✅ pass |
| SDL 同步 | `run365-schema --check frontend/schema.graphql` | up to date | ✅ pass |
| 分層鐵律 | `grep strawberry src/api/service.py` | 零 import | ✅ pass |

> 全部數字同任務書預期值逐項對上，冇一個要我修正。

### 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|---|---|---|---|
| 正確性 | 24 | 25 | 行為全對；扣 1 分係 S-083 一句推理過度宣稱 |
| 安全性 | **20** | 20 | 淨值為正，且三條保留路逐條實測未被誤殺 |
| 可維護性 | 18 | 20 | S-084 / S-086 |
| 測試覆蓋 | 10 | 15 | W-030：`logging.DEBUG` mutant 喺 492 條測試面前**生還** |
| 性能 | 10 | 10 | 無回歸；新長跑測試只佔 1.32s |
| 代碼風格 | 10 | 10 | ruff 全清 |
| **總分** | **92** | **100** | |

**結果：✅ pass**

---

## Lane 自報推翻 —— 逐條獨立裁決

| # | Lane 講法 | 我嘅獨立實測 | 裁決 |
|---|---|---|---|
| 1 | 「7 個 token」錯，實際 **11**，用 `MaxTokensLimiter` 嘅計法 | 用 graphql-core `parse(max_tokens=N)` 二分：`{ activities(offset: -1) { id } }` = **11 token / 33 byte**；CUI-0029 個 budget document = **57 token / 168 byte**（完全對上佢引用嘅 57）。讀 `MaxTokensLimiter` 源碼：佢淨係 `parse_options["max_tokens"] = ...`，即係 graphql-core parser budget 本身 | ✅ **成立**，歸因亦準確 |
| 2 | 票上 byte 數唔可移植；差額係 checkout 絕對路徑長度 | worktree prefix 78 char vs 主 checkout 36 char，delta 42。實測每條 traceback **prefix 出現剛好 9 次**；把 prefix 改寫返主 checkout，四個 scenario **每個都 drop 剛好 378 = 9 × 42** | ✅ **機制成立**（見下方「部分成立」註） |
| 3 | CUI-0005 失敗點係 **GC 嘅性質**唔係 pool 嘅 | 決定性實驗（mutant = `call_on_close`，真 pool，1s timeout）：<br>`hold_refs=True` → **每次都 request 16**（= pool 5+10 ceiling+1，完全確定）<br>`hold_refs=False, cyclic GC on` → **request 39**（遊走）<br>`hold_refs=False, gc.disable()` → **request 16**<br>⇒ 真正還 connection 嘅係**循環 GC**（唔係純 refcount），關咗佢就即刻退返去 pool-bound | ✅ **成立，而且比佢講得更準**。fixture 揸住 reference「把 GC 剔出量度」係啱嘅設計 |
| 4 | CUI-0038 票上 553 bytes 錯，實際 **341**（bounds **103**） | configureless logging + OS fd 2：`REFUSAL_LOG_LEVEL=WARNING` 之下 budget refusal = **341 byte**、bounds(offset) = **103 byte**；`INFO` 之下兩者都 = **0** | ✅ **完全成立**（逐 byte 對上） |
| 5 | CUI-0039：mutant 多出嗰條 ERROR record **冇 `exc_info`**，所以改用數 record | 實測 mixed operation + unfiltered `super()`：共 3 條 record、2 條 ERROR，其中 **1 條 `exc_info=NONE`**（就係被重覆 log 嘅 refusal）。「每條 fault 都有 traceback」式寫法會**行過** | ✅ **成立**，係本 lane 最有價值嘅發現 |
| 6 | Amend 更正咗過度宣稱（widen mutant 同時殺死另外兩條測試） | widen 做 `GraphQLError` 實測**killed by 8 個 test id / 4 個 test function**：`unseeded_budget`(×3 param)、`unexpected_resolver_error`、`int_coercion`(×2)、`fault_still_writes_where_no_logging_is_configured`、`refusal_and_a_fault_in_one_operation` | ⚠️ **方向啱但仍然數少咗**（見 S-084） |
| 7 | CUI-0040 揀 raise 唔 clamp，因為 service 層 clamp 只可以向上 clamp 去 `MAX_TRACK_POINTS` | 用 `git show 6576e56:src/api/service.py` exec 返**真‧修前 module**：`-1` / `-5` / `-1000` **每個都剛好回 1 row**，同 `points=1` 一模一樣；falsy(`0`/`None`/`False`) = 1000 = ceiling | ✅ **事實前提完全成立**；**決定我同意**（raise 係啱）。但「would have to clamp upward」呢句推理本身過度（見 S-083） |

### 關於 #2「純粹係路徑長度」嘅 nuance

機制我驗到係鐵證：**9 frame × 42 char = 378，四個 scenario 一個唔差**。但 lane 報俾你嗰四對數（票 2016/2046/2145/2154 → 佢 2394/2424/2507/2516）嘅差額係 **378 / 378 / 362 / 362**——後兩個差 16 byte 唔係路徑解釋到。所以口頭講「**純粹**」略為過度。

**不過代碼入面嗰句寫嘅係 "dominated by the absolute length of the checkout's own path"**——「dominated by」係準確嘅（378 佔咗成個差額嘅絕大部分），而且**佢選擇唔把 byte 數寫成 assertion 係完全正確嘅判斷**。呢條唔開 finding。

---

## Section A — CUI-0037（抽 `ClientRefusalError` base + `BoundsError`）

**Commit**：`b73510e` ｜ **改動**：`src/api/schema.py`、`tests/test_api.py`

### 安全面淨值（真 `api/graphql.py` entry + 真 WSGI server + 真 DB + **OS 層 fd 2 捕捉**）

修前用 in-process `setattr` 把 `_page` / `_track_points` 還原成 bare `ValueError`（oracle 已 assert 兩個 helper 真係掟返 `ValueError` 先開跑）：

| Scenario | 修前 stderr | frames | 洩露絕對路徑 | 修後 stderr | 客戶端 `locations` |
|---|---|---|---|---|---|
| `activities(offset: -1)` | **2406 B** | 9 | 6 | **0 B** | 保留 ✅ |
| `activities(limit: 0)` | **2430 B** | 9 | 6 | **0 B** | 保留 ✅ |
| `activities(limit: 1001)` | **2439 B** | 9 | 6 | **0 B** | 保留 ✅ |
| `track(points: 0)` | **2522 B** | 9 | 6 | **0 B** | 保留 ✅ |

洩露內容實見：`/home/user/.../.venv/lib/python3.11/site-packages/graphql/execution/execute.py`、`strawberry/schema/schema_converter.py`、`.../src/api/schema.py` —— repository layout + interpreter site-packages，正如 CUI-0029 所講。**四條路全關。**

### 冇順手關錯（逐條實測，全部係修前=修後）

| 必須保留 | 修後實測 | 結果 |
|---|---|---|
| Resolver fault | **2335 B / 9 frames / 6 絕對路徑 / `Traceback` 在**，ERROR | ✅ 原封不動 |
| `Int` coercion（`2.5`） | 167 B、**0 frame**（raise 喺任何 resolver 之前，本來就冇 `original_error`），ERROR | ✅ 未被靜音，且證實「本來就零 frame」嘅講法屬實 |
| `Int` coercion（`2147483648`） | 195 B、0 frame、ERROR | ✅ |
| Unseeded budget `RuntimeError` | `test_an_unseeded_budget_still_logs_its_traceback` 釘死 ERROR + `exc_info` + `issubclass(RuntimeError)` | ✅ |

### Mutation（重做，全部 in-process `setattr` + active-oracle）

| Mutant | 結果 | 殺手 |
|---|---|---|
| **M1 classifier widen → `GraphQLError`**（安全面最關鍵） | ☠️ **8 failed** | 見上表；`Int` coercion **唔會靜晒**，被 4 個 test function 圍殺 |
| M2 `_page`/`_track_points` 還原 bare `ValueError` | ☠️ **12 failed** | `bounds_refusal_is_not_logged_as_a_server_fault` ×6 + `refusal_writes_nothing_where_no_logging_is_configured` ×6 |

**評分 A**：正確性 25/25、安全性 20/20 → 呢個 fix 冇 finding。

---

## Section B — CUI-0038（`< ERROR` 收緊做 `< WARNING` + configureless 行為測試）

**Commit**：`856a821`

`_unconfigured_logging()` 把 `strawberry.execution` / `strawberry` / root 三層 handler 剝走、root 留返 `WARNING` 自身 default，再用 `capfd` 量 fd 2。做法係啱嘅（唔用 `caplog`，因為「冇 handler」正正係要模擬嘅條件），而且配埋 `test_a_fault_still_writes_where_no_logging_is_configured` 做 control，避免「剝到乜都唔出」嘅假綠。

`assert logging.lastResort.level == logging.WARNING` 呢條 meta-assertion 好——把「WARNING 係門檻」嘅前提讀返出 `logging` 而唔係喺度重述。

**「432 tests stayed green」核實**：`b73510e` 實際 collect **432**；`856a821` 係 441。數字準確（指嘅係加新 assertion 之前嗰個 parent）。

### Mutation（四個 level）

| Mutant | 結果 |
|---|---|
| `REFUSAL_LOG_LEVEL = WARNING` | ☠️ **10 failed** |
| `= ERROR` | ☠️ **11 failed** |
| `= CRITICAL` | ☠️ **10 failed** |
| **`= DEBUG`** | ✅ **197 passed — 生還** |

→ 見 **W-030**。

---

## Section C — CUI-0039（refusal 同 fault 同一 operation）

**Commit**：`93da9d6`

`_mixed_operation()` 用 **points budget** 而唔係 bounds check 嚟製造 refusal，係經過思考嘅：budget 係「行到一半先拒」，所以 operation 真係同時揹住兩種 error 而唔係第一個 field 就死。field 數由 `MAX_TRACK_POINTS_PER_REQUEST // MAX_TRACK_POINTS + 1` 推導而唔係寫死，好。

**M4（unfiltered `super()`）**：☠️ **17 failed**，包括 `test_a_refusal_and_a_fault_in_one_operation_are_logged_apart`。而且我獨立確認咗佢個關鍵理由：多出嗰條 ERROR record **`exc_info=NONE`**，所以「數 record」係唯一捉到嘅寫法。呢條測試補得非常準。

---

## Section D — CUI-0040（`service.tracks()` 負數改 raise）

**Commit**：`c87f518`

修前行為（真‧修前 module）：

```
MAX_TRACK_POINTS = 1000 ；v0 存 1250 row
PRE-FIX points=-1     -> 1 row      PRE-FIX points=1         -> 1 row
PRE-FIX points=-5     -> 1 row      PRE-FIX points=0 (falsy) -> 1000 rows
PRE-FIX points=-1000  -> 1 row
HEAD    points=-1     -> ValueError: points must not be negative, got -1
HEAD    points=0/None/False -> 1000 rows
```

`False` 落 falsy 桶而唔係 negative 桶，測試有覆蓋（`test_a_falsy_points_still_means_the_ceiling_rather_than_a_refusal`），呢個 edge 揀得好。

兩層唔同措辭（service 講 "must not be negative"、schema 講 "between 1 and 1000"）係刻意而且正確——`0`/`None` 喺 service 層合法，schema 層唔合法，所以唔可以講同一句。`test_a_negative_points_never_reaches_the_service_from_a_client` 用 `assert "must not be negative" not in message` 釘死呢條界，係漂亮嘅負向 assertion。

**M5（clamp 唔 raise）**：☠️ **3 failed**。

**決定裁決**：我**同意 raise**。但推理嘅表述有問題 → **S-083**。

---

## Section E — CUI-0005（`call_on_close` → `g` + `teardown_request`）

**Commit**：`2d0742c`

### Production 行為不變（真 WSGI server + 真 HTTP，唔係 test client）

| | 修前（`call_on_close`） | 修後（`teardown_request`） |
|---|---|---|
| 40 request：opened / closed | 40 / 40 | 40 / 40 |
| 回傳 row / distinct | 40 / 40 | 40 / 40 |
| `/api/health` 有冇開 session | 冇 | 冇 |

**production 一致，零回歸。**

### 「view raise 咗一樣會 close」——呢句 comment 冇 assertion，我補驗

```
explode=False mutant=False  status=200  opened=1 closed=1
explode=True  mutant=False  status=500  opened=1 closed=1   ← HEAD：掟咗都照收
explode=False mutant=True   status=200  opened=1 closed=0
explode=True  mutant=True   status=500  opened=1 closed=0
```

comment 講嘅嘢屬實。

### GC 歸因（決定性實驗，見上方 #3）

`REQUESTS_PAST_THE_POOL = 400` 個 margin 足夠：本機修前喺 request **39** 就死，票係 134、lane 係 100，全部 ≪ 400。而且呢條測試只跑 **1.32s**（`--durations` 實測），唔係負擔——我本來想開嘅「長跑測試太慢」suggestion 量完之後撤回。

**M6（還原 `call_on_close`）**：☠️ **2 failed**（兩條 CUI-0005 測試）。

---

## 其他 lane 有冇被撞爛

```
git log --format='%h %p' -1 06ad4bd  →  06ad4bd 7f25812 aed2bd2
git diff aed2bd2..06ad4bd  → 只有另外三條 lane 嘅嘢（含 tests/test_api.py +105 行 CUI-0035）
git diff 7f25812..06ad4bd  → 只有本 lane 嘅嘢（tests/test_api.py +386 行）
```

**兩邊 parent → merge 嘅 diff 剛好互補，零交叉污染、零 hunk 掉失。** CUI-0035 嘅 Vercel entry 測試完整留喺 `tests/test_api.py` 尾。`.proj-docs/tickets.md` 同五張 ticket 檔案狀態都更新齊，CUI-0005 仲加咗 status log 行。

---

## 問題清單

### 🔴 Critical — 無

### 🟡 Warning

#### W-030 — `REFUSAL_LOG_LEVEL` 嘅斷言係單邊，`logging.DEBUG` mutant 喺 492 條測試面前生還

**位置**：`tests/test_api.py` `test_a_budget_refusal_is_not_logged_as_a_server_fault`（`assert REFUSAL_LOG_LEVEL < logging.WARNING`）；受影響契約喺 `src/api/schema.py` `REFUSAL_LOG_LEVEL` docstring

**描述**：CUI-0038 把上界由 `< ERROR` 收到 `< WARNING`，正確。但**下界完全冇人守**。實測 `REFUSAL_LOG_LEVEL = logging.DEBUG` → `197 passed`，全 suite **零條變紅**。

**影響**：`REFUSAL_LOG_LEVEL` 個 docstring 自己寫明嘅承諾係：

> "An operator who wants to watch refusals, whether to size the budgets or to spot a client hammering them, **opts in by lowering the level and gets every one of them**."

喺 DEBUG 之下，一個 operator 照文件把 level 降到 `INFO` 去睇 refusal 會**一條都見唔到**。呢個唔係 equivalent mutant——佢違反咗模組自己寫低嘅契約，只係啱啱好唔違反安全性質（DEBUG 一樣係 `< WARNING`，所以 configureless 之下仍然靜音，`security` 維度我冇扣分）。

諷刺嘅係：CUI-0038 成張票嘅主旨就係「一條單邊斷言放生咗一個 mutant」，而個修正**留低咗對面嗰邊**。同時呢條亦推翻 lane 自報嘅「CUI-0038 四個 level 全殺」——WARNING / ERROR / CRITICAL 三個死，DEBUG 生還。

**方案 A（推薦）**：加一條下界斷言，同現有上界並排：
```python
assert logging.INFO <= REFUSAL_LOG_LEVEL < logging.WARNING, (
    "a refusal below INFO is invisible to the operator the docstring tells to lower the level"
)
```
- 優點：直接把 docstring 嗰句承諾變成 gate，兩邊夾死，改動最細，讀落同上面嗰條對稱
- 缺點：仍然容許 `INFO` 同 `WARNING` 之間嘅自訂 level（實務上唔存在）

**方案 B**：改成行為測試，唔測常數——加一條 test：把 `strawberry.execution` 設到 `INFO` 並掛一個 handler，assert refusal 真係收到 record。
- 優點：測嘅係 operator 真正做嘅動作，唔會隨常數改寫而失效；同 CUI-0038 自己「測性質唔測常數」嘅精神一致
- 缺點：多一條測試同多一個 fixture；`_unconfigured_logging` 之外再多一套 logging 操弄，維護面大啲

**推薦：方案 A + 方案 B 都做**。A 係一行、即刻堵窿；B 先係 CUI-0038 自己立嘅標準（「the property itself, rather than an inequality that restates the constant」），而呢個 warning 恰恰係「淨係有 inequality」造成嘅。若只肯做一個，做 A。

---

### 🟢 Suggestion

#### S-083 — `service.tracks` docstring「it would have to clamp *upward*」係過度宣稱

**位置**：`src/api/service.py` `tracks()` docstring

**描述**：原文——

> "Clamping is the smaller change and was the ticket's own first suggestion, but at this layer **it would have to clamp *upward*, to MAX_TRACK_POINTS**, because that is already what falsy means here."

「would have to」唔成立：向下 clamp 入合法區間（`sample = min(max(points, 1), MAX_TRACK_POINTS)`）同樣係一個 clamp，而且更加係 clamp 呢個字嘅慣常讀法。

不過**佢個結論仍然啱**，只係理由寫錯咗一層。真正嘅殺著係我實測出嚟嗰樣：修前 `-1` / `-5` / `-1000` **已經**各自回 1 row，同 `points=1` 完全冇分別——即係**向下 clamp 到 1 = 原封不動保留 CUI-0040 報嘅 bug**。所以能夠改變現狀嘅 clamp 只剩向上嗰個，而向上嗰個荒謬。

**影響**：純文件。但本 repo 反覆出現嘅缺陷類別就係「代碼啱、解釋佢點解啱嗰句寫錯」，而呢句係 CUI-0040 唯一嘅決策記錄。

**方案 A（推薦）**：改寫成實測支撐嘅版本：
```
Clamping is the smaller change and was the ticket's own first suggestion, but
neither direction survives. Clamping *down* into the legal range lands on 1 --
which is exactly what a negative already returned: measured against a 1250-row
track, -1, -5 and -1000 each came back as one row, the same row `points=1`
gives. So the only clamp that changes anything is *upward*, to
MAX_TRACK_POINTS, because that is already what falsy means here -- and that
hands the largest answer this function can give to the most obviously broken
question.
```
- 優點：由「斷言唯一選項」變成「窮舉兩個選項再各自否決」，論證強咗而且可複核；順手把已經量到嘅 1250-row 數字擺入去支撐
- 缺點：長多三四行

**方案 B**：只刪「it would have to」改成「the only clamp that would change today's behaviour is upward」。
- 優點：一行改完，保留原有節奏
- 缺點：讀者仍然唔知「向下 clamp 會點」，要自己推

**推薦：方案 A**。呢段本來就係成個 repo 最重視嘅「決策記錄」，值得寫足。

---

#### S-084 — coercion 測試個 comment 列嘅「同時被殺嘅測試」列表，喺本 lane 之內已經過期

**位置**：`tests/test_api.py` `test_an_int_coercion_failure_is_not_reclassified_as_a_client_refusal` 嘅 comment

**描述**：原文講 widen 做 `GraphQLError` 「also reds `test_an_unseeded_budget_still_logs_its_traceback` and `test_an_unexpected_resolver_error_is_still_logged_as_a_server_fault`」，語氣讀落係窮舉。實測係 **4 個 test function**，仲有 `test_a_fault_still_writes_where_no_logging_is_configured`（CUI-0038 加）同 `test_a_refusal_and_a_fault_in_one_operation_are_logged_apart`（CUI-0039 加）——**兩條都係本 lane 後面兩個 commit 自己加嘅**，即係呢句喺同一條 lane 入面就已經 stale。

**影響**：低。但呢個 comment 存在嘅唯一目的就係更正一個過度宣稱（lane 自己 amend 過），而更正完仍然數少咗。

**方案 A（推薦）**：改成「at least four tests red on this mutant, including …」，唔再扮窮舉。
- 優點：日後再加測試都唔會 stale
- 缺點：具體性少咗少少

**方案 B**：補齊四個名。
- 優點：最具體，讀者即刻知去邊度睇
- 缺點：下次再加一條同類測試又會 stale——即係重蹈同一個覆轍

**推薦：方案 A**。呢個 comment 本身就係「窮舉式宣稱會過期」嘅受害者，唔應該再用窮舉。

---

#### S-085 — CUI-0037 ticket 標題個「平 140 倍」冇任何量度支撐，而且同修正後嘅 token 數對唔上

**位置**：`.tickets/pending/0001-0200/CUI-0037.md` 標題行

**描述**：標題寫「觸發成本比 CUI-0029 量到嗰個平 **140 倍**」。實測兩個 document：bounds **11 token / 33 byte** vs budget **57 token / 168 byte** → 比例 **≈ 5.2×**（token 同 byte 各自計都係 5.1–5.2）。140 應該係由票上錯嘅「7 token」同 CUI-0029 個 log 輸出量（998 token）夾出嚟嘅，屬於兩個唔同單位相除。

lane **做啱咗一半**：`ClientRefusalError` docstring 只寫 57 同 11，冇轉載個 140 倍。但票個標題冇改，而票已經標 `✅ done`。

**方案 A（推薦）**：改票標題做「平 ~5 倍」，喺 ticket body 加一行講明 140 係點嚟同點解唔啱。
- 優點：ticket registry 係 SSoT，留一個查得返嘅更正紀錄
- 缺點：要郁一張已標 done 嘅票

**方案 B**：標題刪走個倍數，淨係留「觸發成本遠平過 CUI-0029」。
- 優點：唔使再維護一個數
- 缺點：失去「平幾多」呢個本來幾有用嘅量級資訊

**推薦：方案 A**。

---

#### S-086 — `tracks()` docstring 有約 20 行散文排喺 `Raises:` section 之後

**位置**：`src/api/service.py` `tracks()` docstring

**描述**：Google convention（本 repo `ruff D` 開住、pydocstyle Google）嘅慣例係 docstring 以 section 作結。而家 `Raises:` 之後仲有一大段 body-indent 散文（「Refused rather than clamped…」起）。ruff 過到，唔係 gate 問題，但 Sphinx/Napoleon 渲染出嚟會變成 Raises 之後一段游離 body text，讀者掃 signature 嗰陣要行過成段論證先見到 `tracks()` 完。

**方案 A（推薦）**：把論證段搬去 `Args:` 之前（Summary 同 Args 之間），`Raises:` 留喺最尾。
- 優點：完全符合 Google convention；論證同 summary 貼近，讀者一路讀落去
- 缺點：docstring 頭段變長，`Args:` 推後

**方案 B**：把論證搬出 docstring，變成 function 上面嘅 `#` block comment（本 repo `tracks()` body 入面已經有 CUI-0033(a) 嗰段 block comment 嘅先例）。
- 優點：同 module 內既有做法一致；docstring 回復簡潔、純 API 契約
- 缺點：`help()` / IDE hover 見唔到呢段決策理由

**推薦：方案 B**。同一個 function body 入面已經有 CUI-0033(a) 嘅 block comment 做先例，兩段決策理由放埋一齊更順。

---

## ✅ 做得好嘅地方（跨 fix）

1. **安全修正係真嘅，唔係聲稱嘅**。我用真 entry / 真 server / 真 DB / OS 層 fd 2 獨立量，4 條路 2406–2522 byte → 0 byte，9 frame → 0，6 個絕對路徑 → 0。同時三條應該保留嘅路（resolver fault 2335B+9frame、coercion 167/195B、unseeded `RuntimeError`）**逐條實測未被誤殺**。呢個係「narrow classifier」做啱咗嘅教科書例子。
2. **主動預判最危險嘅 widening，仲寫低咗**。`RefusalAwareSchema` docstring 明文指出「matching on `GraphQLError` itself, which every error reaching here already is」係個誘惑，並專門開一條測試釘住。實測 M1 被 8 個 test id 圍殺。
3. **CUI-0039 嗰個 `exc_info` 發現係真本事**。「多出嗰條 ERROR record 冇 exc_info，所以要數 record 唔可以驗 traceback」——我獨立重現到（2 條 ERROR，1 條 `exc_info=NONE`）。呢類洞察好易走漏。
4. **CUI-0005 揀咗攻擊自己嘅 measurement**。fixture 特登揸住 session reference「把 GC 剔出量度」，再拒絕把 request 數當 gate。我用 `gc.disable()` 三路對照證實佢個歸因唔單止啱，仲係循環 GC（唔係 refcount）——即係佢揸 reference 呢個設計係必要而唔係保險。
5. **拒絕把唔可移植嘅數字寫成 assertion**。`test_a_bounds_refusal_is_not_logged_as_a_server_fault` 明寫「byte count is deliberately not asserted」，改為 assert `exc_info` 同 level。我實測 9 frame × 42 char = 378 證實佢個判斷完全正確。
6. **`assert logging.lastResort.level == logging.WARNING` 呢條 meta-assertion**。把「WARNING 係 configureless 門檻」呢個前提讀返出 `logging` 本身，CPython 改咗就會響——而唔係喺 comment 度重述一次然後靜靜過期。
7. **configureless 測試配埋 control**。`test_a_fault_still_writes_where_no_logging_is_configured` 防止「剝到乜都唔出」嘅假綠，係做 logging 測試最易漏嘅一步。
8. **數字可核**：「432 tests stayed green」實測 `b73510e` = 432，逐個對上；「103 / 341 bytes」逐 byte 對上；「11 token」二分同手數都係 11。
9. **Merge 乾淨**：兩邊 parent → merge 嘅 diff 剛好互補，另外三條 lane 一行冇跌。
10. **Ticket bookkeeping 完整**：五張票狀態、完成日期、status log 全齊，仲坦白標明「檔案未搬 `pending/`」而唔係扮搬咗。

---

## 修正優先順序

| 次序 | ID | 嚴重度 | 內容 | 建議 commit |
|---|---|---|---|---|
| 1 | W-030 | 🟡 | `REFUSAL_LOG_LEVEL` 補下界斷言（+ operator-opt-in 行為測試） | `fix: W-030 \| bound REFUSAL_LOG_LEVEL from below as well` |
| 2 | S-083 | 🟢 | `tracks()` clamp 論證改寫成窮舉兩個方向 | `fix: S-083 \| argue both clamp directions instead of asserting one` |
| 3 | S-084 | 🟢 | coercion comment 唔再扮窮舉 | `fix: S-084 \| stop enumerating the tests a widening reds` |
| 4 | S-085 | 🟢 | CUI-0037 票標題 140× → ~5× | `fix: S-085 \| correct the cost ratio in the CUI-0037 title` |
| 5 | S-086 | 🟢 | `tracks()` 論證段搬出 docstring | `fix: S-086 \| move the clamp rationale out of the Raises section` |

> 五項全部係 non-blocking。`status=pass`，可以照 Protocol 1 行落 QA；以上建議由 developer 另開 commit（一 review item 一 commit）處理。

---

## 修訂後代碼

只列有實質改動嘅 block（`schema.py` 1400+ 行、`test_api.py` 3100+ 行，全文轉載無助審閱）。

### 1) W-030 — `tests/test_api.py`

```python
    # Bounded against WARNING rather than ERROR (CUI-0038). The property
    # CUI-0029 sold is not "quieter than a fault", it is "emitted by nothing at
    # all under the configuration Vercel runs", and WARNING is the threshold
    # that decides that -- so `< logging.ERROR` left the WARNING mutant alive.
    #
    # Bounded from *below* as well (W-030). `< logging.WARNING` alone left
    # `logging.DEBUG` alive: measured on this branch, REFUSAL_LOG_LEVEL =
    # logging.DEBUG passed all 492 tests. DEBUG keeps the silence, so the
    # security property survives -- what it breaks is the affordance this
    # constant's own docstring promises, that an operator "opts in by lowering
    # the level and gets every one of them". An operator lowering to INFO would
    # get none. One-sided assertions are the whole subject of CUI-0038; this is
    # the side it left open.
    assert logging.INFO <= REFUSAL_LOG_LEVEL < logging.WARNING, (
        "a refusal at WARNING or above is emitted by a deployment that configures "
        "nothing; one below INFO is invisible to the operator who opts in"
    )
    # The premise of the line above, asserted rather than assumed: WARNING is
    # the threshold only because `logging.lastResort` -- the handler a process
    # that has configured nothing falls back to -- carries that level. If
    # CPython ever moves it, the reasoning moves with it and this says so
    # rather than going quietly wrong.
    assert logging.lastResort.level == logging.WARNING, (
        "the configureless threshold is read off `logging` rather than restated here"
    )
```

配套行為測試（方案 B，建議一併加喺 CUI-0038 嗰個 section 尾）：

```python
def test_an_operator_who_lowers_the_level_gets_every_refusal(year_client, monkeypatch):
    # The affordance REFUSAL_LOG_LEVEL's docstring promises, asserted rather
    # than described (W-030). The silence test above says a refusal reaches
    # nothing by default; this says it is silenced rather than discarded, which
    # is the other half of the same sentence -- and it is what `logging.DEBUG`
    # would break while leaving every existing assertion green.
    _unconfigured_logging(monkeypatch)
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logger = logging.getLogger("strawberry.execution")
    monkeypatch.setattr(logger, "handlers", [handler])
    monkeypatch.setattr(logger, "level", logging.INFO)  # the operator's own opt-in

    for document in CONFIGURELESS_REFUSALS:
        assert gql_errors(year_client, document.values[0])

    assert len(records) == len(CONFIGURELESS_REFUSALS), (
        "an operator who lowered the level to INFO was handed fewer refusals than "
        "were earned"
    )
```

### 2) S-083 + S-086 — `src/api/service.py`

`tracks()` docstring 收窄返純契約（`Raises:` 作結）：

```python
    Raises:
        ValueError: If *points* is negative. Refused rather than clamped; the
            reasoning is in the block comment on the guard below.
    """
    # CUI-0040, and the half of it that was a choice rather than a bug.
    # `min(points, MAX_TRACK_POINTS)` passed a negative straight through, and
    # `_even_positions` turns anything under 2 into the last row alone:
    # measured against a 1250-row track, -1, -5 and -1000 each came back as
    # exactly one row, which is also what `points=1` returns.
    #
    # Clamping was the ticket's own first suggestion, but neither direction
    # survives. Clamping *down* into the legal range lands on 1 -- which is
    # precisely what a negative already returned, so it would preserve the bug
    # it was meant to fix while looking deliberate. That leaves clamping *up*,
    # to MAX_TRACK_POINTS, because that is already what falsy means here: it
    # hands the largest answer this function can give to the most obviously
    # broken question, and it erases the distinction CUI-0025 was argued over.
    # `0` means "I did not ask", while `-5` means "I asked for something that
    # cannot exist". Those deserve different answers, and only one of them can
    # be silent.
    #
    # The wording is deliberately not `run365days.api.schema._track_points`'s
    # sentence, near as it is. That one says "between 1 and MAX_TRACK_POINTS",
    # which would be false here, where `None` and `0` are both legal. The two
    # layers refuse different sets, so they say different things -- and no
    # client ever reads this one: `_track_points` and `checkPoints` in
    # `frontend/src/data/static/source.ts` both refuse a negative first, in the
    # single sentence CUI-0025 bought for both deployment modes.
    if points is not None and points < 0:
        raise ValueError(f"points must not be negative, got {points}")
```

`Args:` 嗰行同時收短返：

```python
        points: Samples per track, or ``None``/``0`` for every stored row up to
            :data:`MAX_TRACK_POINTS`. A negative count is refused, not clamped;
            see Raises.
```
（呢行原本已經啱，保持不變。）

### 3) S-084 — `tests/test_api.py`

```python
    # Measured rather than assumed, because the tempting claim is wrong: that
    # mutant does *not* slip past the existing suite. Widening the classifier
    # to `GraphQLError` reds at least four test functions -- among them
    # `test_an_unseeded_budget_still_logs_its_traceback` and
    # `test_an_unexpected_resolver_error_is_still_logged_as_a_server_fault`,
    # since graphql-core wraps both of those in a `GraphQLError` on the way out
    # -- and the count grows with every fault test added, so it is left
    # unenumerated on purpose. What this adds is not the only guard but the
    # named one: CUI-0037 decided in writing that a coercion failure stays a
    # fault, and a decision held only by a test about something else is a
    # decision that moves the first time that test is rewritten.
```

### 4) S-085 — `.tickets/pending/0001-0200/CUI-0037.md`

```markdown
# CUI-0037 `_page` / `_track_points` 嘅 bounds refusal 仍然每 request 寫 9 個絕對路徑 frame，觸發成本比 CUI-0029 量到嗰個平約 5 倍

...

## 更正（2026-09-17，Code Review S-085）

原標題寫「平 140 倍」，冇量度支撐。實測（graphql-core parser budget，即
`MaxTokensLimiter` 用嗰個計法）：

| Document | tokens | bytes |
|---|---|---|
| `{ activities(offset: -1) { id } }` | 11 | 33 |
| CUI-0029 嗰個 list-rows budget document | 57 | 168 |

⇒ 比例 ≈ **5.2×**（token 同 byte 各自計都係 5.1–5.2）。
原本個 140 係由票上錯咗嘅「7 token」同 CUI-0029 個 log **輸出**量（998 token）
相除得出，兩個唔同單位，唔成立。
```

---

```handoff-receipt
protocol: 1
status: pass
score: 92/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "96%"
next_action: merge_develop
next_agent: quality-assurance
branch: "claude/ai-dev-team-start-05jie2"
context: "CUI-0037/0038/0039/0040/0005 batch pass 92/100, 0 Critical. Five lane commits b73510e..aed2bd2 are already merged into 06ad4bd; no further git merge is needed for this lane. Security net independently re-measured through the real api/graphql.py entry, a real WSGI server and OS-level fd 2 capture: four bounds-refusal paths went from ~2406-2522 bytes / 9 frames / 6 absolute paths each to 0 bytes, while resolver faults (2335 B, 9 frames), Int coercion (167/195 B, 0 frames) and the unseeded-budget RuntimeError all kept ERROR and their tracebacks. Mutation redone in-process with active-mutant oracles: widen-to-GraphQLError killed by 8 test ids, bounds-to-ValueError by 12, unfiltered super() by 17, clamp-not-raise by 3, call_on_close by 2. One live mutant found that the lane did not report (W-030): REFUSAL_LOG_LEVEL = logging.DEBUG passes all 492 tests because the CUI-0038 assertion has no lower bound, which breaks the operator opt-in the constant's own docstring promises. Four suggestions S-083..S-086 are documentation-accuracy items. All five are non-blocking; QA may proceed."
blockers:
  - "W-030 (non-blocking): tests/test_api.py asserts REFUSAL_LOG_LEVEL < logging.WARNING with no lower bound; logging.DEBUG survives the full 492-test suite while breaking the documented operator opt-in. Fix: assert logging.INFO <= REFUSAL_LOG_LEVEL < logging.WARNING, plus a behaviour test that an operator lowering the level to INFO receives every refusal."
  - "S-083 (non-blocking): src/api/service.py tracks() docstring claims a clamp 'would have to clamp upward'; a downward clamp to 1 is equally available and is exactly what a negative already returned pre-fix (-1/-5/-1000 each gave 1 row, same as points=1). Rewrite to eliminate both directions rather than assert one."
  - "S-084 (non-blocking): the comment in test_an_int_coercion_failure_is_not_reclassified_as_a_client_refusal enumerates two tests the widen mutant reds; measured, it reds four test functions, two of which this same lane added later. Stop enumerating."
  - "S-085 (non-blocking): .tickets/pending/0001-0200/CUI-0037.md title still claims the trigger cost is '140x cheaper'; measured ratio is ~5.2x (11 vs 57 tokens, 33 vs 168 bytes). The 140 came from dividing the corrected-away '7 tokens' into CUI-0029's 998-token log output."
  - "S-086 (non-blocking): src/api/service.py tracks() places ~20 lines of prose after the Raises: section, against the Google docstring convention this repo enforces via ruff D. Move it into the block comment beside the guard."
```