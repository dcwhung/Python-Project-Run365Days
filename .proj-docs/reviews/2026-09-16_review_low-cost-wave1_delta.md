# Delta Re-Review — 2026-09-16 — low-cost-wave1

| 項目 | 內容 |
|---|---|
| 審閱者 | code-reviewer（獨立，未參與呢批代碼撰寫） |
| 類型 | Delta re-review（只審修正，重新評分整批 5 張票） |
| 上一輪 | `.proj-docs/reviews/2026-09-15_review_low-cost-wave1_batch.md`（86/100 — warn） |
| Delta base | `dadb415` |
| Delta commit | `fc57b24` W-022+S-041 / `3920fba` W-023 / `a3cfeee` S-039 / `344ce03` S-040 |
| 涵蓋票 | CUI-0017、CUI-0020、CUI-0003、CUI-0006、CUI-0014 |
| Branch | `claude/ai-dev-team-start-05jie2` |

## 總評

**5 個 finding 全部真正修好，冇一個係表面遮蓋。** 我逐個獨立重做咗 developer 嘅驗證聲稱，
唔係讀 diff 就算——四個聲稱**全部數字級對得上**，連 601 vs 208、393 條洩漏 statement
呢啲精確數字都同我自己量到嘅一模一樣。

Delta 零 production code 行為改動（41 個 `src/**/*.py` 全部 AST 比對），測試數目維持 363，
hard gate 全綠。

發現 **2 個新 🟢 Suggestion**，其中 S-042 值得留意：**S-039 修好嘅嗰個不變式，
喺同一個檔案入面仲有 12 個 test case 繼續靠緊佢**，而且嗰邊用嘅係 exact `==`，
比 S-039 嗰個有 24 條 slack 嘅仲脆。呢個唔係 developer 做錯——S-039 個 brief 就係一個測試——
但係一個 senior 應該指出嘅系統性觀察。

**分數由 86 → 97**，`status=pass`。

## Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| Tests | `.venv/bin/python -m pytest tests -q` | ✅ pass — **363 passed** in 27.09s |
| Lint | `.venv/bin/ruff check src tests` | ✅ pass — All checks passed |
| Format | `.venv/bin/ruff format --check src tests` | ✅ pass — 58 files already formatted |
| SDL sync | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ pass — up to date |
| Type check | 本 repo 無 mypy / tsc gate（ruff 承擔） | n/a |
| Coverage | 改動路徑全部有測試覆蓋 | ✅ pass |
| No Critical | 🔴 Critical = 0 | ✅ pass |
| Security scan | 本 delta 零新增依賴 | n/a |

**全部 hard gate 通過**（probe 全部還原之後再跑一次，仍然 363 passed）。

## 評分結果

| 維度 | 得分 | 滿分 | 相對上一輪 | 備注 |
|------|------|------|---|------|
| 正確性 | 24 | 25 | — | S-038 維持記錄（已判定唔修，本輪唔翻案） |
| 安全性 | 20 | 20 | — | 無新增依賴、無敏感資料 |
| 可維護性 | **20** | 20 | **+6** | W-022 ✅、S-041 ✅ 全部解決 |
| 測試覆蓋 | **13** | 15 | **+5** | W-023 ✅、S-039 ✅、S-040 ✅；新增 S-042、S-043 各 -1 |
| 性能 | 10 | 10 | — | 無回歸 |
| 代碼風格 | 10 | 10 | — | ruff 全綠 |
| **總分** | **97** | **100** | **+11** | |

**結果：✅ pass**

### 分數點解由 86 郁到 97

```
86  上一輪
+5  W-022 解決（可維護性 -5 撤回）
+1  S-041 解決（可維護性 -1 撤回）
+5  W-023 解決（測試覆蓋 -5 撤回）
+1  S-039 解決（測試覆蓋 -1 撤回）
+1  S-040 解決（測試覆蓋 -1 撤回）
-1  S-042 新增（測試覆蓋）
-1  S-043 新增（測試覆蓋）
---
97
```

S-038 嘅 -1 保留：上一輪已判定「建議唔修、純記錄」，本輪依指示唔翻案，所以佢仍然係一條
open suggestion，扣分維持。

---

## 逐個 Finding 驗證

### W-022 — dangling `the 16x above` ✅ 已修（完全消除）

**Commit**：`fc57b24` | **位置**：`src/api/schema.py:120-121`

改後寫法：

```
~0.01 s on the fixture but ~0.2 s on the export (10.4 ms and 165.6 ms, a
16x gap), and the only reading here that moves with the export's size at
all, ...
```

**獨立驗證**：

1. **`above` 消失咗**——`grep -n "16x" src/api/schema.py` 得返兩個命中：
   line 59（`MAX_TRACK_POINTS_PER_REQUEST` 嘅餘裕比）同 line 121（本句）。
   兩者再冇任何文字連繫，上一輪講嗰個「向上搵會搵到一個同值異義 16x」嘅誤讀路徑
   **已經斷咗**。
2. **新寫法自足**：`165.6 / 10.4 = 15.92`，寫 `a 16x gap` 準確，而且個比率
   同佢兩個來源數字喺**同一括號入面**，唔再依賴任何其他段落存在。呢個係方案 A 嘅
   根治做法（數字寫喺用佢嗰句），唔係方案 B 嘅補鑊。
3. **成篇 docstring 掃過有冇其他同類 dangling**——我逐個檢查
   `MAX_TRACK_FIELDS_PER_REQUEST` docstring 入面每個內部交叉引用：

| 引用 | 指向 | 判斷 |
|---|---|---|
| `the wall-clock readings below` | 下面 bullet 群 | ✅ 有效 |
| `every figure below names the scale` | 同上 | ✅ 有效 |
| `bounded there by the served case beside it` | 緊接上一句嗰個 served case | ✅ 有效 |
| `the 52/53 boundary by the tests beside it` | `tests/test_api.py` 鄰接測試 | ✅ 有效 |
| `Every token and statement count above` | 上面兩個 bullet | ✅ 有效 |
| `the worst case above is unshared batches` | 52-different-activities 嗰段 | ✅ 有效 |
| `not the sixty an earlier revision inferred` | 歷史敘述，非交叉引用 | ✅ 不適用 |

**零剩餘 dangling reference。** 呢個 finding 修得乾淨。

---

### S-041 — `223x` vs QA 報告 `224×` ✅ 已修（處理方式恰當，唔需要另開 lane）

**Commit**：`fc57b24` | **位置**：`src/api/schema.py:104-106`

Developer **冇改 QA 報告**，改為喺 `schema.py` 明文寫出兩個數字同佢哋嘅關係：

```
The real export holds 134,041 track rows, 223x what this fixture holds
-- 134,041 / 600 is 223.4, rounded down here and up to 224x in QA's
2026-09-15 batch report -- and since AU-050 ...
```

**獨立驗證**：`134041 / 600 = 223.4017`。截尾 223、四捨五入 224，兩個 spelling 都講得通，
`schema.py` 寫嘅除式同兩個取整方向**完全正確**。QA 報告 line 404 / 571 確認係 `224×` / `224 倍`。

**我對「夠唔夠」嘅判斷：夠，而且比改 QA 報告好。** 三個理由：

1. **引用方向係啱嘅一邊。** `schema.py` 係**活文件**——任何人要郁呢個 constant 都會讀佢；
   QA 報告係一份**有日期嘅定點紀錄**（2026-09-15）。改一份已發佈嘅 QA 報告去遷就後來嘅
   代碼改動，係**篡改當日量度紀錄**，比留低一個 0.45% 嘅取整差更加壞。Developer 講
   「The QA report is outside this lane, so it is quoted, not edited」——呢個唔單止係
   lane 紀律，本身就係啱嘅文件倫理。
2. **誤讀風險喺最重要嗰邊已經消除。** S-041 原本嘅風險係「讀者見到兩個數字，
   以為有一份寫錯」。而家由 QA 報告嘅 `224×` 行去 `schema.py` 嘅讀者，
   即刻見到和解說明。反方向（只讀 QA 報告、永遠唔開 `schema.py`）嘅讀者，
   手上得一個數字，根本唔會察覺有衝突——**冇兩個數字就冇矛盾**。
3. **殘餘風險唔會造成錯誤決策。** 223 同 224 差 0.45%，兩個數都唔係任何門檻嘅
   load-bearing 輸入（佢只係用嚟講「fixture 同 export 差兩個數量級」）。

**結論：唔需要另開 lane 改 QA 報告。** 上一輪嘅方案 A 寫成「統一成 223x」，
developer 交出嘅係「保持 223x + 明文和解」，我認為呢個係**比原方案好嘅執行**，
唔算 deviation。

---

### W-023 — 深度 guard 對 interface / union 盲點 ✅ 已修（盲點真實存在，已被堵住）

**Commit**：`3920fba` | **位置**：`tests/test_api.py:501-509`

我做咗**兩個獨立 probe**——一個驗證 assertion 會叫，一個驗證佢守嘅嘢真係存在。

#### Probe A — assertion 真係會紅，而且叫得啱

我冇改 repo 任何檔案：喺 scratchpad 寫咗個 pytest plugin，`monkeypatch` 住
`schema.as_str`，喺 SDL 尾注入 `union Anything = Meta | Activity`，再跑原測試：

```
E  AssertionError: the SDL grew an abstract type; _deepest_selection only walks
   object types and counts an interface or union as a leaf, so it now
   under-reports depth and this guard would stay green while MAX_QUERY_DEPTH
   quietly becomes able to fire. Teach _deepest_selection to walk an
   interface's fields and a union's possible types before trusting the number
   below
E  assert not ['Anything']
```

**係 `AssertionError`，唔係 collection error**（`CLAUDE.md` §6 嗰個陷阱），
而且 pytest assertion rewriting 自動補上 `assert not ['Anything']`——**failure 直接
點名邊個 type 出事**。Message 讀得明，而且明確講咗下一步（去教 `_deepest_selection`
點行 interface 同 union），符合呢個 codebase 已建立嘅 failure-message 水準。

#### Probe B — 盲點係真嘅，唔係假想（呢個先係關鍵）

一句 assertion 值唔值，睇佢守緊嘅嘢係咪真存在。我構造咗一個**由 Query 真正可達**、
經 union 再向下 nest 5 層嘅 SDL，直接量 `_deepest_selection`：

```
baseline reachable            : 4  (pinned DEEPEST_REACHABLE_DEPTH=4, MAX_QUERY_DEPTH=5)
with union nesting 5 levels   : 4   <- _deepest_selection 報嘅數
true depth of the deepest doc : 6   (probe > inner > inner > inner > inner > leaf)

舊測試會斷言 reachable == 4  ->  reachable 仍然係 4：測試照樣 GREEN
但 client 而家可以送深度 6 > MAX_QUERY_DEPTH=5：limiter 已經可以 fire，而冇人知

with interface nesting 4 levels: 4  (baseline 4) -> 同樣 GREEN，深度被隱藏
```

**上一輪嘅描述完全成立**：union 同 interface 兩種形狀都會令個 walk **靜默低估深度**，
`reachable` 唔郁，測試保持綠，而 `MAX_QUERY_DEPTH` 已經悄悄變成可以 fire——
正正係呢個測試存在嘅唯一理由。一個靜默失效嘅 guard 比冇 guard 更差，
因為佢派緊虛假安全感。**修得啱。**

#### 補充：呢個 guard 覆蓋完整嗎？

我特別檢查咗會唔會仲有第三種漏網形狀。GraphQL 嘅 composite output type **得三種**：
object、interface、union。`_unwrap` 已經剝走 `GraphQLList` / `GraphQLNonNull`，
enum 同 scalar 做 leaf 係正確嘅。所以**新 assertion 覆蓋晒 `_deepest_selection`
唔識行嘅全部形狀**，唔係補一半。

實測現狀：`type_map` 29 個 entry（21 個 object，其中 15 個 user-defined），
**abstract type = `[]`**——今日確實係潛伏而非已發生，commit message 講嘅
「29 types, 0 abstract」對數。

---

### S-039 — flood 測試前半冇重置 counter ✅ 已修（developer 嘅聲稱逐個數字核實通過）

**Commit**：`a3cfeee` | **位置**：`tests/test_api.py:1091-1096`

Developer 聲稱呢個唔止係對稱問題：參數次序一調轉，冇 reset 嘅前半會喺**完全正確嘅
request** 上面變紅（601 vs 208），因為 fixture setup 灌入 393 條 statement。

**我獨立重做（冇睇佢個 script，自己寫 probe）**：將
`(year_client, sql_count)` 調轉成 `(sql_count, year_client)`，令 listener 喺
fixture 建立之前就 attach，再喺前半釘死已知嘅 208：

```
PROBE 1 — 調轉次序，RESET 保留：
  PROBE first-half sql_count = 208
  1 passed

PROBE 2 — 調轉次序，RESET 移除：
  PROBE first-half sql_count = 601
  E  AssertionError: PROBE: first half read 601 statements, expected 208
  1 failed
```

**逐個數字對得上：**

| Developer 聲稱 | 我實測 | |
|---|---|---|
| 有 reset → 208 | 208 | ✅ |
| 冇 reset → 601 | 601 | ✅ |
| fixture setup 灌入 393 條 | 601 − 208 = **393** | ✅ |
| 遠超測試斷言嘅 ceiling | ceiling = 52×2 + 64×2 = **232**；601 ≫ 232 | ✅ |
| 係 AssertionError 唔係 collection error | 確認係 AssertionError | ✅ |

**聲稱成立，而且結論正確**：呢個唔係美觀問題。喺嗰個次序之下，
**一個完全正確嘅 request 會令測試變紅**，而 failure message（`601 <= 232` 超標）
會將人引去查一個根本唔存在嘅 statement 爆炸。修咗之後前後兩半量度方式一致，
而且個 comment 明文寫低咗原本冇人講過嘅 argument-order 不變式——**呢點特別好**，
佢將一個隱性依賴變成文字。

> ⚠️ 但呢個不變式喺**同一個檔案其他地方**仲有 12 個 test case 靠緊——見下面 **S-042**。

---

### S-040 — `isinstance` 有冇真正守住 annotation 契約 ✅ 已修（守得住）

**Commit**：`344ce03` | **位置**：`tests/test_weather_models.py:248, 263-266`

**獨立驗證**：我自己寫咗一個 stand-in（冇睇 developer 點寫）——`__eq__` 對 `""` 回 `True`、
唔係 `None`、但**唔係 `str`**，`monkeypatch` 落
`run365days.weather.models._MISSING_TEXT`（佢喺 call time 讀 module global，patch 得到）：

```
baseline（唔 patch）            : 2 passed
PROBE 'notstr'（等於 "" 但唔係 str）:
  assert record.description == ""              -> 通過（舊嘅 == 同 is not None 兩條都捉唔到）
  assert isinstance(record.description, str)   -> AssertionError ✅
  E  AssertionError: <NotAString: a non-str that equals ''>
  E  assert False
  E   +  where False = isinstance(<NotAString: ...>, str)
  2 failed（兩個測試都捉到）
PROBE 'none'（退化成 None）:
  assert record.description == ""  -> AssertionError: assert None == ''  ✅（仍然守住）
  2 failed
```

**三個結論：**

1. **新斷言真係守住嘢。** 佢捉到一個**舊寫法在數學上永遠捉唔到**嘅回歸
   （`is not None` 同 `None not in (...)` 喺 `== ""` 之後恆真）。
   由恆真變成有內容，係實質改善而唔係換句說話。
2. **舊有保護冇失去。** 普通 `None` 回歸仍然喺 `== ""` 嗰行先紅——developer 講
   「the probe used a value that gets past it」係準確描述，唔係推搪。
3. **係 AssertionError 唔係 collection error**，兩個測試都係。

**額外發現（正面）**：呢個 commit 順手將 `icon_url` 加入第二條檢查——原本佢有
`== ""` 但被漏出 `None` tuple 之外。**呢個係 finding 範圍以外嘅真實覆蓋改善**，
唔係湊數。

**小觀察（不列為 finding）**：`assert all(isinstance(v, str) for v in text_fields), text_fields`
用 genexp，pytest 展開唔到係邊個 field 出事，只顯示 `assert False`。
但因為 message 帶住成個 tuple（上面 probe output 見到四個值都印晒），
讀者一樣睇得出邊個唔對。可接受。

---

## 新 Finding

> ID 依 `skills/sw-ticket-management` 規範賦號。掃 `.proj-docs/reviews/` 得出各類型最高序號：
> **C-002 / W-023 / S-041**。本輪由 **S-042** 起，全局唯一、無重用。

### 🟢 S-042 — S-039 修好嘅不變式，同一檔案仲有 12 個 test case 靠緊（而且用 exact `==`）

**位置**：`tests/test_api.py`，`sql_count` fixture（`:914`）同佢六個 consumer

**描述**：S-039 喺
`test_a_flood_of_aliased_parents_issues_more_statements_than_the_field_cap_bounds`
加咗 reset，並且喺 comment 明確命名咗個問題——
「an argument-order invariant nothing states or holds」。呢個描述**完全準確**，
但個不變式唔止服務嗰一個測試：檔案入面**另外五個 test function（展開後 12 個 case）**
一樣喺冇 reset 之下直接讀 `sql_count[0]`，而且其中大部分用嘅係 **exact `==`**，
比 S-039 嗰個有 24 條 slack 嘅區間斷言**更脆**。

| 行 | 測試 | 斷言形狀 |
|---|---|---|
| 990 | `test_the_documented_worst_cases_still_measure_as_documented`（4 param） | `== statements` |
| 1005 | `test_aliased_track_fields_under_one_parent_reach_the_field_cap` | `== 2 + 2` |
| 1050 | `test_an_alias_flood_under_the_points_budget_is_still_rejected` | `0 < ... <= MAX` |
| 1396 | `test_a_page_of_tracks_costs_the_same_statements_however_wide_it_is`（5 param） | `== 2 + 2` |
| 1409 | `test_a_batch_is_keyed_by_points_as_well_as_by_activity` | `== 2 + n*2` |
| 1456 | `test_the_batch_never_reads_further_than_the_field_budget_reaches` | `== 2 + 2` |

**獨立驗證**：我將呢六個測試嘅 `sql_count` 調去 `year_client` 之前（即係 S-039
自己用過嘅同一個 probe），跑測試：

```
E  assert 397 == 4          E  assert 397 == (2 + 2)   × 6
E  assert 499 == 106        E  assert 401 == (2 + (3 * 2))
E  assert 601 == 208
E  assert 725 == 332
12 failed
```

**決定性對照**：S-039 修好嗰個測試**冇出現喺 FAILED 名單入面**——佢個 reset 保住咗佢。
即係話個修正真係有效，但**佢周圍嘅嘢全部仍然冇保護**。

**影響：低，但值得寫低。** 今日全部 signature 都啱啱好將 `year_client` 排喺
`sql_count` 前面，所以全綠。風險係將來有人（a）重排參數、（b）喺 `sql_count`
之後插入一個會出 SQL 嘅 fixture、或者（c）照抄現有測試但打錯次序——
12 個**完全正確**嘅測試會一齊變紅，而 failure message（`397 == 4`）
會將人引去查一個唔存在嘅 statement 爆炸。同 S-039 本身一模一樣嘅 failure mode，
只係闊 12 倍。

同時有個次要嘅一致性問題：而家全個檔案得一個測試示範咗「點樣唔靠呢個不變式」，
讀者好容易以為其餘嗰啲已經安全。

**方案 A（根治，推薦）**：喺 fixture 層解決，令次序無關緊要——

```python
@pytest.fixture
def sql_count(year_client):          # ← 令 client 必定先建好，listener 後 attach
    """Count statements every engine issues while the fixture is alive."""
```

一行，**一次過覆蓋全部 12 個 case**，而且將個不變式由「靠每個測試自己排啱次序」
變成「由 fixture graph 保證」——pytest 會強制先建 `year_client`，
無論測試點寫參數次序。

*Trade-off*：`sql_count` 由通用 fixture 變成同 `year_client` 綁死。
日後有測試想數另一個 client 嘅 statement，就會白白建多個 `year_client`。
睇返現況六個 consumer **全部**都用 `year_client`，呢個成本今日係零；
真係出現第二種 client 嗰陣，正路做法係抽一個 `sql_count_for(client)` factory。

**方案 B（跟 S-039 做法推廣）**：喺其餘五個測試各加一行 `sql_count[0] = 0`。

*Trade-off*：明確、局部、零 fixture 耦合，而且同 S-039 已經做咗嘅嘢一致。
但係五個地方重複同一行，而且**仍然靠紀律**——下一個新測試照樣可以漏。
另外兩個 exact `==` 測試加 reset 之後語意唔變，但要逐個確認 reset 位置
（要喺 `gql(...)` 之前、`year_client` 實例化之後）。

**推薦：方案 A。** S-039 個 comment 自己講咗問題係「nothing states or holds」
呢個不變式。方案 B 只做到 *states*（喺五個地方），方案 A 做到 ***holds***——
由 fixture 依賴關係強制執行，新測試自動受保護。而且佢同呢批 review 一路稱讚嘅思路一致：
**令錯誤形狀根本入唔到**，而唔係靠每個 call site 記得處理。

> 📌 呢條**唔係** delta 引入嘅回歸，亦唔係 developer 做漏——S-039 個 brief
> 明確就係嗰一個測試。呢個係 batch review 應該做嘅系統性觀察（`global-rules.md`
> Senior 思維：「發現系統性問題……提出整體改善建議」）。**唔阻塞本批 proceed。**

---

### 🟢 S-043 — W-023 測試入面重複 build 同一個 schema

**位置**：`tests/test_api.py:501` 同 `:512`

```python
sdl_types = build_schema_from_sdl(schema.as_str()).type_map.values()   # 501
...
root = build_schema_from_sdl(schema.as_str()).query_type               # 512
```

**描述**：同一個 SDL 喺同一個測試入面 build 咗兩次，相隔 11 行。兩個 call 攞嘅係
同一個 schema object 嘅唔同部分（`type_map` 同 `query_type`）。

**影響：極低。** 實測 `build_schema_from_sdl` 8.23 ms 一次，所以總成本約 8 ms——
**唔係性能問題**，純粹係 DRY。列出嚟係因為 🟢 檢查項有「重複代碼（DRY 改善機會）」，
而且兩個 call 用同一個 schema 讀唔同嘢，binding 咗會令「呢兩句講緊同一個 schema」
呢件事睇得出。

**方案 A**：抽一個 local——
```python
sdl_schema = build_schema_from_sdl(schema.as_str())
assert not [t.name for t in sdl_schema.type_map.values() if isinstance(...)], (...)
root = sdl_schema.query_type
```
**方案 B**：維持現狀。兩句相隔住一大段 assertion message，共用一個 local
反而要讀者記住個變數跨越十幾行。

**推薦：方案 A，但明確標為 optional。** 8 ms 同三行改動，
價值主要喺「兩句明顯讀緊同一個 schema」而唔喺慳時間。
**如果 developer 揀方案 B 我完全接受**，唔值得為佢單開一個 commit——
如果日後有人再郁呢個測試，順手做就得。

---

## CUI-0020 Ticket 狀態核實

`.tickets/in-progress/0001-0200/CUI-0020.md`：

| 要求 | 實況 | 判斷 |
|---|---|---|
| 標咗 blocked on CUI-0019 | line 4-6：`**Blocked on**：**CUI-0019**` + 理由 | ✅ |
| 未剔 DoD box 有理由 | line 50-53：`**未做，blocked on CUI-0019**` + 解釋「predicate 未郁，重量冇意義」 | ✅ |
| 狀態歷史有記錄 | line 111：2026-09-16 一行，寫明 review 跟進、唔可以標 done、檔案留 `in-progress/` | ✅ |
| **冇**被誤標 done | line 3 `**狀態**：in-progress`；全檔 grep `done` 只出現喺「唔可以標 done」 | ✅ |

**四項全中。** 另外兩個判斷我認為啱：

- **留喺 `in-progress/` 而唔搬去 `on-hold/`** —— 其餘 DoD 全部完成，
  只剩一個等外部 ticket 嘅重量動作。搬去 `on-hold/` 會令一張實質做完 95% 嘅票
  睇落似停擺。
- **「最終狀態交 QA 判斷」** —— Reviewer 同 Developer 都唔應該自行 close
  一張由 QA 開嘅票。呢個交接寫得啱。

---

## 新問題掃描（delta 範圍）

| 檢查 | 方法 | 結果 |
|---|---|---|
| **Production code 行為改動** | `ast` parse `dadb415` 同 `HEAD` **全部 41 個 `src/**/*.py`**，剝走所有 docstring 後比對 dump | ✅ **NONE** — 零差異 |
| 測試數目 | `pytest -q` | ✅ **363**（未變） |
| 測試增刪 | `git diff dadb415..HEAD -- tests/ \| grep '^[+-].*def test_\|class Test'` | ✅ **零新增、零刪除**（三項都係加 assertion 入現有 test，聲稱成立） |
| `test_api.py` 兩個 commit 干擾 | `git diff 3920fba..HEAD -- tests/test_api.py` | ✅ **只有 S-039 一個 hunk**；兩個改動相隔約 590 行（`:501` vs `:1096`），零重疊。兩個 marker 喺 HEAD 各出現 1 次 |
| Scope 越界 | `git diff --name-only` | ✅ 4 個檔案，全部喺 finding 宣告範圍內。**無**掂 `frontend/`、`.github/`、`pyproject.toml`、`docs/` |
| Secret / 絕對路徑 / email | grep `api_key`/`secret`/`password`/`token =`/`ghp_`/`sk-`/`PRIVATE KEY`/`/home/`/`/Users/`/email | ✅ **零命中** |
| Model identifier | grep `opus`/`sonnet`/`haiku`/`claude-[0-9a-z]`/`gpt-`/`gemini` 於 commit message | ✅ 唯一命中係 4 條 `Claude-Session:` URL trailer，**係 session URL 唔係 model identifier** |
| 新增依賴 | delta 無掂 `pyproject.toml` | ✅ 零新增 |

---

## ✅ 做得好嘅地方（本輪 delta）

1. **每個修正都有一個「點解呢個唔止係美觀」嘅驗證。** S-039 唔係講「加咗就對稱啲」，
   而係去證明喺另一個 fixture 次序之下一個**正確嘅 request 會變紅**；
   S-040 唔係講「isinstance 好啲」，而係造一個等於 `""` 但唔係 `str` 嘅值
   去證明舊寫法在數學上捉唔到。**呢個係「修 finding」同「令 finding 消失」嘅分別。**

2. **每個 probe 都分清 AssertionError 同 collection error。** 三個 commit message
   都明文講「an assertion failure, not a collection error」——直接對應
   `CLAUDE.md` §6 嗰個陷阱。上一輪讚過呢點，本輪**做到成為習慣**。

3. **W-022 揀咗根治而唔係補鑊。** 冇喺 preamble 寫返個 16x 等 `above` 有嘢指
   （方案 B，下次再有人改 preamble 又斷一次），而係將個比率寫入用佢嗰句。
   交叉引用之所以會斷就係因為佢依賴另一段文字繼續存在——呢個理解揸得準。

4. **S-041 識得分「呢份文件我可以改」同「呢份唔係我 lane」。** 冇為咗令兩個數字一致
   而去郁一份有日期嘅 QA 報告。lane 紀律同文件倫理喺呢度啱啱好同向。

5. **W-023 揀方案 A 之後仲補咗 helper docstring。** 唔單止加 assertion，
   仲喺 `_deepest_selection` 個 docstring 寫明「Only object types are walked...
   which is why the caller asserts the schema has none before trusting what this
   returns」——令**斷言同佢守護嘅假設互相指向**。三個月後接手嘅人喺任何一邊
   落腳都搵到另一邊。

6. **S-040 順手補返 `icon_url`。** 發現佢有 `== ""` 但被漏出 `None` tuple 之外，
   一併補上。finding 範圍以外嘅真實改善。

7. **Ticket 狀態處理誠實。** 冇因為「其餘 DoD 都做晒」就順手標 done，
   反而主動寫低 blocked on CUI-0019 同埋「最終狀態交 QA 判斷」。

---

## 修正優先順序

| 次序 | ID | 嚴重度 | 位置 | 工作量 | 阻塞 proceed？ |
|---|---|---|---|---|---|
| 1 | S-042 | 🟢 | `tests/test_api.py:914` `sql_count` fixture | 一行（方案 A） | ❌ 否 |
| 2 | S-043 | 🟢 | `tests/test_api.py:501,512` | 三行，optional | ❌ 否 |
| — | S-038 | 🟢 | `src/common/time.py:11` | **維持唔修**（上一輪已判定，本輪唔翻案） | ❌ 否 |

**冇任何一條阻塞本批 proceed。** 兩條新 🟢 都係測試檔案內嘅 robustness / DRY，
零 production 風險，可以交俾 QA 之後再排，或者順手併入下一個掂到
`tests/test_api.py` 嘅 lane。

> 依 `skills/sw-ticket-management` 規範，每個 review item 一個獨立 commit，
> 格式 `fix: S-042 | <描述>`。

## 未修訂代碼交代

本報告唔附「修訂後完整代碼」：兩個新 finding 都係一至三行嘅局部改動，
方案 A / B 已喺各自 section 內列出確切寫法。Reviewer 亦唔寫 production code。

## 驗證用臨時改動聲明

為咗獨立重做 developer 嘅驗證聲稱，我曾經臨時改動 `tests/test_api.py`
（S-039 probe 兩次、S-042 probe 一次，全部調轉 fixture 參數次序），
每次即時 `git checkout tests/test_api.py` 還原。
W-023 同 S-040 兩個 probe **完全冇掂 repo 檔案**——用 scratchpad 嘅 pytest plugin
加 `monkeypatch` 做。

**最終狀態確認：**

```
$ git diff --stat src tests
（空）
$ git status --short
（空）
$ .venv/bin/python -m pytest tests -q
363 passed in 27.32s
```

**`git diff --stat src tests` 為空，working tree 乾淨，全部 probe 產物留喺
session scratchpad，repo 零殘留。**

---

## 結論

| 票 | 上一輪 | 本輪 | 阻塞項 |
|---|---|---|---|
| CUI-0006 | ✅ pass | ✅ pass | — （S-038 維持記錄不修） |
| CUI-0017 | ✅ pass | ✅ pass | — （S-039 已修；S-042 係鄰接系統性觀察） |
| CUI-0014 | ✅ pass | ✅ pass | — （S-040 已修） |
| CUI-0003 | ⚠️ warn（W-023） | ✅ **pass** | — （W-023 已修；S-043 optional） |
| CUI-0020 | ⚠️ warn（W-022） | ✅ **pass** | — 代碼層面已清；**ticket 本身仍 blocked on CUI-0019，唔可以標 done** |

**整體：✅ pass（97/100，0 Critical，0 Warning，3 Suggestion）**

兩個 Warning 全部解決並經獨立驗證，三個 Suggestion 全部解決，
hard gate 全綠，delta 零 production code 行為改動。
新增嘅兩條 🟢 都唔阻塞。**可以交 QA。**

唯一要 main agent / QA 記住嘅**非代碼**事項：**CUI-0020 唔可以標 done**，
佢最後一個 DoD（同 CUI-0019 一齊重量）要等 CUI-0019 郁完 batch predicate。
Ticket 檔案已經正確標註，最終狀態由 QA 判斷。

```
HANDOFF_RECEIPT
protocol: 1
agent: code-reviewer
status: pass
score: 97
critical: 0
warning: 0
suggestion: 3
report: .proj-docs/reviews/2026-09-16_review_low-cost-wave1_delta.md
tickets: CUI-0017, CUI-0020, CUI-0003, CUI-0006, CUI-0014
next_action: invoke-qa
notes: 5 個 finding 全部獨立驗證通過（S-039 601-vs-208 同 393 條洩漏 statement、W-023 union/interface 盲點真實存在、S-040 非-str stand-in 捉得住），hard gates 全綠、delta 零 production 行為改動、測試維持 363；新增 S-042（sql_count fixture-order 不變式仲有 12 個 case 靠緊）同 S-043（重複 build schema）兩條 🟢 皆不阻塞；CUI-0020 仍 blocked on CUI-0019 唔可以標 done。
```
