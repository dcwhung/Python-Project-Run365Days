# Batch Review — 2026-09-15 — low-cost-wave1

| 項目 | 內容 |
|---|---|
| 審閱者 | code-reviewer（獨立，未參與呢批代碼撰寫） |
| 目標 | 5 個已 merge 入 `claude/ai-dev-team-start-05jie2` 嘅 fix |
| Base | `e827d95` |
| 涵蓋 commit | `6e901ce` CUI-0017 / `58d5c4b` CUI-0020 / `81a5588` CUI-0003 / `8068a81` CUI-0006 / `aad7c29` CUI-0014 |
| 報告路徑 | `.proj-docs/reviews/2026-09-15_review_low-cost-wave1_batch.md` |

## 總評

呢批係我近期見過質素相當高嘅 batch。整批 758 行改動入面，**只有一個檔案有可執行代碼改動**（`src/common/time.py`），其餘全部係 docstring 同測試 —— 呢點我用 AST 比對獨立證實咗，唔係靠 commit message 聲稱。

CUI-0006 嘅 `byte-identical` 聲稱我冇照單全收：獨立重寫咗 pre-fix 分支，對真實 export 全部 **429,078 個 timestamp** 逐個新舊對比，isoformat 同 instant 零分歧，而且 shape 分佈（235,070 個 `Z`-with-ms + 194,008 個 `+NN:NN`）同 developer 報嘅數字**完全一致**。呢個係整批最值得信嘅一環。

兩個 🟡 Warning 都係小修即可，冇 🔴 Critical。整體分數 86 → `warn`。

## Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| Tests | `.venv/bin/python -m pytest tests -q` | ✅ pass — **363 passed** in 20.45s |
| Lint | `.venv/bin/ruff check src tests` | ✅ pass — All checks passed |
| Format | `.venv/bin/ruff format --check src tests` | ✅ pass — 58 files already formatted |
| Type check | （本 repo 無 mypy/tsc gate，ruff 承擔） | n/a |
| Coverage | 核心邏輯：改動路徑全部有測試覆蓋（見下） | ✅ pass |
| No Critical | 🔴 Critical = 0 | ✅ pass |
| Security scan | 本批零新增依賴 | n/a |

**全部 hard gate 通過。**

## 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|------|------|------|------|
| 正確性 | 24 | 25 | S-038（offset grammar 收窄） |
| 安全性 | 20 | 20 | 無新增依賴、無敏感資料、無注入面 |
| 可維護性 | 14 | 20 | W-022（dangling 交叉引用）、S-041 |
| 測試覆蓋 | 8 | 15 | W-023（guard 盲點）、S-039、S-040 |
| 性能 | 10 | 10 | 無回歸；statement count 有上限測試把關 |
| 代碼風格 | 10 | 10 | ruff 全綠，命名同 docstring 規範一致 |
| **總分** | **86** | **100** | |

> 測試覆蓋 8/15 係機械套用扣分規則嘅結果，**唔代表測試質素差**。呢批加咗 12 個測試，我逐個驗過都係 load-bearing。扣分係針對 W-023 個 guard 盲點同兩個 assertion 寫法，唔係針對覆蓋率本身。

**結果：⚠️ warn**

---

## Section A — CUI-0006（`8068a81`，唯一有行為改動）

### 改動清單
- `src/common/time.py`：新增 `_ISO_OFFSET_PATTERN`，`elif "+" in rec_time` → `elif _ISO_OFFSET_PATTERN.search(rec_time)`，加 `.astimezone(tz)`，重寫 `Returns` 契約
- `tests/test_common_time.py`：`TestParseDateTimeOffsetIsReturnedAsWritten` → `TestParseDateTimeIsoOffsetIsConvertedToTimezone`

### ✅ Looks Good — regex 正確性（獨立驗證）

我用 13 種 input 逐個打，包括你特別點名嘅三類：

| Input | 命中 offset 分支？ | 正唔正確 |
|---|---|---|
| `1634422256000`（Unix-ms 13 位） | ❌ 否 | ✅ 正確（無 `T`，根本入唔到，而且無前置 `+/-`） |
| `2021-10-17 06:10:56`（naive） | ❌ 否 | ✅ 正確 |
| `2021-10-17T06:10:56`（naive with T） | ❌ 否 | ✅ 正確 |
| `2021-10-16T22:10:56.000Z` | ❌ 否 | ✅ 正確（前一分支已食） |
| `2021-10-17T06:10:56+08:00` / `-05:00` / `+0800` / `+00:00` | ✅ 係 | ✅ 正確 |

另外我用 **200,000 個隨機 13 位數字**跑過，**零 false positive**。個 anchor 同 character class 係啱嘅 —— 關鍵在 `$`，冇咗佢 `[+-]` 就會俾日期分隔符食中，呢點 docstring 都解釋得好清楚。

至於 `$` 喺 Python 會 match trailing newline 呢個經典陷阱：**唔構成問題**，因為 `rec_time.strip()` 喺 pattern 之前已經行咗。

### ✅ Looks Good — `.astimezone(tz)` 位置正確

我驗證過：**pattern 命中時 `dateutil.parser.parse` 必定回 aware datetime**，所以 `.astimezone(tz)` 唔會踩到「naive datetime 被當成系統本地時間」嗰個坑。呢個係最容易出事嘅位，佢避開咗。

### ✅ Looks Good — byte-identical 聲稱經獨立抽驗（唔係照單全收）

我冇信 commit message。我照 `git show e827d95:src/common/time.py` 由零重寫咗 pre-fix 邏輯，然後掃晒 `data/raw/garmin/{kml,gpx,tcx}` 全部 1,095 個檔：

```
timestamps scanned : 429,078
distinct shapes    :
     235,070  NNNN-NN-NNTNN:NN:NN.NNNZ
     194,008  NNNN-NN-NNTNN:NN:NN+NN:NN
old raised         : 0
new raised         : 0
instant mismatches : 0
isoformat mismatch : 0
```

三個結論：
1. **零分歧**，byte-identical 聲稱成立
2. Shape 分佈同 developer 報嘅 235,070 / 194,008 **逐個數字對得上**
3. 真實資料**只有兩種 shape**，所以下面 S-038 講嘅收窄係不可達嘅

而且我核實過 `_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"`（三個 parser 都係）—— **輸出完全唔帶 offset**。呢個正正證實咗 fix 嘅理據係真嘅：一個日本跑嘅 KML 寫 `+09:00`，舊行為會直接攞 `06:10:56` 去 strftime，而佢自己嘅 GPX/TCX 孿生檔（走 `Z` 分支，有轉換）會寫 `05:10:56`，兩者差一個鐘。唔係理論風險。

### ✅ Looks Good — 測試改寫係正當繼承，唔係掩蓋回歸

呢個係你特別要我判嘅位，我逐條追咗舊 class 4 個測試嘅去向：

| 舊測試 | 去向 | 判斷 |
|---|---|---|
| `test_a_foreign_offset_survives_the_hong_kong_default` | → `test_a_foreign_positive_offset_is_converted_to_the_requested_zone` | ✅ 反轉斷言，正確 |
| `test_the_timezone_argument_does_not_reach_this_branch` | → `test_the_offset_follows_the_argument_not_the_input` | ✅ 精確逆命題 |
| `test_the_only_offset_the_callers_feed_is_the_one_that_hides_this` | → `test_the_offset_the_callers_actually_feed_is_left_where_it_was` | ✅ **保留並加強**（多咗 isoformat 斷言） |
| `test_neighbouring_iso_shapes_are_not_supported`（2 param） | negative-offset → 變成正面支援測試；utc-without-millis → `test_a_utc_z_string_without_milliseconds_is_still_not_supported` | ✅ 兩半都有交代 |

**零覆蓋損失。** 而且決定性證據：舊 class 自己個 docstring 白紙黑字寫住「*this is what a future `.astimezone()` would turn red*」—— 即係舊測試**預先授權**咗呢次改寫。呢張票就係嗰個 future。改寫合理，唔係掩蓋。

特別欣賞 `test_the_offset_the_callers_actually_feed_is_left_where_it_was` 保留咗落嚟 —— 佢由「解釋舊 defect 點解隱形」轉職做「365 個 activity 嘅回歸守門員」，呢個轉職做得好。

### ✅ Looks Good — 負 offset 同跨 zone 絕對時刻不變，真係覆蓋到

- 負 offset：`test_a_negative_offset_is_parsed_rather_than_rejected`，`-05:00` × 三個 zone（HK / New_York / UTC），連 DST 之下 `-04:00` 都計啱
- 絕對時刻不變：`test_the_instant_is_the_same_whatever_timezone_asks_for` 用 `set` of `.timestamp()` 斷言 `len == 1`，跨三個 zone × 正/負/零 offset

呢個係正確嘅寫法 —— 斷言嘅係 instant 不變而唔係字串相等，抓到「轉換 vs 重新詮釋」嘅真正分別。

### 🟢 S-038 — offset grammar 相對舊分支收窄，兩種 ISO 形狀由「接受」變「拋 ValueError」

**位置**：`src/common/time.py:11`

**描述**：舊 `elif "+" in rec_time` 會接受 `+08`（只有鐘數）同 `+08:00:00`（帶秒）兩種合法 ISO 8601 offset 並原樣回傳。新 pattern `[+-]\d{2}:?\d{2}$` 兩者都 miss，會跌落 naive `strptime` 拋 `ValueError`。我實測確認：

```
'2021-10-17T06:10:56+08:00:00'  match=0   ← 舊行為接受
'2021-10-17T06:10:56+08'        match=0   ← 舊行為接受
```

**影響**：**實際為零。** 真實 export 只有兩種 shape（上面 429,078 個實測），Garmin 唔會寫呢兩種。而且 docstring 寫得好準確 —— 只聲稱「Any `±HH:MM` or `±HHMM` offset is accepted」，冇 over-claim。

**方案 A**：放置不理，現狀已經誠實。
**方案 B**：`[+-]\d{2}(?::?\d{2}(?::?\d{2})?)?$` 覆蓋埋 `+08` 同 `+08:00:00`。

**推薦：方案 A。** 收窄咗一個不可達嘅輸入，而且 docstring 冇講大話。加複雜度去接住一個 Garmin 唔會產生嘅形狀，唔抵。呢條純粹記錄低，等日後有人接第四種 export 格式時有跡可循。

### 評分（CUI-0006）
正確性 24/25、其餘滿分 → **本票 pass**

---

## Section B — CUI-0003（`81a5588`）

### 改動清單
- `src/api/schema.py`：`MAX_QUERY_DEPTH` docstring 擴寫（**零代碼改動**，AST 證實）
- `tests/test_api.py`：`DEEPEST_REACHABLE_DEPTH`、`_unwrap`、`_deepest_selection`、2 個新測試

> 依指示，`MAX_QUERY_DEPTH = 5` 係用戶已拍板嘅決定，本 review **唔 challenge**，只審 docstring 準確性同測試質素。

### ✅ Looks Good — docstring 準確講清楚「今日唔會 fire」

呢個係我要核實嘅重點。docstring 寫：

> **this limiter cannot fire against the schema as it stands**, and that is the choice, not an oversight (CUI-0003). A document deep enough to refuse cannot be written, so nothing a client sends today is stopped here.

我獨立量度咗 type graph：`_deepest_selection(root)` 回 5 levels → reachable `max_depth` = 4，而 `MAX_QUERY_DEPTH = 5`。**4 < 5，所以「cannot fire」係事實陳述，唔係修辭。** 準確。

更加值得讚嘅係下一段：明確指出 alias flood 係「wide rather than deep」，`MAX_QUERY_DEPTH` 點都攔唔到，真正做嘢嘅係 `MAX_QUERY_TOKENS`，最後一句 "Read 'we have a depth limit' as covering that and the cover is imaginary" —— 呢句就係呢張票嘅價值所在。將一個 **security theatre** 明確標記成 headroom 而唔係防護，避免日後有人靠住佢做錯風險判斷。

### ✅ Looks Good — failure message 讀得明（我人為整紅驗過）

Developer 自己 flag 咗呢點要人睇，我照做，兩個 scenario 都試：

**Scenario A — 有人加咗一層 nested field：**
```
AssertionError: the type graph changed depth; re-read MAX_QUERY_DEPTH's docstring,
which says the limiter cannot fire, and the test above, which says where it would
```

**Scenario B — graph 深過 MAX_QUERY_DEPTH（limiter 變成 live）：**
```
AssertionError: the limiter can now refuse a document the schema allows
```

**判斷：讀得明，而且合格有餘。** 兩個 message 唔單止講「乜嘢壞咗」，仲**指住去邊度讀下一步**（constant 個 docstring + 隔籬嗰個測試）。再加 pytest assertion rewriting 會自動補上 `assert 4 == 3`，一個三個月後接手嘅人睇到呢個 message 係知道要做乜嘅。呢點做得比大部分 codebase 好。

### ✅ Looks Good — 無 infinite loop 風險

`_deepest_selection(gql_type, seen=())` 用 tuple 沿住**路徑**帶住已訪問嘅 type name，`if ... or gql_type.name in seen: return 0`。即使 type graph 日後變 cyclic，遞迴一樣終止。`seen=()` 係 immutable default，冇 mutable-default 陷阱。**設計正確。**

（次要：path-based memo 喺一個密集 graph 會有指數級複雜度，但呢個 schema 15 個 type、跑起身瞬間完，唔值得改。）

### 🟡 W-023 — 深度 guard 對 interface / union 有靜默盲點

**位置**：`tests/test_api.py`，`_deepest_selection`

```python
if not isinstance(gql_type, GraphQLObjectType) or gql_type.name in seen:
    return 0
```

**描述**：只認 `GraphQLObjectType`。如果將來有人加 `GraphQLInterfaceType` 或 `GraphQLUnionType`，佢會直接回 `0`，**當佢係 leaf**。

**影響**：呢個測試嘅存在意義就係「有人加 nested field 時會變紅」。但如果嗰個 nested field 係加喺 union / interface 之下，深度會被**低估**，`reachable` 數字唔變，測試**照樣綠**，而 `MAX_QUERY_DEPTH` 已經悄悄變成可以 fire —— 正正係呢個測試要防嘅事。Guard 靜默失效，比冇 guard 更危險，因為佢會俾人虛假安全感。

我核實過現狀：SDL 入面 **0 個 interface、0 個 union**（15 個全部係 `type`），所以**今日係潛伏而非已發生**。Strawberry code-first 加 union 只係一個 `Annotated[A | B, strawberry.union(...)]` 嘅距離，唔算遙遠。

**方案 A**（cheap，推薦）：加一句斷言，明確講清楚呢個 walk 嘅適用前提 ——
```python
assert not any(
    isinstance(t, GraphQLInterfaceType | GraphQLUnionType)
    for t in build_schema_from_sdl(schema.as_str()).type_map.values()
), "_deepest_selection only walks object types; teach it about abstract types first"
```
一行，令盲點變成會叫嘅嘢，唔使即刻實作抽象類型嘅 walk。

**方案 B**（完整）：擴充 `_deepest_selection` 處理 `GraphQLInterfaceType`（walk `.fields`）同 `GraphQLUnionType`（walk `.types` 取 max）。正確，但為咗一個未存在嘅 schema 形狀而家就寫，屬於預先設計。

**推薦：方案 A。** 依家就用一行令「遇到未知形狀」由靜默低估變成大聲 fail，等真係加 union 嗰日，係個測試提你去補 walk，而唔係你自己發現。呢個同 developer 喺 `seen` 參數上面嘅思路（「this is what keeps the walk finite on the day it stops being [acyclic]」）完全一致 —— 佢已經為 cyclic 諗過一步，只係漏咗 abstract type 呢一步。

### 🟢 S-039 — 見 Section C（同類問題，歸喺 CUI-0017）

### 評分（CUI-0003）
測試覆蓋 -5（W-023）→ **本票 warn**

---

## Section C — CUI-0017（`6e901ce`）

### 改動清單
- `src/api/schema.py`：`MAX_TRACK_FIELDS_PER_REQUEST` docstring 加 3 行交叉引用（**零代碼改動**）
- `tests/test_api.py`：`_distinct_parent_flood` 加 `points` 參數、`PARENT_FLOOD_WIDTH`、`_widest_parent_flood`、1 個新測試

### ✅ Looks Good — width 真係由 `MAX_QUERY_TOKENS` 推導（核實通過）

呢個聲稱我實測咗：

```
derived widest parent flood      : 52  (pinned 52)
tokens at that width             : 990 / 1000
tokens at width+1                : 1009
```

`_widest_parent_flood()` 係**真推導**（loop 到 `_token_count > MAX_QUERY_TOKENS` 為止），測試body 用嘅係推導值。`PARENT_FLOOD_WIDTH = 52` 唔係 hard-code 嘅替代品，而係一個**釘**：`assert parents == PARENT_FLOOD_WIDTH` 令 `MAX_QUERY_TOKENS` 一郁就變紅，而唔係靜靜雞跟住飄。

呢個係比純推導**更好**嘅寫法 —— 純推導嘅測試喺 limit 改變時會自我調整，永遠綠，等於冇守到。佢個 docstring「derived rather than trusted」講得啱，而且 `990 / 1009` 兩個數同我實測**逐個對上**。聲稱成立。

### ✅ Looks Good — 208 = 52×2 + 52×2 實測對數

逐個 verify 咗 docstring 每一個數字：

```
statements issued (served whole) : 208
ticket claim 52*2 + 52*2         : 208   ✅
cap alone would account for      : 128   ✅（= MAX_TRACK_FIELDS_PER_REQUEST 64 × SQL_PER_TRACK_BATCH 2）
test ceiling                     : 232
statements (tracks refused)      : 124   floor 104 ✅
```

docstring 新加嗰句「the widest flood of parents the token limit admits issues 208 statements where this cap on its own would account for 128」—— **208 同 128 都實測對上**。Developer 判定 prose 已準確所以唔改，呢個判斷我核實過係**正確**嘅。

### ✅ Looks Good — 唔 flaky

你特別問 statement counting 會唔會對 SQLAlchemy 版本 / DB 狀態敏感。我連跑 5 次同一個 client：

```
5 consecutive runs, same client : [208, 208, 208, 208, 208]
```

完全確定性。而且測試設計本身就抗飄：
- 用 `>` floor 同 `<=` ceiling 嘅**區間**斷言，唔係 `==` 死數（實測 208，ceiling 232，有 24 條 slack）
- `SQL_PER_TRACK_BATCH` / `SQL_PER_ACTIVITY_FIELD` 抽咗做具名常數，唔係散落嘅 magic number
- 兩個 limit 任何一個郁都會紅

唯一真 `==` 嘅係 `parents == PARENT_FLOOD_WIDTH`，但嗰個純粹係 token 計數，完全唔掂 DB，零飄動來源。

### ✅ Looks Good — `points` 參數嘅設計理由成立

`_distinct_parent_flood(parents, points)` 個 docstring 講「an INT is one token however many digits it spells」，所以谷高 `points` 可以**喺唔改 token 成本嘅前提下**，將同一份 document 由「points budget 服務晒」變成「refuse 大部分」。呢個係好嚴謹嘅實驗設計 —— 排除咗 parser 先拒絕嘅可能，令「parent 一樣要俾錢」嗰半嘅斷言真係量緊佢想量嗰樣嘢。我實測 refuse 情況下 124 條 ≥ floor 104，成立。

### 🟢 S-039 — 測試前半冇重置 `sql_count[0]`，隱式依賴 fixture 實例化次序

**位置**：`tests/test_api.py`，`test_a_flood_of_aliased_parents_issues_more_statements_than_the_field_cap_bounds`

**描述**：測試後半有 `sql_count[0] = 0` 明確重置，但**前半冇**，直接讀 `sql_count[0]`。安全性建基於 `year_client` 喺 `sql_count` 之前實例化（因為參數次序），令 fixture setup 嘅 statement 唔會計埋入去。

**影響**：低。24 條 slack 食得起偶發雜訊，而且呢個寫法同檔案入面現有 alias-flood 測試一致（`assert 0 < sql_count[0] <= ALIAS_FLOOD_MAX_SQL`），唔係本票引入嘅。但「同一個測試前半唔重置、後半重置」讀落有啲唔對稱。

**方案 A**：前半 `gql(...)` 之前加一行 `sql_count[0] = 0`，同後半對稱。
**方案 B**：維持現狀，跟檔案既有慣例。

**推薦：方案 A。** 一行，令測試唔再依賴一個唔明顯嘅 fixture 次序不變式，而且消除同一函數內嘅不對稱。

### 評分（CUI-0017）
測試覆蓋 -1（S-039）→ **本票 pass**

---

## Section D — CUI-0020（`58d5c4b`，純 docs）

### 改動清單
- `src/api/schema.py`：`MAX_TRACK_FIELDS_PER_REQUEST` docstring 重寫（**零代碼改動**，AST 證實）
- 確認**冇**加任何 wall-clock assertion ✅

### ✅ Looks Good — 每個 wall clock 都標明規模，數字全部對得上 QA 報告

我逐個 grep 返 `.proj-docs/qa/2026-09-15_qa_cui0016-au048-au050_batch.md` 對數：

| docstring 講 | QA 報告 | 對得上？ |
|---|---|---|
| `activities(limit:64){track(points:1)}` 10.4 ms fixture / 165.6 ms export | line 410, 582 | ✅ |
| 64 aliased track under one parent 20.8 ms / 21.7 ms | line 412, 584 | ✅ |
| 52 parents 同一 activity 68.7 ms / 78.2 ms | line 413, 585 | ✅ |
| 52 parents 唔同 activity 141.4 ms / 194.8 ms | line 414, 586 | ✅ |
| 最差合法 shape 390 ms，38x | line 321, 393, 519, 529 | ✅ |
| export 134,041 track rows | line 9, 69, 404 | ✅ |

**六項全中，零捏造。** 而且「166 aliased activities ... ~2 s on the fixture」呢個冇 export 數字嘅讀數，佢主動標明「That is the one reading here with no export figure beside it: QA has not measured this shape at production scale」，並且明確區分 "expected, not measured"。**呢個誠實度正正係呢張票想要嘅嘢** —— 冇量過就講冇量過，唔靠估數充場面。

### ✅ Looks Good — 刪「60x」改用 export 38x 係**正確**，而且唔算超出 brief

Developer 話呢個超出咗 brief 但有講。我核實之後判斷：**唔算超出，反而係照做。** QA 報告 line 588 白紙黑字：

> production 最差合法 shape 實測 390 ms → 約 **38x** 喺 15 s 之內，唔係 60x。

即係 QA **明確指名**要改呢個數。而且舊講法「60x inside the 15 s Vercel function」本身兩個 scale 都對唔上（15 s ÷ 0.2 s = 75x，÷ 141 ms = 106x），係 AU-050 之前嘅殘留。將一個 production 餘裕 claim 由 fixture 數字改成 export 數字，方向完全正確 —— **餘裕呢種嘢就係要用真實規模嚟講，用 fixture 講等於冇講。** 呢個改動我完全支持。

同樣 preamble 由「fixture readings do not carry to production」收窄成「只有 wide-batch 嗰個唔 carry，其餘大致 carry」，亦有實測支撐（16x vs <1.4x）。精確化，好。

### 🟡 W-022 — `the 16x above` 變成 dangling 交叉引用，而且會指去一個唔相干嘅 16x

**位置**：`src/api/schema.py:120`

**描述**：本 commit 由 preamble 刪走咗呢句 ——

```
-QA measured both on 2026-09-15 -- the 64-track page
-below read 10.4 ms against the fixture and 165.6 ms against the real export,
-16x, while the two 52-parent shapes ...
```

—— 即係刪走咗 `MAX_TRACK_FIELDS_PER_REQUEST` docstring 入面**唯一一個** `16x`。但下面 bullet 依然寫住：

```
~0.01 s on the fixture but ~0.2 s on the export (10.4 ms and 165.6 ms) --
the 16x above, and the only reading here that moves with ...
```

我 grep 咗成個 `schema.py`，`16x` 得兩個位置：

| 行 | 屬於邊個 constant | 意思 |
|---|---|---|
| 59 | **`MAX_TRACK_POINTS_PER_REQUEST`** | 「this sits roughly 16x above real **traffic**」— 額度 vs 真實流量嘅**餘裕比** |
| 120 | `MAX_TRACK_FIELDS_PER_REQUEST` | 「the 16x **above**」— 想指 fixture vs export 嘅**規模比** |

**影響**：呢個比單純 dangling 更差少少。讀者跟住 "above" 向上搵，會搵到 line 59 嗰個 `16x` —— 一個**數值啱啱好一樣但意思完全唔同**嘅比率（餘裕比 vs 規模比），而且係屬於**另一個 constant** 嘅 docstring。數字巧合相同，令呢個誤讀特別難察覺。

呢張票嘅**全部意義**就係 docstring 準確性。喺一個以「令每個數字都講清楚出處」為目標嘅 commit 入面留低一個指向錯地方嘅交叉引用，係要修嘅。純文檔、零風險、零測試影響，但要修。

**方案 A**（推薦）：bullet 唔再靠 "above"，自己講清楚 ——
```
(10.4 ms and 165.6 ms, a 16x gap), and the only reading here that moves ...
```
一個詞嘅改動，令個數字自足，唔依賴上文仲喺唔喺度。

**方案 B**：喺 preamble 重新寫返個 16x，等 "above" 有嘢可指。但咁樣就局部撤回咗本 commit 想做嘅精簡，而且第二次再有人改 preamble 又會再斷一次。

**推薦：方案 A。** 根治而唔係補鑊：交叉引用之所以會斷，就係因為佢依賴另一段文字繼續存在。將個數字寫返喺用佢嗰句度，就再冇得斷。順帶消除同 line 59 嗰個同值異義 `16x` 嘅混淆。

### 🟢 S-041 — `223x` vs QA 報告 `224×`（跨文件細微不一致）

**位置**：`src/api/schema.py:104`（`223x`）vs QA 報告 line 404 / 571（`224 倍`）

**描述**：134,041 ÷ 600 = **223.4**。`schema.py` 取 223（截尾），QA 取 224（四捨五入）。兩者都講得通，但同一個比率喺兩份文件寫唔同數字。呢個 `223x` 係 base 已有、本 commit 冇郁。

**方案 A**：統一成 `223x`（更貼近 223.4）。
**方案 B**：放置不理。

**推薦：方案 A**，順手喺修 W-022 嗰陣一齊改；唔值得為佢單獨開一個 commit。

### DoD 備註（非 finding）

`CUI-0020.md` 有一個 DoD box 仲未剔：

```
- [ ] 同 CUI-0019 嘅修復一齊重量，避免改完 predicate 之後啲數又過期
```

Developer 喺 commit message 明確講咗係**刻意留低**（CUI-0019 未郁 batch predicate，依家重量只會再過期一次）。判斷合理。但意味住**呢張票唔可以標 done**，要 blocked-on CUI-0019。交返 main agent 處理。

### 評分（CUI-0020）
可維護性 -5（W-022）-1（S-041）→ **本票 warn**

---

## Section E — CUI-0014（`aad7c29`）

### 改動清單
- `src/weather/models.py`：`_MISSING_TEXT` 加 docstring、三個 `from_raw_row()` 加 docstring
- `tests/test_weather_models.py`：`TestMissingStringColumnsReadAsEmptyText`（2 個測試）

### ✅ Looks Good — 真係零 production code 改動（AST 證實，唔靠讀 diff）

我冇淨係眼睇 diff。我 parse 咗 `e827d95` 同 HEAD 兩個版本嘅 AST，**剝走所有 docstring** 之後比對：

```
src/weather/models.py        executable code identical: True
src/api/schema.py            executable code identical: True
src/common/time.py           executable code identical: False   ← 預期（CUI-0006）
```

`models.py` 同 `schema.py` 嘅可執行代碼同 base **逐個 AST node 完全相同**。即係 CUI-0014、CUI-0020、CUI-0017、CUI-0003 四張票加埋，**冇改過一行會行嘅代碼**。「diff 係 docstrings 同 tests only」呢個聲稱，成立。

### ✅ Looks Good — 兩條測試確實 load-bearing（我自己 flip 過）

Developer 話佢 flip `_MISSING_TEXT` 驗過會紅。我獨立做多次：將 `_MISSING_TEXT: Final = ""` 改成 `= None`，跑測試：

```
FAILED ... test_hourly_from_raw_row_reads_a_missing_string_column_as_empty_text
FAILED ... test_warning_from_raw_row_reads_a_missing_string_column_as_empty_text
2 failed, 19 passed

E  AssertionError: assert None == ''
E   +  where None = WeatherWarning(date='2021-01-08', warning_type=None, ...).warning_type
```

**關鍵：係 `AssertionError`，唔係 collection error。** 呢點正正就係 `CLAUDE.md` §6 列咗嘅陷阱（「TDD Red 階段唔好 import 未存在嘅 symbol，會變 collection Error 而唔係 assertion Fail」）。Developer 唔單止驗過，仲係用**啱嘅標準**驗（分清 red 同壞），呢個要讚。

（已即時 `cp` 還原，`git diff --stat src/weather/models.py` 乾淨，repo 無殘留。）

### ✅ Looks Good — `date` 保留 `KeyError` 嘅判斷正確

docstring 講：

> `Date` is the exception and is read with `row[...]`: it is the row's identity, the key every export record and day lookup joins on, so a row without one is broken data. It raises `KeyError` by design rather than defaulting to `""` and quietly producing a record keyed on empty text.

**我認同，而且呢個係整張票最好嘅判斷。** 理由：

1. **identity 同 reading 有本質分別。** 缺一個 `Description` 係「呢個鐘冇描述」—— 資料完整，讀數缺失，降級成 `""` 講得通。缺 `Date` 係「呢一行係乜都唔知」—— 唔係缺讀數，係缺身份。
2. **降級嘅後果不對稱。** `date=""` 唔會即時炒，會靜靜雞造出一個 key 係空字串嘅 record，然後流去 join、去 day lookup、去 export。到出事嗰陣已經離源頭好遠。fail fast 喺呢度係啱嘅取捨。
3. **同 `_MISSING_TEXT` 嘅決定唔矛盾，反而互補** —— 兩者一齊先講得出完整規則：「讀數降級，身份 fail fast」。三個 `from_raw_row` docstring 都交叉引去 `HourlyWeather.from_raw_row` 講原因，唔使三邊重複，呢個組織方式好。

另外 `DailyWeather` 嗰段講明「`_MISSING_TEXT` does not arise here」—— 主動交代一個**唔適用**嘅情況，避免日後有人以為漏咗。細心。

### 🟢 S-040 — 兩個冗餘斷言（`is not None` 喺 `== ""` 之後）

**位置**：`tests/test_weather_models.py`

```python
assert record.description == ""
assert record.description is not None          # ← 前一行過咗就必然成立
...
assert None not in (record.warning_type, record.start_time, record.end_time)   # ← 同上
```

**描述**：`== ""` 通過之後，`is not None` 同 `None not in (...)` 邏輯上恆真，永遠唔會單獨變紅。

**影響**：極低。睇得出意圖係**寫俾人睇**（強調「重點係佢唔係 `None`，唔淨係啱啱好等於空字串」），而呢個意圖本身係啱嘅 —— class docstring 都係環繞「`""` 而唔係 `None`」。但用一個恆真斷言嚟表達意圖，會令讀者以為佢有守住啲嘢。

**方案 A**：刪走兩行，意圖交返俾 class docstring（已經講得好清楚）同測試名（`..._reads_a_missing_string_column_as_empty_text`）承擔。
**方案 B**：換成真係有內容嘅斷言，例如 `assert isinstance(record.description, str)` —— 咁就真係守住「annotation 係 `str`」呢個 docstring 明確講嘅契約，而且 flip 成 `None` 一樣會紅。

**推薦：方案 B。** 佢保住原本想表達嘅意思（「型別冇被 widen」），同時令嗰行真係有守到嘢。呢個正正接返 `_MISSING_TEXT` docstring 講嘅「what keeps that annotation honest」—— 用 `isinstance` 就係直接斷言嗰個 honesty。

### 評分（CUI-0014）
測試覆蓋 -1（S-040）→ **本票 pass**

---

## Section F — 橫向檢查（batch 特有）

### ✅ 三張票掂過 `schema.py`，合併後無矛盾

CUI-0017、CUI-0020、CUI-0003 都改過 `src/api/schema.py`。我讀咗合併後嘅最終文本：

- CUI-0017 插入 `MAX_TRACK_FIELDS_PER_REQUEST` 嘅 208 vs 128 句，同 CUI-0020 重寫嘅 wall-clock 段落**互相支撐**（208 statements 呢個數兩邊一致，而且我實測都係 208）
- CUI-0003 改嘅係 `MAX_QUERY_DEPTH`，同前兩者唔同 constant，無重疊
- CUI-0003 講「`MAX_QUERY_TOKENS` 先係真正攔 alias flood 嗰個」，同 CUI-0017 講「parent 嗰半成本淨係俾 `MAX_QUERY_TOKENS` 綁住」**論述一致**，兩邊都指向同一個結論

**唯一矛盾就係 W-022 嗰個 dangling `16x`**（CUI-0020 刪咗 preamble 嘅數字，但 bullet 仲引用緊）—— 已喺 Section D 記錄。

### ✅ 測試數加減完全對數（351 → 363，+12）

我唔淨係信 `363 passed`，我逐個檔案對數：

| 檔案 | base defs | HEAD defs | 實際 collected |
|---|---|---|---|
| `tests/test_api.py` | 74 | 77 (+3) | 104 |
| `tests/test_common_time.py` | 30 | 32 (+2) | 41 |
| `tests/test_weather_models.py` | 18 | 20 (+2) | 21 |
| **總計** | | **+7 defs** | **363** |

`+7` 個 def 點解係 `+12` 個 case？因為 CUI-0006 用咗 parametrize。逐個算：

- **CUI-0006**：舊 class 4 defs / 5 cases（1+1+1+2）→ 新 class 6 defs / **12 cases**（3+3+3+1+1+1）= **+7**
- **CUI-0003**：+2 cases
- **CUI-0017**：+1 case
- **CUI-0014**：+2 cases

**7 + 2 + 1 + 2 = 12。351 + 12 = 363 ✅ 逐項對得上，冇人偷數。**

### ✅ 冇偷偷刪測試

`git diff e827d95..HEAD -- tests/` 只有 4 個 `-    def test_`，全部嚟自 CUI-0006 嗰個**刻意被繼承**嘅 pin class。我已喺 Section A 逐條追晒去向，**零覆蓋損失**。批次內無其他測試被刪。

### ✅ 無 lane scope 以外嘅意外改動

`git diff e827d95..HEAD --stat` 13 個檔案：
- 3 個 `src/`（`api/schema.py`、`common/time.py`、`weather/models.py`）—— 全部喺 5 張票宣告嘅範圍內
- 3 個 `tests/`（對應上面三個）
- 7 個 `.tickets/`（5 張票 pending → in-progress 搬遷）

**零意外檔案。** 特別確認：無掂 `frontend/`、無掂 `.github/workflows/`、無掂 `pyproject.toml`、無掂 `docs/`、無新增依賴、無改 CI config。`run365-schema --check` 綠，證實 SDL 同 `frontend/schema.graphql` 冇甩拖。

### ✅ 無 secret / 絕對路徑 / 個人資料

| 掃描 | 結果 |
|---|---|
| `api_key` / `secret` / `password` / `token =` / `ghp_` / `sk-` / PRIVATE KEY | **零命中** |
| `/home/` / `/Users/` / `C:\` 絕對路徑 | **零命中** |
| Email address | **零命中** |

### ✅ Commit message 無 model identifier

掃 `opus` / `sonnet` / `haiku` / `claude-*` / `gpt` / `gemini`：唯一命中係 5 條 `Claude-Session: https://claude.ai/code/session_...` trailer —— **係 session URL，唔係 model identifier**。Co-author trailer 一律 `Co-Authored-By: Claude <noreply@anthropic.com>`，冇帶版本號。**符合項目規則。**

### ✅ 整批只有一個檔案有行為改動

值得獨立講一次：AST 比對證實 5 個 commit、758 行改動入面，**得 `src/common/time.py` 一個檔案嘅可執行代碼變咗**。其餘全部係 docstring 同測試。對一個 batch review 嚟講，呢個係最理想嘅風險分佈 —— 審閱火力可以集中,而回歸面收窄到一個已經被 429,078 個真實 timestamp 驗證過嘅函數。**Lane 切割做得好。**

---

## ✅ 做得好嘅地方（跨 fix 通用）

1. **Docstring 當契約寫，唔係當註解寫。** 呢批最突出嘅特質。`_ISO_OFFSET_PATTERN` 解釋咗點解要 anchor（`"-" in rec_time` 會俾日期分隔符食中）；`MAX_QUERY_DEPTH` 直認個 limiter「cannot fire」係選擇唔係疏忽；`_MISSING_TEXT` 講明係相對舊 reader 嘅**narrowing**。呢啲都係三個月後救人嘅資訊。

2. **識得分「呢個數量過」同「呢個數估嘅」。** CUI-0020 對住一個冇 export 讀數嘅 shape，明寫 "expected, not measured"。呢種自覺喺效能文檔度好罕有，而且正正係呢張票值錢嘅原因。

3. **抗拒咗喺 CI 度 assert wall clock。** docstring 自己講 "a wall time in CI buys a flaky test rather than a guarantee"。完全正確 —— 呢個誘惑好大，佢忍住咗。

4. **測試守嘅係「限制郁咗要嘈」而唔係「跟住限制飄」。** `assert parents == PARENT_FLOOD_WIDTH` 同 `assert reachable == DEEPEST_REACHABLE_DEPTH` 都係呢個模式。一個純推導嘅測試喺常數改變時會自我調整、永遠綠，等於冇守到。呢個分別佢兩處都揸得啱。

5. **Failure message 指住下一步。** 「re-read MAX_QUERY_DEPTH's docstring ... and the test above, which says where it would」—— 唔止講壞咗，仲講去邊度讀。

6. **CUI-0006 嘅 pin-then-fix 紀律。** 舊測試預先寫低「future `.astimezone()` 會令我紅」，今次就係嗰個 future，於是**改寫 pin 而唔係繞開佢**。呢個係處理「刻意 pin 住已知錯誤行為」嘅教科書做法。

7. **Red 階段分得清 AssertionError 同 collection Error**（CUI-0014），直接對應 `CLAUDE.md` §6 列低嘅陷阱。團隊踩過嘅坑真係入咗腦。

8. **實驗設計嚴謹。** CUI-0017 用「INT 無論幾多位都係一個 token」嚟喺唔改 token 成本下轉換 document 行為，乾淨咁排除咗 parser 先拒絕呢個混淆因子。

---

## 修正優先順序

| 次序 | ID | 嚴重度 | 票 | 位置 | 工作量 |
|---|---|---|---|---|---|
| 1 | W-022 | 🟡 | CUI-0020 | `src/api/schema.py:120` | 一個詞 |
| 2 | W-023 | 🟡 | CUI-0003 | `tests/test_api.py` `_deepest_selection` | 一行斷言 |
| 3 | S-039 | 🟢 | CUI-0017 | `tests/test_api.py` parent-flood 測試 | 一行 |
| 4 | S-040 | 🟢 | CUI-0014 | `tests/test_weather_models.py` | 兩行 |
| 5 | S-041 | 🟢 | CUI-0020 | `src/api/schema.py:104` | 一個字 |
| — | S-038 | 🟢 | CUI-0006 | `src/common/time.py:11` | **建議唔修**（記錄用） |

> 依 `skills/sw-ticket-management` 規範，每個 review item 一個獨立 commit，格式 `fix: W-022 | <描述>`。
> S-041 例外：建議同 W-022 同一個 commit（同一個 docstring、同一段文字、分開改反而製造無謂 diff）。

## 未修訂代碼交代

本報告唔附「修訂後完整代碼」。理由：5 個 finding 全部係一至兩行嘅局部改動（一個詞、一行斷言、一行重置），方案 A/B 已喺各 Section 內列出確切寫法。貼成段重寫代碼只會令 developer 難分邊行先係要改嗰行。Reviewer 亦唔寫 production code。

---

## 結論

| 票 | 分項結果 | 阻塞項 |
|---|---|---|
| CUI-0006 | ✅ pass | — |
| CUI-0017 | ✅ pass | — |
| CUI-0014 | ✅ pass | — |
| CUI-0003 | ⚠️ warn | W-023 |
| CUI-0020 | ⚠️ warn | W-022 |

**整體：⚠️ warn（86/100，0 Critical，2 Warning，4 Suggestion）**

Hard gate 全綠，唯一有行為改動嘅 CUI-0006 經獨立大規模抽驗確認無回歸，**技術上可以 proceed**。兩個 Warning 都係一行修、零回歸風險，建議 developer 收咗先再 QA —— 特別係 W-022，佢係一個**以文檔準確性為唯一目的**嘅 commit 入面留低嘅文檔不準確，唔修返會好諷刺。

CUI-0020 另有一個刻意留開嘅 DoD box（等 CUI-0019），呢張票唔可以標 done，要標 blocked-on CUI-0019。

```
HANDOFF_RECEIPT
protocol: 1
agent: code-reviewer
status: warn
score: 86
critical: 0
warning: 2
suggestion: 4
report: .proj-docs/reviews/2026-09-15_review_low-cost-wave1_batch.md
tickets: CUI-0017, CUI-0020, CUI-0003, CUI-0006, CUI-0014
next_action: invoke-developer
notes: Hard gates 全綠、CUI-0006 byte-identical 經 429,078 個真實 timestamp 獨立抽驗確認零分歧；W-022（CUI-0020 dangling 16x 引用）同 W-023（CUI-0003 深度 guard 對 union/interface 盲點）各一行可修，CUI-0020 另有 DoD box blocked-on CUI-0019。
```
