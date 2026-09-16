# Batch QA — 2026-09-16 — low-cost wave 1

| 項目 | 內容 |
|---|---|
| 測試員 | quality-assurance（獨立，未參與本批代碼撰寫） |
| 類型 | **Batch QA**（5 張已 merge 嘅票，一次過驗） |
| 涵蓋票 | CUI-0006、CUI-0017、CUI-0020、CUI-0003、CUI-0014 |
| Base | `e827d95` |
| HEAD | `a0de0cd` |
| Branch | `claude/ai-dev-team-start-05jie2` |
| 上游 review | `2026-09-15_review_low-cost-wave1_batch.md`（86, warn）→ `2026-09-16_review_low-cost-wave1_delta.md`（**97, pass**，0 Critical / 0 Warning / 3 Suggestion） |
| 環境 | Python 3.11.x、`.venv`、SQLite 3.4x、真實 `data/raw/garmin`（365 × tcx/gpx/kml）|
| 角色定位 | **行為驗證同回歸**。Reviewer 已做代碼審查同 finding 驗證，本報告唔重複，改為由**產出物層面**獨立確認 |

## 整體 verdict

**✅ pass** — 5 張票全部通過行為驗證，hard gates 全綠，0 Critical。

本批只有 **CUI-0006** 有 production 行為改動，其餘四張係 docstring / 測試。
我用 AST 比對獨立確認咗呢件事：**41 個 `src/**/*.py` 入面，剝走 docstring 之後
只有 1 個檔案（`src/common/time.py`）嘅可執行 AST 有變**，`api/schema.py` 同
`weather/models.py` 兩個都係純 docstring。

CUI-0006 嘅 blast radius 實測係 **零**：1,087 個 activity record + 328,752 個
track point 全部 byte-identical，`run365.db` 同 370 個 static JSON 除咗
`generated_at` 一個 build timestamp 之外完全一致，13 條真實 GraphQL query
回應同 SDL 全部 byte-identical。

額外收穫：CUI-0020 嘅 **production 規模數字係第一次被獨立重量**（developer 當時
寫明「冇重 build production DB，太貴」）。我 build 咗真 export（134,041 track rows，
同 docstring 寫嘅數字**一個不差**）並喺佢之上重量晒六個 shape，**全部對得上**。

---

## Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| Tests | `.venv/bin/python -m pytest tests -q` | ✅ pass — **363 passed** in 28.52s |
| Lint | `.venv/bin/ruff check src tests` | ✅ pass — All checks passed |
| Format | `.venv/bin/ruff format --check src tests` | ✅ pass — 58 files already formatted |
| SDL sync | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ pass — up to date |
| Coverage | `pytest --cov=src` | ✅ pass — **TOTAL 94.71%**，核心邏輯遠高於 80%（`api/schema.py` 100%、`api/service.py` 99%、`weather/models.py` 100%、`export/*` 100%、`common/time.py` 93.2%）|
| Data integrity | export → SQLite → static JSON → GraphQL 全鏈 base vs HEAD 比對 | ✅ pass — 唯一差異係 `generated_at` build timestamp |
| Security | 本批零新增依賴、零 secret、read-only API 路徑未改 | ✅ pass |
| No Critical | 🔴 Critical = 0 | ✅ pass |

**全部 hard gate 通過。**

---

## Section A — CUI-0006（`parse_datetime` ISO-offset 分支無視 `timezone` 參數）

> **本批唯一有 production 行為改動嘅票，所以佔本報告最大篇幅。**

### A.1 票上「驗證方式」逐項

| 票上要求 | 方法 | 結果 |
|---|---|---|
| 重現 script 三個 timezone 各回唔同 offset 而絕對時刻相同 | 照抄票上 script 跑 | ✅ 見下 |
| 現有測試全綠 | `pytest tests -q` | ✅ 363 passed（票寫 188，係開票時嘅數字）|

```
ISO with offset:
  Asia/Hong_Kong     -> 2021-10-17 06:10:56+08:00   (UTC 2021-10-16 22:10:56+00:00)
  America/New_York   -> 2021-10-16 18:10:56-04:00   (UTC 2021-10-16 22:10:56+00:00)
  UTC                -> 2021-10-16 22:10:56+00:00   (UTC 2021-10-16 22:10:56+00:00)
naive control:
  Asia/Hong_Kong     -> 2021-10-17 06:10:56+08:00
  America/New_York   -> 2021-10-17 06:10:56-04:00
  UTC                -> 2021-10-17 06:10:56+00:00
```

三個 timezone **offset 各自唔同、UTC instant 完全相同** —— 正正係票上寫嘅預期結果。

### A.2 Export byte-identical（產出物層面獨立確認）

Developer 同 reviewer 都驗過，但兩者都係由 parser 內部或 timestamp 層面睇。
我改為**由產出物層面**重做一次，而且刻意繞開 `_activities_to_jsonl`
（佢會 `pop("track_points")`，只驗佢等於冇驗過 track point）。

做法：由 `e827d95` 同 `HEAD` 各 `git archive` 一份 `src/` 落 scratchpad，
喺同一份真實 `data/raw/garmin` 上面各跑一次三個 parser，
每個 activity **連同佢全部 track point** dump 成 JSON 再 sha256。
（`RUN365_DATA_DIR` 指向 scratchpad，所以 repo 嘅 `data/processed/` 由頭到尾冇被寫過。）

| 產出 | activities | track points | base sha256 | head sha256 | |
|---|---|---|---|---|---|
| `activities_kml.jsonl`（shallow）| 357 | — | `ea53a120…` | `ea53a120…` | ✅ |
| `deep_kml.jsonl`（record + **全部** track point）| 357 | 97,004 | `879fc29c…` | `879fc29c…` | ✅ |
| `activities_gpx.jsonl` | 365 | — | `2d152fb1…` | `2d152fb1…` | ✅ |
| `deep_gpx.jsonl` | 365 | 97,004 | `319c931c…` | `319c931c…` | ✅ |
| `activities_tcx.jsonl` | 365 | — | `d3a23471…` | `d3a23471…` | ✅ |
| `deep_tcx.jsonl` | 365 | 134,744 | `0d188693…` | `0d188693…` | ✅ |

`diff -rq` 全部檔案（最大嗰個 27.8 MB）：**byte-for-byte 相同**。
合共 **1,087 個 activity record + 328,752 個 track point**。

### A.3 呢個綠係唔係「空綠」？——兩個對照

一個 byte-identical 結果只有喺個 probe **真係會捉到差異**、而且**個分支真係行過**
嘅前提下先有意義。兩樣都驗咗。

**對照 1 — 改動嘅分支真係被行過。** 我 spy 住 `parse_datetime`，
數每條 input path 喺真實數據入面被行過幾多次：

```
kml: 97,361 calls    gpx: 98,083 calls    tcx: 135,109 calls
branch hits: {'iso_offset': 97,004, 'naive': 1,071, 'utc_z_ms': 232,478}
distinct trailing offsets on the iso_offset branch: {'+08:00': 97,004}
```

改動嗰條分支被行咗 **97,004 次**（全部由 KML 嚟），而且真實數據入面
**只有 `+08:00` 一種 offset** —— 香港冇夏令時間，所以
`+08:00 → astimezone(Asia/Hong_Kong)` wall clock 不變，
零分歧係**有機制解釋**嘅結果，唔係巧合。

**對照 2 — 個 probe 真係會紅（negative control）。** 我喺 scratchpad 複製咗一份
HEAD，將 ISO-offset 分支嘅 `.astimezone(tz)` 改成
`.astimezone(ZoneInfo("America/New_York"))`，再跑同一個 dump：

```
kml  deep_sha256 879fc29c… -> ab4696b0…   *** 變咗 ***
gpx  deep_sha256 319c931c… -> 319c931c…   冇變
tcx  deep_sha256 0d188693… -> 0d188693…   冇變
```

**只有 KML 變** —— 正正係三個格式之中唯一行 ISO-offset 分支嗰個。
即係話 A.2 個綠係量到嘢之後嘅綠。

### A.4 下游：export → SQLite → static JSON → GraphQL

**先講一個結構事實**：`run365-export` **只 parse TCX + GPX**
（`src/cli/export_data.py:52,54`），而 A.3 顯示 TCX / GPX **零次**行過 ISO-offset 分支
（佢哋只出 `Z` 同 naive）。所以 CUI-0006 改嘅嗰條路**根本到唔到 export**。
不過呢個係推論，所以照跑一次實測。

Base 同 HEAD 各行一次完整 `run365-export`：

| 產出 | 結果 |
|---|---|
| `run365.db` 邏輯 dump（136,057 行） | **1 行有差異** |
| 嗰一行 | `INSERT INTO "meta" VALUES('generated_at','2026-09-16T08:03:26');` vs `…08:04:04` |
| 遮走 `generated_at` 之後嘅 dump sha256 | `f82a20e8…` == `f82a20e8…` ✅ |
| static JSON（**370 個檔案**） | 369 個 byte-identical；`meta.json` 只差 `generated_at` |
| `meta.json` 遮走 `generated_at` | `{"counts": {"activities": 365, "warnings": 461, "weather": 363, "weight": 365}, "year": 2021}` 兩邊完全一樣 ✅ |

`generated_at` 係 build 時間戳（兩次 run 相差 38 秒），屬預期嘅 nondeterminism。

**GraphQL 層**：用 base / head 兩套 package + 各自嘅 DB，跑 **13 條真實 query**
（涵蓋 `meta` / `year`（totals・monthly・weekly・dailyDistance・trainingLoad・personalBests）
/ `activities` 全欄位 + `weather` + `warnings` / 40 個 activity 各取 150 點 track /
單一 activity 取 600 點 / `weight` / `weather` / `warnings`）：

- **13 條全部 `status=200`、零 GraphQL error**
- 每條回應嘅 sha256 **base == head**
- `schema.as_str()` 嘅 sha256 **base == head**（`312eea7a…`）

### A.5 邊界探索（自主，唔限於票上寫嘅）

33 個 case，base 同 head 並排跑。**17 個行為改變 —— 全部都係修復嘅本意或者其直接後果**：

| 類別 | 輸入 | base | HEAD | 判斷 |
|---|---|---|---|---|
| UTC offset | `…+00:00` | `06:10:56+00:00`（參數被無視） | `14:10:56+08:00` | ✅ 修復 |
| 負零 | `…-00:00` | **ValueError** | `14:10:56+08:00` | ✅ 新支援 |
| 最大真實 offset（Kiribati）| `…+14:00` | `06:10:56+14:00` | `00:10:56+08:00` | ✅ 修復 |
| 最小 offset（Baker Is.）| `…-12:00` | **ValueError** | `2021-10-18T02:10:56+08:00` | ✅ 新支援 |
| 半小時（印度）| `…+05:30` | `06:10:56+05:30` | `08:40:56+08:00` | ✅ 修復 |
| 45 分鐘（尼泊爾）| `…+05:45` | `06:10:56+05:45` | `08:25:56+08:00` | ✅ 修復 |
| 45 分鐘（Chatham）| `…+12:45` | `06:10:56+12:45` | `01:25:56+08:00` | ✅ 修復 |
| 無冒號 | `…+0800` / `…-0500` | 前者原樣 / 後者 **ValueError** | 兩者都正確轉換 | ✅ |
| 目標 zone | `+08:00` → NY / UTC / Kathmandu | 三個都回 `+08:00` | `-04:00` / `+00:00` / `+05:45` | ✅ 修復 |
| DST 春季前移（US）| `2021-03-14T07:00:00+00:00` → NY | `+00:00` | `03:00:00-04:00` | ✅ EDT 正確 |
| DST 秋季回撥（US，本地時刻 ambiguous）| `2021-11-07T05:30:00+00:00` → NY | `+00:00` | `01:30:00-04:00` | ✅ 正確揀 EDT（輸入係 instant，唔存在 fold 歧義）|
| DST gap（EU）| `2021-03-28T01:30:00+00:00` → London | `+00:00` | `02:30:00+01:00` | ✅ BST 正確 |

**未變嘅（確認冇連累）**：`Z` 帶毫秒、`Z` 唔帶毫秒（仍然照票上刻意保留咁 raise）、
13 位 epoch-ms、naive、空字串、純空白、只有 offset 冇日期、`T+08:00`、
閏秒 `:60`、`+25:00` 出界、前後空白、naive 帶空格 + offset。

**兩個異常類型有變（已判定無影響）**：

| 輸入 | base | HEAD |
|---|---|---|
| `2021-10-17T06:10:56+08:00:30`（offset 帶秒）| `ParserError` | `ValueError` |
| `2021-10-17T06:10:56+080`（三位 offset）| `ParserError` | `ValueError` |

原因：anchored regex 唔 match 呢兩個畸形串，佢哋跌落尾嗰句 `strptime`。
**判定：無影響。** `dateutil.parser.ParserError` 嘅 MRO 係
`['ParserError', 'ValueError', 'Exception', …]`，即係佢**本身就係 ValueError 子類**；
而全部 caller 嘅 except 清單（`parsers/base.py:105,140,207`、`parsers/kml.py:196`）
捉嘅都係 `ValueError`，兩種都照捉。兩個輸入喺真實數據亦不存在。

**Regex fuzz（false positive / false negative）**：

```
pattern: [+-]\d{2}:?\d{2}$
offsetless ISO shapes fuzzed : 3,402   false positives: 0
offset spellings swept       :   240   matched: 240   missed: []
    （± × 00..14 時 × {00,15,30,45} 分 × 有/冇冒號）
instant-preservation checks  :   348 個 (raw, tz) 組合   mismatches: 0
zone-label checks            :    16 個組合              mismatches: 0
```

3,402 個**冇 offset** 嘅 ISO / naive 形狀（包括 `2021-10-17T06:10:56`，
佢個 `-17T06:10:56` 睇落好似 offset）**零誤判** —— 錨定喺字串尾係有效嘅。
240 個合法 offset 拼法**零漏網**。348 個轉換**全部保住絕對時刻**、
**全部用要求嗰個 zone 嘅 offset 標籤**。

**`MissingTimeZoneDataError` 路徑**（CLAUDE.md §6：要三路齊斬先測得到）：

```
sys.modules["tzdata"] = None + zoneinfo.reset_tzpath(to=[]) + ZoneInfo.clear_cache()

MissingTimeZoneDataError MRO: ['MissingTimeZoneDataError', 'RuntimeError', 'Exception', ...]
  iso offset (the changed branch)      -> MissingTimeZoneDataError
  negative offset (newly reachable)    -> MissingTimeZoneDataError
  Z with ms                            -> MissingTimeZoneDataError
  epoch ms                             -> MissingTimeZoneDataError
  naive                                -> MissingTimeZoneDataError
  malformed                            -> MissingTimeZoneDataError
  caught by `except ValueError`?  False   (must be False)
```

✅ **完全冇被新 regex 影響**：`_zone_info(timezone)` 喺任何分支判斷**之前**就行，
所以六種輸入形狀（包括新開嘅負 offset 路徑）一律先撞環境錯誤；
而佢繼承 `RuntimeError` 唔繼承 `ValueError`，所以照樣食唔到 per-file 嘅
`except ValueError`，W-004 同 CLAUDE.md §6 記低嘅意圖維持成立。

### A.6 Coverage

`src/common/time.py` 93.2%。CUI-0006 新加嘅兩行**全部有覆蓋**：

```
L119  elif _ISO_OFFSET_PATTERN.search(rec_time):              COVERED
L126  return dateutil.parser.parse(rec_time).astimezone(tz)   COVERED
```

未覆蓋嘅 3 行（178 / 179 / 182）全部屬於 `activity_time_range()` —— 一個**全 repo 冇 caller**
嘅 function，pre-existing，同本批無關。已開 **CUI-0022** 記錄（見 §新 ticket）。

### Section A verdict：✅ **pass**。建議標 completed。

---

## Section B — CUI-0017（track field budget docstring 準確度）

性質：test-only（文件部分早喺 `2a043f7` / `bc0a7ca` 完成）。
**驗證重點：新測試真係守住 208 vs 128 嘅聲稱。**

### B.1 數字獨立重量

用 repo 自己嘅 fixture 量（唔用 developer 嘅 script）：

```
MAX_QUERY_TOKENS                     = 1000
widest aliased-parent flood          = 52   (PARENT_FLOOD_WIDTH=52)
MAX_TRACK_FIELDS_PER_REQUEST         = 64
SQL_PER_TRACK_BATCH                  = 2
SQL_PER_ACTIVITY_FIELD               = 2
field cap alone bounds               = 128   <- docstring 寫嘅 128
derived request ceiling              = 52*2 + 128 = 232
ACTUAL statements issued             = 208   <- 票上寫嘅 208
measured > field cap alone?            True
measured <= derived ceiling?           True
```

**208 同 128 兩個數字都一模一樣重現。**

### B.2 個測試係咪 load-bearing？——三個 mutant

一條永遠綠嘅測試等於冇測試。我用 `monkeypatch` 造三個回歸，
睇吓佢會唔會紅同埋紅得啱唔啱：

| Mutant | 模擬嘅回歸 | 結果 |
|---|---|---|
| 0（對照，不改） | — | GREEN ✅ |
| 1：`MAX_QUERY_TOKENS` 1000 → 600 | 有人郁 token 限制，令下面個 ceiling 過期 | **RED**：`the token limit moved; so does the ceiling below` |
| 2：field cap 64 → 128 | cap 大到唔再 under-count 一個 request，docstring 嗰句變假 | **RED**：`the cap alone would under-count this request` |
| 3：`SQL_PER_ACTIVITY_FIELD` 2 → 0 | 將來有人 batch 走 parent lookup，令 cap 重新變成 request 上限 | **RED**：`assert 208 <= 128` |

三個 mutant 全部紅，而且 failure message **直接講中係邊個假設崩咗**。
呢條測試真係釘住緊 docstring 嗰句「the cap is not a bound on the statements a request issues」。

### B.3 Edge case gap 評估

**低風險（test-only，零 production 改動）**，但唔算冇 gap：本測試只量
distinct-parent 形狀。`tickets.md`（2026-09-15 補充）已記低 same-activity 形狀
喺 AU-050 之後係 **106** 而唔係 208，而 `test_the_documented_worst_cases_still_measure_as_documented`
已經覆蓋咗嗰半。兩者合起嚟完整，**無需補測**。

### Section B verdict：✅ **pass**。建議標 completed。

---

## Section C — CUI-0020（wall-clock 量度規模）

性質：docs-only。**驗證重點：數字同 QA 報告對得上；呢張票唔可以標 completed。**

### C.1 每個數字 traceback 去來源

`src/api/schema.py` 嘅 `MAX_TRACK_FIELDS_PER_REQUEST` docstring 引嘅每個數字，
我都去 `.proj-docs/qa/2026-09-15_qa_cui0016-au048-au050_batch.md` 對過：

| docstring 寫 | QA 報告出處 | ✓ |
|---|---|---|
| 134,041 track rows | line 9 / 69 / 404 / 499 / 571 | ✅ |
| `223.4`、「rounded down here and up to 224x in QA's report」 | line 404 / 571 寫 **224×** | ✅ 兩個 spelling 都準，和解說明正確 |
| 10.4 ms / 165.6 ms，16x gap | line 410 / 582 | ✅ |
| 20.8 ms / 21.7 ms | line 334 / 412 / 584 | ✅ |
| 68.7 ms / 78.2 ms | line 335 / 413 / 585 | ✅ |
| 141.4 ms / 194.8 ms | line 414 / 586 | ✅ |
| 390 ms → 38x inside 15 s | line 321 / 393 / 519 / 529 / 588 | ✅ |
| `limit:65` export 未量 | QA 報告 line 411 / 583 嗰格的確係 `—` | ✅ 誠實 |
| 166 aliased `activities` ~2 s，export 未量 | fixture 實測，QA 報告無 production 欄 | ✅ 誠實 |

**134,041 由我自己 build 嘅 export 直接核實**：

```
activities      365
track_points    134,041      <- 同 docstring 一模一樣
weight          365
```

`134041 / 600 = 223.4017` → 截尾 223、四捨五入 224。S-041 嘅和解句正確。

### C.2 Production 數字第一次被獨立重量

Developer 喺票上寫明「⚠️ **冇重 build production DB**（134,041 row，太貴），
production 數字全部引用 QA 報告」。我 build 咗，所以呢批數字而家有第二次獨立量度。
（9 次 median，本機）

| Shape | fixture：doc / 我 | export：doc / 我 | 我量到嘅 ratio |
|---|---|---|---|
| `activities(limit:64){track(points:1)}` | 10.4 / **9.6** ms | 165.6 / **149.5** ms | **15.6x** |
| `activities(limit:65)`（被拒）| 10.8 / **9.2** ms | 未量 / **145.5** ms | 15.8x |
| 64 aliased track，一個 parent | 20.8 / **20.8** ms | 21.7 / **20.2** ms | **1.0x** |
| 52 parents，同一 activity | 68.7 / **68.5** ms | 78.2 / **70.7** ms | **1.0x** |
| 52 parents，52 條唔同 | 141.4 / **134.3** ms | 194.8 / **170.5** ms | **1.3x** |

**每個 claim 都成立：**

- docstring 寫「a 16x gap」→ 我量到 **15.6x** ✅
- docstring 寫「~0.02 s at either scale」→ 我量到 20.8 / 20.2 ms ✅
- docstring 寫「Neither moved by as much as 1.4x between the two scales」
  （指兩個 52-parent shape）→ 我量到 **1.0x 同 1.3x**，都細過 1.4x ✅
- docstring 寫 `limit:65` 喺 export「unmeasured … bounded there by the served case beside it」
  → 我量到 145.5 ms < 149.5 ms（served case），**呢個 bound 實測成立** ✅

**Vercel 餘裕（390 ms → 38x）**：呢個數字唔係上面五個 shape 任何一個，
而係 QA 報告 line 321 / 519 嘅 W-020 worst case
`activities(limit:64){track(points:156)}`。我照樣量埋：

```
served: True   points returned: 9,984 across 64 activities
W-020 worst case at EXPORT scale: median 404.8 ms (min 376.3, max 431.2)
  QA 2026-09-15 measured: 390.2 ms  ->  38x inside 15 s
  this machine:           404.8 ms  ->  37x inside 15 s
```

**38x vs 37x —— 同一個量級，claim 成立而且係保守嗰邊。**

### C.3 DoD 核實

| DoD | 狀態 | 核實 |
|---|---|---|
| docstring 引嘅每個 wall clock 都標明係邊個規模 | ✅ 已剔 | 逐句讀過：每個數字都帶 `on the fixture` / `on the export` 或者明寫「unmeasured」。C.1 逐項對過 |
| Vercel 餘裕用 production 數字重算 | ✅ 已剔 | 舊「60x … of this fixture」已刪，改成由 export 讀嘅 390 ms → 38x。C.2 重量到 37x |
| 同 CUI-0019 一齊重量 | ❌ **未做，blocked on CUI-0019** | CUI-0019 仍喺 `.tickets/pending/`，batch predicate 未郁 |

### C.4 狀態判斷（⚠️ 本節係 main agent 要跟嘅結論）

**CUI-0020 唔可以標 completed。** 理由：

1. 第三個 DoD 未達成，而且**唔係疏忽而係刻意**：CUI-0019 未修，predicate 未郁，
   而家重量只會令數字第二次過期。票上同 review 都核實過呢個判斷合理。
2. 其餘 DoD 已 100% 達成，代碼層面零遺留（W-022 / S-041 已清）。

**我嘅建議：留喺 `.tickets/in-progress/0001-0200/`，標 blocked on CUI-0019，
唔好搬去 `on-hold/`。** 三個理由：

- 佢實質做完 95%，只欠一個等外部 ticket 嘅重量動作。搬去 `on-hold/`
  會令一張幾乎完成嘅票睇落似停擺，下次 triage 容易被跳過。
- 佢嘅 blocker 有明確、已存在、已編號嘅解除條件（CUI-0019 收工），
  唔係「等產品決定」嗰種無限期 hold。
- 檔案入面 line 4-6 同 line 50-53 已經寫得好清楚，狀態歷史亦有 2026-09-16 一行，
  留喺 `in-progress/` 唔會有人誤會佢做緊。

**建議動作**：CUI-0019 完成嗰陣，由嗰張票嘅 owner 順手重量呢六個 shape
並剔最後一個 box，然後先至將 CUI-0020 標 completed。

### Section C verdict：✅ **pass**（代碼同文件層面），但 **ticket 保持 blocked，唔標 completed**。

---

## Section D — CUI-0003（depth limiter 邊界 + type graph guard）

性質：test + docstring。

### D.1 票上「驗證方式」逐項

| 票上要求 | 方法 | 結果 |
|---|---|---|
| 新測試喺 `max_depth` 兩邊各斷言一次 | 獨立掃 `max_depth` ∈ {3,4,5,6} | ✅ 見下 |
| 會喺有人加深 type graph 時失敗 | SDL 注入 mutant | ✅ 見 D.2 |
| 測試全綠 | `pytest tests -q` | ✅ 363 passed |

**邊界喺邊（獨立於 repo 測試自己行一次）**：

```
DEEPEST_REACHABLE_DEPTH = 4,  MAX_QUERY_DEPTH = 5
DEEPEST_CLIENT_QUERY = { year { personalBests { longest { weather { description } track { sec } } } } }

  max_depth=3: REFUSED  'anonymous' exceeds maximum operation depth of 3
  max_depth=4: served
  max_depth=5: served
  max_depth=6: served
```

邊界啱啱好喺 3 / 4 之間 —— 同票上「重現步驟」量到嘅
`max_depth=3: d4=REJECT` / `max_depth=4: d4=pass` 完全一致。

**而且 §A.4 嘅真實 GraphQL probe 獨立佐證咗 docstring 嗰句
「this limiter cannot fire against the schema as it stands」**：
我喺真 export 上送 `{ year { personalBests { longest { track(points: 2) { sec } } } } }`
（我能夠砌到最深嘅 document），**served，零 error**。

### D.2 Guard 係咪 load-bearing？——三個 mutant

`monkeypatch` 住 `schema.as_str` 喺 SDL 尾注入形狀（**完全冇改 repo 檔案**）：

| Mutant | 注入 | 結果 |
|---|---|---|
| 0（對照）| — | GREEN ✅ |
| 1：type graph 深多一層 | `type Deeper { leaf: String }` + `extend type TrackPoint { deeper: Deeper }` | **RED**：`the type graph changed depth; re-read MAX_QUERY_DEPTH's docstring…` |
| 2：SDL 生出 union | `union Anything = Meta \| Activity` | **RED**：`the SDL grew an abstract type; _deepest_selection only walks object types…` |
| 3：SDL 生出 interface | `interface Nameable { id: ID! }` | **RED**：同上 |

Mutant 1 就係票上「風險」嗰段講嘅場景（有人加一層 nested field，
令 limiter 由 unreachable 靜靜變成 reachable）—— **確認接得住**。
Mutant 2 / 3 係 reviewer W-023 補嘅 abstract-type guard，**確認兩種形狀都捉到**。

### D.3 Edge case gap 評估

**冇 gap。** GraphQL 嘅 composite output type 得三種（object / interface / union），
`_unwrap` 已剝走 list / non-null，enum 同 scalar 做 leaf 正確。三個 mutant
覆蓋晒 `_deepest_selection` 唔識行嘅全部形狀。**無需補測。**

### Section D verdict：✅ **pass**。建議標 completed。

---

## Section E — CUI-0014（缺失 string 欄位讀成 `""`）

性質：docstring + test，**零行為改動**。

### E.1 票上「驗證方式」逐項

| 票上要求 | 方法 | 結果 |
|---|---|---|
| 加測試覆蓋「string 欄位缺失」 | `pytest -k MissingStringColumns` | ✅ 2 passed |
| 現有測試全綠 | `pytest tests -q` | ✅ 363 passed（票寫 283，開票時數字）|
| static JSON 仍然 byte-identical | §A.4 全鏈比對 | ✅ 370 個檔案，只有 `meta.json` 嘅 `generated_at` 有差 |

### E.2 行為實測

```
_MISSING_TEXT = ''

HourlyWeather  缺 Time/Description  -> time='', description=''      (float readings -> None)
WeatherWarning 五個 text 全缺       -> warning_type='', warning_signal='',
                                       start_time='', end_time='', icon_url=''
DailyWeather   缺 readings          -> 六個全部 None（冇 string 欄位可 default）

HourlyWeather   缺 Date -> KeyError('Date')   (fail fast，如 docstring 所寫)
WeatherWarning  缺 Date -> KeyError('Date')
DailyWeather    缺 Date -> KeyError('Date')

string-ness held? True    (全部係真 str，唔係 None 亦唔係其他型)
```

四個 docstring claim **逐條對上**：readings 降級成 `""`、annotation 保持誠實嘅 `str`、
key column（`date`）fail fast、`DailyWeather` 唔會出現同類情況。

### E.3 測試係咪 load-bearing？——六個 mutant

`monkeypatch` `_MISSING_TEXT` 成三個值 × 兩條測試：

| `_MISSING_TEXT` | hourly 測試 | warning 測試 |
|---|---|---|
| `None`（即 pre-CUI-0011 行為）| **RED** `assert None == ''` | **RED** `assert None == ''` |
| `'not-empty'` | **RED** `assert 'not-empty' == ''` | **RED** |
| `0`（非 str）| **RED** `assert 0 == ''` | **RED** |

六個全紅。特別係 `0` 呢個 —— 佢係 reviewer S-040 加嘅 `isinstance` 檢查
先捉得到嘅類型（舊寫法 `is not None` 喺 `== ""` 之後恆真）。

### E.4 docstring 引嘅「461 行」核實

`WeatherWarning.from_raw_row` docstring 寫「all 461 committed warning rows were found
to carry every column」。直接掃真實數據：

```
warnings: 461 rows
  Date            absent=0  empty=0  null=0
  Type            absent=0  empty=0  null=0
  Warning_Signal  absent=0  empty=0  null=0
  Start_Time      absent=0  empty=0  null=0
  End_Time        absent=0  empty=0  null=0
  Ico             absent=0  empty=0  null=0

hourly: 17,984 rows
  Date / Time / Description  absent=0  empty=0
```

**461 行、六個欄位、零缺失** —— docstring 嘅依據完全準確。
即係話呢個 default **喺 production 不可達**，票上「實務上不可達嘅 case」嘅判斷成立，
亦解釋咗點解「接受現狀 + 文件化 + 測試釘死」係啱嘅取捨（唔使將五個欄位放寬成 `str | None`）。

### E.5 Edge case gap 評估

**無 gap，唔需要補測。** 零行為改動、default 喺真實數據不可達、
兩條測試經六個 mutant 證實 load-bearing、`date` 嘅 fail-fast 三個 class 都實測過。
（依 batch 模式規則，低風險 fix 喺報告解釋並標 ✅ no edge case gap，唔當 P1 violation。）

### Section E verdict：✅ **pass**。建議標 completed。

---

## 跨 fix 互動評估（batch 模式必做）

| 檢查 | 方法 | 結果 |
|---|---|---|
| **同檔案唔同 function 改動** | CUI-0017 / 0020 / 0003 三張票同時改 `src/api/schema.py` 同 `tests/test_api.py` | ✅ `ast.parse` 兩個檔案都 clean；三段 docstring 改動分屬 `MAX_TRACK_FIELDS_PER_REQUEST`（0017・0020）同 `MAX_QUERY_DEPTH`（0003），互不重疊 |
| **Symbol collision** | AST 掃六個受影響檔案嘅 top-level 符號 | ✅ `test_api.py` 155 個、`schema.py` 43 個、其餘四個 11–24 個，**duplicates=none**（全部檔案）|
| **重複 test id** | `pytest --collect-only` 之後 `sort \| uniq -d` | ✅ 零重複 |
| **Import / global 次序** | 三個 src 檔案 base vs HEAD AST 比對 | ✅ 41 個 `src/**/*.py` 入面只有 `common/time.py` 嘅可執行 AST 有變；`schema.py` / `weather/models.py` 純 docstring |
| **CUI-0006 × CUI-0020 有冇互相污染量度** | export 只 parse TCX+GPX，而 TCX/GPX 零次行 ISO-offset 分支 | ✅ CUI-0006 改嘅分支**到唔到** export / DB / GraphQL；C.2 嘅 wall clock 唔可能被佢影響 |
| **CUI-0006 × CUI-0014 都掂 export** | 兩者都可能改 `run365.db` / static JSON | ✅ 全鏈比對只差 `generated_at`；CUI-0014 零行為改動、CUI-0006 到唔到 export |
| **Batch 整體 coverage 仍達標** | `pytest --cov=src` | ✅ TOTAL 94.71%；核心模組見 Hard Gates 表，全部 ≥ 80% |
| **Batch 整體改動量** | `git diff --stat e827d95..HEAD` | 15 個檔案；`src/` 3 個、`tests/` 3 個、其餘係 `.proj-docs/` 同 `.tickets/` |

**結論：零跨 fix 互動風險。** 三張票共用 `test_api.py` 但改動點相隔幾百行且無共用符號；
唯一有行為改動嗰張（CUI-0006）嘅 blast radius 實測到唔到其他三張票量度嘅路徑。

---

## 回歸

| 項目 | 結果 |
|---|---|
| `pytest tests -q` | ✅ **363 passed** in 28.52s（同 review delta 報告一致）|
| `ruff check src tests` | ✅ All checks passed |
| `ruff format --check src tests` | ✅ 58 files already formatted |
| `run365-schema --check frontend/schema.graphql` | ✅ up to date |

### 前端

| 檢查 | 結果 |
|---|---|
| `git diff --stat e827d95..HEAD -- frontend/` | ✅ **零改動** |
| SDL：base vs HEAD（`schema.as_str()`）| ✅ sha256 同為 `312eea7a…` |
| SDL：HEAD vs 已 commit 嘅 `frontend/schema.graphql` | ✅ 唯一差異係檔尾 newline（commit 版有、生成版無），`run365-schema --check` 認為 up to date |
| static JSON 介面（`activities.json` / `meta.json`）| ✅ base vs HEAD 一致；`activities.json` 365 條、17 個欄位；`meta.json` 除 `generated_at` 外一致 |
| 前端消費點 | `frontend/src/data/static/source.ts:43,48` 只讀 `activities.json` / `meta.json`，兩者 shape 未變 |

**結論：零 SDL drift、零 static JSON shape drift。** 本批唔需要重新 codegen。

### Coverage

```
TOTAL                        1606    85    95%      (94.71%)

src/api/schema.py             286     0   100%
src/api/service.py            103     1    99%   257
src/weather/models.py          76     0   100%
src/export/records.py          56     0   100%
src/export/sqlite.py           30     0   100%
src/export/static_json.py      24     0   100%
src/common/time.py             44     3    93%   178-182
```

**⚠️ 一個同交代要求唔同嘅發現（詳見 CUI-0023）**

任務交代話「CUI-0004 提過 `service.py:150` 係唯一 uncovered line，嗰張票未做，
所以佢應該仍然 uncovered」。**實測係：數目啱，但唔係同一行。**

| 項目 | CUI-0004 寫嘅 | 2026-09-16 實測 |
|---|---|---|
| `service.py` statement 數 | 84 | **103**（AU-050 `2a043f7` 令佢變大）|
| 未覆蓋 line 數 | 1 | 1 ✅ |
| 係邊行 | `150`：`return [total - 1]` | **`257`：`return found`** |
| `return [total - 1]` 而家 | — | 喺 **L152，已經有覆蓋** |

覆蓋佢嘅係 AU-047 C-001 嗰批用 `CHEAP_TRACK = "track(points: 1)"`
（`tests/test_api.py:750`）嘅測試 —— `points < 2` 呢條早返 branch 被順帶行到。

新嗰條未覆蓋 line 係 `tracks()` 嘅空輸入護欄（`if not wanted: return found`），
由 GraphQL 入唔到，性質係 defensive guard 而唔係 bug。

**冇人意外郁過嘢** —— 呢個係 AU-050 擴大檔案 + AU-047 新增測試嘅自然結果，
唔係本批引入。但 CUI-0004 個 title 同依據已經過期，執票嗰陣會被誤導，
所以開咗 **CUI-0023** 記低更正。**CUI-0004 嘅核心 bug 仍然有效**，實測：

```
_even_positions(total=600, points=1) = [599]      <- 只有 last，docstring 講「first and last」
_even_positions(total=600, points=2) = [0, 599]
```

---

## 問題與新 Ticket

本批 **0 Critical / 0 Warning**，5 張票全部通過。以下兩條係量度過程中喺鄰接範圍
撞到嘅 pre-existing 事項，**唔阻塞本批 proceed**：

| ID | 嚴重度 | 內容 | 位置 | 阻塞？ |
|---|---|---|---|---|
| **CUI-0022** | 🔵 Low | `activity_time_range()` 係 dead code（全 repo 零 caller），亦係 `common/time.py` 唯一未覆蓋嘅代碼（L178-182），令 93.2% 呢個 coverage 訊號被溝淡 | `src/common/time.py:167` | ❌ 否 |
| **CUI-0023** | 🔵 Low | CUI-0004 引嘅 coverage 依據已過期：`service.py:150` 而家有覆蓋，唯一未覆蓋嘅係 L257；照票去做會寫一條重複現有覆蓋嘅測試 | `.tickets/pending/0001-0200/CUI-0004.md` | ❌ 否 |

> 編號依 `skills/sw-ticket-management`：掃過 `.tickets/pending/`、`.tickets/in-progress/`
> 同 `.proj-docs/tickets.md`，現存最高係 **CUI-0021**，所以由 **CUI-0022** 起，無重用無跳號。
> 兩個檔案已放喺 `.tickets/pending/0001-0200/`。

兩條都係**低風險、低優先**，可以併入下一個掂到對應檔案嘅 lane，唔值得單開。

---

## Ticket 狀態建議（main agent 執行）

| 票 | Verdict | 建議狀態 | 理由 |
|---|---|---|---|
| **CUI-0006** | ✅ pass | **標 completed** → `.tickets/completed/` | 票上驗證方式全部通過；export byte-identical 由產出物層面 + negative control 雙重確認；33 個邊界 case 全部符合預期；`MissingTimeZoneDataError` 路徑未受影響 |
| **CUI-0017** | ✅ pass | **標 completed** | 208 / 128 兩個數字獨立重現；三個 mutant 證實測試 load-bearing |
| **CUI-0003** | ✅ pass | **標 completed** | 3/4 邊界獨立重現；三個 mutant（加深一層 / union / interface）全部接得住 |
| **CUI-0014** | ✅ pass | **標 completed** | 行為同四條 docstring claim 逐條對上；六個 mutant 全紅；461 行依據實測準確 |
| **CUI-0020** | ✅ pass（代碼/文件）| ⛔ **唔可以標 completed**。建議**留喺 `.tickets/in-progress/0001-0200/`，標 blocked on CUI-0019** | 第三個 DoD（同 CUI-0019 一齊重量）刻意留開，CUI-0019 仍喺 `pending/`、batch predicate 未郁。**唔建議搬 `on-hold/`**，理由見 §C.4。CUI-0019 收工時由嗰張票 owner 順手重量並剔 box，然後先標 completed |

> QA 依規範**冇執行任何 git 操作**，亦**冇 `git mv` 任何 ticket 檔案**。
> 以上由 main agent 按 receipt 執行。

---

## 驗證用臨時改動聲明

**repo 零改動。** 全部 probe 都喺 session scratchpad 進行：

- base / HEAD 兩份 `src/` 由 `git archive` 抽出，**冇用 `git worktree`**（唔會寫 `.git/`）
- 所有 export 產出寫落 scratchpad（`RUN365_DATA_DIR` 指過去），
  **repo 嘅 `data/processed/` 由頭到尾只有一個 `.gitkeep`，零 build 垃圾**
- mutant（改 timezone 嘅 `time.py`）係 scratchpad 入面 HEAD 嘅副本，**唔係 repo 檔案**
- SDL / `_MISSING_TEXT` / 常數 mutant 全部用 `monkeypatch`，**冇掂過 repo 檔案**
- CUI-0017 / 0003 / 0014 嘅 mutant 測試係 scratchpad 嘅獨立 test module，
  `import` repo 嘅 fixture 而唔改佢
- 量 coverage 曾經喺 `.venv` 裝 `pytest-cov`（`.venv` 係 gitignored，唔入 repo），
  佢跌低嘅 `.coverage` 檔案已刪

**最終狀態確認：**

```
$ git diff --stat src tests
（空）
$ git status --short
?? .tickets/pending/0001-0200/CUI-0022.md
?? .tickets/pending/0001-0200/CUI-0023.md
$ ls data/processed/
.gitkeep
$ .venv/bin/python -m pytest tests -q
363 passed in 28.52s
```

`src/` 同 `tests/` 零改動；`git status` 剩返嘅兩個 untracked 就係本次 QA 建立嘅新 ticket。

---

## 建議補測（由 developer subagent 跟進，QA 不寫代碼）

**本批無強制補測項。** 五張票逐個評估：

| 票 | Edge case gap | 理由 |
|---|---|---|
| CUI-0006 | ✅ 無 gap | 33 個邊界 case + 3,402 個 fuzz + 348 個 instant-preservation check 全部通過；`TestParseDateTimeIsoOffsetIsConvertedToTimezone` 12 個 case 已涵蓋正/負/零 offset × 三個目標 zone |
| CUI-0017 | ✅ 無 gap | distinct-parent 由本測試守，same-activity 由 `test_the_documented_worst_cases_still_measure_as_documented` 守，兩者合起完整 |
| CUI-0020 | ✅ 無 gap | docs-only；wall clock 刻意唔 assert（CI 量 wall time 只買到 flaky test），呢個決定 docstring 已寫明 |
| CUI-0003 | ✅ 無 gap | 三種 composite output type 全部有 guard |
| CUI-0014 | ✅ 無 gap | 零行為改動；default 喺 production 不可達（461 / 17,984 行零缺失）|

Reviewer 留低嘅 **S-042**（`sql_count` fixture-order 不變式仲有 12 個 case 靠緊）
同 **S-043**（重複 build schema）兩條 🟢 我同意**唔阻塞**，
建議併入下一個掂到 `tests/test_api.py` 嘅 lane。

---

## 結論

| 票 | Review delta | 本輪 QA | 阻塞項 |
|---|---|---|---|
| CUI-0006 | ✅ pass | ✅ **pass** | — |
| CUI-0017 | ✅ pass | ✅ **pass** | — |
| CUI-0003 | ✅ pass | ✅ **pass** | — |
| CUI-0014 | ✅ pass | ✅ **pass** | — |
| CUI-0020 | ✅ pass | ✅ **pass**（代碼）| **ticket 仍 blocked on CUI-0019，唔可以標 completed** |

**整體：✅ pass。Hard gates 全綠，0 Critical，363 passed，coverage 94.71%，
export / DB / static JSON / GraphQL 全鏈零行為分歧。可以 merge 去 `master`。**

唯一要 main agent 記住嘅**非代碼**事項同 reviewer 一致：
**CUI-0020 唔可以標 completed**，留 `in-progress/` 標 blocked on CUI-0019。

```
HANDOFF_RECEIPT
protocol: 2
agent: quality-assurance
status: pass
critical: 0
report: .proj-docs/qa/2026-09-16_qa_low-cost-wave1_batch.md
tickets_pass: CUI-0006, CUI-0017, CUI-0003, CUI-0014
tickets_blocked: CUI-0020 — 最後一個 DoD（同 CUI-0019 一齊重量）未達成，CUI-0019 仍喺 pending 未郁 batch predicate；建議留喺 .tickets/in-progress/0001-0200/ 並標 blocked，唔好搬去 on-hold/（其餘 DoD 已全數完成）
new_tickets: CUI-0022（activity_time_range 係 dead code 兼 common/time.py 唯一未覆蓋代碼）, CUI-0023（CUI-0004 引嘅 coverage 依據已過期：service.py:150 而家有覆蓋，唯一未覆蓋嘅係 L257）
next_action: invoke-devops
notes: 5 張票行為驗證全部通過；CUI-0006 由產出物層面獨立確認零分歧（1,087 record + 328,752 track point byte-identical，另有 negative control 證明 probe 會捉到差異），export→SQLite→static JSON→GraphQL 全鏈只差 generated_at；AST 比對確認 41 個 src 檔案只有 common/time.py 有可執行改動；CUI-0020 嘅 production 數字第一次被獨立重量（134,041 track rows 對得上，16x→15.6x、38x→37x）；hard gates 全綠 363 passed / coverage 94.71%；新開兩張 🔵 Low ticket 皆不阻塞。
```
