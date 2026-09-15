# Ticket Registry — Run365Days

**最後更新**：2026-09-14（AU-047 實測 + 修復；更正 Lane E 嘅 `num_points` 假設；分拆 AU-050）

> 由 `/audit`（AU-NNN）同 `/review`（C/W/S-NNN）產生嘅 ticket 集中登記處。
> 編號全局唯一、永不重用。已完成嘅保留紀錄，只改狀態。

---

## AU-NNN — 來自架構審計

來源：[`audits/2026-09-13_22-42_audit_full-codebase.md`](audits/2026-09-13_22-42_audit_full-codebase.md)

### 已完成

| ID | 標題 | 狀態 | Commit |
|---|---|---|---|
| AU-004 | CI 只喺 `develop` 觸發，default branch `master` 零 gate | ✅ Done | `bf7913f` |
| AU-003 | `parse_datetime()` 回傳 LMT `+07:37` | ✅ Done | `7218a84` |
| AU-002 | Parser 靜默失敗鏈 | ⚠️ Partial（見 W-004） | `ea36622` |
| AU-001 | GraphQL 零 query cost 控制 | ⚠️ Partial（見 AU-047） | `991c19b` |

### 新開（P0 修復過程中發現，均由 developer 主動申報並經核實）

| ID | 優先 | 標題 | 來源 |
|---|---|---|---|
| **AU-047** | P1 | `Activity.track` N+1 fan-out 仍未解決 | Lane C 申報，main agent 核實 `src/api/schema.py:137-139` |
| **AU-048** | P1 | epoch-ms path 語義錯 8 小時；測試常數係捏造 | Lane B 申報 → ✅ **Done** `6fcb3cc`（2026-09-15） |
| **AU-049** | P2 | `create_app(..., graphiql: bool = True)` 預設仍然開 | Lane C 申報 |
| **AU-050** | P2 | `{ activities { track } }` 仍然係 2N round-trip（AU-047 加咗 budget 但冇 batch） | ✅ **Done** `2a043f7`（2026-09-15）—— 但見下方 CUI-0019，wall clock 倒退 |

#### AU-047 — `Activity.track` N+1 fan-out

AU-001 修好咗**每次 query 嘅讀取放大**（SQL 層 stride，唔再讀晒 600 點先喺 Python 降採樣），但**冇修 fan-out**：`src/api/schema.py:137-139` 仍然每個 activity 各發一次 query。

- 修復前最壞：365 query × 600 行 ≈ 219,000 個 ORM object
- 修復後最壞：500 query × `DEFAULT_TRACK_POINTS` 行

單次請求資料量降咗一個數量級，但 500 次 query 嘅 round-trip 仍然要喺 15 秒 function 入面行完。

**原建議解法**：Strawberry DataLoader（keyed by `activity_id`），或 field-level complexity extension 去 bound `activities × points` 個乘積。

##### 2026-09-14 實測 —— 推翻「fan-out 係主因」

建一個同真實 export 同形狀嘅 DB（365 activities × 600 stored points，17.1 MB SQLite），逐條 query 用 SQLAlchemy `before_cursor_execute` 數語句：

| Query | SQL 語句數 | 耗時 | Track rows |
|---|---|---|---|
| `activities(limit: 365) { id }` | 2 | 0.01 s | 0 |
| `+ track(points: 5)` | 732 | 0.60 s | 1,825 |
| `+ track(points: 150)`（default） | 732 | 1.81 s | 54,750 |
| `+ track(points: 1000)`（`MAX_TRACK_POINTS`） | **732** | **8.29 s** | **219,000** |
| `activity(id) { track(points: 1000) }`（前端真實路徑） | 4 | 0.03 s | 600 |

第 2 行同第 5 行係同一組 732 條 query、但差 217,175 行：**730 條額外 query 嘅 round-trip 成本只係 0.60 s，8.29 s 入面 93% 係 row materialisation。** 即係話 DataLoader 就算做到完美（732 → 2 條），最壞情況仍然 ~7.7 s，貼住 Vercel 15 秒 function 上限；真正冇 bound 嘅係 `activities × points` 個乘積本身。

**採用方案（2026-09-14 用戶拍板）**：per-request track point budget（`MAX_TRACK_POINTS_PER_REQUEST`），喺 `Activity.track` resolver 按 requested points 扣數，扣爆即拒。直接 bound 最壞情況，零 SQL 取樣語義改動。DataLoader batching 另開 follow-up（見 AU-050）。

**前端零影響**：`frontend/src/data/api/queries.ts` 由頭到尾冇發過 `activities { track }` —— `TrackQuery` 係單 activity。呢條係敵意 query 曝險，唔係 client 路徑。

#### AU-048 — epoch-ms path 語義錯 8 小時（含捏造測試常數）

`src/common/time.py` 嘅 13 位 epoch-ms path 唔止 tzinfo 錯（AU-003 已修），佢**將 UTC 牆鐘時間貼上 HK 標籤**，實質早咗 8 小時。

**決定性證據**：真實 `data/raw/garmin/summarized_activities.json` 有 `"beginTimestamp":1634422256000`，對應香港時間 `2021-10-17 06:10:56+08:00`。而 `tests/test_common_time.py` 用嘅係 `1634451056000` —— 啱啱好大 28,800,000 ms（8 小時）。**測試作者將香港牆鐘時間當成 UTC 編碼，令錯嘅實作睇落係啱。**

**目前風險為零**：三個 parser 全部餵 ISO 字串；`SUMMARIZED_ACTIVITIES_JSON` 喺 `src/common/config.py` 有宣告但零 import（見 AU-036）。呢條 path 係死碼，已 export 嘅數據冇受影響。

**修法**：`datetime.fromtimestamp(ts, tz=ZoneInfo(timezone))`（轉換，唔係貼標籤）＋ 同時改正捏造嘅測試常數為 `1634422256000`。會令該 path 平移 +8 小時。

⚠️ AU-003 現有嘅 `TestParseDateTimeWallClockUnchanged` **刻意釘死緊呢個錯誤行為**，做 AU-048 時必須同步更新該測試。

#### AU-049 — `create_app` 嘅 graphiql 預設值

AU-001 只改咗 Vercel entry point（`api/graphql.py`）。`src/api/app.py` 嘅 `create_app(..., graphiql: bool = True)` 預設仍然係開，任何其他 caller 都會攞到危險預設值。建議翻轉為 `False`，開發環境 opt-in。

---

## C/W/S-NNN — 來自 P0 Batch Review

來源：[`reviews/2026-09-13_review_p0-batch.md`](reviews/2026-09-13_review_p0-batch.md)（84/100，0 Critical，10 Warning）

| ID | 標題 | 位置 | 狀態 |
|---|---|---|---|
| W-001 | `docs/architecture.md` 仍寫 CI 喺 `develop` 觸發；test count 93 應為 133 | `docs/architecture.md:142` | ✅ Done |
| W-002 | concurrency group 為常數 `pages` + `cancel-in-progress`，PR 嘅 CI run 會取消進行中嘅 production 部署 | `.github/workflows/pages.yml:15-18` | ✅ Done |
| W-003 | 釘死錯誤 epoch-ms 語義嘅測試，其「tracked separately」所指 ticket 不存在 | `src/common/time.py:41`、`tests/test_common_time.py:60` | ✅ Done |
| W-004 | `ValueError` 一桶裝「刻意 skip」同「數據錯誤」並記 DEBUG；實測 8/365 真實 KML 被靜默丟棄 | `src/activities/parsers/base.py:124-127`、`kml.py:71` | ✅ Done |
| W-005 | `optional_int` 違反 docstring：`"inf"` raise `OverflowError`，`parse_all` 唔 catch → 整個 export 崩潰 | `src/activities/parsers/base.py:70-73` | ✅ Done |
| W-006 | KML lap `Time`/`Distance` 當 optional，TCX 同義欄位當 required；缺一個 lap 會靜默少計 | `src/activities/parsers/kml.py:152-168` | ✅ Done |
| W-007 | 新增嘅三段分級 `except` 只有 `ET.ParseError` 一段有測試 | `src/activities/parsers/base.py` + 三個 parser | ✅ Done |
| W-008 | docstring 宣稱關 GraphiQL 同時關 introspection；實測仍然回 200 完整 schema | `api/graphql.py:18-20` | ✅ Done |
| W-009 | 四個 list field 靜默截斷於 `limit=500`，無 `totalCount`/`hasNextPage` | `src/api/schema.py:32-38` | ✅ Done |
| W-010 | SQL stride + 二次 downsample 令間距不均，與 "evenly" 描述矛盾 | `src/api/service.py:94-116` | ✅ Done |
| S-001 | `README.md:260` 將 Vercel production branch 寫成已驗證事實 | `README.md:260` | ✅ Done |
| S-006 | TCX 仍計算從未被讀取嘅 `MaxSpeed` lap 欄位 | `src/activities/parsers/tcx.py:107` | 📋 併入 AU-021 |

### Warning 修復結果（2026-09-14）

三條並行 lane 修復全部 10 個 Warning + S-001，merge 後合併驗證：

| 指標 | Review 時 | 修復後 |
|---|---|---|
| Tests | 133 | **185** |
| Coverage TOTAL | 80% | **82%** |
| Parser coverage | base 86 / gpx 91 / kml 91 / tcx 92 | **全部 100%** |
| `src/api/schema.py` | — | 100% |
| 真實數據 export | 365 activities / 11,460 KB | **完全一致** |

實測確認：introspection 真正禁用（`data: False` + 明確 error）；`activitiesCount` 回 365；生產路徑 GraphiQL IDE = 404。

#### W-004 產品決定：維持 skip（方案 b），而且方案 a 根本做唔到

Lane D 逐個檢查咗全部 8 個檔案（`6055376813`、`6055377172`、`6055377549`、`6055377879`、`6055378214`、`6067566088`、`6067566456`、`6130263512`）：每個都有 6 個真實 lap placemark、**零個 `TimeSpan`/`when` 元素** —— Garmin KML 喺 track point 以外冇任何時間戳。

`Activity.date` 係必填，下游 `src/export/records.py:103` 對佢跑 `datetime.strptime`，`:214-216` 再排序同切片。所以方案 (a) 等於**捏造一個開始時間去令一個計數器讀到 365**。

TCX 對同樣 8 個 id 全部解析成功（365/365），而 dashboard 係由 TCX 建出嚟 —— 產品層面冇任何損失，只係 KML-only 嘅 `run365-activities` CLI 少 8 條，而家喺 WARNING 講明原因（`ActivityParseError: no track points in <file>`）而唔再係 DEBUG 級靜默。

---

## 新開 ticket（Warning 修復過程中發現）

| ID | 優先 | 標題 | 來源 |
|---|---|---|---|
| **W-011** | P1 | `workflow_dispatch` 由任何 branch 都會做 production Pages 部署 | ✅ **Done** `12ad224`（隨 AU-004 調整一併關閉） |
| **S-005** | P2 | `src/cli/process_activities.py:55-58` 寫 0 行 JSONL 仍然 exit 0 | Lane D |
| **S-007** | P2 | `_SUMMARY_ROW_COLSPAN` 喺 `kml.py` 身兼三個無關語義（:130 colspan、:132 min cells、:148 min coordinate parts） | Lane D |
| **S-009** | P3 | KML 時間戳被解析兩次 | Lane D |
| **S-011** | P2 | static mode 仍然當 `points: 0` 為「攞全部」，api mode 已拒絕 —— 兩個 data mode 對同一個 `useTrack(id, 0)` 行為不一致 | Lane E |
| **S-012** | P3 | `MAX_TRACK_POINTS` / `MAX_PAGE_SIZE` 邊界只存在於 Python docstring 同 runtime error，冇寫入 SDL description | Lane E |

### W-011 詳情

`.github/workflows/pages.yml` 嘅 build / deploy job 只 gate 喺 `if: github.event_name != 'pull_request'`。W-002 嘅 concurrency 修復令佢**唔會同 master 部署撞**，但冇令佢**變成有意為之** —— 任何側枝嘅 `workflow_dispatch` 仍然會部署上 production。

建議收窄為 `github.event_name == 'push' || github.ref == 'refs/heads/master'`。屬行為改動而非 Warning 修復，故另開 ticket。

### AU-047 補充（Lane E 發現）

除咗 N+1 fan-out，`{ activities { track } }` 而家係 **2N** query —— 每個 activity 一個 COUNT 加一個 SELECT。~~`models.Activity.num_points` 已經存住該數目，可以直接消走個 COUNT~~；徹底解法係 window function。

> ⛔ **2026-09-14 更正：「用 `num_points` 消走 COUNT」呢個假設係錯嘅，唔好照做。**
>
> `src/export/records.py:120` 嘅 `num_points = len(points)` 係 **downsample 之前**嘅原始點數，而 `records.py:230` 存落 DB 嘅係 `downsample(track_rows(...), point_limit)` 之後嘅行數（`run365-export` 預設 `DEFAULT_POINT_LIMIT = 600`，`src/cli/export_data.py:27`）。兩者只喺原始點數 ≤ 600 嗰陣先啱。
>
> **實測 365 個真實 TCX 嘅 `<Trackpoint>` 數**：min 250、median 366、**max 1,250**、**2 個 activity > 600**。即係話照做會令嗰批 activity 嘅 `_even_positions(total, points)` 用一個大過實際行數嘅 `total` 去計位置，downsample 位置全錯而且**唔會拋錯**（`seq IN (...)` 只係撈唔到嘢），係靜默資料錯誤。
>
> 要消個 COUNT 嘅話得兩條路：export 時另存一個 `stored_points` 欄位，或者用 `count(*) OVER (PARTITION BY activity_id)` 喺同一句 SQL 計。兩條都要先過 `test_track_samples_match_the_reference_downsampler`（佢將 SQL 取樣同 `dashboard.builder.downsample` 釘到完全一致）。實測顯示呢個 COUNT 只值成個最壞情況嘅 ~7%，唔值得為咗佢冒語義風險 —— 撥入 AU-050。

### AU-049 補充（實測確認）

W-008 令 flag 喺 `schema.py` 讀取之後，`src/api/app.py:28` 嘅 `create_app(graphiql=True)` 成為**唯一一條可以喺 introspection 關閉之下仍然服務 IDE 嘅路徑**。實測：`create_app(graphiql=False)` GET → 404（生產路徑）；`create_app()` GET → **200**（預設值路徑）。目前潛伏 —— `api/graphql.py` 係唯一 production caller 且傳 `graphiql_enabled()`。

### AU-050 — `{ activities { track } }` 嘅 2N round-trip（AU-047 分拆）

AU-047 用 per-request budget 封住咗最壞情況嘅資料量，但冇改 fan-out 本身：每個 activity 仍然係 1 個 COUNT + 1 個 SELECT。實測值 0.60 s / 730 條額外 query（見 AU-047 實測表），即最壞情況嘅 ~7%。

**點解唔喺 AU-047 一次過做**：唯一乾淨嘅 batch 寫法係 `row_number() OVER (PARTITION BY activity_id ORDER BY seq)` 加 `count(*) OVER (PARTITION BY activity_id)` 一句過；但 `_even_positions()` 嘅 `round(i * step)` 係 Python 層計，搬入 SQL 要保證 SQLite 嘅 rounding 同 Python round-half-even 完全一致，否則撞爆 `test_track_samples_match_the_reference_downsampler`（byte-level 釘死）。風險同工作量都遠高於佢慳嘅 7%。

**做嘅時候順帶**：同一句 window function 可以一併消走 `_even_sample_filter()` 嗰個 COUNT（見上面「AU-047 補充」嘅更正 —— 唔可以用 `num_points` 代替）。

### `ActivitySkipped` 命名（需團隊決定）

Lane D 為 `ActivitySkipped` 加咗 `# noqa: N818`。ruff 嘅 `N818` 要求 exception 以 "Error" 結尾，但 `ActivitySkippedError` 會同個名嘅本意矛盾（佢代表刻意 skip，唔係錯誤）。建議喺 `pyproject.toml` 加 per-file-ignore 取代逐行 noqa，或維持現狀。

---

## CUI-NNNN — 來自 Batch QA

來源：[`qa/2026-09-14_qa_p0-batch.md`](qa/2026-09-14_qa_p0-batch.md)（0 Critical，regression 通過）

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| CUI-0001 | 🟡 Major | 必填數值行裸 `float()` 繞過 W-005；`inf` 令 Pages build 崩潰但 Vercel 照 ship `Infinity` | ✅ **Done** `3723849` `e5b04f1` |
| CUI-0002 | 🟡 Major | 改用 `zoneinfo` 後未宣告 `tzdata`，`pytz` 成死依賴 | ✅ **Done** `9559231` |
| CUI-0003 | 🟢 Minor | depth limit 實際永遠唔會觸發（schema 最深 4 層，limiter 要 `max_depth=3` 先拒） | pending |
| CUI-0004 | 🟢 Minor | `track(points: 1)` 只回最後一點，同 docstring 承諾不符 | pending |
| CUI-0005 | 🟢 Minor | `app.test_client()` 過 ~130 request 洩漏 session（已確認係 test client artifact，真 WSGI server 400/400 正常） | pending |
| **CUI-0006** | 🟢 Low | `parse_datetime` 嘅 ISO-with-offset 分支無視 `timezone` 參數 | pending（新開） |

### CUI-0002 修復摘要

| 決定 | 結果 |
|---|---|
| `tzdata` 宣告方式 | **無條件**，唔加 platform marker —— 曝險唔止 Windows，slim / distroless container（即本 repo 嘅實際部署形態）一樣冇 `/usr/share/zoneinfo`；成本不對稱（幾百 KB vs runtime crash） |
| 錯誤處理 | 方案 (a)：`time.py` 包成 `MissingTimeZoneDataError(RuntimeError)`。**刻意揀 `RuntimeError`** 而唔係 `LookupError` / `ValueError` 子類，令 `parse_all` 任何 except 分支都食唔到，第一個 timestamp 即刻爆 —— 環境錯誤唔應該降格成逐檔靜默 skip |
| `pytz` 實際效果 | 真係會消失。`pandas 3.0.5` 嘅 `Requires` 已經冇 `pytz`（2.x 先有），`pip show pytz` 顯示 `Required-by: run365days` —— 我哋係唯一 requester |

**Main agent 獨立核實**：`MissingTimeZoneDataError` MRO = `RuntimeError → Exception`（確認 `parse_all` 食唔到）；錯誤訊息明確提到 `pip install tzdata`；`pandas 3.0.5 Requires: numpy, python-dateutil`；真實數據 export 仍然 365 activities / 11,460 KB。

### 兩處測試設計陷阱（值得記低）

1. **唔可以用 `PYTHONTZPATH` 寫測試** —— 佢係 `zoneinfo` import 時讀取，喺測試入面改 `os.environ` 唔生效，會寫出一個永遠綠嘅測試。要同時斬三條路：`sys.modules["tzdata"] = None`（斬 PyPI fallback）＋ `zoneinfo.reset_tzpath(to=[])`（斬 host TZPATH）＋ `ZoneInfo.clear_cache()`（斬 module cache）
2. **Red 階段唔好 import 未存在嘅 symbol** —— 會造成 collection Error 而唔係 assertion Fail，睇落似紅其實係壞。先 assert 行為（`pytest.raises(Exception)` + 訊息內容），Green 之後喺 Refactor 階段先收緊做具體 exception type

### CUI-0001 修復摘要

Tests **188 → 209**｜parsers 同 `export/records.py` 全部 **100%** 覆蓋｜static JSON 對比基準 `1896778` **byte-identical**（6,857,102 bytes / 370 files，只差 `generated_at`）— main agent 獨立重跑確認。

#### 兩處推翻咗原本假設嘅發現

**1. Main agent 嘅前提錯咗，developer 實測推翻。** 我當時話「做完方向 1 之後，非有限值喺讀取點已經變成 `ActivityParseError`，`OverflowError` 再傳唔到上 `parse_all`，所以方向 3 係死代碼」。

實測：`pandas` 將 `1.5e308 + 1.5e308` reduce 成 `inf`（只有一個 `RuntimeWarning`），所以**兩個有限嘅 lap time 相加仍然可以係 inf**，跟住 `seconds_to_hhmmss()` 入面嘅 `int(inf)` 照樣掟 `OverflowError`。逐項讀取防護**唔足以**封住 abort path。

Developer 冇因為我講咗就照跟，而係喺**正確位置**補上 `ensure_finite()`（TCX total time / total distance、KML total distance），令每個交俾 `int()` / `timedelta()` 嘅值喺使用點都已證明有限。做完之後方向 3 先至真係變成死代碼 —— 佢最終同意唔加，但理由係佢自己驗證出嚟嗰個，唔係我嗰個。

**2. Ticket 方向 2 照字面做會製造一個新分歧。** Ticket 話將 ±inf 映射到 `None`。但 `activities.distance_km` 同 `duration_sec` 喺 SQLite schema 係 **NOT NULL** —— `None` 係 static writer 收得但 SQLite 拒絕嘅值，即係製造咗一個新嘅 writer 分歧。改為降級成數值（distance 經 `distance_by_coord_km` fallback 到 `0.0`，duration 到 `0`），nullable 欄位維持 `None`。已用 `test_not_null_columns_stay_numeric_when_the_source_value_is_not_finite` 釘住。

#### CUI-0002 性質嘅回歸防護

Developer 用三種方式驗證 `MissingTimeZoneDataError` 仍然逃得出 `parse_all`，其中最有價值嗰個係新增測試 `test_should_not_catch_an_environment_error_raised_from_a_parser` —— 佢 monkeypatch `parse` 掟一個**裸 `RuntimeError`**，斷言佢逃得出。咁樣係針對 base class 而唔係針對 tzdata 本身，所以將來有人擴闊 except 清單就一定會撞爆一條測試。

#### 硬化範圍

| 檔案 | 改動 |
|---|---|
| `base.py` | 新增 `ensure_finite()`、`parse_finite_float()`、`required_float()` |
| `tcx.py:106-107` | `TotalTimeSeconds` / `DistanceMeters` 改用 `required_float()` |
| `tcx.py:84-90` | 加總後嘅 total time / total distance 加 `ensure_finite()` |
| `gpx.py:124-125` | trkpt `lat` / `lon` 改用 `parse_finite_float()` |
| `kml.py:160-161` | 座標三元組嘅 lat / lon 改用 `parse_finite_float()` |
| `kml.py:201,205` | 每個 lap Distance 同加總後嘅 total distance 加 `ensure_finite()` |
| `records.py` | `_round()` 由 `value != value` 改 `math.isfinite()` |

---

## 再新開（CUI-0001 修復過程發現）

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0007** | 🟡 High | `dashboard/builder.py:22-31` 嘅 `_num()` 有同 CUI-0001 一模一樣嘅 ±inf 缺口 | ✅ **Done** `3d83e71` |
| **CUI-0008** | 🟢 Low | `haversine_distance` 對非有限座標只出 `RuntimeWarning` 唔拒絕 | pending |

**CUI-0007 值得留意**：developer 為咗避開呢個缺口，**要削弱自己嗰個 writer parity 測試**。一個測試要繞路先過到，本身就係被繞開嗰度有嘢未修嘅證據。修佢嗰陣順帶處理 AU-024（`_num()` 同 `_round()` 本來就係重複 helper，應收歸 `src/common/numeric.py`），並將 parity 測試恢復到未削弱嘅版本。

### CUI-0007 修復摘要

Tests **209 → 215**｜TOTAL 覆蓋率 83% → **84%**｜`src/common/numeric.py` 新增 **100%**｜`dashboard/builder.py` 86% → 91%｜static JSON 對比基準 `1896778` 仍然 **byte-identical**（6,857,102 bytes / 370 files，只差 `generated_at`）— main agent 獨立重跑確認，並抽樣 `tracks/6055376813.json` 逐 byte 相同。

#### 順帶收歸咗 AU-024（部分）

Developer 揀咗收歸而唔係只修 `_num()`，論證：**重複本身就係呢個 bug 嘅成因** —— `_num()` 同 `_round()` 之所以對 ±inf 有兩套答案，正正因為佢哋係兩份獨立實作；只修一邊等於留返個機制俾佢再次漂移。

新 `src/common/numeric.py` 提供 `finite()` / `round_or_none()` / `to_float()`，取代七處：

| 原本 | 位置 |
|---|---|
| `_num` / `_to_float` | `dashboard/builder.py` |
| `_finite` / `_round` / `_to_float` | `export/records.py` |
| `_safe` | `weather/collectors/hko_daily.py` |
| `_safe_float` | `weather/collectors/hourly.py` |

**AU-024 剩餘部分**：`activities/metrics.py:79 _hhmmss_to_sec()` 同 `common/time.py:47 hhmmss_to_seconds()` 仍然逐字重複 —— 呢條同 AU-023（`metrics.py` 整個 module 冇 production 消費者）綁埋，一齊處理較合理。

#### 兩個 developer 主動識別嘅型別陷阱

**1. `round_or_none()` 保留 `float()` cast。** `records._round` 有 cast 而 `builder._num` 冇 —— 統一時揀咗有 cast 嗰個，因為對應嘅 SQLite column 係 REAL，整數輸入唔 cast 會令 JSON writer 寫 `330` 而 DB 存 `330.0`，**即係同一類 writer 分歧**。

**2. `cadence` 用 `finite()` 而唔用 `round_or_none()`。** `cadence` 係 INTEGER column 而唔係 REAL，用 `round_or_none()` 會令 int `83` 變 `83.0`，靜態 JSON 寫 `83.0` 而 DB Integer 存 `83` —— 又係製造新分歧。`finite()` 對有限 int 係 identity。

Main agent 已核實：export 出嚟嘅 `cadence` 樣本 `[85, 86, 87, 86, 87]` 全部係 `int`。

#### `common/` 純度守住咗

`src/common/numeric.py` 只 import `math`。三重驗證：載入後 `sys.modules` 唔含 pandas / numpy / lxml / bs4 / requests（關鍵係 `common/__init__.py` 得一行 docstring，唔會連帶拉 `common/geo.py`，後者 import numpy）；`tests/test_api_imports.py` 1 passed；全 suite 215 passed。Main agent 獨立重驗 `heavy: none`。

#### Parity 測試已恢復（覆蓋率 0/8 → 8/8）

| | 改前 | 改後 |
|---|---|---|
| `_poisoned_records()` | 只污染 activity summary 四個欄位，**track point 一個都冇掂** | 三個 track point 帶 `lat=+inf` / `lon=-inf` / `elevation=+inf` / `temperature=nan` / `cadence=+inf` / `speed=-inf` / `distance_m=nan`；GPX twin 用同一批 timestamp 令 `merge_temperature` 真係將 nan 溫度餵入 track row |
| Track column 覆蓋 | **0 / 8** | **8 / 8** |

`models.TrackPoint` 只有三條非 nullable（`activity_id`、`seq`、`sec`），而 `sec` 由 `int((p.time - t0).total_seconds())` 得出，唔經任何 numeric helper，所以中毒值傳唔到去。餘下七條全部 nullable，映射 `None` 安全。

---

## 再新開（CUI-0007 修復過程發現）

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0009** | 🟢 Low | `to_float()` 對 `"inf"` / `"nan"` 字串回傳非有限值，經 `daily_weather_record()` / `hourly_at()` 直接餵入 writer | ✅ **Done** `91413fc` |
| **CUI-0010** | 🟡 Medium | `src/weather/collectors/` 三個模組零測試覆蓋（審計已列為兩個結構性盲點之一） | ✅ **Done** `7106627` `5569155` `1d4e814` `71d6351` |

**CUI-0009 嘅修法要小心**：唔好改 `to_float()` 本身（佢被 collectors 共用，喺嗰邊 `"inf"` 語意未必想當 `None`），而係喺 `daily_weather_record()` / `hourly_at()` 外面包 `finite(to_float(...))` —— 喺「準備寫入 writer」呢個明確位置收斂。

**CUI-0010 建議同 AU-013 / AU-014 一併做** —— 呢三個檔案正正就係做 network I/O（五個無 timeout 嘅 `requests.get`）同吞 exception（`except Exception: continue`）嘅地方，高風險同零測試完全重疊。測試網同硬化應該同一輪落，否則寫測試釘死緊一個即將改變嘅行為。

### CUI-0010 修復摘要（連 AU-014 + AU-013 collectors 部分）

Tests **215 → 247**（+32）｜三個 collector **0% → 100%**｜`weather/models.py` 0% → 100%（副作用：之前冇嘢建構過嗰啲 dataclass）｜**TOTAL 83% → 93%**｜static JSON 仍然 byte-identical。

| 票 | 結果 |
|---|---|
| AU-014 | 5 個 `requests.get` 全部加 `(5, 30)` connect/read timeout |
| AU-013（collectors） | `except Exception` 收窄 + WARNING 記低年份、月份、exception type 同訊息 |
| CUI-0010 | 32 個離線測試 + 8 個 fixture |

#### `except` 收窄嘅界線劃得好

```
except Exception  →  except (json.JSONDecodeError, KeyError, IndexError)
```

條線：**回應到咗但用唔到** = 數據問題（skip + WARNING）；**回應根本冇到**（`requests.RequestException` 及所有子類 —— DNS 失敗、connection refused、timeout）= 環境問題，向上傳。

理由講得準確：一部連唔到嘅 host **唔會**話你知任何一個月嘅情況；吞咗佢等於將一次 outage 變成十二個靜默空月，最後交返一個近乎空白嘅年度。同 AU-002 / W-004 / CUI-0002 同一條 fail-fast 界線。兩條測試釘住（`ConnectionError` 同 `Timeout` 都要逃得出 `fetch_year`）。

#### 「冇打真網絡」佢係證明而唔係聲稱

Module-scoped autouse fixture `block_real_sockets` 將 `socket.socket.connect` 換成會 raise 嘅版本 —— 呢個 choke point **喺 `requests` 之下**，所以冇任何 fake 繞得過。

佢仲實際驗證咗個 block 真係會咬：跑一個冇裝 fake 嘅測試 → `AssertionError: a test tried to open a real network connection`，而且**嘗試連接嘅係 `127.0.0.1:32901`（agent proxy）**。呢點好重要 —— 佢指出「sandbox 冇 internet」會係一個**假證明**，因為 proxy 就喺本機。socket 層嘅 block 先係真證明。

#### Timeout 測試斷言要求而唔係字面值

測試斷言「有 bounded pair、connect ≤ 10s、read ≤ 60s」而唔係 `(5, 30)` 呢個字面值 —— 所以日後調 timeout 唔會撞爛測試，但**刪走 timeout 就會**。

---

## ⚠️ CUI-0010 挖到兩件遠超原 ticket 範圍嘅嘢

### 1. `run365-weather --source hko-daily` 一直完全跑唔到（已修，`5569155`）

`hko_daily.py:51` 傳 `avg_temp_c=`，但 `DailyWeather` 宣告嘅係 **`mean_temp_c`** ——
**第一行 parse 就 `TypeError`**。Main agent 實測確認：

```
TypeError: DailyWeather.__init__() got an unexpected keyword argument 'avg_temp_c'
```

即係話呢個 collector 由 rewrite 之後就冇 work 過，而**0% coverage 就係冇人發現嘅唯一原因**。
Developer 唔修就達唔到 CUI-0010 嘅覆蓋目標，所以獨立 commit 咗。`models.py` 唔喺 lane scope，
所以修喺呼叫點：`mean_temp_c=to_float(data[3])`（dataclass 文件寫明該欄位係 mean air temperature，
而 extract 個 "Avg. Temp" 欄正是此值）。

### 2. Collector 寫出嘅 schema 同 exporter 讀嘅 schema 唔夾 → **CUI-0011（🔴 Critical latent）**

Main agent 核實後發現**比 developer 報告更嚴重**。Developer 話重新採集後 exporter 會「讀成全部 `None`」，
但實際 `records.py:143` 係 `row["Date"]` —— **bracket access**，所以第一行就 `KeyError` 直接死，
根本行唔到 `None` 嗰步。

三方 key 對照（實測）：

| 來源 | Key |
|---|---|
| 已 commit 嘅 `data/raw/weather/*.json` | `Date`, `Max. Temp`, `Avg. Temp`, `Sunrise`, `Sunset` … |
| Collector 而家寫出（`record.__dict__`） | `date`, `mean_temp_c`, `max_temp_c` … |
| Exporter 讀 | `row["Date"]`, `row.get("Max. Temp")`, `row.get("Sunrise")` … |

**呢條 pipeline 而家行得到，純粹因為冇人重新採集過資料。** 另外 `Sunrise` / `Sunset` 兩個
exporter 會讀嘅欄位，`DailyWeather` 完全冇宣告（同 AU-037 係同一個斷層）。

根源：collector 層同 export 層之間**冇任何 contract**，而 collector 層長期 0% coverage，所以冇人發現佢哋已經漂移。

---

## 再新開（CUI-0010 修復過程發現）

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0011** | 🔴 Critical（latent） | Collector 寫出嘅 schema 同 exporter 讀嘅唔夾，重新採集即炸 pipeline | ✅ **Done** `755fdd5` `7092964`（方案 C） |
| **CUI-0012** | 🟡 Medium | 三處無防護 `.find()`：`hourly.py:57` 硬崩、`warnings.py:55` **靜默錯**、`warnings.py:70` 硬崩 | pending |
| **CUI-0013** | 🟢 Low | `collect_weather.py` 0% 覆蓋 + `print()`；`_REQUEST_TIMEOUT` 三份重複；magic index | pending |

**CUI-0012 之中 `warnings.py:55` 最陰險**：`html.find(marker)` 搵唔到返 `-1`，加 `len(marker)` 之後變 `31` ——
即係由整頁第 31 個字元開始 parse，而唔係乜都唔 parse。HKO 一改標題，collector 唔會失敗，
而係去 scrape 一批唔相干嘅 6-cell table，靜靜攞返錯資料。**比崩潰差 —— 崩潰至少睇得見。**

**CUI-0011 需要你做設計決定**（統一用 dataclass 欄位名 / collector 寫返舊 key / 兩層之間加明確 contract），
唔應該由 developer 單獨拍板。無論邊個方案都必須加一條 round-trip 測試 —— 冇咗佢就係今次潛伏咁耐嘅原因。

### CUI-0009 修復摘要

Tests **247 → 260**（+13）｜TOTAL 維持 93%｜static JSON byte-identical（`weather.json` / `warnings.json` 抽樣逐 byte 相同 — main agent 獨立確認）。

`src/common/numeric.py` **純 append** 一個 `finite_float()`（= `finite(to_float(...))`），原有 `finite()` / `round_or_none()` / `to_float()` **byte-for-byte 未改** — main agent 用 `git diff` 確認過。九個天氣呼叫點改用佢。

加 helper 而唔係手寫九次嵌套，理由講得好：**九次手寫就係九次「記得 cast、唔記得 bound」嘅機會 —— 而呢個 bug 本身就係咁嚟嘅。**

`to_float()` 對兩個 collector 保持寬鬆（ticket 指定嘅設計），因為喺 collector 層 `"inf"` 未必想當 `None`。

#### NOT NULL 分析（第三次做，做法已成慣例）

`src/export/models.py` 嘅 `DailyWeather` 只有一條 NOT NULL（`date`，String PK），六個數值讀數全部 `Mapped[float | None]`；`Activity` 三條 hourly 衍生欄位亦係 nullable Float。所以九個位置映射 `None` 兩個 writer 都收 —— **冇將一個分歧換成另一個**。`date` 唔經 cast。冇 INTEGER column 喺 scope 內，所以 CUI-0007 嗰個 int→float 陷阱唔適用。

六個 daily 讀數**刻意維持唔 round**（只加有限性 gate），呢個正是 export 保持 byte-identical 嘅原因。

#### 第三次撞同一個失敗模式

這是 `json.dumps(allow_nan=False)` 崩潰 vs SQLite 存 `Infinity` 呢個 writer 分歧嘅**第三次出現**（CUI-0001 → CUI-0007 → CUI-0009）。三次都係「某個數值路徑冇經過有限性 gate」。`finite_float()` 令呢個 gate 由「記得手動加」變成「import 一個名」。

#### 順帶更正咗 main agent 一個過時數字

我 brief 寫 baseline 215，但 developer 個 branch base 已含 CUI-0010 嘅 merge，實測係 247。佢冇照抄我個數，而係自己抽 HEAD 去 temp tree 度出真實 baseline。

### CUI-0011 修復摘要（方案 C）

Tests **260 → 283**（+23）｜`weather/models.py` **100%**（新）｜`cli/collect_weather.py` 0% → 45%｜**TOTAL 94%**｜`data/raw/` **零改動**｜static JSON byte-identical（`weather.json` / `warnings.json` 逐 byte 相同 — main agent 獨立確認，`sunrise: '07:03'` 仍在輸出）。

#### 我張 ticket 講錯咗一樣嘢：三條路徑失敗方式各異，唔係「同樣 mismatch」

Developer 實測三條路徑，**兩條係靜默**：

| 路徑 | 失敗方式 | 可見性 |
|---|---|---|
| daily | `records.py` `row["Date"]` bracket access → `KeyError: 'Date'`，export 第一行中止 | **大聲** |
| hourly | `builder.hourly_at()` 掃 `r.get("Date") != date`，而 collector 寫 `"date"` → 永遠對唔上 → 每個 activity 都回 `None` | **靜默**：export 成功，每個天氣區塊 null |
| warnings | `build_records()` 篩 `if r.get("Warning_Signal")`，collector 寫 `"warning_signal"` → **461 行喺 `warning_record()` 執行之前已經全部被丟棄** | **靜默**：`warnings_by_date()` → `{}` |

**兩條靜默嗰啲先係更差嗰半** —— 一次成功嘅 export，靜靜咁剝走晒所有天氣。

順帶發現：raw warning 欄位係 `Ico`、dataclass 係 `icon_url`，而 `warning_record()` **由頭到尾從未輸出過 icon**。

#### Sunrise / Sunset：排除，而且證據夠硬

Developer 冇求其揀一邊，佢揾到決定性證據：

1. **佢哋根本唔係 HKO daily extract 嘅數據。** 舊 pipeline 由第二次 scrape 將六個 sun/moon 欄位 join 入 `hko_daily_weather_extract.json`，而**兩份副本至今仍然唔一致** —— `2021-01-01` 喺 daily extract 讀 `Sunrise 07:03`，喺 `sun_moon_rise_set_history.json` 讀 `07:02`。證明佢哋從來唔係同一個來源。
2. **`SunMoon` dataclass 已經宣告咗 sunrise/sunset。** 加入 `DailyWeather` 等於一個欄位兩個主人 —— 正正就係方案 C 要終結嘅失敗模式。
3. **`hko_daily.fetch_year()` 結構上填唔到佢哋**（AU-037，collector 從未移植）。一個「唯一寫入者填唔到」嘅欄位會令 `to_raw_row()` 輸出 `"Sunrise": null`，**下次採集就會摧毀現有嘅好資料**。

處理得誠實：`daily_weather_record()` 仍然讀呢兩欄以維持 byte-identity，但經 `models.py` 匯出嘅 `SUN_MOON_SUNRISE_COLUMN` / `SUN_MOON_SUNSET_COLUMN` —— **用真正擁有者嘅名**，令呼叫點自己講出呢個決定。三條測試斷言六個 joined 欄位既唔寫亦唔讀。

#### 有限性 gate 分工守住

`from_raw_row()` = 欄位映射 + `to_float`（寬鬆，保留 inf/nan）；`finite()` = 邊界，留喺 `records.py` / `builder.py` 寫入 writer 之前。因為 `finite_float(x)` 本身就係 `finite(to_float(x))`，將組合拆返兩層行為完全相同。

三重確認：CUI-0009 全部測試原封不動通過；新增 `TestMappingOnlyNotBounding` 兩條測試**釘死 `from_raw_row()` 會保留 inf/nan**（防止 gate 靜靜咁遷移入 dataclass）；byte-identical。

---

### ⚠️ 一個關於測試設計嘅重要更正（我張 ticket 寫錯咗重點）

我張 ticket 將 round-trip 測試寫成「方案 C 嘅核心」。Developer 證明咗**呢個判斷係錯**：

> 三條 round-trip 測試**對住一個 `__dict__` stub 一樣會 PASS** —— 兩邊本來就自洽，佢哋只係同**磁碟上嘅檔案**唔一致。Round-trip 單獨存在**捉唔到今次個 bug**。

真正捉到嘅係另外兩類：
- **real-fixture 測試** —— 由 `data/raw/weather/*.json` **逐字**抽出嚟嘅樣本（escape 都保留：`"km\/h"`、`"\u00b0C"`），斷言 `to_raw_row()` 嘅 key 等於已 commit 檔案嘅 key
- **end-to-end 測試** —— collector → `_write_jsonl` → `load_jsonl` → exporter，斷言逐個欄位相等

教訓：**round-trip 只證明「你自己同你自己一致」。要捉跨層漂移，必須有一端錨定喺真實 artifact 上。**

---

## 再新開 / 升級（CUI-0011 發現）

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **AU-037** | 🟡 **升級** | `SunMoon` 冇 collector —— 而家變成 load-bearing | pending |
| **CUI-0014** | 🟢 Low | `from_raw_row()` 對缺失 STRING 欄位預設 `""` 而舊 code 出 `None` | pending |

**AU-037 升級理由**：排除 sunrise/sunset 之後，一次重新採集會寫出一個冇 sun/moon 欄位嘅 `hko_daily_weather_extract.json`，令 export 出嘅 sunrise/sunset 變 `None`。資料唔會損壞、export 亦唔會爆，但 dashboard 會失去呢兩個欄位，直到 `SunMoon` collector 移植好為止。**呢個係排除決定嘅誠實代價，唔係新引入嘅 regression** —— collector 從來都冇呢啲值。

**CUI-0014**：`from_raw_row()` 將缺失嘅 string 欄位預設做 `""`，舊 code 出 `None`（hourly `Description`；warning `Type` / `Start_Time` / `End_Time`）。目的係令 dataclass annotation 保持誠實嘅 `str` 而唔使將五個欄位放寬成 `str | None`。實務上不可達 —— developer 掃過全部 17,984 個 hourly 同 461 個 warning 已 commit 行，**每個檔案 key set 完全劃一**。對 byte-identical 零影響。值得 reviewer 睇一眼。

---

## AU-004 調整 + W-011 關閉（2026-09-14，用戶改變決定）

用戶決定：「暫時 deployment branch 我都仲想係 develop branch 做嘢先，到無晒問題先再決定去 master branch」。

AU-004 原本將 CI 全轉 `master`。就咁擺會令 develop 嘅 push **完全冇 CI** —— 原本個 bug 方向調轉。已調整為：

| Job | 行為 |
|---|---|
| `lint-test`、`frontend` | push 同 PR 到 **`develop` + `master` 兩條**都跑 |
| `build`、`deploy` | **只喺 `refs/heads/develop`** |

咁樣 develop 有 CI 接住、`github-pages` environment 設定唔使郁，而 AU-004 換返嚟嘅嘢（master PR 有 gate，PR #10 個洞唔會翻疊）亦保住。順帶關閉 **W-011**。

### Concurrency 喺雙 branch 之下係真 bug，唔係 formality

W-002 嘅 group 用 **event name** 做判別：

```yaml
group: pages-${{ github.event_name == 'pull_request' && github.ref || 'deploy' }}
```

雙 branch 之後，`master` 嘅 push 係 `push` event → 落 `pages-deploy` group，即使佢根本唔部署。兩個實際傷害：

1. 純 CI 嘅 master run 佔住 deploy group，develop 嘅真部署要等佢跑完 lint-test + frontend 先開始
2. **更嚴重**：GitHub 每個 group 只保留一個 pending run。develop 部署跑緊 + develop 部署 A 排緊隊 + 一個 master push 到 → **A 被踢走並報 "cancelled"（唔係 "failed"，冇 alert）**。W-002 明明就係為咗「部署唔會被 cancel」而做，卻被第二條 branch 由側面打爆

修法：判別 key 由 event name 改成 **ref**，即「呢個 run 究竟部唔部署」：

```yaml
group: pages-${{ github.ref == 'refs/heads/develop' && 'deploy' || github.ref }}
cancel-in-progress: ${{ github.ref != 'refs/heads/develop' }}
```

W-002 兩個性質原封不動：會部署嘅 run 共用一個 group、永不 cancel、排隊唔互殺；唔部署嘅 run 各自 per-ref group 兼 cancel-in-progress（即 runner saving）。

### `workflow_dispatch` 決定

Gate 改成 `github.ref == 'refs/heads/develop'`，即**只准喺 develop 手動部署**。理由：手動重新部署係真需求（transient failure 後重推同一 commit），完全禁止會迫人用 empty commit 去觸發 —— 反而更差。W-011 個 bug 唔係「有得手動部署」，而係「任何側枝手動觸發都會做 production 部署」。

副作用（正面）：`github.ref` 對 pull_request event 係 `refs/pull/<n>/merge`，永遠唔等於 `refs/heads/develop`，所以同一個條件已經排除 PR，唔使再疊 `event_name != 'pull_request'`。

### README 七處逐一處理，兩處刻意保留

| # | 處理 |
|---|---|
| 1 CI badge | 改返 `develop` |
| 2 CI 描述 | **改寫**而唔係還原：兩條 branch 都 gated、只有 develop 部署 |
| 3 Vercel production branch | 改返 develop，但 **S-001 加嘅謹慎措辭原句保留** |
| 4 Versioning `(tag only) \| v1.0.0` | **保留** —— 事實更正，v1.0.0 由 tag 持有、冇 branch head 指住，同 branch 之爭無關 |
| 5 Versioning `\| v3.0.0` | 改返 develop，冇加多一行 master（內容一樣，加行會令人以為係兩份嘢） |
| 6 Release 敘述 | 改返 develop，保留「PR 有 CI gate」（而家兩條都真），補一句講 master 角色 |
| 7 Privacy | **保留** —— 已唔再提 branch，係純事實表述 |

### 新增切換 checklist

`docs/deployment.md` 新增 `## Switching the deploy source from develop to master`：**六件事、四個地方、兩件喺 repo 外面睇唔到亦驗唔到**。

Repo 內：pages.yml 觸發清單｜pages.yml 嘅 build/deploy `if:` **同埋** concurrency 嘅 group + cancel-in-progress（四處都寫住 `refs/heads/develop`，**最易漏**）｜tag-release.yml default｜README 四處。
Repo 外：GitHub `github-pages` environment deployment branch｜Vercel Production Branch。

---

## 新開

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0015** | 🟢 Low | `docs/architecture.md:142` 嘅 CI 描述**同一 session 內過時兩次**，應改為自動同步 | pending |

呢段喺 AU-004 之後由 W-001 修好，W-011 之後**又再過時**。根本問題係一段描述 CI 行為嘅文字同 `pages.yml` 之間冇任何同步機制。建議參考本 repo 已經證明有效嘅 `run365-schema --check frontend/schema.graphql` pattern。

---

## C/W/S-NNN — 來自 AU-047 Review（2026-09-14）

來源：[`reviews/2026-09-14_review_au-047.md`](reviews/2026-09-14_review_au-047.md)
審閱 `2adf6ba` + `e088a1b`｜評分 **66/100**｜結果 ❌ **fail**（1 🔴）｜next_action `invoke_developer`

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **C-001** | 🔴 Critical | budget 扣 points 唔扣 round trip：27 alias × `activities(365){track(points:1)}` = 9,855 點（合法）→ **19,764 SQL / 13-14 s**，換算真機約 30 s，爆 15 s Vercel limit | 🔧 修正輪處理中 |
| **W-012** | 🟡 Warning | `10000` 嘅理據引用咗前端發唔出嘅 document（`ActivityFields` fragment 冇 `track`，`YearQuery` 消耗 0 點；真實上限係 `TrackQuery` 嘅 600 點） | 🔧 修正輪處理中 |
| **W-013** | 🟡 Warning | `isinstance(context, MutableMapping)` guard 守得住，但註解講嘅理由錯（read-only `Mapping` 去到 `track`，真正安全網係扣數函數嘅 `KeyError`）；client 收到裸 internal key | 🔧 修正輪處理中 |
| **W-014** | 🟡 Warning | 刻意選擇嘅 fail-closed 行為零測試 —— 加句 `.get(KEY, MAX_...)` 291 條測試全部照綠 | 🔧 修正輪處理中 |
| **S-014** | 🟢 Suggestion | 被拒請求喺 ERROR log 留完整 traceback（既有行為，非本次引入） | ➡️ 轉 **S-018** 整批處理 |
| **S-015** | 🟢 Suggestion | 兩處測試斷言／註解偏鬆 | 🔧 修正輪一併修 |
| **S-016** | 🟢 Suggestion | 「per-request」實為「per-operation」，相等性靠 Strawberry batching 預設熄咗 | 🔧 修正輪一併修 |
| **S-017** | 🟢 Suggestion | `on_operation` 缺 return annotation；`# Factories, not instances` 註解同下面第四個（class）唔完全對應 | 🔧 修正輪一併修 |
| **S-018** | 🟢 Suggestion | Client 輸入錯誤（`_page` / `_track_points` / track budget）一律以 `ValueError` + 完整 traceback 記入 ERROR log。應引入 `ClientError` + Strawberry `process_errors` override 整批降級 | pending（**唔喺 AU-047 範圍**） |

### C-001 採用方案

`MAX_TRACK_FIELDS_PER_REQUEST = 64`，同 points budget **正交**嘅第二個 counter。

64 唔係憑空定：points budget 喺 default `points=150` 之下**已經隱含**咗 `10000 // 150 = 66` 個 track field 嘅上限，攻擊就係靠將 `points` 壓到 1 去繞過呢個隱含限制。顯式定 64 對現有合法用法接近零影響，但令「平 budget 換貴 round trip」呢條路徑收窄到最壞 64 × 2 = 128 條 SQL。

未採用：`cost = max(points, K)`（一條公式收兩樣嘢，SDL 難解釋）；淨係調低 budget（攻擊成本同 budget 成線性 —— budget 1000 仍然 2,006 SQL / 1.44 s，只係縮細個洞）。

### ⛔ 連帶更正：AU-050 唔可以再引用「batching 只值 7%」

`MAX_TRACK_POINTS_PER_REQUEST` 原本個 docstring 寫「The cost is building the rows, not the per-activity round trips, so batching the queries would not have bought the headroom back」。

呢句**只喺當時度嗰個 shape（732 條固定 query）成立**。C-001 證明咗 query 條數本身先係冇 bound 嗰樣嘢：同一個 10,000 點預算，喺 alias flood 之下係 19,764 條 SQL / 14 s，round trip 佔 99.9% 成本。兩句被否證嘅結論（連「10,000 rows costs about 0.37 s」）必須喺修正輪刪走。

**AU-050 嘅價值因此上調**：batching 做完之後，每個 `track` field 由 2 條 SQL 變成攤分一條批次查詢，`MAX_TRACK_FIELDS_PER_REQUEST` 可以獨立放寬而唔使郁 points budget。

### AU-047 Review 第二輪（2026-09-14）—— ✅ pass 91/100

來源：[`reviews/2026-09-14_review_au-047_round2.md`](reviews/2026-09-14_review_au-047_round2.md)
審閱 `830d0a0` + `6a040f1` + `c42ed6a`｜**0 Critical**｜next_action `merge_develop`

四個 blocking item 全部經 reviewer 獨立實驗核實關閉：

| Item | 裁決 | 關鍵證據 |
|---|---|---|
| C-001 | ✅ closed | 原條 alias flood：**19,764 SQL / 14.68 s → 130 SQL / 0.15 s**。窮舉過 alias 寬度（1/2/27/28/52/53/100/200）、page window（64/65/365/1000）、offset paging、每 activity 多個 aliased track、nested `year→personalBests→track`、`activity(id:)`×alias、fragment / inline fragment / `__typename` —— **冇任何合法 document 超出 64 個 track field**。被拒請求同樣只燒 130 條（上一輪被拒都燒 20,056 條 / 13 s） |
| W-012 | ✅ closed | 新理據逐句核實對得上前端源碼；兩句被否證嘅結論全 repo `grep` 零命中 |
| W-013 | ✅ closed | 9 種 context 形狀全部 fail-closed；`RuntimeError` 訊息唔再洩漏 internal key |
| W-014 | ✅ closed | 5 個 runtime mutant 全部被捉（M1 `.get()` fallback → 1 failed / 297 passed） |

### 新開（AU-047 review 第二輪，全部非 blocking）

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **W-015** | 🟡 Warning | `tests/test_api.py` 嘅 `MAX_SQL_PER_REQUEST = 182` 推導乾淨（唔係 magic number），但常數名同測試名 claim 咗一個唔成立嘅全域性質 —— 實測有合法、被服務、零 error 嘅 document 去到 **208 SQL**（`52 × activities(limit:1){track(points:1)}`）。同 W-012 同一類缺陷（claim 大過實測支持嘅範圍），只係今次喺測試碼。建議改名做 `ALIAS_FLOOD_MAX_SQL` | pending |
| **S-019** | 🟢 Suggestion | `test_track_fails_closed_when_the_context_cannot_be_seeded` 過唔到自己個註解：喺 `.get()` fallback mutant 之下照樣綠，而綠嘅真正原因係 `'mappingproxy' object does not support item assignment`，唔係註解講嘅 missing key。加 `assert "not seeded" in ...` 就有牙（同一標準亦套落姊妹測試） | pending |
| **S-020** | 🟢 Suggestion | 「every `track` field after the one that overran it is refused in constant time」只對 field budget 成立 —— field budget sticky，points budget 唔 sticky（overrun 後仲剩 999 點，`track(points: 500)` 會被服務）。行為冇問題，句子要收窄 | pending |
| **S-021** | 🟢 Suggestion | 「64 is the ceiling the points budget already implied … `10000 // 150` is 66」同一句兩個數對唔上。建議明寫「66, rounded down to 64」 | pending |
| **S-022** | 🟢 Suggestion | `_cheap_tracks(n)` 用 `n` 做 `activities(limit:)`，而 `MAX_PAGE_SIZE = 1000`。`MAX_TRACK_FIELDS_PER_REQUEST` docstring 明寫 AU-050 之後要調高呢個數 —— 一旦 ≥ 1000，三條測試會因為一個同 track budget 無關嘅 `limit` 錯誤紅起，訊號誤導。建議改用 alias 砌 fan-out，或加 `assert MAX_TRACK_FIELDS_PER_REQUEST < MAX_PAGE_SIZE` | pending |
| **S-023** | 🟢 Suggestion | **AU-047 範圍外**：非 track 嘅 `year` fan-out 而家先係最貴嘅合法請求 —— `110 × year{trainingLoad{ctl}}` = 330 SQL / 1.58 s / errors=0。每個 `year` alias 都重新 materialise 成年 365 條 activity 再行 full stats，track budget 完全睇唔到佢。真機換算約 3.5 s，仍然安全，而且 AU-047 之前就存在 | pending（建議另開 audit ticket，同 AU-050 並列） |

### AU-047 最終狀態

| 輪次 | Commit | Review |
|---|---|---|
| 第一輪 | `2adf6ba` `e088a1b` | ❌ fail 66/100（C-001） |
| 第二輪 | `830d0a0` `6a040f1` `c42ed6a` | ✅ **pass 91/100，0 Critical** |

**最終效果**（365 activities × 600 points 嘅 production-shaped DB）：

| 形狀 | AU-047 之前 | 之後 |
|---|---|---|
| `activities(365){track(1000)}` | 732 SQL / 8.29 s / 219,000 rows | 22 SQL / 0.22 s（被 points budget 拒） |
| 27 alias × `activities(365){track(1)}` | 19,764 SQL / 14 s / HTTP 200 | **130 SQL / 0.15 s**（被 field cap 拒） |
| `activity(id){track(600)}`（前端真實路徑） | 4 SQL / 0.02 s / 600 點 | **4 SQL / 0.02 s / 600 點**（零回歸） |

---

## CUI-NNNN — 來自 AU-047 QA（2026-09-14）

來源：[`qa/2026-09-14_qa_au-047.md`](qa/2026-09-14_qa_au-047.md)
✅ **pass**｜0 Critical / 1 Major / 2 Minor｜next_action `merge_develop`

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0016** | 🟡 Major | `ActivityView` 將 track 載入失敗誤報成「Activity not found.」—— `ActivityView.tsx:70` 有 `all.isError` 處理但**冇** `track.isError`，所以 track 失敗跌落 `:71` 嗰條「唔存在」分支，連已經成功載入嘅 KPI／天氣一併丟棄。唔會白畫面（乾淨 early return，無需 ErrorBoundary），純粹係訊息報錯咗因 | pending |
| **CUI-0017** | 🟢 Minor | `src/api/schema.py:84` 寫「Worst case is therefore 64 × 2 = 128 statements」，但實測 request 層面去到 **208**：每個 aliased `activity(id:)` parent 本身要 ~2 條語句（`session.get` + `selectinload`），而且個 track 就算被拒都照收 —— 呢半邊唔受 field cap 約束，只受 token limiter 約束。Bound 本身冇問題（86 倍 headroom），純文件準確性 | pending |
| **CUI-0018** | 🟢 Minor | Budget 拒絕嘅 GraphQL error 得 `['locations','message','path']`，**冇 `extensions.code`**，client 只能 match 由常數 f-string 砌出嚟嘅字串（常數一改就靜靜哋失效）。另外 non-null propagation 令一個被拒 track 清空成個 `data`（連成功嘅 sibling `meta` 都冇）。今日不可達，AU-050 之前應處理 | pending |

> ✅ Response **零洩漏**：177 bytes、無 stacktrace、無絕對路徑、無 context key 名。W-013 特登唔講出 `track_points_remaining` 係啱嘅。

### CUI-0016 / CUI-0018 嘅共同前提

兩者**今日都不可達**：全 codebase 只有一個 `useTrack` call site，600 點 × 1 個 field，離 budget 好遠。但 AU-047 為呢兩條路徑各新增咗一個成因，所以 **AU-050 加 batch track field 之前要修**，否則第一個用到嘅人就會撞。

### ⚠️ 同一類缺陷第四次出現

`CUI-0017` 同 `C-001` docstring、`W-012`、`W-015` 係**同一個失敗模式**：**寫低嘅 claim 闊過實測支持嘅範圍**。

| # | 位置 | Claim | 實測 |
|---|---|---|---|
| 1 | `MAX_TRACK_POINTS_PER_REQUEST` docstring（已修） | 「batching would not have bought the headroom back」 | alias flood 之下 round trip 佔 99.9% 成本 |
| 2 | 同上，W-012（已修） | 「twice the most expensive document the dashboard can send」 | 該 document 消耗 0 點 |
| 3 | `MAX_SQL_PER_REQUEST`，W-015（pending） | 名為 per-request 上限 | 有合法、被服務、零 error 嘅 document 去到 208 |
| 4 | `MAX_TRACK_FIELDS_PER_REQUEST` docstring，CUI-0017（pending） | 「Worst case is therefore 64 × 2 = 128 statements」 | request 層面 208 |

第 3 同第 4 其實係**同一個 208** 由兩個唔同角度撞到。修嘅時候應該一次過處理：講清楚 field cap bound 嘅係 **track 語句**，而 request 總語句數仲有 parent field 嗰半邊（由 `MAX_QUERY_TOKENS` 封頂）。

### QA 建議補測（developer 執行）

| 優先 | 測試 | 理由 |
|---|---|---|
| **高** | E5 跨 budget fail-closed（points 耗盡後平價 alias 全部要拒） | QA 實測：而家改壞 `_charge_track_field` 嘅收費順序，**298 條測試全部照綠** |
| 中 | E1 具名 fragment / E2 inline fragment | 兩個都過咗，但冇測試釘住 |
| 中 | E6 `@skip` 唔誤收費 | 同上 |

### Regression：byte-identical 守住

QA 用 `git archive origin/develop` 抽基準（冇切 branch），以 `PYTHONPATH` shadow 掉 editable install，並核實 baseline 真係跑舊 code（`hasattr(schema, "MAX_TRACK_FIELDS_PER_REQUEST") == False`）。

| 指標 | 基準 | HEAD | |
|---|---|---|---|
| Activities | 365 | 365 | ✅ |
| SQLite | 11,460 KB | 11,460 KB | ✅ |
| Static JSON bytes | 6,857,102 | 6,857,102 | ✅ |
| Static JSON files | 370 | 370 | ✅ |

md5 逐檔對拍 **369/370 完全相同**，唯一差異係 `meta.json` 嘅 `generated_at`。

### 前端 gates（reviewer 冇跑過，QA 補齊）

```
npm run lint          → eslint 零輸出，exit 0
npx vitest run        → 19 files / 87 tests passed
npm run build         → 292 modules, 657.73 kB
npm run build:static  → 292 modules, 657.69 kB
```

真實前端 8 條 document 打真 Flask + 真 `run365.db`：全部 200 / `errors=[]`；`TrackQuery` 攞足 390 點；**`YearQuery` 收費 0 個 track field**（直接 patch `_charge_track_field` 數出嚟，核實 reviewer 講法）；全 365 條 track 嘅 `track(points:600)` 都攞足。static vs api：point-count mismatch 0、point-value mismatch 0，S-011 冇惡化。

效能：240 個 request 零錯誤，P95 ≤ 76 ms；300/300 連續 full-cap request 無 budget 洩漏；最壞 request 174 ms —— 對 Vercel 15 s 有 **86 倍 headroom**。

---

## AU-047 Review 第三輪（2026-09-14，收窄範圍驗證）—— ⚠️ warn 87/100

來源：[`reviews/2026-09-14_review_au-047_round3.md`](reviews/2026-09-14_review_au-047_round3.md)
審閱 `9cd1125` + `5ff91d3` + `3ce2bd5`（cleanup 輪）｜0 Critical｜next_action `invoke_developer`

Cleanup 輪嘅六條全部關閉（CUI-0017 / W-015 / S-019 / S-020 / S-021 ✅），**除咗 S-022 ❌ still open** —— 佢嘅理由本身就係新一個 over-claim。

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **W-016** ✅ | 🟡 Warning | `tests/test_api.py:613-620` `_page_field` docstring 三句斷言都錯：「a fan-out cannot reach the field cap at all」「64 aliased `track` fields lex to 1218 tokens」「Only a page window can put that many tracks in one operation」。實測單一 parent 加 64 個 aliased `track` = **714 tokens，完全服務**；65 個 = 725 tokens，被 **field cap**（唔係 parser）拒；90 個先啱啱好貼 `MAX_QUERY_TOKENS`。`1218` 只屬「64 個 aliased **parent** 各帶一個 track」嗰個讀法。**連帶令 `activity(id:)` + aliased track 呢條真實可達路徑零測試覆蓋 —— 而嗰條就係 C-001 原本嘅攻擊面** | ✅ **Done** |
| **W-017** ✅ | 🟡 Warning | 八個新量度數字（`130 / 208 / 990 / 1009 / 1218 / 332 / 166 / 0.11–0.20 s`）**只以 docstring 散文存在，零 assert 釘住**。改動 `MAX_QUERY_TOKENS`、`SQL_PER_TRACK_FIELD` 或 warnings query 之後會靜靜咁腐爛。**呢個就係 AU-047 重複五次同一個錯嘅結構成因** | ✅ **Done** |
| **S-024** ✅ | 🟢 Suggestion | `src/api/schema.py:97`「Saturating this cap takes one list field.」讀落似必要條件 | ✅ **Done** |
| **S-025** ✅ | 🟢 Suggestion | `src/api/schema.py:225-227`「charges it below 0 too」：拒收唔會寫回，儲存值永遠停 0，只有 local 值計到 −1 | ✅ **Done** |
| **S-026** ✅ | 🟢 Suggestion | `src/api/schema.py:93`「Measured against a 365-activity, 600-point export」同 `year_db` fixture 唔符（fixture 只有 `r0` 有 track，共 600 rows 唔係 219,000）。**冇一個數字係錯**，只係描述誤導 | ✅ **Done** |

### ⚠️ 同一類缺陷第五次 —— 而且係喺專門修佢嘅 commit 入面

| # | 位置 | Claim | 實測 | 狀態 |
|---|---|---|---|---|
| 1 | budget docstring | 「batching would not have bought the headroom back」 | round trip 佔 99.9% | ✅ 已修 |
| 2 | 同上（W-012） | 「twice the most expensive document the dashboard can send」 | 消耗 0 點 | ✅ 已修 |
| 3 | `MAX_SQL_PER_REQUEST`（W-015） | 名為 per-request 上限 | 208 | ✅ 已修 |
| 4 | field cap docstring（CUI-0017） | 「64 × 2 = 128 statements」 | 208 | ✅ 已修 |
| 5 | `_page_field` docstring（W-016） | 「alias fan-out cannot reach the field cap」 | 714 tokens，服務 | ✅ **Done** |

**W-017 係呢五次嘅共同成因**：每次都係「散文寫咗一個冇 gate 嘅數」。修 W-017（將 headline 數字變成 assert）比逐個修 claim 更根本 —— 呢個係本 ticket 最有價值嘅一項。

> ⚠️ 留意 W-016 嘅方向：field cap 嘅實際保護面**比 docstring 講嘅闊**（佢真係擋到 alias fan-out，fails safe）。出事嘅係描述，唔係 bound。零 runtime 影響。


### W-016 / W-017 修復結果（`b127a7e` `57317e9` `bab3858`）

測試 **303 → 309**。`src/api/schema.py` 經 AST 證明**只改咗 docstring**。

**W-017 嗰條測試真係有牙** —— developer 試咗三種漂移，每次都紅，還原後回綠：

| 改動 | 結果 |
|---|---|
| `MAX_TRACK_FIELDS_PER_REQUEST` 64 → 32 | 紅：`statement cost drifted: saturating the cap takes one list field` / `assert 66 == 130` |
| `MAX_QUERY_TOKENS` 1000 → 900 | 紅：兩條闊 row 變 `Document contains more than 900 tokens` |
| `SQL_PER_TRACK_FIELD` 2 → 3 | 紅：`assert 130 == (2 + (64 * 3))` |

`_token_count()` helper 用二分搜尋揾 graphql-core 肯 parse 嘅最細 `max_tokens` —— 即係**定義上** `MaxTokensLimiter` 攞嚟同 `MAX_QUERY_TOKENS` 比嗰個數，而唔係重寫一個會自己漂嘅 lexer。

Alias route（原本零覆蓋）而家有三條測試，其中 `test_the_parser_never_gets_to_refuse_an_aliased_track_flood` **唔 hardcode 90**，而係由 64 開始長大直到 parser 肯收嘅最闊 fan-out，再 assert 佢仍然係 cap 拒 —— 兩個常數點郁都仲係啱。

### Developer 差啲犯第 6 次，自己捉返

寫 S-024 嗰陣佢加咗「53 lexes to 1009」—— 一個冇 gate 嘅新數字，正正係 W-017 要斬嘅 pattern。自己捉返、補咗 assertion 先 commit。

### 兩個 reviewer 表未 cover 嘅數字更正

| 原本寫 | 實測 |
|---|---|
| page window `~0.1 s` | **0.026–0.039 s**（差 3 倍） |
| `75x inside the 15 s Vercel function` | 用返實測最慢值 0.209 s → **~70x** |

### 新開（W-016 修復過程發現）

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **S-027** | 🟢 Suggestion | `_page_field` 個 `MAX_PAGE_SIZE` assertion 嘅理由弱咗一半：原本立論係「fan-out 根本唔可能」所以 page window 係唯一出路；而家已知 alias route 可行，所以呢個 helper 揀 page window 純粹係短。AU-050 抬高 field cap 時重建佢嘅選項多過一個 | pending |
| **S-028** | 🟢 Suggestion | `from graphql import ...` 被 ruff 排入 first-party block（`run365days` 隔籬），即使 `pyproject.toml` 寫住 `known-first-party = ["run365days"]`。放第三方 block 會被 `I001` 拒。ruff 自己嘅判決、CI 一致，但讀落怪 —— 可能同 repo root 有個 `api/graphql.py` 有關 | pending |

---

## 2026-09-15 Batch — CUI-0016 / AU-048 / AU-050（三 lane 並行 + 兩輪 review + QA）

流程：3 條並行 lane → UI visual gate → batch review round 1（**warn 89**）→ 2 條修正 lane →
batch review round 2（**pass 99**）→ batch QA（**pass，0 Critical**）→ 1 條 docstring lane。

| Ticket | 狀態 | Commit |
|---|---|---|
| **CUI-0016** 🟡 `ActivityView` 將 track 失敗誤報成「Activity not found.」 | ✅ **Done** | `88c5c6f` `b84b7b7` |
| **AU-048** P1 epoch-ms 語義錯 8 小時 | ✅ **Done** | `6fcb3cc` |
| **AU-050** P2 track batching | ✅ **Done** | `2a043f7` |

測試：Python **309 → 350**、前端 **87 → 108**。

### Review round 1 開嘅 item（全部 ✅ Done）

| ID | 級別 | 內容 | Commit |
|---|---|---|---|
| **W-018** | 🟡 | `track.isPending` 留喺 global loading gate 嘅**申報理由係假**（reviewer 實測拆走之後 105 條全綠）。真理由係拆走會令載入途中 `readableError(null)` 彈「unknown error」——即 CUI-0016 本身要修嗰類謊話。保留啱，但冇註釋冇測試 | `d1dbbde` |
| **W-019** | 🟡 | `parse_datetime` 個 `Returns: aware datetime in *timezone*` 對 offset 分支係**假**（`+09:00` 入 `+09:00` 出，零轉換），AU-048 仲新加一句將呢個未檢查嘅假設寫成 contract | `bd6de39` |
| **W-020** | 🟡 | `WIDEST_BATCH_ACTIVITIES` 自稱 bound-parameter worst case 但唔係 | `cb41ac8` |
| **W-021** | 🟡 | `_charge_track_field` 講「兩個 counter 都唔會寫返」，但只搬 field counter 去 points check 之前，97 條測試零紅 —— 一半 gate | `24fd43b` |
| **S-029** | 🟢 | `readableError` 嘅 `Array.isArray` guard 冇 load-bearing input（`isRecord` 嗰半係 equivalent mutant，唔算 gap，見下） | `ef5ae67` |
| **S-031** | 🟢 | `round(ts/1000, 1)` 偽造 0.1 秒精度，無註釋無測試 | `1ba8ec0` |
| **S-032** | 🟢 | `src/common/time.py` 常數欠 docstring（違反 CLAUDE.md §5） | `9165f0d` |
| **S-033** | 🟢 | wall-clock 讀數 `0.17-0.21` 低過實測下限 | `150d7d9` |

### Review round 1 開但**刻意未做**

| ID | 級別 | 內容 | 狀態 |
|---|---|---|---|
| **S-030** | 🟢 | activities list 空 → `current` undefined → `enabled: !!id` 令 query 永遠 `isPending` → 頁面**永遠「Loading…」**，冇 empty state。Pre-existing，同 CUI-0016 唔同源 | **pending** —— 加 empty state 係真視覺改動，要另行 UI visual gate |
| **S-034** | 🟢 | pending 軸嘅分層降級（per-region 三態：skeleton / error / charts）。做咗之後先可以將 `track.isPending` 由 global gate 拆走 | **pending** —— 已喺 `ActivityView.tsx:70` 註釋點名為前置條件 |

### 三處「下游用實測推翻上游」（本批最有價值嘅部分）

| # | 誰推翻誰 | 內容 |
|---|---|---|
| 1 | Reviewer → Lane A | 「拆走 `track.isPending` 會整 flaky `views.test.tsx`」係假 —— 實測拆走之後 105 條全綠。理由錯，但結論啱 |
| 2 | Reviewer → Lane C | `WIDEST_BATCH_*` 唔係 worst case；真 worst case 係 64×156 |
| 3 | **Lane D → Reviewer** | ① fixture 綁 **10,101** 唔係 10,100（公式係 `positions + 2N + 1`，第三項係 `row_number() OVER (...) - 1` 嗰個 `- 1` 被 SQLAlchemy 綁成 parameter，五個 shape 全部食正）② reviewer 建議嘅 `P // (F+1)` 會得出 9,921，**低過 fixture**；正確係 `P // F` = 156 → 10,113 |

Reviewer round 2 **兩處都撤回**，並自我診斷：

> Round 1 我**量咗** (64,156)=10,113 但**算咗** (50,200)=10,100，再將兩者放埋同一個表，冇標示邊個係量邊個係算……呢個正正係我開 W-020 去 ticket 佢嘅同一個毛病。

即係話 Lane C 最初報嘅 10,101 **一直都係啱**（只不過係 fixture 值而非 worst case），reviewer round 1 嗰句「tree 入面零命中」已正式撤回。

### Bound parameter 實測（三項公式，已 gate）

| shape | 實測 params |
|---|---|
| fixture (50, 200) | 10,101 |
| **真 worst case (64, 156)** | **10,113** |
| SQLite 編譯預設上限 | 32,766（headroom 3.24×，超限係硬 raise `too many SQL variables`，唔係變慢） |

⚠️ **`MAX_TRACK_POINTS_PER_REQUEST` 而家多咗一個隱性 consumer**：調高佢會一比一推高 bound parameter 數。呢個 coupling 有 gate 守住（調高會令 `assert widest < 32766` 變紅）。

### Equivalent mutant 判定（Lane E 提出，reviewer 裁決成立）

`isRecord(first)` 換成 null-safe `first?.message` 之後 108 條全綠。域窮舉：`first` 來自 `JSON.parse`，要分開兩者需要「唔係 object 但有 truthy `.message`」嘅值（boxed String、function），**JSON 表達唔到**。`isRecord` 剩返嘅職責係型別收窄避免 `as` cast，由 `tsc` 守唔係由 suite 守。

> Lane E **冇**砌人為測試去殺一個 equivalent mutant（嗰樣就係 test-gaming），而係去釘真正有分別嘅 `null`，再喺註釋寫明測試買到乜、買唔到乜。

### QA 開嘅 ticket（全部**唔阻 release**）

| ID | 級別 | 內容 | 狀態 |
|---|---|---|---|
| **CUI-0019** | 🟠 High | AU-050 嘅 `_sample_filter()` 砌 N 條 OR arm，SQLite 對 subquery 每 row 評估晒 → **O(batch²)**。真實 export（134,041 track row）上 field cap 嗰個 fan-out **67 ms → 158.7 ms（2.37×）**，128 條 3.9×，365 條 8.9×（3.8 秒）。Statement count 的確 130 → 4，但 wall clock 升咗 | pending |
| **CUI-0020** | 🟡 Medium | docstring 嘅 wall-clock 喺 `year_db` fixture（**600** 條 track row）量，卻用嚟論證 production（**134,041** 條，224×）嘅餘裕。同一 shape fixture 10.4 ms vs production 165.6 ms（16×）；「some 60x inside the 15 s Vercel function」production 實際係 **38×** | pending |
| **CUI-0021** | 🔵 Low | api / static 取樣 rounding 差一個 index（**pre-existing**，今日不可達） | pending |

### QA 等價性證據（0 Critical 嘅依據）

QA **唔收**我提供嘅 baseline DB —— 佢查到個 `generated_at` 係 AU-048 之後 AU-050 之前，唔係真 pre-batch baseline，於是自己 `git archive 1e820fe` 展開成棵舊代碼樹重新生成。

| 驗證 | 規模 | 結果 |
|---|---|---|
| Export 逐 table | 7 table，`track_points` 134,041 row | 6/7 完全相同，唯一差異 `meta.generated_at` |
| Static JSON | 370 檔 | **369 byte-identical** |
| batch vs single track | 365 activity × 13 個 points = **4,745 條逐點（8 欄）** | 0 不符 |
| service 層 pre/post | **6,205 次** `service.track()` | 0 差異 |
| Document A/B | 87 條（含前端 8 條真實 document） | 全部 SAME |
| AU-048 死碼 claim | 重 parse **1,095 個 raw 檔**，330,553 次 `parse_datetime` | **epoch-ms 分支 0 次** —— claim 成立，冇一條平移 8 小時 |
| 前端真機 | 9 個場景 | 全對，console 零意外 error |

### 系統性觀察 —— W-017 要擴展

AU-047 立嘅 W-017 係「**數字**值得寫低就值得 assert」。本批顯示要擴展成「**判斷**值得寫低就值得 assert」：

- 「呢個 fixture 係 worst case」（W-020）—— 判斷，錯，冇 gate
- 「呢個 counter 唔會寫返」（W-021）—— 判斷，啱，半 gate
- 「呢句唔可以拆走，否則 flaky」（W-018）—— 判斷，錯，冇 gate

另外兩條新嘅失敗模式：

1. **「量」同「算」混喺同一個表而唔標示** —— reviewer round 1 自己中招，而佢當時已經見到實測同算式差 1 但冇追。
2. **測試只斷言「應該出現嘅嘢」，冇斷言「唔應該出現嘅嘢」** —— CUI-0016 第一版斷言「有冇 track 專屬訊息」，所以成舊序列化 `ClientError`（連 raw GraphQL document 同 variables）吐晒出街都照樣綠。要真機截圖先捉到。
