# Ticket Registry — Run365Days

**最後更新**：2026-09-14（backlog screen 後；AU-049 表格行補回 Done 標記）

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
| **AU-048** | P1 | epoch-ms path 語義錯 8 小時；測試常數係捏造 | ✅ **Done** `9f856a7` |
| **AU-049** | P2 | `create_app(..., graphiql: bool = True)` 預設仍然開 | ✅ **Done** `31f49f4`（採用方案 (b)：跟 `RUN365_GRAPHIQL`，非單純翻轉為 `False`） |

#### AU-047 — `Activity.track` N+1 fan-out

AU-001 修好咗**每次 query 嘅讀取放大**（SQL 層 stride，唔再讀晒 600 點先喺 Python 降採樣），但**冇修 fan-out**：`src/api/schema.py:137-139` 仍然每個 activity 各發一次 query。

- 修復前最壞：365 query × 600 行 ≈ 219,000 個 ORM object
- 修復後最壞：500 query × `DEFAULT_TRACK_POINTS` 行

單次請求資料量降咗一個數量級，但 500 次 query 嘅 round-trip 仍然要喺 15 秒 function 入面行完。

**建議解法**：Strawberry DataLoader（keyed by `activity_id`），或 field-level complexity extension 去 bound `activities × points` 個乘積。

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

除咗 N+1 fan-out，`{ activities { track } }` 而家係 **2N** query —— 每個 activity 一個 COUNT 加一個 SELECT。`models.Activity.num_points` 已經存住該數目，可以直接消走個 COUNT；徹底解法係 window function。

### AU-049 補充（實測確認）

W-008 令 flag 喺 `schema.py` 讀取之後，`src/api/app.py:28` 嘅 `create_app(graphiql=True)` 成為**唯一一條可以喺 introspection 關閉之下仍然服務 IDE 嘅路徑**。實測：`create_app(graphiql=False)` GET → 404（生產路徑）；`create_app()` GET → **200**（預設值路徑）。目前潛伏 —— `api/graphql.py` 係唯一 production caller 且傳 `graphiql_enabled()`。

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
| **CUI-0006** | 🟢 Low | `parse_datetime` 嘅 ISO-with-offset 分支無視 `timezone` 參數 | ✅ **Done** `323fba9` |

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
| **CUI-0012** | 🟡 Medium | 三處無防護 `.find()`：`hourly.py:57` 硬崩、`warnings.py:55` **靜默錯**、`warnings.py:70` 硬崩 | ✅ **Done** `f392d19` `ce8c4ff` |
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

## AU-048 + CUI-0006 修復摘要（2026-09-14）

Tests **283 → 294**（+11）｜TOTAL 94%｜static JSON byte-identical（`activities.json` 同抽樣 track 檔逐 byte 相同 — main agent 獨立確認）。

### 捏造常數：由「上手同事聲稱」升級為「實測證實」

我要求 developer 自己複核而唔好信 ticket。佢冇抽樣，係 parse 咗成個真實檔案：`data/raw/garmin/summarized_activities.json` 有 **660 個** `beginTimestamp`，`1634422256000` 係其中第一個，而 `1634451056000` **一個都冇**。

Main agent 再獨立 grep 確認：真常數出現 1 次，捏造常數 **0 次**。

差額啱啱好 28,800,000 ms = 8.0 小時。`1634422256000` → UTC `22:10:56` → HK `06:10:56+08:00`；`1634451056000` → UTC `06:10:56` → HK `14:10:56+08:00`。**測試作者將 HK 牆鐘當成 UTC 編碼，令貼標籤式實作睇落係啱。**

### Epoch path 死碼三重確認（唔係靠估）

1. `grep -rn parse_datetime src api` → 只有三個 parser，全部餵 XML text
2. `grep -rn SUMMARIZED_ACTIVITIES_JSON` → 一個 hit，就係 `config.py:23` 嘅宣告本身（AU-036）
3. **全語料掃描**而唔係抽樣：`grep -rlE "<(Id|time|when|begin)>[0-9]{13}<"` 過晒 1095 個 raw 檔 → 零 match。TCX/GPX 餵 `…Z` 毫秒，KML 餵 `+08:00`

### 負 offset 判別做得準

`"+" in rec_time` 確實漏咗 `-05:00`（實測 raise `ValueError`）。但 `-` 亦出現喺日期部分，唔可以簡單加。Developer 嘅做法：

```python
_ISO_OFFSET_PATTERN = re.compile(r"[+-]\d{2}(?::?\d{2})?$")
...
elif _ISO_OFFSET_PATTERN.search(rec_time.split("T", 1)[1]):
```

**只對 `T` 之後嘅時間部分做 match**，日期嘅 `-` 永遠唔喺範圍內。佢仲主動指出：呢個 pattern 如果套喺成個字串會 match 到 `2021-10-17`，正正就係要 post-`T` 嘅原因。

### 修復後五條 path（main agent 獨立實測）

| Input | HK / NY / UTC 三個參數下嘅絕對時刻 |
|---|---|
| `2021-10-16T22:10:56.000Z` | 全部 `22:10:56Z` |
| `2021-10-17T06:10:56+08:00` | 全部 `22:10:56Z`（修復前 NY/UTC **無視參數**） |
| `2021-10-17T06:10:56-05:00` | 全部 `11:10:56Z`（修復前三個都 **`ValueError`**） |
| `1634422256000` | 全部 `22:10:56Z`（修復前 HK 係 `14:10:56Z`，**早 8 小時**） |
| `2021-10-17 06:10:56`（naive） | 隨 zone 變（`22:10:56Z` / `10:10:56Z` / `06:10:56Z`）—— **正確**，naive 本質就係 zone-relative |

四條絕對時刻 path 而家喺任何 `timezone` 參數下都落喺同一瞬間。

### 釘死測試已按其 docstring 指示更新

`TestParseDateTimeWallClockUnchanged` 原 docstring 明文寫住「AU-048 must update this class in the same change — a red test is the expected outcome, not a regression to revert」。Developer 照做：常數換成真值，兩段「刻意釘死錯誤行為」嘅文字刪走，改成「四種 input 編碼同一瞬間，所以每條 path 都要落喺同一個 HK 牆鐘」。`time.py` 入面嘅 `KNOWN INCORRECT` 註解一併移除。另外兩處用咗捏造常數嘅測試同 docstring 例子亦一併改正。

---

## 新開

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0016** | 🟢 Low | `parse_datetime` 唔接受冇毫秒嘅 ISO UTC（`2021-10-17T06:10:56Z`） | pending |

**同 CUI-0006 同一類缺口但唔同條件**：CUI-0006 係「有 offset 但係負數」，呢條係「係 UTC 但冇毫秒」（第一個分支要求 `"." in rec_time`）。兩者都係**分支條件用表面特徵代替真正判別**。

建議唔好再加 `elif` —— 五種形態用四個特徵條件去分已經證明會漏。應改為先試 `isoparse()` / `fromisoformat()`，由 library 認 ISO 變體，失敗先落 fallback 處理 epoch-ms。目前唔可達（1095 個 raw 檔零 match），但 `…Z` 冇毫秒係好常見嘅合法 ISO 形態。

---

## CUI-0012 + AU-049 修復摘要（2026-09-14）

Tests **294 → 310**（CUI-0012 +11、AU-049 +5）｜三個 collector 連新 helper 全部 **100%**｜**TOTAL 95%**｜static JSON byte-identical。

> ⚠️ **Test count 記錄慣例**：本檔案早期喺多處硬寫 test 數（其中「185」過時咗好耐，被兩條唔同 lane 各自指出一次）。由今次起，**test count 只記喺呢個「最新狀態」表**，唔再散落各段。

| 指標 | 目前 |
|---|---|
| Tests | **310 passed** |
| Coverage TOTAL | **95%** |
| Static JSON vs 基準 `1896778` | **byte-identical**（6,857,102 bytes / 370 files，只差 `generated_at`） |

### CUI-0012 — marker 嗰個「靜默錯」有咗實證

Developer 嘅 red evidence 直接證明咗 ticket 講嘅「靜靜攞返錯資料」：marker 缺失時，**舊 code 回一個完整成形嘅 warning**（由 offset-31 切片 scrape 出嚟）：

```
E  assert [WeatherWarning(date='2021-01-01', warning_type='Unknown',
           warning_signal='COLD WEATHER WARNING', ...)] == []
```

Main agent 獨立重現：fixture 確實冇 marker，舊 code 確實由**第 31 個字元**開始 parse，修復後回 `[]` 並記 WARNING 講明日期同缺失標題。

**決定：明確空結果 + WARNING，唔 raise。** 條線同 AU-013 一致（回應到咗但用唔到 = 數據問題）。Developer 加咗一個我冇諗到嘅論據：`fetch_range()` 逐日呼叫 `fetch_day()`，raise 會令**第一個改版頁面就賠上全部 365 日** —— 正正就係呢張票開頭 #1 講緊嗰種 all-or-nothing 失敗。

### 新 helper 放位有論證

新開 `src/weather/collectors/html_reads.py`（`child_string` / `child_attr`，兩者對「唔存在」都回 `None`，由 caller 決定缺失代表乜）。

- **唔放 `src/common/`**：bs4 會違反 `tests/test_api_imports.py` 釘死嘅輕依賴約束（同 `common/numeric.py` 嘅 stdlib-only 約束同源）。Main agent 已驗證該測試仍然 1 passed。
- **同 `base.py` 嗰套唔同**：`xml.etree` 只有一個失敗模式（`find()` 回 None）；BeautifulSoup 有兩個 —— `find()` 回 None，**或者**回一個 `.string` 本身係 None 嘅 Tag。
- **冇加 `required_*` raising variant**：呢三處全部係 degrade 而唔係 reject。

Developer 仲更正咗自己第一版嘅錯誤假設：佢原本以為 mixed-children 嘅 `<script>` 會令 `.string` 係 None，實測 bs4 4.15 之下 `html.parser` 將 script 內容當 raw text 處理，`.string` 係 non-None。測試改為 empty-`<script>` 個 case。

### 一個超越行覆蓋率嘅檢查

`child_string` 嘅「存在但空」同 `child_attr` 嘅「存在但冇 attribute」兩條分支，喺 **line 同 branch coverage 都係 100%** 嘅情況下**其實從未被真正行過**（單行 guard）。Developer 冇收貨，直接釘死兩者並做 mutation check：拆走任一 guard 就有對應測試變紅，裝返就 6 條全綠。

### AU-049 — 採用方案 (b) 而非 ticket 原文建議

Ticket 原文建議「翻轉預設值為 `False`」。最終採用 **(b)**：`graphiql: bool | None = None`，`None` 跟 `RUN365_GRAPHIQL`。

理由（developer 提出，main agent 接納）：
1. `src/api/schema.py:60-63` 已經明文寫住「One variable, not two」—— `create_app(graphiql=...)` 正正就係第二個變數，(a) 令呢句 docstring 繼續講大話
2. (a) 完全冇處理**反方向 desync**：`create_app(graphiql=True)` + env 未設 = 服務一個 introspection 被封嘅**廢 IDE**，QA 已實測記錄
3. **有真實 caller 依賴預設值**：`README.md:132` 記錄嘅 `flask --app run365days.api.app:create_app run` 唔傳參數，(a) 會令佢靜靜雞冇咗 IDE

Main agent 獨立實測四個組合全部符合預期，同一 process 內翻 env 得 `(404, 200)`（確認冇喺 import 凍結），introspection 行為（W-008）逐字未變。

`README.md:132` 因本改動而變錯，已喺**同一條 branch** 補返（amend 而非另開 commit —— 原 body 寫住「README 唔喺本 lane」，補做後嗰句變假話）。

---

## 新開

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0017** | 🟢 Low | `hourly.py` script 字串切割兩個 `str.find()` 可返 `-1`；`_load_signal_metadata()` 無防護索引 | pending |

**同 CUI-0012 嘅 marker 完全同一類** —— `str.find()` 返 `-1` 被當成有效位置。呢個 pattern 喺本 repo 已經出事一次，ticket 明確建議**唔好再用 `str.find()` + slice**，改用 regex 並喺 match 唔到時明確回 `None`。
