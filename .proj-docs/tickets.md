# Ticket Registry — Run365Days

**最後更新**：2026-09-14（P1 全清 + P2 Round 1：AU-019/025/026/027/029/036/037 + CUI-0021/0029 完成）

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
| **S-005** | P2 | `src/cli/process_activities.py:55-58` 寫 0 行 JSONL 仍然 exit 0 | ✅ **Done** `2d69a9d` |
| **S-007** | P2 | `_SUMMARY_ROW_COLSPAN` 喺 `kml.py` 身兼三個無關語義 | ✅ **Done** `3564f53` |
| **S-009** | P3 | KML 時間戳被解析兩次 | ✅ **Done** `adb2052` |
| **S-011** | P2 | static mode 仍然當 `points: 0` 為「攞全部」，api mode 已拒絕 | ✅ **Done** `b82e6f5` |
| **S-012** | P3 | 四個上限只存在於 Python docstring 同 runtime error，冇寫入 SDL description | ✅ **Done** `4bc7c0b` |

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
| CUI-0003 | 🟢 Minor | depth limit 實際永遠唔會觸發（schema 最深 4 層，limiter 要 `max_depth=3` 先拒） | ✅ **Done** `75abcda` |
| CUI-0004 | 🟢 Minor | `track(points: 1)` 只回最後一點，同 docstring 承諾不符 | ✅ **Done** `abc591c` |
| CUI-0005 | 🟢 Minor | `app.test_client()` 過 ~130 request 洩漏 session（**實測係 50，浮動**；真 WSGI server 400/400 正常） | ✅ **Done** `2f9ced4` |
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
| **CUI-0008** | 🟢 Low | `haversine_distance` 對非有限座標只出 `RuntimeWarning` 唔拒絕 | ✅ **Done** `07f158b`（連 AU-039 向量化） |

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
| **CUI-0013** | 🟢 Low | `collect_weather.py` **45%**（唔係 0%）+ `print()`；`_REQUEST_TIMEOUT` 三份重複；magic index | ✅ **Done** `a27690d` `40a1414` `d658bfb` |

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
| **CUI-0014** | 🟢 Low | `from_raw_row()` 對缺失 STRING 欄位預設 `""` 而舊 code 出 `None`（實際係**七**個欄位唔係五個） | ✅ **Done** `a5ed956`（接受 `""`） |

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
| **CUI-0015** | 🟢 Low | `docs/architecture.md:142` 嘅 CI 描述**同一 session 內過時兩次**，應改為自動同步 | ✅ **Done** `10a1b08` `d4b9da4` |

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
| **CUI-0016** | 🟢 Low | `parse_datetime` 唔接受冇毫秒嘅 ISO UTC（`2021-10-17T06:10:56Z`） | ✅ **Done** `978b7d6` `226a197` |

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
| **CUI-0017** | 🟢 Low | `hourly.py` script 字串切割兩個 `str.find()` 可返 `-1`；`_load_signal_metadata()` 無防護索引 | ✅ **Done** `2fef340` `d755436` |

**同 CUI-0012 嘅 marker 完全同一類** —— `str.find()` 返 `-1` 被當成有效位置。呢個 pattern 喺本 repo 已經出事一次，ticket 明確建議**唔好再用 `str.find()` + slice**，改用 regex 並喺 match 唔到時明確回 `None`。

---

## CUI-0016 + CUI-0017 修復摘要（2026-09-14）—— 「消除表面特徵判別」一輪

用戶指定一次過做，目標唔係補兩個 edge case，而係**拆走兩個會反覆漏嘅機制**。

| 指標 | 目前 |
|---|---|
| Tests | **362 passed**（310 → 356 → 362） |
| Coverage TOTAL | **95%** |
| 三個 collector + `html_reads.py` | **全部 100%** |
| Static JSON vs 基準 `1896778` | **byte-identical** |

### CUI-0016 —— Developer 推翻咗 ticket 寫嘅執行次序，而且啱

Ticket（main agent 寫）指示「**先試 `isoparse()`**，失敗先落 fallback 處理 epoch-ms」。

Developer 寫代碼之前先量度，發現 **`isoparse` 識讀 ISO basic format，所以 13 位數字可以被拆成 `YYYYMMDD` + 時間**。20 萬個 epoch-ms 樣本入面約 **0.5%** 會被當成日期接受。

**Main agent 獨立重現**：1068 / 200,000 = **0.53%**，例如 `1981082071747` → `1981-08-20T17:47:00`。

即係 ticket 寫嗰個次序會**靜靜咁將約 1/200 個 epoch 時間戳讀錯年份** —— 一個舊 code 冇嘅回歸。所以改為 **epoch 先**。

Developer 主動論證咗呢個唔係退回 sniffing：**「13 個 ASCII 數字」係 epoch-ms 形態嘅完整規格，唔係一個代替 parse 嘗試嘅代理特徵。** 兩個碰撞值已寫成回歸測試。

#### Parser 選擇避開咗一個陷阱

`fromisoformat()` 要 Python **3.11+** 先支援 `Z`，而 `pyproject.toml` 宣稱 `>=3.10`。本機 3.11、CI 3.12 —— **兩者都唔會踩到 3.10**，所以揀佢會令「宣稱支援 3.10 但實際爆」永遠唔會被發現。改用 `isoparse()`（已係現有依賴、版本無關）。

#### 一個明講出嚟嘅行為放寬

date-only `"2021-10-17"` 之前 raise，而家回午夜。現行資料唔可達（byte-identical 確認）。Developer 冇收埋，寫入 matrix 同 follow_up。

### CUI-0017 —— 揾到第四個實例，而且 sentinel 係 load-bearing

除咗 ticket 列嘅兩處，加上 main agent 核實時發現嘅 `warnings.py:55` `rfind`（共三處，全部已修），Developer 做全域掃描時揾到**第四個**：

`hourly.py:112` `wind_text[wind_text.find("°") + 1 :]`

**Main agent 實測確認**：
```
'N° 20 Km/h'          find(°)=  1  -> 切片由 2 開始 -> ' 20 Km/h'
'Variable at 20 Km/h' find(°)= -1  -> 切片由 0 開始 -> 'Variable at 20 Km/h'
```

「Variable at」form **根本冇 `°`**，佢 parse 得到純粹因為 `-1 + 1 == 0`。呢個係四個實例入面**唯一一個 sentinel 正喺度做有用嘢**嘅 —— 現行正確，但建立喺意外之上。而且有條測試正釘住呢個意外，所以任何「加 guard 令佢更安全」嘅改動會即刻打爛 Variable form。

已開 **CUI-0018**。

#### 三處修法各自對症

| 處 | 修法 | 點解 |
|---|---|---|
| `hourly.py` script | regex | match 咗就係 match 咗 —— 「讀唔到」唔再係算術可達嘅結果。而且 fixture 證明舊 code 對一個**完全冇 icon call** 嘅 script body 會報 `"Rain"` |
| `warnings.py` `rfind` | `PurePosixPath(src).stem` | **移走 sentinel 而唔係檢查佢** —— 根本冇 `-1` 可以洩漏 |
| `_load_signal_metadata` 索引 | `cells(parent, minimum)` helper | 沿用 `html_reads.py` 既有契約（`None` = 唔存在，由 caller 決定意義） |

---

## 新開

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0018** | 🟡 Medium | `hourly.py:112` wind `find("°")` —— `-1` sentinel 係 load-bearing，refactor 就會靜默出錯 | ✅ **Done** `f6dce6b` |

---

## AU-047 評估更正（2026-09-14）

Main agent 曾兩次向用戶建議：「`models.Activity.num_points` 已經存住 track point 數，可以直接讀返、慳走 `_even_sample_filter` 嗰個 `SELECT COUNT(*)`，由 2N 變 N。」

**實測之後推翻 —— 呢個做法係錯嘅。**

`records.py:120` 嘅 `num_points` 係**降採樣之前**嘅原始點數，而儲存行數受 `--points 600` 封頂。365 條入面有 **2 條**唔一致（`6701104700`：653 vs 600；`7264441638`：**1250** vs 600）。

用佢代替 COUNT 會以 1250 為總數去算 stride 位置，但實際只有 600 行 —— **而錯嘅正正係點數最多、最需要降採樣嗰兩條**。

已開 **CUI-0019** 處理語義問題。**AU-047 喺 CUI-0019 解決之前，唔可以走呢條捷徑**，真正解法仍然係 DataLoader 或 window function。

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0019** | 🟡 Medium | `num_points` 係降採樣前點數，但讀落似「可取得點數」；已經經 GraphQL 到咗前端 | ✅ **Done** `209ea43`（方案 D：整條移走） |

---

## CUI-0015 修復摘要（2026-09-14）

Tests **362 → 381**（+19）｜byte-identical｜新 CLI `run365-ci-docs`，`pages.yml:70` 已接入 CI。

### 我只寫目標唔寫方法，結果比我構想嘅好

Ticket 原本列咗三個方向（生成 / drift 檢查 / 收窄描述）。Developer **三個都唔係單獨採用**，並逐個講出點解唔夠：

| 方向 | 點解單獨唔夠 |
|---|---|
| 只做生成 | 塊外面嘅散文仍然自由咁同塊入面矛盾 |
| 只做收窄 | 刪走讀者想要嘅資訊；而且 W-001 已證明「啱但冇釘住」嘅句子捱唔過一條 lane |
| 只做 drift 檢查 | **workflow 由頭到尾自己同自己一致，落後嘅係英文** —— 呢個檢查喺今次乜都捉唔到 |

最終做法：**生成 + 收窄一齊用** —— 每份文件將 branch 事實**只講一次**，放喺 `<!-- ci-facts:start/end -->` 塊入面由 `pages.yml` 生成，塊外嘅散文改到唔提 branch 名。

### 三條規則各自對應一個真實漂移過嘅位

1. **塊同步** —— 正正係 AU-004 同 W-011 打爛嗰樣
2. **禁止塊外出現 `refs/heads/...`** —— 一個具體 gate 唔可以喺冇嘢驗證嘅地方被引述
3. **README badge `?branch=` 要等於部署 branch** —— 呢個聲明本來**冇任何人擁有**，而佢會喺錯嘅 branch 上render 出一個綠剔

另加兩條 workflow 自檢（取自 `deployment.md` 已有嘅半途切換警告）：`build` 同 `deploy` 必須用同一個 gate；檔案入面每個 `refs/heads/` 都要指向同一條 branch 且該 branch 喺 push trigger 清單內。

### Fail-closed 設計

Reader 只 match 佢需要嗰幾個 key，搵唔到就 raise —— **workflow 一改結構就 CI 紅，而唔係靜靜咁用預設值報一個佢從未驗證過嘅「一致」**。

### 冇加新依賴

PyYAML 喺呢個環境只係 pre-commit 嘅 transitive，**唔係 package 宣告嘅依賴** —— 所以 reader 用純標準庫寫。

### Gate 實證（main agent 獨立三次整壞）

| 整壞方式 | 結果 |
|---|---|
| `pages.yml` 部署 branch `develop` → `master` | ❌ exit=1，三份文件 + badge 都報 |
| **只改 `build` 唔改 `deploy`**（半完成切換） | ❌ exit=1，`error: the workflow names 2 branches... Every gate must agree on a single deploy branch` |
| 手改文件塊內文字 | ❌ exit=1 |
| 還原 | ✅ exit=0 |

第二個特別有價值 —— 嗰個正正係 `docs/deployment.md` 警告過但**之前冇任何嘢擋住**嘅失敗模式。

### 邊界誠實

**冇覆蓋而且明講咗**：Vercel production branch（喺 dashboard，checkout 讀唔到）、`github-pages` environment rule（同上）、`tag-release.yml` 嘅 `ref` default（tool 只讀 `pages.yml`）。六件人手項目全部保住，其中兩件 repo 外嘅仍然標明人手。

順帶刪咗 `architecture.md` 嘅「(133 tests)」—— 實際 381，而**散文入面手維護嘅數字係同一個缺陷嘅縮影**，唔值得為佢再開一個 gate。

---

## CUI-0018 + S-005 修復摘要（2026-09-14）

| 指標 | 目前 |
|---|---|
| Tests | **392 passed**（381 → 385 → 392） |
| Coverage TOTAL | 93% |
| `src/cli/process_activities.py` | 56% → **96%** |
| 三個 collector + `html_reads.py` | 維持 **100%** |
| Static JSON vs `1896778` | byte-identical |

### CUI-0018 —— 舊 code 唔止靜默，係**靜默俾一個錯嘅數字**

Ticket 寫「最壞情況回 `None`」。實測更差：

```
舊 code: 'Variable at 20 mph' -> 切出 '2' -> 回 2.0 Km/h
```

`.replace()` 掃走前綴之後 `[:-5]` 多食一個位，所以一個寫住 20 mph 嘅 cell 會報 **2.0 Km/h**。Main agent 獨立重現。

修復後：`24.0` / `20.0` / `None` + WARNING（「states neither a bearing nor a variable direction」）。

**揀 regex 而唔係 `if "Variable at" in ...` 分支**，第一條理由最有力：分支寫法**唔會真係移走 sentinel** —— bearing 路徑仍然要搵 `°`，即係要自己加 `find(...) != -1` guard，等於將要拆走嗰個算術重新 import 再貼膠布。

速度 pattern 用 `\d+(?:\.\d+)?` 而唔係 `[\d.]+` —— match 到就保證 parse 到，所以 cast 用純 `float()`；用 `to_float()` 會加一個**永遠唔會觸發嘅 guard**。

**F-2 部分做並講得出界線**：wind 個 `[:-5]` 順手做咗（regex 自然帶出，而且令第三個 case 可示範）；temperature `[:-2]` / humidity `[:-1]` **冇做**，因為加 WARNING 會撞爛兩條現有「exact warning count == 1」斷言，diff 翻倍。

**一個誠實嘅未知**：`'Calm'` 回 `None` + WARNING（同舊 code 一樣，冇回歸）。Developer 指出語義上應該係 `0 Km/h`，但**冇編呢個映射** —— 冇證據該 form 存在，而憑空加映射就係呢張票要拆走嗰種估估下。

✅ **`find()` / `-1` 家族四個實例到此全部處理完**（CUI-0012 ×1、CUI-0017 ×2、CUI-0018 ×1）。

### S-005 —— 三個判斷位都揀得有理由

| 判斷 | 決定 | 理由 |
|---|---|---|
| 「0 條」vs「357/365」 | **只睇 `len == 0`**，冇門檻 | 冇 magic number 要維護，357 亦唔可能漂入範圍 |
| 零記錄時寫唔寫 | **唔寫** | 將舊有好檔案 truncate 成空再 exit 非零，比原本個 bug 更差。`export_data` 亦係喺 write 之前 exit |
| `--format all` 一個空 | **跑埋三個先報** | 三個 format 互相獨立；「全部空」= `RUN365_DATA_DIR`/年份錯，「一個空」= 嗰個 parser/資料夾問題 —— **fail-fast 會毀掉呢個分辨訊號** |

Main agent 獨立驗證三個情境（**注意：要避開 pipeline `$?` 陷阱**，`cmd \| tail` 攞到嘅係 `tail` 嘅退出碼）：

```
正常年份 2021    exit=0   KML 357（冇誤觸發）
零記錄 2030      exit=1   列出三個 format，冇寫任何檔案
目錄唔存在        exit=0   維持 continue，唔算失敗
```

---

## 新開

| ID | 級別 | 標題 | 狀態 |
|---|---|---|---|
| **CUI-0020** | 🟢 Low | `--year` help text 講「其他年份會 skip」，實際係**下限**過濾 | ✅ **Done** `878f905` |

實測：`--year 1999` 保留全部 365 條 2021 activity。Developer 就係因為呢點，砌真實零記錄案例時要用 `2030` 而唔係 `1999`。

### S-005 順帶回答咗一個我叫佢諗嘅問題

我問「呢個 CLI 輸出冇下游消費者，加 guard 係咪令佢定位更尷尬」。佢答：**係，而且 guard 令矛盾更尖銳而唔係解決咗**。

`grep` 確認 `TCX_JSON` / `GPX_JSON` / `KML_JSON` 三個 config 常數**只被 `process_activities.py` 自己引用**。加完 guard 之後，呢個 CLI 誠實嘅描述係「一個順便留低三個檔案嘅 parser smoke-test」—— 佢唯一真實價值係**全 repo 唯一行使 `KMLParser` 嘅入口**，而 guard 正正將呢點變成一個真檢查。

兩個自洽嘅終局（另開一票）：(a) 正式變成驗證指令；(b) 令 `export_data` 讀呢啲 jsonl 而唔係重新 parse，同時消除雙重 parse。**唔應該**維持現狀 —— 一個產出冇人讀嘅 CLI。

---

## P3 一次清 + AU-035 + CUI-0019 修復摘要（2026-09-14）

五條並行 lane 清晒餘下 12 張 P3，加 main agent 做嘅 AU-035 文件同步，再加用戶拍板嘅 CUI-0019。

### ⚠️ 新基準（本 session 第一次、亦係唯一一次刻意打破 byte-identical）

| 項目 | 舊基準 `1896778` | **新基準 `772b6ca`** |
|---|---|---|
| 總 bytes | 6,857,102 | **6,850,896** |
| 檔案數 | 370 | 370（不變） |
| `activities.json` | 152,649 | **146,443** |

**由今日起，byte-identical gate 對比嘅係 `772b6ca`，唔再係 `1896778`。**

打破基準之前先寫低精確預測，收貨時逐個對：四個數字全中，而且差異證明到**只係** `num_points` 一個 key ——
`re.sub(r'"num_points":\d+,', '', 舊檔)` 逐 byte 等於新檔，365 行每行只差呢個 key，其餘 368 個檔案完全冇郁。
呢個做法值得沿用：**「接受基準會變」同「知道佢會變成點」係兩件事**，後者先捉得到夾帶。

| 指標 | 開始 | 完成 |
|---|---|---|
| Python tests | 392 | **459** |
| Frontend tests | 87 | **96**（19 files） |
| Coverage TOTAL | 93% | **94%** |
| `api/schema.py`、`api/service.py`、`api/app.py`、`api/db.py`、`common/geo.py` | — | **全部 100%** |
| `cli/collect_weather.py` | 45% | **98%** |

### Lane 分工同衝突分析

| Lane | Tickets | 擁有檔案 |
|---|---|---|
| A | CUI-0003, CUI-0004, S-011, S-012 | `api/schema.py`、`api/service.py`、`frontend/.../source.ts`、`tests/test_api.py` |
| B | CUI-0005 | `api/app.py`、`api/db.py`、新測試檔 |
| C | S-007, S-009, CUI-0020 | `parsers/kml.py`、`cli/process_activities.py` |
| D | CUI-0013, CUI-0014 | `cli/collect_weather.py`、`weather/**`、`common/config.py` |
| E | CUI-0008, AU-023, AU-024 餘 | `common/geo.py`、`activities/metrics.py`、`src/__init__.py` |

AU-035 **刻意唔開 lane** —— 佢同 Lane E 嘅 `metrics.py` 生死、Lane C 嘅 `--year` 講法、`ci-facts` 塊全部有交集，
由 main agent 喺全部 merge 之後做，令文件描述**最終狀態**而唔係中途狀態。事後證明係啱：`metrics.py` 真係刪咗，
如果 docs lane 同時跑就會寫錯。

---

## ⛔ Main agent 派咗一個由頭到尾唔可能綠嘅 gate（派咗五份）

我喺五條 lane 嘅 brief 都寫咗：

```
ruff check . && ruff format --check .
```

**呢個 gate 喺 base commit `e844926` 已經 exit=1，531 個 pre-existing error，100% 喺 `legacy/`**（N816 232 個、E501 127 個…）。
CI 實際跑嘅係 `.github/workflows/pages.yml:54,57` 嘅 `ruff check src tests` / `ruff format --check src tests`，兩條都 exit=0。

三條 lane 喺我發更正之前已經自己量到並改用 CI 形式。最壞情況冇發生（冇 lane 為咗令個 gate 變綠而去改 `legacy/`），
但呢個係本輪最大嘅單一失誤 —— **一個錯 gate 乘以五**。

根因（Lane E 追到）：`legacy/` 嘅排除只寫喺 `.pre-commit-config.yaml` 嘅 `exclude:`，**冇寫入 `[tool.ruff]`**，
所以 bare `ruff check .` 永遠唔可能通過。已開 **CUI-0022**。

---

## 四次 lane 推翻 main agent 而且係啱嘅

| 我寫 | 實測 |
|---|---|
| 「KML parser 直接喺 export path 上」 | `src/cli/export_data.py` **零 KML 引用**（只用 TCX + GPX）。即 static JSON gate **結構上捉唔到 KML regression**。Lane C 自己補 `activities_kml.jsonl` md5（`fd5e260f…` / 357 records），main agent 獨立重跑脗合 |
| 「`metrics.py` 可以搬入 `legacy/`」 | `legacy/met.py` **已經存在**（127 行），而 `legacy/README.md:19` 寫住 `run365days.activities.metrics` 係**佢嘅取代品**。搬入去 = 將取代品擺返被取代品隔籬，仲會拖第 5 份 `datetime(1900,1,1)` idiom 入去，**而且 AU-024 嗰個重複唔會消失** |
| 「CUI-0018 冇做溫度/濕度，因為加 WARNING 會撞爛兩條 exact-warning-count 斷言」 | **假**。兩個 fixture 都冇 unreadable 溫度/濕度，所以唔會多出 record。Lane D 對 `test_weather_collectors.py` 係**純新增、零刪改**，八條 exact-count 斷言全部原封不動 |
| 「CUI-0003 要補一條真正行過 limiter 拒絕分支嘅測試」 | 嗰條測試 base 已經有（`build_schema(max_depth=2)` 真係會拒）。真正缺口係**佢只證明一個人為造淺嘅 schema 會拒絕，從未證明出貨嗰個會** |

### AU-035 我自己亦冇照抄 audit —— 佢兩處寫錯

| Audit 講 | 實際 |
|---|---|
| BMI 用 `config.BODY_HEIGHT_CM` | 用 `weight/analysis.py:21 _HEIGHT_CM_DEFAULT = 170.0`。而 `BODY_HEIGHT_CM` **零消費者**（AU-036 死常數） |
| api 仲依賴 `export.sqlite`、`dashboard.builder` | `export.sqlite` ✅，但 **`dashboard.builder` 根本冇被 api import**（只有 `dashboard.stats`） |

另外按 CUI-0015 嘅教訓，將散文入面兩個手維護嘅 test count（README 寫 93 / 87，實際 455 / 95）**刪走而唔係更新** ——
一個由人手維護嘅數字係同 branch 名一樣嘅缺陷。

---

## 各 lane 值得記低嘅發現

### CUI-0003 —— depth limiter 對 introspection 完全豁免

Lane A 讀咗 `strawberry/extensions/query_depth_limiter.py` 而唔係靠估，揾到決定性事實：`is_introspection_key` 令 limiter
**完全跳過 introspection**，實測 `max_depth=1` 之下完整 introspection query 照過。

即係「留 headroom 保護 GraphiQL」呢個唯一理由**根本唔成立**。而喺一個 acyclic graph 加 introspection 豁免之下，
**任何高過 graph 深度嘅 limit 都永遠拒絕唔到嘢**。所以夾到 `MAX_QUERY_DEPTH = 4`。

三條測試取代原本一條，最有價值嗰條係**由已發佈嘅 SDL 推導最深 chain 再同常數比對**（而且對 cyclic graph 有斷言而唔係遞歸落去）。
實測 tripwire 真係會響：喺 `RunWeather` 底下多加一層 composite →
`AssertionError: deepest query the schema allows is ... at depth 5, but MAX_QUERY_DEPTH is 4`。

**Lane A 自我更正咗一次**：佢第一版將 `track` field description 改成「first and last always included」——
**正正就係 CUI-0004 要修嗰個 overclaim**。佢自己 revert 咗，將 edge case 全部放喺 `points` argument description，
令兩段描述唔可能互相矛盾。

### CUI-0005 —— 三個 main agent / QA 都漏咗嘅點

1. **「第 ~134 個 request 開始爆」係浮動數** —— 實測第一次失敗喺 **request 50**。leaked Session 幾時被 GC 回收會影響幾時爆，
   即係話呢個 bug 本身 flaky，更難查。
2. **失敗係靜默嘅** —— pool 爆咗之後 HTTP status **仍然係 200**，錯誤收喺 `{'data': None, 'errors': [QueuePool limit...]}`。
   任何只 check status code 嘅 harness 會報「400/400 全過」然後收到 341 個 unique id 當 365。已開 **CUI-0021**。
3. **順帶拆咗一個 production 地雷** —— `get_context` 喺 `execute_operation` **之前**行，而 Strawberry 撞到 `HTTPException`
   （malformed body / 冇 `query` / 錯 content-type）會**掉咗個 `sub_response`**，連 `call_on_close` 一齊掉。
   今日只 leak 一個未用過嘅 Session（唔佔 connection），但只要將來有 code 喺 error 之前掂過 session 就即刻變真 leak。

**否決咗 `NullPool` 方案**，理由講得準：嗰個係令 leak **變免費**而唔係修 lifecycle，session 照樣永遠唔 close。
`src/api/db.py` 一行都冇改。

### CUI-0008 + AU-039 —— 向量化 16x 而且一個 bit 都冇郁

Main agent 獨立 A/B（134,744 座標，其中 37,565 個帶 `None`）：

```
old (DataFrame.apply)    2.412s
new (vectorised)         0.170s
bit-identical totals : 365 / 365
identical seg counts : 365 / 365
```

我 brief 特別警告過嘅「浮點運算次序一變第 6 位小數就唔同」**冇出現** —— `np.sin` array-vs-scalar 喺呢個 build
實測 20000/20000 bit-identical，而且刻意保留 pandas 嗰個會跳過 nan 嘅 `.sum()`，因為嗰個正正就係產生已 commit bytes 嘅實作。

`geo.py:45` 嗰個函數體內 `import pandas`（為 API cold-start 而設）維持喺函數體內，並補返 AU-039 要求嘅 WHY comment。

### CUI-0013 —— 真正嘅缺陷唔係 `print()`

我 brief 將重點放喺 print vs logger。Lane D 指出嗰啲 print 係進度輸出、同 `process_activities.py` / `export_data.py` 一致，
異常本來就已經行 module logger。**該檔案真正嘅缺陷係另一個**：

`_write_jsonl` 喺**未知道 collector 有冇回嘢之前**就用 `"w"` 開目標檔 ——
一個乜都攞唔到嘅 source 會將已 commit 嘅檔案 truncate 成空，然後 print `Wrote 0 records` 並 exit 0。
即 AU-002 / S-005 同一個洞，而且係喺**唯一一個入口**，偏偏 CUI-0012 之後每個 guard 都將「頁面改版」轉成空 list。

順帶：`data[11]` 係 collector 最後一個 all-or-nothing 讀取 —— 一行冇咗 wind block 就 `IndexError` 飛出 loop，賠上成年。
而家記低日期再 skip。

**溫度/濕度切片實測到嘅靜默錯**（同 CUI-0018 個 wind 完全同一家族）：

```
'52 °F' -> [:-2] -> '52 '  -> 報 52.0 °C      ← 華氏當攝氏
'24'    -> [:-1] -> '2'    -> 報 2.0 %        ← 冇 % 號就食少一位
```

### CUI-0014 —— 三個量度支撐一個「接受現狀」

1. 已 commit 資料上不可達（17,984 / 461 / 363 行，每個檔案剛好一個 distinct key set）
2. `""` **本來就係呢個格式嘅 in-band 值**，唔係 reader 發明出嚟 —— `warnings.py` 自己寫 `icon_url=""`，
   `tests/conftest.py` 亦有 literal `"Warning_Signal": ""`
3. 改成 `str | None` 會將 `None` 塞入 `export/models.py` 嘅 `WeatherWarning.signal`（`Mapped[str]`，**NOT NULL**）——
   即係用一個**真實**嘅型別衝突去換一個**不可達**嘅

而且加咗 parametrised 測試每次重新量度 key-set 呢個前提，**前提唔再成立就會自動 reopen**。

### CUI-0019 —— 方案 D 嘅決定性證據係「零 `.tsx` 消費」

`numPoints` 由 `records.py` 一路行到 `mappers.ts`，三條前端 query 每次都攞，但 main agent 掃晒 `frontend/src` 全部
`.ts` / `.tsx`：只出現喺 codegen 生成物、型別宣告、query 選欄、測試 fixture，**任何 `.tsx` 零出現**。

所以 A（改語義）/ B（拆兩個欄位）/ C（補文件）三個方案都係**喺養一個冇人食嘅欄位**。呢個選項之所以到最後先講得出，
正正因為「有冇人 render」呢點要掃過先知 —— 而三張前置票都冇掃。

Lane 亦實測確認冇留半截：`PRAGMA table_info(activities)` 18 欄冇咗佢；
`{ activities { numPoints } }` → `Cannot query field 'numPoints' on type 'Activity'.`；正常 query 照行。

**AU-047 嘅捷徑至此永久消失**，真解法確定係 DataLoader 或 window function（見下面 AU-047 補充）。

---

## AU-047 補充（CUI-0019 lane 讀碼後嘅設計註記）

`_even_sample_filter`（`src/api/service.py:162-191`）**已經砌緊一個 `row_number() OVER (ORDER BY seq)` subquery** ——
即係 window function 嘅機件已經有一半喺度。一個 `PARTITION BY activity_id` 嘅單一 query 可以將 COUNT 同 sampling
摺埋一個 round trip，同時消走 2N fan-out。**未做，只係註記。**

---

## 新開

| ID | 級別 | 標題 | 來源 |
|---|---|---|---|
| **CUI-0021** | 🟡 Medium | 基建故障（pool 耗盡）回 HTTP **200** + `data: null` + `errors` —— 前端同監控都會當成功 | Lane B |
| **CUI-0022** | 🟢 Low | `ruff check .` 永遠唔可能綠：`legacy/` 排除只寫喺 pre-commit，冇入 `[tool.ruff]` | Lane E |
| **CUI-0023** | 🟢 Low | `dashboard/builder.py:31 downsample` 帶住同 CUI-0004 一模一樣嗰句 overclaim | Lane A |
| **CUI-0024** | 🟢 Low | 兩個必須一致嘅手寫 `600` 分處兩種語言，冇任何 check | Lane A |
| **CUI-0025** | 🟢 Low | `tests/conftest.py` 冇 socket guard —— 冇嘢阻止將來嘅測試打去 `127.0.0.1` 嘅 agent proxy | Lane A |
| **CUI-0026** | 🟢 Low | `export_data.py --year` 一個 flag 三個無關語義，而且**完全冇 help text** | Lane C |
| **CUI-0027** | 🟢 Low | 冇任何 CLI 設定 logging；`process_activities.py` 叫用戶「re-run with logging at WARNING」，但冇任何 flag 提供得到 | Lane D |
| **CUI-0028** | 🟢 Low | `cli/export_schema.py` **0%**、`cli/check_ci_docs.py` 62% —— CUI-0013 為 `collect_weather.py` 補好嘅同一個缺口 | Lane D |

### CUI-0024 詳情（Lane A 講得最清楚）

```
src/cli/export_data.py:27            DEFAULT_POINT_LIMIT = 600    （export 存幾多點）
frontend/src/data/api/source.ts:20   DEFAULT_TRACK_POINTS = 600   （api mode 預設攞幾多點）
```

**今日相等純屬巧合。** 調高 export limit → static mode 嘅 `undefined` 會回超過 600，而 api mode 仍然回 600 ——
**一個 S-011 形狀完全相同嘅新分歧，而且正正落喺 S-011 冇改嗰個 input 上。**

### CUI-0026 詳情

`src/cli/export_data.py:33` 嘅 `--year` 冇 help text，而且餵三個無關用途：
`:52,:54` parser 嘅**下限過濾**、`:65` `parse_weight_file(year=)` 係**貼落 bare day/month 上嘅年份**（constructor 唔係 filter）、
`:47` `config.daily_weight_file(args.year)` 係**檔案選擇器**。同 S-007 完全同一個形狀，只係高一層。

---

## 兩個只有人手保證嘅項目 —— ✅ 用戶已確認（2026-09-14）

| 項目 | 狀態 |
|---|---|
| GitHub → Settings → Environments → `github-pages` → Deployment branches 容許 `develop` | ✅ 用戶已檢查，容許 |
| Vercel → Settings → Git → Production Branch | ✅ 用戶已檢查，已經係 `develop` |

`run365-ci-docs` 明確講明佢覆蓋唔到呢兩樣（喺 dashboard，checkout 讀唔到），所以呢個確認**冇任何自動化承接**
—— 一旦有人改咗 dashboard 設定，repo 入面唔會有嘢紅。`docs/deployment.md` 嘅切換 checklist 仍然將佢哋列為人手項目，
呢點維持不變。

---

## P1 Round 1 修復摘要（2026-09-14）

### ⚠️ 先更正我自己一句話

我喺 P3 收尾時講「**AU-047 係唯一剩低嘅 P1**」—— **錯**。嗰句只覆蓋咗 CUI 系列。
Audit 原本嘅 P1 行有**七組**，逐條實測全部仍然開住。呢個係「登記處只記自己開嘅票，唔記原始 audit」造成嘅盲點。

| 組 | Findings | Round |
|---|---|---|
| 1 | AU-010, AU-011, AU-015, AU-020 | Round 2 |
| 2 | AU-006, AU-007 | ✅ Round 1 |
| 3 | AU-012 | Round 2 |
| 4 | AU-033, AU-044 | Round 2 |
| 5 | AU-008 | ✅ Round 1 |
| 6 | AU-031 | ✅ Round 1 |
| 7 | AU-032 | ✅ Round 1 |
| 8 | AU-047 | ✅ Round 1 |

**點解分兩輪**：第 1 組加 `ANN` + `S` 之後會逼幾乎每個 `.py` 檔案改動，同任何 code lane 都撞；
第 1、4、7 組又全部搶 `pyproject.toml`。所以 Round 1 只做 correctness + 部署設定，
tooling 留到所有 code 改動落地之後先做 —— 咁 annotation 加喺最終代碼上而唔係中途版本。

### ⚠️ 基準第二次改動（AU-008 lbs→kg）

| 項目 | 上一個基準 `772b6ca` | **新基準 `cc0235f`** |
|---|---|---|
| 總 bytes | 6,850,896 | **6,850,876**（−20） |
| 檔案數 | 370 | 370 |
| `activities.json` | 146,443 | 146,443（未變） |
| `weight.json` | — | **25,840** |

**只有 `weight.json` 郁**。Main agent 獨立驗證：365 個 track 檔嘅 md5 集合前後完全相同；
`activities.json` / `weather.json` / `warnings.json` 逐 byte 相同；`weight.json` 入面
`date` 同 `weight_lbs` **365/365 未變**，`weight_kg` 同 `bmi` **365/365 依新因數重算**
（`154.8 lbs` → `70.28` → **`70.22`**，kg delta `−0.07…−0.05`，bmi delta `−0.03…−0.01`）。

| 指標 | Round 1 前 | 完成 |
|---|---|---|
| Python tests | 459 | **504** |
| Frontend tests | 96 | **100** |
| Coverage TOTAL | 94% | 94% |

---

## AU-008 —— 真正嘅發現唔係常數散落，而係測試釘死咗錯答案

修復前嗰兩條應該捉到錯因數嘅測試：

```python
tests/test_weight_analysis.py:31   assert r.weight_kg == pytest.approx(154.8 * 0.454, rel=1e-3)
tests/test_weight_analysis.py:37   assert r.bmi == pytest.approx(expected_bmi, rel=1e-2)
```

`0.454` 對 `0.45359237` 嘅相對誤差係 **0.0899%**，同時細過 `rel=1e-3` 同 `rel=1e-2`。

但**寬容差只係次要**。`:31` 個 expected value **本身就係用同一個錯常數 `0.454` 計出嚟** ——
即係測試同實作各自錯咗同一個地方，兩邊自洽。

**呢個係本 session 第三次撞到「測試釘死錯行為」**：
1. AU-048 —— `tests/test_common_time.py` 用捏造常數 `1634451056000`（真值 `1634422256000`，差 8 小時），令貼標籤式實作睇落係啱
2. CUI-0017 —— 一條測試正釘住 `hourly.py:112` 個 `-1 + 1 == 0` 意外，令任何加 guard 嘅改動即刻紅
3. AU-008 —— expected value 由被測嘅錯常數推導

三次嘅共通形狀：**測試冇獨立來源，佢嘅期望值同實作嘅錯誤同源。**

### 一個關於「刪除」而唔係「修補」嘅判斷

AU-006（`build_dataframe` 靜默無視 `height_cm`）**冇**用「將 `height_cm` 傳落去第二步」去修，
而係**刪走第二次計算** —— `build_dataframe` 改為由 record 讀返 `weight_kg` / `bmi`，因此**完全唔再收 height**。

理由：咁樣「一步收參數、下一步靜靜用預設值」由「已修好」變成「結構上不可能」。同 CUI-0011 嗰個
「一個欄位一個主人」係同一條原則。

### AU-008 之後每個概念嘅唯一擁有者

| 概念 | 唯一擁有者 | 守住佢嘅嘢 |
|---|---|---|
| 預設身高 | `common/config.py BODY_HEIGHT_CM`（**由零 import 變成真有人用**） | 單一擁有者掃描 + `prefs.test.ts` 對住 SDL |
| lbs→kg（Python） | `common/config.py LBS_TO_KG = 0.45359237` | 同上 |
| lbs→kg（TS） | `frontend/src/lib/prefs.ts LBS_TO_KG` | `prefs.test.ts` 掃全部非測試 `.ts(x)`，斷言 `0.45359237` **只**出現喺 `prefs.ts` |
| 150 = 預設送幾多點 | `dashboard/builder.py TRACK_POINT_LIMIT` | 單一擁有者掃描 |
| 600 = export 存幾多點 | `common/config.py EXPORT_TRACK_POINTS` | 單一擁有者掃描 + `source.test.ts` 讀返 SDL |

`600` 放喺 `config.py` 而唔係 `cli/export_data.py`，理由係硬嘅：`schema.py` 要佢嚟寫 SDL description，
而 import CLI 會將 parsing stack 拖入 API 嘅 import graph，打爛 `tests/test_api_imports.py`。

**150 ↔ 600 嘅關係**由 `test_the_export_stores_at_least_the_served_default`（`EXPORT_TRACK_POINTS >= TRACK_POINT_LIMIT`）守住 ——
export 唔可以送一個佢從來冇存過嘅預設值。**CUI-0024 一併關閉**。

**Main agent 獨立驗證 guard 真係咬**（第一次做錯：我個 `sed` 因為多打咗 `: int` 而冇 match 到，
所以第一次「測試全綠」係無效結果 —— 重做之後）：
```
EXPORT_TRACK_POINTS 600 -> 900，重新生成 SDL
  frontend: FAIL src/data/api/source.test.ts > asks for exactly as many samples as the export stores
            AssertionError: expected 900 to be 600
  python:   FAILED tests/test_dashboard_builder.py::TestOneOwnerPerConstant::...[samples stored]
還原 -> 504 passed / 100 passed
```

---

## AU-047 —— 我兩個前提都錯，第二個更重要

### 錯一：我傳落去嘅 `PARTITION BY` 單 query 構想會**靜默改變輸出**

Python 半數進位**取偶**，SQLite 進位**遠離零**。Main agent 獨立掃描：**644 對** `(total, points)` 有分歧
（lane 用另一組 points 值掃出 547），**最細個案兩邊完全一致**：`total=6, points=3` → Python 揀位置 2、SQLite 揀 3（`2.5`）。

即係話個「捷徑」會回一個唔同嘅點。**COUNT 必須留喺 Python，只有「攞佢」呢個動作可以 batch。**

### 錯二：「500 次 round trip 要喺 15 秒 function 入面行完」誇大咗成本 —— SQLite 係 in-process，冇網絡

Main agent 用真 DB（365 activities / 134,041 track points）獨立 A/B：

| activities | before q | after q | before ms | after ms | 回傳點數一致 |
|---|---|---|---|---|---|
| 1 | 2 | **2** | 33.4 | 4.5 | ✅ 150 |
| 10 | 20 | **12** | 34.2 | 44.7 | ✅ 1,500 |
| 50 | 100 | **52** | 193.5 | 174.5 | ✅ 7,500 |
| 100 | 200 | **102** | 404.8 | 337.3 | ✅ 15,000 |
| 365 | **730** | **367** | 1411.1 | 1330.5 | ✅ 54,750 |

Query 數逐格對上 lane 報嘅。**但時間只快約 5.7%** —— COUNT 只佔 7.4%，**80% 係 row SELECT**。

### 真正嘅瓶頸，同埋點解佢冇順手做

Lane 原型咗 Core column-select：**1196 ms → 805 ms（−33%）**，同樣 query 數、同樣 rows。刻意冇做，理由：

> `tests/test_api.py:329` 靠 `loaded_as_persistent` ORM event 計數：`assert len(loaded) <= 2 * points`。
> 改用 Core select 之後個 event **一次都唔會 fire**，斷言變成 `0 <= 10` —— **測試照樣綠，但由嗰刻起乜都冇量度緊。**

Main agent 核實咗斷言原文，講法準確。而且該檔案唔喺佢 lane 內。已開 **CUI-0029**。

### 三個被否決方案都有量度撐住

| 否決 | 量度 |
|---|---|
| SQL 內做 `_even_positions` | 上述 644 對 rounding 分歧 |
| DataLoader | `Activity.track` 係 **sync** resolver，Strawberry DataLoader 只支援 async → 要改 `schema.py` |
| 每次都 `GROUP BY` | 12.19 ms（掃 134k）vs 0.35 ms（scoped seek）—— 為 list page 慳 6.6% 令單條 activity 頁慢 4.5 倍 |

順帶量到 `SQLITE_MAX_VARIABLE_NUMBER` 喺呢個 build 係 **250,000**（唔係一般以為嘅 999 / 32,766），
所以將來要做真 O(1)，**卡住嘅係 sync resolver 而唔係參數上限**。

---

## AU-031 / AU-032

### AU-031 —— 逐條 header 對住 build artifact 而唔係抄 checklist

`npm run build` 之後查實物：`dist/index.html` **零 inline `<script>` / `<style>`**；657 kB bundle **零 `eval` / `new Function`**；
CSS **零 `url()` / `@import` / `@font-face`**；`setAttribute("style"` / `insertRule` / `cssText` **全部 0**。

**我 brief 叫佢考慮「地圖 tile 外部來源」—— 根本冇地圖。** Main agent 獨立 grep：
`leaflet|mapbox|openstreetmap|tile|fonts.googleapis|cdn.` 喺 `frontend/src` 同 `index.html` **零命中**（`RouteMap` 係自繪）。
整個前端零外部資源，所以 CSP 遠比 ticket 預期簡單。

**冇因為靜態證據夠好就收緊到底**：`style-src` 保留 `'unsafe-inline'`，因為佢**載入唔到真瀏覽器**
（`playwright-core install` 俾 proxy 403 擋住），而一條錯 CSP 令頁面白畫面比冇 CSP 更差。明講咗邊部分未驗證。

**Main agent 獨立重跑 mutation check**：`script-src` 加返 `'unsafe-inline'` → `1 failed, 13 passed`；還原 → `14 passed`。

**Pages 側不對稱冇掩飾**：GitHub Pages **根本冇能力送 custom response header**，所以呢個唔係 `vercel.json` 補得到嘅洞。

### AU-032 —— 揀上限而唔係 lock，理由係硬約束

CI 行 `pip install -e ".[dev]"`（`pages.yml:51`）。**一個 lock file 除非改埋 workflow 否則完全惰性**，
而 `.github/workflows/` 唔喺該 lane 內。原話：「shipping a lock nobody installs would be worse than nothing」。
誠實講明呢個 commit **只做到收窄 blast radius，未達到 ticket 寫嘅「build 可重現」**。

順帶宣告咗 `pytest-cov`（一直冇宣告，只靠 image 預裝）。

---

## ⚠️ 一個由我落嘅 gate 造成嘅跨 worktree 污染

我要求 AU-032 lane 跑 `pip install -e ".[dev]"` 做 gate。跑完之後 editable path hook
**由主 checkout 轉指去佢個 worktree**。Main agent 實測：

```
plain import run365days -> .../.claude/worktrees/agent-a9420c53e43836d27/src/__init__.py
```

即任何**唔用 `.pkgroot`** 嘅 import 都會靜靜咁讀緊嗰棵樹。已喺主 checkout 重跑 `pip install -e . --no-deps` 復原。

**教訓：`pip install` 唔應該出現喺任何 lane 嘅 gate 入面。** `.pkgroot` 慣例保護咗其餘 lane
（`PYTHONPATH` 排喺 `.pth` 之前），但呢次係 gate 本身製造咗問題。

---

## 新開

| ID | 級別 | 標題 | 來源 |
|---|---|---|---|
| **CUI-0029** | 🟡 Medium | `track()` 用 ORM entity + 兩次掃描，佔請求 80% 時間；原型 Core select 快 33%，但改咗會令 `test_api.py:329` 個 guard 變空殼 | AU-047 lane |
| **CUI-0030** | 🟢 Low | `requires-python = ">=3.10"` 係假宣稱：numpy / pandas 都要 `>=3.11`，而本機 3.11、CI 3.12 令佢永遠唔會被發現 | AU-032 lane（Round 2 處理） |
| **CUI-0031** | 🟢 Low | 已發佈嘅 `bmi` 由**未 round** 嘅公斤計，但 `weight_kg` published 係 round 到 2dp —— client 由 `weightKg` 重新推導 BMI 會得出第三個答案（實測 16/365 行唔一致） | AU-008 lane |
| **CUI-0032** | 🟢 Low | `WeightRecord.day_number` 零讀者（`src/` / `tests/` / `frontend/` / SDL 全部冇），同 CUI-0019 同一形狀 | AU-008 lane |
| **CUI-0033** | 🟢 Low | 寬 `pytest.approx` 容差掃描 —— `rel=1e-3` / `rel=1e-2` 呢個 pattern 藏起咗 AU-008 成世 | AU-008 lane |
| **CUI-0034** | 🟢 Low | `meta.json.generated_at` 令 export 唔可能 bit-reproducible；支援 `SOURCE_DATE_EPOCH` 可令 byte-identical gate 變成真 content hash | AU-032 lane |
| **CUI-0035** | 🟢 Low | `playwright-core` 喺 `frontend/package.json` devDependencies 但零引用，而且佢個 browser download 俾 proxy 403 擋住 | AU-032 lane |

### CUI-0029 特別要留意

呢個係**第二次**見到「改動會令一條 guard 測試靜靜變成空殼」。第一次係 CUI-0017 嗰條釘住 `-1` 意外嘅測試。
分別係：嗰次個測試會**變紅**（所以有人會發現），呢次個測試會**保持綠**（所以冇人會發現）。**後者危險好多。**

---

## P1 Round 2 修復摘要（2026-09-14）—— tooling

兩條 lane：Python tooling（AU-010 / AU-011 / AU-015 / AU-020 / AU-033 / AU-044 / CUI-0022 / `requires-python`）
同前端 formatter（AU-012）。CI wiring 由 main agent 喺兩者 merge 之後自己加。

**P1 至此全清。**

| 指標 | Round 2 前 | 完成 |
|---|---|---|
| Python tests | 504 | 504（tooling 唔加測試） |
| Coverage TOTAL | 94% | **94.07%**，而且**有 gate**（`fail_under = 90`） |
| Frontend tests | 100 | 100 |
| `ruff check .` | ❌ exit 1，531 errors | ✅ **exit 0** |
| `ruff format --check .` | ❌ | ✅ 64 files |
| 前端最長一行 | **637** 字元 | **180** |
| 前端 >100 字元嘅行 | 315 / 5404 | **29** |
| Static export | 6,850,876 / 370 | **未變** |

---

## 三個我報錯嘅數字，全部係同一種錯法

| 我寫 | 實測 | 點解我會錯 |
|---|---|---|
| coverage 1825 statements | **1843** | 抄咗上一輪嘅數 |
| AU-020「20 個 function 缺 annotation」 | **35 個 ANN finding** | audit 個手數喺寫落去嗰刻已經過時 |
| AU-015「4 個 `open()`」 | **8 個 text I/O site** | 我 grep `open(`，而 `pathlib` 唔係咁串 |

第三個最值得記：我漏咗嗰四個入面有 **`src/export/static_json.py:32` 嘅 `Path.write_text`** ——
**即係寫 static JSON export 本身**。JSON 規格就係 UTF-8，一部非 UTF-8 locale 嘅機會寫出瀏覽器解錯碼嘅檔案。
一個 `grep "open("` 結構上永遠搵唔到佢。

**共通形狀：三個都係「人手數出嚟嘅數字」，而三個都喺被引用嗰刻已經錯。** 呢個正正就係 AU-011 要解決嘅嘢 ——
所以 lane 揀咗開 rule 而唔係逐點修，係啱嘅。

---

## AU-011 —— 兩條我冇要求、佢自己揾到嘅 rule

| Rule | 點解加 |
|---|---|
| `BLE` | `api/graphql.py` **本來已經有兩個 `# noqa: BLE001`**，附埋書面理由 —— 即係有作者以為佢開咗。實際冇開，所以嗰兩行係**死文字**。開咗之後零新 finding，但令佢哋嘅理由變成有效 |
| `RUF100` | 就係佢揾到上面兩個死 directive。同時保證新加嘅 5 個 suppression 保持 load-bearing |

**每個 ignore 都有理由，而且範圍收得緊**：`S101` 只 ignore 喺 `tests/*`（`assert` 就係 pytest 表達期望嘅方式），
但**唔係**整個 `S` prefix —— fixture 入面 hardcode 一個 credential 仍然會紅。
`ANN001/002/003/201` 喺 tests ignore（884 個 hit，全部係 `def test_x() -> None`），
但 **`ANN202/204/205` 唔 ignore** —— helper 嘅 return type 正正就係 caller 推唔到嗰樣，22 個 test helper 全部補齊。

### 一條 rule 試過逼佢改行為，佢停低咗

`S314` 要求將 `xml.etree` 換做 `defusedxml` —— 即係新依賴 + 換 parser。佢**冇做**，改為 inline suppress
並將 threat model 寫喺該行（輸入係用戶自己嘅 Garmin export，本機磁碟，從不下載或上傳），另開 follow-up。
符合我 brief 嗰條「tooling lane 唔准改行為」。

### Preview rule 嘅代價控制得住

`PLW1514`（AU-015）仲係 preview。淨開 `preview = true` 會拉入所有 unstable rule ——
喺一個 patch-only ruff pin 之下唔值。所以同時設 `explicit-preview-rules = true`，令 preview rule
**只有被逐個 code 點名嗰啲**先生效。實測：兩個 flag 都設、再移走 `PLW1514`，`ruff check .` 同 preview off 完全一樣。

---

## CUI-0022 —— 第二半我完全冇預料

修 `extend-exclude = ["legacy"]` 之後 `ruff check .` 綠咗。但 **`ruff format --check .` 仍然紅，喺 6 個 Markdown 檔**。

Ruff 0.16 會格式化 Markdown 入面嵌住嘅 Python code block。而本 repo 嘅 review report 同 ticket
**刻意引用殘缺片段** —— 壞代碼、半截 expression、before/after 對。Formatter 會當佢哋係完整程式重寫。

**Main agent 獨立重現**（`--diff`，冇寫入）：

```
.proj-docs/reviews/2026-09-13_review_p0-batch.md:393
-     raise ActivityParseError(f"no track points in {file_path.name}")   # ← 嗰 8 個檔案會喺度出 WARNING
+     raise ActivityParseError(
+         f"no track points in {file_path.name}"
+     )  # ← 嗰 8 個檔案會喺度出 WARNING
```

**證據必須逐字存活**，所以 `[tool.ruff.format] exclude = ["*.md"]`。冇呢行，任何人打 `ruff format .`
都會靜靜咁改寫成個 ticket 檔案庫。

---

## AU-033 —— 門檻揀 90，兩個方向都測過

| 選項 | 否決理由 |
|---|---|
| 94 | 精確係 94.031%，門檻設 94 剩 0.03pp = **零個 statement** 嘅餘裕。一條未覆蓋嘅防禦分支就令正確嘅改動變紅。**一個會喺好改動上響嘅 gate 會被人調低或刪走** |
| 80（DoD 下限） | 容許 259 個未覆蓋 statement，比今日實際存在嘅 110 個多 149 個 —— 核心可以整批失去測試而佢一次都唔響 |
| **90** | 容許 184 個，比今日多 74 個 ≈ 一個實質 module 未測就 merge，正正係值得捉嘅事件 |

**兩個方向都實測過**（一個從未見紅嘅 gate 只係一個聲稱）：

```
as committed          504 passed, 94.07%  -> exit 0  Required test coverage of 90.0% reached.
+140 unreached lines  504 passed, 81.69%  -> exit 1  FAIL Required ... not reached.
```

失敗嗰次**504 條測試全部仍然通過** —— 正正就係綠色測試套件會遮住嘅情況。

`fail_under` 放喺 `pyproject.toml` 而唔係 CI flag，理由就係 AU-010 嗰個教訓。

---

## AU-010 —— 佢改法同我 spec 唔同，而且更好

我 spec 話將 CI 改成 `ruff check src tests api`。佢改成 **`ruff check .`**。

理由：喺 workflow 寫一條 path list，係將一個住喺 `pyproject.toml` 嘅決定抄多一份 ——
**而嗰份抄本已經漂移過一次**（config 寫 `src = ["src","tests","api"]`，CI 只跑兩個）。
而家 config 係唯一擁有者，將來新增一個 top-level package 一落地即刻被覆蓋。

`64 files = src 42 + tests 21 + api 1`，證明 `.` 同 `src tests api` 係同一個檔案集。

---

## CUI-0030 —— `requires-python` 改為 `>=3.11`，`target-version` **刪除**

```
pip install --dry-run --python-version 3.10 "numpy>=2.4,<3"
  ERROR: 3.10 最新只到 numpy 2.2.6，低過宣告嘅 2.4 下限
pip install --dry-run --python-version 3.11 "numpy>=2.4,<3" "pandas>=3.0,<4"
  exit 0
```

`target-version` **刪走而唔係 bump**：ruff 會 fallback 去 `project.requires-python`，
所以呢一對唔再有第二份可以漂移（`--show-settings` 確認 `target_version = 3.11` 係推導出嚟）。

副作用：開咗 UP017，ruff 將 8 處 `datetime.timezone.utc` 改為 `datetime.UTC`，
已驗證係精確別名（`datetime.UTC is datetime.timezone.utc` → `True`）。

---

## AU-012 —— 偏離團隊規範，而且論證成立

團隊規範（`rules/global-rules.md`）寫 Prettier `tabWidth=4` + `singleQuote`。Lane 揀咗 **`tabWidth=2` + `singleQuote: false`**。

**Main agent 核實咗佢個關鍵論據**：`rules/global-rules.md` **唔喺 repo 入面** ——
佢住喺 `/root/.claude/plugins/marketplaces/claude-teams/ai-dev-team/rules/global-rules.md`，係 plugin 層預設值。
Repo 從來冇 commit 過 4-space / single-quote，而唯一一份手寫 config（`frontend/eslint.config.js`）本身就係 2-space + double-quote。

量度結果：縮排 **100% 空格且成雙**、**零 tab**；引號 **321 個 double-quote import vs 0 個 single-quote**。
硬套規範會改寫 86 個檔案幾乎每一行去改變一啲讀者睇唔出嘅嘢，**摧毀 git blame** ——
正正係 AU-012 想避免嘅傷害而唔係修復。`printWidth` / `trailingComma` 呢兩條真正無主嘅軸就跟返團隊規範。

### `Design Origin` 規則點滿足

呢張票掂到 `.tsx` 同 `.css`，所以要證明零視覺改動：

| artifact | 結果 |
|---|---|
| `dist/assets/*.css` | **逐 byte 相同**（hash `3a6a1b86…` 前後一樣）—— 直接覆蓋咗 design-source 規則嘅 CSS 半邊 |
| `dist/assets/*.js` | hash 變咗，**+6 bytes** |

JS 個 6 bytes 係兩處 3-byte `","` 插入：prettier 為咗**保住**一個佢 reflow 時會丟失嘅 JSX 空格而寫 `{" "}`，
令一個 React text child 變成兩個相鄰 text child，串接結果相同。

---

## 我自己驗證過程中犯咗兩個錯，記低

1. **第一次驗 CUI-0024 個 guard 用錯 `sed`** —— 我打 `EXPORT_TRACK_POINTS: int = 600`，實際係
   `EXPORT_TRACK_POINTS = 600`，所以 mutation 根本冇發生，而我差啲將「測試全綠」當成結果。
   重做之後兩層 guard 都確認咬到。**一個冇 assert 過「mutation 真係落咗」嘅 mutation test 係無效嘅。**
2. **`pkill -f "difflib"` 殺埋自己個 shell** —— 因為 command line 本身含住 `difflib`。

---

## 又一次跨 worktree 污染（同一個原因）

Round 2 嘅 Python tooling lane 亦要跑 `pip install -e ".[dev]"`（改咗 `pyproject.toml` 就要驗佢裝得起），
path hook 再次指去佢個 worktree，已再次喺主 checkout 復原。

**呢個係第二次。** `.pkgroot` 慣例保護到 lane，但保護唔到 main agent 自己 —— 我每次 merge 之後嘅驗證
都要先確認 path hook 指住主 checkout。已成為固定步驟。

另外發現本環境有**兩個 ruff**：PATH 嗰個 **0.15.8**，而 `pip install -e ".[dev]"` 俾 CI 嗰個係 **0.16.7**。
我頭幾次 gate 驗證跑咗 0.15.8，已用 `python3 -m ruff`（0.16.7）重驗，兩者結論一致。

---

## 新開

| ID | 級別 | 標題 | 來源 |
|---|---|---|---|
| **CUI-0036** | 🟢 Low | 冇任何 Python 版本 pin：`requires-python >= 3.11`、本機 3.11、CI 3.12、**冇 `.python-version`**，一個只喺其中一版出現嘅 bug 只有一邊會見到 | Lane P |
| **CUI-0037** | 🟢 Low | Vercel 嘅 Python 版本完全冇 pin（`vercel.json` 冇 runtime，`scripts/vercel-build.sh` 直接裝 `.`） | Lane P |
| **CUI-0038** | 🟢 Low | 3 個 `ET.parse` 用 `# noqa: S314` 長期壓住；`defusedxml` 值得獨立評估而唔係永久 noqa | Lane P |
| **CUI-0039** | 🟢 Low | `fail_under` 係地板唔係棘輪 —— coverage 可以由 94 慢慢跌到 90 而唔會有人知；要 diff-coverage 先捉到 | Lane P |
| **CUI-0040** | 🟢 Low | `"api/*" = ["D"]` 可能已經過闊：`api/graphql.py` 實際有齊 module 同 function docstring | Lane P |
| **CUI-0041** | 🟢 Low | `WeightView.tsx` 有個無大括號嘅單句 `if` 被 prettier 拆成兩行，讀落更差，亦違反團隊「所有條件都要 `{}`」規則 | Lane F |
| **CUI-0042** | 🟢 Low | `dist/assets/index-*.js` **657 kB**（gzip 204 kB），每次 build 都有 Vite chunk-size 警告，冇配置 code-splitting | Lane F |
| **CUI-0043** | 🟢 Low | pre-commit 冇 JS/TS hook；prettier local hook 可以直接加入（現有 `end-of-file-fixer` / `trailing-whitespace` 已經覆蓋 `frontend/` 而且同 prettier 唔衝突） | Lane F |

---

## P2 Round 1 修復摘要（2026-09-14）

三條並行 lane：前端 token / a11y（AU-019/025/026/027）、死碼 + SunMoon（AU-029/036/037）、API（CUI-0021/0029）。

| 指標 | Round 1 前 | 完成 |
|---|---|---|
| Python tests | 504 | **544** |
| Frontend tests | 100（19 files） | **123（23 files）** |
| Coverage TOTAL | 94.07% | **95.07%** |
| `rgba()` 硬寫（非 token module） | 24 | **0** |
| `.tsx` 入面嘅顏色 hex | 78 | **1**（`MARKER_RING = "#fff"`，具名兼有註釋） |
| Static export | 6,850,876 / 370 | **未變** |

**刻意排除**：AU-028（六個頂層 `.tsx` 搬入 `<type-group>/<name>/`）、AU-022（命名縮寫）、AU-005（`dashboard` → `analytics`）。
三者都係純搬位／改名，同任何內容改動硬撞，應該單獨一輪做，令 diff 係純 rename、reviewer 可以整段跳過。

---

## 我報錯咗五個數字（連續第二輪）

| 我寫 | 實測 |
|---|---|
| `rgba()` 硬寫 23 | **24** |
| `.tsx` hex 70 | **78** |
| token 來源 3 份 | **4 份** |
| AU-019 有 4 處 | **5 處，而且唔係我嗰四處** |
| AU-027 係「26 個 module-scope 常數」 | **20 個常數**；「26」係另一樣嘢（見下） |

**第三個係 audit 自己都漏咗嘅**：`frontend/src/lib/paceColor.ts` 藏住第四份 palette ——
```
const C_SLOW = [248, 113, 113];   // = #f87171 = --color-danger
const C_MID  = [245, 158, 11];    // = #f59e0b = --color-warn
const C_FAST = [52, 211, 153];    // = #34d399 = --color-accent2
export const NO_PACE_COLOR = "#2e3250";  // = --color-border
```
四個 theme token 換成 RGB array 重新編碼，所以任何 grep `#` 或 `rgba(` 都搵唔到佢。

**第四個係 false positive**：我將 `RecentActivities.tsx:20` 當成 a11y 違規，但 base commit 嗰度**已經係 `<button type="button">`**。
我 grep `onClick` 冇睇元素。真正五處入面有**兩處（`ActivitiesView.tsx:174` 同 `:187`）連 audit 都冇列**。

**第五個係我 framing 錯而 audit 啱**：`.tsx` 入面確實得 20 個 module-scope 常數，但**啱啱好有 26 個裸單位換算**
（`/60`、`/3600`、`/1000`、`*60`）散喺九個 view component 嘅 JSX 同 Chart.js tooltip callback 入面 ——
嗰個先係 ticket 講緊嘅「單位換算」，而 26 就係佢嘅數目。已收歸 `src/lib/units.ts`，殘留 **0**。

---

## ⚠️ AU-019 最重要嘅發現：加 linter 唔等於捉到

Lane A 裝咗 `eslint-plugin-jsx-a11y` 之後**先量度再信**：`recommended` **同** `strict` **都只捉到 5 個入面嘅 1 個**。

原因：`aria-query` 俾 `<tr>` / `<td>` / `<th>` 派咗 **interactive 嘅 `row` / `cell` role**，
所以 `click-events-have-key-events`、`no-noninteractive-element-interactions`、`interactive-supports-focus`
三條全部行過一個可點擊嘅 row 都唔出聲。

**Main agent 獨立重現**（一個淨係載入 `jsx-a11y.flatConfigs.strict` 嘅最小 config，一個裸 `<tr onClick>`）：

```
jsx-a11y strict 對 <tr onClick>  ->  exit=0，零 output
```

即係話**單靠個 plugin 會宣告呢類 bug 已修好，同時留低 5 個入面 4 個永遠捉唔到**。
Lane A 冇收貨，喺 plugin 之上加咗一條 `no-restricted-syntax` selector 專門封呢個窿。Main agent 用同一個 probe 驗返 repo config：

```
repo config 對同一個 probe -> exit=1
  error  A clickable <tr>/<td>/<th>/<li> needs keyboard parity: either move the handler
         onto a real <button>, or add tabIndex={0} plus an onKeyDown that fires on Enter and Space
```

**呢個係本 session 第二次見到「開咗工具唔等於有保護」**：第一次係 CUI-0029 嗰條 `loaded_as_persistent` guard
改用 Core select 之後會靜靜變空殼。兩次嘅共通點都係**冇人量度過個工具實際捉唔捉到**。

---

## Design-Source Binding Rule —— 四張改顏色嘅票，零 CSS 改動

**Main agent 獨立 build base commit 同 branch HEAD 對比**：

```
BASE  (0c9006e): 74a1a510c3d6ff7391d553f4120d8b01f0fc928c10ab3d52e49570eb01f7ecad  index-uk9sh9F2.css
AFTER          : 74a1a510c3d6ff7391d553f4120d8b01f0fc928c10ab3d52e49570eb01f7ecad  index-uk9sh9F2.css
```

**逐 byte 相同**，連檔名 hash 都一樣。加 `--color-blue` 落 `@theme` costs zero bytes ——
Tailwind v4 會 tree-shake 冇人引用嘅 theme variable（Lane A 先實測驗證咗呢點先敢靠佢）。

Lane A 另外做咗一個超出 hash 嘅檢查：抽出 base commit 嗰 **74 個顏色字面值**，逐個由取代佢嘅 token 表達式重新推導再比對 ——
**74 個全部重現，0 個唔同**。`WarningSprite` 再喺 jsdom 同 base 版本並排 render：**DOM 逐 byte 相同**，21 個 symbol id 齊全。

### 一個中途出現、被追到底而唔係含混過去嘅 CSS hash 改動

Lane A 加咗新測試之後 CSS hash 變咗。追查結果：**Tailwind 掃 source text 搵 class candidate，
而佢新寫嘅測試描述入面「container」同「invisible」兩個英文字，令 339 bytes 真實 CSS
（`.container` + 五個 `@media` breakpoint + `.visible` / `.invisible`）ship 咗俾每一個訪客。**

用 `@source not "./**/*.test.ts(x)"` 修好，hash 回復。**呢個係 repo 本來就有嘅曝險**，
只係之前冇一條測試啱啱好包含一個 utility 形狀嘅英文字。

---

## AU-026 —— 49 個 hex 分兩類，界線劃得有理

| 類別 | 數量 | 去向 |
|---|---|---|
| 品牌色 | 16 | → `TOKENS.text`（八個熱帶氣旋信號嘅形狀 fill 同級數字，係 theme foreground，必須跟 theme 走） |
| 固定語義色 | 33 | → `signalColors.ts`（**刻意獨立、frozen**） |

語義色嘅論證：暴雨嘅琥珀／紅／黑係**讀者單靠顏色分辨嘅級別**；七個 `#fff` 字符之所以睇得清，
係因為底下嗰塊 tile 係固定飽和色；山泥傾瀉嘅土色、水浸／海嘯藍、霜凍／寒冷／酷熱、火災黃紅，全部係 HKO 側而唔係 dashboard 側。
**綁去 theme token 會令改 palette 靜靜咁改變個 icon 嘅意思。**

四個「差少少」嘅顏色同一個字體 stack **刻意冇收斂**（收斂會係真視覺改動），已列表交返俾人決定 —— 見下面 CUI-0044。

---

## CUI-0021 —— 我 brief 講錯咗對象

我寫「前端 TanStack Query 亦會當成功」。**Main agent 核實：錯。**

`graphql-request@7.4.0` 嘅 `runRequest.js:43` 對 `!response.ok` 回 `ClientError`，
而 **2xx 帶 `errors` 一樣回 `ClientError`**（`:68`）；八個 view 全部 render `isError`。
即係**前端一直都處理緊**。真正睇唔到嘅係 **HTTP 層** —— 任何淨係 check status code 嘅監控或 harness
（QA 當初撞到嗰個「400 requests 全部 ok，收到 341 個 unique id 當 365」）。

修改仍然啱，但理由要改寫。

### 界線同本 session 已劃四次嗰條一致

`original_error is None` = graphql-core parse / validate 出嚟嘅，即「API 讀咗你送嘅嘢然後答『唔得』」→ **200**。
`ClientArgumentError`（新，`ValueError` 子類）= 同一個答案喺 resolver 層講 → **200**。
其餘 = 去到 resolver 但完成唔到 → **500**。

**唔用裸 `ValueError` 做判別**，因為 `int()` 對一行壞數據都會掟 `ValueError` ——
咁樣會將後端故障重新標籤成用戶錯誤，即係同一個缺陷換條門入。

**Main agent 獨立實測**（自己注入錯誤）：

| 情況 | status |
|---|---|
| 健康請求 | 200 |
| `ClientArgumentError`（points 出界） | **200** |
| GraphQL validation（unknown field） | **200** |
| `OperationalError`（基建） | **500**，payload 保住具名成因 |

順帶：我原本想用「耗盡 pool」重現，**12 個 request 都爆唔到** —— 因為 CUI-0005 已經修好 session leak。呢個係 CUI-0005 生效嘅旁證。

---

## CUI-0029 —— 兩個 ticket 前提錯，佢量度之後做咗啱嘅嘢

| 我寫 | 實測 |
|---|---|
| 「兩次掃描」係成本所在 | `EXPLAIN QUERY PLAN` 顯示內層係 **COVERING INDEX**（只讀 seq，冇 row payload），外層係逐 seq seek。單次掃描原型**每次都更慢**（843.7 vs 807.6 ms 等），**冇 ship** |
| 「`track()` 佔請求 80%」 | 唔重現。80% 係**上一輪量嘅 service 層佔比**（分母 1490 ms）；佢量嘅係 **end-to-end GraphQL**（分母 3370 ms），ORM 讀取佔 ~34%。兩者唔矛盾，係我傳遞時冇講清楚分母 |

實際改動：`select(models.TrackPoint)` → Core column select（由 `TRACK_COLUMNS` 砌，所以新增 export 欄位會自動到 API）。

| | before | after |
|---|---|---|
| service `track()`（預設取樣數） | 1161.3 ms | **807.6 ms（−30%）** |
| service `track()`（全條 track） | 1643.2 ms | **855.6 ms（−48%）** |
| end-to-end `{ activities { track } }` | 3370.8 ms | **2883.0 ms（−14.5%）** |
| query 數 | 367 | 367（不變） |

**行為不變**：2,555 組合 / 459,061 點，`cmp` exit 0，兩邊 sha256 `c5a7f9a3…` 相同。

### 個 guard 測試點樣避免變空殼

而家同時睇兩樣嘢，**而且用同一個 session 先校準兩個計數器再信佢哋**：
- `sqlite3` 嘅 `row_factory`（每行 fire 一次，SQLAlchemy 唔會掂佢，唔知有 mapping 呢回事 → 任何 query 改寫都殺唔死佢），斷言 `<= 2 * points`
- `loaded_as_persistent`（起咗幾多 ORM entity），斷言 **`== []`** 而唔係設上限

校準步驟先讀全條 track、斷言兩個計數器都動咗 `STORED_TRACK_POINTS`，並喺之後 `expunge_all()` ——
否則一個熱 entity 會遮住 regression。**一個停止報數嘅儀器會大聲失敗，而唔係靜靜俾斷言過關。**

四個 mutation 逐個單獨落，三紅一綠，而嗰個綠正正解釋咗 `expunge_all()` 點解要喺度。
Main agent 獨立重跑第一個：還原 ORM select → 紅，還原 → 綠。

---

## AU-037 —— 我個前提只啱一半，佢冇假裝做到

我寫「SunMoon 有真正寫入者，令重新採集唔會摧毀現有資料」。**後半錯。**

**Main agent 核實**：`src/cli/export_data.py:77-79` 只載入 `HKO_DAILY_JSON` / `WEATHER_HISTORY_JSON` / `WEATHER_WARNING_JSON`，
**冇 `SUN_MOON_JSON`**；而 `records.py:171-172` 讀嘅係 **hko_daily 個 row**。

所以移植 collector 保護到 `sun_moon_rise_set_history.json`，但**重新採集 hko-daily 之後 export 出嘅 sunrise/sunset 一樣會變 `None`**。
要補呢個窿就要喺 `records.py` + `export_data.py` join sun/moon 檔 —— 兩者都唔喺該 lane，
**而且會打破 byte-identical**（兩份副本逐分鐘唔一致：`2021-01-01` → hko_daily `07:03`、sun_moon `07:02`）。
Lane B 改咗 `to_raw_row` 個 docstring 講返實情，而唔係留住一句「collector 未移植」嘅假話。已開 **CUI-0045**。

### 喺睇唔到 HKO 頁面嘅情況下，佢揾到最硬嗰個佐證

Legacy script 有個 `colspan==4` 分支硬寫 `100.0%`，睇落似 bug。**Main agent 掃 365 行 raw 確認**：

```
Moon Transit == "/"  ->  12 行，Illumination 全部 100.0%
Moonrise == "/" 13 次 ｜ Moonset == "/" 12 次 ｜ 從不同時出現（農曆月節奏）
```

即嗰個 `100.0%` **唔係 bug，係滿月情況**。

**時間 pattern 拒絕 12 小時制** —— legacy 個 `[:5]` 切片會將 `7:05 pm` 讀成 `07:05`，**差十二個鐘而且靜默**（CUI-0018 同一家族）。

`sun_moon.py` **100% 覆蓋**。而且佢喺 `tests/test_weather_collectors.py` 加咗 `TestTheNetworkGuardItself` ——
之前嗰個 `block_real_sockets` **聲稱係證明但從未被行使過**，同 CUI-0010 揭到嘅失敗形狀一模一樣。
實測 sandbox **唔係離線**：TCP connect 去 `127.0.0.1:45183`（`$HTTPS_PROXY`）**成功**。

---

## AU-029 —— 四個全刪，證據鏈完整

| function | production 消費者 | 測試消費者 |
|---|---|---|
| `parse_weight_file` | `cli/export_data.py:26,70` | 多條 |
| `build_dataframe` | **0**（AU-006 之後） | 只有自己嗰條測試 |
| `monthly_summary` / `weekday_summary` / `describe_weight` | **0** | **0** |

`legacy/README.md:17` 將 `05_GetDailyWeightSummary.py` 對應到 `run365days.weight`，
而讀該 script 顯示四個 function 係佢**console 列印 / matplotlib 半邊**嘅逐行移植。
唯一寫檔嗰行（`:155 to_json`）喺 `exit()`（`:137`）之後 —— **喺 legacy script 入面本身都係死碼**，
而佢寫嘅正正就係 AU-036 要刪嗰個 `DAILY_WEIGHT_JSON`。同源，同死。

`weight` package 而家唔再需要 pandas + numpy（有新 subprocess 測試釘住），
**但 `pyproject.toml` 仍然要兩者**（`weather/collectors/`、`activities/parsers/`、`common/geo.py`）—— 呢點佢講清楚咗，冇誇大。

`weight/analysis.py`：62 stmts / 12 miss（81%）→ **31 / 0（100%）**。

---

## 我 gate list 又寫錯一條

我叫前端 lane 跑 `npx tsc -b`。**Fresh checkout 上佢必然 exit 2** ——
`src/data/api/queries.ts` import `@/gql`，而 `frontend/.gitignore:5` 將 `src/gql/` 排除、從未 commit。
**Main agent 核實**：`git ls-files frontend/src/gql` 零 output。

之前跑得通純粹因為早前 build / test 已經生成咗佢。正確嘅 gate 係 **`npm run typecheck`**（= `codegen && tsc -b`），
CI 亦係咁叫。已修正。

---

## 新開

| ID | 級別 | 標題 | 來源 |
|---|---|---|---|
| **CUI-0044** | 🟢 Low | 四個 sprite 顏色 + 一個 font stack「差少少」但唔相等，收斂會係真視覺改動 —— **需要人決定** | Lane A |
| **CUI-0045** | 🟡 Medium | `SUN_MOON_JSON` 有寫入者但**冇讀取者**；補窿要 join 入 export，而兩份副本差一分鐘，會打破 byte-identical —— 要決定邊份權威 | Lane B |
| **CUI-0046** | 🟢 Low | Tailwind 掃 source text，任何測試描述含 utility 形狀嘅英文字都會 ship 死 CSS（實測 339 bytes）。`@source not` 只封咗測試檔，值得一條 CI 檢查 | Lane A |
| **CUI-0047** | 🟢 Low | `WeatherView.tsx:201` 用 `hsl(${210 - i * 30},70%,60%)` 生成溫度帶顏色 —— 唯一一個既非 token 亦非固定 palette 嘅顏色來源，唔會跟 theme 走 | Lane A |
| **CUI-0048** | 🟢 Low | `eslint.config.js` 個 `no-restricted-syntax` selector **冇測試**，改壞咗會靜默失效 | Lane A |
| **CUI-0049** | 🟢 Low | `test_a_single_activity_request_is_not_made_slower` 斷言喺**生成嘅 SQL 文字**上；今次靠彩數過關，將來 alias 表名會為咗無關原因而紅 | Lane C |
| **CUI-0050** | 🟢 Low | Coverage 未開 `branch = true`，所以「100%」只代表行行過，唔代表兩邊都行過（實例：batched GraphQL document 分支） | Lane C |
| **CUI-0051** | 🟢 Low | `TestOneOwnerPerConstant` 連 docstring 同註釋都掃，所以任何**討論**受管常數嘅文件都會令佢紅 —— 目前無法引用一個實測數字 | Lane C |

### CUI-0044 詳情（要你決定）

| `signalColors.ts` | 現值 | 最接近嘅 token | 相等？ |
|---|---|---|---|
| `thunderTile` | `#2a2f3d` | `--color-surface2` `#22263a` | ❌ |
| `thunderTileEdge` | `#4b5268` | `--color-border` `#2e3250` | ❌ |
| `rainBlack` | `#0b0d14` | `--color-bg` `#0f1117` | ❌ |
| `rainBlackEdge` | `#cbd5e1` | `--color-text` `#e2e8f0` | ❌ |

加上 sprite 數字用 `system-ui,sans-serif` 而 `--font-sans` 係 `"Segoe UI", system-ui, sans-serif` ——
收斂會令 Windows 上 Segoe UI 贏，係真改動。已具名為 `SIGNAL_DIGIT_FONT` 並加註釋。

**問題本質係：呢啲係「我哋揀嘅顏色」定「人哋俾我哋嘅顏色」。** 五個都係一行改動，只等一句話。

---

## 🔴 BLOCKER — AU-005 改名未完成，`pyproject.toml` 仍然宣告舊 package 名

**狀態：未修。呢條 branch 目前唔可以 merge 入 `develop`。**

`pyproject.toml` 用**手寫 package 清單**（冇 auto-discovery），而 `run365days.dashboard` 呢一行冇跟住改名：

```
[tool.setuptools]
packages = [ ..., "run365days.dashboard", ... ]     ← 應為 run365days.analytics
```

### 點解全部 gate 都捉唔到

| 入口 | 安裝方式 | 會唔會撞到 |
|---|---|---|
| CI `pages.yml:51` | `pip install -e ".[dev]"` | ❌ editable，path hook 直接指去 `src/` |
| CI `pages.yml:150` | `pip install -e "."` | ❌ 同上 |
| Lane 嘅 `.pkgroot` | `PYTHONPATH` symlink | ❌ 完全繞過 packaging |
| **Vercel `scripts/vercel-build.sh:23,25`** | **`pip install "."`（真安裝）** | ✅ **會爆** |

### Main agent 實測（乾淨 venv，唔受 editable `.pth` 污染）

```
改名後：pip install --no-deps . -> run365days/ 入面有
        activities api cli common export weather weight     ← 冇 analytics
        python -c "import run365days.analytics.stats"
          ModuleNotFoundError: No module named 'run365days.analytics'

改名前：同一個測試
        python -c "import run365days.dashboard.stats"  -> 成功
```

⚠️ **第一次測試我用 `PYTHONPATH` 指向 target 目錄，結果 import 成功 —— 呢個係假結果**：
editable install 個 `.pth` 裝咗一個 meta-path finder，佢蓋過 `PYTHONPATH`。**要喺乾淨 venv 度驗先算數。**
呢個同本 session 嘅 worktree PYTHONPATH 陷阱係同一個機制嘅另一面。

### 影響

Vercel 部署會裝到一個冇 `run365days.analytics` 嘅 distribution，
`api/graphql.py` → `run365days.api.schema` → `run365days.analytics.stats` **喺 import 階段就爆**，
GraphQL API 全掛 —— 而 CI 由頭到尾綠。

### 修法（一行）

```diff
-    "run365days.dashboard",
+    "run365days.analytics",
```

### 點解未修

執行 lane 嘗試改呢一行時**被權限系統拒絕**（`Modify Shared Resources`），然後要求 main agent 代佢改。
Main agent **拒絕代做** —— 代一個被拒絕嘅 subagent 執行佢做唔到嘅改動，就係繞過用戶嘅權限決定。
已交返俾用戶決定。

### 順帶：呢個清單值得有 gate

`grep -rn "packages" tests/` 零結果 —— **冇任何測試守住呢個手寫清單**。
一條「真安裝之後每個 `src/` 子 package 都 import 得到」嘅測試會令呢類錯誤唔可能再靜默發生。已開 **CUI-0052**。

---

## AU-005 / AU-021 修復摘要（2026-09-15）

| 指標 | 前 | 後 |
|---|---|---|
| Python tests | 544 | 544 |
| Coverage | 95.07% | 95.08% |
| Static export | 6,850,876 / 370 | **未變** |
| `activities_kml.jsonl` md5 | `fd5e260f…` | **未變**（357 records） |

### AU-005 —— rename commit 係純 rename

`17 files changed, 30 insertions(+), 30 deletions(-)`，完全對稱。`git mv` 保住 history（rename 偵測 100%/100%/100%/98%/98%）。
唯一非替換改動係 6 個檔案嘅 isort 重排 —— 因為 `analytics` 排喺 `api`/`common`/`export` **之前**，
而 `dashboard` 本來排喺後面。

**Import site 數目更正**：我報 8 個檔案，實際係 **8 個 import statement 分佈喺 7 個檔案**（`schema.py` 有兩個），
另加 3 處 docstring 交叉引用喺 2 個檔案。Audit 報「4 個」係只計 `src/` 而且**包括咗 `service.py`——
但佢根本冇 import，只有 docstring 提及**。

指「真正 v2/React 前端」嗰啲 "dashboard" 字眼**刻意保留**（9 處），`docs/CHANGELOG.md:46` 亦保留 ——
佢記錄嘅係 v2.0.0 當時用嗰個名。

### AU-021 —— 「36 個 function >30 行」係 docstring 造成嘅假象

Lane 用 AST 量度：**36 個 function 嘅 span >30 行，但扣走 docstring 之後只有 5 個嘅實際代碼 >30 行。**

| 代碼行 | span | function |
|---|---|---|
| 51 | 70 | `tcx.py:43 parse` |
| **50** | **69** | **`kml.py:50 parse`** ← 已拆 |
| 46 | 65 | `gpx.py:44 parse` |
| 43 | 44 | `cli/export_data.py:50 main` |
| 31 | 33 | `cli/check_ci_docs.py:142 render` |

**呢個指標本身應該改為量度代碼行而唔係 span** —— 用 span 會獎勵刪 docstring。已開 **CUI-0053**。

#### 我對 `kml.py:parse()` 嘅描述有兩處過時

我寫佢「一個 method 做四件事：XML 導航、BeautifulSoup HTML table 解析、track point 抽取、pandas lap 聚合」。實際：
- HTML table 解析**早就抽咗出去**（`_lap_table_cells`，經 `_parse_laps`）
- **`kml.py` 完全冇 import pandas** —— pandas 喺 `tcx.py:87`

拆完之後代碼行 **50 → 34**。Lane 冇夾硬壓到 30 以下，理由：剩返嗰啲係 9 行 WHY 註釋
（S314 suppression 理由、W-004 界線）同 11 行 `Activity(...)` constructor ——
**constructor 就係呢個 function 嘅產物**，收埋入 `_build_activity()` 只會搬走行數而唔會加一個值得讀嘅名。

---

## 新開

| ID | 級別 | 標題 |
|---|---|---|
| **CUI-0052** | 🟡 Medium | `pyproject.toml` 嘅手寫 package 清單**零測試守住**；一條「真安裝後每個子 package 都 import 得到」嘅測試會令 AU-005 呢類錯誤唔可能靜默 |
| **CUI-0053** | 🟢 Low | AU-021 嘅「>30 行」指標用 span 量度，會獎勵刪 docstring；應改為量度代碼行（36 → 5） |
| **CUI-0054** | 🟢 Low | `tcx.py:parse`（51 代碼行）同 `gpx.py:parse`（46）係 AU-021 剩返嘅真正工作，結構同啱啱拆咗嗰個平行 |
| **CUI-0055** | 🟢 Low | 三處前端註釋指住已經唔存在嘅路徑：`data/stats.ts:14`、`stats.test.ts:5`（→ `src/dashboard/stats.py`）、`test/fixtures.ts:25`（→ `tests/test_dashboard_stats.py`） |
