# Ticket Registry — Run365Days

**最後更新**：2026-09-14（P0 + Warning + CUI-0002 / 0001 / 0007 / 0010 / 0009 完成）

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
| **AU-048** | P1 | epoch-ms path 語義錯 8 小時；測試常數係捏造 | Lane B 申報，證據見下 |
| **AU-049** | P2 | `create_app(..., graphiql: bool = True)` 預設仍然開 | Lane C 申報 |

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
| **W-011** | P1 | `workflow_dispatch` 由任何 branch 都會做 production Pages 部署 | Lane F 申報 |
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
| **CUI-0011** | 🔴 Critical（latent） | Collector 寫出嘅 schema 同 exporter 讀嘅唔夾，重新採集即炸 pipeline | pending |
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
