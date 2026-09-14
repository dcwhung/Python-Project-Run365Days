# Ticket Registry — Run365Days

**最後更新**：2026-09-14

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
| W-001 | `docs/architecture.md` 仍寫 CI 喺 `develop` 觸發；test count 93 應為 133 | `docs/architecture.md:142` | 🔧 修復中 |
| W-002 | concurrency group 為常數 `pages` + `cancel-in-progress`，PR 嘅 CI run 會取消進行中嘅 production 部署 | `.github/workflows/pages.yml:15-18` | 🔧 修復中 |
| W-003 | 釘死錯誤 epoch-ms 語義嘅測試，其「tracked separately」所指 ticket 不存在 | `src/common/time.py:41`、`tests/test_common_time.py:60` | 🔧 修復中（引用 AU-048） |
| W-004 | `ValueError` 一桶裝「刻意 skip」同「數據錯誤」並記 DEBUG；實測 8/365 真實 KML 被靜默丟棄 | `src/activities/parsers/base.py:124-127`、`kml.py:71` | 🔧 修復中 |
| W-005 | `optional_int` 違反 docstring：`"inf"` raise `OverflowError`，`parse_all` 唔 catch → 整個 export 崩潰 | `src/activities/parsers/base.py:70-73` | 🔧 修復中 |
| W-006 | KML lap `Time`/`Distance` 當 optional，TCX 同義欄位當 required；缺一個 lap 會靜默少計 | `src/activities/parsers/kml.py:152-168` | 🔧 修復中 |
| W-007 | 新增嘅三段分級 `except` 只有 `ET.ParseError` 一段有測試 | `src/activities/parsers/base.py` + 三個 parser | 🔧 修復中 |
| W-008 | docstring 宣稱關 GraphiQL 同時關 introspection；實測仍然回 200 完整 schema | `api/graphql.py:18-20` | 🔧 修復中 |
| W-009 | 四個 list field 靜默截斷於 `limit=500`，無 `totalCount`/`hasNextPage` | `src/api/schema.py:32-38` | 🔧 修復中 |
| W-010 | SQL stride + 二次 downsample 令間距不均，與 "evenly" 描述矛盾 | `src/api/service.py:94-116` | 🔧 修復中 |
| S-001 | `README.md:260` 將 Vercel production branch 寫成已驗證事實 | `README.md:260` | 🔧 修復中 |
| S-006 | TCX 仍計算從未被讀取嘅 `MaxSpeed` lap 欄位 | `src/activities/parsers/tcx.py:107` | 📋 併入 AU-021 |
