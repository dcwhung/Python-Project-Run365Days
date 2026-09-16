# Batch Review — 2026-09-16 — CUI-0027 / CUI-0022 / CUI-0024

**審閱者**：Code Reviewer（獨立角色，冇參與呢批代碼嘅撰寫）
**Branch**：`claude/ai-dev-team-start-05jie2`
**Delta**：`d108c82..1ace951`（9 files, +541 / −73）
**涵蓋 commit**：

| Ticket | Commit | 性質 | Merge commit |
|---|---|---|---|
| **CUI-0027** 🟠 High | `bb8032a` | 行為改動 + **breaking change** | `9c76201` |
| CUI-0022 🔵 Low | `abe0d40` | dead code 刪除 | `314f2ea` |
| CUI-0024 🟢 Low | `c768a4f` | docs-only | `3e5881b` |
| （收尾） | `1ace951` | 刪走 `pending/` 入面重複嘅 CUI-0024 | — |

---

## 整體 verdict

**Status：⚠️ warn（84 / 100，0 Critical，2 Warning，6 Suggestion）**

呢批嘢做得非常紮實。CUI-0027 係三張入面唯一有風險嘅，而佢每一項聲稱我都獨立重量過，
**冇一項係誇大嘅，部分仲係保守咗**。兩個 linearity gate 我人為整紅過，兩個都真係有牙。
等價性我用獨立 harness 做咗 24 個 document 嘅 SHA-256 前後對比，**21 個 byte-identical，
3 個唔同嘅啱啱好就係三個 intended breaking change**，冇一個意外。

兩個 Warning 都**唔係行為缺陷**，係「呢個 module 自己定落嘅標準」冇貫徹到底：
一條 fail-closed 分支冇 test 釘住（`track` 嗰邊有兩條），同埋一個睇落 exhaustive
嘅 docstring 列表實際上漏咗 5 個 field。兩個都係平嘅，改完就可以 pass。

---

## Hard Gates

| Gate | 指令 | 結果 |
|---|---|---|
| **Lint（Python）** | `.venv/bin/ruff check src tests` | ✅ pass — All checks passed! |
| **Format（Python）** | `.venv/bin/ruff format --check src tests` | ✅ pass — 58 files already formatted |
| **Tests（Python）** | `.venv/bin/python -m pytest tests -q` | ✅ pass — **378 passed** in 47.58s |
| **SDL 同步** | `.venv/bin/run365-schema --check frontend/schema.graphql` | ✅ pass — is up to date |
| **Lint（前端）** | `npm run lint` | ✅ pass |
| **Type check（前端）** | `npm run typecheck`（先跑 codegen） | ✅ pass |
| **Tests（前端）** | `npm test` | ✅ pass — **126 passed / 22 files** |
| **Coverage（核心邏輯）** | `pytest tests/test_api.py --cov=run365days.api` | ✅ pass — `src/api/schema.py` **99%**，唯一 miss 係 478–481（見 W-027）；`src/common/time.py` **100%** |
| **No Critical** | — | ✅ pass — 0 |
| **Security scan** | 本 delta 零新增依賴 | ✅ n/a |

> ⚠️ 驗證過用 `npm test` 而唔係 `npx vitest run`：後者唔跑 codegen，`@/gql` 未生成會有
> 4 個檔 fail（84 passed），**唔係迴歸**。本次全部用 `npm test`。

---

## 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|------|------|------|------|
| 正確性 | 24 | 25 | 每條軸都獨立驗過；扣 S-057 |
| 安全性 | 19 | 20 | 訊息零洩漏、fail-closed 正確；扣 S-055 |
| 可維護性 | 12 | 20 | 扣 W-028、S-052、S-053、S-056 |
| 測試覆蓋 | 9 | 15 | 扣 W-027、S-054 |
| 性能 | 10 | 10 | 最差合法 document 由 4.9× 餘裕升到 18.2×，零 finding |
| 代碼風格 | 10 | 10 | ruff 全綠，完全跟 `track` budget 既有 pattern |
| **總分** | **84** | **100** | |

**結果：⚠️ warn**（`hard_gates` 全 pass + 75–89 分 + 無 Critical）

---

# Section A — CUI-0027｜per-request list row budget（`bb8032a`）

## A.0 改動清單

| 檔案 | 改動 |
|---|---|
| `src/api/schema.py` | 新增 `MAX_LIST_ROWS_PER_REQUEST = 4000`、`LIST_ROWS_KEY`、`_charge_list_rows()`、`LIST_ROWS_NOTE`；`_TrackBudget` → `_RequestBudgets`；5 個 resolver 加收費；6 段 docstring 改寫 |
| `frontend/schema.graphql` | **只有 description 改動**（見 A.7） |
| `tests/test_api.py` | +10 個 test function（其中 1 個 parametrized ×5），−1 個 `DOCUMENTED_WORST_CASES` 項 |
| `.tickets/…/CUI-0027.md` | pending → in-progress |

---

## A.1（a）Budget 覆蓋面 —— 我自己掃咗 SDL 每一個 root field

Root `Query` 一共 **11 個 field**。逐個實測佢哋喺 `MAX_QUERY_TOKENS = 1000` 之下
最闊可以去到幾多，同埋喺 production export（365 activities / 134,041 track rows）上
用 SQLite VDBE 實測嘅耗時：

| Field | 收費 | 最闊 alias 數 | 實測最差 | 15 s 餘裕 | 判斷 |
|---|---|---|---|---|---|
| `activities` | `limit` | 166 | 被拒 @ 8 | — | ✅ 收得啱 |
| `weight` / `weather` / `warnings` | `limit` | — | — | — | ✅ 收得啱 |
| `year` | `DEFAULT_PAGE_SIZE` | 166 | 被拒 @ 8 | — | ✅ 收得啱（見 A.2） |
| `activity(id:)` | 不收 | **90**（實測啱啱好 90，docstring 寫 90 ✅） | 42.2 ms | 355× | ✅ 唔收合理 |
| `activitiesCount` | 不收 | 332 | 83.0 ms | 181× | ⚠️ 見 W-028 |
| `weightCount` | 不收 | 332 | 77.2 ms | 194× | ⚠️ 見 W-028 |
| `weatherCount` | 不收 | 332 | — | — | ⚠️ 見 W-028 |
| `warningsCount` | 不收 | 332 | 75.4 ms | 199× | ⚠️ 見 W-028 |
| `meta` | 不收 | 166 | 47.6 ms | 315× | ⚠️ 見 W-028 |

**結論：冇任何漏收會造成實際風險。** 五個唔收費嘅 field（`meta` + 四個 `*Count`）
最差都喺 83 ms 以下、181× 餘裕以上 —— 完全唔值得收，判斷係啱嘅。
但 docstring 冇講過佢哋（W-028）。

### `year` 收 `DEFAULT_PAGE_SIZE` 合唔合理 —— 實測

呢個係唯一一個「proxy」而唔係「ceiling」嘅收費，所以我特別量咗：

```
1x year (charged 500)                    activity_rows=  365  other=  384  stmts= 3  ms= 31.5
8x year (charged 4000)                   activity_rows= 2920  other= 3072  stmts=24  ms=140.9
8x activities(limit:500) (charged 4000)  activity_rows= 2920  other= 3056  stmts=16  ms=144.7
```

**校準得非常準**：同樣收 4000，`year` 同 `activities(limit:500)` 讀嘅 activity row
一模一樣（2,920），耗時 140.9 ms vs 144.7 ms，差 2.7%。呢個代理值係對嘅。
（唯一未寫低嘅前提見 S-057。）

### 重複收費 —— 冇

Nested field（`activities { warnings }`、`activities { weather }`）**唔會再收一次**：
`service._activity_dict()` 喺 `selectinload` 之後由已 materialise 嘅 row 砌出嚟。
實測 8 個 aliased `activities` 出 **16 條 statement**（2 條 / field，即 list + warnings 嘅
selectinload），無論 nested selection 幾闊都係 2 條。`year` 嘅 sub-field
（`totals` / `monthly` / `weekly` / `dailyDistance` / `trainingLoad` / `personalBests`）
亦係一次收費：`{ year { year } }` 同 full `YearQuery` 都係 3 條 statement。
**冇搵到任何 double-charge。**

---

## A.2（b）收費時機 —— 核實過，被拒嘅 request 真係冇發出多餘 statement

Lane 聲稱「喺 SQL 出去之前收費」。我用 SQLAlchemy `before_cursor_execute` event
直接數實際發出嘅 statement：

```
166 aliased activities (refused)                statements= 16   data_is_none=True
  breakdown: Counter({'SELECT ': 16})
8 aliased activities (served, exactly budget)   statements= 16   data_is_none=False
9 aliased activities (refused at 9th)           statements= 16   data_is_none=True
```

**被拒嘅 166-field document 同「啱啱好食晒 budget」嘅 8-field document 發出完全一樣嘅
16 條 statement。** 即 8 × `SQL_PER_LIST_FIELD`(2) = 16，同
`test_the_widest_list_fan_out_is_refused_after_the_budget` 嘅 assertion 完全一致。
第 9 個 field 一條 SQL 都冇發出就被拒。✅ 聲稱成立。

`year` 嗰條路一樣：166 aliased `year` 被拒之後出 24 條 statement = 8 × 3。✅

代碼上亦睇得出 ordering 係啱嘅 —— `_charge_list_rows(info, limit)` 喺
`service.activities(...)` 之前，而 `raise` 喺 `info.context[LIST_ROWS_KEY] = rows_left`
之前（refusal 唔會寫返 counter，但因為 non-null propagation，寫唔寫都冇人睇得到，
呢點 docstring 有講清楚，而且有 test 釘住）。

---

## A.3（c）兩個 linearity gate 有冇牙 —— 人為整紅驗過，**兩個都有牙**

> ⚠️ 所有 mutation 做完即刻還原，見文末「驗證用臨時改動聲明」。

### Gate 1：`test_the_widest_list_fan_out_reads_no_more_rows_than_the_budget`

**第一次 mutation（`MAX_LIST_ROWS_PER_REQUEST = 10**9`）—— 呢條 test 冇紅。**
原因唔係 test 冇牙，係我個 mutation 揀錯咗：assertion 係
`rows_loaded <= MAX_LIST_ROWS_PER_REQUEST`，即係同個常數比，唔係同絕對數字比。
拉高常數等同拉高 assertion 本身。（順帶：另外 7 條 test 紅咗，但係因為
`ROW_FLOOD_LIMIT = 4000 // 64` 跟住變成 15,625,000 而超過 `MAX_PAGE_SIZE`，屬 noise。）

**第二次 mutation（保留常數 4000，只刪走 `activities` 入面嗰句
`_charge_list_rows(info, limit)`）—— 即刻紅**，而且訊息啱啱好係 ticket 記錄嗰個數字：

```
AssertionError: 166 aliased list fields read 60590 rows; nothing is bounding the fan-out
assert 60590 <= 4000
```

✅ **有牙。** 呢條 gate 釘住嘅係「有冇收費」，唔係「常數係幾多」—— 咁樣先啱，
因為常數本身係用戶決定嘅，唔應該由 test 鎖死。

### Gate 2：`test_the_fan_out_cost_does_not_widen_with_the_document`

Mutation：喺 `Query.activities` 注入一個隨 document 闊度線性增長嘅額外查詢
（即 CUI-0019 / AU-050 嗰種 O(N²) 形狀）。**即刻紅**：

```
AssertionError: a field in a wide fan-out has started costing more than one in a
narrow fan-out: the list path has picked up a term that grows with the width of
the document (CUI-0019 in the other half)
assert (557152 / 64) < (1.1 * (42206 / 8))
```

✅ **有牙。** 而且我量咗乾淨狀態下嘅裕度：

```
narrow/field=5072   wide/field=5058   ratio=0.9972   ceiling=1.1
```

真實 slope 係 **0.9972**（理論值應該係 1.0，實測基本平），ceiling 1.1 留咗約 10% 空間，
同 `BATCH_COST_FLAT_CEILING` 嘅取值同理由一致。呢個 ceiling 唔算鬆。

---

## A.4（d）效能聲稱 —— 獨立重量，**冇誇大，部分保守**

用 production export（`data/processed/run365.db`：365 activities、134,041 track_points、
365 weight、363 daily_weather、461 warnings —— 同 ticket 記錄嘅規模一致）。
「before」用 `git worktree` 喺 `d108c82` 開出嚟真正嘅舊代碼跑，唔係 monkeypatch。
每個 shape 跑 2 次取 min。

| Shape | Lane 聲稱 | 我實測 before | 我實測 after | 判斷 |
|---|---|---|---|---|
| 166 aliased `activities` | 3,071 → 170 ms | **2,893.3 ms** | **160.9 ms**（被拒） | ✅ 成立（我部機快少少） |
| 166 aliased `year` | 3,394 → refused | **2,770.9 ms** | **157.8 ms**（被拒） | ✅ 成立 |
| 最差仍 served | 542 ms（27.7×） | 455.0 ms | **477.0 ms** | ✅ 成立（lane 保守咗） |

### 「最差仍 served」我自己掃過，冇搵到 lane 漏咗嘅更差 shape

Lane 聲稱「scanning every (pages, limit) the token limit admits finds it at
16 pages of `limit: 250`」。我獨立掃咗 3 種 selection × 17 個 limit：

```
sel       pages  limit  charged  tokens       ms
full         20    200     4000     662    472.7
full         13    300     3900     431    470.7
full         16    250     4000     530    470.5   <- lane 揀嗰個
full         25    160     4000     827    469.6
full         30    125     3750     992    455.7
full          8    500     4000     266    341.6
warnings     80     50     4000     882    280.2
id           80     50     4000     882    267.5
```

**最差區域係一個 plateau（469–473 ms），16×250 喺誤差範圍內就係最差嗰個。**
Lane 冇揀錯，而且佢報 542 ms 比我量到嘅 477 ms 保守。
15000 / 477 = **31.4× 餘裕**（lane 報 27.7×，一樣係保守）。

⛔ 順帶更正任務簡報入面「3,071 → 170 ms（**88×**）」呢個括號：3071 / 170 = 18.1，
唔係 88。88 係 **15000 / 170 = 88.2**，即係「餘裕」而唔係「加速倍數」。
Commit message 本身冇寫過 88×，所以呢個唔算 lane 嘅錯，但報告入面要講清楚
免得下一個人照抄。

---

## A.5（e）等價性 —— **獨立抽驗，21 / 24 byte-identical**

獨立 harness，前後兩個版本各自 `schema.execute_sync`，對 `{data, errors}` 做
canonical JSON → SHA-256。

| Document | before ms | after ms | hash 相同 | 備注 |
|---|---|---|---|---|
| 166 aliased activities | 2893.3 | 160.9 | ❌ | **intended break** |
| 166 aliased year | 2770.9 | 157.8 | ❌ | **intended break** |
| 啱啱好 4001 rows | 34.1 | 35.4 | ❌ | **intended break** |
| 16×250 ActivityFields（最差 served） | 455.0 | 477.0 | ✅ | |
| 4 lists × MAX_PAGE_SIZE = 啱啱好 4000 | 32.1 | 31.7 | ✅ | |
| 35 aliased pages（token 超限） | 9.3 | 9.1 | ✅ | 兩邊都係 parse error |
| 90 aliased `activitiesCount` | 22.8 | 23.3 | ✅ | |
| 332 aliased `activitiesCount` | 81.4 | 82.9 | ✅ | |
| 100 aliased `meta` | 27.4 | 27.6 | ✅ | |
| 80 aliased `activity(id:)` | 41.5 | 42.2 | ✅ | |
| **FE::ActivitiesQuery** | 31.1 | 32.9 | ✅ | |
| **FE::WeightQuery** | 8.8 | 8.8 | ✅ | |
| **FE::WeatherQuery** | 14.6 | 14.9 | ✅ | |
| **FE::WarningsQuery** | 13.5 | 12.1 | ✅ | |
| **FE::YearQuery**（全 selection） | 29.1 | 30.4 | ✅ | |
| FE::ActivityDetail（`track(points:150)`） | 2.0 | 2.3 | ✅ | |
| FE::MetaQuery | 1.1 | 1.5 | ✅ | |
| FE::CountsQuery（四個 count） | 1.8 | 2.0 | ✅ | |
| FE::FilteredPage（date + minKm + hasGps + limit + offset） | 2.8 | 2.9 | ✅ | |
| FE::TrackWide | 9.8 | 10.1 | ✅ | |
| FE::BadLimit（`limit: 0`） | 1.6 | 1.5 | ✅ | error 訊息都一樣 |
| FE::BadOffset（`offset: -1`） | 1.5 | 1.5 | ✅ | error 訊息都一樣 |
| FE::TrackOverBudget（65 tracks） | 4.7 | 4.7 | ✅ | |
| FE::EmptyWindow | 2.2 | 2.5 | ✅ | |

**24 個 document，21 個 SHA-256 完全一致，3 個唔同嘅啱啱好就係三個 intended breaking
change，冇一個係意外。** 五條前端真實 document 全部 byte-identical。
Error path（bad limit / bad offset / track over budget）亦冇變。✅ 聲稱成立。

---

## A.6（f）Breaking change 邊界 —— 全部核實

### 4000 served / 4001 refused

```
4 lists × MAX_PAGE_SIZE = 啱啱好 4000  →  served   ✅
啱啱好 4001 rows                        →  refused  ✅
8 aliased activities（8×500 = 4000）    →  served   ✅
9 aliased activities（9×500 = 4500）    →  refused @ 第 9 個  ✅
```

Test 亦釘得住兩邊：`test_every_list_field_at_the_maximum_page_is_still_inside_the_row_budget`
釘 4000 served；`test_a_refused_page_stops_the_operation_where_it_stands` 入面
`over: activities(limit: left + 1)` 即 3904 + 97 = **4001** 釘 refused。
（`test_the_row_budget_is_the_boundary` 用 64×62 = 3968 / 65×62 = 4030，
唔係啱啱好嘅 4000/4001，但上面兩條已經涵蓋，唔另計 finding。）

### 前端五個 document 全部唔受影響

`CLIENT_LIST_DOCUMENTS` parametrized test（5 個 case）驗過，我亦獨立驗過：
五個 document 全部 served，SHA-256 前後一致，每個只收 500（budget 嘅 1/8）。
`test_the_whole_front_end_in_one_request_is_still_inside_the_row_budget`
再驗埋五個夾埋一個 request 都仲 served。✅

### Non-null propagation —— **有 test 釘住**

被拒時 `data` 變 `null` 而唔係嗰個 alias 變 `null`，因為 list field 係 non-null type。
`test_the_widest_list_fan_out_is_refused_after_the_budget` 有
`assert body["data"] is None`，`test_a_refused_page_stops_the_operation_where_it_stands`
有 `assert [e["path"] for e in body["errors"]] == [["over"]]` + statement count 證明
排喺後面嘅 `under` 完全冇 resolve 過。我實測四個 case 全部 `data_is_none=True`。✅
呢個行為 docstring 亦有明文交代（解釋點解 track budget 嗰種 sticky / not-sticky
分別喺呢度唔存在）。

### 錯誤訊息有冇洩漏 internal detail —— **冇，同 `track` budget 一致**

```
ValueError  : "list row budget exhausted: one request may read at most 4000 rows"
RuntimeError: "list row budget was not seeded for this request, so list fields cannot be served"
```

對比 `_charge_track_field`：

```
ValueError  : "track field budget exhausted: one request may read at most 64 tracks"
RuntimeError: "track budget was not seeded for this request, so `track` cannot be served"
```

句式、資訊量、甚至「deliberately not surfacing the key that was missing」嗰句註釋
都完全對齊。**冇洩漏 context key、冇洩漏 table / column / SQL、冇 stack 內容。**
完全符合 S-014 / W-013 定落嘅先例。✅

（唯一相關嘅係 S-055：server 端 log 有 full traceback，但呢個係 `track` budget
一路以來嘅行為，唔係本次引入。）

---

## A.7（g）alias 軸有冇如實記錄 —— **有，而且量得準**

用戶決定「記錄但唔處理」，`MAX_LIST_ROWS_PER_REQUEST` 嘅 docstring 係唯一載體。
原文：

> What this does **not** bound is the other axis of a wide document: aliasing many
> *fields* onto the rows a page already returned. `activities` taking 331 aliased
> `warnings` fields is one page, charges 500, and reads ~0.8 s of the export.
> Opening more pages does not widen it -- `MAX_QUERY_TOKENS` bounds the product of
> pages and aliases, so eight pages of 39 aliases reads ~0.9 s, the same figure
> again -- which is exactly why a row budget cannot answer it: the cost grows with
> fields resolved, not with rows read. That shape is the worst legal document left,
> at some 17x inside the function against the 30x this one leaves, and it wants a
> limit of its own rather than a smaller number here.

逐句核實：

| 聲稱 | 我實測 | 判斷 |
|---|---|---|
| 331 aliased `warnings` 係 token 上限 | **331**（998 tokens） | ✅ |
| 收 500（一頁） | 收 500 | ✅ |
| ~0.8 s | **739.1 ms** | ✅ |
| 8 頁 × 39 alias「同一個數字」 | **824.1 ms**（39 係 token 上限 ✅） | ✅ |
| 「the worst legal document left」 | 我掃過 10 種 alias 形狀，824 ms 係最高 | ✅ |
| 「some 17x inside the function」 | 15000 / 824 = **18.2×**（lane 用 863 ms → 17.4×） | ✅ lane 保守 |
| 「30x this one leaves」 | 15000 / 477 = **31.4×** | ✅ |
| 「a row budget cannot answer it」 | 成立：cost 跟 field resolved 走，唔跟 row 走 | ✅ |

我另外掃咗其他 alias 形狀確認 331×`warnings` 真係最差：

```
activities{331 aliased warnings}       739.1 ms   20.3×
activities{331 aliased distanceKm}     392.5 ms   38.2×
activities{165 aliased weather{tempC}} 435.7 ms   34.4×
8 pages × 39 aliased warnings          824.1 ms   18.2×   <- 最差
year{165 aliased dailyDistance{date}}  335.7 ms   44.7×
year{165 aliased weekly{week}}          84.7 ms  177.1×
```

**呢條軸記錄得清楚、數字準確、原因講得明、並且明講咗「it wants a limit of its own」。
作為「記錄」嘅載體，寫得足夠。** ✅

唯一唔足夠嘅係：呢個記錄**只活喺一個 docstring 入面，冇開 follow-up ticket**，
而且 CUI-0027 ticket 檔本身完全冇記低呢個決定（見 S-056）。

### SDL 只加 description —— 核實過

`frontend/schema.graphql` 嘅 diff 我逐個 hunk 睇過：**5 個 hunk 全部係 description
block 嘅增刪，零 type / field / argument / nullability 改動。**
`run365-schema --check` 綠，前端 codegen 綠，`npm run typecheck` 綠，126 條前端測試綠。
✅ 前端零影響。

---

## A.8 CUI-0027 發現

### 🟡 W-027｜`_charge_list_rows` 嘅 fail-closed 分支冇任何 test 釘住

- **位置**：`src/api/schema.py:477-481`（coverage report 嘅 miss 就係呢四行）
- **描述**：`_charge_list_rows` 嘅 `except KeyError → raise RuntimeError` 分支
  係成個 budget 嘅最後防線（context 唔係 `MutableMapping`、或者 schema 冇
  `_RequestBudgets` extension 嗰陣，唔可以 fall back 成 unbounded）。
  `track` 嗰邊有**兩條** test 專門釘呢件事：
  `test_track_fails_closed_when_the_schema_has_no_budget_extension` 同
  `test_track_fails_closed_when_the_context_cannot_be_seeded`。
  `_RequestBudgets` 嘅 docstring 仲白紙黑字寫住
  「and ``_charge_list_rows``, which reads it the same way」—— 但呢句冇 assertion 撐。
- **證據**：`pytest tests/test_api.py --cov=run365days.api --cov-report=term-missing`
  → `src/api/schema.py  307  2  99%  478-481`。成個 api package 唯一未覆蓋嘅新代碼
  就係呢條分支。
- **行為本身係啱嘅**（我手動驗過四個 case 全部 fail closed）：

  ```
  no extension  + activities   data=None  err=list row budget was not seeded…
  no extension  + year         data=None  err=list row budget was not seeded…
  frozen context+ activities   data=None  err=list row budget was not seeded…
  frozen context+ year         data=None  err=list row budget was not seeded…
  ```

  所以呢個係 **gate 缺口，唔係 bug**。
- **影響**：如果將來有人改 `_RequestBudgets` 嘅 seeding（例如開 batching、或者換
  transport 令 context 變成另一種 mapping），`track` 嗰邊會即刻紅，
  **list 嗰邊會靜靜咁變成 unbounded**，而 378 條測試照樣全綠 —— 即係啱啱好
  倒退返 CUI-0027 修之前嗰個狀態，但今次冇人發現。
- **方案 A（推薦）**：照抄 `track` 嗰兩條 test 嘅形狀，加兩條 list 版本
  （`strawberry.Schema(query=Query, extensions=[])` + `MappingProxyType`），
  assert `NOT_SEEDED` 喺訊息入面同 `data is None`。約 20 行，零風險。
  Trade-off：test 檔再長啲 —— 但呢個 module 嘅慣例本身就係「寫低嘅嘢要有 assertion 撐」。
- **方案 B**：將 `track` 現有嗰兩條 test parametrize，一次過涵蓋
  `TRACK_QUERY` / `{ activities { id } }` / `{ year { year } }` 三個 document。
  Trade-off：改動到現有 test，diff 冇方案 A 咁乾淨，但覆蓋面更闊而且以後加新
  budget 自動有位擺。
- **推薦：方案 B** —— 因為 `_RequestBudgets` 而家係「複數 budget」，
  test 亦應該係複數形狀，咁先同重新命名嘅意圖一致。

### 🟡 W-028｜`MAX_LIST_ROWS_PER_REQUEST` docstring 嘅「What pays, and what does not」列表睇落 exhaustive，實際漏咗 5 個 field

- **位置**：`src/api/schema.py:302-313`
- **描述**：個標題係「What pays, and what does not:」，跟住三個 bullet：
  四個 list field、`year`、`activity(id:)`。**root `Query` 一共 11 個 field，
  呢個列表只講咗 6 個。** 漏咗 `meta`、`activitiesCount`、`weightCount`、
  `weatherCount`、`warningsCount`。
- **影響**：呢個 module 嘅文化係「docstring 就係契約，每個數字都有 assertion 撐」
  （AU-047 兩輪 finding 都係呢件事）。一個以「what does not pay」為題、
  仲特登為 `activity(id:)` 寫低咗「90 rows and ~0.05 s on the export, some 300x
  inside the 15 s function, so charging it would buy noise」呢種量化理由嘅列表，
  讀者好合理咁會當佢係窮舉。下一個加 root field 嘅人翻到呢度，
  **搵唔到一條「幾時要收費」嘅規則，只有例子**。
- **實測（呢啲 field 唔收費係啱嘅，唔係風險）**：

  ```
  332 aliased activitiesCount   83.0 ms   181×
  332 aliased weightCount       77.2 ms   194×
  332 aliased warningsCount      75.4 ms   199×
  166 aliased meta               47.6 ms   315×
  ```

  全部遠平過 `activity(id:)` 嘅 42 ms / 355× 量級，同樣「charging it would buy noise」。
  **所以呢個係文件完整性問題，唔係覆蓋面問題。**
- **方案 A**：加一個 bullet：「the four `*Count` fields and `meta` do not pay either
  —— a count is one scan returning one row and `meta` is two rows; the widest
  flood `MAX_QUERY_TOKENS` admits (332 counts, 166 `meta`) measures 83 ms and
  48 ms on the export, 180x and 315x inside the function.」
  Trade-off：docstring 再長少少，但同 `activity(id:)` 嗰段嘅寫法一致。
- **方案 B**：唔逐個列，改成寫一條規則：「a field pays if and only if the rows it
  reads grow with a client-supplied or implicit window; a field that returns a
  fixed number of rows (a primary-key get, a scalar aggregate, the meta table)
  does not.」然後 `activity(id:)` 嗰段保留做例子。
  Trade-off：規則式更耐用（新 field 自動有答案），但失去咗現時逐個 field 都有
  實測數字撐嗰種說服力。
- **推薦：方案 A + B 都要** —— 先寫規則（B）令下一個人有得跟，再用一句列埋
  漏咗嗰五個同佢哋嘅實測數（A）令個列表真係窮舉。呢個 module 一路都係
  「規則 + 實測數字」兩樣齊，單做一樣都唔似佢自己。

### 🟢 S-052｜兩個 test 註釋仲寫住已經改咗名嘅 `_TrackBudget`

- **位置**：`tests/test_api.py:1326`、`tests/test_api.py:1338`
- **描述**：本次 commit 將 `_TrackBudget` 改名做 `_RequestBudgets`，`src/` 入面全部
  更新咗（我 grep 過 `src/` 零命中），但呢兩條 test 嘅註釋
  （「A schema built without _TrackBudget seeds nothing」/
  「a read-only Mapping is not a MutableMapping, so _TrackBudget skips it」）
  仲係舊名。`.proj-docs/` 入面嘅係歷史紀錄，正確，唔好改。
- **方案 A**：直接改成 `_RequestBudgets`。
- **方案 B**：連埋 W-027 方案 B 一齊做（parametrize 嗰陣順手改）。
- **推薦：方案 B**，同一個 commit 做晒。

### 🟢 S-053｜`LIST_ROWS_NOTE` 貼咗落 `year`，但 `year` 冇 `limit`

- **位置**：`src/api/schema.py:1018-1024`，出街 SDL 見
  `frontend/schema.graphql` `year` 嘅 description
- **描述**：`year` 而家個 description 係「…spends one default page (500 rows) of the
  same budget the list fields spend.」**再加**整條 `LIST_ROWS_NOTE`，
  而 `LIST_ROWS_NOTE` 尾句係「charged on \`limit\` as asked for rather than on the
  rows a page turns out to hold」。**`year` 根本冇 `limit` argument。**
  客戶端讀 SDL 會見到一條唔適用於呢個 field 嘅規則貼咗喺呢個 field 上面。
- **影響**：純 SDL 可讀性。前端 codegen 唔受影響（description 唔入 type）。
- **方案 A**：`year` 唔貼 `LIST_ROWS_NOTE`，只留自己嗰句，另外自己補一句
  「counted against the same 4000-row per-request budget」。
  Trade-off：4000 呢個數字要喺兩個地方出現（但兩邊都係 f-string，唔會 drift）。
- **方案 B**：將 `LIST_ROWS_NOTE` 拆兩半 —— 共用嗰句（總額 + 跨 field 累計）
  同只適用於有 `limit` 嗰句（收費基準），`year` 只貼前半。
  Trade-off：多一個常數，但語意最準。
- **推薦：方案 B** —— 同 `TRACK_DESCRIPTION` 分開講兩個 budget 嘅做法一致。

### 🟢 S-054｜row-counting gate 喺 fixture 上有約 27% 鬆位

- **位置**：`tests/test_api.py:1946-1962`
- **描述**：`test_the_widest_list_fan_out_reads_no_more_rows_than_the_budget` 用
  `year_client`（`year_db` 得 365 個 activity）。8 頁每頁收 500 但實際各只回 365 行，
  所以 assertion 實際係 `2920 <= 4000` —— 鬆咗 27%。
- **影響**：**可以捉到「完全冇收費」**（60,590 行，我 mutation 驗過會紅），
  但捉唔到「收費少計咗最多四分一」嗰種 bug。
- **方案 A**：加一句 `assert activity_rows_loaded[0] == affordable * len(year_db_activities)`
  （即精確值 2920）而唔係 `<=`。Trade-off：fixture 大細一變就要改 test。
- **方案 B**：註釋講明呢個鬆位同點解接受（fixture 一年少過 `DEFAULT_PAGE_SIZE`），
  保持 `<=`。Trade-off：唔收緊，但誠實。
- **推薦：方案 B** —— 呢條 test 嘅目的係「有冇嘢綁住」，唔係「綁得幾準」，
  而準唔準已經由 `test_the_row_budget_is_the_boundary` 同 4000/4001 邊界釘住。
  寫低鬆位就夠。

### 🟢 S-055｜被拒嘅 request 喺 server 端留低完整 traceback

- **位置**：`src/api/schema.py:483-487`（`raise ValueError`），經 graphql-core
  預設 logger 輸出
- **描述**：每個超額 request 都會喺 server log 打一個完整 Python traceback。
  `track` budget 一路都係咁（**pre-existing，唔係本次引入**），但 list budget
  令可觸發呢個 log 嘅 document 形狀多咗好多，而 endpoint 係 public、免認證。
- **影響**：log 體積 / 噪音；唔係資訊洩漏（traceback 只入 log，唔會返俾 client —— 
  我核實過 client 收到嘅只有一句乾淨訊息）。
- **方案 A**：定義一個 `BudgetExceeded(Exception)` 並喺 Strawberry 層
  mask 佢唔入 error log。Trade-off：改動到 `track` 嗰邊，超出本票 scope。
- **方案 B**：唔改代碼，開票記低，等有真實 log 噪音問題先處理。
  Trade-off：問題留住，但佢本身就係 pre-existing。
- **推薦：方案 B**，而且應該當做獨立 ticket（連 `track` 一齊），唔好塞落 CUI-0027。

### 🟢 S-056｜CUI-0027 ticket 檔冇執行紀錄，alias 軸冇 follow-up ticket

- **位置**：`.tickets/in-progress/0001-0200/CUI-0027.md`
- **描述**：三件事：
  1. 狀態歷史最後一行仲係「`MAX_LIST_ROWS_PER_REQUEST` 落地，**等用戶確認常數值**」，
     但用戶已經拍板 4000 —— ticket 冇更新。
  2. 用戶三個決定（4000、接受 breaking change、alias 軸記錄但唔處理）
     **一個都冇寫入 ticket 檔**。對比 CUI-0024 有完整「執行紀錄」section
     連四個唔同讀數嘅表，落差好明顯。
  3. DoD「餘裕同 track shape 同一個數量級（**目標 ≥ 20×**）」對最差合法 document
     嚟講係 **18.2×**（lane 報 17.4×）—— **未達標**。呢個係因為用戶決定 alias 軸
     唔處理（我唔 challenge 呢個決定），但 ticket 入面冇任何地方記低
     「DoD 呢一項按用戶決定豁免 / 順延」。
  4. 現存最高 ticket 係 CUI-0028，**alias 軸冇開 CUI-0029**。整個「記錄」
     只活喺一個 docstring 入面。
- **影響**：CUI-0027 而家唔應該直接標 completed —— 標咗就等於 DoD 全中，
  但實際上有一項係靠用戶決定豁免嘅，而豁免本身冇留痕。
- **方案 A**：喺 CUI-0027 補一個「執行紀錄」section（三個用戶決定 + 實測表 +
  DoD 逐項打勾／註明豁免），再開 CUI-0029 記 alias 軸（331 aliased `warnings`、
  824 ms、18.2×、點解 row budget 綁唔到）。
  Trade-off：多一張 ticket 同多幾分鐘。
- **方案 B**：只補 ticket，唔開 CUI-0029，靠 docstring 做唯一記錄。
  Trade-off：符合「記錄但唔處理」嘅字面意思，但呢個 repo 嘅記錄制度係 ticket，
  下次做 audit 掃 `.tickets/` 會見唔到呢條軸。
- **推薦：方案 A** —— 用戶話「記錄」，喺呢個 repo 度「記錄」嘅正式載體係 ticket，
  docstring 係補充。開票唔等於處理，唔違反用戶決定。

### 🟢 S-057｜`year` 嘅 500 行收費係 proxy 而唔係 ceiling，前提冇寫低

- **位置**：`src/api/schema.py:306-309`、`src/api/schema.py:1025-1028`
- **描述**：成個設計嘅原則係「charged on the window asked for rather than on the
  rows the page turns out to hold … which keeps the bound conservative rather
  than exact」—— 即每個收費都係上界。**`year` 係唯一例外**：佢收死 500，
  但實際讀幾多行取決於嗰年有幾多個 activity。今日 365 < 500 所以保守
  （我實測 8×`year` 讀 2,920 行、140.9 ms，同 8×`activities(limit:500)` 嘅
  2,920 行、144.7 ms 幾乎一模一樣 —— 校準得好準）。
  但一年如果多過 500 個 activity（一日兩次跑），收費就會**少計**。
  另外 `year` 每次仲讀約 384 行 weight，呢批完全冇收費。
- **點解唔係 Warning**：`DEFAULT_PAGE_SIZE` 自己個 docstring 講明佢就係
  sized to hold「a whole year in one request (the dashboard does)」，
  所以一年超過 500 個 activity 嘅話，**`DEFAULT_PAGE_SIZE` 本身會先出事，
  前端 dashboard 會先斷**，budget 少計只係第二順位嘅後果。邏輯上自洽。
- **方案 A**：喺 `year` 嗰個 bullet 補一句「this is a proxy rather than a ceiling:
  it is exact only while a year holds at most `DEFAULT_PAGE_SIZE` runs, which is
  the same assumption the dashboard's unwindowed read already makes」。
- **方案 B**：改成真收費 —— `year` 先 `count` 嗰年嘅 activity 數再收。
  Trade-off：多一條 SQL，而且違反「refuse before the SQL goes out」嘅原則。
- **推薦：方案 A**。方案 B 唔好做。

---

# Section B — CUI-0022｜刪走 `activity_time_range()`（`abe0d40`）

## B.1 改動清單

`src/common/time.py` −18 行（整個 `activity_time_range` function），ticket pending → in-progress。**淨係咁多。**

## B.2「零 caller」獨立核實 —— ✅ 成立

用 `git grep` 掃晒 1,398 個 tracked file（包含 `legacy/`、`api/`、`frontend/`、`tests/`）：

```
命中全部喺文件度，零代碼命中：
  .claude/session-logs/2026-09-16_10-27.md
  .proj-docs/audits/…  .proj-docs/qa/…  .proj-docs/tickets.md
  .tickets/in-progress/0001-0200/CUI-0022.md
```

- `legacy/`（2022 舊 script）：零命中 ✅
- `__all__`：`src/common/time.py` 根本冇 `__all__`，所以冇 re-export ✅
- Dynamic call：grep 過 `getattr(...time` / `importlib`，零命中 ✅
- 刪完之後 `ruff check` 全綠 → 冇 import 變 dead（`timedelta` / `datetime` 仲有
  `seconds_to_hhmmss` / `hhmmss_to_seconds` / `pace_str` 用緊）✅

## B.3 Coverage 93% → 100% —— ✅ 核實

```
$ .venv/bin/python -m pytest tests/test_common_time.py -q --cov=run365days.common.time --cov-report=term-missing
Name                 Stmts   Miss  Cover   Missing
src/common/time.py      40      0   100%
41 passed
```

由 ticket 記錄嘅 44 stmts / 3 miss / 93% 去到 **40 stmts / 0 miss / 100%**。
刪走 4 個 statement、消滅 3 個 miss，數啱。✅

## B.4 有冇順手刪咗唔應該刪嘅嘢 —— ✅ 冇

`git show --stat abe0d40` 只掂兩個檔（`src/common/time.py` + ticket 檔），
`-18` 行全部係 `activity_time_range` 嘅 signature + docstring + body。
**零測試被刪**（我 grep 過成個 delta 嘅 `-.*def test_`，只有 CUI-0027 嗰個
`DOCUMENTED_WORST_CASES` 項，同呢張票無關）。
Commit message 對「無 caller / 無 `__all__` / 無 dynamic lookup / 無 import 變 dead /
period=30 只活喺 signature default」逐項交代過，**我核實全部屬實**。

## B.5 發現

**零 finding。** 呢張票做得乾淨。Commit message 仲主動寫低咗
「whoever revives it writes the first assertion for the untested ceiling boundary
(`end.minute` exactly on a period edge)」—— 即係將 QA 喺票入面指出嗰個未測試邊界
一齊交俾將來，冇當佢唔存在。做得好。

---

# Section C — CUI-0024｜清 AU-035 docs drift（`c768a4f` + `1ace951`）

## C.1 六項改動逐項對返代碼 —— ✅ 六項全中

| # | 位置 | 處理 | 我嘅核實 |
|---|---|---|---|
| 1 | `README.md:118` | 「three console scripts」→ 指向下面個表 + `[project.scripts]` | ✅ `pyproject.toml` 實際 **4** 個（`run365-activities` / `run365-weather` / `run365-export` / `run365-schema`），README 下面個表亦係列齊 4 個 |
| 2 | `docs/architecture.md:28` | 「Three console scripts」→「The console scripts…（declared under `[project.scripts]`）」 | ✅ 同上 |
| 3 | `docs/architecture.md:104` | 刪走 `write_data_js`，保留 `load_jsonl` | ✅ `grep -rn write_data_js README.md docs/architecture.md src/ tests/ frontend/src` 零命中；`src/dashboard/builder.py:112 def load_jsonl` 入面 `:116 with open(path)` 係成個 `dashboard.builder` **唯一**一個 `open(` → 「file I/O is limited to `load_jsonl`」改完之後係準確嘅 |
| 4 | `docs/architecture.md:139` | 刪走「(133 tests)」 | ✅ 已刪 |
| 5 | `README.md:212` | 「93 tests cover…」→「The Python suite covers…」 | ✅ 覆蓋範圍描述原封不動 |
| 6 | `README.md:214` | 「87 Vitest tests」→「the Vitest suite covers…」 | ✅ 同上 |

`grep -rnE "[0-9]+ (tests|Vitest)" README.md docs/*.md` 而家只剩 `docs/CHANGELOG.md`
兩處（「35 Vitest tests」/「51 tests」）—— **歷史紀錄，正確保留。**

## C.2「nine views」同 CHANGELOG 嘅 `write_data_js` 有冇被誤改 —— ✅ 冇

- `README.md:220` 「all nine views」**仲喺度，冇改** ✅
  實測 `frontend/src/views/` 有 10 個 entry，其中 `views.test.tsx` 係檔案，
  **目錄啱啱好 9 個**（activities / activity / load / overview / performance /
  settings / weather / weight / year）→ 「nine」正確。
- `docs/CHANGELOG.md:170` 嘅 `write_data_js` **仲喺度，冇改** ✅
  （`git diff d108c82..HEAD -- docs/CHANGELOG.md` 零 diff）

## C.3 CUI-0015 收窄咗嘅 CI 段落有冇被還原 —— ✅ 冇，核實過

呢條 branch 一度由舊 base（`e827d95`，PR #13 merge **之前**）開過，rebase 落
`d108c82`，所以呢個係真風險。核實方法：

```
$ git diff d108c82..HEAD -- docs/deployment.md
（零 diff —— CUI-0015 改過嘅第三個檔完全冇掂）

$ git diff d108c82..HEAD -- README.md docs/architecture.md \
    | grep -iE "^[+-].*(GitHub Actions|workflow|pages.yml|CI |develop|master)"
（零命中 —— 冇一行 CI 相關文字被加或刪）
```

`docs/architecture.md` 嘅 CI bullet（「**GitHub Actions** (`.github/workflows/pages.yml`)
runs the ruff and pytest gates above, together with the frontend checks, on the
branches…」）喺 diff 入面係 **context line**，即完全冇動。
**✅ CUI-0015 嘅成果完整保留，rebase 冇還原任何嘢。**

## C.4「移除所有寫死數字（包括 console script 數）」呢個做法啱唔啱

**我認為啱，而且理由比 ticket 寫嘅更強。**

Ticket 原本嘅建議係「第 1、2 項（console script 數目）改動頻率低好多，直接更正亦可接受
—— 執票者判斷」。執票者揀咗連 console script 數都移除。判斷依據寫喺 commit message：
**同一日、同一個 repo，測試數出現過四個唔同讀數（363 / 364 / 351 / 365），
而且呢個 commit message 自己第一版寫錯咗 351，rebase 之後即刻變錯。**
呢個係「寫死數字必然會錯」嘅實證，唔係論證。

**有冇刪走讀者真正需要嘅資訊？我逐項睇過 —— 冇：**

| 移除咗嘅 | 讀者仲搵唔搵到？ |
|---|---|
| 「three console scripts」（README） | ✅ **同一段落下面就係一個列齊 4 個 script 嘅表**，連「What it does / Reads / Writes」都有。數字係純冗餘 —— 讀者數個表就知，而且個表唔會 drift |
| 「Three console scripts」（architecture） | ✅ 改成指向 `[project.scripts]`，即 SSoT 本身。比一個數字更有用：讀者知去邊度搵權威答案 |
| 「93 tests」/「87 Vitest tests」/「133 tests」 | ✅ 三處嘅**覆蓋範圍描述全部原封不動**（「geo and time helpers、MET and calorie maths、weight parsing (including the undated-last-line quirk)…」）。「測試覆蓋咗乜」先係讀者需要嘅資訊；「有幾多條」對讀者嚟講冇任何 actionable 價值 —— 佢唔會因為知道係 378 條而做到任何嘢，而佢隨時可以跑 `pytest tests -q` 攞最新數 |

**唯一可以商榷嘅**：console script 個數確實係低頻率改動，保留一個數字亦唔會經常錯。
但既然下面個表已經列齊，移除係嚴格更優 —— 一份資料只有一個出處。
**呢個做法啱，而且做法一致（六項用同一把尺）。**

## C.5 `1ace951` —— 收拾重複 ticket 檔

Lane 喺舊 base 開 worktree，結果係喺 `in-progress/` **新建**咗一份 CUI-0024
而唔係 **move** 咗 `pending/` 嗰份，造成兩份共存。`1ace951` 刪走 `pending/` 嗰份
（92 行），保留帶執行紀錄嗰份。核實：

```
$ ls .tickets/pending/0001-0200/ | grep -E "0022|0024|0027"
（零命中）
$ find .tickets -name "CUI-002[247].md"
.tickets/in-progress/0001-0200/CUI-0022.md
.tickets/in-progress/0001-0200/CUI-0024.md
.tickets/in-progress/0001-0200/CUI-0027.md
```

✅ 三張都喺 `in-progress/`，冇重複，冇殘留。

## C.6 發現

**零 finding。** 判斷、執行同記錄都一致，而且 ticket 檔嘅「執行紀錄」section
係本批三張入面寫得最好嘅一份（S-056 就係攞佢做對比標準）。

---

# 橫向檢查

## 測試數 365 → 378（+13）—— ✅ 數啱，零測試被刪

| 來源 | 條數 |
|---|---|
| `test_the_widest_list_fan_out_reads_no_more_rows_than_the_budget` | +1 |
| `test_the_widest_list_fan_out_is_refused_after_the_budget` | +1 |
| `test_the_row_budget_is_the_boundary` | +1 |
| `test_a_refused_page_stops_the_operation_where_it_stands` | +1 |
| `test_the_row_budget_is_per_request_and_does_not_leak_across_requests` | +1 |
| `test_year_pays_a_page_of_the_row_budget` | +1 |
| `test_every_document_the_front_end_sends_is_inside_the_row_budget` | **+5**（parametrized） |
| `test_the_whole_front_end_in_one_request_is_still_inside_the_row_budget` | +1 |
| `test_every_list_field_at_the_maximum_page_is_still_inside_the_row_budget` | +1 |
| `test_the_fan_out_cost_does_not_widen_with_the_document` | +1 |
| **小計** | **+14** |
| `DOCUMENTED_WORST_CASES` 移除 `_list_flood(166)` 一項 | **−1** |
| **淨** | **+13** |

365 + 13 = **378** ✅ 同實測完全對上。

`git diff d108c82..HEAD | grep -E "^-.*def test_"` **零命中** → **冇任何 test function
被刪**。唯一減少嗰條係 `DOCUMENTED_WORST_CASES` 嘅一個 tuple，而且刪得啱 ——
嗰個 table 嘅語意係「served worst cases」，該 document 而家被拒，
再擺喺度就係假嘅；佢嘅 width 同 token 數改為由 `LIST_FLOOD_WIDTH` /
`LIST_FLOOD_TOKENS` 兩個有 docstring 嘅常數 + 新 test 接住，冇失去任何釘子。
**移除有 5 行註釋交代埋原因**，做得好。

## Scope 越界 —— ✅ 冇

三個 commit 各自只掂自己該掂嘅檔：

- `abe0d40`：`src/common/time.py` + ticket
- `c768a4f`：`README.md` + `docs/architecture.md` + ticket
- `bb8032a`：`src/api/schema.py` + `frontend/schema.graphql` + `tests/test_api.py` + ticket

冇交叉、冇夾帶。`docs/CHANGELOG.md`、`docs/deployment.md`、`docs/roadmap.md`、
`frontend/src/` 全部零改動。

## Secret / 絕對路徑 / model identifier —— ✅ 全清

```
$ git diff d108c82..HEAD | grep -E "^\+" \
    | grep -iE "/home/|/Users/|api[_-]?key|secret|token *= *['\"]|password|claude-(opus|sonnet|haiku)|gpt-"
（零命中）
```

（Commit trailer 入面嘅 `Co-Authored-By` / `Claude-Session` 係規定格式，唔算。）

## `.tickets/` 狀態 —— ✅ 三張都喺 `in-progress/`，冇重複

見 C.5。`.proj-docs/tickets.md` 三行仲寫住 `pending` —— 核實過呢個 repo 嘅慣例係
**完成先更新 registry**（29 行全部係 `pending`，包括已 completed 嘅），
所以 in-flight 狀態下唔一致係正常，**唔算 finding**。

## SDL 只加 description —— ✅ 核實

見 A.7 尾段。5 個 hunk 全部係 description，零 type / field / argument 改動，
`run365-schema --check` 綠、codegen 綠、`tsc` 綠、126 條前端測試綠。

---

# ✅ 做得好嘅地方（跨 fix 通用）

1. **「refuse before the SQL goes out」呢個原則貫徹到底，而且驗得到。**
   被拒嘅 166-field document 同「啱啱好食晒 budget」嘅 8-field document 發出
   **完全一樣嘅 16 條 statement** —— 呢個唔係靠 code review 睇出嚟，係設計成
   可以量度。收費喺 `_page()` 之後、`service.*()` 之前，`raise` 喺寫返 counter 之前。

2. **Regression gate 揀得啱，而且兩個都真係有牙。**
   ticket 明確警告過「CUI-0019 教訓：statement 數唔等於成本」，lane 唔單止收到訊息，
   仲揀咗 **ORM row count + VDBE step slope** 兩個互補嘅非 wall-clock 量度。
   我人為注入 O(N²) 之後 gate 即刻紅，注入「冇收費」之後另一個 gate 亦即刻紅，
   而且紅出嚟嘅數字（60,590 行）啱啱好就係 ticket 記錄嗰個。
   **零 wall-clock assertion** —— docstring 亦明文講低點解唔 assert 時間。

3. **Docstring 對「邊啲數字有 assertion 撐、邊啲冇」分得好清楚。**
   `MAX_LIST_ROWS_PER_REQUEST` 尾段明講「None of these readings is asserted,
   because a wall time in CI buys a flaky test rather than a guarantee;
   what is asserted is the rows, in `test_…`」。仲特登為「332 statements」加咗
   一段講明佢由「reading」變咗「history」。**舊文字冇留低做半真半假嘅殘骸。**

4. **等價性做到 byte 級，而且覆蓋咗 error path。**
   我獨立驗證 24 個 document、21 個 SHA-256 一致，包括五條前端真實 document、
   filtered / paged variant、四個 count、track shape、同三個 error case。
   三個唔一致嘅啱啱好就係三個 intended break。

5. **`_charge_list_rows` 完全跟足 `_charge_track_field` 嘅既有 pattern** ——
   訊息句式、「deliberately not surfacing the key」嗰句註釋、`Raises:` section 結構
   全部對齊。新讀者讀完一個就識另一個。S-014 / W-013 定落嘅先例守得住。

6. **主動處理咗「唔適用」而唔係靜靜跳過。**
   `_charge_list_rows` docstring 專門有一段解釋點解 `_charge_track_field` 嗰個
   sticky / not-sticky 分別喺呢度唔存在（non-null propagation），
   **而且開咗 `test_a_refused_page_stops_the_operation_where_it_stands` 去釘住個理由**，
   而唔係留低「應該冇問題」。

7. **CUI-0024 用自己身上發生嘅事去證明自己個判斷。**
   Commit message 講低「呢個 commit message 第一版寫咗 351，rebase 之後即刻變錯」——
   用一次真實失敗去支持「移除寫死數字」而唔係「更新寫死數字」。
   **連記錄 drift 嘅文字本身都 drift 咗** 呢個觀察，比任何論證都有力。

8. **CUI-0022 將「未測試嘅邊界」交俾將來而唔係當佢唔存在。**
   Commit message 明講「whoever revives it writes the first assertion for the
   untested ceiling boundary」—— QA 喺票入面提出嘅 `end.minute` 邊界問題
   冇因為刪咗 function 就消失。

9. **Lane 嘅量度全部保守。** 三個效能數字（170 ms / 542 ms / 863 ms）
   我實測係 161 ms / 477 ms / 824 ms，**每一個 lane 都報得比實際差**。
   呢個係啱嘅方向 —— 效能聲稱寧願低估。

---

# 修正優先順序

| 順序 | ID | 嚴重度 | 內容 | 建議方案 | 成本 |
|---|---|---|---|---|---|
| 1 | **W-027** | 🟡 | `_charge_list_rows` fail-closed 分支冇 test | 方案 B（parametrize `track` 現有兩條） | ~30 分鐘 |
| 2 | **W-028** | 🟡 | 「What pays, and what does not」列表漏 5 個 field | 方案 A + B（寫規則 + 補齊實測數） | ~20 分鐘 |
| 3 | S-056 | 🟢 | CUI-0027 ticket 冇執行紀錄、alias 軸冇開票 | 方案 A（補 ticket + 開 CUI-0029） | ~20 分鐘 |
| 4 | S-053 | 🟢 | `LIST_ROWS_NOTE` 貼咗落冇 `limit` 嘅 `year` | 方案 B（拆兩半） | ~10 分鐘 |
| 5 | S-052 | 🟢 | 兩個 test 註釋仲寫 `_TrackBudget` | 跟 W-027 一齊做 | ~2 分鐘 |
| 6 | S-057 | 🟢 | `year` 嘅 500 係 proxy 唔係 ceiling，前提冇寫低 | 方案 A（補一句） | ~5 分鐘 |
| 7 | S-054 | 🟢 | row gate 有 27% 鬆位 | 方案 B（註釋講明） | ~5 分鐘 |
| 8 | S-055 | 🟢 | 被拒 request 留低 full traceback | 方案 B（獨立開票，連 `track` 一齊） | 開票即可 |

> **Commit 規範**：每個 review item 一個獨立 commit，
> format `fix: <ID> | <簡短描述>`（例：`fix: W-027 | pin the list budget's fail-closed branch`）。
> 唔可以合併，唔可以夾帶無關改動。S-052 係唯一例外 —— 佢喺 W-027 改嘅同一段代碼入面，
> 可以併入 W-027 嗰個 commit。

---

# 修訂後代碼

> 本次 **0 Critical**，冇任何嘢需要即時改先可以用。以下係兩個 Warning 嘅建議實作，
> 由 developer 執行（Reviewer 唔改代碼）。

## W-027 + S-052 — `tests/test_api.py`

```python
# ── 取代現有嘅 test_track_fails_closed_when_the_schema_has_no_budget_extension
#    同 test_track_fails_closed_when_the_context_cannot_be_seeded ──────────

BUDGETED_DOCUMENTS = (
    # 每個 budget 一個最平嘅 document。_RequestBudgets 而家 seed 三個 budget，
    # 所以呢度亦係複數 —— 一個 budget 嘅 fail-closed 分支冇 test，
    # 等於嗰個 budget 喺 seeding 改動之下會靜靜變成 unbounded（CUI-0027 / W-027）。
    ("track", TRACK_QUERY),
    ("list rows via activities", "{ activities { id } }"),
    ("list rows via year", "{ year { year } }"),
)
"""(label, document) for each field that refuses to serve on an unseeded budget."""


@pytest.mark.parametrize(
    ("label", "document"), BUDGETED_DOCUMENTS, ids=[c[0] for c in BUDGETED_DOCUMENTS]
)
def test_a_budgeted_field_fails_closed_without_the_budget_extension(
    label, document, year_session
):
    # A schema built without _RequestBudgets seeds nothing. Serving the field
    # anyway would mean an unbounded request, so it must refuse instead.
    unbounded = strawberry.Schema(query=Query, extensions=[])

    result = unbounded.execute_sync(document, context_value={"session": year_session})

    assert result.errors, f"{label} was served on an unseeded budget"
    assert NOT_SEEDED in result.errors[0].message
    assert result.data is None


@pytest.mark.parametrize(
    ("label", "document"), BUDGETED_DOCUMENTS, ids=[c[0] for c in BUDGETED_DOCUMENTS]
)
def test_a_budgeted_field_fails_closed_when_the_context_cannot_be_seeded(
    label, document, year_session
):
    # A read-only Mapping is not a MutableMapping, so _RequestBudgets skips it --
    # but info.context["session"] still reads, so the field is reachable. The
    # invariant is that the *missing budget* is what refuses it. This context
    # happens to reject the write back as well, so it would error either way;
    # NOT_SEEDED is what pins the reason rather than the symptom.
    frozen = MappingProxyType({"session": year_session})

    result = schema.execute_sync(document, context_value=frozen)

    assert result.errors, f"{label} was served on a context that could not be seeded"
    assert NOT_SEEDED in result.errors[0].message
    assert result.data is None
```

> ℹ️ 核實過 `tests/test_api.py:1311` 嘅 `NOT_SEEDED = "not seeded"`，
> **已經同時 match 兩句訊息**（`"track budget was not seeded for this request…"` /
> `"list row budget was not seeded for this request…"`），所以上面段 code 唔使改佢。
> 但順帶一提：正因為佢咁鬆，佢分唔到係邊個 budget 拒絕咗 —— 如果三個 case
> 共用同一個片語，一個「list 用咗 track 嘅訊息」嘅 bug 會照樣綠。
> 建議 `BUDGETED_DOCUMENTS` 每個 tuple 多帶一個 expected 片語
> （`"track budget"` / `"list row budget"`），assert 嗰句改成
> `assert expected in result.errors[0].message`。**唔好為咗夾個 assertion 而放寬到
> 淨係 check `"budget"`** —— 咁就變返「釘症狀唔釘原因」。

## W-028 — `src/api/schema.py`，`MAX_LIST_ROWS_PER_REQUEST` docstring

```python
What pays, and what does not. A field pays if and only if the rows it reads
grow with a window -- one the client names, or one the field opens on its
behalf. A field that returns a fixed number of rows however it is asked does
not, and charging it would buy noise rather than a bound:

* the four list fields, each charged its ``limit`` -- :data:`DEFAULT_PAGE_SIZE`
  when the client names no window;
* ``year``, charged one :data:`DEFAULT_PAGE_SIZE` page. It takes no ``limit``
  and opens no window, but it reads a whole calendar year of activities
  unwindowed, which is the read DEFAULT_PAGE_SIZE was sized for. That makes it
  the one charge here that is a proxy rather than a ceiling: it is exact only
  while a year holds at most DEFAULT_PAGE_SIZE runs -- the same assumption the
  dashboard's unwindowed read already rests on, so DEFAULT_PAGE_SIZE itself
  would want revisiting before this charge did (S-057);
* ``activity(id:)`` does not pay. It reads one row by primary key, and
  :data:`MAX_QUERY_TOKENS` admits at most 90 of them: 90 rows and ~0.05 s on
  the export, some 300x inside the 15 s function;
* the four ``*Count`` fields and ``meta`` do not pay either, for the same
  reason: a count is one scan returning one row, and ``meta`` returns two.
  The widest floods MAX_QUERY_TOKENS admits -- 332 counts, 166 ``meta`` --
  measure 83 ms and 48 ms on the export, 180x and 315x inside the function
  (W-028).

The bound this constant states is therefore on *paged* reads, and a request may
hold that many rows plus up to 90 single ones, 332 counts and 166 ``meta``.
```

---

# 整體建議

1. **W-027 同 W-028 都係「呢個 module 對自己嘅要求」，唔係外部標準。**
   `src/api/schema.py` 已經係我見過最嚴謹嘅其中一個 module —— 每個常數有 docstring、
   每個數字有 test 撐、每個「唔做」嘅決定有量化理由。
   正因為咁，一條冇 test 嘅 fail-closed 分支同一個唔完整嘅窮舉列表先特別刺眼：
   **佢哋唔符合呢個檔自己定落嘅標準。** 兩個加埋大約一個鐘。

2. **修完 W-027 / W-028 之後，呢批嘢應該係 pass。** 兩個都係加嘢
   （一段 docstring、幾條 test），零行為改動，唔會令等價性或效能結論失效，
   所以再 review 可以只睇 delta，唔使重做 A.1–A.7。

3. **CUI-0027 唔應該直接標 completed（S-056）。**
   DoD 有一項（餘裕 ≥ 20×）係靠用戶決定「alias 軸唔處理」而豁免嘅，
   實測最差合法 document 係 18.2×。呢個豁免要喺 ticket 入面留痕，
   再開 CUI-0029 記住 alias 軸。**開票唔等於處理，唔違反用戶決定。**

4. **關於 `4000` 呢個數字本身**（用戶已拍板，我唔 challenge，只補資料供將來參考）：
   我實測佢留低 **31.4×** 餘裕，而前端最大真實 document 只用 500（budget 嘅 1/8），
   四個 list field 同時食盡 `MAX_PAGE_SIZE` 啱啱好等於 4000。
   **三個約束啱啱好喺呢個數字度對齊**，選得好。

5. **下一個 bottleneck 已經識別咗**：alias 軸，824 ms / 18.2×。
   如果將來要再收，應該收 *fields resolved*，唔係收 rows —— docstring 已經
   講明原因。呢個係一個好嘅交接。

---

# 驗證用臨時改動聲明

為咗驗證兩個 linearity gate 有冇牙，我對 working tree 做過以下臨時改動，
**全部已還原**：

| # | 改動 | 目的 | 還原方式 |
|---|---|---|---|
| 1 | `MAX_LIST_ROWS_PER_REQUEST = 4000` → `10**9` | 試還原舊行為 | 由 `schema.py.bak` 覆蓋 |
| 2 | 刪走 `Query.activities` 入面 `_charge_list_rows(info, limit)` | 整紅 gate 1 | 同上 |
| 3 | 喺 `Query.activities` 注入隨闊度增長嘅額外查詢 | 整紅 gate 2 | 同上 |
| 4 | `git worktree add … d108c82` | 攞真正嘅 before 代碼做等價性對比 | `git worktree remove --force` |

**還原後核實**：

```
$ md5sum src/api/schema.py tests/test_api.py
303ac33bea274540b13a872d0653a45d  src/api/schema.py      （同 backup 一致）
5566777b2e2e720a61e913977ee1ecee  tests/test_api.py      （同 backup 一致）

$ git diff --stat
（空）
$ git status --porcelain
（空）
$ git log --oneline -1
1ace951 fix: drop the duplicate CUI-0024 left in pending
```

**✅ Working tree 完全乾淨，`git diff --stat` 為空，HEAD 未變。**
Reviewer 全程冇執行過任何 `git merge` / `git push` / `git commit`。
所有量度 script 寫喺 scratchpad，唔喺 repo 入面。
所有效能量度用 `data/processed/run365.db`（git-ignored 嘅 production export，
365 activities / 134,041 track_points），冇改過佢。

---

```handoff-receipt
protocol: 1
agent: code-reviewer
status: warn
score: 84/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "99% (src/api/schema.py), 100% (src/common/time.py)"
critical: 0
warning: 2
suggestion: 6
report: .proj-docs/reviews/2026-09-16_review_CUI-0027_batch.md
tickets: CUI-0027, CUI-0022, CUI-0024
next_action: invoke_developer
next_agent: backend-developer
branch: "claude/ai-dev-team-start-05jie2"
context: "CUI-0022 同 CUI-0024 零 finding 可直接收；CUI-0027 實作正確（等價性 21/24 byte-identical、3 個唔同全部係 intended break；效能聲稱獨立重量全部成立且 lane 保守；兩個 linearity gate 人為整紅驗證有牙），但有 2 個 Warning 要補：W-027 fail-closed 分支冇 test、W-028 docstring 收費列表漏 5 個 field。"
blockers:
  - "W-027: _charge_list_rows 嘅 unseeded-budget fail-closed 分支（src/api/schema.py:477-481）冇任何 test，coverage 唯一 miss 就係佢；track 嗰邊有兩條同款 test，_RequestBudgets docstring 亦聲稱兩者行為一致但無 assertion 撐。建議 parametrize 現有兩條 track test 涵蓋 activities / year。"
  - "W-028: MAX_LIST_ROWS_PER_REQUEST docstring 嘅「What pays, and what does not」列表睇落窮舉，實際 11 個 root field 只講咗 6 個，漏咗 meta 同四個 *Count（實測 83ms/181x、48ms/315x，唔收費係啱嘅，但要寫低規則同數字）。"
```
