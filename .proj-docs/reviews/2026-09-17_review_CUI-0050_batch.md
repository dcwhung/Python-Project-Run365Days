# Batch Review — 2026-09-17 — CUI-0050 / CUI-0048 / CUI-0051

**審閱者**：Code Reviewer subagent
**目標**：`git diff 77868dd..86ca2ad -- src/api/ tests/test_api.py frontend/schema.graphql`
**HEAD**：`86ca2ad`（三條 lane 嘅 merge）
**涵蓋 commit**：`b912658`（CUI-0050）、`3517d50`（CUI-0048）、`7bf3aca`（CUI-0051）

## 整體 verdict

改動範圍細（4 個檔案、310 insertions / 34 deletions），但改嘅係一條**新 wire 契約**。我獨立重量咗 lane 自報嘅五條推翻，**五條全部成立**，冇一條需要反推翻。工程質素本身相當高：5 個 mutant 我親手落，4 個被測試套件殺死，其中「拆兩個 code」嗰個連**原有**嘅 CUI-0018 (a) 守衛都紅埋。

但**第 5 個 mutant 活咗**——而佢活得正正就係本 batch 自己立咗一條文字承諾嘅地方。呢個係本次唯一有份量嘅發現。

- Hard gates：**全 pass**（下表）
- 🔴 Critical：**0**
- 🟡 Warning：**2**（W-050、W-051）
- 🟢 Suggestion：**4**（S-103 … S-106）
- 總分：**86/100**
- Status：**⚠️ warn**（DoD 要 ≥ 90；hard gate 全綠，無 Critical，唔係 fail）

---

## Hard Gates 結果

| Gate | 指令 | 結果 |
|---|---|---|
| Lint（Python） | `ruff check src tests` | ✅ pass — All checks passed! |
| Format（Python） | `ruff format --check src tests` | ✅ pass — 61 files already formatted |
| Lint（前端） | `npm run lint` | ✅ pass — exit 0 |
| Type check | `npm run typecheck`（codegen + `tsc -b`） | ✅ pass — exit 0 |
| Tests（Python） | `pytest tests -q` | ✅ pass — **573 passed** |
| Tests（前端） | **`npm test`**（codegen + vitest） | ✅ pass — **144 tests / 25 files** |
| Coverage | `--cov=src --cov=api` | ✅ pass — `api/graphql.py` **100%**、`src/api/schema.py` **100%**、`src/api/service.py` **100%**、**TOTAL 96%** |
| SDL 同步 | `run365-schema --check frontend/schema.graphql` | ✅ pass — is up to date |
| Security（npm） | `npm audit`（全樹，冇 `--omit=dev`） | ✅ pass — **found 0 vulnerabilities** |
| Security（pip） | `pip install -U pip setuptools` → `pip-audit` | ✅ pass — No known vulnerabilities found |
| Build api mode | `npm run build` | ✅ pass |
| Build static mode | `npm run build:static` | ✅ pass |
| 分層鐵律 | `grep strawberry src/api/service.py` | ✅ pass — 零命中 |

全部數字同 main agent 交落嚟嘅預期**逐個對上**，冇一個要修正。

---

## 逐條推翻嘅獨立裁決

### 裁決 1 — CUI-0051 byte 數：**lane 對，QA 錯（但錯得情有可原）** ✅

我自己重量。臨時 probe：無任何 logging handler（`logging.lastResort` 條件，即 Vercel 實際跑嘅配置）、`contextlib.redirect_stderr` 收 stderr、in-process `setattr` 落 `REFUSAL_LOG_LEVEL`、**改完即刻讀返個 level 做 oracle** 確認 mutant 真係生效。

| Document | WARNING | INFO |
|---|---|---|
| `{ activities(offset: -1) { id } }`（33 bytes 請求） | **103 bytes** | **0** |
| `REFUSED_DOCUMENTS` 個 `list-rows` param（173 bytes 請求） | **341 bytes** | **0** |

**103 / 341，一個字節不差。** 註釋寫嘅數從來冇錯過；錯嘅淨係冇講用邊個 document。QA 量到 102/332 同 161/239 唔係矛盾，係換咗 document（byte 數 100% 由 document 決定）。Lane 嘅裁決成立。

### 裁決 2 — CUI-0051 換理由（S-085 唔適用）：**lane 對** ✅

我 dump 咗兩條 refusal 嘅 stderr 全文：

```
offset must not be negative, got -1

GraphQL request:1:3
1 | { activities(offset: -1) { id } }
  |   ^
```

兩條 **一個絕對路徑都冇**（`exc_info=None`，唔 render traceback）。所以張票寫嘅「S-085 已經證過：checkout 絕對路徑長度會影響」喺呢個 comment 上**根本唔適用**——張票係把 S-085 嘅結論隔空套落嚟，冇驗過。Lane 驗咗，換理由係對嘅。

Lane 換上嘅理由我亦驗到：graphql-core 把出事嗰行 document 原文 render 入 message。bounds 條 103 bytes 入面英文只佔 35，其餘 68 係 document 原文加 gutter；budget 條 341 入面英文佔 64，其餘 277 全部係 document render。「大部分係 document 自己嘅字」成立。

> 附帶裁決：lane 實際做嘅係**方案 B + 名咗 document**（數剷走晒、document 名咗），同時滿足張票兩條驗證方式。做法比張票任何一個單一方案都好。

### 裁決 3 — CUI-0048 建議修法：**lane 對，而且係本 batch 最有價值嘅一發現** ✅

Mutant：`points < 0` → `not (points > 0)`，清 `__pycache__`，`inspect.getsource` 讀返確認 mutant 真係載入。

```
FAILED test_a_falsy_points_still_means_the_ceiling_rather_than_a_refusal[None]
FAILED test_a_falsy_points_still_means_the_ceiling_rather_than_a_refusal[0]
FAILED test_a_falsy_points_still_means_the_ceiling_rather_than_a_refusal[False]
FAILED test_a_falsy_points_still_means_the_ceiling_rather_than_a_refusal[-0.0]
4 failed, 569 passed
```

**紅 4 條，同 lane 聲稱一致。** `not (0 > 0)` 係 `True`，`points=0` 即刻開始 raise，而 `0` 喺呢層係合法並且喺 docstring 上兩段寫明。張票寫「Guard 改 `not (points > 0)`（同時收 `-0.0`）」係**開窿唔係補窿**。

> ⚠️ 一個精準修正：lane 講「推翻咗張票嘅建議修法」略為 overstate。張票其實開咗**兩個**選項（「改 guard **或者** docstring 明文寫低 `-0.0` 當 `0`」），並寫「兩個都得」。落地嗰個係第二個選項，**喺張票個菜單入面**。真正被推翻嘅係「**兩個都得**」呢句——實測其中一個完全唔得。呢個修正唔減損 lane 嘅發現價值，反而令佢更精準：張票畀咗一個會炸嘅選項，lane 逐個驗先揀。

另外留意 `[None]` 都紅——佢紅唔係因為 guard，係因為 CUI-0048 新加嗰句 `assert rows == service.tracks(varied_session, [OVER_CAP_ID], 0)`（對照 `0`）。即係話**新加嗰句對照 assertion 本身就有殺傷力**，唔係裝飾。

### 裁決 4 — CUI-0048 `-0.0` 當 `0` 嘅論證同自洽性：**成立** ✅

IEEE-754 三條我都核過：`-0.0 == 0` 真、`-0.0 < 0` 假、`bool(-0.0)` 假。符號位記錄嘅係「由邊個方向趨近零」，唔係一個細過零嘅量值——一個 sample count 對呢件事冇意見。

同 CUI-0040 自洽性：CUI-0040 劃嘅係「我冇問過」vs「我問咗一個唔可能存在嘅嘢」，第二類係**低於**零嘅值。`-0.0` 唔係低於零。所以 guard（`< 0`）同下面個 falsy test（`if points`）對 `-0.0` 得出同一個答案，**兩者一致本身就係契約**，唔係契約入面一個窿。呢個論證成立，而且同 CUI-0040 立場自洽。

反面亦成立：要 refuse `-0.0` 就要讀符號位（`math.copysign` 或者嗅 `repr`），即係**按一個值點樣寫**而唔係**佢係乜**去 refuse 佢，而且冇辦法用呢個 function 自己嘅字彙（「negative」）講得出口。

`TypeError` 嗰半我亦核過：`_stored_counts` 排喺 sampler 之前（源碼順序確認），所以 docstring 講「Costs one grouped COUNT before it raises」係真；`Int` coercion 三層之上擋死 fractional literal 亦係真（我喺 wire 上實測 `track(points: 2.5)` → `Int cannot represent non-integer value: 2.5`，`extensions` 空）。

### 裁決 5 — CUI-0050「`code` 分類 / `argument` 定位」條線：**成立，而且有守衛** ✅

判準「個值係咪由 client 自己送過嚟」我認為企得住：argument name 係 client 自己份 document 讀返畀佢，server 冇講多過佢送嘅嘢；budget counter 係 server state，唔可能由 document 推導，而且 client 會開始照住個數 pacing，等於把 extension 嘅簿記變成契約。呢個係 W-013 條線被**重新論證**，唔係偷偷加 key——我用兩個 mutant 驗證咗條線兩邊都有牙（見下）。

---

## Mutation 重做（5 個，逐個 in-process oracle 確認 mutant 真係生效）

方法：改源碼 → `rm -rf __pycache__` → `inspect.getsource` / `inspect.signature` 讀返確認 mutant 載入咗（唔假設）→ 跑套件 → `git checkout --` 還原 → **md5 核實**。

| # | Mutant | Lane 聲稱 | 我實測 | 判定 |
|---|---|---|---|---|
| M1 | `points < 0` → `not (points > 0)` | 紅 4 | **紅 4** | ✅ 一致 |
| M2 | 拆 `LIMIT_/OFFSET_OUT_OF_RANGE` 兩個 code | 紅 14 | **紅 14** | ✅ 一致 |
| M3 | `extensions` 混入 `list_rows_remaining` | 紅 9 | **紅 9** | ✅ 一致 |
| M4 | 所有 refusal（連 budget）都塞 `argument` | 紅 3 | **紅 3** | ✅ 一致 |
| M5 | SDL description 把 `extensions.argument` 改名 `extensions.which` | （lane 冇報） | **紅 4 + `run365-schema --check` exit 1** | ✅ gate 有牙 |
| **M6** | **`extensions` 加第三個 key `retryAfter`** | （lane 冇報） | **573 passed，零紅，而且個 key 真係去到 wire** | ❌ **mutant 生還 → W-050** |

**M2 最重要嗰點證實咗**：紅 14 條入面包含**原有**嘅

```
test_a_bounds_refusal_carries_a_machine_readable_code[×4]
test_the_four_refusals_a_client_can_earn_carry_four_different_codes
test_a_refused_page_still_takes_the_whole_response
```

即係 CUI-0018 (a)「一個 remedy 一個 code」條線**仍然有守衛**，新加嘅 `argument` key 冇令佢鬆手。

**M3 嘅守衛我按要求核實過讀乜**：`test_a_coded_refusal_still_leaks_nothing_it_did_not_leak_before` 用嘅係
`year_client.post(...).get_data(as_text=True)` ——**raw response text，唔係 parsed body**，註釋亦明文講明點解（「so a key added anywhere -- nested under a sub-object, or on an error this test did not think to look at -- is caught by the same line」）。Lane 聲稱正確。

---

## 新 wire 契約全面驗證（獨立 oracle，唔經測試套件）

自建 Flask test client + 臨時 DB，直接讀 response：

**16 格（4 條 list field × 4 個越界點）全部 OK**

| Field | `limit: -5, offset: 0` | `limit: 0` | `limit: 1001` | `limit: 10, offset: -1` |
|---|---|---|---|---|
| `activities` | `limit` | `limit` | `limit` | `offset` |
| `weight` | `limit` | `limit` | `limit` | `offset` |
| `weather` | `limit` | `limit` | `limit` | `offset` |
| `warnings` | `limit` | `limit` | `limit` | `offset` |

全部 `code=ARGUMENT_OUT_OF_RANGE`、`path=[field]`、`locations=[{line:1,column:3}]`（證實 `locations` 依然指住 field name 而唔係 argument，即係呢個 key 嘅存在理由仍然有效）。

**`track(points:)` 三格**：`points: 0` / `1001` / `-1` → 全部 `argument="points"`、`path=["activity","track"]`。

**Budget refusal 三個 code 全部冇 `argument`**：

| Code | errors | `argument`? | raw 有冇 `remaining`? |
|---|---|---|---|
| `LIST_ROW_BUDGET_EXCEEDED` | 1 | ❌ 冇 | ❌ 冇 |
| `TRACK_FIELD_BUDGET_EXCEEDED` | 1 | ❌ 冇 | ❌ 冇 |
| `TRACK_POINTS_BUDGET_EXCEEDED` | 5 | ❌ 冇 | ❌ 冇 |

**Fault / coercion `extensions` 仍然空**：`activities(limit: "x")`、`track(points: 2.5)`、`{ nope }` → 三個都 `extensions` 缺席。

**安全性核查**：`argument` 個值喺三個 raise site 都係 hard-coded literal（`"limit"` / `"offset"` / `"points"`），唔係由 client 嘅 alias 或輸入嚟。我用 aliased document（`a: activities(...)`）實測，出嚟仍然係 schema 嘅 argument name，冇回響 client 輸入。零注入面。

---

## SDL 驗證

用 `graphql-core` 把兩個版本嘅 description **全部剝走**再 `lexicographic_sort_schema` + `print_schema` 對比：

```
STRUCTURALLY IDENTICAL
```

即係 **type / argument / nullability / default 一律冇郁**，只有 5 條 description 改咗（`track` + 4 條 list field），同 diff 數目對得上。

Codegen 出嚟嘅 TS：跑完 `npm run typecheck`（會先 codegen）之後 `git status --porcelain` **完全空白** ⇒ `frontend/src/gql/` 一個字節都冇變。符合預期（description 唔入 TS）。

---

## 其他 lane 冇被撞爛

```
git diff --stat a3bcf36..86ca2ad -- src/common/geo.py tests/test_common_geo.py   → 空
git diff --stat a2a4706..86ca2ad -- .github/                                     → 空
```

三條 CUI-0050/0048/0051 commit 嘅 `--name-only` 只掂 `src/api/*`、`tests/test_api.py`、`frontend/schema.graphql` 同自己張 ticket。geo lane 同 CI lane 嘅檔案喺 merge 之後同佢哋自己嗰個 commit **完全一致**，零污染。而且 573 條入面已經包含 `tests/test_common_geo.py`，全綠。

---

## 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|------|------|------|------|
| 正確性 | 24 | 25 | 行為 19 個 wire 格 + 3 個 budget code + 3 個 coercion 全部實測正確；扣 S-104 |
| 安全性 | 20 | 20 | 無洩漏、無注入面、W-013 守衛 9 條紅證實仍然生效 |
| 可維護性 | 13 | 20 | 扣 W-051（-5）、S-105（-1）、S-106（-1） |
| 測試覆蓋 | 10 | 15 | 三個 api 檔案 100%；扣 W-050（-5，M6 生還） |
| 性能 | 10 | 10 | 無改動；`_stored_counts` 順序同 docstring 一致 |
| 代碼風格 | 9 | 10 | 扣 S-103（-1） |
| **總分** | **86** | **100** | |

**結果：⚠️ warn**（hard gate 全 pass、0 Critical，但 86 < DoD 嘅 90）

---

## 問題清單

### 🔴 Critical

**冇。**

---

### 🟡 W-050 — `extensions` 個 key set 冇 allowlist gate，而 docstring 已經寫咗「and last」

**位置**：`src/api/schema.py:562`（`REFUSAL_ARGUMENT_KEY` docstring 第一句）、`src/api/schema.py:747-749`（`ClientRefusalError.__init__` 起 `published` 個 dict）

**描述**：新 docstring 開頭第一句寫：

> The second and **last** key this schema publishes.

呢個係一句**關於將來**嘅絕對聲明，但冇任何 gate 撐住。我落咗 mutant M6：

```python
published = {REFUSAL_CODE_KEY: code}
published["retryAfter"] = "30"      # ← 第三個 key
```

結果：**573 passed，零紅**，而且個 key 真係去到 wire（M3 同一機制已證實 `published` 直出 `extensions`）。

現存兩條守衛都唔覆蓋呢個情況：

- `test_a_coded_refusal_still_leaks_nothing_it_did_not_leak_before` 係一個**三個名嘅 denylist**（`CONTEXT_KEY_NAMES = ("track_points_remaining", "track_fields_remaining", "list_rows_remaining")`），唔係 allowlist；改個名就過骨
- `test_a_budget_refusal_names_no_argument` 淨係查 `argument` 一個 key 嘅缺席

**影響**：呢個改動本身把不變式由**結構性**降級成**散文**。CUI-0050 之前係

```python
extensions={REFUSAL_CODE_KEY: code}     # literal，「只有 code」由構造保證，唔使 gate
```

而家係一個條件累加嘅 mutable dict ⇒ 不變式變成**可以失去**，但冇加返個 gate 補上。呢個正正就係本 batch 自己（CUI-0051 / S-068 / S-069 / W-017）喺整緊嗰種病：**一句冇 gate 嘅絕對數 / 絕對聲明**。喺同一個 commit 入面一邊修呢種病一邊製造一個新嘅，值得指出。

**方案 A**：加一條 allowlist 測試，跨所有 refusal document 釘死 key set
```python
PUBLISHED_EXTENSION_KEYS = frozenset({"code", "argument"})

@pytest.mark.parametrize("document", [...CODED_BUDGET_DOCUMENTS + BOUNDS_REFUSED_DOCUMENTS...])
def test_a_refusal_publishes_no_extension_key_beyond_the_two(year_client, document):
    body = year_client.post(GRAPHQL_PATH, json={"query": document}).get_json()
    for error in body["errors"]:
        assert set(error["extensions"]) <= PUBLISHED_EXTENSION_KEYS
```
- 優點：直接把 docstring 嗰句「and last」變成有牙嘅 gate；第三個 key 無論改乜名都紅；同現有 `REFUSAL_ARGUMENT_KEY` 喺測試度重新 spell out 嘅做法同一風格（wire contract 唔 import）
- 缺點：將來真係要加第三個 key 時要改兩處（但呢個正正係想要嘅——變成一個決定而唔係一次漂移）

**方案 B**：改 docstring，把「and last」降級成非絕對嘅講法（例如「the second key this schema publishes today; the test of whether a third may join them is below」），唔加 gate
- 優點：零測試改動
- 缺點：等於承認呢句嘢冇人守；而張 CUI-0051 票正正就係喺教訓「數字／聲明要有 gate 綁住（W-017）」。同一個 batch 兩套標準

**推薦：方案 A。** 理由：呢句「and last」係本 batch 自己新寫落去嘅，而唔係承繼返嚟；`ClientRefusalError` 只得一個 `super().__init__` 出口，allowlist 測試寫落去係四行嘢、零維護成本；而且方案 A 順手令 M6 呢類 mutant 以後即刻紅。B 只係把問題寫得誠實啲，冇解決「下一個 key 會靜靜雞落地」。

---

### 🟡 W-051 — `.proj-docs/tickets.md` 嘅 CUI-0048 描述同落地決定相反，而 registry 冇 follow-up note 記低

**位置**：`.proj-docs/tickets.md:104`（CUI-0048 嗰行）、`.proj-docs/tickets.md:40-44`（2026-09-17 後續 note block）

**描述**：registry 嗰行原文：

> **CUI-0048** | 🟢 Low | `service.tracks` 個 negative guard **漏咗** `-0.0`（`-0.0 < 0` 係 `False`，而且 falsy ⇒ **回成條 track**），`2.5` 掟未記錄嘅 `TypeError` | **✅ done** |

「漏咗」+「⇒ 回成條 track」係一句**缺陷描述**，而落地決定係**呢個唔係缺陷**——`-0.0` 當 `0` 係對嘅，行為冇改過。而家配上「✅ done」，任何後來嘅 reader（或者 agent）最自然嘅讀法係「個窿補咗」，**同事實相反**。

同樣 CUI-0051 嗰行仍然寫住「QA 用兩組 document 量到 102/332 同 161/239，兩組都唔等於」，冇記低我同 lane 都重現到 103/341 一個字節不差、**個數從來冇錯**。

**注意呢條唔係叫人改票檔**：CLAUDE.md §7 明文「完成嘅保留紀錄只改狀態」，所以 `CUI-0048.md` / `CUI-0051.md` **只 flip 狀態列係完全合規嘅**，我唔會 flag 佢。問題喺 registry：registry 本身就有 free-form 嘅 `⚠️ 2026-09-17 後續` note block（lane 已經用咗嚟記「刻意冇搬去 completed/」），所以記低推翻**唔會**違反 §7，只係冇做。

**影響**：本 session 已經有十二次下游推翻上游。一張「✅ done」但描述同結論相反嘅 registry 行，係下一個 agent 踩中嘅高機率陷阱——尤其 `CUI-0048.md` 個「建議」段仍然原文留住「Guard 改 `not (points > 0)`」，而我實測嗰個改動**紅 4 條、會打爛 `points=0/None/False`**。

**方案 A**：喺 registry 現有嘅 `⚠️ 2026-09-17 後續` block 加兩句
```
> ⚠️ CUI-0048 嘅結論係「`-0.0` 當 `0`，行為不變」——上表描述入面嘅「漏咗」係開票時嘅
> 讀法，唔係落地結論。張票「建議」段嗰個 `not (points > 0)` 實測會令 `points=0` 開始
> raise（紅 4 條），**唔好照做**。
> ⚠️ CUI-0051：103 / 341 呢兩個數重量過，一個字節不差；缺陷淨係冇講用邊個 document。
```
- 優點：零違反 §7（用返 registry 自己嘅 note 機制）；把兩個高危誤讀一次過封死；下一個 agent 讀 registry 就見到
- 缺點：registry note block 再長少少

**方案 B**：淨係把 registry 嗰兩行嘅**描述欄**改成中性（例如 CUI-0048 改成「`-0.0` / `2.5` 喺 `tracks` 嘅契約未寫明」）
- 優點：改得最少，描述欄本來就係摘要唔係結論
- 缺點：改咗描述欄等於改「保留紀錄」，同 §7 精神有少少擦邊；而且冇處理到「張票建議段嗰個修法有害」呢個更危險嘅資訊

**推薦：方案 A。** 理由：§7 只約束「只改狀態」，note block 係 registry 設計出嚟畀人寫後續嘅地方，用返佢零爭議。而且 A 覆蓋到 B 覆蓋唔到嗰件更要緊嘅事——**張票寫住一個實測會炸嘅修法，而張票標住 ✅ done**。呢個組合對下一個 agent 嘅殺傷力遠高過描述欄用字。

---

### 🟢 S-103 — 四條 list field 嘅 SDL 冇講 `argument` 嘅兩個值，`track` 就有講

**位置**：`src/api/schema.py:1042-1048`（`PAGE_WINDOW_NOTE`）vs `src/api/schema.py:988-993`（`TRACK_DESCRIPTION`）

`track` 寫到明：``` `extensions.argument: "points"` ```（而且 `test_the_sdl_says_track_names_its_argument_too` 連個值都 assert 埋）。四條 list field 淨係寫「`extensions.argument` naming which of the two was refused」，冇講個值係 `"limit"` / `"offset"`，而對應嘅四條 SDL 測試亦都只 assert `"argument"` 呢個字串喺唔喺 description 入面，**冇 assert 兩個值**。

緩解因素：呢兩個值就係同一條 field signature 上面睇得見嘅 argument name（`limit: Int! = 500, offset: Int! = 0`），所以 client 唔算摸黑。所以只係 🟢。

**方案 A**：`PAGE_WINDOW_NOTE` 補一句 ``` （`"limit"` 或 `"offset"`） ```，順手把四條 SDL 測試由 assert key 升級成 assert 埋兩個值
**方案 B**：維持現狀，喺 `REFUSAL_ARGUMENT_KEY` docstring 講明「值一律係 SDL 上見到嗰個 argument name，所以唔另外列」
**推薦：A**——成本一句字，而且令四條 SDL 測試由「呢個字出現過」升級成「呢個契約寫全咗」，同 `track` 嗰條對齊。B 都企得住，但 `track` 已經列咗值，兩邊標準唔一樣先係真正嘅刺。

---

### 🟢 S-104 — 兩個 argument 同時越界時只 name `limit`，SDL 嘅單數措辭讀落似「只可能有一個錯」

**位置**：`src/api/schema.py:1102-1115`（`_page`）、SDL 四條 list field description

實測：

```
{ activities(limit: -5, offset: -1) { id } }   → argument="limit"（只有一個 error）
{ activities(limit: 1001, offset: -7) { id } } → argument="limit"
```

`_page` 先查 `limit` 後查 `offset`，fail-fast。SDL 寫「naming **which of the two** was refused」讀落似「兩者之中壞咗嗰個」，冇提示可能要兩個 round trip。

呢個**唔係 regression**：CUI-0050 之前 message 一樣只講 `limit`，行為完全冇變，只係而家個 fail-fast 變咗 machine-readable 但冇寫低佢係 fail-fast。亦冇測試釘住呢個先後次序。

**方案 A**：SDL 加半句（「the first of the two that is out of range」）+ 加一條測試釘死 `limit` 行先
**方案 B**：完全唔郁——fail-fast 係 GraphQL argument validation 嘅通行做法
**推薦：A**，但優先級最低。理由：加條測試釘住次序嘅價值唔喺文件，喺將來有人把兩個 check 調轉時（例如為咗先查平嗰個）會有嘢紅。而家調轉係靜靜雞過嘅。

---

### 🟢 S-105 — CUI-0038 註釋講「reproduces the figures it quoted exactly」，但嗰啲數已經喺 repo 入面完全唔存在

**位置**：`tests/test_api.py:1830-1832`

```
# Re-measured here on the two documents named above, it reproduces the
# figures it quoted exactly, so they were never wrong; ...
```

方案 B 已經把 103 / 341 剷走晒，所以「the figures it quoted」而家指住一啲 repo 入面已經冇嘅嘢。呢句係想保住「QA 話錯，其實冇錯」呢個歷史裁決（有價值），但寫成一個**自我指涉而個指涉物已經刪咗**嘅句子，下一個 reader 冇辦法 check。

**方案 A**：改成講清楚係邊度嘅數——「reproduces the figures the earlier revision of this comment quoted (see CUI-0051 for what they were), so they were never wrong」，把可查嘅落點指去張票
**方案 B**：整句剷走，只留「what was missing was the sentence that let anyone check」
**推薦：A**。理由：「QA 重現唔到」呢個結論本身值得留低（唔留嘅話下次有人會再開同一張票），但要留就要留一條可查嘅路。B 剷得太多，把一個有用嘅歷史裁決一齊掉咗。

---

### 🟢 S-106 — `tracks` 個 docstring 為一個 `int | None` 參數寫低 `float` 輸入嘅契約，冇講呢啲係 annotation 以外

**位置**：`src/api/service.py:341-343`（signature）、`374-377` / `385-398`（docstring）

`def tracks(session, activity_ids, points: int | None = None)`，但 docstring 而家花咗相當篇幅講 `-0.0` 同 `2.5` ——兩個都係 `float`，即係**已經違反咗 annotation**。內容本身全部正確（我核過），只係冇一句講「呢啲係 annotation 以外但 runtime 到得嘅值」，讀落似係喺放寬接受嘅型別。

**方案 A**：`Args:` 嗰句補半句（「（`points` 標註係 `int | None`；以下講嘅 `float` 行為係 annotation 以外、runtime 到得嘅值嘅契約）」）
**方案 B**：維持現狀——Python annotation 唔 enforce，一個 public module-level function 記低真實 runtime 行為本身就係啱嘅
**推薦：A**，但好輕。理由：呢個 function 嘅 docstring 質素本身極高（成段講清楚點解唔 convert `TypeError`），加半句令佢連「點解喺度講 float」都自我解釋，同成篇風格一致。B 都完全可以接受，呢條純粹係 polish。

---

## ✅ 做得好嘅地方

1. **推翻上游前逐個實測，而唔係推理。** 三條推翻我全部重做，三條全中，數字逐個對上（紅 4 / 紅 14 / 103 / 341）。喺一個已經有十二次下游推翻上游嘅 session 度，呢種「唔信就量」嘅紀律係最值錢嘅嘢。

2. **M2 揀得極準。** 「拆兩個 code」呢個 mutant 唔止驗新 feature，佢**同時**驗返 CUI-0018 (a) 舊條線仲喺唔喺度——紅 14 條入面 6 條係舊測試。一個 mutant 驗兩代決定，選得好。

3. **`BoundsError.__init__` 把 `argument` 由 optional 收成 required。** 基類留 `None` default（budget refusal 真係冇嘢 name），子類收窄成必填。M4 實測紅 3 條證實咗呢個設計買到嘢：一個 convenience default 會靜靜雞令 budget refusal 講一個從來唔係問題嘅 argument。四行代碼買一個「將來第四個 raise site 唔可以靜靜雞漏」嘅保證，抵。

4. **測試檔案重新 spell out `REFUSAL_ARGUMENT_KEY` 而唔係 import。** 而且 docstring 講明點解（「a test that followed a rename would stay green while every deployed client stopped finding the key」），再加 SDL 測試從第二條路徑（field description）到達同一個字串——**改名要錯兩次先會冇人察覺**。M5 我實測咗呢條路：改 SDL 描述即刻紅 4 條 + `run365-schema --check` exit 1。

5. **`test_the_two_bounded_arguments_of_a_list_field_are_told_apart` 同時 assert「相同」同「唔同」。** Assert `path` / `locations` / `code` 三樣一樣，先至 assert `argument` 唔一樣。註釋亦講明點解：如果將來 `locations` 真係開始指住 argument，呢個 key 嘅存在理由就蒸發咗而冇人察覺；如果有人改用第二個 code，個「唔同」會照樣過而 CUI-0018 (a) 靜靜雞爛咗。呢個係少見嘅好測試設計。

6. **CUI-0048 新加嗰句對照 assertion 有真殺傷力。** `assert rows == service.tracks(..., 0)` 而唔係淨係比長度——M1 實測 `[None]` 嗰個 param 就係被呢句殺死嘅。註釋亦解釋咗點解（「a cap that returned the first MAX_TRACK_POINTS rows for one value and an even sample for the other would pass a length check」）。

7. **`test_an_empty_batch_is_still_bounds_checked` 有對照組。** 先 assert `tracks(s, [], -1)` raise，再 assert `tracks(s, [], 1) == {}`，所以第一句紅嘅時候你知係 guard 開火而唔係個 call 自己為咗第二個原因炸咗。

8. **W-013 條線用 raw response text 而唔係 parsed body。** M3 紅 9 條全部係呢條守衛，而佢讀 `get_data(as_text=True)`，所以一個 nested 落 sub-object 嘅 key 一樣捉到。呢個決定喺註釋度有明文理由。

9. **三條 lane 零互撞。** geo lane 同 CI lane 嘅檔案喺 merge 之後同佢哋自己嗰個 commit byte-identical。

10. **SDL 改動乾淨。** 剝走 description 之後 schema 結構完全一致，codegen 出嚟嘅 TS 零變化。冇踩返 CLAUDE.md §6 嗰個「nullability widening `tsc` 捉唔到」嘅陷阱——因為根本冇郁過 type。

---

## 修正優先順序

| 次序 | Item | 嚴重性 | 落點 | 預計成本 |
|---|---|---|---|---|
| 1 | W-050 | 🟡 | `tests/test_api.py` 加一條 allowlist 測試 | ~6 行 |
| 2 | W-051 | 🟡 | `.proj-docs/tickets.md` note block 加兩句 | ~4 行 |
| 3 | S-103 | 🟢 | `src/api/schema.py` `PAGE_WINDOW_NOTE` + 4 條 SDL 測試 | ~3 行 |
| 4 | S-105 | 🟢 | `tests/test_api.py` 改一句註釋 | ~2 行 |
| 5 | S-104 | 🟢 | SDL 半句 + 一條次序測試 | ~8 行 |
| 6 | S-106 | 🟢 | `src/api/service.py` docstring 半句 | ~1 行 |

> ⚠️ 提醒 developer：`skills/sw-ticket-management` 規定**一個 review item 一個 commit**，格式 `fix: W-050 | 一句描述`，唔可以合併。W-050 同 S-103 都掂 `tests/test_api.py` 但要分兩個 commit。
> ⚠️ S-103 改 `PAGE_WINDOW_NOTE` 之後**一定要跑 `run365-schema > frontend/schema.graphql`**，否則 `--check` 會紅（M5 已實證）。

---

## 修訂後代碼（W-050，最高優先）

```python
# tests/test_api.py —— 接喺 CONTEXT_KEY_NAMES 同 REFUSAL_ARGUMENT_KEY 之後

PUBLISHED_EXTENSION_KEYS = frozenset({"code", REFUSAL_ARGUMENT_KEY})
"""Every key a refusal's ``extensions`` may carry, as a client reads them.

An allowlist rather than a denylist, which is the difference CUI-0050 made
necessary. Before it, ``extensions={REFUSAL_CODE_KEY: code}`` was a literal, so
"nothing but the code" held by construction and needed no gate. It is now a
dict built up across two statements, so the invariant became one that can be
lost -- measured: a third key added to it leaves all 573 tests green and still
reaches the wire. ``CONTEXT_KEY_NAMES`` does not catch it, being three specific
counter names; a key under any other spelling walks straight past.

Spelled out rather than imported, for the reason the codes and
:data:`REFUSAL_ARGUMENT_KEY` are: this is the wire contract, so a test that
followed a rename would stay green while every deployed client broke.
"""


@pytest.mark.parametrize(
    "document",
    [p.values[0] for p in CODED_BUDGET_DOCUMENTS] + [p.values[0] for p in BOUNDS_REFUSED_DOCUMENTS],
)
def test_a_refusal_publishes_no_extension_key_beyond_the_two(year_client, document):
    # The gate under REFUSAL_ARGUMENT_KEY's own first sentence, which calls
    # `argument` "the second and last key this schema publishes" -- a claim
    # about the future that nothing held until this test. Held over the same
    # list the codes and the arguments are held over, so a raise site added
    # later is measured by all three lines at once.
    #
    # Read off the parsed body rather than the raw text on purpose: this is the
    # complement of `test_a_coded_refusal_still_leaks_nothing_it_did_not_leak_before`,
    # which reads raw text to catch a key *anywhere*. That one bans three names;
    # this one admits two. A new key needs a decision in both places to land.
    body = year_client.post(GRAPHQL_PATH, json={"query": document}).get_json()

    for error in body["errors"]:
        extra = set(error["extensions"]) - PUBLISHED_EXTENSION_KEYS
        assert not extra, f"the refusal published an extension key nothing decided on: {extra}"
```

實測預期：現狀綠；把 M6 嗰個 `published["retryAfter"] = "30"` 加返落 `ClientRefusalError.__init__` → 呢條測試會喺 9 個 param 上全紅。

---

## 附：我做嘅臨時 mutation 同還原核實

6 個 mutant 全部還原，md5 逐個對返 baseline：

| 檔案 | Baseline md5 | 還原後 md5 |
|---|---|---|
| `src/api/schema.py` | `8001a6169bc17825cf1e4a336474de7a` | ✅ 相同 |
| `src/api/service.py` | `1ae990ecdd77ccaca336ec6732cc189f` | ✅ 相同 |
| `tests/test_api.py` | `cbbb8412fbc257a76118044e4ff96661` | ✅ 相同（從未改過） |
| `frontend/schema.graphql` | `892b03b6ad6f1ae4d1f6517718e38023` | ✅ 相同（從未改過） |

`git status --porcelain` **完全空白**。兩個臨時 probe script（`_rv_probe.py`、`_rv_bytes.py`、`_rv_both.py`）已刪除——⚠️ 呢三個檔案係寫喺 worktree 根目錄嘅，因為 scratchpad 被 `project-boundary` hook 擋咗、heredoc 又被 worktree isolation guard 判為 too complex。三個都已 `rm`，`git status` 空白證實冇殘留。

還原後 pristine tree 上最後確認：**573 passed**、ruff clean、format clean、SDL up to date。

零 git 操作（冇 commit / merge / branch delete / push）。

---

```handoff-receipt
protocol: 1
status: warn
score: 86/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "96%"
next_action: invoke_developer
next_agent: backend-developer
branch: "claude/ai-dev-team-start-05jie2"
context: "CUI-0050/0048/0051 batch @ 86ca2ad: hard gates 全綠 (573 pytest / 144 vitest / api 三個檔案 100% / TOTAL 96% / npm audit 0 / pip-audit 0 / 兩個 build mode)，0 Critical。Lane 五條推翻我逐條重量，五條全部成立 (紅4 / 紅14 / 紅9 / 紅3 / 103+341 一字節不差)。新 wire 契約 19 格 + 3 個 budget code + 3 個 coercion 全部實測正確，SDL 結構零改動，codegen TS 零改動，另外兩條 lane 零污染。86 分因為 2 個 Warning：W-050 是我自己落的第 6 個 mutant 生還 —— extensions 加第三個 key 573 全綠仍然出到 wire，而新 docstring 已經寫咗 argument 係 'the second and last key'；W-051 是 registry 嘅 CUI-0048 描述同落地決定相反。"
blockers:
  - "W-050 | extensions key set 冇 allowlist gate。實測 mutant：ClientRefusalError.__init__ 加 published['retryAfter']='30' → 573 passed 零紅，個 key 真係去到 wire。現有守衛唔覆蓋：test_a_coded_refusal_still_leaks_nothing_it_did_not_leak_before 係 CONTEXT_KEY_NAMES 三個名嘅 denylist，改個名就過骨。CUI-0050 之前 extensions={REFUSAL_CODE_KEY: code} 係 literal，『只有 code』由構造保證；而家係條件累加嘅 dict，不變式變成可以失去但冇加返 gate。而 src/api/schema.py:562 已經寫咗『The second and last key this schema publishes』。修法見報告方案 A：tests/test_api.py 加 PUBLISHED_EXTENSION_KEYS = frozenset({'code','argument'}) + 一條跨 CODED_BUDGET_DOCUMENTS 同 BOUNDS_REFUSED_DOCUMENTS 嘅 allowlist 測試（報告附完整代碼，約 6 行）。"
  - "W-051 | .proj-docs/tickets.md:104 CUI-0048 嗰行描述仍然寫『guard 漏咗 -0.0 ⇒ 回成條 track』並標 ✅ done，但落地決定係『-0.0 當 0，行為不變、唔係缺陷』—— 配上 done 會被讀成『個窿補咗』，同事實相反。CUI-0051 嗰行亦仍然寫『兩組都唔等於』，冇記低 103/341 重現到一個字節不差。⚠️ 唔好改票檔本身（CLAUDE.md §7『完成嘅保留紀錄只改狀態』，lane 只 flip 狀態係合規嘅）；改 registry 現有嘅『⚠️ 2026-09-17 後續』note block 就得。特別要記低：CUI-0048.md 個『建議』段原文仍然留住『Guard 改 not (points > 0)』，我實測嗰個改動紅 4 條、會令 points=0/None/False 全部開始 raise，下一個 agent 照做會炸。"
  - "S-103 | 四條 list field 嘅 SDL 只講有 extensions.argument，冇講個值係 'limit'/'offset'；track 就寫到明 extensions.argument: \"points\" 而且測試連值都 assert。四條 SDL 測試只 assert 'argument' 呢個字串存在。⚠️ 改 PAGE_WINDOW_NOTE 之後一定要跑 run365-schema > frontend/schema.graphql，否則 --check 紅（已實證）。"
  - "S-104 | limit 同 offset 同時越界時只 name limit（_page fail-fast，先查 limit）。SDL 寫『naming which of the two was refused』讀落似只可能一個錯。唔係 regression（改動前 message 一樣只講 limit），但冇測試釘住次序，將來有人調轉兩個 check 會靜靜雞過。"
  - "S-105 | tests/test_api.py:1830 寫『it reproduces the figures it quoted exactly』，但方案 B 已經把 103/341 剷走晒，呢句指住一啲 repo 入面唔存在嘅數。建議改成指去 CUI-0051 張票（嗰度有原數），保住『QA 話錯其實冇錯』呢個裁決同時留返一條可查嘅路。"
  - "S-106 | src/api/service.py tracks() 標註係 points: int | None，但 docstring 而家大篇幅講 -0.0 同 2.5（兩個都係 float，已經喺 annotation 以外）。內容全部正確，只係冇一句講明呢啲係 annotation 以外但 runtime 到得嘅值。補半句就得。"
```