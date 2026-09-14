# Code Review（第三輪，收窄範圍驗證）— AU-047 cleanup

**日期**：2026-09-14
**審閱者**：Code Reviewer（獨立 subagent）
**範圍**：`git diff 2cca183..3ce2bd5`（`9cd1125`、`5ff91d3`、`3ce2bd5`）—— `src/api/schema.py` docstring、`tests/test_api.py`
**前兩輪**：[round 1](2026-09-14_review_au-047.md)（66 fail）、[round 2](2026-09-14_review_au-047_round2.md)（91 pass）

## 結論

| 項目 | 值 |
|---|---|
| 評分 | **87 / 100** |
| 結果 | ⚠️ **warn** |
| 🔴 Critical | 0 |
| 🟡 Warning | 2（W-016、W-017） |
| 🟢 Suggestion | 3（S-024 ~ S-026） |
| next_action | `invoke_developer` |

> 建議先修 W-016 + W-017 再開 PR。**理由唔係質量問題** —— hard gates 四項全綠、303 passed、零 Critical、零行為改動、24/25 個數字命中、mutation 測試證明新測試真係有牙。單看代碼呢三個 commit 係淨改善。但 AU-047 剩低嘅唯一風險就係 claim discipline，而呢三個 commit 嘅唯一目的就係修 over-claim —— 帶住一個同科嘅新 over-claim 入 develop，會令第 5 次變第 6 次。

## Hard gates

```
pytest -q                         → 303 passed in 9.56s
ruff check src tests              → All checks passed!
ruff format --check src tests     → 58 files already formatted
run365-schema --check             → frontend/schema.graphql is up to date
```

## 一、數字核實表（24 / 25 命中）

量度環境：自建 Flask test client + `before_cursor_execute`；token 數用「令 `parse(doc, max_tokens=k)` 剛好成功嘅最小 k」。**兩種 DB 都量**：(A) `year_db` fixture（只有 `r0` 有 600 points，共 600 track rows）、(B) 真 365 × 600 export（219,000 track rows）。

| # | Claim | Developer 報 | 實測 (A/B) | |
|---|---|---|---|---|
| 1-3 | `52 × activity(id:"rN"){track(1)}` tokens / SQL / 結果 | 990 / 208 / 服務 | 990 / 208 / SERVED 0 errors | ✅ |
| 4 | `52 × activities(limit:1){track(1)}` | 990 / 208 | 990 / 208 SERVED | ✅ |
| 5-6 | 同上 × 53 | 1009 / 0 / parse 拒 | 1009 / 0 / `more than 1000 tokens` | ✅ |
| 7-9 | `activities(limit:64 或 65){track(1)}` | 19 / 130 | 19 / 130；第 65 個被 field cap 拒 | ✅ |
| **10** | **「64 aliased `track` fields → 1218 tokens，parser 拒收」** | 1218 / 拒 | **714 tokens，SERVED，130 SQL** | ❌ |
| 11-13 | `166 × activities { id }` | 998 / 332 / 1.8–2.3 s | 998 / 332 / 1.70–2.19 s (n=12) | ✅ |
| 14 | 167 個 → 拒 | — | 1004 tokens 拒 → 166 確係上限 | ✅ |
| 15-16 | 52-parent 時間 / headroom | 0.11–0.20 s / ~75× | 0.106–0.182 s / 15÷0.20 = 75 | ✅ |
| 17-18 | `10000 // 150` = 66；128 上限「however the fields are spread」 | 66 / 128 | 66；三種 shape 全部 130 = 2 + 128 | ✅ |
| 19-20 | S-020 sticky 對照 | — | 19×500 剩 500 → 1000 拒 → 500 服務 500 rows；field 飽和後兩個都拒 | ✅ |
| 21-22 | S-019 mutant 行為 | — | plain dict serve 10 rows 0 error；frozen ctx 逐字命中 mappingproxy TypeError | ✅ |
| 23 | 零行為改動 | — | **AST 剝走 docstring 後完全相同**（機器證明，唔係讀 diff） | ✅ |
| 24 | 測試數 | 303 | 303 passed | ✅ |

## 二、六條 finding 判定

| Item | 判定 | 摘要 |
|---|---|---|
| CUI-0017 | ✅ closed | docstring 而家明講「What this bounds is `track` statements, **and only those**」，並自寫 request 層面 208、無 track 時 332。128 track 上限喺三種 shape 都實測成立 |
| W-015 | ✅ closed | 改名 `ALIAS_FLOOD_MAX_SQL`，**算式一個字冇改**；新 docstring 明講「Deliberately *not* named as a per-request ceiling, because it is not one」。順帶重驗 flood 515 tokens、9855、10220 三個數全中 |
| S-019 | ✅ closed | 上一輪報嘅 frozen-context 存活問題已修；`.get()` mutant 兩條都捉到。Developer 主動申報「第一版註解寫錯、實測推翻、commit 前改正」核實屬實，改正後版本逐字準確 |
| S-020 | ✅ closed | sticky / 唔 sticky 對照逐項對得上 |
| S-021 | ✅ closed | `2^6 = 64 ≤ 66 < 128`；用字有 hedge（「**about** where … **rounded down**」），唔係 derivation 但冇 over-claim |
| S-022 | ❌ **still open** | 見 W-016 |

## 三、W-016 —— 第 5 次 over-claim

**位置**：`tests/test_api.py:613-620`（`_page_field` docstring）、連帶 `src/api/schema.py:97`

三句斷言喺字面讀法下全部被推翻：

| Document | tokens | SQL | 結果 |
|---|---|---|---|
| `{ activity(id:"r0") { t0: track(points:1){sec} … t63: … } }` | **714** | 130 | **完全服務** |
| 同上 65 個 | 725 | 130 | 被 **field cap** 拒（唔係 parser） |
| 同上 90 個 | **1000**（啱啱好貼上限） | 130 | 被 field cap 拒 |
| `{ activities(limit:1) { t0…t63 } }` | 714 | 130 | 完全服務 |

`1218` 只喺另一個讀法成立 —— 「64 個 aliased **parent**，每個帶一個 track」（52 → 990、64 → 1218，已驗）。但句子字面寫嘅係 "64 aliased **`track` fields**"。

回答兩條關鍵問題：

- **field cap 今日係咪只可以經 page window 觸發？** —— **唔係。** 單一 parent 加 65 個 aliased `track` 就觸發到，只需 725 tokens。Alias 路線每個 track 約 **11** tokens，比 parent 路線嘅 19 tokens **更平**，token 預算塞到 ~90 個。
- **cap 嘅實際保護面係咪比 docstring 講嘅窄？** —— **相反，闊咗。** Cap 真係擋到 alias fan-out，fails safe。出事嘅係**描述**。

**連帶後果**：因為註解斷定 alias 路線唔存在，全部新測試（包括兩條 fragment 測試）都只行 `_one_page` 一條路，`activity(id:)` + 64 aliased track 呢條**真實可達路徑零測試覆蓋** —— 而嗰條路徑正正就係 C-001 原本嘅攻擊面。

## 四、W-017 —— 八個新量度數字零 assert（重複五次嘅結構成因）

`130 / 208 / 990 / 1009 / 1218 / 332 / 166 / 0.11–0.20 s` 全部**只以 docstring 散文存在**，`grep` 確認零 assert。改動 `MAX_QUERY_TOKENS`、`SQL_PER_TRACK_FIELD` 或 warnings query 之後會靜靜咁腐爛，冇人會紅。

**呢個就係 AU-047 四輪重複同一個錯嘅結構成因** —— 前面每次都係「散文寫咗一個冇 gate 嘅數」。

建議：加 `test_the_documented_worst_cases_still_measure_as_documented`，用現有 `sql_count` fixture assert `(tokens, sql)` 三組：`(19, 130)`、`(990, 208)`、`(998, 332)`。令散文變成可執行契約。

## 五、🟢 Suggestion

| ID | 內容 |
|---|---|
| **S-024** | `src/api/schema.py:97`「Saturating this cap takes one list field.」讀落似必要條件，同 `_page_field` 嗰句「Only a page window」合讀更誤導。應明寫 one list field 係 *shortest* way |
| **S-025** | `src/api/schema.py:225-227`「charges it below 0 too」唔準：拒收唔會寫回，儲存值永遠停喺 0，只有 local `fields_left` 計到 −1。行為敘述（sticky）啱，儲存值敘述錯 |
| **S-026** | `src/api/schema.py:93`「Measured against a 365-activity, 600-point export」同 `year_db` fixture 唔符（fixture 只有 `r0` 有 track，共 600 rows，唔係 219,000）。兩種 DB 都量過，**冇一個數字係錯**，但描述令人以為量度基底係完整 export |

## 六、✅ 做得好嘅地方

- **零行為改動係機器證明嘅**：`src/api/schema.py` 剝走全部 docstring / bare-string literal 之後 **AST 完全相同**，唔係靠讀 diff 判斷。
- **三個 commit 各自獨立綠**，各自只掂一個檔案（61 / 61 / 66 passed）。
- **E5 三個 mutant 全部如報**：`m1`（先寫 points 再 check）、`m2`（兩個 counter 都早寫）、`m3`（`points==1` 跳過 points check）—— 舊測試 61 passed **全部存活**，新測試逐個捉到，而且捉住嘅係**對嘅嗰條測試**。
- **S-019 嘅自我糾錯值得記低**：developer 主動申報自己第一版註解寫錯、實測推翻、commit 前改正。獨立重驗，改正後版本逐字準確。**呢個正正係 AU-047 前四輪缺失嘅動作。**
- `gql_partial` helper 補得啱：`gql` / `gql_errors` 各自 assert 走一半，半服務半拒收嘅 document 本來測唔到。
- **`ALIAS_FLOOD_MAX_SQL` 算式冇被偷改去遷就數字** —— 改嘅係個名同佢聲稱嘅範圍。呢個係正確嘅修法。
