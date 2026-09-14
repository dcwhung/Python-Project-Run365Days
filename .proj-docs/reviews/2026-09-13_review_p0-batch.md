# Batch Review — 2026-09-13 — P0 batch (AU-001 / AU-002 / AU-003 / AU-004)

| | |
|---|---|
| Reviewer | code-reviewer (independent — did not author any of this code) |
| Branch | `claude/stoic-ritchie-rkynjn` |
| Baseline | `1896778` |
| Commits | `bf7913f` (AU-004), `7218a84` (AU-003), `ea36622` (AU-002), `991c19b` (AU-001) |
| Fixes in batch | 4 (cap is 5 — within limit) |

---

## 整體 verdict

### Hard gates

| Gate | 指令 | 結果 |
|---|---|---|
| Lint | `ruff check src tests api` | ✅ pass — All checks passed! |
| Format | `ruff format --check src tests api` | ✅ pass — 56 files already formatted |
| Type check | — | n/a — 本 repo 冇配置 Python type checker（ruff only） |
| Tests | `pytest tests -q` | ✅ pass — 133 passed in 3.94s |
| Coverage | `pytest --cov=src` | ✅ pass — TOTAL 80% (≥ 80% 門檻，剛好踩線) |
| No Critical | 本次 review | ✅ pass — 0 個 🔴 Critical |
| Security scan | 無新增第三方依賴 | ✅ n/a — 呢批冇加任何 dependency |

Hard gates 全部通過。我獨立重跑咗 lint / format / pytest / coverage，數字同 main agent 報告一致，冇質疑。

### 個別評分

| Section | Ticket | 分數 | 狀態 |
|---|---|---|---|
| A | AU-004 CI 觸發 branch | 88 / 100 | ⚠️ warn |
| B | AU-003 `parse_datetime` timezone | 93 / 100 | ✅ pass |
| C | AU-002 Parser 靜默失敗鏈 | 75 / 100 | ⚠️ warn |
| D | AU-001 GraphQL query cost | 81 / 100 | ⚠️ warn |
| **整體** | | **84 / 100** | **⚠️ warn** |

**Status：⚠️ warn** — 0 個 🔴 Critical，hard gates 全綠，但 10 個 🟡 Warning，其中 4 個（W-004、W-005、W-006、W-008）係「綠燈之下嘅真問題」。整體低於 90 分 DoD 門檻。

### 呢批做得最好嘅一件事

四個 commit 嘅 message 都準確描述咗**行為改變**同**未做嘅嘢**，包括主動申報 scope 限制（AU-003 嘅 epoch-ms 語義）同 out-of-repo 設定（AU-004 嘅 Vercel / github-pages）。呢個係少見嘅誠實度，令 review 可以聚焦真問題而唔使先拆穿包裝。以下 10 個 Warning 全部係我另外驗出嚟，唔係佢哋隱瞞嘅嘢。

### 我用真實數據做嘅獨立驗證

唔重複 main agent 跑過嘅嘢，以下係我自己補跑、並且**推翻或補強咗開發者自報結論**嘅驗證：

| 驗證 | 方法 | 結果 |
|---|---|---|
| Parser 對真實 export 嘅覆蓋 | 逐個 parse `data/raw/garmin/{tcx,gpx,kml}` 共 1095 檔 | tcx 365/365、gpx 365/365、**kml 357/365 — 8 個真實檔案被 DEBUG 級別靜默丟棄**（見 W-004） |
| 嗰 8 個檔案嘅成因 | ElementTree 逐檔拆 | 全部係 `Track Points` folder 有 0 個 placemark（室內跑），**唔係** wrong year |
| KML lap table 真實 schema | 掃 365 檔 2592 個 lap row | `Time` / `Distance` 100% 存在；placemark 首字 histogram = Lap 2592 / Start 357 / End 357 |
| Fixture 對真實 schema 嘅忠實度 | 真實 KML vs `activity_3001.kml` | 結構吻合（escaped HTML lap table、`null,null` lap 座標、`lon,lat, ele` 用 `", "` 分隔、track point placemark 無 `<name>`）；**唯獨缺 Start / End placemark** |
| Guarded helper contract | 直接餵 `12.7 / nan / inf / abc / "" / "  "` | **`optional_int("nan")` raise ValueError、`optional_int("inf")` raise OverflowError** — 違反自身 docstring（見 W-005） |
| Stride 取樣行為 | 13 組 (total, points) 組合實跑，同舊 python `downsample` 逐一對比 | 數量**永遠準確等於 points，冇少過**；但**間距嚴重不均**（見 W-010） |
| GraphiQL 關咗之後 introspection | `create_app(graphiql=False)` + POST `{ __schema { queryType { name } } }` | **回 200 + 完整 schema** — docstring 宣稱嘅 introspection 關閉並不存在（見 W-008） |
| Depth / token limiter 對 introspection | `get_introspection_query()` 打真 schema | 163 tokens、通過 depth limiter，**開發者報嘅數字完全準確**，本地開 GraphiQL 唔會撞牆 |
| 前端有冇送 `points: 0` | `ActivityView.tsx` / `api/source.ts` / generated `TrackQuery` | **冇** — 全部係 600，且 `points: Int!` 必傳（開發者聲稱屬實） |
| 前端會唔會撞 `MAX_TRACK_POINTS` | `TRACK_POINTS = 600` vs cap 1000 | 唔會，安全 |

---

## Section A — AU-004（`bf7913f`）CI 觸發 branch 由 `develop` 改 `master`

改動：`pages.yml` / `tag-release.yml` 觸發 branch、README badge + 三段文字、`docs/deployment.md` 四段文字。

### 評分

| 維度 | 得分 | 滿分 | 備註 |
|---|---|---|---|
| 正確性 | 20 | 25 | W-002 |
| 安全性 | 20 | 20 | — |
| 可維護性 | 13 | 20 | W-001、S-001、S-002 |
| 測試覆蓋 | 15 | 15 | n/a（workflow 改動，無法單元測試）|
| 性能 | 10 | 10 | — |
| 代碼風格 | 10 | 10 | — |
| **總分** | **88** | **100** | ⚠️ warn |

### 🟡 W-001 — `docs/architecture.md:142` 仍然寫住 CI 喺 `develop` 觸發

**位置**：`docs/architecture.md:141-142`

```
- **GitHub Actions** (`.github/workflows/pages.yml`) runs lint and tests on
  every push and pull request to `develop`, then builds and deploys the
```

**Step-by-step**：

1. Commit message 明確聲稱：「Documentation brought in line with the new trigger, **so the docs do not describe a branch layout the workflows no longer use**」。
2. 我 grep 全 repo（排除 `node_modules` / `.git` / worktree 副本）搵 `develop`：改咗 `README.md` 同 `docs/deployment.md`，但 `docs/architecture.md:142` 冇改。
3. 呢句唔係歷史敘述 —— 佢係現在式描述 pipeline 現況，同 `pages.yml` 直接矛盾。
4. 順帶：同段 `docs/architecture.md:138` 寫「(93 tests)」，實際 133（AU-002 / AU-001 加咗 40 個）。呢個係 pre-existing，但同一段落、同一次 pass 應該一齊掃。

**影響**：`architecture.md` 係新人入門文件。一個讀者跟住佢去開 PR 入 `develop`，會得到零 CI —— 呢個正正就係 AU-004 要修嘅嗰個 bug，只不過而家由文件重新製造一次。

**方案 A（推薦）**：改 `docs/architecture.md:142` 嘅 `develop` → `master`，同時順手更新 test count 93 → 133。
- 優點：一行改動，同 AU-004 已做嘅兩份文件保持一致，成本近乎零。
- 缺點：test count 呢類數字會再次過期。

**方案 B**：喺 `architecture.md` 刪走呢個 bullet，改為連結去 `docs/deployment.md`（單一真相來源）。
- 優點：branch layout 只喺一處描述，永久消除 drift。
- 缺點：architecture.md 失去 self-contained 嘅概覽價值；改動面比 A 大。

**推薦 A**，並額外考慮 B 作為 P2 文件重整。

---

### 🟡 W-002 — `concurrency: group: pages` + `cancel-in-progress: true`：PR 嘅 CI run 會取消進行中嘅 production 部署

**位置**：`.github/workflows/pages.yml:15-18`

```yaml
concurrency:
  group: pages
  cancel-in-progress: true
```

**Step-by-step**：

1. `group` 係常數字串 `pages`，冇 `${{ github.ref }}` 之類嘅區分 key，所以**所有 workflow run 共用同一個 concurrency group**：master push、每個 PR 嘅 push、`workflow_dispatch` 全部互相排隊同互相取消。
2. AU-004 之前：deploy 由 `develop` push 觸發，PR 亦係入 `develop`，同樣共用一個 group —— 問題已經存在。
3. AU-004 之後，commit message 自己講咗：「a merge to master is a **production Pages deploy**, not just a CI run」。
4. 兩者相乘出現一個新嘅時間窗：merge PR-1 入 master → master push run 開始 build + deploy → 同一分鐘有人 push 去 PR-2（base = master）→ PR-2 嘅 run 進入 group `pages` → **cancel-in-progress 會殺死進行緊嘅 production deploy**。
5. 被殺嘅 run 顯示為 cancelled（唔係 failed），冇人會收到告警，而 Pages 停留喺舊版本。

**影響**：merge 之後靜默唔部署。AU-004 令「merge = 部署」，所以呢個 pre-existing 缺陷嘅命中率由「偶爾」升到「每次多人同時開 PR」。

**方案 A（推薦，最小改動）**：只喺 PR run 允許互相取消，push 到 master 嘅 deploy 唔准被殺。

```yaml
concurrency:
  group: pages-${{ github.event_name == 'pull_request' && github.ref || 'deploy' }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
```

- 優點：production deploy 永遠唔會被 PR 取消；PR 之間仍然係「新 push 殺舊 push」慳 runner。
- 缺點：expression 略為難讀，需要一行 comment 解釋。

**方案 B**：`group: pages-${{ github.ref }}`，`cancel-in-progress: true` 維持。
- 優點：一行改動，最易讀；每條 ref 各自一條隊。
- 缺點：連續兩次 merge 入 master，第二次仍然會取消第一次嘅 deploy（同一個 ref）。對 Pages 呢個場景其實可接受（後者本來就覆蓋前者），但語義上係「取消緊部署」而非「排隊」。

**推薦 A**；如果團隊想要最簡單，B 亦可接受，但要清楚知道連續 merge 嘅行為。

---

### 🟢 S-001 — README 將 Vercel production branch 寫成既成事實，`docs/deployment.md` 寫成待核對

**位置**：`README.md:260` vs `docs/deployment.md:15-19`

`docs/deployment.md` 嘅措辭係**完全正確**嘅（見下方 ✅ 段落）：「The production branch **should be** `master` … so it cannot be read or changed from this checkout — **confirm it there** after any change」。

但 `README.md:260` 寫：「The production **branch is** `master`.」—— 直述句，讀者會當成已驗證事實。同一個 repo 兩份文件對同一個 out-of-repo 設定有兩種確定性語氣。

**建議**：README 改為「The production branch should be `master` (set in the Vercel dashboard — see [docs/deployment.md](docs/deployment.md))」，令兩處語氣一致。

---

### 🟢 S-002 — `origin/develop` 仍然存在，而且而家係零 gate

**位置**：`git branch -a` → `remotes/origin/develop`

Commit message 講「develop is retired」，但 branch 本身仍然喺 remote。AU-004 之後冇任何 workflow 以 `develop` 為觸發條件，所以而家 push 去 develop = 零 lint、零 test、零 schema check —— 同 AU-004 修之前 master 嘅處境一模一樣，只係換咗個 branch。另外 `legacy/README.md:22`（「will be removed once the `develop` branch reaches feature parity」）亦係一句指向已退役 branch 嘅前瞻語句。

**建議**：喺 `docs/deployment.md` 或 README 明確寫一句「`develop` is retired; delete it on the remote」，並由人手喺 GitHub 刪除（reviewer 同 developer 都冇權限做呢件事，屬於 out-of-repo action，同 Vercel / github-pages 設定同一類）。`legacy/README.md:22` 順手改成 `master`。

---

### ✅ Looks Good（Section A）

- **`docs/deployment.md:15-19` 同 `:76-81` 嘅措辭係典範**。呢個正正係我被要求核實嘅第 7 點，我逐字睇過：兩處都係「描述應有狀態 + 明確指出設定唔喺 repo 入面 + 叫人手去核對」，冇一句將未驗證嘅 dashboard 狀態寫成已完成。`:79-81` 仲進一步解釋咗**失敗模式**（「otherwise the deploy job is rejected at the environment gate even though the workflow itself ran」），令下一個人 debug 時唔使由零開始。**裁決：措辭正確，冇問題。**
- **`README.md:270` 嘅 versioning table 處理得細緻**：`v1.0.0` 由 `master` 改成 `(tag only)` 而唔係直接刪走 —— 保留咗「呢個版本仍然攞得返」嘅資訊，同時避免 `master` 一行喺 table 出現兩次。
- **`tag-release.yml:63` 連 comment 都跟住改**（`usually develop` → `usually master`）。呢種「comment 都唔留 drift」嘅細緻度係 W-001 顯得可惜嘅原因 —— 三份文件改咗兩份半。

---

## Section B — AU-003（`7218a84`）`parse_datetime` 改用 `zoneinfo`

改動：`src/common/time.py`（-1 import、+2 常數、epoch-ms path 重寫）、`tests/test_common_time.py`（+2 test class，9 個新 assert）。

### 評分

| 維度 | 得分 | 滿分 | 備註 |
|---|---|---|---|
| 正確性 | 24 | 25 | S-004 |
| 安全性 | 20 | 20 | — |
| 可維護性 | 14 | 20 | W-003、S-003 |
| 測試覆蓋 | 15 | 15 | 四條 path × offset × wall clock 全覆蓋 |
| 性能 | 10 | 10 | — |
| 代碼風格 | 10 | 10 | magic number 已抽成 `_UNIX_MS_DIGITS` / `_MS_PER_SECOND` |
| **總分** | **93** | **100** | ✅ pass |

### 我被要求核實嘅第 6 點：佢係咪真係只改咗 tzinfo，冇順手改語義？

**裁決：屬實。逐條 path 核對如下。**

| Path | 舊 | 新 | 語義 |
|---|---|---|---|
| ISO + `.` + `Z` | `.replace(tzinfo=pytz.UTC).astimezone(tz)` | `.replace(tzinfo=dt_timezone.utc).astimezone(tz)` | 不變（本來就正確，`astimezone` 唔受 LMT 影響）|
| ISO + `+` offset | `dateutil.parser.parse(rec_time)` | 一模一樣，未改 | 不變 |
| epoch ms | `utcfromtimestamp(x).replace(tzinfo=pytz_tz)` | `fromtimestamp(x, tz=utc).replace(tzinfo=None).replace(tzinfo=ZoneInfo)` | **牆鐘不變**，只換 tzinfo 物件；`round(…, 1)` 嘅怪癖亦原封不動保留 |
| naive local | `.replace(tzinfo=pytz_tz)` | `.replace(tzinfo=ZoneInfo)` | 牆鐘不變 |

`.replace(tzinfo=None)` 嗰步係刻意將 aware UTC 打回 naive 再貼上 HK 標籤 —— 即係 `utcfromtimestamp` 嘅精確等價物，同時避開 3.12 deprecation。**呢個係正確嘅最小改動做法**，冇偷偷加 `astimezone`。

`time.py:40-41` 嘅 comment 亦準確講明「the epoch is read as UTC, then **relabelled (not converted)**」—— 用咗最容易搞錯嗰個字眼去釘死意圖。

### 🟡 W-003 — 釘死錯誤行為嘅測試冇指向任何 follow-up ticket

**位置**：`src/common/time.py:41`、`tests/test_common_time.py:60`

```python
# src/common/time.py:41
# (not converted) to *timezone*. The 8-hour semantic question is tracked separately.

# tests/test_common_time.py:60
"""The offset fix must not move any wall clock -- see AU-003 scope note."""
```

**Step-by-step**：

1. `TestParseDateTimeWallClockUnchanged` 用 parametrize 釘死四條 path 嘅牆鐘，其中 epoch-ms 嗰條釘住嘅係**已知錯誤**嘅語義（UTC 當本地讀，差 8 小時）。
2. 代碼 comment 講「tracked separately」，測試 docstring 講「see AU-003 scope note」。
3. 我搵過 `.proj-docs/`：得 `audits/2026-09-13_22-42_audit_full-codebase.md` 同 `index.md`，冇 ticket 目錄。審計報告嘅 AU-003 段（`:191-202`）由頭到尾只講 LMT offset，**完全冇提 epoch-ms 嘅 8 小時語義問題**，亦冇 AU-0XX 編號承接佢。
4. 即係話：「tracked separately」呢句所指嘅 tracker **並不存在**。

**影響**：一個刻意保留嘅錯誤行為，被一個綠色測試釘死，而冇任何 artefact 記錄佢應該幾時解除。半年後有人真係去修 epoch-ms 語義，會見到 `test_wall_clock_is_preserved` 變紅，然後合理地推斷「呢個係 regression」並且將自己嘅正確修正 revert。呢個係測試由資產變負債嘅典型路徑。

> 補充：呢個 scope 限制係 main agent 落嘅，唔係 developer 嘅錯。W-003 針對嘅純粹係「留低咗一個冇出口嘅 TODO」。

**方案 A（推薦）**：喺 `.proj-docs/` 開一個 AU-0XX ticket（例如 AU-029「`parse_datetime` epoch-ms path 將 UTC 當本地時間讀，差 8 小時」），然後代碼 comment 同測試 docstring 都改成引用該編號：

```python
# 唔係 "tracked separately"，而係：
# The epoch is read as UTC then relabelled (not converted) to *timezone*;
# correcting that 8-hour shift is AU-029.
```

```python
"""Wall clocks are frozen until AU-029 corrects the epoch-ms semantics."""
```

- 優點：測試變成有出口 —— 修 AU-029 嗰個人一睇就知呢個 class 應該連同修改一齊更新，唔會誤判成 regression。成本係開一個 ticket。
- 缺點：要 PM / architect 分配編號（見 `sw-ticket-management`），跨角色協調。

**方案 B**：唔開 ticket，改為喺測試度用 `@pytest.mark.xfail(strict=True)` 標記 epoch-ms 嗰個 case，並喺 reason 寫明係已知錯誤。
- 優點：唔使開 ticket；`strict=True` 令「有人修好咗但冇更新測試」都會被 CI 捉到，語義上更誠實（呢個 case 唔係「期望行為」而係「已知錯誤」）。
- 缺點：其餘三條 path 嘅牆鐘保護會同 epoch-ms 分開兩處寫，parametrize 要拆；而且 xfail 唔會提示「應該點修」。

**推薦 A**（開 ticket + 引用編號），B 可以疊加上去。

---

### 🟢 S-003 — `pytz` 仍然係 runtime dependency，但 package 已經冇用

**位置**：`pyproject.toml:14`

AU-003 刪走咗 `src/common/time.py` 最後一個 `import pytz`。我 grep 全 repo：而家淨低 `legacy/test.py` 用 pytz，而 `pyproject.toml:39-51` 嘅 `packages` 清單**冇包含 `legacy`**，即係 `legacy/` 唔會被安裝。所以 `pytz` 已經係一個純粹嘅死依賴。

**建議**：由 `pyproject.toml` 嘅 `dependencies` 移除 `pytz`。收益係少一個 transitive dependency（同 `zoneinfo` 標準庫功能重疊）、少一個 CVE 面。風險近乎零 —— 唯一使用者係唔會被打包嘅 `legacy/`。順帶留意 `run365days.egg-info/requires.txt` 係 build artefact，重裝就會同步，唔使手改。

---

### 🟢 S-004 — `parse_datetime` 對三種合法 ISO 8601 變體會 raise

**位置**：`src/common/time.py:33-45`

我實跑咗三個 input：

```
'2021-10-17T06:10:56-05:00' -> ValueError: time data ... does not match format '%Y-%m-%d %H:%M:%S'
'2021-10-16T22:10:56Z'      -> ValueError（有 T、有 Z，但冇 '.'，兩個分支都唔中，跌落最尾 strptime）
'2021-10-17T06:10:56'       -> ValueError（naive ISO，有 T 冇 offset）
```

成因係 `if "T" in rec_time:` 之下只有兩個分支（`"." and endswith("Z")`、`"+" in`），兩個都唔中就會 fall through 去尾行嘅 `strptime("%Y-%m-%d %H:%M:%S")`，而該 format 唔接受 `T`。負號 offset 冇被檢查（只 check `"+"`）。

**呢個係 pre-existing，AU-003 冇令佢變差**，而且真實 Garmin 檔案全部用 `.000Z`，所以今日唔會中。但兩點令佢值得寫低：

1. Docstring（`:16-21`）列出「四種支援格式」，讀者好易以為 ISO 一律得 —— 實情係只得列出嗰兩種 ISO 變體。
2. 呢個 ValueError 會直接跌入 `parse_all` 嘅 **DEBUG** 桶（見 W-004），即係一個 `<Id>2021-01-08T04:04:52Z</Id>`（合法 ISO，只係冇毫秒）嘅檔案會靜默消失。呢個係 S-004 同 W-004 嘅交叉點。

**建議**：最小做法係將整個 dispatch 換成 `dateutil.parser.isoparse` / `datetime.fromisoformat` 打頭陣，只保留 epoch-ms 同 naive-local 兩個特例；或者最低限度喺 docstring 寫明「ISO 輸入必須帶毫秒 + `Z`，或者帶 `+HH:MM` offset」。呢個屬於獨立 ticket，唔應該塞入 AU-003。

---

### ✅ Looks Good（Section B）

- **`TestParseDateTimeOffset` 精準命中審計嘅根因**：審計指出「`tests/test_common_time.py:29-33` 只 assert hour/minute/second，**從來冇 assert `utcoffset()`**，所以 89% coverage 之下測試全綠」。新測試就係五條 `assert dt.utcoffset() == timedelta(hours=8)`，逐條 input path 一個。呢個係「補返嗰個被遺漏嘅斷言」而唔係「加多幾個 happy path」，正中要害。
- **`_UNIX_MS_DIGITS = 13` / `_MS_PER_SECOND = 1000`** 消除咗原本 `len(rec_time) == 13` 同 `/ 1000` 兩個 magic number，符合 `sw-coding-style` 嘅 Critical 規則。命名係 UPPER_SNAKE_CASE、module-private，正確。
- **`from datetime import timezone as dt_timezone`** 呢個 alias 係必要嘅 —— function 嘅參數就叫 `timezone`，唔改名會 shadow。開發者留意到咗，而且用 alias 而唔係改參數名（改參數名會係 breaking API change）。呢個係正確嘅取捨。

---

## Section C — AU-002（`ea36622`）Parser 靜默失敗鏈

改動：`base.py` +85（`ActivityParseError` + 4 個 guarded helper + logging）、`tcx.py` / `gpx.py` / `kml.py` 重寫讀取路徑、`export_data.py` +10（zero-record guard）、9 個 golden fixture、`test_activities_parsers.py` +150。

### 評分

| 維度 | 得分 | 滿分 | 備註 |
|---|---|---|---|
| 正確性 | 10 | 25 | W-004、W-005、W-006 |
| 安全性 | 20 | 20 | — |
| 可維護性 | 17 | 20 | S-005、S-006、S-007 |
| 測試覆蓋 | 9 | 15 | W-007、S-008 |
| 性能 | 9 | 10 | S-009 |
| 代碼風格 | 10 | 10 | — |
| **總分** | **75** | **100** | ⚠️ warn |

> 分數偏低唔代表呢個 commit 差 —— 佢係四個入面工作量最大、淨改善最明顯嘅一個。低分反映嘅係：ticket 嘅核心目標（「stop silently dropping activities」）喺真實數據上**仲有 8 個檔案未達成**，而且新引入嘅 helper 有一個未關嘅 crash 洞。

### 🟡 W-004 — `ValueError` 一桶裝兩種語義；真實數據有 8 個檔案因此被 DEBUG 級別靜默丟棄

**位置**：`src/activities/parsers/base.py:124-127`、`src/activities/parsers/kml.py:71-72`

```python
# base.py:124-127
except ValueError as exc:
    # Wrong year / wrong sport is the normal case for most of the
    # export folder, so it stays below the warning threshold.
    logger.debug("Skipping %s: %s", fp.name, exc)
```

```python
# kml.py:70-72
act_time = parse_datetime(track_points[0].time) if track_points else None
if act_time is None or act_time.year < self.current_year:
    raise ValueError(f"Skipping activity: {activity_id}")
```

**Step-by-step**：

1. 新設計用**異常類型**去分級：`ActivityParseError` / `ET.ParseError` → WARNING，`ValueError` → DEBUG。前提係「`ValueError` = 刻意 skip」。
2. 但 `ValueError` 喺 parser 入面有**兩個來源**：
   - 刻意嘅 `raise ValueError("Skipping old activity" / "Not a running activity")`；
   - 任何真實數據錯誤 —— `float(required_text(...))`（`tcx.py:105-106`）、`float(lon_lat[1])`（`kml.py:144`）、`int(tds[0]["colspan"])`（`kml.py:121`）、`hhmmss_to_seconds()`（`kml.py:163`）、以及 `parse_datetime()` 對非預期格式（見 S-004）。
3. 第二類會被當成第一類，用 DEBUG 記錄，喺 default log level 之下**完全睇唔到**。
4. `kml.py:71` 更加將兩種語義寫埋喺同一個 `if` 同同一句訊息：`act_time is None`（一個 track point 都冇 —— 真實數據問題）同 `year < current_year`（刻意過濾）都會 raise `ValueError("Skipping activity: <id>")`，連訊息都分唔到。

**真實數據證據**（我逐檔跑 `data/raw/garmin/kml` 365 個檔案）：

```
### kml: files=365 kept=357
       8 × ValueError: Skipping activity: <id>
```

再拆嗰 8 個（`6055376813 6055377172 6055377549 6055377879 6055378214 6067566088 6067566456 6130263512`）：

```
6055376813 folders=[('Laps', 6), ('Track Points', 0)] usable_points=0
… 八個全部一樣：6 個有效 lap placemark，Track Points folder 得 0 個 placemark
```

即係：**8 個真實檔案（2.2%）帶住 6 個 lap 嘅真實時間同距離數據，因為冇 GPS track point 而被整個丟棄，而 log 只喺 DEBUG 出現，訊息仲讀落似「舊年度活動，正常過濾」。** 呢個正正就係 AU-002 標題所講嘅 “silently dropping activities”，喺修完之後仍然存在，而且**佢自己揀嘅 DEBUG level 就係令佢繼續睇唔見嘅原因**。

**影響評估（要講公道話）**：

- ✅ 唔係 regression —— 修之前係 `contextlib.suppress` 完全冇 log，而家至少 DEBUG 有記錄。
- ✅ 唔影響 production dashboard —— `export_data.main()`（CI / Vercel 行嗰個）只用 TCX + GPX，我核對過嗰 8 個 activity id 喺 `data/raw/garmin/tcx/` 同 `gpx/` 都有對應檔案，而 TCX / GPX 兩邊都係 365/365 全通過。KML 只俾 `run365-activities` 用。
- ❌ 但 ticket 嘅核心目標喺唯一一個真實命中嘅 case 上冇達成。

因為冇 regression、冇錯誤數據上線、唔喺部署路徑，我評 🟡 Warning 而唔係 🔴 Critical。但呢個係本批**最應該先修**嘅一條。

**我被要求裁決嘅 Lane B 偏離（刻意 skip 記 DEBUG 而唔係 WARNING）—— 裁決：**

> **理由接受，實作推翻。** 「正常多年份資料夾會噴幾百行淹冇真訊號」呢個理由完全成立 —— 我實跑確認 365 個 TCX 全部通過，但如果 folder 入面有 2020 同 2022 嘅檔案，WARNING 會變成雜訊機。**但「刻意 skip」唔可以用 `ValueError` 呢個 Python 內建、任何 `float()` / `int()` / `strptime()` 都會 raise 嘅類型去表示。** 分級應該由專屬類型承擔，而唔係由一個共用類型承擔。

**方案 A（推薦）**：加一個 `ActivitySkipped` 例外，刻意 skip 用佢，泛型 `ValueError` 保持 WARNING。

```python
# base.py — 同 ActivityParseError 並列
class ActivitySkipped(Exception):
    """The file parsed fine but does not belong in this run (wrong year, wrong sport)."""

# parse_all
except ActivitySkipped as exc:
    logger.debug("Skipping %s: %s", fp.name, exc)          # 刻意，靜
except ActivityParseError as exc:
    logger.warning("Skipping %s: %s", fp.name, exc)
except ET.ParseError as exc:
    logger.warning("Skipping %s: malformed XML (%s)", fp.name, exc)
except ValueError as exc:
    logger.warning("Skipping %s: unreadable value (%s)", fp.name, exc)  # 真問題，響
```

三個 parser 嘅刻意 `raise ValueError(...)` 改成 `raise ActivitySkipped(...)`；同時拆開 `kml.py:71`：

```python
if not track_points:
    raise ActivityParseError(f"no track points in {file_path.name}")   # ← 嗰 8 個檔案會喺度出 WARNING
if act_time.year < self.current_year:
    raise ActivitySkipped(f"Skipping old activity: {activity_id}")
```

- 優點：完全達成 ticket 目標；DEBUG 嘅好處（唔噴雜訊）原封保留；嗰 8 個真實檔案即刻浮面；日後任何新嘅 `float()` 失敗自動響。改動係 1 個 class + 5 處 `raise` 改名 + 1 個 `if` 拆開。
- 缺點：`ActivitySkipped` 唔係 `ValueError` 子類，所以任何直接 call `parse()` 並且 `except ValueError` 嘅外部代碼（我 grep 過：得測試）要更新。測試入面 `pytest.raises(ValueError, match="old activity")` 要改。

**方案 B**：保留 `ValueError`，但令刻意 skip 用一個 `ValueError` 子類（`class ActivitySkipped(ValueError)`），先 catch 子類記 DEBUG，再 catch `ValueError` 記 WARNING。
- 優點：向後相容 —— 現有 `except ValueError` / `pytest.raises(ValueError)` 全部照舊 work，測試唔使改。
- 缺點：`except` 順序變成語義關鍵（子類必須排喺父類之前），漏咗順序就靜默退化返今日嘅狀態，而且冇任何測試會捉到。呢個係一個好易腐化嘅約束。

**推薦 A**（顯式類型分離，唔靠 except 順序）。如果團隊優先向後相容則 B，但一定要補一個測試釘死「泛型 ValueError → WARNING」。

無論 A 定 B，都**必須另外拆開 `kml.py:71`** 嘅 `act_time is None or year < current_year`，因為呢個 `or` 本身就係兩種語義共用一句訊息，換咗例外類型都仲係分唔開。

---

### 🟡 W-005 — `optional_int` 違反自己嘅 docstring：`"nan"` raise ValueError、`"inf"` raise OverflowError 並衝穿 `parse_all`

**位置**：`src/activities/parsers/base.py:70-73`

```python
def optional_int(parent: ET.Element, path: str, namespaces: dict[str, str]) -> int | None:
    """Return *path*'s text as an int, or ``None`` when absent or unparsable."""
    value = optional_float(parent, path, namespaces)
    return None if value is None else int(value)
```

**Step-by-step**（我實跑嘅結果）：

```
'12.7' -> optional_int 12   | optional_float 12.7
'nan'  -> RAISED ValueError    cannot convert float NaN to integer
'inf'  -> RAISED OverflowError cannot convert float infinity to integer
'abc'  -> optional_int None | optional_float None
''     -> optional_int None | optional_float None
```

1. `optional_float` 用 `try: float(text) except ValueError: return None` 攔截不可解析嘅字串 —— 但 `float("nan")` 同 `float("inf")` **係成功嘅**，佢哋係合法 float 字面量。
2. 所以呢兩個值會原封不動傳去 `int(value)`。
3. `int(float("nan"))` → **ValueError**。喺 `parse_all` 度落入 DEBUG 桶（W-004），整個 activity 靜默消失。
4. `int(float("inf"))` → **OverflowError**。`parse_all` 只 catch `ActivityParseError` / `ET.ParseError` / `ValueError`，**OverflowError 唔喺入面** → 例外一路衝穿 `parse_all` → 衝穿 `export_data.main()` → **整個 export 崩潰**。
5. 而 AU-004 之後，`run365-export` 崩潰 = master 嘅 production Pages 部署失敗。

**影響**：呢個正正就係我被要求審嘅第 1 點所講嘅風險 —— 「收窄 suppress 之後，會唔會有真實檔案由靜默 skip 變成成個 export 爆煲」。我逐個 parser 睇過所有讀取路徑，**冇任何遺留嘅 unguarded `.find(...).text` 鏈**（AttributeError 面已經關乾淨），但 `optional_int` 自己開咗一個新洞。真實 Garmin 唔會出 `inf` cadence，所以係 latent，但呢個係新代碼違反自身合約，修正成本兩行。

**方案 A（推薦）**：喺 `optional_int` 自己 catch。

```python
def optional_int(parent: ET.Element, path: str, namespaces: dict[str, str]) -> int | None:
    """Return *path*'s text as an int, or ``None`` when absent or unparsable."""
    value = optional_float(parent, path, namespaces)
    if value is None or not math.isfinite(value):   # nan / inf are valid floats, not valid ints
        return None
    return int(value)
```

- 優點：合約回復真確；`math.isfinite` 一次過處理 nan / inf / -inf；改動兩行，唔郁 `parse_all`。
- 缺點：`optional_float` 仍然會回 `nan` / `inf`（elevation、speed 等）—— pandas `describe()` 會 skip nan，但 `inf` 會污染 min/max。如果想徹底，同一個 `isfinite` guard 應該落喺 `optional_float`。

**方案 B**：喺 `parse_all` 加一層 `except Exception` 兜底，記 WARNING 並繼續。
- 優點：一勞永逸擋住所有未預見嘅例外類型（OverflowError、KeyError、IndexError、TypeError…），冇一個檔案可以拉冧成個 export。
- 缺點：**同 AU-002 嘅方向相反** —— ticket 就係要收窄 catch 面。寬泛 catch 會將真正嘅 programming error（例如 typo 造成嘅 AttributeError）重新變成一句 WARNING，等於走回頭路。ruff `BLE001` 亦會投訴。

**推薦 A**，並將 `isfinite` guard 同時落喺 `optional_float`（一併解決 `inf` 污染統計）。如果團隊真係想要「一個檔案唔可以拉冧成個 export」呢個保證，應該喺 `export_data.main()` 嘅最外層做，唔好落喺 `parse_all`。

---

### 🟡 W-006 — KML lap 嘅 `Time` / `Distance` 當 optional，TCX 嘅同義欄位當 required：一個 lap 缺數就靜默少計

**位置**：`src/activities/parsers/kml.py:152-168` vs `src/activities/parsers/tcx.py:104-109`

```python
# kml.py:160-167 — 缺就當 0
for row in lap_rows:
    time_text = row.get(_LAP_TIME_KEY)
    if time_text:
        total_sec += hhmmss_to_seconds(time_text)
    distance_text = row.get(_LAP_DISTANCE_KEY)
    if distance_text:
        total_km += float(distance_text.split()[0])
```

```python
# tcx.py:105-106 — 缺就 raise
"Time": float(required_text(lap, "ns:TotalTimeSeconds", _NS)),
"Distance": float(required_text(lap, "ns:DistanceMeters", _NS)),
```

**Step-by-step**：

1. 兩個 parser 都用 lap 總和去砌 `total_sec` 同 `distance_km` —— 呢兩個欄位係 dashboard 嘅核心數字，亦係 `pace_str(total_sec, dist_km)` 嘅唯一輸入。
2. TCX 將佢哋當 mandatory：缺 → `ActivityParseError` → WARNING + 跳過該 activity。**呢個係啱嘅。**
3. KML 將佢哋當 optional：缺 → 該 lap 貢獻 0 → activity 照樣產出，但 `total_sec` / `distance_km` / `pacing` **全部係錯數，冇任何 log**。
4. `_aggregate_laps` 嘅 docstring 仲主動記錄咗呢個選擇：「Laps whose HTML table omits a statistic contribute nothing for it **rather than discarding the whole activity**」—— 即係刻意嘅設計決定，但方向錯咗：對聚合欄位而言，「少計嘅錯數」比「缺失嘅 activity」更難發現、更難修復。
5. 對比舊代碼：舊 `lap_df[col].apply(lambda v: float(v.split()[0]))` 遇到缺失 key 會喺該 cell 攞到 NaN → `float(nan.split())` → AttributeError → 被 suppress → 整個 activity 消失。所以呢個係一個**由「靜默丟失」換成「靜默錯數」嘅行為改變**，而唔係純改善。

**真實數據**：我掃咗全部 365 個 KML、2592 個 lap row，`Time` 同 `Distance` **100% 存在**（`lap rows missing keys: none`）。所以今日唔會中，屬 latent。但呢個路徑冇任何測試覆蓋（`activity_3002.kml` 嘅 Lap 2 只係缺 `Elevation Gain` / `Loss`，而嗰兩個欄位而家根本冇人讀 —— 見 S-006），即係 docstring 描述嘅行為由頭到尾未被驗證過。

**方案 A（推薦）**：向 TCX 睇齊 —— lap 缺 `Time` 或 `Distance` 就當 parse error。

```python
def _aggregate_laps(lap_rows: list[dict]) -> tuple[float, float]:
    total_sec = 0.0
    total_km = 0.0
    for row in lap_rows:
        try:
            total_sec += hhmmss_to_seconds(row[_LAP_TIME_KEY])
            total_km += float(row[_LAP_DISTANCE_KEY].split()[0])
        except (KeyError, IndexError, ValueError) as exc:
            raise ActivityParseError(f"lap {row.get('Lap')} is missing a total: {exc}") from exc
    return total_sec, total_km
```

- 優點：兩個 parser 對「聚合來源欄位」嘅政策一致；永遠唔會出錯數；WARNING 會講清楚邊個 lap 出事。順帶關咗 `float(...split()[0])` 嘅 IndexError / ValueError 逃逸路徑。
- 缺點：由「保住 activity」變成「丟棄 activity」。因為真實數據 0/2592 命中，實際影響為零；但如果 Garmin 將來改格式，會由「數字微錯」變成「批量丟失 + 一堆 WARNING」—— 我認為呢個係更好嘅失敗模式（響過無聲）。

**方案 B**：保留寬容行為，但缺數時記 WARNING 並喺 `Activity` 標記數據不完整。

```python
if time_text:
    total_sec += hhmmss_to_seconds(time_text)
else:
    logger.warning("Lap %s has no Time; activity totals will be short", row.get("Lap"))
```

- 優點：唔丟 activity，同時錯數唔再靜默；改動最細（兩個 `else` 分支）。
- 缺點：`Activity` 本身唔帶「呢個數係唔完整」嘅標記，所以 dashboard 仍然會將一個少計嘅距離當成準確值展示；log 只幫到跑 CLI 嗰個人。

**推薦 A**（同 TCX 一致，錯數比缺數更危險）。無論揀邊個，都要補一個 fixture：lap table 缺 `Distance` 嘅 KML，釘死所選行為 —— 呢個係目前完全未測嘅路徑。

---

### 🟡 W-007 — 新增嘅失敗路徑基本上未被測試覆蓋（86% 係假象）

**位置**：`src/activities/parsers/base.py:55, 66-67, 121, 124-127`；`tcx.py:60, 78`；`gpx.py:62`；`kml.py:75, 100, 104`

我跑 `pytest --cov=src --cov-report=term-missing`：

```
src/activities/parsers/base.py    49    7    86%   55, 66-67, 117, 121, 124-127
src/activities/parsers/gpx.py     54    5    91%   62, 66, 70, 113, 131
src/activities/parsers/kml.py     89    8    91%   58, 72, 75, 100, 104, 119, 124, 140
src/activities/parsers/tcx.py     51    4    92%   60, 68, 78, 120
```

逐條對照 `base.py`：

| 行 | 內容 | 狀態 |
|---|---|---|
| 55 | `raise ActivityParseError(f"missing required element {path}")` | **未覆蓋** |
| 66-67 | `except ValueError: return None`（`optional_float` 不可解析分支）| **未覆蓋** |
| 121 | `except ActivityParseError → logger.warning` | **未覆蓋** |
| 124-127 | `except ValueError → logger.debug` | **未覆蓋** |

**Step-by-step**：

1. AU-002 嘅核心產出係「三段分級 except」加「四個 guarded helper」。
2. 三段 except 之中，**只有 `ET.ParseError` 一段有測試**（`test_should_log_warning_naming_the_skipped_file` 靠 `activity_1003.tcx`）。`ActivityParseError` 段同 `ValueError` 段一個測試都冇。
3. 四個 helper 之中，`required_text` 嘅 raise 分支同 `optional_float` 嘅「有值但唔係數字」分支都冇 fixture 觸發 —— 所有 fixture 嘅 optional 欄位都係「完全冇呢個 tag」，冇一個係「有 tag 但內容係垃圾」。
4. 三個 parser 入面**每一個 `raise ActivityParseError` 都未被執行過**（`tcx.py:60` missing Activity、`tcx.py:78` no laps、`gpx.py:62` missing metadata、`kml.py:75` no laps）。
5. 所以 86% / 91% 呢啲數字反映嘅係「happy path 覆蓋得好」，而風險完全集中喺未覆蓋嗰 9%。W-005 嗰個 `optional_int("nan")` bug 就係喺呢個盲區入面生存 —— 加一個「cadence 內容係 `nan`」嘅 fixture 就會即刻捉到。

**影響**：呢批工作嘅立項理由就係審計嗰句「測試同 lint 都繞開咗風險最高嗰幾個檔案」。而家檔案唔再係零覆蓋，但**新加嘅錯誤處理邏輯本身仍然係零覆蓋**，性質相同，只係下沉咗一層。

**方案 A（推薦）**：加 3 個 fixture + 4 個測試，直接打未覆蓋嗰幾行。

| Fixture | 內容 | 打中 |
|---|---|---|
| `activity_1004.tcx` | `<Activity>` 冇 `<Id>`（或者一個 Lap 都冇）| `required_text` raise（base 55）、`tcx.py:60/78`、`except ActivityParseError`（base 121）|
| `activity_1005.tcx` | 合法但 `<Id>2019-…`（舊年度）| `except ValueError → DEBUG`（base 124-127）|
| `activity_1006.tcx` | `<ns3:RunCadence>nan</ns3:RunCadence>` + `<AltitudeMeters>abc</AltitudeMeters>` | `optional_float` except（base 66-67）+ **W-005 嗰個 bug** |

配合三個 assert：`caplog` 確認 `ActivityParseError` 出 WARNING；`caplog` 確認舊年度出 DEBUG **而唔係** WARNING（呢個先至真正釘死 Lane B 嘅偏離）；`parse_all` 對 `nan` fixture 唔會令整個 call 拋錯。
- 優點：直接令 `base.py` 去到 ~100%，而且係覆蓋喺真正有風險嗰幾行；順手釘死 DEBUG/WARNING 分級決定，令將來有人改 log level 會被 CI 捉到。
- 缺點：fixture 由 9 個加到 12 個。

**方案 B**：唔加 fixture，改用 `unittest.mock` / 手砌 `ET.Element` 直接單元測試四個 helper 同 `parse_all` 嘅 except 分支。
- 優點：唔使維護更多 XML 檔案；測試更聚焦、跑得更快。
- 缺點：離真實輸入更遠，會漏咗「真實 Garmin 檔案入面呢種形狀係咪真係存在」呢個問題 —— 而呢個正正係 golden fixture 嘅價值所在。

**推薦 A**（AU-002 揀咗 golden fixture 路線，應該行到底），helper 嘅純函數測試可以用 B 補充。

---

### 🟢 S-005 — Zero-record guard 只加咗喺 `export_data`，`process_activities` 仍然會靜默寫 0 筆

**位置**：`src/cli/process_activities.py:55-58`

```python
activities = parser_cls(current_year=year).parse_all(directory)
print(f"  Parsed {len(activities)} activities")
_activities_to_jsonl(activities, out_json)     # 0 筆一樣照寫，exit code 0
```

AU-002 喺 `export_data.main()` 加咗「parsed 0 → `sys.exit(1)`」，但 `run365-activities` 係**第二個** call `parse_all` 嘅入口，亦係**唯一**用 KMLParser 嗰個。佢會開開心心寫一個 0 行嘅 JSONL 然後 exit 0。

呢個嚴格嚟講超出 ticket 範圍（審計嘅建議 (b) 明確只點名 `export_data.main()`），所以我列為 Suggestion 而唔係 Warning。但既然係同一個失敗模式、同一次 pass，值得順手關埋。

**建議**：喺 `run()` 每個 format 之後加

```python
if not activities:
    sys.exit(f"No {name.upper()} activities parsed from {directory} for year {year}")
```

`--format all` 之下要諗清楚係「任何一個 format 空就死」定「全部空先死」—— 我傾向前者（一個 format 突然變零通常代表路徑或年份填錯）。

---

### 🟢 S-006 — TCX `_lap_row` 仍然計算冇人讀嘅 `MaxSpeed`

**位置**：`src/activities/parsers/tcx.py:107`

```python
"MaxSpeed": optional_float(lap, "ns:MaximumSpeed", _NS) or 0.0,
```

`lap_df` 之後只讀 `["Time"]`（:81）、`["Distance"]`（:82）、`["Calories"]`（:83）。`MaxSpeed` 由頭到尾冇人用 —— 同 Lane B 喺 KML 度（正確地）刪走嘅 `Elevation Gain` / `Loss` / `Max Speed` 係一模一樣嘅死碼，只係佢喺 TCX 度留低咗。

**建議**：刪走呢一行。順帶：`optional_int(...) or 0` 對 `Calories` 而言，會將「缺失」同「真係 0 卡」壓成同一個值 —— 今日冇影響（真實檔案全部有 Calories），但如果將來要區分「未知」同「零」，`if x is None` 會比 `or` 準確。

---

### 🟢 S-007 — `_SUMMARY_ROW_COLSPAN = 2` 一個常數借咗三個唔同語義

**位置**：`src/activities/parsers/kml.py:30, 121, 123, 139`

```python
_SUMMARY_ROW_COLSPAN = 2

if tds[0].has_attr("colspan") and int(tds[0]["colspan"]) == _SUMMARY_ROW_COLSPAN:  # :121 真係 colspan
if len(tds) < _SUMMARY_ROW_COLSPAN:                                                # :123 最少 cell 數
if len(lon_lat) < _SUMMARY_ROW_COLSPAN:                                            # :139 最少座標分量數
```

抽 magic number 係啱嘅，但呢度將三個「碰巧都係 2」嘅無關概念綁咗喺一個名之下。`:139` 讀落尤其誤導 —— 座標分量數同 HTML table 嘅 summary row colspan 冇任何關係。將來如果 summary row 變成 colspan=3，改一個常數會同時默默改壞座標解析。

**建議**：拆成三個名：`_SUMMARY_ROW_COLSPAN = 2`、`_LAP_TABLE_MIN_CELLS = 2`、`_MIN_COORD_PARTS = 2`。三行 module-level 常數，語義各自獨立。

---

### 🟢 S-008 — Fixture 缺咗每個真實 KML 都有嘅 `Start` / `End` lap placemark

**位置**：`tests/fixtures/activity_3001.kml`、`activity_3002.kml`

我掃咗真實 365 個 KML 嘅 lap placemark 首字 histogram：

```
Lap 2592 / Start 357 / End 357
```

即係每個有 track 嘅真實檔案都有一個 `Start` 同一個 `End` placemark（兩者都有 `<description>` 同 `<Point>`），全部靠 `kml.py:103-104` 嘅 `if "Lap" not in parts[0]: continue` 濾走。而 coverage 顯示 **`kml.py:104` 未覆蓋** —— 因為兩個 fixture 都只有 `Lap 1` / `Lap 2`。

呢個係我審「fixture 係咪遷就實作寫出嚟」嗰一點入面**唯一**搵到嘅真實 schema 偏差。整體而言 fixture 忠實度好高（見下方 ✅），但呢個過濾器係 lap 聚合正確性嘅前哨 —— 如果佢壞咗，`Start` / `End` 嘅 table 會被當成 lap 加入總和，距離會多計。

**建議**：喺 `activity_3001.kml` 嘅 `Laps` folder 頭尾各加一個 `Start` / `End` placemark（照抄真實結構：有 name、有 description、有 `<Point><coordinates>null,null</coordinates></Point>`），然後現有嘅 `assert act.distance_km == 2.0` 就會自動變成「過濾器有效」嘅斷言 —— 如果過濾器失效，總距離會變成 4.0 而測試會紅。零新增測試，只改 fixture。

---

### 🟢 S-009 — KML 每個 track point 嘅時間戳被解析兩次

**位置**：`src/activities/parsers/kml.py:146` → `:70`

```python
time=parse_datetime(begin).strftime(_TIMESTAMP_FORMAT),   # :146 parse → format
...
act_time = parse_datetime(track_points[0].time) if track_points else None   # :70 再 parse 返
```

`_parse_track_points` 已經 parse 過 `begin`，`strftime` 之後 `parse()` 再攞返 `track_points[0].time` 重新 parse 一次。舊代碼喺 loop 入面用 `if act_time is None: act_time = beg` 記住第一個，冇呢個 round-trip。

功能上等價（`strftime` 掉走嘅 offset 資訊喺 `_TIMESTAMP_FORMAT` 之下唔影響牆鐘），性能影響亦微不足道（每個 activity 一次）。但呢個 round-trip 令「時間戳格式」變成兩個函數之間嘅隱性契約：如果 `_TIMESTAMP_FORMAT` 改成帶 offset，`:70` 就會行 `parse_datetime` 嘅 `"+" in` 分支而攞到唔同結果。

**建議**：`_parse_track_points` 額外回傳第一個 point 嘅 `datetime`，或者由 `parse()` 直接讀第一個 placemark 嘅 `begin`。呢個係 refactor 留低嘅小尾巴，唔急。

---

### ✅ Looks Good（Section C）

- **Golden fixture 對真實 schema 嘅忠實度高**。我逐項對過真實 export：lap `<description>` 係 HTML-escape 過嘅 `<table>`（fixture 用 `&lt;table…` —— parse 完等價）✅；lap placemark 嘅座標係 `null,null`（呢個係好易忽略嘅真實怪癖，fixture 有）✅；track point 座標係 `lon,lat, ele` 用 `", "` 分隔（fixture 有）✅；track point placemark 冇 `<name>`（fixture 有）✅；root folder name 含 "Running"（真實係 `Kowloon Running`，fixture 用 `Running`，兩者都通過 `not in` 檢查）✅。TCX 方面，`activity_1002.tcx` 嘅「有 Time / DistanceMeters 但冇 Position / AltitudeMeters」形狀，我喺真實 `activity_6108462888.tcx` 第一個 Trackpoint 見到一模一樣嘅結構 —— **呢個唔係憑空砌出嚟遷就實作，係真實形狀**。
- **測試 assert 嘅係實質數值，唔係「唔 raise」**。`distance_km == 2.0`、`pacing == "0:05:00"`、`calories == 150`、`avg_cadence == 148.0`（63+85 = 148，即 `_STEPS_PER_CADENCE_SAMPLE` 相乘之後嘅平均，呢個數字要真係理解過 cadence 語義先寫得出）、`min_elevation == 329.8` / `max_elevation == 331.0`、`track[0].sec == 0 and track[-1].sec == 599`。冇一個係 tautological。
- **裁決：Lane B 刪死碼 —— 屬實，而且比佢自己講嘅更有價值。** 我對過舊代碼 `kml.py:104-105`：`for col in ("Distance", "Elevation Gain", "Elevation Loss", "Max Speed"): lap_df[col] = lap_df[col].apply(...)`，而 `:107-108` 只讀 `Time` 同 `Distance`。三個欄位確認係死碼。更重要嘅係佢確認咗係一個 crash mode，而且係一個**衝穿舊 suppress 嘅** crash mode —— 如果任何一個 lap 嘅 HTML table 缺 `Elevation Gain`，`lap_df["Elevation Gain"]` 會 raise **KeyError**，而舊 `contextlib.suppress(ValueError, AttributeError, ET.ParseError)` **唔 catch KeyError**，即係整個 `run365-activities` 崩潰。刪得啱。同樣 `_HK_TZ`（AU-003 度）我 grep 過全 repo，確認無人引用。
- **裁決：KML refactor —— 合理，超出「最小改動」但物有所值。** `parse()` 96 → 45 行，拆成 `_parse_laps` / `_lap_table_cells` / `_parse_track_points` / `_aggregate_laps`。我嘅判斷理由：(1) 舊 `parse()` 係一個四層巢狀（for subfolder → if name → for placemark → for tr）嘅 96 行函數，遠超 `sw-coding-style` 嘅 Python 30 行上限，本身就係一個 Warning；(2) 拆出嚟嘅四個函數都係 module-level 純函數，可以獨立測試 —— 我自己驗證 lap table 嗰段時就直接 `from ...kml import _parse_laps` 掃咗 365 個真實檔案，如果佢仲喺 `parse()` 入面我做唔到；(3) lap 聚合由 pandas 改純 Python **順帶消除咗一個依賴同一個 crash mode**（見上一點嘅 KeyError）—— 兩行 `total_sec += / total_km +=` 做嘅嘢，本來要起一個 DataFrame。三個理由加埋，refactor 嘅範圍係由 ticket 目標推導出嚟嘅，唔係品味驅動。**接受。**
- **`export_data.main()` 嘅 zero-record guard 訊息寫得好**：唔止講「refusing to write an empty export」，仲講埋「Re-run with logging at WARNING to see which files the parsers skipped and why」—— 將錯誤訊息同新加嘅 logging 機制連埋，令下一個撞到嘅人知道點查。呢個係 error message 應有嘅樣。
- **`tcx.py:77-78` 嘅 `if not lap_rows: raise ActivityParseError`** 補咗一個舊代碼冇處理嘅洞：舊代碼 `pd.DataFrame([])["Time"]` 會 raise KeyError（同樣唔喺舊 suppress 入面）拉冧成個 export。而家變成一句 WARNING + 跳過。
- **`gpx.py:112` 嘅 `if time_text is None or lat is None or lon is None: continue`** 同 **`tcx.py:118-120`** 都揀咗「跳過壞 point」而唔係「丟棄整個 activity」，方向正確 —— point 級嘅缺失唔應該升級成 activity 級嘅損失。

---

## Section D — AU-001（`991c19b`）GraphQL query cost

改動：`schema.py` +137（4 個常數 + `_page` / `_track_points` 驗證 + `build_schema`）、`service.py` +102（`_page` / `_stride_filter` + 四個 resolver 加窗口）、`api/graphql.py` +14（GraphiQL env gate）、`schema.graphql` regenerate、`test_api.py` +230。

### 評分

| 維度 | 得分 | 滿分 | 備註 |
|---|---|---|---|
| 正確性 | 15 | 25 | W-009、W-010 |
| 安全性 | 15 | 20 | W-008 |
| 可維護性 | 18 | 20 | S-011、S-013 |
| 測試覆蓋 | 14 | 15 | S-012（`schema.py` 100%）|
| 性能 | 9 | 10 | S-010 |
| 代碼風格 | 10 | 10 | — |
| **總分** | **81** | **100** | ⚠️ warn |

### 🟡 W-008 — Docstring 宣稱關咗 introspection，實際完全冇關

**位置**：`api/graphql.py:18-20`（同 commit message 第 4 個 bullet）

```
- The GraphiQL IDE (and with it the introspection a browser IDE needs) stays
  off unless ``RUN365_GRAPHIQL`` is set to a truthy value, so a public
  deployment exposes the endpoint only.
```

**Step-by-step**（我實跑驗證）：

```python
app = create_app(p, graphiql=False)
c.post('/api/graphql', json={'query': '{ __schema { queryType { name } } }'})
# -> 200 {"data": {"__schema": {"queryType": {"name": "Query"}}}}

c.get('/api/graphql', headers={'Accept': 'text/html'})
# -> 404 Not Found
```

1. `create_app` 將 `graphiql` 轉成 `graphql_ide="graphiql" if graphiql else None`（`app.py:44-46`）。
2. Strawberry 嘅 `graphql_ide` 參數**只控制 GET 請求返唔返 IDE 嘅 HTML 頁**。GET 確實變成 404 ✅。
3. Introspection 係 **schema 層**嘅能力，要靠 validation rule（例如 `NoSchemaIntrospectionCustomRule`）先關得到。`build_schema()` 只加咗 `QueryDepthLimiter` 同 `MaxTokensLimiter`，冇任何 introspection rule。
4. 所以 POST `{ __schema { … } }` 喺 production 一樣**完整回覆整個 schema**。

**影響**：安全姿態本身冇變差（AU-001 之前 introspection 一樣開住）。真正嘅問題係**代碼文件斷言咗一個唔存在嘅控制**。下一個做 security review 嘅人讀完 `api/graphql.py` 嘅 module docstring，會將「introspection 已關」當成既有事實而唔會再驗 —— 呢個係 false assurance，比冇寫更差。

考慮到呢個 API 本身係公開唯讀嘅個人跑步數據、冇 auth 可繞、真正嘅 DoS 控制（depth / token limiter）確實生效，我評 🟡 Warning 而唔係 🔴 Critical。

**方案 A（推薦，改文字）**：修正 docstring 同埋講清楚實際狀態。

```
- The GraphiQL IDE is off unless ``RUN365_GRAPHIQL`` is set to a truthy value,
  so a public deployment serves the POST endpoint only. Schema introspection
  itself stays on: the data is public and the depth and token limits bound
  what any single document can cost.
```

- 優點：零功能風險，一分鐘完成，文件變返真確。
- 缺點：introspection 仍然開住 —— 如果團隊本來就想關，呢個方案冇解決意圖。

**方案 B（改行為，令文件變真）**：introspection 跟 GraphiQL 一齊由環境變數控制。

```python
# schema.py
from graphql import NoSchemaIntrospectionCustomRule
from strawberry.extensions import AddValidationRules

def build_schema(max_depth=MAX_QUERY_DEPTH, max_tokens=MAX_QUERY_TOKENS, introspection=False):
    extensions = [
        lambda: QueryDepthLimiter(max_depth=max_depth),
        lambda: MaxTokensLimiter(max_token_count=max_tokens),
    ]
    if not introspection:
        extensions.append(AddValidationRules([NoSchemaIntrospectionCustomRule]))
    return strawberry.Schema(query=Query, extensions=extensions)
```

- 優點：真正做到 docstring 講嘅嘢；減少攻擊者免費攞 schema map 嘅便利。
- 缺點：`schema` 係 module-level singleton（`schema.py:386`）而 GraphiQL 開關喺 `api/graphql.py`，要將 schema 建構下推到 `create_app`，改動面比 A 大好多；而且 `run365-schema --check` 同前端 codegen 都靠 SDL（`frontend/schema.graphql`）而唔係 runtime introspection，所以要小心確認冇工具依賴 runtime introspection。另外開 `RUN365_GRAPHIQL=1` 做本地開發時要記得同時開返 introspection，否則 IDE 冇 autocomplete。

**推薦 A**（呢個 API 嘅數據本來就公開，關 introspection 收益細、改動面大）。如果團隊決定要 B，一定要順手加一個測試 assert `{ __schema { queryType { name } } }` 喺預設之下被拒 —— 而家一個都冇（測試只 assert IDE 嘅 HTML 唔出現），呢個亦係點解呢個錯誤斷言可以喺全綠之下存活。

---

### 🟡 W-009 — List field 會靜默截斷：冇 `totalCount` / `hasNextPage`，client 分唔到「就係咁多」定「仲有」

**位置**：`src/api/schema.py:32-38`、`frontend/schema.graphql:78`

```python
DEFAULT_PAGE_SIZE = 500
"""Rows a list field returns when the client asks for no window.

Deliberately above the 365 rows a full year holds, so clients that read a
whole year in one request (the dashboard does) need no paging.
"""
```

**Step-by-step**：

1. `activities` / `weight` / `weather` / `warnings` 而家有 `limit: Int! = 500`。
2. 前端**唔會傳 `limit`**（我 grep 過 `frontend/src/data/api/queries.ts` 同 generated `gql.ts`，四個 list query 都冇 `limit` argument），所以永遠攞 default 500。
3. 回應係一個裸 list（`[Activity!]!`），冇 `totalCount`、冇 `pageInfo`、冇 `hasNextPage`。
4. 所以當 DB 有 501 行時，前端會收到 500 行、HTTP 200、零錯誤、零警告，然後照樣渲染 —— **一個少咗數據嘅 dashboard，睇落完全正常**。
5. 今日安全係因為 DB 只裝一年（365 行）。但 `weather` / `warnings` 嘅行數唔一定同 activity 對齊（`warnings` 一日可以多過一條），而且 project 嘅路線圖係跑多過一年。呢個係一個「加多一年數據就靜默壞」嘅陷阱，而且壞嘅時候冇任何訊號。
6. 相比之下，超出上限嘅請求（`limit: 1001`）會**明確報錯**（`_page` raise）—— 即係「client 要得太多」有保護，「server 給得太少」冇保護。方向係啱嘅但只做咗一半。

**方案 A（推薦，最小改動）**：`limit` 攞滿時，喺 server 記一句 WARNING，並喺 GraphQL 回應加 extension 或者一個獨立 `activitiesCount` field。

```python
# schema.py
@strawberry.field(description="Total activities matching the filters, ignoring limit/offset.")
def activities_count(self, info: Info, from_date=None, to_date=None, min_km=None, has_gps=None) -> int:
    return service.activities_count(info.context["session"], _iso(from_date), _iso(to_date), min_km, has_gps)
```

- 優點：唔改現有 field 嘅形狀（唔係 breaking change），前端可以喺一個 query 入面同時攞 list 同 total，發現唔夠就自己分頁。SQL 側就係一個 `select(func.count())` 套同一組 filter。
- 缺點：四個 list field 各要一個 count field（四個 resolver + 四個 service function），有少少重複；filter 參數要同步維護兩處。

**方案 B**：改成 Relay-style connection（`ActivityConnection { totalCount, pageInfo { hasNextPage }, nodes }`）。
- 優點：業界標準，一次過解決全部四個 field，`hasNextPage` 語義清晰，將來加 cursor 分頁有現成路。
- 缺點：**breaking schema change** —— 前端四個 query、generated types、`frontend/schema.graphql`、`run365-schema --check` 全部要改；對一個單用戶 dashboard 嚟講係大炮打蚊。

**推薦 A**（或者更輕：先淨係喺 `service._page` 度，當回傳行數 == limit 時 `logger.warning`，令 server log 至少有訊號，成本三行）。B 留俾真係需要分頁 UI 嗰日。

---

### 🟡 W-010 — SQL stride + 二次 Python `downsample` 令取樣間距明顯不均，同 field description 嘅「evenly」矛盾

**位置**：`src/api/service.py:94-116`、`src/api/schema.py:136`

```python
# service.py:113-116
stride = total // points
return or_(models.TrackPoint.seq % stride == 0, models.TrackPoint.seq == last_seq)
```

```python
# schema.py:136
@strawberry.field(description="GPS track, evenly downsampled to at most `points` samples.")
```

**我嘅實測**（13 組 (total, points)，每組同舊嘅純 Python `downsample` 逐一對比）：

| total | points | 新（SQL stride → downsample）| 舊（純 downsample）|
|---|---|---|---|
| 600 | 5 | `0, 120, 240, 480, 599` gap 120/120/**240**/119 | `0, 150, 300, 449, 599` gap 150/150/149/150 |
| 600 | **7** | `0, 85, 255, 340, 425, 595, 599` gap 85/**170**/85/85/**170**/**4** | `0, 100, 200, 300, 399, 499, 599` 全部 ~100 |
| 100 | 3 | `0, 66, 99` | `0, 50, 99` |
| 10 | 3 | `0, 6, 9` | `0, 4, 9` |
| 600 | 150 | gap 3–8（輕微抖動）| gap 4（均勻）|

**Step-by-step**（點解會咁）：

1. `stride = total // points` 係**向下取整**，所以 SQL 揀返嚟嘅候選點數**多過** `points`（`ceil(total/stride) + 1`）。
2. 例如 600 / 7 → stride = 85 → 候選 `0, 85, 170, …, 595` 共 8 個，加 `last_seq=599` 共 9 個。
3. 跟住 `downsample(rows, 7)` 喺呢 9 個**已經稀疏化**嘅候選上再抽 7 個，index = `round(i × 8/6)` = 0,1,3,4,5,7,8 → 即係丟走咗 170 同 510 兩個候選。
4. 結果：間距由 85 跳到 170（2 倍差距），而尾段 595 同 599 差 4（幾乎相鄰）。
5. 兩層取樣疊加 —— SQL 一層 floor stride，Python 一層 `round(i × step)` —— 誤差互相放大。單獨用任何一層都會係均勻嘅。

**好消息（我要講清楚）**：

- **數量永遠準確**。13 組全部 `len(result) == points`，一次都冇少過。呢個係因為 `stride` 向下取整保證候選數 ≥ points。我被問嘅「stride 除唔盡會唔會出現點數少過要求」—— **答案係唔會**，呢個方向係安全嘅。
- **尾段唔會密集到出問題**，但 600/7 嗰個 case 出現咗 595→599 相鄰嘅一對，係「尾段輕微密集」嘅實例。
- **對前端零影響**：`ActivityView.tsx:17` `TRACK_POINTS = 600`，而 export 每個 activity 最多存 600 點（`export_data.py:27 DEFAULT_POINT_LIMIT = 600`），所以 `total <= points` → `_stride_filter` 直接 `return None` → 行返全量讀取舊路徑。**stride 路徑喺 production 前端根本唔會被行到。**
- 唯一會行到嘅係 schema default `points = 150`（即 `{ activities { track } }` 呢類 ad-hoc query），而 600/150 嘅結果只係輕微抖動（gap 3–8 vs 均勻 4），視覺上睇唔出。

所以呢個係「合約同實作唔一致 + 小 points 時取樣質素下降」，唔係即時 bug。但 field description 白紙黑字寫住 **evenly**，而 600/7 嘅 2 倍間距差距唔算 evenly。

**方案 A（推薦，精確重現舊行為）**：喺 Python 直接算出目標 `seq` 清單，用一次 `IN` query 攞。

```python
def _target_seqs(total: int, points: int, first: int, last: int) -> list[int]:
    step = (total - 1) / (points - 1)
    return [first + round(i * step) for i in range(points)]

# track()
if points:
    total, first, last = session.execute(
        select(func.count(), func.min(TrackPoint.seq), func.max(TrackPoint.seq))
        .where(TrackPoint.activity_id == activity_id)
    ).one()
    if total > points:
        stmt = stmt.where(TrackPoint.seq.in_(_target_seqs(total, points, first, last)))
```

- 優點：取樣位置**同舊 Python `downsample` 逐點一致**，「evenly」合約回復真確；讀返嚟啱啱好 `points` 行（比而家仲少）；之後唔使再 `downsample`。仍然係兩個 query。
- 缺點：假設 `seq` 連續無斷層（今日成立 —— export 寫入時逐點遞增）；`IN` 清單最多 `MAX_TRACK_POINTS = 1000` 個參數，舊版 SQLite（< 3.32）嘅 `SQLITE_MAX_VARIABLE_NUMBER` 預設係 999，需要確認 runtime 版本或者將 `MAX_TRACK_POINTS` 降到 999。

**方案 B（保留 stride，加大過取樣倍數）**：`stride = total // (points * OVERSAMPLE)`，之後照舊 `downsample`。

```python
_OVERSAMPLE = 10   # denser candidate grid so the second pass lands near the ideal positions
stride = max(1, total // (points * _OVERSAMPLE))
```

我實算過 600/7 用 `_OVERSAMPLE = 10`：stride = 8 → 候選 76 個 → `downsample` 抽 7 → `0, 104, 200, 304, 400, 504, 599`，gap 104/96/104/96/104/95 —— 基本均勻。
- 優點：唔假設 `seq` 連續（純粹 modulo，對斷層 robust）；改一行；讀返嚟 `points × 10` 行（7 → 76 行）仍然遠少過 600。
- 缺點：只係逼近而唔係精確；`_OVERSAMPLE` 係一個要解釋嘅調參數；讀返嚟嘅行數比 A 多 10 倍（絕對值仍然好細）。

**推薦 A**（精確、行數更少、消除二次取樣）；如果擔心 `seq` 將來會有斷層（例如加入刪點功能），揀 B。無論邊個，都應該補一個測試 assert 取樣間距嘅最大值同最小值比例喺合理範圍內 —— 而家嘅測試（`test_track_returns_at_most_the_requested_points`）只 assert 數量同頭尾，所以呢個不均勻性完全冇被捕捉到。

---

### 🟢 S-010 — `_stride_filter` 令每個 activity 多一次 round-trip，N+1 由 1× 變 2×

**位置**：`src/api/service.py:107-111`

```python
total, last_seq = session.execute(
    select(func.count(), func.max(models.TrackPoint.seq)).where(...)
).one()
```

`{ activities { track(points: 150) } }` 之下，365 個 activity 而家係 365 × (1 個 COUNT/MAX + 1 個 SELECT) = **730 個 query**，之前係 365 個。記憶體大幅改善（呢個係 ticket 目標，達成咗），但 round-trip 數翻倍，而審計嘅 AU-001 條目本身就有點名 N+1。

對本地 SQLite 而言每個 query 係微秒級，實際影響細；但喺 Vercel serverless 嘅 15 秒預算下，730 次 SQLite 呼叫比 365 次值得留意。

**建議**：`_stride_filter` 需要嘅 `total` / `max(seq)` 其實可以由 `models.Activity.num_points` 攞到（`export/models.py` 已經有呢個欄位，`test_api.py` 亦有填），咁就慳返個 COUNT。或者真正解決 N+1 —— 用一個 window function query 一次過攞晒所有 activity 嘅 track。後者屬獨立 ticket。

---

### 🟢 S-011 — `points: 0` 嘅新行為正確，但兩個 data mode 而家唔一致，而且範圍冇寫入 schema

**位置**：`src/api/schema.py:60-73`、`frontend/src/data/static/source.ts:66`

**我被要求核實嘅第 5 點 —— 裁決：開發者聲稱屬實。** 我逐個 call site 查過：

- `frontend/src/views/activity/ActivityView.tsx:17` → `export const TRACK_POINTS = 600`，`:33` `useTrack(current, TRACK_POINTS)`
- `frontend/src/data/api/source.ts:20` → `DEFAULT_TRACK_POINTS = 600`，`:57` `track(id, points = DEFAULT_TRACK_POINTS)`
- generated `TrackDocument` → `$points: Int!`（NonNull，必傳）

**repo 同前端都冇任何地方送 0。** 而且要留意舊行為：`points=0` 之下 `if points:` 係 falsy → **完全唔 downsample，回傳全部點** —— 即係「0」以前係「無上限」，正正就係 AU-001 要堵嘅 DoS 向量。**所以拒絕 0 唔止安全，仲係修正咗一個反直覺嘅語義。呢個判斷啱。**

兩個殘留小問題：

1. `frontend/src/data/static/source.ts:66` `return points ? downsample(rows, points) : rows;` —— static mode 仍然將 0 當「攞晒」。同一個 `useTrack(id, 0)` 喺 static mode 回全部點、喺 api mode 回錯誤。今日冇 caller 送 0，但兩個 mode 嘅契約應該一致。
2. `MAX_TRACK_POINTS` / `MAX_PAGE_SIZE` 嘅範圍只存在於 Python docstring 同 runtime 錯誤訊息，**冇寫入 SDL**。前端開發者睇 `frontend/schema.graphql` 只會見到 `track(points: Int! = 150)`，唔知道 1–1000 呢個界。

**建議**：static source 改成 `points && points > 0 ? … : rows`（或者直接同步拋錯）；schema field description 補上範圍，例如 `"GPS track, evenly downsampled to `points` samples (1–1000)."` —— 呢個會自動流入 `frontend/schema.graphql` 同 generated types 嘅註釋。

---

### 🟢 S-012 — Depth limit 嘅測試冇驗證出貨嗰個值

**位置**：`tests/test_api.py`（`test_query_deeper_than_the_configured_limit_is_rejected`）

```python
shallow_schema = build_schema(max_depth=2)
result = shallow_schema.execute_sync(DEEPEST_CLIENT_QUERY)
assert result.errors
```

呢個測試驗證嘅係「`QueryDepthLimiter` 呢個機制接得上」，唔係「出貨嘅 `MAX_QUERY_DEPTH = 5` 會拒絕深度 6」。配合 `test_deepest_client_query_is_within_the_depth_limit`（深度 4 通過），中間嘅 5 同 6 完全冇覆蓋。如果有人手誤將 `MAX_QUERY_DEPTH` 改成 50，兩個測試都仍然綠。

Token limit 嗰邊做得啱 —— `test_document_with_too_many_tokens_is_rejected` 打嘅係真 client（真 schema、真 1000 上限），所以出貨值有被驗證。

**建議**：加一個用真 `client` 嘅深度 6 query（例如 `{ activities { track { ... } } }` 再套多一層 alias fragment），assert 被拒。或者更簡單：`assert MAX_QUERY_DEPTH == 5` 之類嘅 golden assert 至少令改動變得顯眼。

---

### 🟢 S-013 — Track point 常數而家散落 5 處（餵大 AU-008）

| 值 | 位置 | 語義 |
|---|---|---|
| 600 | `src/cli/export_data.py:27` `DEFAULT_POINT_LIMIT` | export 每個 activity 最多存幾多點 |
| 600 | `frontend/src/views/activity/ActivityView.tsx:17` `TRACK_POINTS` | 前端要求幾多點 |
| 600 | `frontend/src/data/api/source.ts:20` `DEFAULT_TRACK_POINTS` | api source 預設 |
| 150 | `src/dashboard/builder.py:17` `TRACK_POINT_LIMIT` | `downsample` 預設 |
| 150 / 1000 | `src/api/schema.py:20, 24` `DEFAULT_TRACK_POINTS` / `MAX_TRACK_POINTS` | GraphQL 預設 / 上限 |

AU-001 加咗兩個（`MAX_TRACK_POINTS`、`MAX_PAGE_SIZE`）。三個 `600` 之間有隱性耦合 —— 前端要求 600 而 export 存 600，所以「啱啱好唔觸發 stride」；如果有人將 `DEFAULT_POINT_LIMIT` 降到 300 而唔郁前端，stride 路徑會突然喺 production 生效（連同 W-010 嘅不均勻取樣）。

審計嘅 AU-008（P1，「同一 domain constant 散落 5 處」）已經涵蓋呢個問題，所以我唔要求喺呢批處理。**建議**：喺 AU-008 嘅 ticket 描述補上呢五個位，避免遺漏。

---

### ✅ Looks Good（Section D）

- **`selectinload` + LIMIT 係啱嘅**，而且係一個好易寫錯嘅位。`activities` 用 `selectinload(models.Activity.warnings)` 配 `.limit()` —— `selectinload` 行第二條 `IN` query，唔會影響主 query 嘅行數。如果當初用咗 `joinedload`，LIMIT 就會作用喺 **JOIN 之後**嘅行數上，一個有 3 條 warning 嘅 activity 會食掉 3 個 limit 額度，`limit: 1` 隨時回 0 個 activity。呢個 bug 喺 365 行數據下極難察覺。開發者揀啱咗（或者保留咗）正確嘅 loader。
- **`_page` 喺 `order_by` 之後施加，四個 resolver 全部一致**。冇 ORDER BY 嘅 LIMIT/OFFSET 喺 SQLite 係非確定性嘅，分頁會出現重複同遺漏。四個 field 我逐個對過：`activities`（date, start_time）、`weight`（date）、`daily_weather`（date）、`warnings`（date, id）—— 全部有 order，而且 `warnings` 加咗 `id` 做 tie-break（同一日多條 warning 嘅情況）。細節做足。
- **Depth / token 嘅數字係實測出嚟嘅，唔係拍腦袋。** 我獨立驗證：`get_introspection_query()` lex 出 **163 tokens**（同 docstring 寫嘅 163 完全一致），而且通過 depth limiter（Strawberry 嘅 `QueryDepthLimiter` 預設略過 introspection key）。即係**開 `RUN365_GRAPHIQL=1` 做本地開發時，GraphiQL 嘅 autocomplete 唔會被 depth limit 打死** —— 呢個係加 depth limiter 最常見嘅 DX 陷阱，佢避開咗，而且喺 docstring 留低咗數字俾人核對。`MAX_QUERY_DEPTH` 嘅 docstring 仲寫埋「5 leaves one level of headroom for a new nested field without reopening the limit question」，解釋咗點解係 5 而唔係 4 —— 呢個係 WHY comment 嘅正確示範。
- **`build_schema()` 用 lambda 工廠而唔係 extension 實例**，並且留咗一行 comment 講明「Factories, not instances: Strawberry builds a fresh extension per request」。共用 extension 實例喺 concurrent 請求下會出現 state 洩漏，呢個係文件深處先講嘅細節。
- **`_graphiql_enabled()` 嘅實作穩陣**：`os.environ.get(GRAPHIQL_ENV, "").strip().lower() in {"1","true","yes","on"}` —— allowlist 而唔係 `bool(os.environ.get(...))`，所以 `RUN365_GRAPHIQL=0` 同 `RUN365_GRAPHIQL=false` 都正確地係「關」（用 truthy 判斷嘅話 `"0"` 會變成開）。而且 default 係關 —— fail-closed。
- **`test_track_does_not_materialise_every_stored_point` 係一個好測試**：用 `event.listen(session, "loaded_as_persistent")` 數實際 hydrate 咗幾多個 ORM object，`assert len(loaded) <= 2 * points`。呢個直接驗證咗 ticket 嘅**真正目標**（唔好 materialise 219k 個 object），而唔係只驗證回傳行數。回傳行數用純 Python downsample 都做得到，只有呢個 assert 分辨到「SQL 側真係瘦咗」。
- **Vercel entry 嘅測試用 `importlib.util.spec_from_file_location` 真正載入 `api/graphql.py`**，而唔係 mock。`api/` 唔喺 package 入面（ruff 有 `"api/*" = ["D"]` 嘅 per-file-ignore），好容易變成零測試地帶；呢兩個測試（default 關 / env 開）令 Vercel 入口點真係有 gate。
- **`schema.py` coverage 100%**，`service.py` 95%（未覆蓋嘅四行全部係 pre-existing 嘅 date filter 分支，唔係 AU-001 新增嘅）。呢個係四個 commit 入面測試最扎實嗰個。

---

## 修正優先順序

| 次序 | ID | 位置 | 為何排呢個位 |
|---|---|---|---|
| 1 | **W-004** | `base.py:124-127`、`kml.py:71` | 唯一一條喺真實數據上命中嘅 finding（8/365 檔案）；ticket 核心目標未達成 |
| 2 | **W-005** | `base.py:70-73` | 新代碼違反自身合約，其中 `inf` 會衝穿 `parse_all` 拉冧整個 export；修正兩行 |
| 3 | **W-008** | `api/graphql.py:18-20` | 文件斷言咗唔存在嘅安全控制，會誤導下一個 reviewer；改文字即可 |
| 4 | **W-002** | `pages.yml:15-18` | AU-004 令「merge = 部署」，PR run 可以取消進行中嘅 production deploy |
| 5 | **W-006** | `kml.py:152-168` | 靜默錯數風險（今日 latent），同 TCX 政策不一致 |
| 6 | **W-007** | `base.py` + 三個 parser | 新錯誤處理邏輯零覆蓋；補咗會即刻捉到 W-005 |
| 7 | **W-009** | `schema.py:32-38` | 加多一年數據就靜默截斷，冇任何訊號 |
| 8 | **W-010** | `service.py:94-116` | 合約（"evenly"）同實作不符；今日前端唔受影響 |
| 9 | **W-001** | `docs/architecture.md:142` | 文件 drift，一行 |
| 10 | **W-003** | `time.py:41`、`test_common_time.py:60` | 開 follow-up ticket 並引用編號 |
| 11 | S-001 … S-013 | 見各節 | 可選，唔阻 merge |

**建議分批**：1–3（一個 commit 一個 review item）→ 重跑 `/review`；4–8 可以同一輪處理；9–11 併入下一次文件 pass。

> 依 `sw-ticket-management`：每個 review item 一個獨立 commit，訊息格式 `fix: W-004 | 分離刻意 skip 同數據錯誤嘅例外類型`。唔可以將多個 item 併成一個 commit。

---

## 修訂後代碼

**唔提供整檔重寫。** 我係 reviewer，本次 review 嘅邊界明確禁止改動任何 source code，而 10 個 Warning 分佈喺 8 個檔案、當中 4 個（W-004 / W-006 / W-009 / W-010）各有兩個方案需要團隊揀方向 —— 由 reviewer 單方面出一份「修訂後完整代碼」會等於代替 developer 落決定。

每個 finding 之下已經附咗可以直接貼落去嘅修正片段，包含 file:line 定位、方案 A / B 嘅取捨，同推薦。Developer 揀好方案之後照住實作即可。

---

## Handoff receipt

```handoff-receipt
protocol: 1
status: warn
score: 84/100
hard_gates:
  lint: pass
  type_check: n/a
  tests: pass
  coverage: "80%"
next_action: invoke_developer
next_agent: backend-developer
branch: "claude/stoic-ritchie-rkynjn"
context: "P0 batch (AU-001/002/003/004) 4 fix 全部 hard gate 綠、0 Critical，但 10 個 Warning，84/100 未達 90 DoD。AU-003 (93) 可收貨；AU-004 (88)、AU-001 (81)、AU-002 (75) 需修。最高優先 W-004：真實數據 8/365 個 KML 仍然被 DEBUG 級別靜默丟棄，AU-002 核心目標未達成。"
blockers:
  - "W-004 base.py:124-127 + kml.py:71 — ValueError 一桶裝『刻意 skip』同『數據錯誤』兩種語義並記 DEBUG；實測 8/365 真實 KML（Track Points folder 為空）被靜默丟棄，訊息讀落似正常年份過濾。裁決：DEBUG 理由接受、實作推翻 — 需引入 ActivitySkipped 專屬例外，泛型 ValueError 回復 WARNING，並拆開 kml.py:71 的 act_time is None / year 兩種語義"
  - "W-005 base.py:70-73 — optional_int 違反 docstring：'nan' raise ValueError（落 DEBUG 桶靜默丟 activity）、'inf' raise OverflowError（parse_all 不 catch，整個 run365-export 崩潰，AU-004 之後即 production Pages 部署失敗）。加 math.isfinite guard"
  - "W-008 api/graphql.py:18-20 — docstring 及 commit message 宣稱關 GraphiQL 同時關 introspection；實測 graphiql=False 下 POST { __schema } 仍回 200 完整 schema。修正措辭或加 NoSchemaIntrospectionCustomRule"
  - "W-002 .github/workflows/pages.yml:15-18 — concurrency group 為常數 'pages' + cancel-in-progress: true；AU-004 令 merge to master 即 production 部署，PR 的 CI run 會取消進行中的部署"
  - "W-006 kml.py:152-168 — lap Time/Distance 用 row.get 當 optional，TCX 同義欄位用 required_text；缺一個 lap 會靜默少計 total_sec/distance_km/pacing。真實 2592 lap row 全部齊全，屬 latent，且該路徑零測試"
  - "W-007 base.py:55,66-67,121,124-127 + 三個 parser 所有 ActivityParseError raise site 未覆蓋 — 新增的三段分級 except 只有 ET.ParseError 一段有測試；86% 覆蓋率係假象，W-005 就係喺呢個盲區存活"
  - "W-009 schema.py:32-38 — 四個 list field 靜默截斷於 limit=500，無 totalCount/hasNextPage，前端不傳 limit；DB 超過 500 行即靜默渲染不完整數據"
  - "W-010 service.py:94-116 — SQL stride + 二次 Python downsample 令間距不均（實測 total=600/points=7 得 0,85,255,340,425,595,599，gap 85/170/85/85/170/4，舊行為均勻 100），與 field description 的 'evenly' 矛盾。數量永遠準確不會少過 points；前端要求 600 = 存儲上限，故 production 不受影響"
  - "W-001 docs/architecture.md:142 — 仍寫 CI 喺 develop 觸發，與 commit message『docs brought in line』矛盾；同段 test count 93 應為 133"
  - "W-003 time.py:41 + test_common_time.py:60 — 刻意釘死錯誤 epoch-ms 語義的測試，其『tracked separately』所指的 ticket 並不存在（.proj-docs 無 ticket 目錄，審計 AU-003 段亦未涵蓋）；需開 follow-up ticket 並喺 comment/docstring 引用編號"
```
