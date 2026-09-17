# Batch Review — 2026-09-17 — CUI-0012 / 0013 / 0008 / 0041

> ⚠️ **ID 已於 merge 時重新編號。** 三個 reviewer 並行執行，兩個同時由 `S-083` 起、兩個同時由
> `W-030` 起。本報告原文用嘅 ID 已按下表全數改寫，正文其餘部分一字未動：
>
> | 原 | 改為 |
> |---|---|
> | W-030 / W-031 / W-032 | **W-031 / W-032 / W-033** |
> | S-083 … S-087 | **S-087 … S-091** |
>
> ⚠️ **W-033（原 W-032，CHANGELOG）嘅前提由 main agent 核實為錯** —— 見報告末尾嘅更正 block。

---


## 整體 verdict

- 涵蓋 commit：`2a52f1e`（CUI-0012）、`9c06a67`（CUI-0013）、`8622c16`（CUI-0008）、`2ba8f1d`（CUI-0041）
- Base：`6576e56` → HEAD `06ad4bd`
- 整體 score：**80 / 100**
- Status：⚠️ **warn**（hard gates 全 pass、0 🔴 Critical，但 3 個 🟡 Warning）

### Hard Gates（全部我親手跑過）

| Gate | 指令 | 實測 | 結果 |
|---|---|---|---|
| Lint (py) | `ruff check src tests` | `All checks passed!` | ✅ pass |
| Format (py) | `ruff format --check src tests` | `60 files already formatted` | ✅ pass |
| Lint (fe) | `npm run lint` | eslint 零輸出 | ✅ pass |
| Type check | `npm run typecheck`（先跑 codegen） | 零錯誤 | ✅ pass |
| Tests (py) | `pytest tests -q` | **492 passed** in 61s | ✅ pass |
| Tests (fe) | **`npm test`** | **141 passed / 25 files** | ✅ pass |
| Coverage | `pytest --cov=src` | **TOTAL 96%** | ✅ pass（≥ 80） |
| SDL | `run365-schema --check frontend/schema.graphql` | `up to date` | ✅ pass |
| No Critical | — | 0 | ✅ pass |
| Security scan | 四個 in-scope commit 嘅 file list | 零 `pyproject.toml` 改動、零新依賴 | n/a |

Coverage 細項全部對得上你俾嘅數：`cli/collect_weather.py` **98%**（唯一未覆蓋係 line 151 `main()`，喺 `if __name__` 之下）、`_parsing.py` / `hko_daily.py` / `hourly.py` / `warnings.py` / `common/geo.py` / `common/numeric.py` **全部 100%**。

---

## 第一部分 —— 十一條自報推翻嘅獨立裁決

我冇信 lane 任何一句，每條都獨立重做。結論：**11 條全部成立**，但其中 **3 條嘅「解釋」或「方法」需要補注**，而我喺重做途中另外揾到 **1 個 lane 冇報嘅缺口**（見 W-031）。

### Lane B

**① 行號漂移 —— ✅ 成立**

`git grep -n '\.find(' 6576e56` 實測：

```
hourly.py:67:   script = str(tds[9].find("script").string)
hourly.py:68:   desc_key = script[script.find("n(") + 2 : script.find(", 'CurrentWeather")]
hourly.py:75:   tds[3].text[tds[3].text.find("°") + 1 :]...
warnings.py:56: bs = BeautifulSoup(html[html.find(marker) + len(marker) :], "html.parser")
warnings.py:73: icon = tds[0].find("img").get("src", "")
```

票寫 `:57` / `:55` / `:70`，實際 `:67` / `:56` / `:73`。Lane 講啱。

**補注（lane 冇講）**：base 其實有 **5 處** `.find()`，唔係 4 處。第 5 處係 `hourly.py:75` 嘅 `tds[3].text.find("°") + 1`。呢處係**良性**嘅：`-1 + 1 == 0`，slice 由 0 開始即係保留成個字串，所以「冇度數符號」嗰個 variable-direction 格式一直都答啱。Lane 冇將佢當缺陷係正確判斷，但佢一樣重構咗（`_wind_speed_kmh` 改用 `if _BEARING_SEPARATOR in cell_text`）—— 呢個重構引入咗一個未被釘死嘅行為改動，見 **S-087**。

**② `31` 嘅算術啱但描述差一位 —— ✅ 成立**

我自己數：`len("Tropical Cyclone Warning_Signals") == 32`，`-1 + 32 == 31`。`html[31:]` 由**索引** 31 開始 = 1-based 嘅**第 32 個**字元。Lane 對票上描述嘅更正正確。

**③ 🔴 原有 fixture 證明唔到缺陷 —— ✅ 成立，而且係本輪最有價值嘅發現**

我獨立重做：攞 `hko_warning_day.html`，`.replace(marker, "", 1)`，再用 offset 31 parse：

```
== original fixture, marker present ==  offset 302, rows 3
    ['', 'RED FIRE DANGER WARNING']
    ['', 'COLD WEATHER WARNING']
    ['', 'FROST WARNING']
== original fixture, marker REMOVED ==  find() -1, bad offset 31
    ['', 'RED FIRE DANGER WARNING']
    ['', 'COLD WEATHER WARNING']
    ['', 'FROST WARNING']
IDENTICAL to correct parse? True
```

**錯位 parse 返一模一樣嘅 3 條正確記錄。** 原 fixture 客觀上證明唔到缺陷。

我再驗埋 lane 講嘅機制（佢冇講錯，但講得太簡）：

```
s[26:34] = '>\n      '          ← index 31 落喺誘餌 <table> 開 tag 之後嘅空白
tables found: 1
ALL <tr> anywhere: 6
<tr> inside a <table>: 5         ← 誘餌行變咗孤兒
orphan decoy present in text? True
```

即係：offset 31 啱啱斬走咗誘餌 `<table>` 嘅**開 tag**，令誘餌 `<tr>` 孤兒化；而 `for table in bs.find_all("table") for tr in table.find_all("tr")` 呢個 loop 見唔到孤兒 `<tr>`。真正嗰個 table 完好無缺 → 3 條正確記錄。純屬 marker 長度同 fixture 排版嘅巧合。

**所以 `hko_warning_day_no_marker.html` 唔係多餘，係必要嘅。** 我驗過新 fixture：offset 31 落喺 `<title>HKO Warning Database</title>` 中間，兩個 table 完好 → parse 出 **1 條 `ROW FROM AN UNRELATED TABLE`**，即一條「貌似合理但完全錯」嘅記錄。呢個先真正示範到靜默錯。

**④ 第四處 `-1` 算術（icon code）—— ✅ 成立，同一類問題**

`hourly.py:68`。我用真值表驗：

```
'drawIcon(7)'  →  find("n(")=7, find(", 'CurrentWeather")=-1
                  舊 slice = s[9:-1] = "7"  →  _DESCRIPTION_MAP["7"] = "Rain"
```

零證據之下叫咗個名出嚟。同 `warnings.py` 嗰個 marker 完全同一類：**唔 raise，但答錯**。Lane 講啱。

**⑤ `_icon_code` mutant —— ✅ 成立，但 lane 只做咗一個方向，另一個方向 survive**

我重做三個方向（in-process `setattr` + oracle 確認 mutant 真係生效）：

| Mutant | oracle（`"drawIcon(7)"`） | 結果 |
|---|---|---|
| M1 兩個 guard 都刪（= 舊算術） | `'7'` vs 真實 `None` | ✅ **KILLED**（2 fail） |
| M2 只刪 `end < start` | `'7'` vs `None` | ✅ **KILLED**（2 fail） |
| M3 **只刪 `start < 0`** | `None`（呢個 probe 上相同） | ❌ **SURVIVED（492 全綠）** |

Lane 講「補 `freemeteo_unexpected_script.html` 之後先殺到」—— 對 M1 / M2 成立，`test_a_script_that_is_not_the_documented_call_reads_unknown` 同 `test_the_old_icon_arithmetic_would_have_named_a_description` 兩條都紅。但 **M3 唔係等價 mutant**，我用其他輸入驗到真嘅行為差異：

```
'x26y'  →  真實 _icon_code=None → 'Unknown'   |  M3 → '26' → 'Snow'
'a1b'   →  真實 None → 'Unknown'              |  M3 → '1'  → 'Clear weather'
```

即係 `start < 0` 嗰半可以刪咗而 492 條測試一樣全綠 —— **正正就係 lane 為另一半寫測試去防嘅嗰種情況**。Lane 聲稱「5/5 killed」唔準確。→ **W-031**

**⑥ 票上「覆蓋率 0%」過時，base 實際 45% —— ✅ 成立**

我 `git archive 6576e56` 出一棵完整 base tree、開獨立 venv、跑完整 suite：

```
src/cli/collect_weather.py   33 stmts   18 miss   45%   (missing 42-70, 74)
418 passed        TOTAL 95%
```

**45%，唔係 0%。** Lane 講啱。

**額外收穫**：同一次量度顯示 base `src/common/numeric.py` **94%，唯一未覆蓋係 line 85** —— 即 `finite_float` 個 `return finite(to_float(value))`。呢個係第⑪條（零 caller）嘅一個完全獨立嘅佐證。

**⑦ `_REQUEST_TIMEOUT` 收歸 `config.py` —— ✅ 結論成立，但推理有一處唔精確**

Lane 話「`weather/` 本身已經 `from run365days.common.numeric import to_float`」。我驗 base：

```
6576e56:src/weather/collectors/hko_daily.py:8:from run365days.common.numeric import to_float
6576e56:src/weather/collectors/hourly.py:7:from run365days.common.numeric import to_float
6576e56:src/weather/models.py:23:from run365days.common.numeric import to_float
```

**`warnings.py` 唔喺列表入面** —— 佢 base 時完全冇 import `common`。所以嚴格講，呢個改動確實令 `warnings.py` 呢個 module 多咗一層佢本來冇嘅 cross-layer import。但喺**層**嘅粒度上 lane 嘅結論企得住：`weather → common` 呢個方向本身就係既定架構，多一個 module 用唔構成違規。而且 `common/config.py` 只 import `os` / `pathlib`（我讀過），零重量；CLAUDE.md §5 更加明文寫「`src/common/config.py` 收納所有 path 同共用常數」—— 放呢度係**跟足項目規範**，唔止係「唔算違規」。裁決：**推理需要補一句，結論正確且有 CLAUDE.md 背書。**

「一條測試都唔使改」：✅ **屬實**。base 有 7 處 `assert_bounded_timeout` 呼叫，diff 入面一處都冇郁過，helper 本身（`tests/test_weather_collectors.py:76`）亦 byte-for-byte 未改。

### Lane C

**⑧ 票寫「只出 `RuntimeWarning`」只啱一半 —— ✅ 成立**

我喺 base tree（pre-fix 代碼、numpy 2.4.6）實測：

```
+inf lat  -> nan   warnings=2  ['RuntimeWarning: invalid value encountered in sin',
                                'RuntimeWarning: invalid value encountered in cos']
-inf lat  -> nan   warnings=2
nan lat   -> nan   warnings=0     ← 一個都冇
nan lon   -> nan   warnings=0     ← 一個都冇
```

`nan` 座標**完全靜默**。`np.sin(nan)` 唔 set invalid flag，lane 講啱。

順手驗埋 `total_track_distance` 嘅 pre-fix 行為，同新測試 comment 講嘅完全一致：

```
nan 點: (0.0, 2)  warnings=0
inf 點: (0.0, 2)  warnings=4
None 點: (0.0, 2)                  ← 室內語意，刻意保留
```

**⑨ 「檢查要向量化」嘅前提唔成立 —— ✅ 結論成立，方法我親身踩過個坑再驗返**

我第一次做 A/B **量到 +21% / +18%**，同 lane 講嘅 ±0.3% 完全唔夾。查落係**我自己嘅方法錯**：我 arm B 用咗 `arcsin(sqrt(a))`，而真實實作係 `arctan2(sqrt(a), sqrt(1-a))`（多一個 sqrt + arctan2）。兩條 arm 唔止差個 guard。

用 `inspect.getsource` 抽真實 source、**只**剔走 `if not (...): raise ValueError` 嗰幾行、再 exec，令兩條 arm 除咗 guard 之外 byte-for-byte 相同，另加 oracle（guarded → `ValueError`；noguard → `nan`；好輸入答案相同）同 warm-up：

```
N=  3600  guarded= 34.2ms  noguard= 33.9ms  delta=+0.78%  | arm 內 spread A=4.3% B=5.0%
N= 10000  guarded= 93.4ms  noguard= 94.0ms  delta=-0.66%  | arm 內 spread A=2.6% B=4.3%
```

**方向相反，兩個都細過 arm 內 spread** —— 完全重現 lane 嘅定性結論（佢 ±0.3% / ±6%，我 ±0.7% / 2.6–5.0%，同一數量級，同一結論）。

`timeit` 上界我亦獨立量：

```
guard expression : 58.1 ns
haversine call   : 3726.1 ns  → guard = 1.56% of one call        （「low single-digit %」✅）
per-segment      : 8941.2 ns  → guard = 0.650% of one segment    （「under one percent」✅）
```

**0.650% 同 lane 講嘅 0.6% 對得上。** 裁決：方法（fresh process / 交錯 / median）同結論都成立 —— 而且我自己嗰次失敗正好證明點解「兩條 arm 必須只差一樣嘢」呢個設計要求唔係多餘。

**⑩ 🔴 推翻 main agent 寫嘅 CUI-0041 票 —— ✅ 成立，我用兩個 mutant 重做**

先靜態讀：`records.py:166-171` 有 6 個 `finite()`（max/mean/min temp、humidity、rainfall、wind）；`builder.py:150-152` 有 3 個（temp、hum、wind）。6 + 3 = 9。

再動態驗（`finite` 換成 passthrough，oracle 確認 `mutant(inf) == inf`）：

| 中和邊個 | 紅咗嘅 CUI-0009 測試 |
|---|---|
| `export.records.finite` | `test_daily_weather_readings_become_none_when_not_finite[**max_temp_c / avg_temp_c / min_temp_c / humidity_pct / rainfall_mm / wind_kmh**]` = **6 個 daily** |
| `dashboard.builder.finite` | `test_nested_hourly_weather_becomes_none_when_not_finite[**temp_c / humidity_pct / wind_kmh**]` = **3 個 hourly** |

票寫「今日守住 CUI-0009 性質嘅係 `daily_weather_record()` 嘅 `finite()`」**只啱 9 個入面嘅 6 個**。Lane 嘅推翻完全成立，而且佢已經同步更正咗 `tests/test_weather_models.py` 個 class docstring 同 `.proj-docs/tickets.md`。

**⑪ 毒藥 mutant 驗零 caller —— ✅ 成立；方法本身有兩個真盲點，但 lane 已經補咗**

我喺 base tree 跑毒藥（`numeric.finite_float` 換成一叫就掟 `AssertionError`，oracle 確認毒藥真係會燒）：

```
oracle: poison fires -> finite_float was called -- it is NOT dead code
418 passed
### poison finite_float on BASE: rc=0 -> SURVIVED (zero callers)
```

**方法嘅盲點（裁決你問嘅嘢）——毒藥單獨用係唔夠嘅：**

1. **`from ... import finite_float` 會綁一個獨立名。** 咁樣嘅 caller 喺 import 果刻已經攞咗原函數嘅 reference，之後 `setattr` 落 `numeric` module 佢**永遠唔會撞到毒藥** → mutant 假 survive → 假結論「零 caller」。
2. **測試套件行唔到嘅行。** Base 有 `cli/export_schema.py` **0%**、`cli/process_activities.py` **56%**、`cli/export_data.py` 77%、`weight/analysis.py` 80%。`finite_float` 若果出現喺任何一條未覆蓋嘅行，毒藥一世唔會燒。

兩個盲點都**只能靠靜態搜尋去封**。我行咗：`git grep -n "finite_float" 6576e56` 喺 `src/` + `tests/` 除咗佢自己個定義（`numeric.py:69`）同散文提及之外**零命中**（`parse_finite_float` 係另一個名，喺 `activities/parsers/` 真係用緊，已排除）。

第三重獨立證據：base coverage 顯示 `numeric.py` line 85（即 `finite_float` 個 body）**從未執行**。

**三重交叉驗證之下結論成立**。而睇 `CUI-0041.md` 嘅 log（line 77 / 80），lane **自己都係 grep + 毒藥兩者並用**，所以佢方法上冇漏。裁決：**結論正確，方法正確，但「毒藥係最強嗰個」呢個講法要打個折 —— 真正封死盲點嘅係靜態 grep，毒藥只係補咗「動態 / getattr 呼叫」呢一小塊。**

---

## 第二部分 —— 靜默錯：每條新測試分唔分得出「答啱」同「答錯」

呢個係你點名嘅核心。逐條驗：

| 靜默錯 | 有冇測試分得出「答錯」（唔止「唔 raise」）？ |
|---|---|
| `warnings.py` 個 `-1` | ✅ **兩條配對**。`test_a_page_without_the_marker_raises_instead_of_parsing` 管「而家 raise」；`test_the_offset_a_missing_marker_used_to_yield_parses_the_wrong_table` 直接喺測試入面重演舊算術，`assert len(wrong_rows) == 1` + `assert "ROW FROM AN UNRELATED TABLE" in ...` —— **明文釘死「舊路會答出一條錯記錄」**。我 M4 mutant 確認前者會殺。做得好。 |
| `hourly.py` icon code | ⚠️ **一半 OK 一半唔 OK**。`test_the_old_icon_arithmetic_would_have_named_a_description` 釘死咗 `"7" → "Rain"` 呢個錯答案（好），`test_a_script_that_is_not_the_documented_call_reads_unknown` 釘死咗 end 嗰半。但 **`start < 0` 嗰半冇任何測試**，M3 survive → **W-031**。 |
| `nan` 座標 | ✅ `test_non_finite_point_is_rejected_instead_of_summing_to_zero` 嘅 comment 明寫「pre-fix 答 `(0.0, 2)`」，我實測確認咗呢個數。`test_guard_fires_before_numpy_sees_the_value` 用 `simplefilter("error")` 把 warning 升級成錯誤 —— 呢條設計得好：inf 個 case 會變 `RuntimeWarning`、nan 個 case 會**乜都唔發生就 return**，兩邊都唔會係 `ValueError`，所以 guard 一刪即紅。 |
| 單位剝離（wind） | ❌ **完全冇釘死**，見 **S-087**。 |

---

## 第三部分 —— 行為改動點名

### 改動 1：`warnings.fetch_day` / `fetch_range` 新增 `WeatherPageStructureError`

- **下游**：`git grep` 確認 `src/` 入面唯一 caller 係 `cli/collect_weather.py`，而佢已經喺 `_COLLECTION_FAILURES` 入面接住。`hourly` / `hko_daily` 唔受影響。
- **爆唔爆**：repo 內部**唔會爆**。
- **但**：見 **W-032** —— 呢個新嘅 public 失敗模式，型別只能由 `_parsing` 呢個**私有 module** import。
- **CHANGELOG**：應該入。見 **W-033**。

### 改動 2：`run365-weather` exit code 語義

由「一個 source 死 → uncaught exception traceback + exit 1」變成「logged WARNING + 繼續跑其餘 source + exit 1」。

- **下游**：我 grep 咗 `.github/` 同 `scripts/` —— **`run365-weather` 完全冇被任何 CI workflow 或 script 呼叫**，只喺 `docs/data-pipeline.md` 同 `docs/CHANGELOG.md` 提及。所以**零自動化消費者，唔會爆**。
- **實質改善**：`run()` 回傳失敗數目、`main()` 見非零就 `SystemExit(1)`，令「收集到零樣嘢」唔會被讀成「全部成功」。`test_exits_non_zero_when_a_source_could_not_be_collected` / `test_exits_zero_when_every_source_was_collected` 兩邊都釘死咗。
- **CHANGELOG**：**應該入**（對人手跑呢條命令嘅人係可見嘅語義改變）。見 **W-033**。

### 改動 3（lane 冇點名，我加）：`haversine_distance` 新增 `ValueError`

- **可達性**：我追齊三個 parser。`gpx.py:124-125` 同 `kml.py:160-161` 用 `parse_finite_float`（非有限即 `ActivityParseError`）；`tcx.py:134/137` 用 `optional_float`，而 `base.py` 入面 `optional_float` 最後一行係 `return value if math.isfinite(value) else None`。**三條路都喺 geo 之前已經擋咗** → 呢個 `ValueError` 喺現行 pipeline **實際上不可達**，係 public entry point 上嘅縱深防禦。docstring 自己都咁講（"the only way a non-finite pair still reaches this code"），誠實。
- **`lat_inf_2102.gpx` 嘅注腳**：`tests/test_activities_parsers.py:431` 顯示呢個 fixture **今日已經係喺 GPX parser 度就掟 `ActivityParseError`**。geo.py docstring 講「`is what made` ... report `0.0`」係用過去式講 CUI-0001 之前嘅歷史，技術上冇講錯，但讀落容易誤以為係描述今日。
- **爆唔爆**：**唔會**。`parse_all` 有 `except ValueError` 分支（`base.py:207`）會 log WARNING 再跳過該檔。
- **CHANGELOG**：可入可唔入（不可達）。連同 `finite_float` 被刪（一個 public helper 由 `run365days.common.numeric` 消失）一齊記一句就夠。

---

## 第四部分 —— 問題清單

### 🔴 Critical

**無。**

### 🟡 Warning

---

#### W-031 — `_icon_code` 個 `start < 0` guard 可以刪咗而 492 條測試全綠

- **位置**：`src/weather/collectors/hourly.py:63-83`（`_icon_code`）；缺失嘅測試屬於 `tests/test_weather_collectors.py::TestHourlyFetchDay`
- **描述**：`_icon_code` 兩個 `str.find()` 各自可以回 `-1`，代碼用 `if start < 0 or end < start: return None` 一次過擋。我 in-process mutation（oracle 確認 mutant 生效）測到：
  - 刪 `end < start` → **KILLED**（2 條紅）
  - 刪 **兩個** → **KILLED**（2 條紅）
  - **只刪 `start < 0` → SURVIVED，492 全綠**
- **佢唔係等價 mutant**（我另外驗過真實行為差異）：

  ```
  'x26y' → 真實 None → "Unknown"  |  無 start<0 → "26" → "Snow"
  'a1b'  → 真實 None → "Unknown"  |  無 start<0 → "1"  → "Clear weather"
  ```

  當 `start == -1` 而 `end == -1`，`end < start` 係 `-1 < -1` = `False`，guard 放行，slice 變成 `t[1:-1]`。
- **影響**：CUI-0012 引入呢個 guard 就係為咗消滅「唔 raise 但答錯」。而家佢自己一半冇被釘死 —— 將來任何人重構、簡化或者「清理」呢個條件，CI 唔會出聲。呢個正正係 lane 為另一半寫 `test_the_old_icon_arithmetic_would_have_named_a_description` 去防嘅情況。Lane 報「5/5 killed」唔準確。
- **方案 A（推薦）**：喺現有 `test_the_old_icon_arithmetic_would_have_named_a_description` 隔籬加一條純函數測試，唔使新 fixture：

  ```python
  def test_a_script_missing_the_call_prefix_is_not_read_as_a_code(self):
      # 對稱於上面嗰條：呢次係 *prefix* 唔見咗。start 同 end 一齊係 -1，
      # `end < start` 係 False，所以單靠佢會放行 s[1:-1] —— 喺呢個字串上
      # 啱啱好落喺 "26" 上面，無中生有噉叫咗一場雪出嚟。
      assert hourly._icon_code("x26y") is None
      assert hourly._DESCRIPTION_MAP["26"] == "Snow"   # 釘死呢個錯答案真係叫得出名
  ```

  - 成本：4 行，零 fixture，零 I/O。
- **方案 B**：改寫成 `_ICON_CALL_PREFIX in script_text and ...`，令兩個條件變成一個表達式，減少可獨立刪除嘅分支。
  - 缺點：改動已 review 過嘅生產代碼去遷就測試缺口，方向倒轉；而且 `in` + `find` 會掃兩次。
- **推薦：方案 A。** 缺口喺測試唔喺代碼，補測試就夠。

---

#### W-032 — 一個 public 失敗模式，型別只能由私有 module import

- **位置**：`src/weather/collectors/_parsing.py:14`（`WeatherPageStructureError`）、`src/weather/collectors/warnings.py:64-67` 同 `:127-130`（public docstring `Raises:`）、`src/cli/collect_weather.py:24`
- **描述**：`warnings.fetch_day` / `fetch_range` 兩個 public function 嘅 docstring 都正式宣告 `Raises: WeatherPageStructureError`。但呢個 exception class 住喺 `_parsing.py` —— leading underscore = 私有。想接呢個 exception 嘅人唯一途徑係：

  ```python
  from run365days.weather.collectors._parsing import WeatherPageStructureError
  ```

  `cli/collect_weather.py` 而家就係咁做緊：一個 package（`cli`）伸手入另一個 package（`weather.collectors`）嘅私有 module。我確認過 `src/weather/collectors/__init__.py` 得一行 docstring（43 bytes），冇 re-export。
- **影響**：契約同可達性自相矛盾 —— 一個唔應該被外面 import 嘅名，係外面唯一接得住呢個錯嘅方法。將來重整 `_parsing.py`（重命名、拆檔、變成真正內部用）會靜靜哋拆爛 `cli` 同任何第三方 caller，而 `_` 前綴本身就係喺話「呢度可以隨便改」。
- **方案 A（推薦）**：喺 `src/weather/collectors/__init__.py` re-export：

  ```python
  """Scrapers for online weather sources."""

  from run365days.weather.collectors._parsing import WeatherPageStructureError

  __all__ = ["WeatherPageStructureError"]
  ```

  然後 `cli/collect_weather.py` 改成 `from run365days.weather.collectors import WeatherPageStructureError`。helper function 繼續私有（佢哋真係只有 collector 用），淨係個 exception 升做 public —— 因為只有佢出現喺 public `Raises:` 裏面。
  - 成本：3 行 + 1 行 import 改動，零行為改動。
- **方案 B**：整個 `src/weather/collectors/errors.py`（無底線），exception 搬過去，`_parsing.py` import 返。
  - 優點：「public 契約」同「內部 helper」喺檔案層面分得更死。
  - 缺點：多一個 module；而個 exception 嘅 docstring 明文解釋緊點解佢係 `RuntimeError` 唔係 `ValueError`（因為同一頁嘅 `float()`/`strptime()` 會掟 `ValueError`），呢段理由同 `section_after` 擺埋一齊讀先順。
- **推薦：方案 A。** 改動最細，`_parsing` 內聚性保住，而個 `__all__` 令「邊個名係契約」一目了然。

---

#### W-033 — 兩個 public 行為改動 + 一個被刪 public helper，`docs/CHANGELOG.md` 一隻字都冇

- **位置**：`docs/CHANGELOG.md`（本次 range 未改動）
- **描述**：`git diff --stat 6576e56..06ad4bd` 嘅完整 file list 入面**冇 `docs/CHANGELOG.md`**。`grep "CUI-0008\|CUI-0012\|CUI-0013\|CUI-0041" docs/CHANGELOG.md` → **零命中**。

  而同一個 session 嘅**兄弟 lane 有寫**：`[3.2.0] - 2026-09-16` 呢個 section 已經收錄咗 `CUI-0035`–`CUI-0039`（即 `7f25812` 同 `06ad4bd` 兩條 lane）。所以呢個唔係「項目唔寫 CHANGELOG」，係**呢兩條 lane 漏咗**。

  未記錄嘅係：
  1. `run365-weather` exit code 語義改變（人手跑呢條命令嘅人可見）
  2. `warnings.fetch_day` / `fetch_range` 新 public 失敗模式 `WeatherPageStructureError`
  3. `run365days.common.numeric.finite_float` 由 public API 消失
  4. `haversine_distance` / `total_track_distance` 新增 `ValueError`
- **影響**：CLAUDE.md §7 將 `docs/CHANGELOG.md` 列為項目技術文件 SSoT。呢四樣嘢喺呢批之後淨係存在於 `.tickets/` 同 source comment 入面 —— 對一個由 `develop` 直接 deploy（Vercel + Pages）嘅 repo 嚟講，release note 同實際部署行為就開始分岔。
- **方案 A（推薦）**：喺 `[3.2.0]` section 補 entry。`### Changed` 落 (1)（唔係 Breaking —— repo 內零自動化 caller，我 grep 過 `.github/` + `scripts/`）；`### Fixed` 落 (2) 同 (4)；`### Removed` 落 (3)。
- **方案 B**：留到 release 時一次過補。
  - 缺點：同項目自己啱啱示範嘅做法（同 session 兄弟 lane 已經即時寫咗）唔一致，而未寫低嘅行為改動放得愈耐愈易漏。
- **推薦：方案 A。**

### 🟢 Suggestion

---

**S-087 — 單位「按名剝離」呢個改動，source comment 有聲稱但零測試釘死**

- **位置**：`src/weather/collectors/hourly.py:41-44`、`:85-97`（`_wind_speed_kmh`）
- `hourly.py` 有段 comment 明文聲稱：*"Units freemeteo appends to the numbers, stripped by name so that a cell which arrives without its unit keeps its digits instead of losing its last few."*
- 我 mutation 驗呢句：把 `removesuffix(_WIND_SPEED_UNIT)` 改返舊嘅 `[:-5]` 定長切片 →

  ```
  oracle 'Northeast 50° 24'       : 真實 -> 24.0  | mutant -> None      ← 真差異
  oracle 'Northeast 50° 24 Km/h'  : 真實 -> 24.0  | mutant -> 24.0
  ### mutant _wind_speed_kmh -> old [:-5] slice: 492 passed -> SURVIVED
  ```

  我 grep 過全部 `tests/fixtures/weather/*.html`，**五個 wind cell 全部都帶住 ` Km/h`**，所以冇任何測試去到「冇單位」嗰條路。
- 同 W-031 同一類：代碼啱、comment 講明佢點解啱、但**冇嘢證明佢啱**，而呢個正正係你講嘅本 repo 反覆出現嘅缺陷類別。
- **方案 A（推薦）**：加一條純函數測試，零 fixture：`assert hourly._wind_speed_kmh("Northeast 50° 24") == 24.0`（外加 `"Variable at 20"`）。
- **方案 B**：`freemeteo_day.html` 加一行冇單位嘅 wind cell。成本較高，而且會令個 fixture 同時擔負兩個用途。
- **推薦：方案 A。** 溫度（`_TEMPERATURE_UNIT`）同濕度（`_HUMIDITY_UNIT`）有同一個缺口，同一條測試順手一併蓋埋。

---

**S-088 — `_parsing.py` 顯示 100% coverage，但兩個防禦分支冇被釘死**

- **位置**：`src/weather/collectors/_parsing.py:74-78`（`child_attr` 嘅 `isinstance(value, str)`）、`:86-88`（`child_string` 嘅 `child.string is None`）
- 兩個 mutant 我都跑咗，**兩個都 SURVIVED（492 全綠）**：
  - `child_attr` 剔走 `isinstance(value, str)` 回退 → 全綠
  - `child_string` 剔走 `child.string is None` 檢查 → 全綠
- **注意呢個係 line coverage 嘅盲點示範**：`_parsing.py` 報 100%（18/18 statements），因為呢兩行**係執行緊嘅**，只係從來未試過行去 `else` 嗰邊。
- 兩者風險都低：`src` 唔係 HTML 嘅 multi-valued attribute（所以 `child_attr` 嗰個近乎等價 mutant）；而 `child_string` 就算真係回 `"None"` 字串，落到 `_icon_code` 都會俾 `start < 0` 擋住（**前提係 W-031 修好之後 `start < 0` 仲喺度**）。
- **方案 A（推薦）**：兩條 `_parsing` 直測（一個 `<td class="a b">` 攞 `class`、一個 `<script><b>x</b></script>` 攞 string）。**方案 B**：接受缺口，喺 docstring 註明佢哋係 unreachable-by-design。**推薦 A**，成本 6 行。

---

**S-089 — CUI-0008 個 guard 喺唯一一條真實「壞座標」路徑上射唔到**

- **位置**：`src/activities/parsers/base.py:133-144`（`optional_float`）對上 `src/common/geo.py:63-69`
- GPX / KML 用 `parse_finite_float`（非有限即掟）。**TCX 用 `optional_float`，佢把非有限值轉成 `None`**（W-005 嘅刻意決定）。而 `None` 喺 `haversine_distance` 入面正正係「室內、答 `nan`、靜靜哋跌出個 sum」。
- 即係話：一個 `<LatitudeDegrees>inf</LatitudeDegrees>` 嘅 TCX，**唔會**觸發 CUI-0008 個 `ValueError` —— 佢喺更早已經被洗成 `None`，然後行返 CUI-0008 明文話唔可接受嗰條「壞段變 nan、nan 跌出 sum、caller 攞到少咗嘅距離」嘅路。
- 呢個唔係本次改動整出嚟嘅 bug（W-005 早有定案），亦唔 block merge。但 `geo.py` 個 docstring 話個 guard 係為「corrupt input」而設，而唯一真實會出現 corrupt 座標嘅 parser 永遠去唔到嗰度 —— 值得記低，唔好將來以為呢條路已經封咗。
- **方案 A（推薦）**：開一張 follow-up ticket 評估 TCX lat/lon 應否改用 `parse_finite_float`（同 GPX / KML 睇齊）。**方案 B**：喺 `geo.py` docstring 補一句講明 TCX 呢條路係經 `optional_float` 洗成 `None`。**推薦：A + B 一齊**（B 即刻做，A 排期）。

---

**S-090 — timeout 集中化嘅目的本身冇測試守住**

- **位置**：`src/common/config.py:50-61`、`tests/test_weather_collectors.py:76-82`
- `config.py` comment 講明點解要集中：*"Held here rather than once per collector so retiming them cannot leave one behind (CUI-0013)."*
- 但唯一嘅 timeout 測試 `assert_bounded_timeout` 只 assert `0 < connect <= 10` 同 `0 < read <= 60`。我驗過 5 個 request site 今日全部用緊 `HTTP_REQUEST_TIMEOUT`，但如果邊個 collector 重新引入一個本地 `_REQUEST_TIMEOUT = (3, 20)`，**七條 timeout 測試全部照樣綠** —— 即係「唔會漏咗一個」呢個性質冇人守。
- **方案 A（推薦）**：`assert recorder.calls[0]["timeout"] is config.HTTP_REQUEST_TIMEOUT`（identity，唔係 equality）擺喺三個 collector 各一處。**方案 B**：`config.HTTP_REQUEST_TIMEOUT` monkeypatch 成哨兵值，assert 三個 collector 都見到佢。**推薦 A**，一行，而且 `is` 比較會連「值啱但 source 唔同」都抓到。

---

**S-091 — Commit 歷史冇 TDD Red 階段證據**

- 四張票 = 四個 commit，每個都係 test + 實作一齊入（`2a52f1e` / `9c06a67` / `8622c16` / `2ba8f1d`）。**冇任何「失敗測試先於實現」嘅 commit。**
- 不過我認為證據其實存在，只係唔喺 commit 歷史入面：多條測試嘅 comment 明文記錄咗 pre-fix 量度（"Measured on the pre-fix code: this track answered (0.0, 2)"、"Measured on the pre-fix code: ±inf reached np.sin/np.cos ... while nan answered nan behind *no* warning at all"）。**我獨立喺 base tree 上重跑，兩句都逐字屬實。** 所以係量過先寫，唔係事後補。
- 對比 CLAUDE.md 嗰條「TDD Red 階段」陷阱（唔好 import 未存在嘅 symbol，否則變 collection Error 而唔係 assertion Fail）—— 呢種「喺 base 上實測落 comment」嘅做法其實比一個 Red commit 更頂得住嗰個陷阱。
- **方案 A（推薦）**：接受現狀，但要求以後 review 範圍嘅 ticket log 明文寫一句 TDD 執行說明。**方案 B**：強制 Red commit。**推薦 A** —— 呢度嘅實質證據質素高過形式要求。

---

## ✅ 做得好嘅地方（跨 fix 通用）

1. **`tests/test_cli_collect_weather.py` 個 autouse socket 封鎖**：`monkeypatch.setattr(socket.socket, "connect", refuse)`。呢個係整份 diff 我最欣賞嘅一樣嘢 —— 唔係「我哋 mock 咗 collector」，係「任何未 mock 到嘅出口都會大聲死」。一個 scraper 測試套件應該就係咁寫。

2. **`section_after` 個 `-1` 註釋**：唔止講「`find()` 唔見會回 `-1`」，仲講埋「`-1` 係個完全用得嘅 slice index，所以會由頁面頂附近一個隨機位置開始 parse 而唔係 parse 唔到嘢」。呢句就係成張票嘅心臟，而佢寫喺代碼度，唔係淨係喺 ticket 度。

3. **`test_the_offset_a_missing_marker_used_to_yield_parses_the_wrong_table`**：喺測試入面重演舊算術，再 assert 佢會攞到邊條錯行。冇呢條，guard 可以刪咗而全部嘢照綠。**呢個正正係 W-031 講嘅 icon code 嗰半所欠缺嘅嘢** —— 佢哋自己已經示範咗正確做法，只係未做全。

4. **`WeatherPageStructureError` 繼承 `RuntimeError` 嘅理由**：「同一頁嘅 `float()` / `int()` / `strptime()` 都掟 `ValueError`，所以為一格壞 cell 寫嘅 `except ValueError` 唔應該連『成頁變咗形』都食埋」。同 CLAUDE.md 記低嘅 `MissingTimeZoneDataError` 完全同一個推理模式 —— 呢個 codebase 有一套真正嘅 exception 分類法而唔係求其揀。

5. **`geo.py` 入面連量度方法一齊寫低**：交錯 A/B、fresh process、median of five、外加一個 `timeit` 上界，仲寫低「改形狀之前重跑呢兩個量度」。**我自己第一次重做就因為兩條 arm 唔一致而量錯 +21%** —— 佢寫低嘅嚴謹度係有實際價值嘅。

6. **具名 column 常數 + 綁死 header 嘅測試**：`test_the_column_map_matches_the_header_row_it_names` 把每個 `_*_COL` 綁返去佢聲稱指住嘅 header 文字。咁樣「重新編號但冇搬位」會喺呢度紅，唔會變成靜靜哋讀咗濕度做風速。

7. **`_collect_source` 嘅 stdout / logger 分流**：進度 = 用戶要求嘅實況播報 → stdout；失敗 = 「冇做到咩」嘅紀錄 → logger，同 collector 自己啲 skip warning 同一條流。而 `test_logs_the_source_that_failed_rather_than_printing_it` 同時 assert 咗兩邊（`caplog` 有、`capsys` 冇）。

8. **CUI-0041 嘅自我修正**：lane 唔止刪咗死代碼，仲翻返轉頭改 `tests/test_weather_models.py` 個 class docstring 同 `.proj-docs/tickets.md`，去更正 main agent 票上嗰句錯嘅「gate 全喺 `daily_weather_record()`」。**下游用實測推翻上游而唔係照單全收** —— 呢個係本 session 第六次，亦係最乾淨嗰次（佢用兩個 mutant 量，我重做結果完全一致）。

---

## 評分結果

| 維度 | 得分 | 滿分 | 備注 |
|------|------|------|------|
| 正確性（Correctness） | 24 | 25 | 每一處我 probe 過嘅代碼都答啱。−1：S-089（guard 喺唯一真實壞座標路徑上射唔到） |
| 安全性（Security） | 20 | 20 | 零新依賴、零 hardcoded secret、timeout 有界、測試層面封死出站 socket |
| 可維護性（Maintainability） | 9 | 20 | −5 W-032（public 錯型只能由私有 module import）、−5 W-033（CHANGELOG 漏咗兩個 public 行為改動）、−1 S-090 |
| 測試覆蓋（Test Coverage） | 7 | 15 | 96% TOTAL、touched module 全 100%，但 −5 W-031（guard 一半可刪而全綠）、−1 S-087、−1 S-088、−1 S-091 |
| 性能（Performance） | 10 | 10 | A/B 同 timeit 我獨立重現；guard = 單段成本 0.650%，無回歸 |
| 代碼風格（Code Style） | 10 | 10 | ruff check / format 全清、eslint 清、命名零縮寫、常數全部具名兼有 docstring |
| **總分** | **80** | **100** | |

**結果：⚠️ warn**（hard gates 全 pass、0 🔴 Critical、score 落喺 75–89）

---

## 修正優先順序

| 次序 | ID | 位置 | 成本 | 點解排呢個位 |
|---|---|---|---|---|
| 1 | **W-031** | `tests/test_weather_collectors.py` | 4 行 | 唯一一個「靜默錯 guard 可以無聲刪走」嘅缺口，正正係本批要消滅嘅缺陷類別 |
| 2 | **W-032** | `src/weather/collectors/__init__.py` + `src/cli/collect_weather.py` | 4 行 | 令 public 契約真係 public；`_parsing` 一動就拆爛 `cli` |
| 3 | **W-033** | `docs/CHANGELOG.md` | 4 個 entry | 同 session 兄弟 lane 已經寫咗，呢兩條漏；`develop` 直接 deploy |
| 4 | S-087 | `tests/test_weather_collectors.py` | 3 行 | 釘死 source comment 明文聲稱嘅行為 |
| 5 | S-090 | `tests/test_weather_collectors.py` | 3 行 | 用 `is` 比較守住集中化本身 |
| 6 | S-088 | `tests/` | 6 行 | 低風險，但示範咗 line coverage 嘅盲點 |
| 7 | S-089 | follow-up ticket + docstring | — | 排期，唔 block |
| 8 | S-091 | 流程 | — | 接受現狀，日後 ticket log 補 TDD 說明 |

> W-031 / S-087 / S-088 / S-090 **全部係純加測試，零生產代碼改動**，加埋 16 行，一個 commit 可以行完（但按規範要拆 4 個 commit，每個 review item 一個）。W-032 係 4 行機械式改動。W-033 係純文件。**我估計整批 warn 可以喺一個短 session 內清晒。**

---

## 修正指引（按 Fix Convention，每個 item 一個獨立 commit）

```
fix: W-031 | 釘死 _icon_code 個 start<0 guard
fix: W-032 | 由 collectors/__init__ re-export WeatherPageStructureError
docs: W-033 | 補 CUI-0008/0012/0013/0041 入 CHANGELOG
fix: S-087 | 釘死單位按名剝離嘅行為
fix: S-090 | 用 identity 比較守住 timeout 集中化
fix: S-088 | 覆蓋 _parsing 兩個防禦分支
```

---

## 附錄 —— 本次 review 用到嘅方法

跟足 CLAUDE.md §6 嗰條 mutation testing 陷阱（stale `.pyc` / `sys.modules` 已 import 會令 mutant 假綠，而且單一次觀察判定唔到成因）：**全部 mutant 一律 in-process `setattr`、每個都先跑 oracle 證明 mutant 真係生效兼真係有行為差異、再用完整 suite 做獨立 oracle 驗結果。** 零一次 `python -B`，零一次靠「改完即刻跑」。

跑過嘅 mutant 一覽：

| # | 目標 | 結果 |
|---|---|---|
| M1 | `_icon_code` 兩個 guard 全刪 | KILLED |
| M2 | `_icon_code` 刪 `end < start` | KILLED |
| M3 | `_icon_code` **刪 `start < 0`** | **SURVIVED → W-031** |
| M4 | `section_after` 刪 `start < 0` | KILLED |
| M5 | `child_attr` 中和成永遠回 default | KILLED |
| M6 | `child_string` 中和成永遠回 None | KILLED |
| M7 | `export.records.finite` 中和 | KILLED（6 個 daily）→ 佐證 ⑩ |
| M8 | `dashboard.builder.finite` 中和 | KILLED（3 個 hourly）→ 佐證 ⑩ |
| M9 | `_wind_speed_kmh` 改返 `[:-5]` | **SURVIVED → S-087** |
| M10 | `child_attr` 刪 `isinstance(value, str)` | **SURVIVED → S-088** |
| M11 | `child_string` 刪 `.string is None` | **SURVIVED → S-088** |
| M12 | base tree：`numeric.finite_float` 毒藥 | SURVIVED（418 passed）→ 證實 ⑪ |

另外喺一棵由 `git archive 6576e56` 抽出、獨立 venv 嘅完整 base tree 上跑過：完整 suite（418 passed）、完整 coverage（TOTAL 95%、`collect_weather.py` 45%、`numeric.py` 94% 且 miss 喺 line 85）、pre-fix `geo.py` 嘅 warning 行為。

```handoff-receipt
protocol: 1
status: warn
score: 80/100
hard_gates:
  lint: pass
  type_check: pass
  tests: pass
  coverage: "96%"
next_action: invoke_developer
next_agent: backend-developer
branch: "claude/ai-dev-team-start-05jie2 @ 06ad4bd (detached HEAD in review worktree)"
context: "CUI-0012/0013/0008/0041 batch: hard gates all pass (492 pytest, 141 vitest/25 files, 96% coverage, ruff+eslint clean, SDL up to date), 0 Critical, all 11 lane self-overrides independently verified as correct. 3 Warnings block promotion to pass: W-031 the start<0 half of _icon_code's guard survives mutation (492 stay green while 'x26y' would be read as Snow), W-032 WeatherPageStructureError is a documented public failure mode importable only from the private _parsing module, W-033 CHANGELOG missing the run365-weather exit-code semantics change and the new public failure mode while sibling lanes in the same session did record theirs. W-031/S-087/S-088/S-090 are test-only additions totalling ~16 lines; W-032 is 4 lines; W-033 is docs. New IDs used: W-031..W-033 and S-087..S-091 -- renumber on merge, a second reviewer was told to start at S-087 too."
blockers:
  - "W-031: _icon_code's `start < 0` guard can be deleted with all 492 tests still green; mutant is non-equivalent ('x26y' -> 'Snow' instead of 'Unknown'). Add a direct assertion in tests/test_weather_collectors.py::TestHourlyFetchDay, mirroring the existing test_the_old_icon_arithmetic_would_have_named_a_description."
  - "W-032: WeatherPageStructureError is declared in the public Raises: of warnings.fetch_day/fetch_range but lives in the private src/weather/collectors/_parsing.py; src/cli/collect_weather.py imports it across package boundaries from that private path. Re-export it from src/weather/collectors/__init__.py with __all__."
  - "W-033: docs/CHANGELOG.md records nothing for CUI-0008/0012/0013/0041, while the same session's sibling lanes did add CUI-0035..0039. Missing: run365-weather exit-code semantics, the new WeatherPageStructureError failure mode, the removal of public run365days.common.numeric.finite_float, and the new ValueError from haversine_distance/total_track_distance."
```

---

## ⚠️ Main agent 更正 —— W-033 嘅前提唔成立

Reviewer 寫：

> 「而同一個 session 嘅**兄弟 lane 有寫**：`[3.2.0] - 2026-09-16` 呢個 section 已經收錄咗
> `CUI-0035`–`CUI-0039`（即 `7f25812` 同 `06ad4bd` 兩條 lane）。所以呢個唔係『項目唔寫
> CHANGELOG』，係**呢兩條 lane 漏咗**。」

**呢句係誤讀。** 實測 `docs/CHANGELOG.md`：

```
$ awk '/^### /{s=$0} /CUI-003[5-9]/{print NR": "s}' docs/CHANGELOG.md
92: ### Known
98: ### Known
101: ### Known
103: ### Known
```

`CUI-0035`–`CUI-0039` **四處全部喺 `### Known` section**，係 main agent 喺 v3.2.0 release 時
寫落嘅**未修項目**清單，唔係兄弟 lane 記錄自己完成咗嘅嘢。當時嗰五張票一張都未做。

⇒ 「兄弟 lane 有寫、呢兩條漏咗」呢個對比唔成立。本 repo 嘅慣例係 **S-079 立落嘅「bump 時先寫」**
（正正因為呢個慣例，`[Unreleased]` section 從來唔存在），所以四條 lane **一條都冇**寫 CHANGELOG，
係跟慣例而唔係漏。

**不過 finding 本身仍然成立，只係理由同時機唔同**：W-033 列出嗰四項 public 行為改動
（`run365-weather` exit code 語義、`WeatherPageStructureError` 新失敗模式、`finite_float` 由
public API 消失、`haversine_distance` 新增 `ValueError`）**確實要入 CHANGELOG**，而正確時機係
**下次 bump**（即緊接住嘅 v3.3.0，因為 CUI-0018(b) 個 `track` nullable 又係 breaking change）。
Reviewer 嘅清單本身逐項核實成立，會原樣帶入 v3.3.0 嘅 release notes。

扣分方面：W-033 由「lane 漏咗」降格為「release step 要記得帶」，唔應該計喺呢兩條 lane 數上。
