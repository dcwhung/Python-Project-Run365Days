import { describe, expect, it } from "vitest";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";
import * as stats from "@/data/stats";
import { downsample } from "@/lib/downsample";
import { act } from "@/test/fixtures";
import type { Activity, DayDistance } from "@/data/types";

/**
 * TypeScript half of the cross-language conformance gate (AU-009).
 *
 * `src/data/stats.ts` calls itself a line-for-line port of
 * `src/analytics/stats.py`, and `src/lib/downsample.ts` mirrors
 * `analytics.builder.downsample`. Nothing checked that. Both suites pinned the
 * same numbers only because two people typed the same fixture twice, so a
 * formula changed on one side and not the other left both green while api mode
 * and static mode showed different figures to the same visitor.
 *
 * This file recomputes the values in `tests/golden/expected.tsv` from the
 * TypeScript implementation; `tests/test_cross_language_golden.py` recomputes
 * the same keys from Python and compares them to the same file. The golden
 * belongs to neither language, so changing one implementation alone turns that
 * one red. Its module docstring carries the full design, including why
 * regenerating cannot paper over a divergence and why `golden/divergences.tsv`
 * is hand-maintained. That ledger is currently absent, which is the healthy
 * state: its eight entries all came from one root cause -- Python breaking a
 * tie to the even neighbour where `Math.round` breaks it upward -- and AU-009-A
 * closed them by giving TypeScript Python's rule (`@/lib/rounding`). The
 * machinery stays for the next divergence; what must never exist is the file
 * with no rows in it.
 *
 * Reading a file outside `frontend/`: this runs in node, so `readFileSync` is
 * fine and Vite's `server.fs.allow` never enters into it -- that guard is about
 * the dev server answering HTTP requests, not about node I/O in a test. It
 * would apply if the golden were `import`ed as a module, which is one reason it
 * is read as text instead. The other is placement: the golden is shared
 * property of both stacks, and putting it under `frontend/` would both make the
 * Python suite reach into a deploy target and risk it being copied into
 * `public/` or checked by `prettier --check .`, which reformats JSON.
 *
 * `fileURLToPath(new URL(rel, import.meta.url))` does NOT work here: the vitest
 * environment is jsdom, whose `URL` global is whatwg-url rather than node's, so
 * node's `fileURLToPath` rejects the object with "The URL must be of scheme
 * file". Converting `import.meta.url` first and joining with `node:path`
 * sidesteps that, and is independent of the working directory, so it holds
 * whether the suite is started from `frontend/` or from the repository root.
 */

const GOLDEN_DIR = join(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
  "..",
  "tests",
  "golden",
);
const CASES_FILE = join(GOLDEN_DIR, "cases.json");
const EXPECTED_FILE = join(GOLDEN_DIR, "expected.tsv");
const DIVERGENCES_FILE = join(GOLDEN_DIR, "divergences.tsv");

/** Which column of divergences.tsv this side must match. */
const LANGUAGE_COLUMN = 1;

const TWO_POW_53 = 2 ** 53;

/**
 * Render one output value as the canonical text both languages must produce.
 *
 * Integral values collapse onto integers because Python writes `23.0` where
 * JavaScript writes `23` for the same double. Everything else uses the shortest
 * decimal that round-trips, which `String` and Python's `repr` both produce and
 * which is a bijection with the double -- so equality of these strings is bit
 * equality, and there is no tolerance anywhere. Exponent notation is refused:
 * the two languages spell it differently (`1e-7` against `1e-07`) and nothing
 * in this domain reaches for it.
 */
export function canon(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (value === true) return "true";
  if (value === false) return "false";
  if (Array.isArray(value)) return value.map(canon).join(",");
  if (typeof value === "string") {
    if (value.includes("\t") || value.includes("\n")) throw new Error(`not tab-safe: ${value}`);
    return value;
  }
  if (typeof value !== "number") throw new Error(`unsupported golden value: ${String(value)}`);
  if (!Number.isFinite(value)) throw new Error(`non-finite value reached the golden: ${value}`);
  if (Number.isInteger(value) && Math.abs(value) < TWO_POW_53) return String(value);
  const text = String(value);
  if (text.includes("e") || text.includes("E")) {
    throw new Error(`value needs exponent notation, which is not canonical: ${text}`);
  }
  return text;
}

/** Hash a full series so a 365-row array costs one golden line, not 1095. */
function digest(lines: string[]): string {
  return createHash("sha256").update(lines.join("\n"), "utf8").digest("hex");
}

type Field<T> = [string, (row: T) => unknown];

class Emitter {
  readonly values = new Map<string, string>();

  put(key: string, value: unknown): void {
    if (this.values.has(key)) throw new Error(`duplicate golden key: ${key}`);
    this.values.set(key, canon(value));
  }

  /**
   * Record one row under `prefix`, naming each golden field and the accessor
   * that reads it. The pairs are written out rather than derived by
   * camelCase-to-snake_case guessing, so renaming a field on either side
   * changes the key set and fails loudly instead of renaming a golden line.
   */
  row<T>(prefix: string, value: T, fields: Field<T>[]): void {
    for (const [name, read] of fields) this.put(`${prefix}.${name}`, read(value));
  }

  /** Record a long per-day series as length + digest + the case's probe rows. */
  series<T>(prefix: string, rows: T[], fields: Field<T>[], probes: number[]): void {
    this.put(`${prefix}.len`, rows.length);
    const flat: string[] = [];
    rows.forEach((r, i) => {
      for (const [name, read] of fields) flat.push(`${i}.${name}\t${canon(read(r))}`);
    });
    this.put(`${prefix}.digest`, digest(flat));
    for (const i of probes) if (i < rows.length) this.row(`${prefix}[${i}]`, rows[i], fields);
  }
}

type TotalsRow = ReturnType<typeof stats.totals>;
type MonthRow = ReturnType<typeof stats.monthly>[number];
type WeekRow = ReturnType<typeof stats.weekly>[number];
type LoadRow = ReturnType<typeof stats.trainingLoad>[number];

const TOTALS_FIELDS: Field<TotalsRow>[] = [
  ["runs", (t) => t.runs],
  ["days", (t) => t.days],
  ["active_days", (t) => t.activeDays],
  ["distance_km", (t) => t.distanceKm],
  ["duration_sec", (t) => t.durationSec],
  ["calories", (t) => t.calories],
  ["avg_pace_sec_per_km", (t) => t.avgPaceSecPerKm],
  ["avg_cadence", (t) => t.avgCadence],
  ["avg_distance_km", (t) => t.avgDistanceKm],
];
const MONTHLY_FIELDS: Field<MonthRow>[] = [
  ["month", (m) => m.month],
  ["runs", (m) => m.runs],
  ["distance_km", (m) => m.distanceKm],
  ["duration_sec", (m) => m.durationSec],
  ["calories", (m) => m.calories],
  ["avg_pace_sec_per_km", (m) => m.avgPaceSecPerKm],
  ["avg_cadence", (m) => m.avgCadence],
  ["best_pace_sec_per_km", (m) => m.bestPaceSecPerKm],
  ["best_pace_activity_id", (m) => m.bestPaceActivityId],
];

const WEEKLY_FIELDS: Field<WeekRow>[] = [
  ["week", (w) => w.week],
  ["week_start", (w) => w.weekStart],
  ["runs", (w) => w.runs],
  ["distance_km", (w) => w.distanceKm],
  ["duration_sec", (w) => w.durationSec],
  ["avg_pace_sec_per_km", (w) => w.avgPaceSecPerKm],
  ["longest_km", (w) => w.longestKm],
  ["activity_ids", (w) => w.activityIds],
];

const DAILY_FIELDS: Field<DayDistance>[] = [
  ["date", (d) => d.date],
  ["distance_km", (d) => d.distanceKm],
  ["activity_id", (d) => d.activityId],
];

const LOAD_FIELDS: Field<LoadRow>[] = [
  ["date", (l) => l.date],
  ["ctl", (l) => l.ctl],
  ["atl", (l) => l.atl],
  ["tsb", (l) => l.tsb],
];

type CaseActivity = {
  id: string;
  date: string;
  distance_km: number;
  duration_sec: number;
  pace_sec_per_km: number | null;
  calories: number | null;
  avg_cadence: number | null;
};

type Cases = {
  days_in_year: number[];
  week_index: { year: number; dates: string[] }[];
  stats: { id: string; year: number; probe_days: number[]; activities: CaseActivity[] }[];
  training_load: { id: string; daily: { date: string; distance_km: number }[] }[];
  downsample: { id: string; n: number; limit: number }[];
};

function toActivity(a: CaseActivity): Activity {
  return act({
    id: a.id,
    date: a.date,
    distanceKm: a.distance_km,
    durationSec: a.duration_sec,
    paceSecPerKm: a.pace_sec_per_km,
    calories: a.calories,
    avgCadence: a.avg_cadence,
  });
}

function emit(cases: Cases, casesBytes: Buffer): Map<string, string> {
  const out = new Emitter();
  out.put("meta/cases_sha256", createHash("sha256").update(casesBytes).digest("hex"));

  for (const year of cases.days_in_year) out.put(`days_in_year/${year}`, stats.daysInYear(year));

  for (const probe of cases.week_index) {
    for (const day of probe.dates) {
      out.put(`week_index/${probe.year}/${day}`, stats.weekIndex(day, probe.year));
    }
  }

  for (const c of cases.stats) {
    const acts = c.activities.map(toActivity);
    const prefix = `stats/${c.id}`;

    out.row(`${prefix}/totals`, stats.totals(acts, c.year), TOTALS_FIELDS);

    const months = stats.monthly(acts);
    out.put(`${prefix}/monthly.len`, months.length);
    months.forEach((m, i) => out.row(`${prefix}/monthly[${i}]`, m, MONTHLY_FIELDS));

    const weeks = stats.weekly(acts, c.year);
    out.put(`${prefix}/weekly.len`, weeks.length);
    weeks.forEach((w, i) => out.row(`${prefix}/weekly[${i}]`, w, WEEKLY_FIELDS));

    const daily = stats.dailyDistance(acts, c.year);
    out.series(`${prefix}/daily_distance`, daily, DAILY_FIELDS, c.probe_days);
    out.series(`${prefix}/training_load`, stats.trainingLoad(daily), LOAD_FIELDS, c.probe_days);

    const pb = stats.personalBests(acts);
    out.put(`${prefix}/personal_bests.longest`, pb.longest?.id ?? null);
    out.put(`${prefix}/personal_bests.fastest`, pb.fastest?.id ?? null);
    out.put(`${prefix}/personal_bests.longest_time`, pb.longestTime?.id ?? null);
    out.put(`${prefix}/personal_bests.most_calories`, pb.mostCalories?.id ?? null);
    out.put(`${prefix}/personal_bests.top_cadence`, pb.topCadence?.id ?? null);
  }

  for (const c of cases.training_load) {
    const daily: DayDistance[] = c.daily.map((r) => ({
      date: r.date,
      distanceKm: r.distance_km,
      activityId: null,
    }));
    const rows = stats.trainingLoad(daily);
    out.put(`training_load/${c.id}.len`, rows.length);
    rows.forEach((r, i) => out.row(`training_load/${c.id}[${i}]`, r, LOAD_FIELDS));
  }

  for (const c of cases.downsample) {
    out.put(`downsample/${c.id}`, downsample([...Array(c.n).keys()], c.limit));
  }

  return out.values;
}

function readTsv(path: string, columns: number): Map<string, string[]> {
  const rows = new Map<string, string[]>();
  if (!existsSync(path)) return rows;
  const text = readFileSync(path, "utf8");
  text.split("\n").forEach((line, i) => {
    if (!line || line.startsWith("#")) return;
    const parts = line.split("\t");
    if (parts.length < columns) throw new Error(`${path}:${i + 1}: expected ${columns} columns`);
    rows.set(parts[0], parts.slice(1));
  });
  return rows;
}

const casesBytes = readFileSync(CASES_FILE);
const emitted = emit(JSON.parse(casesBytes.toString("utf8")) as Cases, casesBytes);
const divergences = readTsv(DIVERGENCES_FILE, 4);

describe("cross-language golden (AU-009)", () => {
  it("matches golden/expected.tsv, which the Python suite checks against too", () => {
    if (process.env.UPDATE_GOLDEN) {
      const body = [...emitted.entries()]
        .filter(([key]) => !divergences.has(key))
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([key, value]) => `${key}\t${value}\n`)
        .join("");
      writeFileSync(EXPECTED_FILE, body, "utf8");
      return;
    }

    const expected = readTsv(EXPECTED_FILE, 2);
    const shared = [...emitted.keys()].filter((k) => !divergences.has(k));

    expect(
      {
        missing: shared.filter((k) => !expected.has(k)).slice(0, 10),
        stale: [...expected.keys()]
          .filter((k) => !emitted.has(k) || divergences.has(k))
          .slice(0, 10),
      },
      "golden key set has drifted",
    ).toEqual({ missing: [], stale: [] });

    const mismatches = shared
      .filter((k) => expected.get(k)![0] !== emitted.get(k))
      .map((k) => `${k}: expected ${expected.get(k)![0]}, TypeScript produced ${emitted.get(k)}`);
    expect(
      mismatches.slice(0, 20),
      `${mismatches.length} value(s) differ from the shared golden. Each one is either a ` +
        "TypeScript-side change Python has not made, or a fix both sides now agree on that the " +
        "golden has not caught up with.",
    ).toEqual([]);
  });

  it("still produces the TypeScript value recorded for every ledgered divergence", () => {
    expect(
      divergences.size > 0 || !existsSync(DIVERGENCES_FILE),
      "the divergence ledger records nothing -- delete it rather than leave a stub",
    ).toBe(true);
    const unknown = [...divergences.keys()].filter((k) => !emitted.has(k));
    expect(unknown, "ledger names keys nothing emits").toEqual([]);

    const stale = [...divergences.entries()]
      .filter(([key, cols]) => emitted.get(key) !== cols[LANGUAGE_COLUMN])
      .map(
        ([key, cols]) =>
          `${key}: ledger says ${cols[LANGUAGE_COLUMN]}, produced ${emitted.get(key)}`,
      );
    expect(
      stale,
      "ledgered keys no longer produce their recorded TypeScript value. If the implementation " +
        "was fixed, delete the ledger line and regenerate the golden.",
    ).toEqual([]);
  });

  it("canon is exact and refuses ambiguous forms", () => {
    expect(canon(23.0)).toBe("23");
    expect(canon(6.125)).toBe("6.125");
    expect(canon(0.1 + 0.2)).toBe("0.30000000000000004");
    expect(canon(null)).toBe("null");
    expect(canon(true)).toBe("true");
    expect(canon(["a", "b"])).toBe("a,b");
    expect(canon([])).toBe("");
    expect(() => canon(NaN)).toThrow();
    expect(() => canon(Infinity)).toThrow();
    expect(() => canon(1e-7)).toThrow();
    expect(() => canon("has\ttab")).toThrow();
  });
});
