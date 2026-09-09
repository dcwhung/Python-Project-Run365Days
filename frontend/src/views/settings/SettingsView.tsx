import { useState, type FormEvent } from "react";
import { useActivities, useMeta, useWeather, useWeight } from "@/data/hooks";
import { useDataSource } from "@/data/context";
import { DEFAULT_PREFS, resetPrefs, savePrefs, usePrefs, type Prefs } from "@/lib/prefs";
import { VIEWS } from "@/app/views";
import { SPEEDS } from "@/views/activity/usePlayback";
import { Card } from "@/components/Card";

const input = "w-full rounded border border-border bg-surface2 px-2 py-1 text-sm";

export function SettingsView() {
  const prefs = usePrefs();
  const [form, setForm] = useState<Prefs>(prefs);
  const [msg, setMsg] = useState<string | null>(null);
  const meta = useMeta();
  const activities = useActivities();
  const weight = useWeight();
  const weather = useWeather();
  const source = useDataSource();

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (form.paceSlow <= form.paceFast) {
      setMsg("Slow bound must be greater than fast bound");
      return;
    }
    savePrefs({ ...form, heightCm: form.heightCm || DEFAULT_PREFS.heightCm });
    setMsg("Saved");
  };
  const reset = () => {
    setForm(resetPrefs());
    setMsg("Reset to defaults");
  };
  const acts = activities.data ?? [];
  const gps = acts.filter((a) => a.hasGps).length;
  const info: [string, string][] = [
    ["Data source", source.mode === "api" ? "GraphQL API" : "static JSON"],
    ["Year", meta.data ? String(meta.data.year) : "…"],
    ["Generated", meta.data ? meta.data.generatedAt.replace("T", " ") : "…"],
    ["Activities", String(acts.length)],
    ["With GPS", `${gps} (${acts.length - gps} indoor)`],
    ["Weigh-ins", String(weight.data?.length ?? "…")],
    ["HKO daily rows", String(weather.data?.length ?? "…")],
    ["Runs with hourly weather", String(acts.filter((a) => a.weather).length)],
  ];

  return (
    <div className="grid gap-4 lg:grid-cols-2" data-testid="settings-view">
      <Card title="Preferences">
        <form onSubmit={submit} className="space-y-3" aria-label="Preferences">
          <label className="block text-xs text-muted">
            Landing view
            <select className={input} value={form.landing} onChange={(e) => setForm({ ...form, landing: e.target.value })}>
              {VIEWS.map((v) => <option key={v.path} value={v.path}>{v.label}</option>)}
            </select>
          </label>
          <label className="block text-xs text-muted">
            Playback speed
            <select className={input} value={form.speed} onChange={(e) => setForm({ ...form, speed: Number(e.target.value) })}>
              {SPEEDS.map((s) => <option key={s} value={s}>{s}×</option>)}
            </select>
          </label>
          <label className="block text-xs text-muted">
            Weight unit
            <select className={input} value={form.weightUnit} onChange={(e) => setForm({ ...form, weightUnit: e.target.value as Prefs["weightUnit"] })}>
              <option value="lbs">lbs</option>
              <option value="kg">kg</option>
            </select>
          </label>
          <label className="block text-xs text-muted">
            Height (cm, for BMI)
            <input type="number" min={100} max={250} className={input} value={form.heightCm} onChange={(e) => setForm({ ...form, heightCm: Number(e.target.value) })} />
          </label>
          <div className="grid grid-cols-2 gap-3">
            <label className="block text-xs text-muted">
              Fast pace bound (min/km)
              <input type="number" step={0.1} className={input} value={form.paceFast} onChange={(e) => setForm({ ...form, paceFast: Number(e.target.value) })} />
            </label>
            <label className="block text-xs text-muted">
              Slow pace bound (min/km)
              <input type="number" step={0.1} className={input} value={form.paceSlow} onChange={(e) => setForm({ ...form, paceSlow: Number(e.target.value) })} />
            </label>
          </div>
          <div className="flex items-center gap-3">
            <button type="submit" className="rounded bg-accent px-3 py-1.5 text-sm font-medium text-bg hover:brightness-110">Save</button>
            <button type="button" onClick={reset} className="rounded border border-border px-3 py-1.5 text-sm hover:bg-surface2">Reset</button>
            {msg && <span className="text-xs text-muted" role="status">{msg}</span>}
          </div>
          <p className="text-[11px] text-muted">Stored in this browser only. The pace bounds colour the route in the Activity view.</p>
        </form>
      </Card>
      <Card title="About the data">
        <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm" data-testid="data-info">
          {info.map(([k, v]) => (
            <div key={k}>
              <dt className="text-[10px] uppercase tracking-wide text-muted">{k}</dt>
              <dd className="font-medium">{v}</dd>
            </div>
          ))}
        </dl>
      </Card>
    </div>
  );
}
