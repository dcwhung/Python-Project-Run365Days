import type { Activity } from "@/data/types";

export const WX_EMOJI: Record<string, string> = {
  "Clear weather": "☀️",
  "Few clouds": "🌤",
  "Partly cloudy skies": "⛅",
  Rain: "🌧",
  Thunderstorm: "⛈",
};
export const wxEmoji = (description: string | null | undefined) =>
  (description && WX_EMOJI[description]) || "🌡";

/** Temperature during the run: Garmin sensor first, hourly observation as fallback. */
export const actTemp = (a: Activity): number | null => a.avgTempC ?? a.weather?.tempC ?? null;

/** HKO warning signal -> sprite icon id + display name (see WarningSprite). */
const WARN_ICONS: [RegExp, string, string][] = [
  [/NO\. 10/, "ws-t10", "Hurricane Signal No. 10"],
  [/NO\. 9/, "ws-t9", "Increasing Gale or Storm Signal No. 9"],
  [/NO\. 8 NORTHWEST/, "ws-t8nw", "No. 8 Northwest Gale or Storm Signal"],
  [/NO\. 8 SOUTHWEST/, "ws-t8sw", "No. 8 Southwest Gale or Storm Signal"],
  [/NO\. 8 NORTHEAST/, "ws-t8ne", "No. 8 Northeast Gale or Storm Signal"],
  [/NO\. 8 SOUTHEAST/, "ws-t8se", "No. 8 Southeast Gale or Storm Signal"],
  [/NO\. 8/, "ws-t8ne", "No. 8 Gale or Storm Signal"],
  [/NO\. 3/, "ws-t3", "Strong Wind Signal No. 3"],
  [/NO\. 1/, "ws-t1", "Standby Signal No. 1"],
  [/BLACK RAINSTORM/, "ws-rain-black", "Black Rainstorm Warning Signal"],
  [/RED RAINSTORM/, "ws-rain-red", "Red Rainstorm Warning Signal"],
  [/AMBER RAINSTORM/, "ws-rain-amber", "Amber Rainstorm Warning Signal"],
  [/STRONG MONSOON/, "ws-monsoon", "Strong Monsoon Signal"],
  [/THUNDERSTORM/, "ws-thunder", "Thunderstorm Warning"],
  [/LANDSLIP/, "ws-landslip", "Landslip Warning"],
  [/FLOODING/, "ws-flood", "Special Announcement on Flooding in the Northern New Territories"],
  [/FROST/, "ws-frost", "Frost Warning"],
  [/YELLOW FIRE/, "ws-fire-y", "Yellow Fire Danger Warning"],
  [/RED FIRE/, "ws-fire-r", "Red Fire Danger Warning"],
  [/COLD WEATHER/, "ws-cold", "Cold Weather Warning"],
  [/VERY HOT/, "ws-hot", "Very Hot Weather Warning"],
  [/TSUNAMI/, "ws-tsunami", "Tsunami Warning"],
];

export function warnInfo(signal: string): { id: string; name: string } | null {
  const m = WARN_ICONS.find(([re]) => re.test(signal));
  return m ? { id: m[1], name: m[2] } : null;
}

/** Typhoon signal 3 or above, or any rainstorm signal. */
export const SEVERE = /NO\. [38]|NO\. 9|NO\. 10|RAINSTORM/;
export const isSevere = (signal: string) => SEVERE.test(signal);
