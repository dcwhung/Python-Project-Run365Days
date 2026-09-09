/**
 * Domain types shared by both data modes.
 *
 * They mirror the GraphQL schema (camelCase). In api mode the generated
 * operation types are structurally identical; in static mode the snake_case
 * JSON is mapped onto these shapes.
 */

export interface RunWeather {
  description: string | null;
  tempC: number | null;
  humidityPct: number | null;
  windKmh: number | null;
}

export interface Activity {
  id: string;
  date: string;
  startTime: string;
  dayOfYear: number;
  distanceKm: number;
  durationSec: number;
  paceSecPerKm: number | null;
  calories: number | null;
  avgCadence: number | null;
  avgTempC: number | null;
  elevationMinM: number | null;
  elevationMaxM: number | null;
  ascentM: number | null;
  hasGps: boolean;
  numPoints: number;
  weather: RunWeather | null;
  warnings: string[];
}

export interface TrackPoint {
  sec: number;
  lat: number | null;
  lon: number | null;
  elevationM: number | null;
  distanceM: number | null;
  speedMps: number | null;
  cadence: number | null;
  tempC: number | null;
}

export interface WeightEntry {
  date: string;
  weightLbs: number;
  weightKg: number;
  bmi: number;
}

export interface DailyWeather {
  date: string;
  maxTempC: number | null;
  avgTempC: number | null;
  minTempC: number | null;
  humidityPct: number | null;
  rainfallMm: number | null;
  windKmh: number | null;
  sunrise: string | null;
  sunset: string | null;
}

export interface WeatherWarning {
  date: string;
  type: string | null;
  signal: string;
  startTime: string | null;
  endTime: string | null;
}

export interface Meta {
  year: number;
  generatedAt: string;
}

export interface Totals {
  runs: number;
  days: number;
  activeDays: number;
  distanceKm: number;
  durationSec: number;
  calories: number;
  avgPaceSecPerKm: number | null;
  avgCadence: number | null;
  avgDistanceKm: number;
}

export interface MonthSummary {
  month: number;
  runs: number;
  distanceKm: number;
  durationSec: number;
  calories: number;
  avgPaceSecPerKm: number | null;
  avgCadence: number | null;
  bestPaceSecPerKm: number | null;
  bestPaceActivityId: string | null;
}

export interface WeekSummary {
  week: number;
  weekStart: string;
  runs: number;
  distanceKm: number;
  durationSec: number;
  avgPaceSecPerKm: number | null;
  longestKm: number;
  activityIds: string[];
}

export interface DayDistance {
  date: string;
  distanceKm: number;
  activityId: string | null;
}

export interface TrainingLoadPoint {
  date: string;
  ctl: number;
  atl: number;
  tsb: number;
}

export interface PersonalBests {
  longest: Activity | null;
  fastest: Activity | null;
  longestTime: Activity | null;
  mostCalories: Activity | null;
  topCadence: Activity | null;
}

export interface YearSummary {
  year: number;
  totals: Totals;
  monthly: MonthSummary[];
  weekly: WeekSummary[];
  dailyDistance: DayDistance[];
  trainingLoad: TrainingLoadPoint[];
  personalBests: PersonalBests;
}

export interface ActivityFilter {
  fromDate?: string;
  toDate?: string;
  minKm?: number;
  hasGps?: boolean;
}

export interface DateRange {
  fromDate?: string;
  toDate?: string;
}

/** Everything a view can ask for, independent of where the data comes from. */
export interface DataSource {
  readonly mode: DataMode;
  meta(): Promise<Meta>;
  year(): Promise<YearSummary>;
  activities(filter?: ActivityFilter): Promise<Activity[]>;
  activity(id: string): Promise<Activity | null>;
  track(id: string, points?: number): Promise<TrackPoint[]>;
  weight(range?: DateRange): Promise<WeightEntry[]>;
  weather(range?: DateRange): Promise<DailyWeather[]>;
  warnings(range?: DateRange): Promise<WeatherWarning[]>;
}

export type DataMode = "api" | "static";
