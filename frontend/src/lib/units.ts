/**
 * Unit conversions for the running domain.
 *
 * These were 26 bare `/ 60`, `/ 3600` and `/ 1000` scattered through the view
 * components. Each was individually obvious and collectively unsearchable: the
 * data layer stores durations in seconds, distances in metres and pace in
 * seconds per kilometre, while every chart and KPI wants minutes, hours or
 * kilometres, and nothing said so out loud.
 */
export const SEC_PER_MIN = 60;
export const SEC_PER_HOUR = 3600;
export const M_PER_KM = 1000;

export const secToMin = (sec: number) => sec / SEC_PER_MIN;
export const minToSec = (min: number) => min * SEC_PER_MIN;
export const secToHours = (sec: number) => sec / SEC_PER_HOUR;
export const metresToKm = (metres: number) => metres / M_PER_KM;

/**
 * Pace as the charts plot it: minutes per kilometre, two decimals. The axis
 * ticks and tooltips convert straight back with `minToSec` before formatting,
 * so the rounding has to be the same everywhere it is applied.
 */
export const paceToPlotMin = (secPerKm: number) => Math.round(secToMin(secPerKm) * 100) / 100;
