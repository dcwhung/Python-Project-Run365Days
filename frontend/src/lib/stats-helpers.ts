/** Small numeric helpers shared by the views. */
export const sum = (values: (number | null | undefined)[]) =>
  values.reduce<number>((acc, v) => acc + (v ?? 0), 0);

export const mean = (values: (number | null | undefined)[]): number | null => {
  const vals = values.filter((v): v is number => v != null && Number.isFinite(v));
  return vals.length ? sum(vals) / vals.length : null;
};

export const minBy = <T>(list: T[], key: (t: T) => number | null | undefined): T | null => {
  let best: T | null = null;
  let bestKey = Infinity;
  for (const item of list) {
    const k = key(item);
    if (k != null && k < bestKey) {
      best = item;
      bestKey = k;
    }
  }
  return best;
};

export const maxBy = <T>(list: T[], key: (t: T) => number | null | undefined): T | null => {
  let best: T | null = null;
  let bestKey = -Infinity;
  for (const item of list) {
    const k = key(item);
    if (k != null && k > bestKey) {
      best = item;
      bestKey = k;
    }
  }
  return best;
};

/** Centred moving average over 2*w+1 samples, skipping NaN. */
export function smooth(values: number[], w: number): number[] {
  return values.map((_, i) => {
    let s = 0;
    let n = 0;
    for (let j = i - w; j <= i + w; j++) {
      if (j >= 0 && j < values.length && Number.isFinite(values[j])) {
        s += values[j];
        n++;
      }
    }
    return n ? s / n : NaN;
  });
}
