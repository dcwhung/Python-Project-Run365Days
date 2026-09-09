import { useCallback, useEffect, useRef, useState } from "react";

export const SPEEDS = [10, 30, 90] as const;
export type Speed = (typeof SPEEDS)[number];

/**
 * Playback cursor over a time series: `idx` is the current sample, `play`
 * advances it in real time multiplied by `speed`, using requestAnimationFrame.
 */
export function usePlayback(t: number[], initialSpeed: number) {
  const [idx, setIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState<number>(initialSpeed);
  const state = useRef({ idx: 0, clock: 0, lastTs: 0, raf: 0 });
  const n = t.length;

  const goTo = useCallback(
    (i: number) => {
      const clamped = Math.max(0, Math.min(n - 1, i));
      state.current.idx = clamped;
      setIdx(clamped);
    },
    [n],
  );

  const stop = useCallback(() => {
    cancelAnimationFrame(state.current.raf);
    setPlaying(false);
  }, []);

  const start = useCallback(() => {
    if (!n) return;
    if (state.current.idx >= n - 1) state.current.idx = 0;
    state.current.clock = t[state.current.idx];
    state.current.lastTs = 0;
    setPlaying(true);
  }, [n, t]);

  useEffect(() => {
    if (!playing) return;
    const tick = (ts: number) => {
      const s = state.current;
      if (s.lastTs) s.clock += ((ts - s.lastTs) / 1000) * speed;
      s.lastTs = ts;
      while (s.idx < n - 1 && t[s.idx + 1] <= s.clock) s.idx++;
      setIdx(s.idx);
      if (s.idx >= n - 1) {
        setPlaying(false);
        return;
      }
      s.raf = requestAnimationFrame(tick);
    };
    const s = state.current;
    s.raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(s.raf);
  }, [playing, speed, n, t]);

  // reset when the series changes
  useEffect(() => {
    cancelAnimationFrame(state.current.raf);
    state.current.idx = 0;
    setIdx(0);
    setPlaying(false);
  }, [t]);

  return { idx, playing, speed, setSpeed, goTo, start, stop, toggle: () => (playing ? stop() : start()) };
}
