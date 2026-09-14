/**
 * The HKO warning-signal palette: colours the warning system fixes, not
 * colours this dashboard chooses.
 *
 * The distinction matters and is why these are not in `tokens.ts`. A token
 * follows the theme -- change `--color-danger` and everything danger-coloured
 * moves with it. An amber rainstorm signal is amber because the Observatory
 * says amber; if it drifted to whatever `--color-warn` happened to be, the icon
 * would stop meaning what it means. Same for the red and black rainstorm ranks,
 * which readers tell apart by colour alone, and for the white glyphs, which are
 * legible only because the tile underneath them is a fixed saturated colour.
 *
 * So: these are frozen on purpose. The one colour in the sprite that *is*
 * thematic -- the foreground of the typhoon signal shapes -- uses `TOKENS.text`
 * and is not repeated here.
 */
export const SIGNAL = {
  /** Rainstorm ranks. Amber, red and black are the signal, not decoration. */
  rainAmber: "#f5b800",
  rainRed: "#e53935",
  rainBlack: "#0b0d14",
  /** Outline that keeps the near-black rainstorm shape legible on a dark card. */
  rainBlackEdge: "#cbd5e1",

  /** Glyph colour over any of the saturated tiles below. */
  onTile: "#fff",

  monsoonTile: "#e53935",
  thunderTile: "#2a2f3d",
  thunderTileEdge: "#4b5268",
  thunderBolt: "#facc15",
  floodTile: "#1e88e5",
  frostTile: "#d32f2f",
  coldTile: "#1976d2",
  hotTile: "#e53935",
  tsunamiTile: "#1e88e5",

  landslipEarth: "#8d6e63",
  landslipDebris: "#a1887f",

  fireYellow: "#fdd835",
  fireYellowCore: "#fff3b0",
  fireRed: "#e53935",
  fireRedCore: "#ffb3ae",
} as const;

/**
 * The sprite draws its digits with the platform UI face rather than the
 * dashboard's `--font-sans`. Kept deliberate and kept in one place: the digits
 * are part of a pictogram and should not shift with the body font.
 */
export const SIGNAL_DIGIT_FONT = "system-ui,sans-serif";
