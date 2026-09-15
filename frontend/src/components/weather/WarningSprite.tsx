import { SIGNAL, SIGNAL_DIGIT_FONT } from "@/styles/signalColors";
import { TOKENS } from "@/styles/tokens";

/**
 * The eight tropical cyclone signals are the same drawing twice over: a shape
 * on the left, its rank on the right, both in the theme foreground. Only the
 * shape and the digit differ, so only those are arguments.
 */
function TyphoonSignal({
  id,
  shape,
  rank,
  digitX = 19.5,
  digitSize = 13,
}: {
  id: string;
  shape: string;
  rank: string;
  digitX?: number;
  digitSize?: number;
}) {
  return (
    <symbol id={id} viewBox="0 0 24 24">
      <path fill={TOKENS.text} d={shape} />
      <text
        x={digitX}
        y="20"
        fontSize={digitSize}
        fontWeight="800"
        fill={TOKENS.text}
        textAnchor="middle"
        fontFamily={SIGNAL_DIGIT_FONT}
      >
        {rank}
      </text>
    </symbol>
  );
}

/** The three rainstorm ranks share one cloud with three rain strokes under it. */
const RAIN_CLOUD = "M7 14a4 4 0 0 1-.4-8 5.5 5.5 0 0 1 10.6 1.4A3.3 3.3 0 0 1 17.5 14z";
const RAIN_STROKES = "M7 16.5l-1.5 4M11.5 16.5l-1.5 4M16 16.5l-1.5 4";

function RainstormSignal({
  id,
  color,
  edge,
}: {
  id: string;
  color: string;
  /** Black rain only: the fill is too dark to read unedged on a dark card. */
  edge?: string;
}) {
  return (
    <symbol id={id} viewBox="0 0 24 24">
      <path fill={color} stroke={edge} strokeWidth={edge ? 1 : undefined} d={RAIN_CLOUD} />
      <path stroke={edge ?? color} strokeWidth="1.8" strokeLinecap="round" d={RAIN_STROKES} />
    </symbol>
  );
}

/** The rounded square behind the non-cyclone signals. */
function Tile({ fill, stroke }: { fill: string; stroke?: string }) {
  return <rect x="1" y="1" width="22" height="22" rx="4" fill={fill} stroke={stroke} />;
}

/** HKO warning-signal icons, simplified from the HKO legend. Rendered once in the layout. */
export function WarningSprite() {
  return (
    <svg width="0" height="0" style={{ position: "absolute" }} aria-hidden="true">
      <TyphoonSignal id="ws-t1" rank="1" shape="M1 3h13v4H9.5v14h-4V7H1z" />
      <TyphoonSignal id="ws-t3" rank="3" shape="M1 21h13v-4H9.5V3h-4v14H1z" />
      <TyphoonSignal id="ws-t8nw" rank="8" shape="M7.5 5l6.5 12H1z" />
      <TyphoonSignal id="ws-t8sw" rank="8" shape="M7.5 19L1 7h13z" />
      <TyphoonSignal id="ws-t8ne" rank="8" shape="M7.5 2l6 8.5h-12zM7.5 12.5l6 8.5h-12z" />
      <TyphoonSignal id="ws-t8se" rank="8" shape="M7.5 10.5l-6-8.5h12zM7.5 21l-6-8.5h12z" />
      <TyphoonSignal id="ws-t9" rank="9" shape="M1.5 2h12l-6 9zM1.5 22h12l-6-9z" />
      <TyphoonSignal
        id="ws-t10"
        rank="10"
        shape="M5 2h4v6h4v4H9v6H5v-6H1V8h4z"
        digitX={18.5}
        digitSize={11}
      />

      <RainstormSignal id="ws-rain-amber" color={SIGNAL.rainAmber} />
      <RainstormSignal id="ws-rain-red" color={SIGNAL.rainRed} />
      <RainstormSignal id="ws-rain-black" color={SIGNAL.rainBlack} edge={SIGNAL.rainBlackEdge} />

      <symbol id="ws-monsoon" viewBox="0 0 24 24">
        <Tile fill={SIGNAL.monsoonTile} />
        <path
          fill="none"
          stroke={SIGNAL.onTile}
          strokeWidth="2.2"
          strokeLinecap="round"
          d="M5.5 12.5a6.5 6.5 0 1 1 6.5 6.5M12 8.5a3.5 3.5 0 1 0 3.5 3.5"
        />
      </symbol>
      <symbol id="ws-thunder" viewBox="0 0 24 24">
        <Tile fill={SIGNAL.thunderTile} stroke={SIGNAL.thunderTileEdge} />
        <path fill={SIGNAL.thunderBolt} d="M13.5 3L6 13.5h5l-1.5 7.5L18 9.5h-5z" />
      </symbol>
      <symbol id="ws-landslip" viewBox="0 0 24 24">
        <path fill={SIGNAL.landslipEarth} d="M1 21L9.5 6l4.5 7 3-3.5L23 21z" />
        <circle cx="16" cy="6" r="1.6" fill={SIGNAL.landslipDebris} />
        <circle cx="19.5" cy="9.5" r="1.3" fill={SIGNAL.landslipDebris} />
        <circle cx="14" cy="4" r="1" fill={SIGNAL.landslipDebris} />
      </symbol>
      <symbol id="ws-flood" viewBox="0 0 24 24">
        <Tile fill={SIGNAL.floodTile} />
        <path
          fill="none"
          stroke={SIGNAL.onTile}
          strokeWidth="2"
          strokeLinecap="round"
          d="M4 10c2.5-2.5 5.5 2.5 8 0s5.5 2.5 8 0M4 15.5c2.5-2.5 5.5 2.5 8 0s5.5 2.5 8 0"
        />
      </symbol>
      <symbol id="ws-frost" viewBox="0 0 24 24">
        <Tile fill={SIGNAL.frostTile} />
        <path
          fill="none"
          stroke={SIGNAL.onTile}
          strokeWidth="1.8"
          strokeLinecap="round"
          d="M12 4.5v15M4.5 12h15M6.7 6.7l10.6 10.6M17.3 6.7L6.7 17.3"
        />
      </symbol>
      <symbol id="ws-fire-y" viewBox="0 0 24 24">
        <path
          fill={SIGNAL.fireYellow}
          d="M12 1.5c1 4.5 6 6.5 6 12a6 6 0 0 1-12 0c0-2.4 1.2-3.8 2.5-5 0 2.2 1 3.5 2.3 3.5.2-4.2-.8-6.5 1.2-10.5z"
        />
        <path
          fill={SIGNAL.fireYellowCore}
          d="M12 12c.8 2 2.6 3 2.6 5a2.6 2.6 0 0 1-5.2 0c0-1.5 1.2-2.2 1.5-3.3.2.9.6 1.3 1.1 1.3 0-1.4-.3-2 0-3z"
        />
      </symbol>
      <symbol id="ws-fire-r" viewBox="0 0 24 24">
        <path
          fill={SIGNAL.fireRed}
          d="M12 1.5c1 4.5 6 6.5 6 12a6 6 0 0 1-12 0c0-2.4 1.2-3.8 2.5-5 0 2.2 1 3.5 2.3 3.5.2-4.2-.8-6.5 1.2-10.5z"
        />
        <path
          fill={SIGNAL.fireRedCore}
          d="M12 12c.8 2 2.6 3 2.6 5a2.6 2.6 0 0 1-5.2 0c0-1.5 1.2-2.2 1.5-3.3.2.9.6 1.3 1.1 1.3 0-1.4-.3-2 0-3z"
        />
      </symbol>
      <symbol id="ws-cold" viewBox="0 0 24 24">
        <Tile fill={SIGNAL.coldTile} />
        <path fill={SIGNAL.onTile} d="M10 5a2 2 0 0 1 4 0v8.3a4 4 0 1 1-4 0z" />
        <circle cx="12" cy="16.5" r="2.2" fill={SIGNAL.coldTile} />
        <path stroke={SIGNAL.coldTile} strokeWidth="1.6" d="M12 16.5V10" />
      </symbol>
      <symbol id="ws-hot" viewBox="0 0 24 24">
        <Tile fill={SIGNAL.hotTile} />
        <circle cx="12" cy="12" r="3.6" fill={SIGNAL.onTile} />
        <path
          stroke={SIGNAL.onTile}
          strokeWidth="1.8"
          strokeLinecap="round"
          d="M12 4v2.3M12 17.7V20M4 12h2.3M17.7 12H20M6.3 6.3l1.7 1.7M16 16l1.7 1.7M17.7 6.3L16 8M8 16l-1.7 1.7"
        />
      </symbol>
      <symbol id="ws-tsunami" viewBox="0 0 24 24">
        <Tile fill={SIGNAL.tsunamiTile} />
        <path fill={SIGNAL.onTile} d="M3 18c2-6 6-9 11-9-3 1-4 4-3 6 3-1 6 0 9 3H3z" />
      </symbol>
    </svg>
  );
}
