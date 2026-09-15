/**
 * Python's `round`, reproduced exactly (AU-009).
 *
 * `src/data/stats.ts` and `src/lib/downsample.ts` are ports of
 * `run365days.analytics.stats` and `run365days.analytics.builder`, and both
 * modes of the dashboard are meant to show a visitor the same figures. They did
 * not, because the two languages break a tie in opposite directions:
 *
 *     Python  round(500.5) -> 500       round(2.5) -> 2   round(6.125, 2) -> 6.12
 *     JS      Math.round(500.5) -> 501  Math.round(2.5) -> 3
 *
 * Python sends a half to the EVEN neighbour; `Math.round` sends it toward
 * +Infinity, which on negative numbers is not even away from zero
 * (`Math.round(-0.5)` is -0 where Python answers 0). The cross-language golden
 * caught eight such disagreements. Python cannot move -- `builder.downsample`
 * sits on the byte-identical export path in `src/export/records.py` -- so this
 * side adopts Python's rule.
 *
 * ## Why this is not `Math.round(v * 100) / 100`
 *
 * Python's two-argument `round` rounds the decimal the double really is, not
 * the product of that double with a power of ten. The two are different:
 *
 *     2.675 as a double is 2.674999999999999822...  ->  Python answers 2.67
 *     but 2.675 * 100 rounds UP to exactly 267.5    ->  scaling answers 2.68
 *
 * Multiplying invents the tie that the true value did not have.
 *
 * So the arithmetic below never multiplies by a power of ten in floating point.
 * It recovers the double's exact value as a ratio of two integers, compares
 * against the half in exact integer arithmetic, and hands the digits to
 * `Number`, whose string-to-double conversion is correctly rounded -- the same
 * two steps CPython takes with `_Py_dg_dtoa` and `_Py_dg_strtod`.
 *
 * ## Negative values
 *
 * Handled, and symmetrically: the sign is split off first and reapplied, which
 * is what Python does too, so `roundHalfEven(-2.5)` is -2 and
 * `roundHalfEven(-1.5)` is -2. Nothing here feeds it a negative pace or
 * distance today, but `tsb` (fitness minus fatigue) is routinely negative and
 * goes through the two-argument form.
 */

/** Bits of mantissa an IEEE-754 double carries below the implied leading one. */
const MANTISSA_BITS = 52n;
/** Exponent bias (1023) plus MANTISSA_BITS: turns the stored exponent into a power of two. */
const EXPONENT_BIAS = 1075;
/** Power of two of the smallest subnormal, which has no implied leading one. */
const SUBNORMAL_EXPONENT = -1074;

type Exact = { mantissa: bigint; exponent: number };

/**
 * Split a positive finite double into `mantissa * 2 ** exponent`, exactly.
 *
 * Reading the bits is the only way to get the true value: every decimal
 * rendering JavaScript offers is either shortest-round-trip (`String`) or
 * capped at 100 places (`toFixed`), and neither is the exact expansion, which
 * runs to over a thousand digits for a subnormal.
 */
function exactValue(magnitude: number): Exact {
  const bits = new DataView(new ArrayBuffer(8));
  bits.setFloat64(0, magnitude);
  const high = bits.getUint32(0);
  const low = bits.getUint32(4);
  const stored = (high >>> 20) & 0x7ff;
  const fraction = (BigInt(high & 0xfffff) << 32n) | BigInt(low);
  return stored === 0
    ? { mantissa: fraction, exponent: SUBNORMAL_EXPONENT }
    : { mantissa: fraction | (1n << MANTISSA_BITS), exponent: stored - EXPONENT_BIAS };
}

/** Place the decimal point `ndigits` from the right, padding with leading zeros. */
function withPoint(digits: string, ndigits: number): string {
  if (ndigits === 0) return digits;
  const padded = digits.padStart(ndigits + 1, "0");
  return `${padded.slice(0, -ndigits)}.${padded.slice(-ndigits)}`;
}

function roundToPlaces(value: number, ndigits: number): number {
  if (!Number.isFinite(value)) return value;
  // A whole number is already its own answer at any number of decimal places,
  // and this keeps the common case (a rest day's 0 km) off the exact path.
  if (Number.isInteger(value)) return value;

  const negative = value < 0;
  const { mantissa, exponent } = exactValue(Math.abs(value));
  const scale = 10n ** BigInt(ndigits);

  // magnitude * 10 ** ndigits, as an exact ratio of two integers.
  const numerator = exponent >= 0 ? (mantissa << BigInt(exponent)) * scale : mantissa * scale;
  const denominator = exponent >= 0 ? 1n : 1n << BigInt(-exponent);

  let units = numerator / denominator;
  const twiceRemainder = (numerator % denominator) * 2n;
  const isHalf = twiceRemainder === denominator;
  if (twiceRemainder > denominator || (isHalf && units % 2n === 1n)) units += 1n;

  return Number(`${negative ? "-" : ""}${withPoint(units.toString(), ndigits)}`);
}

/**
 * Round `value` the way Python's `round` does: halves go to the even neighbour.
 *
 * @param value Any number. A non-finite one is handed back unchanged -- Python
 *   raises on those, and nothing here produces one.
 * @param ndigits Decimal places to keep. Omit it for Python's one-argument
 *   form, which yields an int and therefore never negative zero; pass 0 for the
 *   two-argument form, which yields a float and does keep the sign.
 * @returns The rounded value.
 * @throws RangeError if `ndigits` is not a non-negative whole number. Python
 *   accepts a negative count (rounding to tens, hundreds); no caller here wants
 *   that, so it is refused rather than quietly given a second meaning.
 */
export function roundHalfEven(value: number, ndigits?: number): number {
  if (ndigits === undefined) return roundToPlaces(value, 0) + 0; // + 0 turns -0 into 0
  if (!Number.isInteger(ndigits) || ndigits < 0) {
    throw new RangeError(`ndigits must be a non-negative whole number, got ${ndigits}`);
  }
  return roundToPlaces(value, ndigits);
}
