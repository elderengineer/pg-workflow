/**
 * Duration values.
 *
 * The engine only needs one thing from a duration: how many milliseconds it is.
 * Rather than depending on a date library, options accept any of:
 *
 *   - a `Duration` value, e.g. `duration(30, TimeUnit.SECONDS)`
 *   - a `number`, interpreted as milliseconds
 *   - an ISO-8601 duration string, e.g. `"PT30S"`, `"P1DT12H"`
 *   - a Temporal-like object (native `Temporal.Duration` or a polyfill)
 *
 * Calendar units (years, months) are rejected: they are not a fixed number of
 * milliseconds. Temporal only becomes native in Node 26, so a Node 24 baseline
 * uses the other forms and can switch later without an API change.
 */

export enum TimeUnit {
  DAYS = 0,
  HOURS = 1,
  MINUTES = 2,
  SECONDS = 3,
  MILLISECONDS = 4,
  MICROSECONDS = 5,
}

const MAX_US = Number.MAX_SAFE_INTEGER;
const MAX_MS = MAX_US / 1000;
const MAX_S = MAX_MS / 1000;
const MAX_MIN = MAX_S / 60;
const MAX_H = MAX_MIN / 60;
const MAX_D = MAX_H / 24;

const MAX_VALUES: Record<string, number> = {
  [TimeUnit.DAYS]: MAX_D,
  [TimeUnit.HOURS]: MAX_H,
  [TimeUnit.MINUTES]: MAX_MIN,
  [TimeUnit.SECONDS]: MAX_S,
  [TimeUnit.MILLISECONDS]: MAX_MS,
  [TimeUnit.MICROSECONDS]: MAX_US,
};

// conversion factor from unit[i] -> unit[i+1]:
// days->hours 24, hours->minutes 60, minutes->seconds 60,
// seconds->millis 1000, millis->micros 1000
const UNIT_CONVERSION = [24, 60, 60, 1000, 1000];

export interface Duration {
  readonly length: number;
  readonly unit: TimeUnit;
  toMillis(): number;
  toSeconds(): number;
  toMinutes(): number;
  toHours(): number;
  toDays(): number;
  toUnit(unit: TimeUnit): number;
}

/**
 * Structural shape of `Temporal.Duration#total`, so a native or polyfilled
 * Temporal duration can be passed without this package depending on it.
 */
export interface TemporalDurationLike {
  total(totalOf: string | { unit: string; relativeTo?: unknown }): number;
}

/**
 * A `Duration` whose methods were stripped by JSON serialization
 * (`{ length, unit }`). Accepted so values carried through `context.options`
 * keep working after a round-trip.
 */
export interface SerializedDuration {
  length: number;
  unit: TimeUnit;
}

/** Any value accepted where a duration is expected. */
export type DurationInput = Duration | TemporalDurationLike | SerializedDuration | number | string;

export class FiniteDuration implements Duration {
  readonly length: number;
  readonly unit: TimeUnit;

  constructor(length: number, unit: TimeUnit) {
    const max = MAX_VALUES[String(unit)];
    if (length > max) {
      throw new Error(`${length} ${TimeUnit[unit]} exceeds max ${max}`);
    }
    this.length = length;
    this.unit = unit;
  }

  toUnit(unit: TimeUnit): number {
    if (this.unit === unit) return this.length;
    const lo = Math.min(this.unit, unit);
    const hi = Math.max(this.unit, unit);
    const multiplier = UNIT_CONVERSION.slice(lo, hi).reduce((a, b) => a * b, 1);
    if (this.unit < unit) return this.length * multiplier;
    return Math.trunc(this.length / multiplier);
  }

  toMillis(): number {
    return this.toUnit(TimeUnit.MILLISECONDS);
  }
  toSeconds(): number {
    return this.toUnit(TimeUnit.SECONDS);
  }
  toMinutes(): number {
    return this.toUnit(TimeUnit.MINUTES);
  }
  toHours(): number {
    return this.toUnit(TimeUnit.HOURS);
  }
  toDays(): number {
    return this.toUnit(TimeUnit.DAYS);
  }

  /** ISO-8601 representation, e.g. `"PT5M"`. */
  toIso(): string {
    const sign = this.length < 0 ? "-" : "";
    const amount = Math.abs(this.length);
    switch (this.unit) {
      case TimeUnit.DAYS:
        return `${sign}P${amount}D`;
      case TimeUnit.HOURS:
        return `${sign}PT${amount}H`;
      case TimeUnit.MINUTES:
        return `${sign}PT${amount}M`;
      case TimeUnit.SECONDS:
        return `${sign}PT${amount}S`;
      case TimeUnit.MILLISECONDS:
        return `${sign}PT${amount / 1000}S`;
      case TimeUnit.MICROSECONDS:
        return `${sign}PT${amount / 1_000_000}S`;
      default:
        throw new Error(`Unsupported time unit: ${this.unit}`);
    }
  }

  /**
   * Wire form used by `JSON.stringify`. Durations travel inside
   * `context.options`, which is serialized on every hop, so this keeps them
   * self-describing (`"PT5M"`) instead of leaking `{ length, unit }`.
   */
  toJSON(): string {
    return this.toIso();
  }
}

export function duration(length: number, unit: TimeUnit): FiniteDuration {
  return new FiniteDuration(length, unit);
}

export function isDuration(value: unknown): value is Duration {
  const candidate = value as Duration | undefined;
  return typeof candidate?.toMillis === "function" && typeof candidate?.length === "number";
}

const ISO_8601_DURATION =
  /^([+-])?P(?:(\d+(?:[.,]\d+)?)Y)?(?:(\d+(?:[.,]\d+)?)M)?(?:(\d+(?:[.,]\d+)?)W)?(?:(\d+(?:[.,]\d+)?)D)?(?:T(?:(\d+(?:[.,]\d+)?)H)?(?:(\d+(?:[.,]\d+)?)M)?(?:(\d+(?:[.,]\d+)?)S)?)?$/;

/**
 * Parse an ISO-8601 duration (`"PT30S"`, `"P1DT12H"`) into a `FiniteDuration`.
 * Years and months are rejected — their length is not fixed.
 */
export function parseIsoDuration(text: string): FiniteDuration {
  const match = ISO_8601_DURATION.exec(text.trim());
  const [, sign, years, months, weeks, days, hours, minutes, seconds] = match ?? [];
  const components = [years, months, weeks, days, hours, minutes, seconds];
  if (!match || components.every((part) => part === undefined)) {
    throw new SyntaxError(`Invalid ISO-8601 duration: "${text}"`);
  }
  if (years !== undefined || months !== undefined) {
    throw new RangeError(
      `Cannot convert "${text}" to milliseconds: years and months are not fixed-length. Use weeks or days.`,
    );
  }
  const value = (part: string | undefined): number =>
    part === undefined ? 0 : Number(part.replace(",", "."));
  const totalSeconds =
    (value(weeks) * 7 + value(days)) * 86_400 +
    value(hours) * 3_600 +
    value(minutes) * 60 +
    value(seconds);
  return duration(Math.round(totalSeconds * 1000) * (sign === "-" ? -1 : 1), TimeUnit.MILLISECONDS);
}

/** Milliseconds for any accepted duration input; `undefined` stays `undefined`. */
export function toMillis(value: DurationInput | null | undefined): number | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "number") return assertFinite(value, "number");
  if (typeof value === "string") return parseIsoDuration(value).toMillis();
  const candidate = value as Duration & TemporalDurationLike;
  if (typeof candidate.toMillis === "function") {
    return assertFinite(candidate.toMillis(), "Duration");
  }
  if (typeof candidate.total === "function") {
    return assertFinite(candidate.total({ unit: "millisecond" }), "Temporal-like duration");
  }
  // Rehydrate a Duration that lost its prototype to JSON serialization
  // (`{ length, unit }`), e.g. one carried through `context.options`.
  if (typeof candidate.length === "number" && typeof candidate.unit === "number") {
    return new FiniteDuration(candidate.length, candidate.unit as TimeUnit).toMillis();
  }
  throw new TypeError(
    "Unsupported duration: expected a Duration, number, ISO-8601 string, or Temporal-like object.",
  );
}

function assertFinite(value: number, source: string): number {
  if (!Number.isFinite(value)) {
    throw new RangeError(
      `Duration from ${source} is not a finite number of milliseconds: ${value}`,
    );
  }
  return value;
}
