import { toMillis, type DurationInput } from "./duration";

/**
 * A wall-clock time on a given weekday, resolved in an IANA timezone.
 *
 * ```
 * // 8:00 local time, next Friday (or today if it is still before 8:00)
 * const friday8am: LocalDayTime = { dayOfTheWeek: 5, time: duration(8, TimeUnit.HOURS) };
 * ```
 */
export interface LocalDayTime {
  /** ISO weekday: 1 = Monday … 7 = Sunday. `0` means "the next day". */
  dayOfTheWeek: number;
  /** Offset from local midnight; must be within `[00:00, 24:00)`. */
  time: DurationInput;
}

const MS_PER_DAY = 86_400_000;
const MS_PER_HOUR = 3_600_000;
const MS_PER_MINUTE = 60_000;

interface WallClock {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  milli: number;
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function formatterFor(timeZone: string): Intl.DateTimeFormat {
  let formatter = formatters.get(timeZone);
  if (!formatter) {
    try {
      formatter = new Intl.DateTimeFormat("en-US", {
        timeZone,
        hourCycle: "h23",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
    } catch {
      throw new RangeError(`Invalid IANA timezone: "${timeZone}"`);
    }
    formatters.set(timeZone, formatter);
  }
  return formatter;
}

function partsInZone(instant: Date, timeZone: string): WallClock {
  const parts = formatterFor(timeZone).formatToParts(instant);
  const value = (type: Intl.DateTimeFormatPartTypes): number =>
    Number(parts.find((part) => part.type === type)?.value);
  return {
    year: value("year"),
    month: value("month"),
    day: value("day"),
    hour: value("hour"),
    minute: value("minute"),
    second: value("second"),
    // Timezone offsets are whole minutes, so the millisecond component is shared.
    milli: instant.getUTCMilliseconds(),
  };
}

function isoWeekday(utcMidnight: number): number {
  return ((new Date(utcMidnight).getUTCDay() + 6) % 7) + 1;
}

/** Instant whose wall clock in `timeZone` equals `wall`. Two passes settle DST. */
function wallClockToUtc(wall: WallClock, timeZone: string): number {
  const desired = Date.UTC(
    wall.year,
    wall.month - 1,
    wall.day,
    wall.hour,
    wall.minute,
    wall.second,
    wall.milli,
  );
  let instant = desired;
  for (let pass = 0; pass < 2; pass++) {
    const actual = partsInZone(new Date(instant), timeZone);
    instant +=
      desired -
      Date.UTC(
        actual.year,
        actual.month - 1,
        actual.day,
        actual.hour,
        actual.minute,
        actual.second,
        actual.milli,
      );
  }
  return instant;
}

/**
 * The next instant strictly after `now` whose wall clock in `timezone` matches
 * `startAt`. `timezone` defaults to UTC.
 *
 * @throws RangeError for an invalid timezone or a time outside `[00:00, 24:00)`.
 */
export function nextLocalDayTime(
  startAt: LocalDayTime,
  timezone = "UTC",
  now: Date = new Date(),
): Date {
  formatterFor(timezone); // fail fast on a bad timezone
  const offset = toMillis(startAt.time) ?? 0;
  if (!Number.isFinite(offset) || offset < 0 || offset >= MS_PER_DAY) {
    throw new RangeError(`LocalDayTime.time must be within [00:00, 24:00), got ${offset}ms`);
  }
  const { dayOfTheWeek } = startAt;
  if (!Number.isInteger(dayOfTheWeek) || dayOfTheWeek < 0 || dayOfTheWeek > 7) {
    throw new RangeError(`LocalDayTime.dayOfTheWeek must be 0..7, got ${dayOfTheWeek}`);
  }

  const wall: WallClock = {
    year: 0,
    month: 1,
    day: 1,
    hour: Math.floor(offset / MS_PER_HOUR),
    minute: Math.floor((offset % MS_PER_HOUR) / MS_PER_MINUTE),
    second: Math.floor((offset % MS_PER_MINUTE) / 1000),
    milli: offset % 1000,
  };

  const today = partsInZone(now, timezone);
  const todayIso = isoWeekday(Date.UTC(today.year, today.month - 1, today.day));
  const target = dayOfTheWeek === 0 ? (todayIso % 7) + 1 : ((dayOfTheWeek - 1) % 7) + 1;

  for (let add = 0; add <= 7; add++) {
    const date = new Date(Date.UTC(today.year, today.month - 1, today.day + add));
    wall.year = date.getUTCFullYear();
    wall.month = date.getUTCMonth() + 1;
    wall.day = date.getUTCDate();
    if (isoWeekday(Date.UTC(wall.year, wall.month - 1, wall.day)) !== target) continue;
    const instant = wallClockToUtc(wall, timezone);
    if (instant > now.getTime()) return new Date(instant);
  }
  throw new Error("Could not resolve the next local day/time within a week.");
}
