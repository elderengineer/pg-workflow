import {
  duration,
  isDuration,
  parseIsoDuration,
  TemporalDurationLike,
  TimeUnit,
  toMillis,
} from "../../src/duration";

/** Stand-in for `Temporal.Duration` (native only from Node 26). */
function temporalLike(millis: number): TemporalDurationLike {
  return {
    total: (totalOf) => {
      if (typeof totalOf === "string") return millis;
      return totalOf.unit === "millisecond" || totalOf.unit === "milliseconds" ? millis : millis;
    },
  };
}

describe("duration", () => {
  it("converts between units", () => {
    expect(duration(5, TimeUnit.MINUTES).toMillis()).toBe(300_000);
    expect(duration(2, TimeUnit.HOURS).toSeconds()).toBe(7_200);
    expect(duration(1, TimeUnit.DAYS).toHours()).toBe(24);
    expect(duration(1500, TimeUnit.MILLISECONDS).toSeconds()).toBe(1);
  });

  it("identifies Duration values", () => {
    expect(isDuration(duration(1, TimeUnit.SECONDS))).toBe(true);
    expect(isDuration(temporalLike(1000))).toBe(false);
    expect(isDuration(1000)).toBe(false);
    expect(isDuration("PT1S")).toBe(false);
    expect(isDuration(undefined)).toBe(false);
  });

  describe("toMillis", () => {
    it("accepts a Duration", () => {
      expect(toMillis(duration(5, TimeUnit.MINUTES))).toBe(300_000);
    });

    it("accepts a number as milliseconds", () => {
      expect(toMillis(1500)).toBe(1500);
      expect(toMillis(0)).toBe(0);
    });

    it("accepts an ISO-8601 string", () => {
      expect(toMillis("PT30S")).toBe(30_000);
      expect(toMillis("PT5M")).toBe(300_000);
      expect(toMillis("P1DT2H3M4S")).toBe(86_400_000 + 7_200_000 + 180_000 + 4_000);
      expect(toMillis("P1W")).toBe(604_800_000);
      expect(toMillis("PT0.5S")).toBe(500);
      expect(toMillis("PT1,5S")).toBe(1500);
      expect(toMillis("-PT1M")).toBe(-60_000);
      expect(toMillis("  PT1S  ")).toBe(1000);
    });

    it("accepts a Temporal-like object", () => {
      expect(toMillis(temporalLike(2500))).toBe(2500);
    });

    it("accepts any object exposing toMillis()", () => {
      expect(toMillis({ length: 1, unit: TimeUnit.SECONDS, toMillis: () => 1000 } as never)).toBe(
        1000,
      );
    });

    it("returns undefined for null/undefined", () => {
      expect(toMillis(undefined)).toBeUndefined();
      expect(toMillis(null)).toBeUndefined();
    });

    it("rejects calendar units", () => {
      expect(() => toMillis("P1M")).toThrow(/not fixed-length/);
      expect(() => toMillis("P1Y")).toThrow(/not fixed-length/);
    });

    it("rejects malformed strings and types", () => {
      expect(() => toMillis("banana")).toThrow(/Invalid ISO-8601 duration/);
      expect(() => toMillis("P")).toThrow(/Invalid ISO-8601 duration/);
      expect(() => toMillis("PT")).toThrow(/Invalid ISO-8601 duration/);
      expect(() => toMillis({} as never)).toThrow(/Unsupported duration/);
      expect(() => toMillis(Number.NaN)).toThrow(/not a finite number/);
      expect(() => toMillis(Number.POSITIVE_INFINITY)).toThrow(/not a finite number/);
    });
  });

  describe("parseIsoDuration", () => {
    it("returns a FiniteDuration", () => {
      expect(parseIsoDuration("PT2H").toMinutes()).toBe(120);
    });
  });

  describe("serialization round-trip", () => {
    it("serializes as ISO-8601, not {length, unit}", () => {
      expect(JSON.stringify(duration(5, TimeUnit.MINUTES))).toBe('"PT5M"');
      expect(JSON.stringify(duration(90, TimeUnit.SECONDS))).toBe('"PT90S"');
      expect(duration(5, TimeUnit.MINUTES).toIso()).toBe("PT5M");
      expect(duration(2, TimeUnit.HOURS).toIso()).toBe("PT2H");
      expect(duration(3, TimeUnit.DAYS).toIso()).toBe("P3D");
      expect(duration(1500, TimeUnit.MILLISECONDS).toIso()).toBe("PT1.5S");
      expect(duration(-30, TimeUnit.SECONDS).toIso()).toBe("-PT30S");
    });

    it("survives a JSON round-trip, as happens in context.options", () => {
      const roundTripped = JSON.parse(JSON.stringify(duration(5, TimeUnit.MINUTES)));
      expect(toMillis(roundTripped)).toBe(300_000);
    });

    it("still rehydrates the legacy {length, unit} form", () => {
      expect(toMillis({ length: 5, unit: TimeUnit.MINUTES })).toBe(300_000);
      expect(toMillis({ length: 2, unit: TimeUnit.HOURS })).toBe(7_200_000);
    });
  });
});
