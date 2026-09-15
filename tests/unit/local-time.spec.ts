import { duration, TimeUnit } from "../../src/duration";
import { nextLocalDayTime, type LocalDayTime } from "../../src/local-time";

const friday8am: LocalDayTime = { dayOfTheWeek: 5, time: duration(8, TimeUnit.HOURS) };

describe("nextLocalDayTime", () => {
  it("defaults to UTC", () => {
    const at = nextLocalDayTime(friday8am, undefined, new Date("2026-09-15T00:00:00Z"));
    expect(at.toISOString()).toBe("2026-09-18T08:00:00.000Z");
  });

  it("resolves the same wall clock in a named zone", () => {
    const tokyo = nextLocalDayTime(friday8am, "Asia/Tokyo", new Date("2026-09-15T00:00:00Z"));
    expect(tokyo.toISOString()).toBe("2026-09-17T23:00:00.000Z"); // Fri 08:00 JST
  });

  it("is DST-correct: same wall clock, different UTC offset", () => {
    const winter = nextLocalDayTime(
      friday8am,
      "America/New_York",
      new Date("2026-01-05T00:00:00Z"),
    );
    const summer = nextLocalDayTime(
      friday8am,
      "America/New_York",
      new Date("2026-07-06T00:00:00Z"),
    );
    expect(winter.toISOString()).toBe("2026-01-09T13:00:00.000Z"); // EST, UTC-5
    expect(summer.toISOString()).toBe("2026-07-10T12:00:00.000Z"); // EDT, UTC-4
  });

  it("returns later the same day when the time is still ahead", () => {
    const at = nextLocalDayTime(friday8am, "UTC", new Date("2026-09-18T06:00:00Z"));
    expect(at.toISOString()).toBe("2026-09-18T08:00:00.000Z");
  });

  it("rolls to the next week once the time has passed", () => {
    const at = nextLocalDayTime(friday8am, "UTC", new Date("2026-09-18T09:00:00Z"));
    expect(at.toISOString()).toBe("2026-09-25T08:00:00.000Z");
  });

  it("treats dayOfTheWeek 0 as the next day", () => {
    const at = nextLocalDayTime(
      { dayOfTheWeek: 0, time: 0 },
      "UTC",
      new Date("2026-09-15T12:00:00Z"),
    );
    expect(at.toISOString()).toBe("2026-09-16T00:00:00.000Z");
  });

  it("accepts any duration input for the time of day", () => {
    const at = nextLocalDayTime(
      { dayOfTheWeek: 5, time: "PT8H" },
      "UTC",
      new Date("2026-09-15T00:00:00Z"),
    );
    expect(at.toISOString()).toBe("2026-09-18T08:00:00.000Z");
  });

  it("rejects invalid timezones, times and weekdays", () => {
    expect(() => nextLocalDayTime(friday8am, "Not/AZone")).toThrow(/Invalid IANA timezone/);
    expect(() =>
      nextLocalDayTime({ dayOfTheWeek: 5, time: duration(24, TimeUnit.HOURS) }, "UTC"),
    ).toThrow(/within \[00:00, 24:00\)/);
    expect(() => nextLocalDayTime({ dayOfTheWeek: 9, time: 0 }, "UTC")).toThrow(/dayOfTheWeek/);
  });
});
