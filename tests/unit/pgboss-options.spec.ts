import { PgBossQueue } from "../../src/queue/pg-boss-queue";
import { duration, TimeUnit } from "../../src/duration";

describe("PgBossQueue option mapping (no DB required)", () => {
  it("maps startAfter Duration to a future Date", () => {
    const before = Date.now();
    const options = PgBossQueue.toSendOptions({
      startAfter: duration(5, TimeUnit.MINUTES),
    });
    expect(options.startAfter).toBeInstanceOf(Date);
    const at = (options.startAfter as Date).getTime();
    expect(at).toBeGreaterThanOrEqual(before + 5 * 60 * 1000 - 1000);
    expect(at).toBeLessThanOrEqual(Date.now() + 5 * 60 * 1000 + 1000);
  });

  it("passes startAt Dates through", () => {
    const at = new Date(Date.now() + 60_000);
    const options = PgBossQueue.toSendOptions({ startAt: at });
    expect(options.startAfter).toBe(at);
  });

  it("rejects startAt + startAfter together", () => {
    expect(() =>
      PgBossQueue.toSendOptions({
        startAt: new Date(),
        startAfter: duration(1, TimeUnit.SECONDS),
      }),
    ).toThrow("mutually exclusive");
  });

  it("converts retryDelay to whole seconds (ceil) and expireInMinutes to seconds", () => {
    const options = PgBossQueue.toSendOptions({
      retryLimit: 3,
      retryDelay: duration(1500, TimeUnit.MILLISECONDS),
      expireInMinutes: 2,
    });
    expect(options.retryLimit).toBe(3);
    expect(options.retryDelay).toBe(2);
    expect(options.expireInSeconds).toBe(120);
  });

  it("strips undefined fields (pg-boss validates present keys)", () => {
    const options = PgBossQueue.toSendOptions({ priority: 5 });
    expect(options).toStrictEqual({ priority: 5 });
    expect("retryDelay" in options).toBe(false);
    expect("startAfter" in options).toBe(false);
  });

  it("maps schedule timezone with default fallback", () => {
    expect(PgBossQueue.toScheduleOptions({ priority: 1 }, "UTC")).toStrictEqual({
      priority: 1,
      tz: "UTC",
    });
    expect(
      PgBossQueue.toScheduleOptions({ priority: 1, timezone: "America/Los_Angeles" }, "UTC"),
    ).toStrictEqual({ priority: 1, tz: "America/Los_Angeles" });
    expect(PgBossQueue.toScheduleOptions({})).toStrictEqual({});
  });

  it("requires connection info up front", () => {
    expect(() => new PgBossQueue()).toThrow("requires `connectionString` or `bossOptions`");
  });

  it("rejects cancel for ids it did not publish", async () => {
    const queue = new PgBossQueue({ connectionString: "postgres://localhost:5432/db" });
    await expect(queue.cancel("nope")).rejects.toThrow("Unknown job id");
  });
});
