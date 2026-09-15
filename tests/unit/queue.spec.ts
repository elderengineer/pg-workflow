import { InMemoryQueue } from "../../src/queue/in-memory-queue";
import type { QueueEvent } from "../../src/queue";
import { duration, TimeUnit } from "../../src/duration";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("InMemoryQueue", () => {
  it("delivers published events to subscribers", async () => {
    const queue = new InMemoryQueue();
    const got = deferred<unknown>();
    await queue.subscribe("q", async (event: QueueEvent<unknown>) => {
      got.resolve(event.message);
    });
    await queue.publish("q", { hello: "world" });
    await expect(got.promise).resolves.toStrictEqual({ hello: "world" });
  });

  it("delays delivery by startAfter without blocking publish", async () => {
    const queue = new InMemoryQueue();
    const startedAt = Date.now();
    const got = deferred<number>();
    await queue.subscribe("q", async () => {
      got.resolve(Date.now() - startedAt);
    });
    const id = await queue.publish("q", "late", {
      startAfter: duration(120, TimeUnit.MILLISECONDS),
    });
    expect(typeof id).toBe("string");
    // publish() returned immediately; delivery happened after the delay.
    expect(Date.now() - startedAt).toBeLessThan(120);
    const elapsed = await got.promise;
    expect(elapsed).toBeGreaterThanOrEqual(90);
  });

  it("accepts duration inputs as ISO-8601 strings and Temporal-like objects", async () => {
    const queue = new InMemoryQueue();
    const stamps: number[] = [];
    const done = deferred<void>();
    const startedAt = Date.now();
    await queue.subscribe("q", async () => {
      stamps.push(Date.now() - startedAt);
      if (stamps.length === 2) done.resolve();
    });
    await queue.publish("q", "iso", { startAfter: "PT0.1S" });
    await queue.publish("q", "temporal", {
      startAfter: { total: () => 100 },
    });
    await done.promise;
    expect(stamps).toHaveLength(2);
    expect(Math.min(...stamps)).toBeGreaterThanOrEqual(80);
  });

  it("cancel() prevents delayed delivery", async () => {
    const queue = new InMemoryQueue();
    let delivered = false;
    await queue.subscribe("q", async () => {
      delivered = true;
    });
    const id = await queue.publish("q", "never", {
      startAfter: duration(60, TimeUnit.MILLISECONDS),
    });
    await queue.cancel(id);
    await new Promise((resolve) => setTimeout(resolve, 120));
    expect(delivered).toBe(false);
  });

  it("records schedules and fires them on demand", async () => {
    const queue = new InMemoryQueue();
    const got = deferred<unknown>();
    await queue.subscribe("cron-q", async (event: QueueEvent<unknown>) => {
      got.resolve(event.message);
    });
    await queue.schedule("cron-q", "* * * * *", { tick: 1 });
    expect(queue.pendingSchedules()).toStrictEqual(["cron-q"]);
    await queue.fireSchedule("cron-q");
    await expect(got.promise).resolves.toStrictEqual({ tick: 1 });
    await queue.unschedule("cron-q");
    expect(queue.pendingSchedules()).toStrictEqual([]);
    await expect(queue.fireSchedule("cron-q")).resolves.toBeNull();
  });

  it("routes subscriber errors to onError instead of rejecting", async () => {
    const seen: Array<{ error: unknown; queue: string }> = [];
    const queue = new InMemoryQueue({
      onError: (error, event) => seen.push({ error, queue: event.queue }),
    });
    await queue.subscribe("q", async () => {
      throw new Error("boom");
    });
    await queue.publish("q", "x");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(seen).toHaveLength(1);
    expect(seen[0].queue).toBe("q");
    expect(String(seen[0].error)).toContain("boom");
  });

  it("rejects publish after stop", async () => {
    const queue = new InMemoryQueue();
    await queue.stop();
    expect(queue.isStopped()).toBe(true);
    await expect(queue.publish("q", "x")).rejects.toThrow("Queue stopped.");
  });

  it("waits for in-flight handlers on graceful stop", async () => {
    const queue = new InMemoryQueue();
    let finished = false;
    await queue.subscribe("q", async () => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      finished = true;
    });
    await queue.publish("q", "x");
    await new Promise((resolve) => setTimeout(resolve, 5)); // let delivery start
    await queue.stop({ graceful: true });
    expect(finished).toBe(true);
  });

  it("reports an approximate backlog via stats()", async () => {
    const queue = new InMemoryQueue();
    await queue.subscribe("q", async () => undefined);
    await queue.publish("q", "deferred", { startAfter: duration(5, TimeUnit.SECONDS) });
    await expect(queue.stats("q")).resolves.toMatchObject({
      queued: 1,
      deferred: 1,
      ready: 0,
      failed: 0,
    });
    await queue.stop({ graceful: false });
  });

  it("does not wait when graceful is false", async () => {
    const queue = new InMemoryQueue();
    let finished = false;
    await queue.subscribe("q", async () => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      finished = true;
    });
    await queue.publish("q", "x");
    await new Promise((resolve) => setTimeout(resolve, 5));
    await queue.stop({ graceful: false });
    expect(finished).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 60)); // drain
  });
});
