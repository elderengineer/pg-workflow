import { randomUUID } from "node:crypto";
import type {
  DeliveryScope,
  QueueEvent,
  PublishOptions,
  Queue,
  QueueName,
  QueueStats,
  ScheduleOptions,
  StopOptions,
} from "../queue";
import { toMillis } from "../duration";
import { nextLocalDayTime } from "../local-time";

type Handler = (event: QueueEvent<unknown>) => Promise<void>;

export interface InMemoryQueueOptions {
  /** Called when a subscriber throws. Defaults to `console.error`. */
  onError?: (error: unknown, event: QueueEvent<unknown>) => void;
}

/**
 * In-memory Queue for tests, local dev, and non-Postgres deployments.
 * Supports publish/subscribe/cancel + startAfter/startAt delays.
 * `schedule()` records the cron entry without firing — call
 * `fireSchedule(queue)` in tests or wire a real cron driver.
 */
export class InMemoryQueue implements Queue {
  private handlers = new Map<QueueName, Handler[]>();
  private timers = new Map<string, NodeJS.Timeout>();
  private schedules = new Map<
    QueueName,
    { cron: string; event: unknown; options?: ScheduleOptions }
  >();
  private stopped = false;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly onError: (error: unknown, event: QueueEvent<unknown>) => void;

  constructor(options: InMemoryQueueOptions = {}) {
    this.onError =
      options.onError ??
      ((error, event) =>
        console.error(`[InMemoryQueue] handler failed for ${event.queue}:`, error));
  }

  isStopped(): boolean {
    return this.stopped;
  }

  async start(): Promise<void> {
    // Nothing to do: the in-memory backend is always ready.
  }

  async stop(options?: StopOptions): Promise<void> {
    this.stopped = true;
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
    if (options?.graceful === false || this.inFlight.size === 0) return;
    const timeout = toMillis(options?.timeout);
    const pending = Promise.all([...this.inFlight]);
    if (timeout === undefined) {
      await pending;
      return;
    }
    await Promise.race([
      pending,
      new Promise<void>((resolve) => {
        setTimeout(resolve, timeout).unref();
      }),
    ]);
  }

  /** Track a delivery so `stop({ graceful: true })` can await it. */
  private track(task: Promise<void>): void {
    this.inFlight.add(task);
    void task.finally(() => this.inFlight.delete(task));
  }

  async publish(queue: QueueName, event: unknown, options: PublishOptions = {}): Promise<string> {
    if (this.stopped) throw new Error("Queue stopped.");
    const id = `mem-${randomUUID()}`;
    const delay = this.delayMs(options);
    const envelope: QueueEvent<unknown> = { queue, id, message: event, createTs: Date.now() };
    if (delay > 0) {
      this.timers.set(
        id,
        setTimeout(() => {
          this.timers.delete(id);
          this.track(this.deliver(envelope));
        }, delay),
      );
      return id;
    }
    // Async handoff so trigger() never runs user code synchronously.
    queueMicrotask(() => {
      this.track(this.deliver(envelope));
    });
    return id;
  }

  async cancel(id: string): Promise<void> {
    const timer = this.timers.get(id);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(id);
    }
  }

  async schedule(
    queue: QueueName,
    cron: string,
    event: unknown,
    options: ScheduleOptions = {},
  ): Promise<void> {
    this.schedules.set(queue, { cron, event, options });
  }

  async unschedule(queue: QueueName): Promise<void> {
    this.schedules.delete(queue);
  }

  async subscribe<T>(
    queue: QueueName,
    subscriber: (event: QueueEvent<T>, scope?: DeliveryScope) => Promise<void>,
  ): Promise<void> {
    const list = this.handlers.get(queue) ?? [];
    list.push(subscriber as Handler);
    this.handlers.set(queue, list);
  }

  /** Test helper: manually fire a recorded cron entry as if the scheduler ticked. */
  async fireSchedule(queue: QueueName): Promise<string | null> {
    const entry = this.schedules.get(queue);
    if (!entry) return null;
    return this.publish(queue, entry.event, { priority: entry.options?.priority });
  }

  pendingSchedules(): string[] {
    return [...this.schedules.keys()];
  }

  /** Approximate backlog: pending timers are deferred, in-flight deliveries active. */
  async stats(_queue?: QueueName): Promise<QueueStats | null> {
    return {
      queued: this.timers.size,
      ready: 0,
      active: this.inFlight.size,
      deferred: this.timers.size,
      failed: 0,
      total: this.timers.size + this.inFlight.size,
    };
  }

  private async deliver(envelope: QueueEvent<unknown>): Promise<void> {
    let subscribers = this.handlers.get(envelope.queue) ?? [];
    if (subscribers.length === 0) {
      // No subscriber yet (e.g. trigger before start in tests) — keep parity
      // with durable backends by retrying briefly instead of dropping.
      await new Promise((resolve) => setTimeout(resolve, 5));
      subscribers = this.handlers.get(envelope.queue) ?? [];
    }
    await Promise.all(
      subscribers.map(async (subscriber) => {
        try {
          await subscriber(envelope);
        } catch (error) {
          this.onError(error, envelope);
        }
      }),
    );
  }

  private delayMs(options: PublishOptions): number {
    const after = toMillis(options.startAfter);
    if (after !== undefined) return Math.max(0, after);
    const { startAt } = options;
    if (startAt instanceof Date) return Math.max(0, startAt.getTime() - Date.now());
    if (startAt && typeof startAt === "object") {
      const target = nextLocalDayTime(startAt, options.timezone ?? "UTC");
      return Math.max(0, target.getTime() - Date.now());
    }
    return 0;
  }
}

export type { StopOptions };
