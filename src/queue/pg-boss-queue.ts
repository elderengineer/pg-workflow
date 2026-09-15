import type { PgBoss, ConstructorOptions, SendOptions, WorkOptions } from "pg-boss";
import type {
  DeliveryScope,
  QueueEvent,
  LocalDayTime,
  PublishOptions,
  Queue,
  QueueName,
  QueueStats,
  ScheduleOptions,
  StopOptions,
} from "../queue";
import { toMillis } from "../duration";
import { nextLocalDayTime } from "../local-time";
import type { SqlExecutor } from "../sql";

type Envelope = {
  createTs: number;
  message: unknown;
};

/** Per-queue creation options as accepted by pg-boss `createQueue`. */
type QueueDefinitionOptions = NonNullable<Parameters<PgBoss["createQueue"]>[1]>;

/** An operational warning emitted by pg-boss. */
export interface PgBossWarning {
  /** e.g. `queue_backlog`, `slow_query`, `xmin_horizon`. */
  type: string;
  message: string;
  data?: unknown;
}

/** A trimmed, durable view of one job, for diagnostics and admin tooling. */
export interface JobInfo<T = unknown> {
  id: string;
  queue: string;
  state: "created" | "retry" | "active" | "completed" | "cancelled" | "failed";
  retryCount: number;
  retryLimit: number;
  /** The stored envelope (`{ createTs, message }`). */
  data: T;
  /** Handler result, or the error details when the job failed. */
  output: object;
  createdOn: Date;
  completedOn: Date | null;
  startAfter: Date;
}

export interface PgBossQueueOptions {
  /** Postgres connection string. */
  connectionString?: string;
  /** Extra pg-boss constructor options, merged over `connectionString`. */
  bossOptions?: ConstructorOptions;
  /** Default `work()` options applied to every subscription. */
  workOptions?: WorkOptions;
  /**
   * Per-queue creation options applied by every auto-created queue — e.g.
   * `warningQueueSize` (backlog warning threshold), `policy`, `deadLetter`,
   * `retryLimit`. Pass a function to configure queues individually.
   * See the "Capacity and back-pressure" section of
   * [`docs/cluster.md`](../../docs/cluster.md).
   */
  queueOptions?:
    QueueDefinitionOptions | ((queue: QueueName) => QueueDefinitionOptions | undefined);
  /** Default IANA timezone for `schedule()` when none is given. */
  defaultScheduleTimezone?: string;
  onError?: (error: unknown) => void;
  /** Operational warnings, including the queue-backlog (capacity) warning. */
  onWarning?: (warning: PgBossWarning) => void;
}

/**
 * Postgres-backed `Queue` implemented on pg-boss.
 *
 * Option mapping:
 * - `startAfter` (Duration) / `startAt` (Date) → pg-boss `startAfter` date.
 *   `LocalDayTime` is resolved in server-local time.
 * - `retryDelay` (Duration) → seconds (pg-boss resolution; sub-second values
 *   round up to 1s). `retryLimit` passes through.
 * - `expireInMinutes` → `expireInSeconds`.
 * - `undefined` fields are stripped: pg-boss validates present keys.
 */
export class PgBossQueue implements Queue {
  private bossPromise: Promise<PgBoss> | undefined;
  private stopped = false;
  private readonly queuesByJobId = new Map<string, QueueName>();
  private readonly ensuredQueues = new Set<QueueName>();
  private readonly options: PgBossQueueOptions;

  constructor(options: PgBossQueueOptions = {}) {
    if (!options.connectionString && !options.bossOptions) {
      throw new Error("PgBossQueue requires `connectionString` or `bossOptions`.");
    }
    this.options = options;
  }

  /**
   * pg-boss is ESM-only and loaded lazily so that consumers using only
   * `InMemoryQueue` never pay for (or break on) the import.
   */
  private async getBoss(): Promise<PgBoss> {
    if (!this.bossPromise) {
      this.bossPromise = (async () => {
        const { PgBoss: PgBossCtor } = await import("pg-boss");
        let boss: PgBoss;
        if (this.options.bossOptions) {
          const merged: ConstructorOptions = {
            ...(this.options.connectionString
              ? { connectionString: this.options.connectionString }
              : {}),
            ...this.options.bossOptions,
          };
          boss = new PgBossCtor(merged);
        } else {
          boss = new PgBossCtor(this.options.connectionString as string);
        }
        boss.on("error", (error) => {
          if (this.options.onError) {
            this.options.onError(error);
          } else {
            console.error("[PgBossQueue]", error);
          }
        });
        boss.on("warning", (warning) => {
          const raw = warning as { message?: string; data?: { type?: string } } | undefined;
          const payload: PgBossWarning = {
            type: raw?.data?.type ?? "unknown",
            message: raw?.message ?? String(warning),
            data: raw?.data,
          };
          if (this.options.onWarning) {
            this.options.onWarning(payload);
          } else {
            console.warn(`[PgBossQueue] ${payload.type}: ${payload.message}`);
          }
        });
        return boss;
      })();
    }
    return this.bossPromise;
  }

  /** pg-boss v12 requires queues to exist before use; create once per instance. */
  private async ensureQueue(queue: QueueName, seen = new Set<QueueName>()): Promise<void> {
    if (this.ensuredQueues.has(queue) || seen.has(queue)) return;
    seen.add(queue);
    const options = this.resolveQueueOptions(queue);
    // A dead-letter queue must exist before the queue that points at it.
    if (options?.deadLetter && options.deadLetter !== queue) {
      await this.ensureQueue(options.deadLetter, seen);
    }
    await (await this.getBoss()).createQueue(queue, options);
    this.ensuredQueues.add(queue);
  }

  private resolveQueueOptions(queue: QueueName): QueueDefinitionOptions | undefined {
    const configured = this.options.queueOptions;
    return typeof configured === "function" ? configured(queue) : configured;
  }

  isStopped(): boolean {
    return this.stopped;
  }

  async start(): Promise<void> {
    await (await this.getBoss()).start();
  }

  async stop(options?: StopOptions): Promise<void> {
    this.stopped = true;
    // pg-boss v12 `timeout` is milliseconds (default 30000).
    const timeout = toMillis(options?.timeout);
    await (
      await this.getBoss()
    ).stop({
      graceful: options?.graceful ?? true,
      ...(timeout !== undefined ? { timeout } : {}),
    });
  }

  async publish(
    queue: QueueName,
    event: unknown,
    options: PublishOptions = {},
  ): Promise<string | null> {
    await this.ensureQueue(queue);
    const id = await (
      await this.getBoss()
    ).send(
      queue,
      { createTs: Date.now(), message: event } satisfies Envelope,
      PgBossQueue.toSendOptions(options),
    );
    if (id) this.queuesByJobId.set(id, queue);
    return id ?? null;
  }

  async cancel(id: string): Promise<void> {
    // pg-boss addresses cancels by (queue, id); only ids published through
    // this instance can be resolved.
    const queue = this.queuesByJobId.get(id);
    if (!queue) throw new Error(`Unknown job id (not published via this adapter): ${id}`);
    await (await this.getBoss()).cancel(queue, id);
    this.queuesByJobId.delete(id);
  }

  async schedule(
    queue: QueueName,
    cron: string,
    event: unknown,
    options: ScheduleOptions = {},
  ): Promise<void> {
    await this.ensureQueue(queue);
    await (
      await this.getBoss()
    ).schedule(
      queue,
      cron,
      { createTs: Date.now(), message: event } satisfies Envelope,
      PgBossQueue.toScheduleOptions(options, this.options.defaultScheduleTimezone),
    );
  }

  async unschedule(queue: QueueName): Promise<void> {
    await (await this.getBoss()).unschedule(queue);
  }

  async subscribe<T>(
    queue: QueueName,
    subscriber: (event: QueueEvent<T>, scope?: DeliveryScope) => Promise<void>,
  ): Promise<void> {
    await this.ensureQueue(queue);
    await (
      await this.getBoss()
    ).work(queue, this.options.workOptions ?? {}, async (jobs: unknown, tx?: unknown) => {
      // With `workOptions.transactional`, pg-boss passes the job's transaction
      // as the second argument; expose it to the engine as a delivery scope.
      const scope = toDeliveryScope(tx);
      const list = Array.isArray(jobs) ? jobs : [jobs];
      await Promise.all(
        list.map((job) => {
          const envelope = job.data as Envelope | null;
          return subscriber(
            {
              queue: job.name,
              id: job.id,
              message: (envelope?.message ?? null) as T,
              createTs: envelope?.createTs ?? Date.now(),
            },
            scope,
          );
        }),
      );
    });
  }

  /**
   * Backlog snapshot from pg-boss's cached queue row. Counts are
   * **eventually consistent** — pg-boss refreshes them every
   * `queueCacheIntervalSeconds` (default 60s) — so this suits dashboards and
   * alerting, not instantaneous decisions.
   */
  async stats(queue: QueueName): Promise<QueueStats | null> {
    const result = await (await this.getBoss()).getQueue(queue);
    if (!result) return null;
    return {
      queued: result.queuedCount,
      ready: result.readyCount,
      active: result.activeCount,
      deferred: result.deferredCount,
      failed: result.failedCount,
      total: result.totalCount,
    };
  }

  /**
   * Cancel a job by `(queue, id)`. Unlike `Queue.cancel(id)`, this works for
   * ids this process did not publish — use it from admin tooling.
   */
  async cancelJob(queue: QueueName, id: string): Promise<void> {
    await (await this.getBoss()).cancel(queue, id);
    this.queuesByJobId.delete(id);
  }

  /**
   * Read one job's durable state, including the error stored in `output` for a
   * failed job. See [`docs/DEBUG.md`](../../docs/DEBUG.md).
   */
  async getJob<T = unknown>(queue: QueueName, id: string): Promise<JobInfo<T> | null> {
    const job = await (await this.getBoss()).getJobById<T>(queue, id);
    if (!job) return null;
    return {
      id: job.id,
      queue: job.name,
      state: job.state,
      retryCount: job.retryCount,
      retryLimit: job.retryLimit,
      data: job.data,
      output: job.output,
      createdOn: job.createdOn,
      completedOn: job.completedOn,
      startAfter: job.startAfter,
    };
  }

  /**
   * Retry a job that is in the `failed` state by id.
   * See [`docs/DEBUG.md`](../../docs/DEBUG.md).
   */
  async retryJob(queue: QueueName, id: string): Promise<void> {
    await (await this.getBoss()).retry(queue, id);
  }

  /**
   * Change the schedule of an existing job (e.g. a failed step, to run it
   * again later). Only jobs in a pre-active state can be edited; a job that is
   * `active` must finish or be cancelled first.
   */
  async rescheduleJob(queue: QueueName, id: string, options: PublishOptions = {}): Promise<void> {
    const update: NonNullable<Parameters<PgBoss["update"]>[2]> = { id };
    const startAt = toDate(options.startAfter) ?? localDayToDate(options.startAt, options.timezone);
    if (startAt) update.startAfter = startAt;
    if (options.priority !== undefined) update.priority = options.priority;
    const retryDelayMillis = toMillis(options.retryDelay);
    if (retryDelayMillis !== undefined)
      update.retryDelay = Math.max(0, Math.ceil(retryDelayMillis / 1000));
    if (options.expireInMinutes !== undefined) {
      update.expireInSeconds = Math.max(1, Math.round(options.expireInMinutes * 60));
    }
    await (await this.getBoss()).update(queue, undefined, update);
  }

  /**
   * Move jobs from a dead-letter queue back to their source queue for another
   * attempt. Returns the number of jobs moved.
   */
  async redriveDeadLetter(
    deadLetterQueue: QueueName,
    options?: { sourceName?: string; limit?: number },
  ): Promise<number> {
    return (await this.getBoss()).redrive(deadLetterQueue, options);
  }

  static toSendOptions(options: PublishOptions): SendOptions {
    const { startAt, startAfter, retryDelay, timezone, expireInMinutes, ...rest } = options;
    if (startAt && startAfter) {
      throw new Error("'startAt' and 'startAfter' are mutually exclusive options.");
    }
    const sendOptions: SendOptions = { ...rest };
    const startAfterDate = toDate(startAfter) ?? localDayToDate(startAt, timezone);
    if (startAfterDate) sendOptions.startAfter = startAfterDate;
    const retryDelayMillis = toMillis(retryDelay);
    if (retryDelayMillis !== undefined) {
      // pg-boss resolution is whole seconds.
      sendOptions.retryDelay = Math.max(0, Math.ceil(retryDelayMillis / 1000));
    }
    if (expireInMinutes !== undefined) {
      sendOptions.expireInSeconds = Math.max(1, Math.round(expireInMinutes * 60));
    }
    stripUndefined(sendOptions);
    return sendOptions;
  }

  static toScheduleOptions(
    options: ScheduleOptions,
    defaultTimezone?: string,
  ): { priority?: number; tz?: string } {
    const out: { priority?: number; tz?: string } = {};
    if (options.priority !== undefined) out.priority = options.priority;
    const tz = options.timezone ?? defaultTimezone;
    if (tz !== undefined) out.tz = tz;
    return out;
  }
}

function toDate(startAfter: PublishOptions["startAfter"]): Date | undefined {
  const millis = toMillis(startAfter);
  if (millis === undefined) return undefined;
  return new Date(Date.now() + Math.max(0, millis));
}

function localDayToDate(
  startAt: Date | LocalDayTime | undefined,
  timezone: string | undefined,
): Date | undefined {
  if (!startAt) return undefined;
  if (startAt instanceof Date) return startAt;
  return nextLocalDayTime(startAt, timezone ?? "UTC");
}

function stripUndefined(record: object): void {
  const entries = record as Record<string, unknown>;
  for (const key of Object.keys(entries)) {
    if (entries[key] === undefined) delete entries[key];
  }
}

/** Adapt pg-boss's transactional `db` handle to a `DeliveryScope`. */
function toDeliveryScope(tx: unknown): DeliveryScope | undefined {
  const candidate = tx as
    { executeSql?: (text: string, values?: unknown[]) => Promise<{ rows: unknown[] }> } | undefined;
  if (!candidate || typeof candidate.executeSql !== "function") return undefined;
  const executeSql = candidate.executeSql.bind(candidate);
  const executor: SqlExecutor = {
    query: <T>(text: string, params?: unknown[]) =>
      executeSql(text, params) as Promise<{ rows: T[] }>,
  };
  return { executor };
}
