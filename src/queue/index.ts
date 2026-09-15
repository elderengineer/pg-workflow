import type { DurationInput } from "../duration";
import type { LocalDayTime } from "../local-time";
import type { SqlExecutor } from "../sql";

export type { LocalDayTime };

export type QueueName = string;

/**
 * Per-delivery scope supplied by a backend that can run a handler inside a
 * transaction. When present, the engine routes its run-store writes through
 * `executor`, so they commit (or roll back) with the job itself.
 */
export interface DeliveryScope {
  /** An executor bound to the delivery's transaction. */
  executor?: SqlExecutor;
}

export type QueueEvent<T> = {
  queue: QueueName;
  /** unique id; undefined until the queue assigns one */
  id: string | undefined;
  /** must be JSON-serializable */
  message: T;
  createTs: number;
};

export type StopOptions = {
  graceful?: boolean;
  /** Maximum time to wait for in-flight work to finish. */
  timeout?: DurationInput;
};

export type PublishOptions = {
  /** IANA timezone for resolving `startAt: LocalDayTime`. Defaults to UTC. */
  timezone?: string;
  startAt?: Date | LocalDayTime;
  startAfter?: DurationInput;
  retryLimit?: number;
  retryDelay?: DurationInput;
  expireInMinutes?: number;
  /** default 0, lower number means higher priority */
  priority?: number;
};

export type ScheduleOptions = {
  priority?: number;
  timezone?: string;
};

/** Backlog snapshot for one queue. */
export interface QueueStats {
  /** Waiting jobs, including future-dated (deferred) ones. */
  queued: number;
  /** Runnable right now (`queued - deferred`) — the true backlog. */
  ready: number;
  /** Jobs currently being processed. */
  active: number;
  /** Future-dated jobs not yet runnable. */
  deferred: number;
  /** Failed jobs still retained by the backend. */
  failed: number;
  total: number;
}

/**
 * Queue abstraction. This is the only thing the workflow engine needs.
 * Any backend (pg-boss, SQS, in-memory) can implement it.
 */
export interface Queue {
  isStopped(): boolean;
  start(): Promise<void>;
  stop(options?: StopOptions): Promise<void>;
  publish(queue: QueueName, event: unknown, options?: PublishOptions): Promise<string | null>;
  cancel(id: string): Promise<void>;
  schedule(
    queue: QueueName,
    cron: string,
    event: unknown,
    options?: ScheduleOptions,
  ): Promise<void>;
  unschedule(queue: QueueName): Promise<void>;
  subscribe<T>(
    queue: QueueName,
    subscriber: (event: QueueEvent<T>, scope?: DeliveryScope) => Promise<void>,
  ): Promise<void>;
  /** Optional backlog snapshot; backends that cannot report one may omit it. */
  stats?(queue: QueueName): Promise<QueueStats | null>;
}
