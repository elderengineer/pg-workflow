import type { QueueEvent, PublishOptions, Queue } from "../queue";
import type { RunStore } from "./run-store";
import type { SqlExecutor } from "../sql";

/**
 * Per-run context handed to every step.
 *
 * `state` is loaded from the `RunStore` before the step runs and saved after,
 * so it survives across steps and processes. It must be JSON-serializable and
 * small (identifiers, counters); see [`docs/context.md`](../../docs/context.md).
 */
export interface Context {
  /** The run id. */
  instanceId?: string;
  timezone?: string;
  state: Record<string, unknown>;
  options?: PublishOptions;
  /**
   * The job's database transaction, when the backend runs the handler in one
   * (`workOptions: { transactional: true }`). Writes made through it commit
   * with the step's run-state save, so a failing step does not leave them
   * behind. Only present under a transactional queue.
   */
  transaction?: SqlExecutor;
}

/**
 * The message published for every step hop. State is **not** here — it lives in
 * the run store; this carries only the routing envelope and the per-hop value.
 */
export interface StepData<T = unknown> {
  runId: string;
  workflow: string;
  version: number;
  step: string;
  /** Dispatch counter per step, used for idempotency. */
  attempt: number;
  data: T;
  options?: PublishOptions;
  timezone?: string;
}

export type StepResult<T> =
  { next?: Step<T, unknown>; message?: T } | Array<{ next?: Step<T, unknown>; message?: T }>;

export interface Step<In, Out> {
  readonly name: string;
  /** mutable by the builder only; engines treat as read-only after register */
  nextSteps: Step<Out, unknown>[];
  options: PublishOptions;
  run(context: Context, event: QueueEvent<In>): Promise<StepResult<Out>>;
  clone(): Step<In, Out>;
}

export class Workflow<In> {
  readonly name: string;
  readonly head: Step<In, unknown>;

  constructor(name: string, head: Step<In, unknown>) {
    if (!name) throw new Error("Workflow name is required.");
    if (!head) throw new Error(`Workflow ${name} has no steps.`);
    this.name = name;
    this.head = head;
  }
}

export interface EngineHooks {
  onError?: (info: { workflow: string; step: string; eventId?: string; error: unknown }) => void;
  onStepSuccess?: (info: { workflow: string; step: string; eventId?: string }) => void;
  logger?: Pick<Console, "debug" | "info" | "warn" | "error">;
}

export interface RegisterOptions {
  /** Workflow version. Defaults to 1; dispatch resolves the version the run started on. */
  version?: number;
}

export interface EngineOptions {
  /** Durable run state. Defaults to an in-memory store (tests/dev only). */
  store?: RunStore;
  hooks?: EngineHooks;
}

export interface TriggerOptions {
  /** Cron expression. When set, the workflow head runs on a schedule. */
  cron?: string;
  /** Options applied to the enqueued (or scheduled) job. */
  publish?: PublishOptions;
}

export interface Engine {
  queue: Queue;
  store: RunStore;
  /** start consuming. Call after all workflows are registered. Idempotent. */
  start(): Promise<void>;
  register(workflow: Workflow<unknown>, options?: RegisterOptions): Promise<void>;
  /** Enqueue one run. Returns the run id, or `null` for a cron schedule. */
  trigger<In>(
    workflow: Workflow<In> | string,
    data: In,
    options?: TriggerOptions,
  ): Promise<string | null>;
  /** Cancel one queued job by id. */
  cancel(id: string): Promise<void>;
  /** Cancel a whole run; the next step to be dispatched is skipped. */
  cancelRun(runId: string): Promise<void>;
  unschedule(workflow: Workflow<unknown> | string): Promise<void>;
}
