import type { DeliveryScope, QueueEvent, PublishOptions, Queue } from "../queue";
import type {
  Context,
  Engine,
  EngineHooks,
  EngineOptions,
  RegisterOptions,
  Step,
  StepData,
  StepResult,
  TriggerOptions,
  Workflow,
} from "./types";
import {
  InMemoryRunStore,
  type PlannedStep,
  type RunRecord,
  type RunStore,
  type StepRecord,
} from "./run-store";

const DEFAULT_VERSION = 1;

/** Payload written to the cron queue; each firing creates a new run. */
interface ScheduledRun<T> {
  workflow: string;
  version: number;
  step: string;
  data: T;
  options?: PublishOptions;
}

function asArray<T>(value: T | T[]): T[] {
  return Array.isArray(value) ? value : [value];
}

/**
 * Single-shared-queue engine. Every step hop is one job carrying the routing
 * envelope `{ runId, workflow, version, step, attempt }`; run state lives in the
 * `RunStore`. See [`docs/architecture.md`](../../docs/architecture.md).
 *
 * Behaviour notes:
 *  - `register` rejects duplicate `(name, version)` pairs; `start` is idempotent.
 *  - A step's outcome is recorded in the idempotency ledger *before* outgoing
 *    jobs are published, so a redelivery re-forwards instead of re-executing.
 *  - Observability is pluggable via `EngineHooks` (no hard tracer dependency).
 */
export class WorkflowEngine implements Engine {
  public static readonly QUEUE_NAME = "pg-workflow-item";

  readonly queue: Queue;
  readonly store: RunStore;
  readonly hooks: EngineHooks;
  private readonly workflows = new Map<string, Map<number, Workflow<unknown>>>();
  private readonly steps = new Map<string, Map<number, Map<string, Step<unknown, unknown>>>>();
  private started = false;
  private readonly scheduledCrons = new Set<string>();

  constructor(queue: Queue, options: EngineOptions = {}) {
    this.queue = queue;
    this.store = options.store ?? new InMemoryRunStore();
    this.hooks = options.hooks ?? {};
  }

  async register(workflow: Workflow<unknown>, options: RegisterOptions = {}): Promise<void> {
    const version = options.version ?? DEFAULT_VERSION;
    const byVersion = this.workflows.get(workflow.name);
    if (byVersion?.has(version)) {
      throw new Error(
        `Duplicate Name: workflow "${workflow.name}" v${version} already registered.`,
      );
    }
    this.validateGraph(workflow);

    let workflows = byVersion;
    if (!workflows) {
      workflows = new Map();
      this.workflows.set(workflow.name, workflows);
    }
    workflows.set(version, workflow);

    let byStep = this.steps.get(workflow.name);
    if (!byStep) {
      byStep = new Map();
      this.steps.set(workflow.name, byStep);
    }
    const steps = new Map<string, Step<unknown, unknown>>();
    byStep.set(version, steps);
    this.registerStep(steps, workflow.head as Step<unknown, unknown>);
  }

  private validateGraph(workflow: Workflow<unknown>): void {
    const seen = new Set<string>();
    const visit = (step: Step<unknown, unknown>, path: string[]): void => {
      if (seen.has(step.name)) return; // diamond joins allowed
      seen.add(step.name);
      for (const next of step.nextSteps) {
        if (path.includes(next.name)) {
          const isWhileSelfLoop = next === step && step.name === next.name;
          if (!isWhileSelfLoop) {
            throw new Error(
              `Cycle detected in workflow "${workflow.name}": ${[...path, next.name].join(" -> ")}`,
            );
          }
        }
        visit(next, [...path, step.name]);
      }
    };
    visit(workflow.head as Step<unknown, unknown>, []);
  }

  private registerStep(
    steps: Map<string, Step<unknown, unknown>>,
    step: Step<unknown, unknown>,
  ): void {
    if (steps.has(step.name)) return; // shared join target — register once
    steps.set(step.name, step);
    for (const next of step.nextSteps) this.registerStep(steps, next);
  }

  private resolveWorkflow(
    name: string,
    version?: number,
  ): { workflow: Workflow<unknown>; version: number } {
    const byVersion = this.workflows.get(name);
    if (!byVersion || byVersion.size === 0) {
      throw new Error(
        `WorkflowNotFound: ${name} (registered: ${[...this.workflows.keys()].join(", ") || "none"})`,
      );
    }
    const resolved = version ?? Math.max(...byVersion.keys());
    const workflow = byVersion.get(resolved);
    if (!workflow) {
      throw new Error(
        `WorkflowVersionNotFound: ${name} v${version} (available: ${[...byVersion.keys()].join(", ")})`,
      );
    }
    return { workflow, version: resolved };
  }

  private async startRun<In>(
    workflow: string,
    version: number,
    step: string,
    data: In,
    options: PublishOptions,
  ): Promise<RunRecord> {
    const run = await this.store.create({ workflow, version, state: {} });
    run.attempts[step] = 1;
    run.currentStep = step;
    await this.store.save(run);
    await this.queue.publish(
      WorkflowEngine.QUEUE_NAME,
      {
        runId: run.id,
        workflow,
        version,
        step,
        attempt: 1,
        data: (data ?? null) as In,
        options,
        timezone: options.timezone,
      } satisfies StepData<In>,
      options,
    );
    return run;
  }

  async runStep<In>(
    workflow: Workflow<unknown>,
    version: number,
    step: Step<In, unknown>,
    job: QueueEvent<StepData<In>>,
    scope?: DeliveryScope,
  ): Promise<void> {
    const envelope = job.message;
    const event: QueueEvent<In> = {
      queue: job.queue,
      id: job.id,
      message: envelope.data,
      createTs: job.createTs,
    };
    let run: RunRecord | null = null;
    try {
      run = await this.store.load(envelope.runId, scope);
      if (!run) throw new Error(`RunNotFound: ${envelope.runId}`);
      if (run.status === "cancelled") return;

      // Idempotency: this attempt already ran, so re-forward its recorded hops
      // instead of executing the step body again.
      const recorded = await this.store.getStep(envelope.runId, step.name, envelope.attempt, scope);
      if (recorded) {
        await this.dispatch(workflow.name, version, run, recorded.next, envelope);
        return;
      }

      const context: Context = {
        instanceId: run.id,
        timezone: envelope.timezone,
        state: run.state,
        options: envelope.options,
        transaction: scope?.executor,
      };
      const result = (await step.run(context, event)) as StepResult<unknown>;

      const planned: PlannedStep[] = [];
      for (const outcome of asArray(await result)) {
        const target = outcome.next;
        if (!target) continue;
        const name = (target as Step<unknown, unknown>).name;
        const attempt = (run.attempts[name] ?? 0) + 1;
        run.attempts[name] = attempt;
        planned.push({ step: name, attempt, message: outcome.message });
      }

      // Record before publishing: a crash after this point recovers by
      // re-forwarding, and a crash before it re-runs the step.
      const record: StepRecord = {
        step: step.name,
        attempt: envelope.attempt,
        next: planned,
        completedAt: new Date(),
      };
      await this.store.recordStep(run.id, record, scope);

      if (planned.length === 0) {
        run.status = "completed";
        run.completedAt = new Date();
      } else {
        run.currentStep = planned[planned.length - 1].step;
      }
      run.updatedAt = new Date();
      await this.store.save(run, scope);

      this.hooks.onStepSuccess?.({
        workflow: workflow.name,
        step: step.name,
        eventId: event.id,
      });

      await this.dispatch(workflow.name, version, run, planned, envelope);
    } catch (error) {
      // Covers the step body *and* everything after it — recording, saving and
      // re-publishing all fail through the same path, so a post-step failure is
      // never a silent retry.
      await this.reportFailure(workflow.name, step.name, event.id, run, error);
      throw error;
    }
  }

  private async reportFailure(
    workflow: string,
    step: string,
    eventId: string | undefined,
    run: RunRecord | null,
    error: unknown,
  ): Promise<void> {
    try {
      if (run) {
        run.lastError = String(error);
        run.updatedAt = new Date();
        await this.store.save(run);
      }
    } catch (secondary) {
      this.hooks.logger?.error(`failed to persist the error for ${run?.id}: ${secondary}`);
    }
    this.hooks.onError?.({ workflow, step, eventId, error });
    this.hooks.logger?.error(`${step} failed: ${error}`);
  }

  private async dispatch(
    workflow: string,
    version: number,
    run: RunRecord,
    planned: PlannedStep[],
    envelope: StepData<unknown>,
  ): Promise<void> {
    await Promise.all(
      planned.map((hop) => {
        const target = this.steps.get(workflow)?.get(version)?.get(hop.step);
        const options: PublishOptions = {
          ...envelope.options,
          ...target?.options,
          timezone: target?.options?.timezone ?? envelope.timezone,
        };
        return this.queue.publish(
          WorkflowEngine.QUEUE_NAME,
          {
            runId: run.id,
            workflow,
            version,
            step: hop.step,
            attempt: hop.attempt,
            data: hop.message,
            options: envelope.options,
            timezone: options.timezone,
          } satisfies StepData<unknown>,
          options,
        );
      }),
    );
  }

  async trigger<In>(
    workflow: Workflow<In> | string,
    data: In,
    options: TriggerOptions = {},
  ): Promise<string | null> {
    if (this.queue.isStopped()) throw new Error("Queue stopped: cannot enqueue.");
    const name = typeof workflow === "string" ? workflow : workflow.name;
    const { workflow: flow, version } = this.resolveWorkflow(name);
    const publish = options.publish ?? {};
    const head = flow.head as Step<In, unknown>;

    if (options.cron) {
      const key = `${name}@${version}`;
      if (!this.scheduledCrons.has(key)) {
        this.scheduledCrons.add(key);
        await this.queue.subscribe<ScheduledRun<In>>(name, async (job) => {
          const scheduled = job.message;
          await this.startRun(
            scheduled.workflow,
            scheduled.version,
            scheduled.step,
            scheduled.data,
            scheduled.options ?? {},
          );
        });
      }
      const scheduled: ScheduledRun<In> = {
        workflow: name,
        version,
        step: head.name,
        data: (data ?? null) as In,
        options: publish,
      };
      await this.queue.schedule(name, options.cron, scheduled, { priority: publish.priority });
      return null;
    }

    const run = await this.startRun(name, version, head.name, data, publish);
    return run.id;
  }

  async cancel(id: string): Promise<void> {
    if (this.queue.isStopped()) throw new Error("Queue stopped: cannot cancel.");
    return this.queue.cancel(id);
  }

  async cancelRun(runId: string): Promise<void> {
    await this.store.cancel(runId);
  }

  async unschedule(workflow: Workflow<unknown> | string): Promise<void> {
    const name = typeof workflow === "string" ? workflow : workflow.name;
    for (const version of this.workflows.get(name)?.keys() ?? []) {
      this.scheduledCrons.delete(`${name}@${version}`);
    }
    return this.queue.unschedule(name);
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    await this.queue.subscribe(
      WorkflowEngine.QUEUE_NAME,
      async (job: QueueEvent<StepData<unknown>>, scope?: DeliveryScope) => {
        const envelope = job.message;
        const { workflow, version } = this.resolveWorkflow(envelope.workflow, envelope.version);
        const step = this.steps.get(envelope.workflow)?.get(version)?.get(envelope.step);
        if (!step) {
          const available = [...(this.steps.get(envelope.workflow)?.get(version)?.keys() ?? [])];
          throw new Error(
            `StepNotFound: ${envelope.step} in ${envelope.workflow} v${version} (available: ${available.join(", ")})`,
          );
        }
        await this.runStep(workflow, version, step, job, scope);
      },
    );
  }
}
