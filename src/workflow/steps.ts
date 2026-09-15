import type { QueueEvent, PublishOptions } from "../queue";
import { duration, toMillis, TimeUnit } from "../duration";
import type { Context, Step, StepResult } from "./types";

export type StepFn<In, Out> = (context: Context, event: QueueEvent<In>) => Out | Promise<Out>;
export type ConditionFn<In> = (
  context: Context,
  event: QueueEvent<In>,
) => boolean | Promise<boolean>;

export class SimpleStep<In, Out> implements Step<In, Out> {
  readonly name: string;
  readonly body: StepFn<In, Out>;
  nextSteps: Step<Out, unknown>[] = [];
  options: PublishOptions;

  constructor(name: string, body: StepFn<In, Out>, options: PublishOptions = {}) {
    this.name = name;
    this.body = body;
    this.options = options;
  }

  async run(context: Context, event: QueueEvent<In>): Promise<StepResult<Out>> {
    const result = await this.body(context, event);
    return { next: this.nextSteps[0], message: result };
  }

  clone(): Step<In, Out> {
    const c = new SimpleStep<In, Out>(this.name, this.body, { ...this.options });
    c.nextSteps = [...this.nextSteps];
    return c;
  }
}

export class IfStep<In> implements Step<In, In> {
  readonly name: string;
  readonly condition: ConditionFn<In>;
  nextSteps: Step<In, unknown>[] = [];
  options: PublishOptions;

  constructor(name: string, condition: ConditionFn<In>, options: PublishOptions = {}) {
    this.name = name;
    this.condition = condition;
    this.options = options;
  }

  async run(context: Context, event: QueueEvent<In>): Promise<StepResult<In>> {
    const ok = await this.condition(context, event);
    return { next: ok ? this.nextSteps[0] : undefined, message: event.message };
  }

  clone(): Step<In, In> {
    const c = new IfStep<In>(this.name, this.condition, { ...this.options });
    c.nextSteps = [...this.nextSteps];
    return c;
  }
}

const DEFAULT_MAX_ITERATIONS = 1000;

export class WhileStep<In> implements Step<In, In> {
  readonly name: string;
  readonly condition: ConditionFn<In>;
  readonly maxIterations: number;
  nextSteps: Step<In, unknown>[] = [];
  options: PublishOptions;

  constructor(
    name: string,
    condition: ConditionFn<In>,
    options: PublishOptions = {},
    maxIterations = DEFAULT_MAX_ITERATIONS,
  ) {
    this.name = name;
    this.condition = condition;
    this.options = options;
    this.maxIterations = maxIterations;
  }

  async run(context: Context, event: QueueEvent<In>): Promise<StepResult<In>> {
    const iterations = Number(context.state[`__while_${this.name}`] ?? 0);
    if (iterations >= this.maxIterations) {
      throw new Error(`WhileStep "${this.name}" exceeded maxIterations=${this.maxIterations}`);
    }
    const ok = await this.condition(context, event);
    if (ok) {
      context.state[`__while_${this.name}`] = iterations + 1;
      return { next: this, message: event.message };
    }
    delete context.state[`__while_${this.name}`];
    return { next: this.nextSteps[0], message: event.message };
  }

  clone(): Step<In, In> {
    const c = new WhileStep<In>(this.name, this.condition, { ...this.options }, this.maxIterations);
    c.nextSteps = [...this.nextSteps];
    return c;
  }
}

export class StreamStep<In> implements Step<In[], In> {
  readonly name: string;
  nextSteps: Step<In, unknown>[] = [];
  options: PublishOptions = {};

  constructor(name: string) {
    this.name = name;
  }

  async run(context: Context, event: QueueEvent<In[]>): Promise<StepResult<In>> {
    const next = this.nextSteps[0];
    if (!next) return { next: undefined, message: undefined as unknown as In };
    const messages: In[] = Array.isArray(event.message) ? event.message : [event.message];
    const interval = toMillis(next.options.startAfter);
    return messages.map((msg, idx) => {
      let target = next;
      if (interval) {
        // Spread delayed fan-out instead of firing all at once
        // (without mutating the registered step).
        target = next.clone() as Step<In, unknown>;
        target.options = {
          ...target.options,
          startAfter: duration(interval * (idx + 1), TimeUnit.MILLISECONDS),
        };
      }
      return { next: target, message: msg };
    });
  }

  clone(): Step<In[], In> {
    const c = new StreamStep<In>(this.name);
    c.nextSteps = [...this.nextSteps];
    return c;
  }
}

export class BatchedStreamStep<In> implements Step<In[], In[]> {
  readonly name: string;
  readonly batchSize: number;
  nextSteps: Step<In[], unknown>[] = [];
  options: PublishOptions = {};

  constructor(name: string, batchSize: number) {
    if (!Number.isInteger(batchSize) || batchSize <= 0) {
      throw new Error(`batchSize must be a positive integer, got ${batchSize}`);
    }
    this.name = name;
    this.batchSize = batchSize;
  }

  async run(context: Context, event: QueueEvent<In[]>): Promise<StepResult<In[]>> {
    void context;
    const next = this.nextSteps[0];
    if (!next) return { next: undefined, message: [] };
    const messages: In[] = Array.isArray(event.message) ? event.message : [event.message];
    const batches: In[][] = [];
    for (let i = 0; i < messages.length; i += this.batchSize) {
      batches.push(messages.slice(i, i + this.batchSize));
    }
    return batches.map((message) => ({ next, message }));
  }

  clone(): Step<In[], In[]> {
    const c = new BatchedStreamStep<In>(this.name, this.batchSize);
    c.nextSteps = [...this.nextSteps];
    return c;
  }
}
