import type { QueueEvent, PublishOptions } from "../queue";
import {
  BatchedStreamStep,
  ConditionFn,
  IfStep,
  SimpleStep,
  StepFn,
  StreamStep,
  WhileStep,
} from "./steps";
import type { Context, Step, Workflow } from "./types";
import { Workflow as WorkflowImpl } from "./types";

/**
 * Fluent builder. Prefer the {@link createWorkflow} factory:
 *
 *   createWorkflow("name")
 *     .from("step-1", (ctx, evt) => ...)
 *     .to("step-2", (ctx, evt) => ...)
 *     .build();
 */
export class WorkflowBuilder<Head, In> {
  readonly name: string;
  private firstStep?: Step<Head, unknown>;
  private lastStep?: Step<unknown, unknown>;

  constructor(name: string, firstStep?: Step<Head, unknown>, lastStep?: Step<In, unknown>) {
    this.name = name;
    this.firstStep = firstStep;
    this.lastStep = lastStep as Step<unknown, unknown> | undefined;
  }

  from<Out>(
    name: string,
    fn: StepFn<Head, Out>,
    options: PublishOptions = {},
  ): WorkflowBuilder<Head, Out> {
    if (this.firstStep) throw new Error("from(...) has already been called.");
    const first = new SimpleStep<Head, Out>(name, fn, options);
    return new WorkflowBuilder<Head, Out>(this.name, first, first as unknown as Step<Out, unknown>);
  }

  fromIf(
    name: string,
    condition: ConditionFn<Head>,
    options: PublishOptions = {},
  ): WorkflowBuilder<Head, Head> {
    if (this.firstStep) throw new Error("from(...) has already been called.");
    const first = new IfStep<Head>(name, condition, options);
    return new WorkflowBuilder<Head, Head>(this.name, first, first);
  }

  to<Out>(
    name: string,
    fn: StepFn<In, Out>,
    options: PublishOptions = {},
  ): WorkflowBuilder<Head, Out> {
    return this.addStep(new SimpleStep<In, Out>(name, fn, options));
  }

  if(
    name: string,
    condition: ConditionFn<In>,
    options: PublishOptions = {},
  ): WorkflowBuilder<Head, In> {
    return this.addStep(new IfStep<In>(name, condition, options));
  }

  while(
    name: string,
    condition: ConditionFn<In>,
    options: PublishOptions = {},
    maxIterations?: number,
  ): WorkflowBuilder<Head, In> {
    return this.addStep(new WhileStep<In>(name, condition, options, maxIterations));
  }

  stream<Out>(name: string): WorkflowBuilder<Head, Out> {
    // StreamStep fans out In[] -> In; the generic shift is intentional.
    const next = new StreamStep<In>(name) as unknown as Step<In, Out>;
    return this.addStep(next);
  }

  batchedStream<Out>(name: string, batchSize: number): WorkflowBuilder<Head, Out> {
    const next = new BatchedStreamStep<In>(name, batchSize) as unknown as Step<In, Out>;
    return this.addStep(next);
  }

  private addStep<Out>(next: Step<In, Out>): WorkflowBuilder<Head, Out> {
    if (!this.lastStep) throw new Error("call from(...) first.");
    (this.lastStep as Step<In, unknown>).nextSteps = [
      ...(this.lastStep as Step<In, unknown>).nextSteps,
      next as unknown as Step<In, unknown>,
    ];
    return new WorkflowBuilder<Head, Out>(
      this.name,
      this.firstStep,
      next as unknown as Step<Out, unknown>,
    );
  }

  build(): Workflow<Head> {
    if (!this.firstStep) throw new Error("call from(...) first.");
    return new WorkflowImpl<Head>(this.name, this.firstStep);
  }
}

/** Start building a workflow. */
export function createWorkflow<In>(name: string): WorkflowBuilder<In, unknown> {
  return new WorkflowBuilder<In, unknown>(name);
}

export type { Context, QueueEvent, Step, StepFn, ConditionFn, Workflow };
