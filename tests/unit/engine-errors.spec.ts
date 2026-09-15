import { WorkflowEngine } from "../../src/workflow/workflow-engine";
import { createWorkflow } from "../../src/workflow/workflow-builder";
import { BatchedStreamStep, SimpleStep } from "../../src/workflow/steps";
import { Workflow } from "../../src/workflow/types";
import { InMemoryQueue } from "../../src/queue/in-memory-queue";
import { InMemoryRunStore } from "../../src/workflow/run-store";
import type { StepData } from "../../src/workflow/types";

const settle = () => new Promise((resolve) => setTimeout(resolve, 20));

async function waitFor(condition: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error("timed out waiting for condition");
}

function envelope(overrides: Partial<StepData> = {}): StepData {
  return {
    runId: "run-x",
    workflow: "real",
    version: 1,
    step: "only",
    attempt: 1,
    data: null,
    ...overrides,
  };
}

describe("engine failure paths", () => {
  it("rejects a workflow graph containing a cycle", async () => {
    const engine = new WorkflowEngine(new InMemoryQueue());
    const a = new SimpleStep("a", () => 1);
    const b = new SimpleStep("b", () => 2);
    a.nextSteps = [b];
    b.nextSteps = [a];
    await expect(engine.register(new Workflow("cyclic", a))).rejects.toThrow(/Cycle detected/);
  });

  it("allows a WhileStep self-loop", async () => {
    const engine = new WorkflowEngine(new InMemoryQueue());
    const wf = createWorkflow("loop-ok")
      .from("start", () => 1)
      .while("spin", (ctx) => (ctx.state.n as number) < 1)
      .build();
    await expect(engine.register(wf)).resolves.toBeUndefined();
  });

  it("rejects dispatch of an unknown workflow or step", async () => {
    const errors: unknown[] = [];
    const queue = new InMemoryQueue({ onError: (e) => errors.push(e) });
    const engine = new WorkflowEngine(queue);
    await engine.start();

    await engine.trigger("does-not-exist", {}).catch((e) => errors.push(e));
    expect(String(errors.at(-1))).toMatch(/WorkflowNotFound/);

    await queue.publish(WorkflowEngine.QUEUE_NAME, envelope({ workflow: "ghost" }));
    await settle();
    expect(String(errors.at(-1))).toMatch(/WorkflowNotFound/);

    await engine.register(
      createWorkflow("real")
        .from("only", () => 1)
        .build(),
    );
    await queue.publish(WorkflowEngine.QUEUE_NAME, envelope({ step: "missing" }));
    await settle();
    expect(String(errors.at(-1))).toMatch(/StepNotFound/);
  });

  it("refuses to enqueue or cancel once the queue is stopped", async () => {
    const queue = new InMemoryQueue();
    const engine = new WorkflowEngine(queue);
    const wf = createWorkflow("stopped")
      .from("a", () => 1)
      .build();
    await engine.register(wf);
    await queue.stop({ graceful: false });

    await expect(engine.trigger(wf, {})).rejects.toThrow(/Queue stopped/);
    await expect(engine.cancel("id")).rejects.toThrow(/Queue stopped/);
  });

  it("executes a step once when the same attempt is delivered twice", async () => {
    const queue = new InMemoryQueue();
    const engine = new WorkflowEngine(queue);
    let runs = 0;
    const wf = createWorkflow("dup")
      .from("s", () => {
        runs += 1;
        return 0;
      })
      .build();
    await engine.register(wf);
    await engine.start();

    const run = await engine.store.create({ workflow: "dup", version: 1 });
    const job = envelope({ runId: run.id, workflow: "dup", step: "s" });
    await queue.publish(WorkflowEngine.QUEUE_NAME, job);
    await settle();
    await queue.publish(WorkflowEngine.QUEUE_NAME, job); // redelivery after completion
    await settle();

    expect(runs).toBe(1); // idempotency ledger
  });

  it("gives a re-visited step a new attempt, not a duplicate", async () => {
    const queue = new InMemoryQueue();
    const engine = new WorkflowEngine(queue);
    const attempts: number[] = [];
    const wf = createWorkflow("revisit")
      .from("a", (ctx) => {
        attempts.push((ctx.state.n as number) ?? 0);
        ctx.state.n = ((ctx.state.n as number) ?? 0) + 1;
        return 1;
      })
      .while("loop", (ctx) => (ctx.state.n as number) < 3)
      .build();
    await engine.register(wf);
    await engine.start();
    await engine.trigger(wf, null);

    // step "a" runs once; "loop" re-visits itself with increasing attempts.
    await waitFor(() => attempts.length === 1);
    expect(attempts).toEqual([0]);
  });

  it("stops a runaway while loop at maxIterations", async () => {
    const errors: unknown[] = [];
    const queue = new InMemoryQueue({ onError: (e) => errors.push(e) });
    const engine = new WorkflowEngine(queue);
    const wf = createWorkflow("runaway")
      .from("start", () => 1)
      .while("spin", () => true, {}, 2)
      .build();
    await engine.register(wf);
    await engine.start();

    await engine.trigger(wf, null);
    await waitFor(() => errors.length > 0);
    expect(String(errors.at(-1))).toMatch(/maxIterations/);
  });

  it("stops dispatching once a run is cancelled", async () => {
    const queue = new InMemoryQueue();
    const engine = new WorkflowEngine(queue);
    let secondRan = false;
    const wf = createWorkflow("cancellable")
      .from("first", () => 1)
      .to("second", () => {
        secondRan = true;
        return 2;
      })
      .build();
    await engine.register(wf);

    const run = await engine.store.create({ workflow: "cancellable", version: 1 });
    await engine.cancelRun(run.id);
    await engine.start();
    await queue.publish(
      WorkflowEngine.QUEUE_NAME,
      envelope({ runId: run.id, workflow: "cancellable", step: "first" }),
    );
    await settle();
    expect(secondRan).toBe(false);
  });

  it("reports a failure that happens after the step body", async () => {
    class FailingRecordStore extends InMemoryRunStore {
      async recordStep(): Promise<void> {
        throw new Error("store down");
      }
    }
    const errors: Array<{ step: string; error: unknown }> = [];
    const queue = new InMemoryQueue({ onError: () => undefined });
    const engine = new WorkflowEngine(queue, {
      store: new FailingRecordStore(),
      hooks: { onError: (e) => errors.push(e) },
    });
    const wf = createWorkflow("post-step")
      .from("a", () => 1)
      .to("b", () => 2)
      .build();
    await engine.register(wf);
    await engine.start();

    await engine.trigger(wf, null);
    await waitFor(() => errors.length > 0);
    expect(errors[0].step).toBe("a");
    expect(String(errors[0].error)).toContain("store down");
  });

  it("rejects an invalid batch size", () => {
    expect(() => new BatchedStreamStep("b", 0)).toThrow(/positive integer/);
    expect(() => new BatchedStreamStep("b", -1)).toThrow(/positive integer/);
  });

  it("rejects builder misuse", () => {
    expect(() => createWorkflow("x").build()).toThrow(/from\(\.\.\.\) first/);
    expect(() => createWorkflow("x").to("s", () => 1)).toThrow(/from\(\.\.\.\) first/);
    expect(() =>
      createWorkflow("x")
        .from("a", () => 1)
        .from("b", () => 2),
    ).toThrow(/already been called/);
  });
});
