import { WorkflowEngine } from "../../src/workflow/workflow-engine";
import { createWorkflow } from "../../src/workflow/workflow-builder";
import { InMemoryQueue } from "../../src/queue/in-memory-queue";
import type { Context } from "../../src/workflow/types";
import type { QueueEvent } from "../../src/queue";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

describe("passing data", () => {
  it("forwards a step's return value to the next step as event.message", async () => {
    const engine = new WorkflowEngine(new InMemoryQueue());
    await engine.start();

    const hop: Array<{ step: string; message: unknown }> = [];
    const done = deferred<void>();
    const wf = createWorkflow<{ seed: boolean }>("hop")
      .from("a", (_ctx, evt) => {
        hop.push({ step: "a", message: evt.message });
        return { n: 42 };
      })
      .to("b", (_ctx, evt) => {
        hop.push({ step: "b", message: evt.message });
        done.resolve();
        return 0;
      })
      .build();

    await engine.register(wf);
    await engine.trigger(wf, { seed: true });
    await done.promise;

    // b sees a's return value, not the trigger payload: per-hop data is not accumulated.
    expect(hop).toStrictEqual([
      { step: "a", message: { seed: true } },
      { step: "b", message: { n: 42 } },
    ]);
  });

  it("keeps context.state visible to every later step", async () => {
    const engine = new WorkflowEngine(new InMemoryQueue());
    await engine.start();

    const done = deferred<Record<string, unknown>>();
    const wf = createWorkflow<unknown>("state")
      .from("a", (ctx) => {
        ctx.state.first = 1;
        return 1;
      })
      .to("b", (ctx) => {
        ctx.state.second = (ctx.state.first as number) + 1;
        return 2;
      })
      .to("c", (ctx) => {
        done.resolve({ ...ctx.state });
        return 3;
      })
      .build();

    await engine.register(wf);
    await engine.trigger(wf, {});
    await expect(done.promise).resolves.toStrictEqual({ first: 1, second: 2 });
  });

  it("feeds each stream() branch its own message", async () => {
    const engine = new WorkflowEngine(new InMemoryQueue());
    await engine.start();

    const seen: number[] = [];
    const done = deferred<void>();
    const wf = createWorkflow<unknown>("fanout-data")
      .from("produce", () => [10, 20, 30])
      .stream<number>("s")
      .to("consume", (_ctx: Context, evt: QueueEvent<number>) => {
        seen.push(evt.message);
        if (seen.length === 3) done.resolve();
        return 0;
      })
      .build();

    await engine.register(wf);
    await engine.trigger(wf, {});
    await done.promise;
    expect([...seen].sort((a, b) => a - b)).toStrictEqual([10, 20, 30]);
  });

  it("does not merge state written by sibling fan-out branches", async () => {
    const engine = new WorkflowEngine(new InMemoryQueue());
    await engine.start();

    const observed: number[] = [];
    const done = deferred<void>();
    const wf = createWorkflow<unknown>("fanout-state")
      .from("produce", () => [1, 2, 3])
      .stream<number>("s")
      .to("consume", (ctx) => {
        ctx.state.count = ((ctx.state.count as number) ?? 0) + 1;
        observed.push(ctx.state.count as number);
        if (observed.length === 3) done.resolve();
        return 0;
      })
      .build();

    await engine.register(wf);
    await engine.trigger(wf, {});
    await done.promise;

    // Each branch loads the run, increments its own view and saves; concurrent
    // branches are last-write-wins, so none sees another's write. Run state is
    // per-run bookkeeping — fan-out results belong in the per-hop message.
    expect(observed).toStrictEqual([1, 1, 1]);
  });
});
