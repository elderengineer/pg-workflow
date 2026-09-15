import { WorkflowEngine } from "../../src/workflow/workflow-engine";
import { createWorkflow } from "../../src/workflow/workflow-builder";
import { InMemoryQueue } from "../../src/queue/in-memory-queue";
import type { Context } from "../../src/workflow/types";
import type { QueueEvent } from "../../src/queue";

function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

async function waitFor(condition: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error("timed out waiting for condition");
}

describe("pg-workflow (InMemoryQueue)", () => {
  it("steps through a linear workflow and threads context.state", async () => {
    const queue = new InMemoryQueue();
    const engine = new WorkflowEngine(queue);
    await engine.start();

    const done = deferred<Record<string, unknown>>();
    const wf = createWorkflow<{ msg: string }>("linear")
      .from("step-1", (ctx: Context, _e: QueueEvent<{ msg: string }>) => {
        ctx.state["step-1"] = 1;
        return 1;
      })
      .to("step-2", (ctx: Context, evt: QueueEvent<number>) => {
        ctx.state["step-2"] = evt.message + 1;
        done.resolve({ ...ctx.state });
        return 2;
      })
      .build();

    await engine.register(wf);
    await engine.trigger(wf, { msg: "hello" });
    await expect(done.promise).resolves.toStrictEqual({ "step-1": 1, "step-2": 2 });
  });

  it("fans out with stream()", async () => {
    const queue = new InMemoryQueue();
    const engine = new WorkflowEngine(queue);
    await engine.start();

    const seen: number[] = [];
    const done = deferred<number[]>();
    const wf = createWorkflow<unknown>("fanout")
      .from("produce", () => [0, 1, 2])
      .stream<number>("stream")
      .to("consume", (_ctx: Context, evt: QueueEvent<number>) => {
        seen.push(evt.message);
        if (seen.length === 3) done.resolve([...seen].sort((a, b) => a - b));
        return 0;
      })
      .build();

    await engine.register(wf);
    await engine.trigger(wf, {});
    await expect(done.promise).resolves.toEqual([0, 1, 2]);
  });

  it("branches on if() and skips the next step when false", async () => {
    const queue = new InMemoryQueue();
    const errors: Array<{ workflow: string; step: string; error: unknown }> = [];
    const engine = new WorkflowEngine(queue, { hooks: { onError: (e) => errors.push(e) } });
    await engine.start();

    let ran = false;
    const wf = createWorkflow<number>("cond")
      .from("start", () => 1)
      .if("gate", (_c: Context, e: QueueEvent<number>) => e.message > 10)
      .to("never", () => {
        ran = true;
        return 0;
      })
      .build();

    await engine.register(wf);
    await engine.trigger(wf, 1);
    await new Promise((r) => setTimeout(r, 50));
    expect(ran).toBe(false);
    expect(errors).toEqual([]);
  });

  it("rejects duplicate workflow names", async () => {
    const engine = new WorkflowEngine(new InMemoryQueue());
    const a = createWorkflow("dup")
      .from("s", () => 1)
      .build();
    const b = createWorkflow("dup")
      .from("s", () => 2)
      .build();
    await engine.register(a as never);
    await expect(engine.register(b as never)).rejects.toThrow(/already registered/);
  });

  it("preserves falsy payloads (0, false)", async () => {
    const queue = new InMemoryQueue();
    const engine = new WorkflowEngine(queue);
    await engine.start();
    const got = deferred<unknown>();
    const wf = createWorkflow<number>("falsy")
      .from("echo", (_c: Context, e: QueueEvent<number>) => {
        got.resolve(e.message);
        return 0;
      })
      .build();
    await engine.register(wf);
    await engine.trigger(wf, 0);
    await expect(got.promise).resolves.toBe(0);
  });

  it("surfaces a step failure through hooks and the queue", async () => {
    const queueErrors: unknown[] = [];
    const queue = new InMemoryQueue({ onError: (e) => queueErrors.push(e) });
    const hookErrors: Array<{ workflow: string; step: string; error: unknown }> = [];
    const engine = new WorkflowEngine(queue, { hooks: { onError: (e) => hookErrors.push(e) } });
    const wf = createWorkflow("boom")
      .from("bad", () => {
        throw new Error("kaboom");
      })
      .build();
    await engine.register(wf);
    await engine.start();

    await engine.trigger(wf, null);
    await waitFor(() => hookErrors.length > 0 && queueErrors.length > 0);
    expect(hookErrors[0].workflow).toBe("boom");
    expect(hookErrors[0].step).toBe("bad");
    expect(String(hookErrors[0].error)).toContain("kaboom");
    expect(String(queueErrors[0])).toContain("kaboom");
  });
});
