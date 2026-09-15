/**
 * Postgres-backed integration tests. Require a live database:
 *
 *   docker compose up -d postgres
 *   PG_URL=postgres://pgworkflow:pgworkflow@localhost:5433/pgworkflow npm run test:integration
 *
 * Skipped automatically when PG_URL is not set.
 */
import { PgBossQueue } from "../../src/queue/pg-boss-queue";
import { WorkflowEngine } from "../../src/workflow/workflow-engine";
import { createWorkflow } from "../../src/workflow/workflow-builder";
import { PostgresRunStore } from "../../src/workflow/run-store";
import type { Context } from "../../src/workflow/types";
import type { QueueEvent } from "../../src/queue";
import { duration, TimeUnit } from "../../src/duration";
import { Pool } from "pg";

const PG_URL = process.env.PG_URL;
const itPg = PG_URL ? it : it.skip;

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("PgBossQueue (integration)", () => {
  itPg(
    "round-trips publish → subscribe",
    async () => {
      const queue = new PgBossQueue({ connectionString: PG_URL });
      await queue.start();
      try {
        const got = deferred<unknown>();
        await queue.subscribe("it-basic", async (event: QueueEvent<unknown>) => {
          got.resolve(event.message);
        });
        await queue.publish("it-basic", { hello: "pg" });
        await expect(got.promise).resolves.toStrictEqual({ hello: "pg" });
      } finally {
        await queue.stop();
      }
    },
    30_000,
  );

  itPg(
    "runs a linear workflow end to end",
    async () => {
      const queue = new PgBossQueue({ connectionString: PG_URL });
      const engine = new WorkflowEngine(queue);
      await queue.start();
      await engine.start();
      try {
        const done = deferred<Record<string, unknown>>();
        const name = `it-flow-${Date.now()}`;
        const workflow = createWorkflow<{ n: number }>(name)
          .from("add-one", (ctx: Context, event: QueueEvent<{ n: number }>) => {
            ctx.state["v"] = event.message.n + 1;
            return event.message.n + 1;
          })
          .to("double", (ctx: Context, event: QueueEvent<number>) => {
            ctx.state["v"] = event.message * 2;
            done.resolve({ ...ctx.state });
            return event.message * 2;
          })
          .build();
        await engine.register(workflow);
        await engine.trigger(workflow, { n: 20 });
        await expect(done.promise).resolves.toStrictEqual({ v: 42 });
      } finally {
        await queue.stop();
      }
    },
    60_000,
  );

  itPg(
    "supports cancel + schedule/unschedule",
    async () => {
      const queue = new PgBossQueue({ connectionString: PG_URL });
      await queue.start();
      try {
        let delivered = 0;
        await queue.subscribe("it-cancel", async () => {
          delivered += 1;
        });
        const { duration, TimeUnit } = await import("../../src/duration");
        const id = await queue.publish("it-cancel", "x", {
          startAfter: duration(30, TimeUnit.SECONDS),
        });
        await queue.cancel(id!);
        await queue.schedule("it-cron", "* * * * *", { tick: 1 });
        await queue.unschedule("it-cron");
        await new Promise((resolve) => setTimeout(resolve, 2000));
        expect(delivered).toBe(0);
      } finally {
        await queue.stop();
      }
    },
    30_000,
  );

  itPg(
    "reports backlog stats (eventually consistent)",
    async () => {
      const queue = new PgBossQueue({
        connectionString: PG_URL,
        // Counts are refreshed by pg-boss's supervise/monitor passes (default 60s each).
        bossOptions: { superviseIntervalSeconds: 1, monitorIntervalSeconds: 1 },
      });
      await queue.start();
      try {
        const name = `it-stats-${Date.now()}`;
        await queue.publish(name, "x", { startAfter: duration(1, TimeUnit.HOURS) });
        await waitFor(async () => ((await queue.stats(name))?.queued ?? 0) >= 1);
        const stats = await queue.stats(name);
        expect(stats!.deferred).toBeGreaterThanOrEqual(1);
        expect(stats!.ready).toBe(0);
      } finally {
        await queue.stop();
      }
    },
    40_000,
  );

  itPg(
    "reschedules a deferred job to run now",
    async () => {
      const queue = new PgBossQueue({ connectionString: PG_URL });
      await queue.start();
      try {
        const name = `it-resched-${Date.now()}`;
        const got = deferred<unknown>();
        await queue.subscribe(name, async (event) => got.resolve(event.message));
        const id = await queue.publish(name, "x", { startAfter: duration(1, TimeUnit.HOURS) });
        await queue.rescheduleJob(name, id!, { startAfter: duration(0, TimeUnit.MILLISECONDS) });
        await expect(got.promise).resolves.toBe("x");
      } finally {
        await queue.stop();
      }
    },
    40_000,
  );

  itPg(
    "retries a failed job by id",
    async () => {
      const queue = new PgBossQueue({
        connectionString: PG_URL,
        queueOptions: { retryLimit: 0 },
      });
      await queue.start();
      try {
        const name = `it-retry-${Date.now()}`;
        let attempts = 0;
        const succeeded = deferred<void>();
        await queue.subscribe(name, async () => {
          attempts += 1;
          if (attempts === 1) throw new Error("first attempt fails");
          succeeded.resolve();
        });
        const id = await queue.publish(name, "x");
        await waitFor(async () => (await queue.getJob(name, id!))?.state === "failed");
        const failed = await queue.getJob(name, id!);
        expect(failed!.retryCount).toBe(0);
        expect(JSON.stringify(failed!.output)).toContain("first attempt fails");
        await queue.retryJob(name, id!);
        await succeeded.promise;
      } finally {
        await queue.stop();
      }
    },
    60_000,
  );

  itPg(
    "retries per the queue's retry policy, then succeeds",
    async () => {
      const queue = new PgBossQueue({
        connectionString: PG_URL,
        queueOptions: { retryLimit: 3, retryDelay: 1 },
        workOptions: { pollingIntervalSeconds: 0.5 },
      });
      await queue.start();
      try {
        const name = `it-policy-${Date.now()}`;
        let attempts = 0;
        const succeeded = deferred<number>();
        await queue.subscribe(name, async () => {
          attempts += 1;
          if (attempts < 2) throw new Error("not yet");
          succeeded.resolve(attempts);
        });
        await queue.publish(name, "x");
        await expect(succeeded.promise).resolves.toBeGreaterThanOrEqual(2);
      } finally {
        await queue.stop();
      }
    },
    60_000,
  );

  itPg(
    "bounds worker concurrency via localConcurrency",
    async () => {
      const queue = new PgBossQueue({
        connectionString: PG_URL,
        workOptions: { localConcurrency: 2, pollingIntervalSeconds: 0.5 },
      });
      await queue.start();
      try {
        const name = `it-concurrency-${Date.now()}`;
        let inFlight = 0;
        let peak = 0;
        let done = 0;
        const all = deferred<void>();
        await queue.subscribe(name, async () => {
          inFlight += 1;
          peak = Math.max(peak, inFlight);
          await new Promise((resolve) => setTimeout(resolve, 250));
          inFlight -= 1;
          done += 1;
          if (done === 6) all.resolve();
        });
        for (let i = 0; i < 6; i++) await queue.publish(name, i);
        await all.promise;
        expect(peak).toBeLessThanOrEqual(2);
        expect(peak).toBeGreaterThanOrEqual(2);
      } finally {
        await queue.stop();
      }
    },
    60_000,
  );

  itPg(
    "dead-letters a failed job and redrives it",
    async () => {
      const stamp = Date.now();
      const source = `it-dlsrc-${stamp}`;
      const dlq = `it-dlq-${stamp}`;
      const queue = new PgBossQueue({
        connectionString: PG_URL,
        queueOptions: (q) =>
          q === source ? { retryLimit: 0, deadLetter: dlq } : { retryLimit: 0 },
        workOptions: { pollingIntervalSeconds: 0.5 },
      });
      await queue.start();
      try {
        let attempts = 0;
        await queue.subscribe(source, async () => {
          attempts += 1;
          if (attempts === 1) throw new Error("first attempt fails");
        });
        await queue.publish(source, "x");

        // The failure is final, so pg-boss moves the job to the dead-letter queue.
        await waitFor(async () => (await queue.redriveDeadLetter(dlq, { sourceName: source })) > 0);
        await waitFor(async () => attempts >= 2);
        expect(attempts).toBeGreaterThanOrEqual(2);
      } finally {
        await queue.stop();
      }
    },
    60_000,
  );

  itPg(
    "drains in-flight work on graceful stop",
    async () => {
      const queue = new PgBossQueue({
        connectionString: PG_URL,
        workOptions: { pollingIntervalSeconds: 0.5 },
      });
      await queue.start();
      const name = `it-graceful-${Date.now()}`;
      let finished = false;
      const started = deferred<void>();
      await queue.subscribe(name, async () => {
        started.resolve();
        await new Promise((resolve) => setTimeout(resolve, 400));
        finished = true;
      });
      await queue.publish(name, "x");
      await started.promise; // definitely in-flight now
      await queue.stop({ graceful: true, timeout: duration(5, TimeUnit.SECONDS) });
      expect(finished).toBe(true);
    },
    30_000,
  );

  itPg(
    "persists run state in Postgres and completes the run",
    async () => {
      const pool = new Pool({ connectionString: PG_URL, max: 4 });
      const store = new PostgresRunStore({ db: pool });
      const queue = new PgBossQueue({ connectionString: PG_URL });
      const engine = new WorkflowEngine(queue, { store });
      await queue.start();
      await engine.start();
      try {
        const name = `it-runstore-${Date.now()}`;
        const done = deferred<Record<string, unknown>>();
        const workflow = createWorkflow<unknown>(name)
          .from("a", (ctx: Context) => {
            ctx.state.a = 1;
            return 1;
          })
          .to("b", (ctx: Context, evt: QueueEvent<number>) => {
            ctx.state.b = evt.message + 1;
            done.resolve({ ...ctx.state });
            return 2;
          })
          .build();
        await engine.register(workflow);
        const runId = await engine.trigger(workflow, null);
        expect(typeof runId).toBe("string");

        await expect(done.promise).resolves.toStrictEqual({ a: 1, b: 2 });
        await waitFor(async () => (await store.load(runId!))?.status === "completed");

        const run = await store.load(runId!);
        expect(run).toMatchObject({
          workflow: name,
          version: 1,
          status: "completed",
          state: { a: 1, b: 2 },
        });
        // the idempotency ledger recorded both step attempts
        expect(await store.getStep(runId!, "a", 1)).not.toBeNull();
        expect(await store.getStep(runId!, "b", 1)).not.toBeNull();
      } finally {
        await queue.stop();
        await pool.end();
      }
    },
    60_000,
  );

  itPg(
    "routes a run by the workflow version it was triggered with",
    async () => {
      const queue = new PgBossQueue({ connectionString: PG_URL });
      const engine = new WorkflowEngine(queue);
      await queue.start();
      await engine.start();
      try {
        const name = `it-version-${Date.now()}`;
        const seen: string[] = [];
        const v1 = createWorkflow<unknown>(name)
          .from("step", () => {
            seen.push("v1");
            return 0;
          })
          .build();
        const v2 = createWorkflow<unknown>(name)
          .from("step", () => {
            seen.push("v2");
            return 0;
          })
          .build();
        await engine.register(v1, { version: 1 });
        await engine.register(v2, { version: 2 });

        await engine.trigger(name, null); // name only → latest version
        await waitFor(() => seen.length === 1);
        expect(seen).toEqual(["v2"]);
      } finally {
        await queue.stop();
      }
    },
    60_000,
  );
  itPg(
    "prunes terminal runs and their step ledger",
    async () => {
      const pool = new Pool({ connectionString: PG_URL, max: 2 });
      const store = new PostgresRunStore({ db: pool });
      try {
        const run = await store.create({ workflow: `it-prune-${Date.now()}`, version: 1 });
        await store.recordStep(run.id, {
          step: "a",
          attempt: 1,
          next: [],
          completedAt: new Date(),
        });
        run.status = "completed";
        await store.save(run);

        const removed = await store.prune({ olderThanDays: 0 });
        expect(removed).toBeGreaterThanOrEqual(1);
        expect(await store.load(run.id)).toBeNull();
        expect(await store.getStep(run.id, "a", 1)).toBeNull(); // cascaded
      } finally {
        await pool.end();
      }
    },
    30_000,
  );
  itPg(
    "records a step once under duplicate delivery (Postgres store)",
    async () => {
      const pool = new Pool({ connectionString: PG_URL, max: 4 });
      const store = new PostgresRunStore({ db: pool });
      const queue = new PgBossQueue({ connectionString: PG_URL });
      const engine = new WorkflowEngine(queue, { store });
      await queue.start();
      await engine.start();
      try {
        const name = `it-idem-${Date.now()}`;
        let runs = 0;
        const wf = createWorkflow<unknown>(name)
          .from("a", () => {
            runs += 1;
            return 0;
          })
          .build();
        await engine.register(wf);

        const run = await store.create({ workflow: name, version: 1 });
        const env = {
          runId: run.id,
          workflow: name,
          version: 1,
          step: "a",
          attempt: 1,
          data: null,
        };
        await queue.publish(WorkflowEngine.QUEUE_NAME, env);
        await waitFor(async () => (await store.getStep(run.id, "a", 1)) !== null);
        await queue.publish(WorkflowEngine.QUEUE_NAME, env); // redelivery
        await new Promise((r) => setTimeout(r, 1500));
        expect(runs).toBe(1);
      } finally {
        await queue.stop();
        await pool.end();
      }
    },
    60_000,
  );

  itPg(
    "persists lastError on a failing step",
    async () => {
      const pool = new Pool({ connectionString: PG_URL, max: 4 });
      const store = new PostgresRunStore({ db: pool });
      const queue = new PgBossQueue({ connectionString: PG_URL, queueOptions: { retryLimit: 0 } });
      const engine = new WorkflowEngine(queue, { store });
      await queue.start();
      await engine.start();
      try {
        const name = `it-err-${Date.now()}`;
        const wf = createWorkflow<unknown>(name)
          .from("boom", () => {
            throw new Error("kaput");
          })
          .build();
        await engine.register(wf);
        const runId = await engine.trigger(wf, null);
        await waitFor(async () => (await store.load(runId!))?.lastError !== undefined);
        expect((await store.load(runId!))!.lastError).toContain("kaput");
      } finally {
        await queue.stop();
        await pool.end();
      }
    },
    60_000,
  );

  itPg(
    "keeps concurrent runs isolated",
    async () => {
      const pool = new Pool({ connectionString: PG_URL, max: 4 });
      const store = new PostgresRunStore({ db: pool });
      const queue = new PgBossQueue({ connectionString: PG_URL });
      const engine = new WorkflowEngine(queue, { store });
      await queue.start();
      await engine.start();
      try {
        const name = `it-concurrent-${Date.now()}`;
        const wf = createWorkflow<number>(name)
          .from("echo", (ctx: Context, evt: QueueEvent<number>) => {
            ctx.state.v = evt.message;
            return evt.message;
          })
          .build();
        await engine.register(wf);
        const a = await engine.trigger(wf, 1);
        const b = await engine.trigger(wf, 2);
        await waitFor(async () => {
          const [ra, rb] = await Promise.all([store.load(a!), store.load(b!)]);
          return ra?.status === "completed" && rb?.status === "completed";
        });
        expect((await store.load(a!))!.state).toStrictEqual({ v: 1 });
        expect((await store.load(b!))!.state).toStrictEqual({ v: 2 });
      } finally {
        await queue.stop();
        await pool.end();
      }
    },
    60_000,
  );

  itPg(
    "rolls back a step's transaction when the step fails",
    async () => {
      const pool = new Pool({ connectionString: PG_URL, max: 4 });
      await pool.query(
        `create table if not exists it_tx_effects (id serial primary key, run_id text, n integer)`,
      );
      const store = new PostgresRunStore({ db: pool });
      const queue = new PgBossQueue({
        connectionString: PG_URL,
        workOptions: { transactional: true, pollingIntervalSeconds: 0.5 },
        queueOptions: { retryLimit: 3, retryDelay: 1 },
      });
      const engine = new WorkflowEngine(queue, { store });
      await queue.start();
      await engine.start();
      try {
        const name = `it-tx-${Date.now()}`;
        let attempts = 0;
        const done = deferred<number>();
        const wf = createWorkflow<unknown>(name)
          .from("write", async (ctx: Context) => {
            attempts += 1;
            await ctx.transaction!.query("insert into it_tx_effects (run_id, n) values ($1, $2)", [
              ctx.instanceId,
              attempts,
            ]);
            if (attempts === 1) throw new Error("first attempt fails");
            done.resolve(attempts);
            return 0;
          })
          .build();
        await engine.register(wf);
        const runId = await engine.trigger(wf, null);
        await expect(done.promise).resolves.toBeGreaterThanOrEqual(2);

        // The transaction commits only once the handler returns, so wait for the
        // run to complete before reading the effect table.
        await waitFor(async () => (await store.load(runId!))?.status === "completed");

        // The failed attempt's insert rolled back with its transaction.
        const { rows } = await pool.query<{ n: number }>(
          "select count(*)::int as n from it_tx_effects where run_id = $1",
          [runId],
        );
        expect(rows[0].n).toBe(1);
      } finally {
        await queue.stop();
        await pool.end();
      }
    },
    90_000,
  );
});

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error("timed out waiting for condition");
}
