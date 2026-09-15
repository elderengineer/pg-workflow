# Examples

All examples use the same three imports:

```ts
import {
  WorkflowEngine,
  InMemoryQueue,
  createWorkflow,
  duration,
  TimeUnit,
  type Context,
  type QueueEvent,
} from "pg-workflow";
```

For the conceptual model behind these, see
[`architecture.md`](architecture.md) and [`context.md`](context.md).

## Quick start

```ts
const queue = new InMemoryQueue();
const engine = new WorkflowEngine(queue, {
  onError: ({ workflow, step, error }) => console.error(`${workflow}/${step}`, error),
});
await engine.start();

const greet = createWorkflow<{ name: string }>("greet")
  .from("hello", (_ctx, evt) => `hello ${evt.message.name}`)
  .to("shout", (_ctx, evt: QueueEvent<string>) => evt.message.toUpperCase())
  .build();

await engine.register(greet);
await engine.trigger(greet, { name: "ada" });
```

## Passing data

The return value goes to the next step only; `context.state` survives the whole
run.

```ts
createWorkflow<{ orderId: string }>("checkout")
  .from("load", async (ctx, evt) => {
    const order = await loadOrder(evt.message.orderId);
    ctx.state.customerId = order.customerId; // visible to every later step
    return order; // visible to the next step only
  })
  .to("charge", async (ctx, evt: QueueEvent<Order>) => {
    await charge(evt.message, ctx.state.customerId as string);
    return { charged: true };
  })
  .build();
```

## Conditionals

`.if(...)` forwards the message unchanged when true and **ends the run** when
false. Start with `.fromIf(...)` if the first step is a condition.

```ts
const process = createWorkflow<{ paid: boolean }>("process")
  .fromIf("is-paid", (_ctx, evt) => evt.message.paid)
  .to("ship", () => ({ shipped: true }))
  .build();
```

## Loops

`while` re-queues itself while the predicate is true. The predicate is the only
code that runs each iteration (it should perform the work or poll), and the
iteration count persists in `context.state`, so a runaway loop is stopped by
`maxIterations`.

```ts
const waitForJob = createWorkflow<{ jobId: string }>("wait-for-job")
  .from("start", (_ctx, evt) => evt.message)
  .while(
    "poll",
    async (ctx, evt) => {
      const status = await jobStatus(evt.message.jobId);
      ctx.state.polls = ((ctx.state.polls as number) ?? 0) + 1;
      return status !== "done";
    },
    {},
    60, // maxIterations — throws when exceeded
  )
  .to("finish", (ctx) => ({ polls: ctx.state.polls }))
  .build();
```

## Fan-out

`stream` turns one array into one message per element. A `startAfter` on the
following step is multiplied by the element index, staggering the fan-out
(30s, 60s, 90s, …) instead of firing everything at once.

```ts
const notify = createWorkflow<{ userIds: string[] }>("notify")
  .from("load-users", (ctx, evt) => evt.message.userIds)
  .stream<string>("per-user")
  .to(
    "send",
    async (_ctx, evt: QueueEvent<string>) => {
      await sendEmail(evt.message);
      return null;
    },
    { startAfter: duration(30, TimeUnit.SECONDS) },
  )
  .build();
```

`batchedStream` groups elements instead of splitting one per element. Note the
generic difference: `stream<T>` declares the **element** type, while
`batchedStream<T[]>` declares the **batch** type.

```ts
const index = createWorkflow<{ ids: string[] }>("index")
  .from("load-ids", (ctx, evt) => evt.message.ids)
  .batchedStream<string[]>("batches", 100)
  .to("bulk-insert", async (_ctx, evt: QueueEvent<string[]>) => {
    await insertAll(evt.message); // up to 100 at a time
    return null;
  })
  .build();
```

## Errors and hooks

Hook into errors and successes without depending on a tracer.

```ts
const engine = new WorkflowEngine(queue, {
  onError: ({ workflow, step, eventId, error }) => {
    metrics.increment(`workflow.error`, { workflow, step });
    console.error(workflow, step, eventId, error);
  },
  onStepSuccess: ({ workflow, step }) => metrics.increment(`workflow.step`, { workflow, step }),
  logger: console,
});
```

A step that throws is logged, reported to `onError`, and **rethrows** so the
backend can retry it. Make side effects idempotent, keyed on
`context.instanceId`.

```ts
.to("charge", async (ctx, evt) => {
  await chargeOnce(evt.message, idempotencyKey(ctx.instanceId!));
  return { charged: true };
})
```

## Per-step options

`PublishOptions` control retries, priority and expiry. Options passed to
`trigger` are merged under each step's own options.

```ts
createWorkflow<unknown>("risky")
  .from("call-api", () => fetchSomething(), {
    retryLimit: 5,
    retryDelay: duration(10, TimeUnit.SECONDS),
    expireInMinutes: 2,
    priority: 1, // lower number = higher priority
  })
  .build();
```

## Delays and wall-clock times

```ts
// Relative delay
await engine.trigger(wf, data, { publish: { startAfter: duration(15, TimeUnit.MINUTES) } });

// Absolute instant (timezone-free)
await engine.trigger(wf, data, { publish: { startAt: new Date("2026-10-01T12:00:00Z") } });

// Wall clock in a named zone, DST-correct; defaults to UTC
const monday9am: LocalDayTime = { dayOfTheWeek: 1, time: duration(9, TimeUnit.HOURS) };
await engine.trigger(wf, data, {
  publish: { startAt: monday9am, timezone: "Europe/London" },
});
```

Any duration can also be a number (milliseconds) or an ISO-8601 string:
`duration(90, TimeUnit.SECONDS)`, `90_000`, `"PT90S"`, `"P1DT12H"`.

## Cron

Run a workflow's head on a schedule, then remove the schedule when done.

```ts
await engine.trigger(dailyReport, { for: "yesterday" }, { cron: "0 3 * * *" });

// …later
await engine.unschedule(dailyReport);
```

Cron is also available directly on the queue, with an explicit timezone:

```ts
await queue.schedule(
  "nightly-report",
  "0 3 * * *",
  { reportId: 7 },
  {
    timezone: "America/New_York",
    priority: 5,
  },
);
```

Cancelling a single enqueued job:

```ts
const id = (await engine.trigger(wf, data)) as string | null;
if (id) await engine.cancel(id);
```

## Postgres in production

Pair the durable queue with a durable **run store**, or run state is lost when
the process exits.

```ts
import { Pool } from "pg";
import { PgBossQueue, PostgresRunStore, WorkflowEngine } from "pg-workflow";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const store = new PostgresRunStore({ db: pool }); // creates its tables on first use

const queue = new PgBossQueue({
  connectionString: process.env.DATABASE_URL,
  bossOptions: { cronWorkerIntervalSeconds: 5 }, // merged over connectionString
  defaultScheduleTimezone: "UTC",
  onError: (error) => console.error("[queue]", error),
});

const engine = new WorkflowEngine(queue, { store });
await queue.start();
await engine.start();

// trigger returns the run id (null for a cron schedule)
const runId = await engine.trigger(checkout, { orderId: "ord_123" });
const run = await store.load(runId!); // inspect state any time
```

`pg-boss` is an optional peer dependency, loaded lazily — install it alongside
this package. `PostgresRunStore` uses a `pg` pool you provide (a `pg.Pool` or
`pg.Client` satisfies it structurally). See [`pg-boss.md`](pg-boss.md) for the
queue tables and [`cluster.md`](cluster.md) for retention.

## Versioned workflows

Register a new version alongside the old one; in-flight runs keep dispatching to
the version they started on.

```ts
await engine.register(checkoutV1, { version: 1 });
await engine.register(checkoutV2, { version: 2 });

await engine.trigger("checkout", data, { publish: {} }); // name only → latest (v2)
```

## Inspecting and cancelling a run

```ts
const run = await store.load(runId);
// { id, workflow, version, status: "running"|"completed"|…, state, currentStep, attempts }

await engine.cancelRun(runId); // stop dispatching further steps
await store.prune({ olderThanDays: 30 }); // delete terminal runs + their ledger
```

## A custom queue backend

Anything that satisfies `Queue` works, with no changes to workflows.

```ts
import type {
  Queue,
  QueueEvent,
  QueueName,
  PublishOptions,
  ScheduleOptions,
  StopOptions,
} from "pg-workflow";

class MyQueue implements Queue {
  private stopped = false;

  isStopped(): boolean {
    return this.stopped;
  }
  async start(): Promise<void> {}
  async stop(_options?: StopOptions): Promise<void> {
    this.stopped = true;
  }
  async publish(
    _queue: QueueName,
    _event: unknown,
    _options?: PublishOptions,
  ): Promise<string | null> {
    // send to your broker; return the job id, or null if unsupported
    return null;
  }
  async cancel(_id: string): Promise<void> {}
  async schedule(
    _queue: QueueName,
    _cron: string,
    _event: unknown,
    _options?: ScheduleOptions,
  ): Promise<void> {}
  async unschedule(_queue: QueueName): Promise<void> {}
  async subscribe<T>(
    _queue: QueueName,
    _subscriber: (event: QueueEvent<T>) => Promise<void>,
  ): Promise<void> {}
}
```

## A custom step

Implement `Step<In, Out>` to add behaviour the builder doesn't cover, e.g.
timing or metrics around any step.

```ts
import type { Context, PublishOptions, QueueEvent, Step, StepResult } from "pg-workflow";

class TimingStep implements Step<unknown, unknown> {
  readonly name: string;
  readonly options: PublishOptions = {};
  nextSteps: Step<unknown, unknown>[] = [];

  constructor(
    name: string,
    private readonly inner: Step<unknown, unknown>,
  ) {
    this.name = name;
    this.nextSteps = [...inner.nextSteps];
  }

  async run(context: Context, event: QueueEvent<unknown>): Promise<StepResult<unknown>> {
    const startedAt = Date.now();
    try {
      return await this.inner.run(context, event);
    } finally {
      metrics.observe("step.duration", Date.now() - startedAt, { step: this.name });
    }
  }

  clone(): Step<unknown, unknown> {
    const copy = new TimingStep(this.name, this.inner);
    copy.nextSteps = [...this.nextSteps];
    return copy;
  }
}
```

## Testing

`InMemoryQueue` needs no infrastructure. `schedule()` records the entry without
firing it; `fireSchedule()` triggers it on demand.

```ts
import { InMemoryQueue, WorkflowEngine, createWorkflow } from "pg-workflow";

it("runs a workflow", async () => {
  const queue = new InMemoryQueue();
  const engine = new WorkflowEngine(queue);
  await engine.start();

  const done = new Promise((resolve) => {
    const wf = createWorkflow<unknown>("t")
      .from("a", () => 1)
      .to("b", (ctx) => {
        ctx.state.seen = true;
        resolve(ctx.state);
        return 2;
      })
      .build();
    void engine.register(wf).then(() => engine.trigger(wf, {}));
  });

  await expect(done).resolves.toStrictEqual({ seen: true });
});
```

```ts
it("fires a recorded schedule", async () => {
  const queue = new InMemoryQueue();
  const seen: unknown[] = [];
  await queue.subscribe("nightly", async (evt) => seen.push(evt.message));

  await queue.schedule("nightly", "0 3 * * *", { reportId: 7 });
  expect(queue.pendingSchedules()).toContain("nightly");

  await queue.fireSchedule("nightly");
  expect(seen).toStrictEqual([{ reportId: 7 }]);
});
```

## Shutdown

```ts
await engine.unschedule(dailyReport);
await queue.stop({ graceful: true });
```
