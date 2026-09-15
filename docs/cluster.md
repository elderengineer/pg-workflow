# Running in a cluster: producers and workers

A pg-workflow deployment has up to four roles. They are just code paths in your
app — a single process may play several, or you may split them across services.

```
   ┌────────────┐        ┌────────────┐        ┌──────────────┐
   │  producer  │        │  worker    │        │  scheduler   │
   │            │        │  (N pods)  │        │  (cron)      │
   │ enqueue    │        │ run steps  │        │ write cron   │
   └─────┬──────┘        └─────┬──────┘        └──────┬───────┘
         │ publish             │ claim                │ schedule
         ▼                     ▼                      ▼
   ┌──────────────────────────────────────────────────────────┐
   │                    Postgres  (pgboss schema)              │
   │  queue · job · schedule · subscription · …               │
   └──────────────────────────────────────────────────────────┘
```

| Role                 | Calls                                                                         | Does **not** call                       |
| -------------------- | ----------------------------------------------------------------------------- | --------------------------------------- |
| **Producer**         | `queue.start()`, `engine.register()`, `engine.trigger()`                      | `engine.start()`                        |
| **Worker**           | `queue.start()`, `engine.register()`, `engine.start()`                        | —                                       |
| **Scheduler**        | `engine.trigger(wf, data, { cron })`                                          | `engine.start()` (unless also a worker) |
| **Admin / operator** | `stats()`, `getJob()`, `retryJob()`, `rescheduleJob()`, `redriveDeadLetter()` | —                                       |

## Producer only

A producer enqueues work. It must **not** call `engine.start()`, because that
subscribes a worker and would make the producer compete for the very jobs it
enqueues.

```ts
const queue = new PgBossQueue({ connectionString: process.env.DATABASE_URL });
const engine = new WorkflowEngine(queue);

await queue.start(); // connect pg-boss and create queues on demand
await engine.register(checkout); // needed to trigger by name
await engine.trigger(checkout, { orderId });

// no engine.start() — this process only produces
await queue.stop({ graceful: true });
```

`queue.start()` is needed even for producers: pg-boss requires a started
instance before it will `send`.

## Run state

Run state lives in a `RunStore`, separate from the queue. **In production, pair a
durable queue with a durable store** — the in-memory default loses every run
when the process exits:

```ts
import { Pool } from "pg";
import { PgBossQueue, PostgresRunStore, WorkflowEngine } from "pg-workflow";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const store = new PostgresRunStore({ db: pool }); // creates its tables on first use
const queue = new PgBossQueue({ connectionString: process.env.DATABASE_URL });
const engine = new WorkflowEngine(queue, { store });
```

Every process — producers, workers, schedulers — must use the **same** store, so
they see the same run rows. The store creates `pg_workflow_run` and
`pg_workflow_step` (prefix overridable) on first use; they live in the same
database as `pgboss`, not inside its schema.

### Transactional steps (optional)

To make a step's **database** side effects commit atomically with its run-state
save and the job completion, run the worker transactionally:

```ts
new PgBossQueue({ connectionString, workOptions: { transactional: true } });
```

Steps can then write through `context.transaction`, and a failing step rolls
those writes back instead of repeating them on retry — see
[`context.md`](context.md#closing-the-window-transactional-steps). External side
effects (HTTP, email) and the publish of downstream hops remain outside the
transaction, so they are still at-least-once.

## Workers

A worker consumes. Scale horizontally — add replicas, and pg-boss hands each
job to exactly one of them (`FOR UPDATE SKIP LOCKED` under the hood).

```ts
const queue = new PgBossQueue({
  connectionString: process.env.DATABASE_URL,
  // Per-worker concurrency. Raise it for I/O-bound steps, lower it to protect
  // a fragile downstream.
  workOptions: { localConcurrency: 8, pollingIntervalSeconds: 2 },
});
const engine = new WorkflowEngine(queue, { store, hooks: { onError, onStepSuccess } });

await queue.start();
await engine.register(checkout); // every worker registers the same workflows
await engine.register(otherWorkflow);
await engine.start(); // subscribe → this process now runs jobs

process.on("SIGTERM", async () => {
  await queue.stop({ graceful: true, timeout: duration(30, TimeUnit.SECONDS) });
  await pool.end();
  process.exit(0);
});
```

Two rules for workers:

1. **Every worker registers the same workflows.** A job dispatched to a worker
   that never registered its workflow raises `WorkflowNotFound`.
2. **Drain on shutdown.** `stop({ graceful: true })` lets in-flight steps finish
   before the process exits; without it, an interrupted step is redelivered and
   runs twice.

Useful `workOptions` (pg-boss `WorkOptions`):

| Option                   | Meaning                                           |
| ------------------------ | ------------------------------------------------- |
| `localConcurrency`       | Workers spawned per node for this queue           |
| `batchSize`              | Jobs fetched per poll (handler receives an array) |
| `pollingIntervalSeconds` | Base poll interval (≥ 0.5; default 2)             |
| `groupConcurrency`       | Per-`group` limit coordinated across the cluster  |

## Scheduler (cron)

`engine.trigger(wf, data, { cron })` writes a row to `pgboss.schedule`. pg-boss
fires due schedules from a scheduler worker. By default **any started instance**
runs the scheduler, coordinated through the database, so with several workers
you do not need a dedicated process. To pin it to one role:

```ts
// workers: scheduling disabled
new PgBossQueue({ connectionString, bossOptions: { schedule: false, supervise: false } });
// scheduler: scheduling enabled (the default)
new PgBossQueue({ connectionString });
```

Only call `trigger(..., { cron })` once per workflow — it is idempotent per
process (the engine guards the head subscription) but writes a schedule row.
`engine.unschedule(workflow)` removes it.

## Capacity and back-pressure

### Back-pressure is out of scope, by design

The engine deliberately implements **no back-pressure**: no admission control, no
producer throttling, and no load shedding. Work is buffered durably in the queue
and the engine never rejects or discards it to protect itself. Back-pressure is
a bad user experience — it means someone loses data or functionality — so the
intended response to overload is to **add worker replicas** (or raise
`localConcurrency`) and let DevOps resolve the capacity problem, rather than
making callers fail or silently dropping work.

### But it still warns you

**Does the engine warn when it cannot keep up?** pg-boss does, and this library
forwards it. pg-boss compares each queue's backlog against `warningQueueSize`
and emits a `queue_backlog` warning; wire it with `onWarning`:

```ts
new PgBossQueue({
  connectionString,
  queueOptions: { warningQueueSize: 5_000 }, // default 10_000
  onWarning: ({ type, message, data }) => {
    if (type === "queue_backlog") alertOps(message, data);
  },
});
```

Three things to know:

- **The default threshold is 10,000 queued jobs**, and it covers jobs in the
  `created`/`retry` states (including future-dated ones).
- **Warning latency is governed by pg-boss's supervise/monitor passes**, which
  default to **60s each**. Lower them if you want faster detection:
  `bossOptions: { superviseIntervalSeconds: 5, monitorIntervalSeconds: 5 }`.
- `queue.stats(queue)` returns a snapshot for dashboards, but it reads pg-boss's
  **cached** counters and is **eventually consistent** (same 60s refresh). Do not
  gate an instantaneous decision on it.

When a backlog warning fires, the responses are to add worker replicas, raise
`localConcurrency` for I/O-bound steps, move a heavy workflow to its own queue,
or scale the database — never to shed work.

> Because there is no shedding, sustained overload has a **deadline**: a job that
> waits longer than the queue's `retentionSeconds` (default 14 days) is deleted
> before it ever runs. Treat the backlog warning as an operational deadline, not
> just a metric.

### Scheduled jobs run late, not skipped

`startAfter` / `startAt` mean _"not before"_, not _"at exactly"_. If workers are
busy, a scheduled job runs as soon as capacity is available — it is **delayed,
not dropped**.

- `expireInSeconds` (default 900) only starts counting once a job becomes
  `active`. Time spent waiting in the queue does **not** count toward it.
- `retentionSeconds` (default 14 days) will eventually delete a job that never
  runs. A job waiting less than that is safe.
- For cron, occurrences missed while nothing was running follow the schedule's
  `missed` policy (`skip` by default; `once` sends a single job for the most
  recent missed occurrence).

This is why a backlog is a capacity problem, not a correctness one — work is
preserved, it just happens later.

## Deploying

- **Rolling deploy:** start new workers, register workflows, `engine.start()`,
  then `stop({ graceful: true })` the old ones. In-flight steps finish.
- **Versioned graphs:** register a new version alongside the old
  (`register(wf, { version: 2 })`). In-flight runs keep dispatching to the
  version they started on, so the two graphs never mix.
- **Queue creation races are safe.** Every instance lazily creates queues;
  pg-boss's `create_queue` is idempotent. The run-store tables are created with
  `create table if not exists`, so they are safe to race too.
- **Schema migrations** belong to pg-boss; the run store only ever creates its
  own tables.

## Retention

Nothing prunes itself. Two clocks run independently:

- **pg-boss jobs** are governed by the queue's `retentionSeconds` /
  `deleteAfterSeconds` (see [`pg-boss.md`](pg-boss.md)).
- **Runs** are pruned by you, explicitly:

```ts
const removed = await store.prune({ olderThanDays: 30 }); // terminal runs + their steps
```

`prune` deletes `completed` / `failed` / `cancelled` runs older than the cutoff
and cascades to their step-ledger rows. Run it on a schedule (a cron workflow is
a tidy place) and size `olderThanDays` to how long you need run history for
debugging. There is an index on `(updated_at)` for terminal runs to keep it cheap.

## Admin tooling

Operators (and AI agents — see [`DEBUG.md`](DEBUG.md)) can inspect and repair
from any process, or via the bundled CLI:

```sh
npm run build
PG_URL=postgres://… node scripts/pg-workflow-admin.mjs stats pg-workflow-item
PG_URL=postgres://… node scripts/pg-workflow-admin.mjs run <runId>
PG_URL=postgres://… node scripts/pg-workflow-admin.mjs prune --days 30
```
