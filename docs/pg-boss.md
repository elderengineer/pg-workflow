# Durable workflows and scheduling on pg-boss

`PgBossQueue` is a thin adapter over [pg-boss](https://github.com/timgit/pg-boss)
v12. Everything durable lives in Postgres; the engine itself keeps no state.
This document describes the tables involved, how the engine maps onto them, and
how scheduling/cron works. All examples below were verified against a live
Postgres 16 instance.

## Schema

pg-boss installs into the `pgboss` schema (configurable). Tables created:

| Table                                      | Purpose                                                            |
| ------------------------------------------ | ------------------------------------------------------------------ |
| `queue`                                    | one row per queue: policy, retry/expiry/retention config, counters |
| `job`                                      | the jobs (partitioned; common columns live in `job_common`)        |
| `job_dependency`                           | flow dependencies — **unused** by this library                     |
| `schedule`                                 | cron definitions: `cron`, `timezone`, `data`, `options`, `kind`    |
| `subscription`                             | pg-boss's own feature subscriptions (cron, monitor, bam)           |
| `queue_stats`, `bam`, `warning`, `version` | monitoring, maintenance, schema version                            |

Plus generated partitions such as `job_<n>` and `queue_stats_<date>`.

Relevant `queue` columns: `policy`, `retry_limit`, `retry_delay`,
`expire_seconds`, `retention_seconds`, `deletion_seconds`, `dead_letter`,
`total_count`, `queued_count`, `active_count`, `deferred_count`, `failed_count`.

Relevant `job_common` columns: `id`, `name`, `data` (jsonb), `state`
(`created`/`retry`/`active`/`completed`/`cancelled`/`failed`), `priority`,
`retry_count`, `start_after`, `created_on`, `completed_on`, `keep_until`,
`output`, `singleton_key`, `dead_letter`.

## How the engine maps onto pg-boss

### One shared queue, one job per step

Every step hop is a single job on the queue `pg-workflow-item`
(`WorkflowEngine.QUEUE_NAME`):

```
pg-workflow-item  →  job{ name: "pg-workflow-item",
                          data: { createTs, message: { workflow, step, context, data } } }
```

A real completed row from a two-step workflow (`charge` → `notify`):

```json
{
  "name": "pg-workflow-item",
  "state": "completed",
  "data": {
    "message": {
      "workflow": "doc-demo",
      "step": "notify",
      "data": { "amount": 100 },
      "context": { "state": { "charged": true }, "options": {} }
    }
  }
}
```

Because the whole `Context` is in `job.data`, its size and JSON limits apply —
see [`docs/context.md`](context.md).

### Queues are created automatically

pg-boss v12 requires a queue row to exist before `send`/`work`/`schedule`. The
adapter calls `createQueue` lazily on first use, once per queue, so you never
have to provision queues by hand.

### Durability and delivery

- A job is marked `completed` only if the step handler returned successfully.
  Downstream jobs are published inside the same handler.
- If a worker crashes after a step's side effect but before the next job is
  published, the job is redelivered when it expires → **at-least-once**. Make
  steps idempotent.
- `PublishOptions` map onto pg-boss job columns:

| `PublishOptions`        | pg-boss                                         |
| ----------------------- | ----------------------------------------------- |
| `startAfter`, `startAt` | `start_after` (`timestamptz`)                   |
| `retryLimit`            | `retry_limit` (default `2`)                     |
| `retryDelay`            | `retry_delay` in seconds (sub-second rounds up) |
| `expireInMinutes`       | `expire_seconds` (default `900`)                |
| `priority`              | `priority` (lower = higher priority)            |

`cancel(id)` addresses jobs by `(queue, id)` in v12, so the adapter remembers
the queue each id was published to.

## Scheduling and cron

**Yes, cron is supported.** Cron rows live in `pgboss.schedule`. Two entry
points:

```ts
// 1. Recurring workflow: runs the head step on a schedule.
await engine.trigger(workflow, { seed: 1 }, { cron: "0 8 * * MON-FRI" });

// 2. Plain queue schedule.
await queue.schedule("nightly-report", "0 3 * * *", { reportId: 7 });
```

`engine.trigger(..., { cron })` subscribes the workflow head on a queue named after
the workflow, then writes a `schedule` row. `engine.unschedule(workflow)` (and
`queue.unschedule(name)`) deletes it.

### Timezone defaults to UTC

Cron expressions are ambiguous without a zone, so pg-boss requires one and
defaults to **UTC** — verified: a schedule created without a `tz` stores
`timezone = 'UTC'`:

```
name       | cron        | timezone | kind | last_job_id is not null
cron-flow  | * * * * *   | UTC      | cron | true
```

Override the default for every schedule, or per call:

```ts
new PgBossQueue({
  connectionString: process.env.PG_URL,
  defaultScheduleTimezone: "America/Los_Angeles",
});
// or
await queue.schedule(name, cron, data, { timezone: "Europe/London" });
```

`PublishOptions.timezone` follows the same rule: it is only consulted for
`startAt: LocalDayTime` ("next Friday 8am"), is resolved via `Intl` so it is
DST-correct, and defaults to UTC — never the host's clock. An absolute
`startAt: Date` needs no timezone at all.

### Firing latency and granularity

- Cron granularity is **one minute** (standard 5-field expression).
- pg-boss fires due schedules from a worker polling every
  `cronWorkerIntervalSeconds` (default **5s**), with a monitor at 30s. Lower it
  in tests to avoid waiting: `bossOptions: { cronWorkerIntervalSeconds: 2 }`.
- A `* * * * *` schedule was observed firing within the same minute, on both
  the workflow path and the plain-queue path.
- Downtime is handled by the `missed` policy (`skip` by default; `once` sends a
  single job for the most recent missed occurrence).

### `InMemoryQueue` records but does not fire

`InMemoryQueue.schedule()` stores the entry; `pendingSchedules()` lists it and
`fireSchedule(name)` fires it manually. Wire a real cron driver, or use pg-boss,
for actual recurrence.

## Operations

Useful queries:

```sql
-- backlog and health per queue
select name, policy, retry_limit, total_count, queued_count, active_count, failed_count
from pgboss.queue order by name;

-- completed steps for a workflow instance
select id, state, retry_count, data->'message'->>'step' as step, created_on
from pgboss.job_common
where data->'message'->>'workflow' = 'doc-demo'
order by created_on;

-- active schedules
select name, cron, timezone, last_job_id from pgboss.schedule;
```

Retention is governed by pg-boss: completed jobs are deleted per
`deletion_seconds`, and never-completed jobs per `retention_seconds`. On a long
workflow, rows stay until each step completes.

## Caveats

- Run state is **not** in the `pgboss` schema. `PostgresRunStore` creates its own
  `pg_workflow_run` / `pg_workflow_step` tables (prefix overridable) in the same
  database — see [`design/run-state.md`](design/run-state.md).
- `job_dependency` and pg-boss **flows are not used**; the engine has no join
  primitive (see the README roadmap).
- Schema migrations are pg-boss's responsibility — the adapter does not
  manage the `pgboss` schema.
- The `subscription` table holds pg-boss's own subscriptions, not your
  workflows; your dispatch is the single `pg-workflow-item` subscription plus
  one queue per cron workflow.
