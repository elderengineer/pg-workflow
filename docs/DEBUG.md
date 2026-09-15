# Debugging failures

How to find, understand and repair a failed or stuck run. Written for operators
and for AI agents doing triage — see [Agent instructions](#agent-instructions).

## Where a failure shows up

A failed step appears in four places:

1. **Your hooks** — `EngineHooks.onError({ workflow, step, eventId, error })`.
   This is the fastest signal; wire it to your alerting.
2. **Logs** — the engine logs `${step} failed: ${error}` to `hooks.logger`.
3. **The job row** — `pgboss.job_common.state = 'failed'`, with the serialized
   error in the `output` column.
4. **A dead-letter queue** — if the queue was created with `deadLetter`, the job
   is moved there once retries are exhausted.

> The engine **rethrows** after calling `onError`. Retrying is the backend's
> job: pg-boss re-runs the job per the queue's `retryLimit` / `retryDelay`, then
> marks it `failed` (or dead-letters it).

## Step 1 — read the error

The stored error is the single most useful thing. Read it before doing anything.

**Library:**

```ts
const job = await queue.getJob(WorkflowEngine.QUEUE_NAME, jobId);
console.log(job.state, job.retryCount, job.retryLimit);
console.log(job.output); // the serialized error
```

**CLI:**

```sh
npm run build
PG_URL=postgres://… node scripts/pg-workflow-admin.mjs show pg-workflow-item <jobId>
```

**SQL (no code):**

```sql
select id, state, retry_count, retry_limit, created_on, completed_on, output
from pgboss.job_common
where name = 'pg-workflow-item'
  and state = 'failed'
order by created_on desc
limit 20;
```

Which queue? Workflow **step** jobs live on `pg-workflow-item`. A workflow
started by **cron** is enqueued on a queue named after the workflow (see
[`cluster.md`](cluster.md)).

## Step 2 — classify

| Symptom in `output`                                  | Class         | Action                                                              |
| ---------------------------------------------------- | ------------- | ------------------------------------------------------------------- |
| `ETIMEDOUT`, `ECONNREFUSED`, `503`, rate-limit       | **Transient** | Reschedule for later (backoff), or retry if the step is idempotent  |
| Expired mid-run (`state` was `active`, then retried) | **Transient** | Retry; consider a larger `expireInMinutes`                          |
| Validation error, `4xx`, "not found", bad payload    | **Permanent** | Fix the data or code; do **not** blindly retry                      |
| `WhileStep … exceeded maxIterations`                 | **Logic**     | A loop condition never went false; fix the workflow                 |
| `WorkflowNotFound` / `StepNotFound`                  | **Deploy**    | A worker ran without registering the workflow — redeploy the worker |
| `Unsupported duration` / serialization errors        | **Data**      | See [`context.md`](context.md) — non-JSON data in `context`         |

**Always ask first: is this step idempotent?** At-least-once delivery means a
retry re-runs the step. If it charges a card or sends an email without a
dedupe key, retrying can duplicate the side effect. When unsure, prefer
`reschedule` over `retry`, and investigate before acting.

## Step 3 — repair

All four operations are available in the library and the CLI.

### Retry now

Re-runs a `failed` job immediately.

```ts
await queue.retryJob(queueName, jobId);
```

```sh
node scripts/pg-workflow-admin.mjs retry pg-workflow-item <jobId>
```

### Reschedule for later

Keeps the job but moves its start time — the right choice for a transient
downstream outage, because it backs off instead of hammering.

```ts
await queue.rescheduleJob(queueName, jobId, {
  startAfter: duration(10, TimeUnit.MINUTES),
});
```

```sh
node scripts/pg-workflow-admin.mjs reschedule pg-workflow-item <jobId> --after PT10M
# or --after 600   (seconds)
```

### Redrive a dead-letter queue

Moves jobs from a DLQ back to their source queue.

```ts
const moved = await queue.redriveDeadLetter("checkout-dlq", { sourceName: "checkout" });
```

```sh
node scripts/pg-workflow-admin.mjs redrive checkout-dlq --source checkout --limit 100
```

### Cancel

Stops a queued/deferred job. Only works on jobs that are not `active`.

```ts
await queue.cancelJob(queueName, jobId);
```

To stop a whole **run** — every further step it would dispatch — cancel the run
record instead:

```ts
await engine.cancelRun(runId); // next dispatch observes status 'cancelled' and stops
```

### Inspect a run

Run state is a row in `pg_workflow_run`; the step ledger is `pg_workflow_step`.

```ts
const run = await store.load(runId);
// { id, workflow, version, status, state, currentStep, attempts, lastError, … }
```

```sh
node scripts/pg-workflow-admin.mjs run <runId>
```

```sql
select id, workflow, version, status, current_step, attempts, last_error, updated_at
from pg_workflow_run
where status = 'running' and updated_at < now() - interval '1 hour';  -- stuck runs
```

`status` is one of `running`, `waiting`, `completed`, `failed`, `cancelled`.
A `failed` run's `last_error` is the same error the job recorded.

### Inspect a backlog

```ts
await queue.stats(queueName); // { queued, ready, active, deferred, failed, total }
```

```sh
node scripts/pg-workflow-admin.mjs stats pg-workflow-item
```

> `stats()` is **eventually consistent** — pg-boss refreshes counts every
> `queueCacheIntervalSeconds` (default 60s). Use it for dashboards, not for
> "is this job done yet" polling. For that, poll `getJob(...).state`.

### Prune history

Run rows and their ledger grow forever unless you delete them.

```ts
await store.prune({ olderThanDays: 30 }); // terminal runs + cascaded steps
```

```sh
node scripts/pg-workflow-admin.mjs prune --days 30
```

Prune only terminal runs (`completed`/`failed`/`cancelled`) — a `running` run is
never touched, so this is safe to schedule.

## Agent instructions

If you are an AI agent asked to debug or repair a pg-workflow run, follow this
loop. Do not skip step 2, and do not batch destructive actions.

1. **Identify the queue and job id.** `pg-workflow-item` for a step; the
   workflow name for a cron-started run.
2. **Read before you write.** Run `show <queue> <id>` (or `getJob`). Capture
   `state`, `retryCount`, `retryLimit`, and the text of `output`.
3. **Classify** the error using [Step 2](#step-2--classify). State your
   classification and the evidence (the error text) explicitly.
4. **Check idempotency before any retry.** If you cannot tell whether the step
   is safe to re-run, stop and ask a human. Do not guess.
5. **Choose the least aggressive action:**
   - transient → `reschedule` with a backoff (e.g. `--after PT10M`), not `retry`
   - permanent → do **not** retry; report the fix needed
   - logic/deploy → do **not** retry; report the workflow/worker problem
6. **Apply once, then verify.** Re-read the job with `show` and confirm the new
   `state`. Never issue the same command in a loop.
7. **Report what you did:** the job id, the error, your classification, the
   command run, and the observed result.

Hard rules for agents:

- **Never** cancel or retry a job whose `state` is `active` — a worker is
  running it. `cancel` will fail; a retry can duplicate work.
- **Never** redrive an entire DLQ blindly. Inspect a sample first
  (`--limit 1`), confirm they share one root cause, then widen the limit.
- **Never** retry more than the queue's `retryLimit` in a single triage without
  human approval — escalating retries indicate a permanent fault.
- **Do not** edit `context.state` or job payloads by hand; the engine owns the
  envelope, and a malformed one fails with `WorkflowNotFound`/`StepNotFound`.

## Worked example

A step `charge` failed with `ETIMEDOUT` talking to a payment provider.

```sh
$ node scripts/pg-workflow-admin.mjs show pg-workflow-item 4f1c…
{
  "id": "4f1c…",
  "queue": "pg-workflow-item",
  "state": "failed",
  "retryCount": 2,
  "retryLimit": 2,
  "output": { "name": "Error", "message": "ETIMEDOUT" }
}
```

Reasoning: retries are already exhausted (`retryCount === retryLimit`) and the
error is transient, so an immediate retry is likely to fail again. Back off, and
confirm the step is idempotent before re-running it (it uses a per-order
idempotency key, so it is safe).

```sh
$ node scripts/pg-workflow-admin.mjs reschedule pg-workflow-item 4f1c… --after PT15M
rescheduled pg-workflow-item/4f1c… to run after PT15M

$ node scripts/pg-workflow-admin.mjs show pg-workflow-item 4f1c…
{ "state": "retry", "retryCount": 2, … }
```

The job is now `retry`, waiting until 15 minutes from now. Done.

## Related

- [`cluster.md`](cluster.md) — back-pressure, warnings and lateness.
- [`context.md`](context.md) — why bad data in `context` fails a step.
- [`pg-boss.md`](pg-boss.md) — the tables and their columns.
