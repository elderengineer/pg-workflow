# pg-workflow

Queue-agnostic workflow engine for TypeScript: a fluent builder (`from` / `to` /
`if` / `while` / `stream` / `batchedStream`), composable steps, and a
single-queue engine with pluggable backends and observability hooks.

Ships `InMemoryQueue` (tests, local dev, zero infrastructure) and a
`PgBossQueue` adapter for Postgres-backed durability.

## Install

Published to GitHub Packages. Point npm at the registry and authenticate once —
public packages here still require a token with `read:packages`:

```sh
# .npmrc
@elderengineer:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

```sh
npm install @elderengineer/pg-workflow
```

Alternatively, install straight from git (no registry or token):

```sh
npm install github:elderengineer/pg-workflow
```

Using the Postgres backends also needs `pg` and `pg-boss` (both optional peers):

```sh
npm install pg pg-boss
```

## Quick start

```ts
import { WorkflowEngine, InMemoryQueue, createWorkflow } from "@elderengineer/pg-workflow";

const engine = new WorkflowEngine(new InMemoryQueue(), {
  onError: ({ workflow, step, error }) => console.error(workflow, step, error),
});
await engine.start();

const wf = createWorkflow<{ msg: string }>("hello")
  .from("step-1", (ctx) => {
    ctx.state["s1"] = 1;
    return 1;
  })
  .to("step-2", (ctx, evt) => {
    ctx.state["s2"] = evt.message + 1;
    return 2;
  })
  .build();

await engine.register(wf);
await engine.trigger(wf, { msg: "hi" });
```

## Queue backends

The engine only depends on the `Queue` interface
(`publish` / `subscribe` / `schedule` / `unschedule` / `cancel` / `start` /
`stop`), so any backend works without touching workflow definitions.

```ts
import { WorkflowEngine, PgBossQueue } from "@elderengineer/pg-workflow";

const engine = new WorkflowEngine(
  new PgBossQueue({
    connectionString: process.env.PG_URL,
    defaultScheduleTimezone: "UTC",
    onError: (error) => console.error("[queue]", error),
  }),
);
```

`InMemoryQueue` supports `publish` / `subscribe` / `cancel` plus
`startAfter` / `startAt` delays. `schedule()` records the cron entry; call
`fireSchedule(queue)` in tests or wire a real cron driver.

## Passing data

Three channels, not interchangeable:

| Channel                            | Scope           | Use for                               |
| ---------------------------------- | --------------- | ------------------------------------- |
| **Return value** → `event.message` | exactly one hop | the value the next step consumes      |
| **`context.state`**                | the whole run   | cross-step bookkeeping, counters, ids |
| **`context.options`**              | the whole run   | `PublishOptions` from `trigger`       |

```ts
.from("a", (_ctx, evt) => {
  evt.message;        // trigger payload
  return { n: 42 };   // → next step's evt.message (not accumulated)
})
.to("b", (ctx, evt) => {
  evt.message;        // { n: 42 }
  ctx.state.count = (ctx.state.count ?? 0) + 1; // visible to all later steps
})
```

`context.state` lives in the **run store** — a row per run, not the job message —
so a run can be inspected, cancelled and deduplicated. It is loaded before a
step and saved after, which means it must be JSON-serializable and **small**
(identifiers and counters; put blobs in object storage).

**Full details, restrictions and limitations: [`docs/context.md`](docs/context.md).**
In short — state is stored as `jsonb`: functions and class instances do not
survive, `Date` becomes a string, `Map`/`Set` become `{}`, and `BigInt` throws.

## Durations

Anything that resolves to milliseconds is accepted wherever a duration is
expected (`PublishOptions.startAfter` / `retryDelay`, `StopOptions.timeout`,
`LocalDayTime.time`):

```ts
duration(30, TimeUnit.SECONDS); // Duration value
30_000; // number = milliseconds
("PT30S"); // ISO-8601 string ("P1DT12H", "PT0.5S", "-PT1M")
Temporal.Duration.from({ minutes: 5 }); // Temporal.Duration (Node 26+) or polyfill
```

Years and months are rejected — they are not a fixed number of milliseconds.
Temporal only becomes native in Node 26; until then use the other forms — the
API does not change when you switch. Durations carried in `context.options` are
rehydrated automatically after serialization.

`startAt` additionally accepts a wall-clock time, resolved in an IANA timezone
(default UTC):

```ts
import { duration, TimeUnit, type LocalDayTime } from "@elderengineer/pg-workflow";

const friday8am: LocalDayTime = { dayOfTheWeek: 5, time: duration(8, TimeUnit.HOURS) };
await engine.trigger(wf, data, { publish: { startAt: friday8am, timezone: "Asia/Tokyo" } });
```

## Layout

```
src/
  index.ts
  duration.ts            FiniteDuration value object
  local-time.ts          LocalDayTime + timezone-aware resolution
  queue/
    index.ts             Queue, QueueEvent, PublishOptions
    in-memory-queue.ts   In-memory Queue backend
    pg-boss-queue.ts     Postgres-backed Queue backend (pg-boss)
  workflow/
    types.ts             Context, StepData, Step, Workflow, Engine, EngineHooks
    steps.ts             SimpleStep, IfStep, WhileStep, StreamStep, BatchedStreamStep
    run-store.ts         RunRecord, RunStore, InMemoryRunStore, PostgresRunStore
    workflow-builder.ts  createWorkflow() + fluent from()/to()/if()/while()/stream()
    workflow-engine.ts   single-shared-queue engine
    index.ts
tests/
  unit/                  no infrastructure required (`npm test`)
  integration/           requires Postgres (`npm run test:integration`)
scripts/                 admin CLI (`npm run admin -- <command>`)
bench/                   design micro-benchmarks (`npm run bench`)
docs/workflow.md         what a workflow engine is (intro)
docs/architecture.md     engine design, execution model, trade-offs
docs/context.md          data channels + context restrictions
docs/examples.md         task-oriented examples
docs/cluster.md          producer/worker topology, capacity, cron
docs/DEBUG.md            debugging failures + reschedule tooling
docs/pg-boss.md          tables, durability and cron on pg-boss
docs/design/             design proposals (not yet implemented)
docker-compose.yml       local Postgres for integration tests
```

All imports are relative — no path aliases required.

## Documentation

- [What is a workflow engine?](docs/workflow.md) — start here if the concept is new.
- [Examples](docs/examples.md) — task-oriented snippets.
- [Architecture](docs/architecture.md) — execution model and trade-offs.
- [Context and data](docs/context.md) — passing data between steps, and its limits.
- [Cluster](docs/cluster.md) — producers, workers, capacity and cron.
- [Debugging](docs/DEBUG.md) — triage and repair, with admin tooling.
- [pg-boss internals](docs/pg-boss.md) — tables and durability.

## Design notes

- **Single shared queue** (`pg-workflow-item`): every step publishes to one
  queue; the envelope carries `{ runId, workflow, version, step, attempt }` for
  dispatch, and the per-hop value.
- **Run state lives in a `RunStore`**: one row per run holding `state`, plus an
  append-only step ledger used for idempotency. Defaults to in-memory for
  tests/dev; pair a durable queue with `PostgresRunStore` in production (see
  [Cluster](docs/cluster.md)).
- **Idempotency**: a step's outcome is recorded _before_ the next hop is
  published, so a redelivered job re-forwards instead of re-running the step
  body. Steps with external side effects should still be idempotent.
- **Optional transactional steps**: `workOptions: { transactional: true }` runs
  a step in the job's DB transaction, so database side effects written through
  `context.transaction` commit (or roll back) with the run state — see
  [Context](docs/context.md#closing-the-window-transactional-steps).
- **Versioned workflows**: `register(workflow, { version })`; a run dispatches
  to the version it started on, so a rolling deploy cannot mix step graphs.
- **Per-step tuning** via `PublishOptions`: the engine merges
  `context.options → step.options` on each hop.
- **Observability is pluggable** via `EngineHooks`
  (`onError`, `onStepSuccess`, `logger`) — no hard tracer dependency.
- **`WhileStep` is the only legal cycle** (self-loop) and is bounded by
  `maxIterations` (default 1000).
- **`start()` is idempotent**; cron heads are subscribed once per workflow.
- **No back-pressure, by design**: the engine never rejects or sheds work. It
  warns (pg-boss `queue_backlog`) so operators can scale workers instead — see
  [Cluster](docs/cluster.md#back-pressure-is-out-of-scope-by-design).
- **Local times are timezone-aware and default to UTC**: `startAt: Date` is an
  absolute instant; `startAt: LocalDayTime` ("next Friday 8am") is resolved in
  `PublishOptions.timezone` via `Intl`, DST-correct. Unset timezone means UTC —
  never the host's clock.

## Testing

```sh
npm test                # unit tests (no infrastructure)
npm run lint            # eslint
npm run format          # prettier check

# Postgres-backed integration tests:
npm run docker:up       # starts postgres:16 on localhost:5433
PG_URL=postgres://pgworkflow:pgworkflow@localhost:5433/pgworkflow npm run test:integration
npm run docker:down
```

Integration tests are skipped automatically when `PG_URL` is not set.

> Note: pg-boss v12 ships ESM. It is an **optional peer dependency**, loaded
> lazily only by `PgBossQueue`, so the core stays dependency-free. Adapter
> consumers should `npm install pg-boss` alongside this package.

## Roadmap

1. **Retry/DLQ policy at workflow level**: `PublishOptions.retryLimit/retryDelay`
   are pass-through today; add `maxAttempts` + dead-letter hook + `onExhausted`
   so poison messages can't loop forever (esp. with `WhileStep`).
2. **Join/aggregate step**: `stream()` fans out but nothing joins; add
   `collect({ by, count, timeout })` for scatter-gather.
3. **Cron driver interface**: `Queue.schedule(cron)` stringly-types recurrence
   and `InMemoryQueue` only records it. Extract a `Scheduler` or accept
   `() => AsyncIterable` triggers; validate cron at `trigger()` time.
4. **Stronger exactly-once**: the ledger closes the duplicate-_delivery_ window,
   but a crash between a step's side effects and its ledger write can still
   re-run the body. Explore pg-boss `workOptions.transactional` to commit the
   side effects and the completion together.
5. **OpenTelemetry**: span wrapper in hooks (`withSpan(workflow>step, fn)`),
   propagate traceparent through the envelope.
6. **Graph tooling**: duplicate step names within one workflow are silently merged
   in the `steps` map today (`registerStep` early-returns). Either namespace or
   throw; also add an `engine.describe()` DAG printer.

Implemented: run state (`RunStore`), idempotency ledger, and versioned routing —
see [`docs/design/run-state.md`](docs/design/run-state.md).

## Releasing

Releases are cut from git tags and published to **GitHub first**; a registry
publish comes later.

1. Move `Unreleased` in [`CHANGELOG.md`](CHANGELOG.md) under a version heading
   with today's date.
2. `npm version <patch|minor|major>` — bumps `package.json` and creates the tag.
3. `git push --follow-tags`.
4. The [`Release`](.github/workflows/release.yml) workflow runs the full suite
   and attaches the packed tarball to a GitHub Release.

Pre-1.0, a minor bump may contain breaking changes; document them under
**Changed** in the changelog.
