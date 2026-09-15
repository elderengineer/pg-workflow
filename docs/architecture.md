# Architecture

## Goals

- Express multi-step, long-running processes as small composable steps.
- Survive process restarts and crashes (durability comes from the queue).
- Stay queue-agnostic: the engine knows only the `Queue` port, so the same
  workflow runs in-memory in tests and on Postgres in production.
- Have no mandatory runtime dependencies for the core.

**Non-goals:** a distributed transaction coordinator, a saga/compensation
framework, or a general DAG engine with joins. See
[Limitations](#limitations).

## Layers

```
                 ┌─────────────────────────────────────────┐
  workflow/      │ WorkflowBuilder  ·  Step implementations │
                 │ WorkflowEngine   ·  Context / StepData   │
                 └───────────────────┬─────────────────────┘
                                     │ depends on
                 ┌───────────────────▼─────────────────────┐
  queue/         │ Queue (port)                            │
                 │  ├── InMemoryQueue   (no infra)         │
                 │  └── PgBossQueue     (Postgres, lazy)   │
                 └───────────────────┬─────────────────────┘
                                     │ uses
                 ┌───────────────────▼─────────────────────┐
  primitives     │ duration.ts (DurationInput)             │
                 │ local-time.ts (LocalDayTime, UTC-aware) │
                 └─────────────────────────────────────────┘
```

Dependencies point downward only. `queue/` never imports `workflow/`, so a
backend can be swapped without touching the engine.

## Core abstractions

| Type             | File                          | Role                                                                                        |
| ---------------- | ----------------------------- | ------------------------------------------------------------------------------------------- |
| `Queue`          | `queue/index.ts`              | The port: `publish` / `subscribe` / `schedule` / `unschedule` / `cancel` / `start` / `stop` |
| `QueueEvent<T>`  | `queue/index.ts`              | A delivered message: `{ queue, id, message, createTs }`                                     |
| `Step<In, Out>`  | `workflow/types.ts`           | A node: `run(context, event) => StepResult<Out>`                                            |
| `StepResult<T>`  | `workflow/types.ts`           | `{ next?, message? }`, or an array for fan-out                                              |
| `Workflow<In>`   | `workflow/types.ts`           | A name plus a head `Step`                                                                   |
| `Context`        | `workflow/types.ts`           | What a step sees: `state`, `options`, `instanceId`, `timezone`                              |
| `RunStore`       | `workflow/run-store.ts`       | Durable run state + idempotency ledger (`InMemory` and `Postgres`)                          |
| `RunRecord`      | `workflow/run-store.ts`       | One run: `{ id, workflow, version, status, state, attempts, … }`                            |
| `Engine`         | `workflow/types.ts`           | `register` / `start` / `trigger` / `cancel` / `cancelRun` / `unschedule`                    |
| `WorkflowEngine` | `workflow/workflow-engine.ts` | The engine implementation                                                                   |
| `EngineHooks`    | `workflow/types.ts`           | `onError` / `onStepSuccess` / `logger`                                                      |

## Execution model

One shared queue (`WorkflowEngine.QUEUE_NAME`, default `pg-workflow-item`) holds
every step of every workflow. Each message is a small routing envelope; the run's
**state lives in the `RunStore`**, not in the message:

```jsonc
{
  "runId": "9f2c…",
  "workflow": "checkout",
  "version": 1,
  "step": "charge",
  "attempt": 1,
  "data": { "amount": 100 },
}
```

`start()` subscribes once to that queue and dispatches by
`(workflow, version, step)`. A step that returns a `next` step publishes a new
message — **one job per hop**:

```
trigger(wf, data)
      │  create run; publish {runId, step:"charge"}
      ▼
┌─────────────────────┐   record step; publish {step:"notify"}   ┌───────────────┐
│ charge              │ ───────────────────────────────────────► │ notify        │
│ load/save run.state │                                          │ (no next→end) │
└─────────────────────┘                                          └───────────────┘
        one job                                                       one job
```

Consequences:

- A step's only output channel to the engine is its returned `StepResult`.
- Execution is asynchronous and unordered between runs; the queue decides what
  runs when and where.
- Durability is per step: a crash resumes at the last message that was published,
  and the run row records how far it got.

### What flows between steps

Three channels, described in detail in [`context.md`](context.md):

- **Return value → `event.message`** — one hop only, not accumulated.
- **`context.state`** — the whole run, loaded from and saved to the run store.
- **`context.options`** — `PublishOptions` merged into every hop.

## Step kinds

| Builder call    | Step                | Behaviour                                                                  |
| --------------- | ------------------- | -------------------------------------------------------------------------- |
| `from` / `to`   | `SimpleStep`        | Runs the function; forwards its return value to the next step              |
| `if` / `fromIf` | `IfStep`            | Forwards unchanged when the condition is true; **ends the run** when false |
| `while`         | `WhileStep`         | Re-queues itself while true; bounded by `maxIterations` (default 1000)     |
| `stream`        | `StreamStep`        | Fans an array out into one message per element                             |
| `batchedStream` | `BatchedStreamStep` | Fans an array out into arrays of `batchSize`                               |

`WhileStep` keeps its counter in `context.state["__while_<name>"]`, so the bound
is enforced across process boundaries; exceeding it throws. `StreamStep` spreads
concurrent fan-out by cloning the next step and multiplying its `startAfter` by
the element index, so a delayed fan-out is staggered rather than simultaneous.

## Registration and graph validation

`register(workflow, { version })` (version defaults to `1`):

1. Rejects a duplicate `(name, version)` pair.
2. `validateGraph` walks the step graph and rejects cycles, with one exception:
   a `WhileStep` self-loop (a step whose sole next step is itself) is legal.
   Diamond joins (two paths reaching the same step) are allowed.
3. Indexes every reachable step as `steps[name][version][stepName]` for O(1)
   dispatch. A run dispatches to the version it was triggered with, so a rolling
   deploy can run old and new graphs side by side.

`start()` is idempotent and must be called after all workflows are registered —
messages for an unknown `(workflow, version)` or step raise `WorkflowNotFound` /
`WorkflowVersionNotFound` / `StepNotFound` rather than being silently dropped.

> **Limitation:** steps are keyed by name within a workflow version. Two distinct
> steps with the same name are merged by `registerStep`, so step names must be
> unique per workflow.

## Failure model

- If a step throws — or anything _after_ it fails (recording, saving, publishing)
  — `runStep` calls `hooks.onError`, logs, and **rethrows**; the error is never a
  silent retry.
- The backend then decides what happens: `PgBossQueue` lets pg-boss retry per
  the queue's `retry_limit` / `retry_delay`, then mark the job failed.
  `InMemoryQueue` has no retry — it reports the error via its `onError`.
- The step's outcome is recorded in the idempotency ledger **before** the next
  hop is published. A redelivered job therefore re-forwards the recorded hops
  instead of running the step body again.
- Delivery is still **at-least-once**: a crash between a step's external side
  effects and its ledger write can re-run the body, so side-effecting steps
  should be idempotent, keyed on `context.instanceId` (the run id).
- With `workOptions: { transactional: true }`, the step runs inside the job's
  database transaction and `context.transaction` exposes it, so **database**
  side effects commit or roll back with the run state. External effects and the
  publish of downstream hops are outside it, so they remain at-least-once.

See [`pg-boss.md`](pg-boss.md) for the queue's durability and [`design/run-state.md`](design/run-state.md)
for the run store.

## Scheduling

`trigger(workflow, data, { cron })` subscribes the workflow head on a queue
named after the workflow and writes a scheduler entry (a `schedule` row on
pg-boss); each firing creates a new run and dispatches into the shared step
queue normally. `unschedule(workflow)` removes the entry; `cancel(id)` cancels
one enqueued job and `cancelRun(runId)` stops a whole run. Timezones default to
UTC. Recurrence is also available directly on the port via
`queue.schedule(queue, cron, data, options)`.

## Extensibility

| To change         | Implement / provide                                                                       |
| ----------------- | ----------------------------------------------------------------------------------------- |
| Queue backend     | Implement `Queue` (see the skeleton in [`examples.md`](examples.md))                      |
| Run state         | Implement `RunStore` (`InMemoryRunStore`, `PostgresRunStore`)                             |
| Tracing / metrics | `EngineHooks.onStepSuccess` / `onError` / `logger`                                        |
| Step semantics    | Implement `Step<In, Out>` and add it via a builder method or the graph directly           |
| Time handling     | Pass any `DurationInput`; `Date` for instants, `LocalDayTime` + `timezone` for wall-clock |

## Design decisions and trade-offs

- **One shared queue instead of one queue per step.** A per-step-queue design
  scales poorly (N steps ⇒ N queues/workers) and hides the overall backlog. The
  cost is that dispatch requires the envelope, and a step name is a key within
  its workflow version.
- **State in a run-store row, not in the message.** Keeps messages tiny and makes
  runs inspectable, cancellable and deduplicable. Measured to roughly halve WAL
  at ≤2 KB of state versus carrying state in `job.data`; a wash for large state —
  hence the "keep state small" contract.
- **No hard dependency on pg-boss or a tracer.** pg-boss is ESM-only and is
  therefore loaded lazily inside `PgBossQueue` (an optional peer dependency);
  the core, `InMemoryQueue` and `InMemoryRunStore` need nothing.
- **UTC by default.** Instants are absolute; cron and `LocalDayTime` default to
  UTC and are resolved with `Intl`, so behaviour does not depend on the host's
  clock or locale.

## Limitations

- No join/aggregate primitive: `stream` fans out, nothing collects results.
- Sibling fan-out branches are last-write-wins on `state`; do not rely on state
  written by another branch.
- Duplicate step names within a workflow version are silently merged.
- The idempotency ledger does not close the window between a step's side effects
  and its ledger write — see the roadmap.
- `InMemoryQueue` approximates production in one respect: it does not retry.
  (`InMemoryRunStore` _does_ clone values, so it matches the store's
  serialization behaviour.)
