# Design: durable run state (Tier 1)

**Status:** implemented — steps 1–3 landed (breaking, pre-1.0).

## Implemented

| Slice                                                         | State                                                                                                                                               |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RunStore` (in-memory + Postgres), envelope shrink, load/save | **done**                                                                                                                                            |
| Idempotency ledger                                            | **done** — the step's planned hops are recorded before publishing, so a redelivery re-forwards instead of re-running                                |
| Versioned routing                                             | **done** — `register(wf, { version })`; a run dispatches to its version                                                                             |
| Retention                                                     | **done as `store.prune()`**, not partitioning (see [cluster.md](../cluster.md#retention))                                                           |
| Stateless opt-out                                             | **dropped** — state always lives in the database; one code path                                                                                     |
| Transactional steps                                           | **done (opt-in)** — `workOptions: { transactional: true }`; run-state writes join the job transaction and `context.transaction` exposes it to steps |

Two implementation notes:

- The envelope is now `{ runId, workflow, version, step, attempt, data, options, timezone }`.
  `context.state` is still the user-facing API: the engine loads it from the run
  row before a step and saves it after.
- **jsonb parameters must be `JSON.stringify`-ed and cast `::jsonb`.** A JS array
  passed straight to node-postgres is serialized as a Postgres _array literal_,
  not JSON — which silently corrupted empty arrays to `{}` and rejected arrays of
  objects with `invalid input syntax for type json`.

## Motivation

Every limitation in [`architecture.md`](../architecture.md#limitations) traces
back to one fact: **a run has no record of its own.** The only state is the
`Context` embedded in each job message, so a run cannot be observed, joined,
cancelled, or deduplicated, and its state is rewritten into every hop.

Giving a run an identity fixes those together. This document proposes the
smallest version that does so.

## Measured cost

Full method and script: [`bench/stateless-bench.mjs`](../../bench/README.md).
Real pg-boss, fresh database, 20 runs × 5 steps = 100 jobs per leg, with
**incompressible** state so on-disk size matches logical size.

| State | Design                            | WAL          | Table growth | `job.data` total | tuple updates |
| ----- | --------------------------------- | ------------ | ------------ | ---------------- | ------------- |
| 1 KB  | A today (state in `job.data`)     | 389.5 KB     | 416 KB       | 118.1 KB         | 203           |
| 1 KB  | **B tier 1** (state in a run row) | **194.3 KB** | **88 KB**    | **12.3 KB**      | 271           |
| 50 KB | A today                           | 5744 KB      | 5320 KB      | 5017 KB          | 201           |
| 50 KB | **B tier 1**                      | 5640 KB      | 5312 KB      | **12.4 KB**      | 274           |

Reading:

- **Small state (< ~2 KB, stored inline): Tier 1 roughly halves WAL and cuts
  table growth ~4.7×.** Today the state is inline in `job.data`, so every job
  lifecycle `UPDATE` (`created → active → completed`) rewrites it.
- **Large state (TOASTed): a wash.** Postgres reuses unchanged TOAST chunks
  across a job's updates, so design A's duplication penalty disappears, while
  design B must rewrite the full value on the run row each hop.
- **Tier 1 costs ~2 extra statements per hop** (load + save) → +35% tuple updates.
- Large state is expensive in _both_ designs (~5.7 MB WAL for 100 jobs). State
  **size**, not state location, is the dominant term.

### What this measures, and what it does not

Design B bundles **two** changes — a tiny envelope and a run-row store — so the
table shows their _combined_ effect, not each in isolation.

- The tiny envelope is measured directly (`avg job.data`: **0.1 KB** for B vs
  1.2 KB / 50.2 KB for A). It is what removes the repeated rewrites of inline
  state inside pg-boss's job lifecycle.
- The run row is _where_ the state goes; it is not a separate saving.
- The 50 KB row is evidence that a tiny envelope **alone** does not help while
  the state must still be written once per hop.

Not covered by this benchmark:

- an arm with a tiny envelope and state **outside Postgres** (S3/Redis), which
  would isolate the envelope from the store;
- an **append-only/delta** state log, which would avoid rewriting a full
  snapshot at all — plausibly the cheapest design for a scarce database;
- **retention**, where tiny retained rows pay off most (completed jobs are
  ~10× smaller, so scans, vacuum and table size improve).

The figure shifts with machine, Postgres version and checkpoint timing; treat it
as relative.

## The contract: `state` stays small

Tier 1's database benefit is real only when `state` stays small. Therefore:

> `context.state` is for identifiers, counters and small bookkeeping
> (target: **< 2 KB**). Large payloads belong in external storage (S3, a blob
> column, etc.) with only a reference in `state`.

To make it enforceable rather than aspirational, the engine should call
`hooks.onWarning` when a serialized `state` exceeds a threshold (default 8 KB),
so the misuse is visible in logs and metrics.

## Scope (slice 1)

In scope:

- `RunRecord` + `RunStore` (Postgres and in-memory implementations).
- Envelope shrink: `job.data` carries `{ runId, workflow, version, step }`;
  state is loaded from the store.
- An append-only idempotency ledger, so a redelivered job does not re-execute a
  step that already completed.

Out of scope (later tiers): joins/`collect`, signals, sagas, OpenTelemetry,
workflow version _routing_ (the field is reserved here, enforced later).

## Data model

```sql
create table pg_workflow_run (
  id            uuid        primary key,
  workflow      text        not null,
  version       integer     not null default 1,
  status        text        not null,               -- running|waiting|completed|failed|cancelled
  state         jsonb       not null default '{}',
  current_step  text,
  attempts      jsonb       not null default '{}',  -- step -> count
  last_error    text,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  completed_at  timestamptz
);

create index pg_workflow_run_by_status on pg_workflow_run (workflow, status);
create index pg_workflow_run_active on pg_workflow_run (updated_at) where status = 'running';

-- append-only: one small insert per completed step, no updates
create table pg_workflow_step (
  run_id       uuid        not null references pg_workflow_run (id) on delete cascade,
  step         text        not null,
  attempt      integer     not null,
  idempotency  text        not null,
  output       jsonb,
  completed_at timestamptz not null default now(),
  primary key (run_id, step, attempt)
);
```

Retention mirrors pg-boss: delete terminal runs after a configurable number of
days, and partition by `created_at` once volume warrants it.

## Interfaces

```ts
interface RunRecord {
  id: string;
  workflow: string;
  version: number;
  status: "running" | "waiting" | "completed" | "failed" | "cancelled";
  state: Record<string, unknown>;
  currentStep?: string;
  attempts: Record<string, number>;
}

interface RunStore {
  create(record: Omit<RunRecord, "id">): Promise<RunRecord>;
  load(id: string): Promise<RunRecord | null>;
  /** Dirty-tracked: skip the write when nothing changed. */
  save(id: string, patch: Partial<RunRecord>): Promise<void>;
  /** Returns false when this step already completed (idempotency). */
  completeStep(runId: string, step: string, attempt: number, output: unknown): Promise<boolean>;
}
```

## Behaviour changes

- `trigger` creates a run and publishes `{ runId, workflow, version, step }`.
- `runStep`:
  1. load the run; if `status === "cancelled"` stop;
  2. if the step already completed → skip the side effects and forward;
  3. run the step;
  4. `save` the run **only if `state` changed**;
  5. record the step in the ledger;
  6. publish the next envelope with the same `runId`.
- `cancel(runId)` marks the run cancelled; steps observe it at dispatch.
- `instanceId` becomes the run id.

## What stays the same

- **No back-pressure**, by design — see [`cluster.md`](../cluster.md#back-pressure-is-out-of-scope-by-design).
- UTC defaults and timezone-aware `LocalDayTime`.
- Single shared queue and one job per hop.
- `Queue` port: the store is a separate abstraction, so custom backends stay
  possible.

## Breaking changes and migration

- Persistence becomes required in production. `InMemoryQueue` pairs with a
  memory `RunStore`; `PgBossQueue` pairs with the Postgres `RunStore`.
- The wire (`job.data`) shape changes. In-flight jobs from an older version must
  drain before deploying, or run versioned side by side.
- `instanceId` semantics change from "first job id" to "run id".
- A stateless opt-out is worth keeping for trivial 1–2 step workflows, which do
  not benefit and would pay the extra round-trips.

## Open questions

1. **Idempotency default:** always on, or opt-in per step (for steps whose side
   effects are already idempotent and where the ledger write is unwanted)?
2. **Snapshots vs deltas:** measurements show the run row is rewritten whole
   either way, so deltas save network/CPU only. Start with whole snapshots.
3. **Transactional steps:** pg-boss v12 supports
   `workOptions.transactional`, committing a handler's writes with the job's
   completion. If the store shares that transaction, state and completion become
   atomic and the ledger may be unnecessary.
4. **Retention default** for terminal runs.
5. **Isolate the envelope from the store:** add a benchmark arm with a tiny
   envelope and state outside Postgres, and one with an append-only/delta state
   log, to see how much of B's win is the envelope versus the run row.

## Phased plan

1. `RunStore` (memory + Postgres), envelope shrink, load/save, stateless opt-out.
2. Idempotency ledger.
3. Versioned workflow routing.
4. Retention + partitioning for `pg_workflow_run`.
