# Passing data, and the limits of `Context`

A step receives two arguments:

```ts
(context: Context, event: QueueEvent<In>) => Out | Promise<Out>;
```

There are three places data can live, and they are not interchangeable.

| Channel                            | Scope           | Persisted                          | Use for                                          |
| ---------------------------------- | --------------- | ---------------------------------- | ------------------------------------------------ |
| **Return value** → `event.message` | exactly one hop | in the next queue message          | the value the next step consumes                 |
| **`context.state`**                | the whole run   | in the **run store** (one row/run) | cross-step bookkeeping, counters, ids            |
| **`context.options`**              | the whole run   | in every queue message             | `PublishOptions` from `trigger`, merged each hop |

## 1. Per-step data — "just to the next step"

The value a step returns becomes the next step's `event.message`:

```ts
createWorkflow<{ seed: true }>("demo")
  .from("a", (_ctx, evt) => {
    evt.message; // { seed: true }  ← trigger payload
    return { n: 42 }; // becomes the next step's message
  })
  .to("b", (_ctx, evt) => {
    evt.message; // { n: 42 }
    return 0; // terminal step: nothing after this
  })
  .build();
```

- It is **not accumulated**: `b` sees only `a`'s return, not the trigger payload.
  If you need earlier values, copy what you need into `context.state`.
- Returning `undefined` still forwards to the next step (with an undefined
  message). A step only ends the run when it has no `nextSteps`, which is the
  terminal step's normal case.
- On `stream()` / `batchedStream()` fan-out, each branch receives its own
  slice as `event.message` (an element, or a batch, respectively).
- Custom `Step` implementations can return an explicit `StepResult`
  (`{ next, message }`, or an array for fan-out) to choose the next step as well
  as the message. The builder's `StepFn` always forwards to the step you
  declared with `.to(...)`.

Because this value travels in the queue message, it is serialized onto the queue
and has the same JSON restrictions as `context.state` below.

## 2. `context.state` — the whole run

```ts
.from("fetch", async (ctx) => {
  const rows = await db.query(...);
  ctx.state.ids = rows.map((r) => r.id); // visible to every later step
  return rows;                           // visible only to the next step
})
```

`state` is the only channel that survives more than one hop. It lives in the
**run store** — a `RunRecord` row per run — not in the job message:

- it is **loaded** before the step runs and **saved** after;
- it is **queryable**: `store.load(runId)` and the `pg_workflow_run` table;
- it is **per-run**, so an operator can inspect or cancel a run.

## 3. `context.options`

The `PublishOptions` passed to `engine.trigger(...)`, carried in the envelope for
the run and merged into every hop (`{ ...context.options, ...step.options }`).
`Duration` values are rehydrated automatically after serialization.

# Restrictions and limitations

### `state` is stored as `jsonb` — keep it plain JSON

The Postgres run store persists `state` as `jsonb`, so what comes back is not
always what went in:

| Value in `state`              | After a round-trip             |
| ----------------------------- | ------------------------------ |
| function, symbol, `undefined` | dropped silently               |
| class instance                | plain object (prototype lost)  |
| `Date`                        | ISO string                     |
| `Map` / `Set`                 | `{}`                           |
| `NaN`, `Infinity`             | `null`                         |
| `BigInt`                      | throws                         |
| `Duration`                    | ISO-8601 string, e.g. `"PT5M"` |

**Functions cannot be stored.** This is the single most common mistake:

```ts
ctx.state.handler = () => doSomething(); // silently dropped by the Postgres store
```

Store a plain value (e.g. a task name or id) and rehydrate the behavior in the
step that needs it.

### Keep `state` small

Every hop writes the whole `state` value back to the run row, so a large `state`
is re-written once per step. The contract is **identifiers and counters, under
about 2 KB**; put blobs in object storage and keep only a reference.

This is measured, not assumed: at ~1 KB, keeping state in a run row roughly
halved WAL and cut table growth ~4.7× versus carrying it in `job.data`; at
~50 KB the two were within ~2%. Large state is expensive however you store it —
the size is the problem. See [`bench/`](../bench/README.md) and
[`design/run-state.md`](design/run-state.md).

### `InMemoryRunStore` is close, but not identical

The in-memory store clones values with `structuredClone`. That means it:

- **throws** on functions (loud, rather than silently dropping them) — good;
- but **preserves** `Date`, `Map` and `Set`, which the Postgres store does not.

So state that round-trips in a unit test can still change shape on Postgres.
Keep state plain to avoid the difference.

### Sibling fan-out branches do not merge

With `stream()` / `batchedStream()`, each branch loads the run and saves it
independently: concurrent branches are **last-write-wins**, and no branch sees
another's write. Run state is per-run bookkeeping — put per-branch results in
the per-hop message (the return value), not in `state`.

### Delivery is at-least-once — assume retries

The step ledger makes a **redelivered** job re-forward its recorded hops instead
of re-running the step body. Two consequences remain:

- A crash between a step's **external side effects** and its ledger write can
  still re-run the body. Steps with side effects should be idempotent, keyed on
  `context.instanceId` (the run id).
- A step's mutations are only visible to later steps once the run is saved;
  a failed attempt is retried from the stored state.

`context.instanceId` is the run id. Use it for correlation and idempotency keys.

### Closing the window: transactional steps

The window above — a crash between a step's side effects and its ledger write —
can be closed for **database** side effects by running the worker transactionally:

```ts
new PgBossQueue({
  connectionString,
  workOptions: { transactional: true }, // pg-boss runs the handler in one DB transaction
});
```

With that enabled, `context.transaction` is present: an executor bound to the
job's transaction. Writes made through it commit together with the run-state
save and the job completion, and roll back together if the step throws, so a
retried step does not repeat them.

```ts
.from("charge", async (ctx, evt) => {
  await ctx.transaction?.query("insert into ledger (run_id, amount) values ($1, $2)", [
    ctx.instanceId,
    evt.message.amount,
  ]);
  return await chargeCard(evt.message); // an external call is still at-least-once
})
```

This covers work on the **same database** that goes through
`context.transaction`. External effects (HTTP, email) remain at-least-once — no
transaction can cover them, so those steps must still be idempotent. Downstream
hops are published on a separate connection, so they are not part of the
transaction either.

### Fan-out does not join

`stream()` / `batchedStream()` produce independent branches with no merge step.
Nothing collects their results or reconciles their `state`. A `collect`/join
primitive is on the roadmap; until then, coordinate through an external store if
branches must combine results.

# Patterns

```ts
// Good: small, plain, namespaced
ctx.state.customerId = customer.id;
ctx.state.retryCount = (ctx.state.retryCount ?? 0) + 1;

// Bad: behavior, class instances, large blobs, secrets
ctx.state.fn = () => ...;
ctx.state.client = new DatabaseClient();     // prototype lost
ctx.state.rows = await fetchAllRows();       // re-written every hop
ctx.state.apiKey = process.env.API_KEY;      // stored in plaintext in the DB
```

# Related

- [`architecture.md`](architecture.md) — the execution model.
- [`design/run-state.md`](design/run-state.md) — the run store design and measurements.
- [`DEBUG.md`](DEBUG.md) — inspecting and repairing runs.
