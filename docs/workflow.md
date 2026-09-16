# What is a workflow engine?

Written for developers who have never used one. If you already know the
concept, skip to [`architecture.md`](architecture.md).

## The problem

Imagine an order fulfilment process: charge the card, reserve stock, ship it,
email the customer. Written as one function, it looks fine in development:

```ts
async function fulfil(orderId) {
  const order = await loadOrder(orderId);
  await chargeCard(order);
  await reserveStock(order);
  const shipment = await ship(order); // ← the network blips here
  await emailCustomer(order, shipment);
}
```

Then production happens. The process dies mid-way. What now?

- **The work is lost.** `chargeCard` already ran, but the process is gone.
- **There is no retry.** Nobody calls `fulfil` again; nothing knows it stopped.
- **There is no record.** You cannot see which orders are half-done.
- **You cannot scale it.** Each process does its own waiting; you add servers and
  still have no idea who is working on what.

The root cause: the _progress_ of the process lives only in one process's
memory. If that memory disappears, so does the progress.

## The idea

A **workflow engine** moves the progress out of memory and into durable
storage, so a process can pause between steps and resume later — possibly on a
different machine, after a crash, or a deploy.

You describe the process as named **steps**. The engine runs each step as a
**job** on a **queue**. A job is just a row in a database. When a step finishes
and says what comes next, the engine writes the next job. When a step fails, the
job stays put and can be retried.

```
   your process                     the engine                        storage
   ─────────────                    ──────────                        ───────
   charge  ──┐                      enqueue "charge"  ──────────────►  [job]
             │                      run "charge"
   reserve ◄─┘                      enqueue "reserve" ──────────────►  [job]
             │                      run "reserve"
   ship    ◄─┘                      💥 crash
                                     (job stays "active", expires)
                                       ──────────────►  retried later ► [job]
   email   ◄───────────────────────────────────────────────────────────┘
```

Nothing is lost: every hop is a row. If the machine dies, the resume point is
the last row that was written.

## The vocabulary

| Term                | Meaning here                                               |
| ------------------- | ---------------------------------------------------------- |
| **Workflow**        | A named process, e.g. `checkout`.                          |
| **Step**            | One unit of work in it, e.g. `charge`.                     |
| **Job**             | A durable row that says "run step _X_ for workflow _Y_".   |
| **Queue**           | Where jobs wait. In production this is Postgres (pg-boss). |
| **Worker**          | A process that claims jobs and runs them.                  |
| **Context**         | Data that travels with the run (counters, ids, results).   |
| **Schedule / cron** | A rule that starts a workflow on a timer.                  |

## What you get

- **Durability** — a crash resumes at the last completed step, not the start.
- **Retries** — a failed step is re-run per a policy, with backoff.
- **Scale-out** — run many workers; the queue hands each job to exactly one.
- **Visibility** — every step is a row you can query and inspect.
- **Scheduling** — "every day at 03:00" or "next Friday at 08:00 local time".

## What you do _not_ get

This is deliberately a small engine, not a platform:

- No visual designer, no BPMN, no human-approval tasks.
- No distributed transactions. A step that charges a card and then crashes
  before recording it can be retried — so **make steps idempotent**.
- No join/aggregate primitive; `stream` fans work out, nothing collects it back.
- No automatic versioning of a running workflow when you deploy new code.

## A taste of the API

```ts
import { WorkflowEngine, InMemoryQueue, createWorkflow } from "@elderengineer/pg-workflow";

const engine = new WorkflowEngine(new InMemoryQueue());
await engine.start();

const checkout = createWorkflow<{ orderId: string }>("checkout")
  .from("charge", async (ctx, evt) => {
    ctx.state.order = await loadOrder(evt.message.orderId);
    return await chargeCard(ctx.state.order);
  })
  .to("reserve", async (ctx, evt) => reserveStock(ctx.state.order, evt.message))
  .to("ship", async (ctx) => ship(ctx.state.order))
  .build();

await engine.register(checkout);
await engine.trigger(checkout, { orderId: "ord_123" });
```

Each call runs as its own job. In tests the queue is in memory; in production
swap in `PgBossQueue` and nothing else changes.

## How this compares to other tools

| Option                     | Different because                                                                                      |
| -------------------------- | ------------------------------------------------------------------------------------------------------ |
| A plain function           | Loses progress on crash; no retries.                                                                   |
| A job queue (BullMQ, etc.) | Runs one job; you wire the steps and state yourself.                                                   |
| **This library**           | Adds step-to-step flow, context and scheduling on a job queue.                                         |
| Temporal / Restate         | Full durable-execution engines: replayable code, versioning, signals. More power, more infrastructure. |
| AWS Step Functions         | The same idea as a managed service, coupled to AWS.                                                    |

Reach for a heavier engine when you need replayable code, long-lived human
interactions, or complex compensation. Reach for this when you need durable
multi-step jobs over Postgres with minimal machinery.

## Where to go next

- [`examples.md`](examples.md) — copy-paste examples for every feature.
- [`architecture.md`](architecture.md) — how the engine works internally.
- [`context.md`](context.md) — moving data between steps, and its limits.
- [`cluster.md`](cluster.md) — running producers and workers in production.
- [`DEBUG.md`](DEBUG.md) — debugging a stuck or failed run.
