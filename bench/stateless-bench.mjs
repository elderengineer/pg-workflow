#!/usr/bin/env node
/**
 * Benchmark: DB write cost of two designs, at two state sizes.
 *
 *   A "today"  — the whole context (state) travels inside `job.data`, and is
 *                therefore rewritten on every pg-boss job lifecycle UPDATE.
 *   B "tier1"  — a tiny envelope travels in `job.data`; state lives in a single
 *                `bench_run` row that is loaded and saved once per step.
 *
 * Both designs run the same number of jobs with the same payload semantics;
 * only the state's location differs. Reports WAL bytes, tuple churn, table
 * growth and job payload size.
 *
 * Usage (needs a reachable Postgres and the `pg`/`pg-boss` dev deps):
 *
 *   docker compose up -d postgres
 *   PG_URL=postgres://pgworkflow:pgworkflow@localhost:5433/pgworkflow npm run bench
 *
 * Env:
 *   PG_URL       connection string used to reach the server (default: docker-compose URL)
 *   BENCH_DB     scratch database to create and drop (must contain "bench")
 *   BENCH_RUNS   workflow executions per measurement (default 20)
 *   BENCH_STEPS  steps per workflow (default 5)
 *   BENCH_SIZES  comma-separated state sizes in bytes (default 1024,51200)
 *
 * Results are sensitive to machine, Postgres version and checkpoint timing;
 * treat them as a relative comparison, not absolute figures.
 */
import { randomBytes } from "node:crypto";
import pg from "pg";
import { PgBoss } from "pg-boss";

const PG_URL = process.env.PG_URL ?? "postgres://pgworkflow:pgworkflow@localhost:5433/pgworkflow";
const BENCH_DB = process.env.BENCH_DB ?? "pgworkflow_bench";
const RUNS = Number(process.env.BENCH_RUNS ?? 20);
const STEPS = Number(process.env.BENCH_STEPS ?? 5);
const SIZES = (process.env.BENCH_SIZES ?? "1024,51200").split(",").map(Number);

if (!BENCH_DB.includes("bench")) {
  console.error(`refusing to drop a database that is not a scratch bench db: ${BENCH_DB}`);
  process.exit(2);
}

const { Client, Pool } = pg;
const url = (database) => {
  const u = new URL(PG_URL);
  u.pathname = `/${database}`;
  return u.toString();
};
const ADMIN_URL = url("postgres");
const DB_URL = url(BENCH_DB);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// base64 of random bytes is incompressible, so on-disk size matches logical size.
const blobOf = (bytes) =>
  randomBytes(Math.ceil(bytes * 0.75))
    .toString("base64")
    .slice(0, bytes);
const kb = (n) => `${(n / 1024).toFixed(1)} KB`;

async function recreateDatabase() {
  const admin = new Client({ connectionString: ADMIN_URL });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS ${BENCH_DB} WITH (FORCE)`);
  await admin.query(`CREATE DATABASE ${BENCH_DB}`);
  await admin.end();
}

async function snapshot(client) {
  const lsn = (await client.query("select pg_current_wal_lsn()::text as l")).rows[0].l;
  const stats = (
    await client.query(
      `select coalesce(sum(n_tup_ins),0) as ins, coalesce(sum(n_tup_upd),0) as upd,
              coalesce(sum(n_dead_tup),0) as dead
       from pg_stat_user_tables`,
    )
  ).rows[0];
  const size = (
    await client.query(
      `select coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint as b
       from pg_class c join pg_namespace n on n.oid = c.relnamespace
       where c.relkind in ('r','p') and n.nspname in ('pgboss','public')`,
    )
  ).rows[0].b;
  return { lsn, ins: +stats.ins, upd: +stats.upd, dead: +stats.dead, size: +size };
}

async function measure(boss, client, design, size) {
  const queue = `bench-${design}-${size}`;
  await boss.createQueue(queue);
  const tier1 = design === "tier1";
  const blob = blobOf(size);
  const pool = new Pool({ connectionString: DB_URL, max: 10 });

  let done = 0;
  let resolveDone;
  const allDone = new Promise((res) => (resolveDone = res));

  const handler = async (jobs) => {
    for (const job of jobs) {
      const msg = job.data.message;
      if (msg.step >= STEPS - 1) {
        done += 1;
        if (done === RUNS) resolveDone();
        continue;
      }
      const step = msg.step + 1;
      if (tier1) {
        const cur = (await pool.query("select state from bench_run where id = $1", [msg.runId]))
          .rows[0];
        await pool.query("update bench_run set state = $1, updated_at = now() where id = $2", [
          { ...cur.state, step },
          msg.runId,
        ]);
        await boss.send(queue, {
          createTs: Date.now(),
          message: { runId: msg.runId, workflow: "bench", step },
        });
      } else {
        await boss.send(queue, {
          createTs: Date.now(),
          message: {
            workflow: "bench",
            step,
            context: { state: { ...msg.context.state, step } },
            data: null,
          },
        });
      }
    }
  };

  await boss.work(queue, { pollingIntervalSeconds: 0.5, localConcurrency: 10 }, handler);

  const before = await snapshot(client);
  for (let r = 0; r < RUNS; r += 1) {
    const runId = `${design}-${size}-${r}`;
    if (tier1) {
      await client.query("insert into bench_run (id, state) values ($1, $2)", [
        runId,
        { step: 0, blob },
      ]);
      await boss.send(queue, {
        createTs: Date.now(),
        message: { runId, workflow: "bench", step: 0 },
      });
    } else {
      await boss.send(queue, {
        createTs: Date.now(),
        message: {
          workflow: "bench",
          step: 0,
          context: { state: { step: 0, blob } },
          data: null,
        },
      });
    }
  }

  await Promise.race([
    allDone,
    sleep(120000).then(() => {
      throw new Error(`timeout: ${design}/${size} (${done}/${RUNS})`);
    }),
  ]);
  await sleep(1500); // let the stats collector flush

  const after = await snapshot(client);
  const wal = +(
    await client.query("select pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint as b", [
      before.lsn,
    ])
  ).rows[0].b;
  const payload = (
    await client.query(
      `select coalesce(avg(pg_column_size(data)),0)::int as avg,
              coalesce(sum(pg_column_size(data)),0)::bigint as total
       from pgboss.job_common where name = $1`,
      [queue],
    )
  ).rows[0];
  await pool.end();

  return {
    design: tier1 ? "B tier1 (state in run row)" : "A today (state in job.data)",
    state: kb(size),
    "tup ins": after.ins - before.ins,
    "tup upd": after.upd - before.upd,
    "dead tup": after.dead - before.dead,
    "table growth": kb(after.size - before.size),
    wal: kb(wal),
    "avg job.data": kb(+payload.avg),
    "total job.data": kb(+payload.total),
  };
}

await recreateDatabase();
const client = new Client({ connectionString: DB_URL });
await client.connect();
await client.query(
  `create table bench_run (
     id text primary key,
     state jsonb not null,
     updated_at timestamptz not null default now()
   )`,
);

const boss = new PgBoss({
  connectionString: DB_URL,
  superviseIntervalSeconds: 3600,
  monitorIntervalSeconds: 3600,
});
await boss.start();

const results = [];
for (const size of SIZES) {
  for (const design of ["today", "tier1"]) {
    results.push(await measure(boss, client, design, size));
  }
}

console.log(`\nworkload: ${RUNS} runs x ${STEPS} steps = ${RUNS * STEPS} jobs per measurement\n`);
console.table(results);

await boss.stop();
await client.end();
process.exit(0);
