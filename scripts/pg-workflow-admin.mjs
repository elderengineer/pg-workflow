#!/usr/bin/env node
/**
 * pg-workflow admin CLI — inspect and repair durable jobs.
 *
 * Build first, then run:
 *
 *   npm run build
 *   PG_URL=postgres://user:pass@host:5432/db node scripts/pg-workflow-admin.mjs <command> [...]
 *
 * Commands:
 *   stats <queue>
 *   show <queue> <id>
 *   retry <queue> <id>
 *   reschedule <queue> <id> [--after <duration>] [--priority <n>]
 *   cancel <queue> <id>
 *   redrive <dead-letter-queue> [--source <queue>] [--limit <n>]
 *
 * <duration> is ISO-8601 ("PT10M", "P1DT2H") or a plain number of seconds.
 * See docs/DEBUG.md for how to decide which command to use.
 */
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { Pool } = require("pg");
const {
  PgBossQueue,
  parseIsoDuration,
  duration,
  TimeUnit,
  PostgresRunStore,
} = require("../dist/index.js");

const PG_URL = process.env.PG_URL ?? process.env.DATABASE_URL;

const USAGE = `pg-workflow admin CLI — inspect and repair durable jobs.

Usage: PG_URL=postgres://… node scripts/pg-workflow-admin.mjs <command> [...]

  stats <queue>
  show <queue> <id>
  retry <queue> <id>
  reschedule <queue> <id> [--after <duration>] [--priority <n>]
  cancel <queue> <id>
  redrive <dead-letter-queue> [--source <queue>] [--limit <n>]
  run <runId>
  prune [--days <n>]

<duration> is ISO-8601 ("PT10M", "P1DT2H") or a plain number of seconds.
See docs/DEBUG.md for how to decide which command to use.
`;

function usage(exitCode = 1) {
  console.error(USAGE);
  process.exit(exitCode);
}

function parseAfter(value) {
  if (!value) return duration(0, TimeUnit.MILLISECONDS);
  if (/^P/i.test(value)) return parseIsoDuration(value);
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) throw new Error(`invalid --after value: ${value}`);
  return duration(seconds, TimeUnit.SECONDS);
}

const [command, ...rest] = process.argv.slice(2);
const flags = {};
const positional = [];
for (let i = 0; i < rest.length; i += 1) {
  if (rest[i].startsWith("--")) {
    const next = rest[i + 1];
    flags[rest[i].slice(2)] = next === undefined || next.startsWith("--") ? true : rest[++i];
  } else {
    positional.push(rest[i]);
  }
}

if (command === undefined || command === "help" || command === "--help") {
  usage(0);
}

if (!PG_URL) {
  console.error("error: set PG_URL (or DATABASE_URL) to the Postgres connection string.");
  process.exit(2);
}

const queue = new PgBossQueue({
  connectionString: PG_URL,
  onWarning: (warning) => console.warn(`warning[${warning.type}]: ${warning.message}`),
});
const pool = new Pool({ connectionString: PG_URL, max: 2 });
const store = new PostgresRunStore({ db: pool });

await queue.start();
try {
  switch (command) {
    case "stats": {
      const [name] = positional;
      if (!name) usage();
      console.log(JSON.stringify(await queue.stats(name), null, 2));
      break;
    }
    case "show": {
      const [name, id] = positional;
      if (!name || !id) usage();
      const job = await queue.getJob(name, id);
      if (!job) {
        console.error(`not found: ${name}/${id}`);
        process.exitCode = 3;
        break;
      }
      console.log(JSON.stringify(job, null, 2));
      break;
    }
    case "retry": {
      const [name, id] = positional;
      if (!name || !id) usage();
      await queue.retryJob(name, id);
      console.log(`retried ${name}/${id}`);
      break;
    }
    case "reschedule": {
      const [name, id] = positional;
      if (!name || !id) usage();
      const options = { startAfter: parseAfter(flags.after) };
      if (flags.priority !== undefined) options.priority = Number(flags.priority);
      await queue.rescheduleJob(name, id, options);
      console.log(`rescheduled ${name}/${id} to run after ${flags.after ?? "0s"}`);
      break;
    }
    case "cancel": {
      const [name, id] = positional;
      if (!name || !id) usage();
      await queue.cancelJob(name, id);
      console.log(`cancelled ${name}/${id}`);
      break;
    }
    case "redrive": {
      const [deadLetter] = positional;
      if (!deadLetter) usage();
      const options = {};
      if (flags.source) options.sourceName = flags.source;
      if (flags.limit) options.limit = Number(flags.limit);
      const moved = await queue.redriveDeadLetter(deadLetter, options);
      console.log(`redrove ${moved} job(s) from ${deadLetter}`);
      break;
    }
    case "run": {
      const [runId] = positional;
      if (!runId) usage();
      const run = await store.load(runId);
      if (!run) {
        console.error(`not found: ${runId}`);
        process.exitCode = 3;
        break;
      }
      console.log(JSON.stringify(run, null, 2));
      break;
    }
    case "prune": {
      const days = flags.days !== undefined ? Number(flags.days) : 30;
      const removed = await store.prune({ olderThanDays: days });
      console.log(`pruned ${removed} terminal run(s) older than ${days} day(s)`);
      break;
    }
    default:
      usage();
  }
} finally {
  await queue.stop({ graceful: true });
  await pool.end();
}
