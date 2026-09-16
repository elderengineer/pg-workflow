import { randomUUID } from "node:crypto";
import type { DeliveryScope } from "../queue";
import type { SqlExecutor } from "../sql";

export type { SqlExecutor };

export type RunStatus = "running" | "waiting" | "completed" | "failed" | "cancelled";

/**
 * A workflow execution. This is the durable record of a run: state lives here,
 * not in the job message. See [`docs/design/run-state.md`](../../docs/design/run-state.md).
 */
export interface RunRecord {
  id: string;
  workflow: string;
  version: number;
  status: RunStatus;
  /** Small JSON bookkeeping only — identifiers and counters, not payloads. */
  state: Record<string, unknown>;
  currentStep?: string;
  /** Dispatch counter per step name, so repeated steps get distinct attempts. */
  attempts: Record<string, number>;
  lastError?: string;
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date | null;
}

export interface RunInit {
  workflow: string;
  version: number;
  state?: Record<string, unknown>;
}

/** An outbound hop recorded on the step ledger, so a redelivery can re-forward. */
export interface PlannedStep {
  step: string;
  attempt: number;
  message: unknown;
}

/**
 * The durable outcome of one step attempt.
 *
 * Recorded *before* the outgoing jobs are published, so a redelivered job
 * re-forwards the recorded hops instead of running the step body again.
 */
export interface StepRecord {
  step: string;
  attempt: number;
  next: PlannedStep[];
  completedAt: Date;
}

export interface RunStore {
  create(init: RunInit, scope?: DeliveryScope): Promise<RunRecord>;
  load(id: string, scope?: DeliveryScope): Promise<RunRecord | null>;
  /** Persist the mutable fields of a run (state, status, currentStep, attempts, lastError). */
  save(run: RunRecord, scope?: DeliveryScope): Promise<void>;
  getStep(
    runId: string,
    step: string,
    attempt: number,
    scope?: DeliveryScope,
  ): Promise<StepRecord | null>;
  /** Append-only and idempotent on `(runId, step, attempt)`. */
  recordStep(runId: string, record: StepRecord, scope?: DeliveryScope): Promise<void>;
  cancel(id: string, scope?: DeliveryScope): Promise<void>;
}

const clone = <T>(value: T): T => structuredClone(value);

/**
 * In-memory store for tests and local development. Values are cloned on the way
 * in and out, so it behaves like a store that serializes (a function in
 * `state` throws, as it would be silently dropped by pg-boss).
 */
export class InMemoryRunStore implements RunStore {
  private readonly runs = new Map<string, RunRecord>();
  private readonly ledger = new Map<string, Map<string, StepRecord>>();

  async create(init: RunInit): Promise<RunRecord> {
    const now = new Date();
    const run: RunRecord = {
      id: randomUUID(),
      workflow: init.workflow,
      version: init.version,
      status: "running",
      state: init.state ?? {},
      attempts: {},
      createdAt: now,
      updatedAt: now,
    };
    this.runs.set(run.id, clone(run));
    return clone(run);
  }

  async load(id: string): Promise<RunRecord | null> {
    const run = this.runs.get(id);
    return run ? clone(run) : null;
  }

  async save(run: RunRecord): Promise<void> {
    if (!this.runs.has(run.id)) throw new Error(`RunNotFound: ${run.id}`);
    this.runs.set(run.id, clone(run));
  }

  async getStep(runId: string, step: string, attempt: number): Promise<StepRecord | null> {
    const record = this.ledger.get(runId)?.get(`${step}@${attempt}`);
    return record ? clone(record) : null;
  }

  async recordStep(runId: string, record: StepRecord): Promise<void> {
    let forRun = this.ledger.get(runId);
    if (!forRun) {
      forRun = new Map();
      this.ledger.set(runId, forRun);
    }
    const key = `${record.step}@${record.attempt}`;
    if (!forRun.has(key)) forRun.set(key, clone(record));
  }

  async cancel(id: string): Promise<void> {
    const run = this.runs.get(id);
    if (!run) throw new Error(`RunNotFound: ${id}`);
    if (run.status !== "completed") {
      run.status = "cancelled";
      run.updatedAt = new Date();
    }
  }
}

export interface PostgresRunStoreOptions {
  db: SqlExecutor;
  /** Schema for the run tables. Default `public`. */
  schema?: string;
  /** Table name prefix. Default `pg_workflow`. */
  tablePrefix?: string;
  /**
   * Create the run tables on first use. Default `true`.
   *
   * Set `false` where a migration role has already created them and the
   * application role may not run DDL: `CREATE TABLE`/`CREATE INDEX` require
   * `CREATE` on the schema and, for an index, ownership of the table, so a
   * least-privilege application role cannot run `migrate()`. The caller then
   * owns ensuring the tables exist before the store is used.
   */
  ensureSchema?: boolean;
}

interface RunRow {
  id: string;
  workflow: string;
  version: number;
  status: RunStatus;
  state: Record<string, unknown>;
  current_step: string | null;
  attempts: Record<string, number>;
  last_error: string | null;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
}

/**
 * Postgres-backed run store. State is a single `jsonb` snapshot row (design "B")
 * rewritten once per hop — see `docs/design/run-state.md`.
 */
export class PostgresRunStore implements RunStore {
  private readonly db: SqlExecutor;
  private readonly runTable: string;
  private readonly stepTable: string;
  private readonly ensureSchema: boolean;
  private migrated: Promise<void> | undefined;

  constructor(options: PostgresRunStoreOptions) {
    this.db = options.db;
    const prefix = options.tablePrefix ?? "pg_workflow";
    const schema = options.schema ? `"${options.schema}".` : "";
    this.runTable = `${schema}${prefix}_run`;
    this.stepTable = `${schema}${prefix}_step`;
    this.ensureSchema = options.ensureSchema ?? true;
  }

  private migrate(): Promise<void> {
    if (!this.migrated) {
      this.migrated = this.ensureSchema ? this.createTables() : Promise.resolve();
    }
    return this.migrated;
  }

  private createTables(): Promise<void> {
    return (async () => {
      await this.db.query(
        `create table if not exists ${this.runTable} (
           id text primary key,
           workflow text not null,
           version integer not null,
           status text not null,
           state jsonb not null default '{}',
           current_step text,
           attempts jsonb not null default '{}',
           last_error text,
           created_at timestamptz not null default now(),
           updated_at timestamptz not null default now(),
           completed_at timestamptz
         )`,
      );
      await this.db.query(
        `create index if not exists ${this.runTable}_by_status
           on ${this.runTable} (workflow, status)`,
      );
      await this.db.query(
        `create index if not exists ${this.runTable}_prunable
           on ${this.runTable} (updated_at)
           where status in ('completed', 'failed', 'cancelled')`,
      );
      await this.db.query(
        `create table if not exists ${this.stepTable} (
           run_id text not null references ${this.runTable} (id) on delete cascade,
           step text not null,
           attempt integer not null,
           next jsonb not null,
           completed_at timestamptz not null default now(),
           primary key (run_id, step, attempt)
         )`,
      );
    })();
  }

  private toRecord(row: RunRow): RunRecord {
    return {
      id: row.id,
      workflow: row.workflow,
      version: row.version,
      status: row.status,
      state: row.state,
      currentStep: row.current_step ?? undefined,
      attempts: row.attempts,
      lastError: row.last_error ?? undefined,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at,
    };
  }

  /** Route writes through the delivery's transaction when there is one. */
  private dbFor(scope?: DeliveryScope): SqlExecutor {
    return scope?.executor ?? this.db;
  }

  async create(init: RunInit, scope?: DeliveryScope): Promise<RunRecord> {
    await this.migrate();
    const { rows } = await this.dbFor(scope).query<RunRow>(
      `insert into ${this.runTable} (id, workflow, version, status, state)
       values ($1, $2, $3, 'running', $4::jsonb)
       returning *`,
      [randomUUID(), init.workflow, init.version, JSON.stringify(init.state ?? {})],
    );
    return this.toRecord(rows[0]);
  }

  async load(id: string, scope?: DeliveryScope): Promise<RunRecord | null> {
    await this.migrate();
    const { rows } = await this.dbFor(scope).query<RunRow>(
      `select * from ${this.runTable} where id = $1`,
      [id],
    );
    return rows[0] ? this.toRecord(rows[0]) : null;
  }

  async save(run: RunRecord, scope?: DeliveryScope): Promise<void> {
    await this.migrate();
    await this.dbFor(scope).query(
      `update ${this.runTable}
          set state = $2::jsonb, status = $3, current_step = $4, attempts = $5::jsonb,
              last_error = $6, updated_at = now(), completed_at = $7
        where id = $1`,
      [
        run.id,
        JSON.stringify(run.state),
        run.status,
        run.currentStep ?? null,
        JSON.stringify(run.attempts),
        run.lastError ?? null,
        run.completedAt ?? null,
      ],
    );
  }

  async getStep(
    runId: string,
    step: string,
    attempt: number,
    scope?: DeliveryScope,
  ): Promise<StepRecord | null> {
    await this.migrate();
    const { rows } = await this.dbFor(scope).query<{ next: PlannedStep[]; completed_at: Date }>(
      `select next, completed_at from ${this.stepTable}
        where run_id = $1 and step = $2 and attempt = $3`,
      [runId, step, attempt],
    );
    const row = rows[0];
    return row ? { step, attempt, next: row.next, completedAt: row.completed_at } : null;
  }

  async recordStep(runId: string, record: StepRecord, scope?: DeliveryScope): Promise<void> {
    await this.migrate();
    await this.dbFor(scope).query(
      `insert into ${this.stepTable} (run_id, step, attempt, next)
       values ($1, $2, $3, $4::jsonb)
       on conflict (run_id, step, attempt) do nothing`,
      [runId, record.step, record.attempt, JSON.stringify(record.next)],
    );
  }

  async cancel(id: string, scope?: DeliveryScope): Promise<void> {
    await this.migrate();
    await this.dbFor(scope).query(
      `update ${this.runTable} set status = 'cancelled', updated_at = now()
        where id = $1 and status <> 'completed'`,
      [id],
    );
  }

  /**
   * Delete terminal runs (and their steps, by cascade) older than a cutoff.
   * Run this on a schedule — the store never prunes on its own.
   * See [`docs/cluster.md`](../cluster.md#retention).
   */
  async prune(options: { olderThanDays?: number } = {}): Promise<number> {
    await this.migrate();
    const days = options.olderThanDays ?? 30;
    const { rows } = await this.db.query<{ id: string }>(
      `delete from ${this.runTable}
        where status in ('completed', 'failed', 'cancelled')
          and updated_at < now() - ($1::int * interval '1 day')
        returning id`,
      [days],
    );
    return rows.length;
  }
}
