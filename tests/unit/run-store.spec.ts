import type { SqlExecutor } from "../../src/sql";
import { PostgresRunStore } from "../../src/workflow/run-store";

// Records every statement so a test can prove whether the store ran DDL.
class RecordingExecutor implements SqlExecutor {
  readonly queries: string[] = [];

  async query<T = Record<string, unknown>>(text: string): Promise<{ rows: T[] }> {
    this.queries.push(text);
    return { rows: [] };
  }
}

const isDdl = (statement: string): boolean =>
  /^\s*create (table|index) if not exists/i.test(statement);

describe("PostgresRunStore schema creation", () => {
  it("creates the four tables and indexes once, on first use", async () => {
    const db = new RecordingExecutor();
    const store = new PostgresRunStore({ db, schema: "bookette_jobs" });

    await store.load("missing");
    await store.load("missing");

    expect(db.queries.filter(isDdl)).toHaveLength(4);
    expect(db.queries.filter((statement) => statement.startsWith("select"))).toHaveLength(2);
  });

  it("qualifies the table but not the index name, which Postgres rejects", async () => {
    const db = new RecordingExecutor();
    const store = new PostgresRunStore({ db, schema: "bookette_jobs" });

    await store.load("missing");

    const indexes = db.queries.filter((statement) => statement.startsWith("create index"));
    expect(indexes).toHaveLength(2);
    for (const statement of indexes) {
      const name = statement.match(/create index if not exists (\S+)/)?.[1];
      expect(name).not.toContain(".");
      expect(statement).toContain('on "bookette_jobs".pg_workflow_run');
    }
  });

  it("runs no DDL when ensureSchema is false, for a least-privilege role", async () => {
    const db = new RecordingExecutor();
    const store = new PostgresRunStore({
      db,
      schema: "bookette_jobs",
      ensureSchema: false,
    });

    await expect(store.load("missing")).resolves.toBeNull();
    expect(db.queries.filter(isDdl)).toEqual([]);
    expect(db.queries).toHaveLength(1);
  });
});
