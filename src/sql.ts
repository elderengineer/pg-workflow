/**
 * Minimal SQL surface, kept structural so callers can pass anything that fits:
 * `pg.Pool` and `pg.Client` satisfy it directly, and a backend transaction can
 * be adapted to it.
 */
export interface SqlExecutor {
  query<T = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}
