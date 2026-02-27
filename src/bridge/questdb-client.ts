// QuestDB client (PG wire for reads, ILP for writes)
import pg from "pg";

const SELECT_PATTERN = /^\s*SELECT\s/i;
const FORBIDDEN_PATTERN = /\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE)\b/i;

export class QuestDBClient {
  private pool: pg.Pool | null = null;

  constructor(
    private readonly pgHost: string = "localhost",
    private readonly pgPort: number = 8812,
    private readonly pgUser: string = "admin",
    private readonly pgPassword: string = "quest",
    private readonly pgDatabase: string = "qdb",
    private readonly ilpPort: number = 9009
  ) {}

  private getPool(): pg.Pool {
    if (!this.pool) {
      this.pool = new pg.Pool({
        host: this.pgHost,
        port: this.pgPort,
        user: this.pgUser,
        password: this.pgPassword,
        database: this.pgDatabase,
        max: 4,
        statement_timeout: 30_000,
      });
    }
    return this.pool;
  }

  async query(sql: string, params: unknown[] = []): Promise<{ columns: string[]; rows: Record<string, unknown>[] }> {
    if (!SELECT_PATTERN.test(sql)) {
      throw new Error("Only SELECT queries are allowed");
    }
    if (FORBIDDEN_PATTERN.test(sql)) {
      throw new Error("DDL/DML statements are not allowed in queries");
    }

    const pool = this.getPool();
    const result = await pool.query(sql, params);

    const columns = result.fields.map((f) => f.name);
    const rows = result.rows as Record<string, unknown>[];
    return { columns, rows };
  }

  async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
  }
}
