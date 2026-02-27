// data.query — Query canonical data (parameterized SELECT only)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const dataQuery: ToolDef = {
  name: "data.query_v1",
  description: "Execute a parameterized SELECT query against QuestDB canonical data. No DDL/DML allowed.",
  inputSchema: {
    type: "object",
    properties: {
      sql: { type: "string", description: "SELECT query (parameterized with $1, $2, etc.)" },
      params: { type: "array", items: {}, description: "Query parameters (positional)" },
      limit: { type: "number", description: "Max rows to return (default 1000, max 10000)" },
    },
    required: ["sql"],
  },
  async handler(args) {
    const sql = args.sql as string;
    const params = (args.params as unknown[] | undefined) ?? [];
    const limit = Math.min((args.limit as number | undefined) ?? 1000, 10000);

    // Enforce SELECT-only at handler level as well
    const trimmed = sql.trim().toUpperCase();
    if (!trimmed.startsWith("SELECT")) {
      return err("Only SELECT queries are allowed");
    }

    // Append LIMIT if not already present
    const hasLimit = /\bLIMIT\b/i.test(sql);
    const finalSql = hasLimit ? sql : `${sql} LIMIT ${limit}`;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(finalSql, params);
      return ok({ columns: result.columns, rows: result.rows, row_count: result.rows.length });
    } catch (e) {
      return err(`Query failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
