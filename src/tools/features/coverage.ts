// features.coverage — Coverage report (missing values, date range per symbol)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const featuresCoverage: ToolDef = {
  name: "features.coverage_v1",
  description: "Report feature coverage: missing values, date range, and row counts per symbol/feature set.",
  inputSchema: {
    type: "object",
    properties: {
      feature_set: { type: "string", description: "Filter by feature set (optional)" },
      symbols: { type: "array", items: { type: "string" }, description: "Filter to specific symbols (optional)" },
    },
    required: [],
  },
  async handler(args) {
    const featureSet = args.feature_set as string | undefined;
    const symbols = (args.symbols as string[] | undefined) ?? [];

    const conditions: string[] = [];
    if (featureSet) {
      conditions.push(`feature_set = '${featureSet.replace(/'/g, "''")}'`);
    }
    if (symbols.length > 0) {
      const symbolList = symbols.map((s) => `'${s.replace(/'/g, "''")}'`).join(",");
      conditions.push(`symbol IN (${symbolList})`);
    }

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";

    const sql = `SELECT symbol, feature_set, count() as row_count, min(timestamp) as first_date, max(timestamp) as last_date, datediff('d', min(timestamp), max(timestamp)) + 1 as calendar_days FROM features${where} GROUP BY symbol, feature_set ORDER BY symbol, feature_set`;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql);
      return ok({
        feature_set: featureSet ?? "all",
        symbols: symbols.length > 0 ? symbols : "all",
        coverage: result.rows,
      });
    } catch (e) {
      return err(`Coverage query failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
