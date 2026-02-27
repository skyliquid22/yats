// features.watermarks — High-water marks per symbol/feature set
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const featuresWatermarks: ToolDef = {
  name: "features.watermarks_v1",
  description: "Show high-water marks (latest computed timestamp) per symbol and feature set.",
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

    const sql = `SELECT symbol, feature_set, feature_set_version, max(timestamp) as latest_data_ts, max(computed_at) as latest_computed_at, count() as row_count FROM features${where} GROUP BY symbol, feature_set, feature_set_version ORDER BY symbol, feature_set`;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql);
      return ok({
        feature_set: featureSet ?? "all",
        symbols: symbols.length > 0 ? symbols : "all",
        watermarks: result.rows,
      });
    } catch (e) {
      return err(`Watermarks query failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
