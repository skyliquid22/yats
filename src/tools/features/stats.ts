// features.stats — Summary statistics for a feature
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const featuresStats: ToolDef = {
  name: "features.stats_v1",
  description: "Get summary statistics (avg, std, min, max, count) for a feature column from the features table.",
  inputSchema: {
    type: "object",
    properties: {
      feature: { type: "string", description: "Feature column name (e.g. 'ret_1d', 'rv_21d')" },
      feature_set: { type: "string", description: "Filter by feature set (optional)" },
      symbol: { type: "string", description: "Filter by symbol (optional)" },
    },
    required: ["feature"],
  },
  async handler(args) {
    const feature = args.feature as string;
    const featureSet = args.feature_set as string | undefined;
    const symbol = args.symbol as string | undefined;

    // Validate feature name (alphanumeric + underscore only to prevent injection)
    if (!/^[a-z0-9_]+$/.test(feature)) {
      return err("Invalid feature name. Must contain only lowercase letters, digits, and underscores.");
    }

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (featureSet) {
      conditions.push(`feature_set = $${paramIdx++}`);
      params.push(featureSet);
    }
    if (symbol) {
      conditions.push(`symbol = $${paramIdx++}`);
      params.push(symbol);
    }

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT avg(${feature}) as mean, stddev_samp(${feature}) as std, min(${feature}) as min, max(${feature}) as max, count(${feature}) as count FROM features${where}`;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      if (result.rows.length === 0) {
        return ok({ feature, message: "No data found" });
      }
      return ok({ feature, feature_set: featureSet ?? "all", symbol: symbol ?? "all", stats: result.rows[0] });
    } catch (e) {
      return err(`Stats query failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
