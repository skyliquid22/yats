// promote.list — List all promotions from QuestDB promotions table
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const promoteList: ToolDef = {
  name: "promote.list_v1",
  description:
    "List all promotion records from the QuestDB promotions table. Optionally filter by tier or experiment.",
  inputSchema: {
    type: "object",
    properties: {
      tier: {
        type: "string",
        enum: ["research", "candidate", "production"],
        description: "Filter by promotion tier (optional)",
      },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      limit: { type: "number", description: "Max number of records to return (default: 50, max: 1000)", minimum: 1, maximum: 1000 },
    },
    required: [],
  },
  async handler(args) {
    const tier = args.tier as string | undefined;
    const experimentId = args.experiment_id as string | undefined;
    const limit = Math.min(Math.max(1, (args.limit as number | undefined) ?? 50), 1000);

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (tier) {
      conditions.push(`tier = $${paramIdx++}`);
      params.push(tier);
    }
    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT promoted_at, experiment_id, tier, promoted_by, qualification_passed,
                        sharpe, max_drawdown, dagster_run_id
                 FROM promotions
                 ${where}
                 ORDER BY promoted_at DESC
                 LIMIT $${paramIdx}`;
    params.push(limit);

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      return ok({ promotions: result.rows, count: result.rows.length });
    } catch (e) {
      return err(`Failed to list promotions: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
