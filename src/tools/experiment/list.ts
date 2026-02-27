// experiment.list — List experiments from QuestDB experiment_index
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const experimentList: ToolDef = {
  name: "experiment.list_v1",
  description:
    "List experiments with optional filters. Queries the QuestDB experiment_index table.",
  inputSchema: {
    type: "object",
    properties: {
      feature_set: { type: "string", description: "Filter by feature set name" },
      policy_type: { type: "string", description: "Filter by policy type (e.g. sma, ppo, sac)" },
      universe: { type: "string", description: "Filter by universe string" },
      order_by: { type: "string", enum: ["created_at", "sharpe", "calmar", "total_return"], description: "Sort column (default: created_at)" },
      limit: { type: "number", description: "Max rows to return (default 50, max 500)" },
    },
    required: [],
  },
  async handler(args) {
    const featureSet = args.feature_set as string | undefined;
    const policyType = args.policy_type as string | undefined;
    const universe = args.universe as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 50, 500);

    const VALID_ORDER = new Set(["created_at", "sharpe", "calmar", "total_return"]);
    const orderBy = VALID_ORDER.has(args.order_by as string) ? (args.order_by as string) : "created_at";

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (featureSet) {
      conditions.push(`feature_set = $${paramIdx++}`);
      params.push(featureSet);
    }
    if (policyType) {
      conditions.push(`policy_type = $${paramIdx++}`);
      params.push(policyType);
    }
    if (universe) {
      conditions.push(`universe = $${paramIdx++}`);
      params.push(universe);
    }

    let sql = "SELECT experiment_id, feature_set, policy_type, universe, sharpe, calmar, max_drawdown, total_return, annualized_return, qualification_status, promotion_tier, created_at FROM experiment_index";
    if (conditions.length > 0) {
      sql += " WHERE " + conditions.join(" AND ");
    }
    sql += ` ORDER BY ${orderBy} DESC LIMIT ${limit}`;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      return ok({ experiments: result.rows, row_count: result.rows.length });
    } catch (e) {
      return err(`Failed to list experiments: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
