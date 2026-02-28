// execution.nav — NAV and portfolio state from QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionNav: ToolDef = {
  name: "execution.nav_v1",
  description:
    "Get the current NAV (Net Asset Value) and portfolio snapshot for a trading run. Returns cash, positions value, total NAV, and daily P&L.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Trading run ID (paper or live)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
    },
    required: [],
  },
  async handler(args) {
    const runId = args.run_id as string | undefined;
    const experimentId = args.experiment_id as string | undefined;

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (runId) {
      conditions.push(`run_id = $${paramIdx++}`);
      params.push(runId);
    }
    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT timestamp, run_id, experiment_id, cash, positions_value,
                        total_nav, daily_pnl, cumulative_pnl, drawdown
                 FROM portfolio_nav
                 ${where}
                 ORDER BY timestamp DESC
                 LIMIT 1`;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      if (result.rows.length === 0) {
        return ok({ snapshot: null, message: "No NAV data found" });
      }
      return ok({ snapshot: result.rows[0] });
    } catch (e) {
      return err(`Failed to query NAV: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
