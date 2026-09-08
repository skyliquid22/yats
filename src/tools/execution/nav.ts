// execution.nav — NAV and portfolio state from QuestDB
// Schema reference: pipelines/yats_pipelines/utils/create_tables.py (PORTFOLIO_STATE)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionNav: ToolDef = {
  name: "execution.nav_v1",
  description:
    "Get the current NAV (Net Asset Value) and portfolio snapshot for a trading run. Returns NAV, cash, exposure, leverage, and daily P&L from portfolio_state.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Dagster run ID of the trading run (optional)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      mode: { type: "string", enum: ["paper", "live", "shadow"], description: "Filter by trading mode (optional)" },
    },
    required: [],
  },
  async handler(args) {
    const runId = args.run_id as string | undefined;
    const experimentId = args.experiment_id as string | undefined;
    const mode = args.mode as string | undefined;

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (runId) {
      conditions.push(`dagster_run_id = $${paramIdx++}`);
      params.push(runId);
    }
    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }
    if (mode) {
      conditions.push(`mode = $${paramIdx++}`);
      params.push(mode);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT timestamp, experiment_id, mode, nav, cash, gross_exposure,
                        net_exposure, leverage, num_positions, daily_pnl, peak_nav,
                        drawdown, dagster_run_id
                 FROM portfolio_state
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
