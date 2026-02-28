// execution.positions — Current positions from QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionPositions: ToolDef = {
  name: "execution.positions_v1",
  description:
    "List current positions for a trading run. Returns symbol, side, quantity, entry price, current value, and P&L.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Trading run ID (paper or live)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      limit: { type: "number", description: "Max positions to return (default: 100, max: 500)", minimum: 1, maximum: 500 },
    },
    required: [],
  },
  async handler(args) {
    const runId = args.run_id as string | undefined;
    const experimentId = args.experiment_id as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 100, 500);

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
    const sql = `SELECT timestamp, run_id, experiment_id, symbol, side, qty,
                        entry_price, current_price, unrealized_pnl, market_value
                 FROM positions
                 ${where}
                 ORDER BY timestamp DESC
                 LIMIT $${paramIdx}`;
    params.push(limit);

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      return ok({ positions: result.rows, count: result.rows.length });
    } catch (e) {
      return err(`Failed to query positions: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
