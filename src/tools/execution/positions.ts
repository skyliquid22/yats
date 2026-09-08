// execution.positions — Current positions from QuestDB
// Schema reference: pipelines/yats_pipelines/utils/create_tables.py (POSITIONS)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionPositions: ToolDef = {
  name: "execution.positions_v1",
  description:
    "List current positions for a trading run. Returns symbol, quantity, average entry price, notional, and realized/unrealized P&L.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      mode: { type: "string", enum: ["paper", "live", "shadow"], description: "Filter by trading mode (optional)" },
      symbol: { type: "string", description: "Filter by symbol (optional)" },
      limit: { type: "number", description: "Max positions to return (default: 100, max: 500)", minimum: 1, maximum: 500 },
    },
    required: [],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string | undefined;
    const mode = args.mode as string | undefined;
    const symbol = args.symbol as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 100, 500);

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }
    if (mode) {
      conditions.push(`mode = $${paramIdx++}`);
      params.push(mode);
    }
    if (symbol) {
      conditions.push(`symbol = $${paramIdx++}`);
      params.push(symbol);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT timestamp, experiment_id, mode, symbol, quantity,
                        avg_entry_price, notional, unrealized_pnl, realized_pnl
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
