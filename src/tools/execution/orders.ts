// execution.orders — Order history from QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionOrders: ToolDef = {
  name: "execution.orders_v1",
  description:
    "List order history for a trading run. Returns order details including symbol, side, quantity, fill price, and status.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Dagster run ID of the trading run (optional)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      symbol: { type: "string", description: "Filter by symbol (optional)" },
      status: {
        type: "string",
        enum: ["filled", "partial", "cancelled", "rejected", "pending"],
        description: "Filter by order status (optional)",
      },
      limit: { type: "number", description: "Max orders to return (default: 100, max: 1000)", minimum: 1, maximum: 1000 },
    },
    required: [],
  },
  async handler(args) {
    const runId = args.run_id as string | undefined;
    const experimentId = args.experiment_id as string | undefined;
    const symbol = args.symbol as string | undefined;
    const status = args.status as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 100, 1000);

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
    if (symbol) {
      conditions.push(`symbol = $${paramIdx++}`);
      params.push(symbol);
    }
    if (status) {
      conditions.push(`status = $${paramIdx++}`);
      params.push(status);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    // Schema reference: create_tables.py (ORDERS)
    const sql = `SELECT timestamp, order_id, experiment_id, mode, symbol, side,
                        quantity, order_type, status, fill_price, fill_quantity,
                        slippage_bps, fees, risk_check_result, broker_order_id, dagster_run_id
                 FROM orders
                 ${where}
                 ORDER BY timestamp DESC
                 LIMIT $${paramIdx}`;
    params.push(limit);

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      return ok({ orders: result.rows, count: result.rows.length });
    } catch (e) {
      return err(`Failed to query orders: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
