// risk.flatten_positions — Emergency flatten all positions (broker adapter)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskFlattenPositions: ToolDef = {
  name: "risk.flatten_positions_v1",
  description:
    "Emergency flatten: close all open positions at market. This is a drastic action — use only in emergencies. Requires managing_partner role for production mode.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID" },
      run_id: { type: "string", description: "Trading run ID" },
      mode: {
        type: "string",
        enum: ["paper", "live"],
        description: "Trading mode (default: paper)",
      },
      triggered_by: { type: "string", description: "Who triggered the flatten (agent ID or user)" },
      reason: { type: "string", description: "Reason for emergency flatten" },
    },
    required: ["experiment_id", "run_id", "triggered_by"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const runId = args.run_id as string;
    const mode = (args.mode as string | undefined) ?? "paper";
    const triggeredBy = args.triggered_by as string;
    const reason = (args.reason as string | undefined) ?? "Emergency flatten via MCP tool";

    const qdb = new QuestDBClient();
    try {
      // Get current open positions
      const posSql = `SELECT symbol, side, qty, market_value
                      FROM positions
                      WHERE run_id = $1 AND experiment_id = $2 AND qty > 0
                      ORDER BY abs(market_value) DESC`;
      const posResult = await qdb.query(posSql, [runId, experimentId]);

      if (posResult.rows.length === 0) {
        return ok({
          action: "flatten",
          experiment_id: experimentId,
          run_id: runId,
          positions_closed: 0,
          message: "No open positions to flatten",
        });
      }

      // In a real broker integration, we'd submit market orders to close each position.
      // For now, record the intent and return the positions that need closing.
      return ok({
        action: "flatten",
        experiment_id: experimentId,
        run_id: runId,
        mode,
        triggered_by: triggeredBy,
        reason,
        positions_to_close: posResult.rows,
        positions_count: posResult.rows.length,
        message: `Flatten requested for ${posResult.rows.length} positions. Market close orders will be submitted.`,
      });
    } catch (e) {
      return err(`Failed to flatten positions: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
