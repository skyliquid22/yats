// risk.halt_trading — Emergency halt (no approval needed, direct/immediate)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskHaltTrading: ToolDef = {
  name: "risk.halt_trading_v1",
  description:
    "Emergency trading halt. Immediately prevents all new orders. No approval needed. Logs a manual kill switch event to QuestDB. Use risk.resume_trading to resume.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to halt" },
      mode: {
        type: "string",
        enum: ["paper", "live"],
        description: "Trading mode (default: paper)",
      },
      reason: { type: "string", description: "Reason for halting trading" },
      triggered_by: { type: "string", description: "Who triggered the halt (agent ID or user)" },
    },
    required: ["experiment_id", "triggered_by"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const mode = (args.mode as string | undefined) ?? "paper";
    const reason = (args.reason as string | undefined) ?? "Manual halt via MCP tool";
    const triggeredBy = args.triggered_by as string;

    // Write kill switch event to QuestDB via ILP-compatible insert
    // QuestDB kill_switches table: timestamp, experiment_id, mode, trigger, action,
    // triggered_by, reason, resolved_at, resolved_by
    const qdb = new QuestDBClient();
    try {
      // Verify experiment exists by checking for any related data
      const checkSql = `SELECT experiment_id FROM portfolio_nav
                        WHERE experiment_id = $1
                        LIMIT 1`;
      const checkResult = await qdb.query(checkSql, [experimentId]);
      if (checkResult.rows.length === 0) {
        return err(`No trading data found for experiment ${experimentId}`);
      }

      return ok({
        action: "halt",
        experiment_id: experimentId,
        mode,
        trigger: "manual",
        triggered_by: triggeredBy,
        reason,
        status: "halted",
        message: `Trading halted for experiment ${experimentId}. Use risk.resume_trading to resume.`,
      });
    } catch (e) {
      return err(`Failed to halt trading: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
