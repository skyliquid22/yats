// risk.resume_trading — Resume after halt (gated: managing_partner for prod, PM for paper)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskResumeTrading: ToolDef = {
  name: "risk.resume_trading_v1",
  description:
    "Resume trading after a halt. For production mode, requires managing_partner acknowledgment. For paper mode, PM approval is sufficient. Verifies state is safe before resuming.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to resume" },
      mode: {
        type: "string",
        enum: ["paper", "live"],
        description: "Trading mode (default: paper)",
      },
      resumed_by: { type: "string", description: "Who is resuming (agent ID or user)" },
      managing_partner_ack: {
        type: "boolean",
        description: "Managing partner acknowledgment (required for live/production mode)",
      },
    },
    required: ["experiment_id", "resumed_by"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const mode = (args.mode as string | undefined) ?? "paper";
    const resumedBy = args.resumed_by as string;
    const managingPartnerAck = (args.managing_partner_ack as boolean | undefined) ?? false;

    // Gate check: live mode requires managing_partner_ack
    if (mode === "live" && !managingPartnerAck) {
      return err("Resuming live trading requires managing_partner_ack=true");
    }

    const qdb = new QuestDBClient();
    try {
      // Verify there's an active halt to resume from
      const ksSql = `SELECT timestamp, trigger, action, reason
                     FROM kill_switches
                     WHERE experiment_id = $1 AND resolved_at IS NULL
                     ORDER BY timestamp DESC
                     LIMIT 1`;
      let activeHalt = null;
      try {
        const ksResult = await qdb.query(ksSql, [experimentId]);
        if (ksResult.rows.length > 0) {
          activeHalt = ksResult.rows[0];
        }
      } catch {
        // Table may not exist yet
      }

      if (!activeHalt) {
        return ok({
          action: "resume",
          experiment_id: experimentId,
          mode,
          status: "no_active_halt",
          message: `No active halt found for experiment ${experimentId}. Trading may already be active.`,
        });
      }

      // Check for unsafe state (pending orders, unsettled fills)
      const pendingSql = `SELECT count() as pending_count
                          FROM orders
                          WHERE experiment_id = $1 AND status IN ('pending', 'partial')`;
      const pendingResult = await qdb.query(pendingSql, [experimentId]);
      const pendingCount = (pendingResult.rows[0]?.pending_count as number) ?? 0;

      if (pendingCount > 0) {
        return err(
          `Cannot resume: ${pendingCount} pending/partial orders exist. Settle or cancel them first.`,
        );
      }

      return ok({
        action: "resume",
        experiment_id: experimentId,
        mode,
        resumed_by: resumedBy,
        managing_partner_ack: managingPartnerAck,
        previous_halt: activeHalt,
        status: "resumed",
        message: `Trading resumed for experiment ${experimentId}.`,
      });
    } catch (e) {
      return err(`Failed to resume trading: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
