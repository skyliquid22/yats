// execution.promote_live — Promote paper trading to live (managing_partner gated)
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionPromoteLive: ToolDef = {
  name: "execution.promote_live_v1",
  description:
    "Promote a paper trading experiment to live trading. Requires managing_partner acknowledgment. Launches the live trading pipeline via Dagster.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to promote to live" },
      run_id: { type: "string", description: "Paper trading run ID that validates readiness" },
      promoted_by: { type: "string", description: "Who is promoting (user or agent ID)" },
      managing_partner_ack: {
        type: "boolean",
        description: "Managing partner acknowledgment required for live promotion (must be true)",
      },
      quanttown_molecule_id: {
        type: "string",
        description: "QuantTown molecule ID if invoked from a molecule (for audit trail linkage)",
      },
      quanttown_bead_id: {
        type: "string",
        description: "QuantTown bead ID if invoked from a molecule step (for audit trail linkage)",
      },
    },
    required: ["experiment_id", "run_id", "promoted_by", "managing_partner_ack"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const runId = args.run_id as string;
    const promotedBy = args.promoted_by as string;
    const managingPartnerAck = args.managing_partner_ack as boolean;
    const quanttownMoleculeId = (args.quanttown_molecule_id as string | undefined) ?? "";
    const quanttownBeadId = (args.quanttown_bead_id as string | undefined) ?? "";

    if (!managingPartnerAck) {
      return err("Live trading promotion requires managing_partner_ack=true");
    }

    const dagster = new DagsterClient();
    try {
      const liveRunId = await dagster.launchRun("live_trading_setup", {
        ops: {
          live_trading: {
            config: {
              experiment_id: experimentId,
              paper_run_id: runId,
              promoted_by: promotedBy,
              managing_partner_ack: true,
            },
          },
        },
      });

      const result: Record<string, unknown> = {
        run_id: liveRunId,
        experiment_id: experimentId,
        promoted_from: runId,
        mode: "live",
        job: "live_trading_setup",
      };

      if (quanttownMoleculeId) {
        result.quanttown_molecule_id = quanttownMoleculeId;
        result.quanttown_bead_id = quanttownBeadId;
      }

      return ok(result);
    } catch (e) {
      return err(`Failed to promote to live: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
