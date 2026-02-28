// promote.to_production — Promote experiment to production tier (requires managing_partner approval)
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const promoteToProduction: ToolDef = {
  name: "promote.to_production_v1",
  description:
    "Promote an experiment to the production tier. Requires managing_partner approval flag. Must already be at candidate tier. Risk overrides cannot be promoted to production.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to promote" },
      promotion_reason: { type: "string", description: "Reason for production promotion" },
      promoted_by: { type: "string", description: "Who is promoting (user or agent ID)" },
      managing_partner_ack: {
        type: "boolean",
        description: "Managing partner acknowledgment required for production promotion (must be true)",
      },
    },
    required: ["experiment_id", "promotion_reason", "promoted_by", "managing_partner_ack"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const promotionReason = args.promotion_reason as string;
    const promotedBy = args.promoted_by as string;
    const managingPartnerAck = args.managing_partner_ack as boolean;

    if (!managingPartnerAck) {
      return err("Production promotion requires managing_partner_ack=true");
    }

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("promote", {
        ops: {
          promote: {
            config: {
              experiment_id: experimentId,
              target_tier: "production",
              promotion_reason: promotionReason,
              promoted_by: promotedBy,
              managing_partner_ack: true,
            },
          },
        },
      });
      return ok({ run_id: runId, experiment_id: experimentId, tier: "production", job: "promote" });
    } catch (e) {
      return err(`Failed to promote to production: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
