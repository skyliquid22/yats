// promote.to_candidate — Promote experiment to candidate tier via Dagster pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const promoteToCandidate: ToolDef = {
  name: "promote.to_candidate_v1",
  description:
    "Promote an experiment to the candidate tier. Must already be at research tier. Creates an immutable promotion record.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to promote" },
      promotion_reason: { type: "string", description: "Reason for promotion" },
      promoted_by: { type: "string", description: "Who is promoting (user or agent ID)" },
    },
    required: ["experiment_id", "promotion_reason", "promoted_by"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const promotionReason = args.promotion_reason as string;
    const promotedBy = args.promoted_by as string;

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("promote", {
        ops: {
          promote: {
            config: {
              experiment_id: experimentId,
              target_tier: "candidate",
              promotion_reason: promotionReason,
              promoted_by: promotedBy,
            },
          },
        },
      });
      return ok({ run_id: runId, experiment_id: experimentId, tier: "candidate", job: "promote" });
    } catch (e) {
      return err(`Failed to promote to candidate: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
