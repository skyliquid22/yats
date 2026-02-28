// shadow.status — Check shadow run progress via Dagster GraphQL
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const shadowStatus: ToolDef = {
  name: "shadow.status_v1",
  description:
    "Check the status of a shadow execution run by polling Dagster for run progress.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Dagster run ID from shadow.run or shadow.run_sim" },
    },
    required: ["run_id"],
  },
  async handler(args) {
    const runId = args.run_id as string;

    const dagster = new DagsterClient();
    try {
      const status = await dagster.getRunStatus(runId);
      return ok({ run_id: runId, ...status });
    } catch (e) {
      return err(`Failed to get shadow run status: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
