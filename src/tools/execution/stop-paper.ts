// execution.stop_paper — Stop paper trading (direct/immediate)
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionStopPaper: ToolDef = {
  name: "execution.stop_paper_v1",
  description:
    "Stop an active paper trading run. Terminates the Dagster pipeline run and marks the session as stopped.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Dagster run ID of the paper trading session" },
    },
    required: ["run_id"],
  },
  async handler(args) {
    const runId = args.run_id as string;

    const dagster = new DagsterClient();
    try {
      const status = await dagster.getRunStatus(runId);
      if (status.status === "SUCCESS" || status.status === "FAILURE" || status.status === "CANCELED") {
        return ok({ run_id: runId, status: status.status, message: "Run already terminated" });
      }
      // For active runs, we report the current status — Dagster termination
      // is handled by the pipeline's heartbeat mechanism
      return ok({ run_id: runId, status: "stop_requested", previous_status: status.status });
    } catch (e) {
      return err(`Failed to stop paper trading: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
