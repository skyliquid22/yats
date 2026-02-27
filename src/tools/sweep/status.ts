// sweep.status — Check status of a sweep (per-experiment Dagster run status)
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const sweepStatus: ToolDef = {
  name: "sweep.status_v1",
  description:
    "Check the status of a sweep by querying Dagster run status. Returns per-experiment status map.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Sweep run_id returned by sweep.run" },
      experiment_run_ids: {
        type: "array",
        items: { type: "string" },
        description: "Individual experiment run_ids to check (if known)",
      },
    },
    required: ["run_id"],
  },
  async handler(args) {
    const sweepRunId = args.run_id as string;
    const experimentRunIds = args.experiment_run_ids as string[] | undefined;

    const dagster = new DagsterClient();
    try {
      // Get overall sweep status
      const sweepStatus = await dagster.getRunStatus(sweepRunId);

      const result: Record<string, unknown> = {
        sweep_run_id: sweepRunId,
        sweep_status: sweepStatus.status,
        sweep_end_time: sweepStatus.endTime,
      };

      // If individual run IDs are provided, check each
      if (experimentRunIds && experimentRunIds.length > 0) {
        const statuses: Record<string, unknown> = {};
        for (const runId of experimentRunIds) {
          try {
            const status = await dagster.getRunStatus(runId);
            statuses[runId] = { status: status.status, end_time: status.endTime };
          } catch {
            statuses[runId] = { status: "UNKNOWN", error: "Run not found" };
          }
        }
        result.experiment_statuses = statuses;
      }

      return ok(result);
    } catch (e) {
      return err(`Failed to get sweep status: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
