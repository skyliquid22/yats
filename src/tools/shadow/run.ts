// shadow.run — Run shadow execution for experiment via Dagster pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const shadowRun: ToolDef = {
  name: "shadow.run_v1",
  description:
    "Run shadow execution for an experiment (execution_mode=none). Replays market data through the policy with direct rebalance. Only promoted or allowlisted experiments can run shadow.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to shadow execute" },
      start_date: { type: "string", description: "Start date (YYYY-MM-DD, optional)" },
      end_date: { type: "string", description: "End date (YYYY-MM-DD, optional)" },
      initial_value: { type: "number", description: "Initial portfolio value (default: 1_000_000)" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const startDate = args.start_date as string | undefined;
    const endDate = args.end_date as string | undefined;
    const initialValue = (args.initial_value as number | undefined) ?? 1_000_000;

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("shadow_run", {
        ops: {
          shadow_run: {
            config: {
              experiment_id: experimentId,
              execution_mode: "none",
              ...(startDate && { start_date: startDate }),
              ...(endDate && { end_date: endDate }),
              initial_value: initialValue,
            },
          },
        },
      });
      return ok({ run_id: runId, experiment_id: experimentId, execution_mode: "none", job: "shadow_run" });
    } catch (e) {
      return err(`Failed to launch shadow run: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
