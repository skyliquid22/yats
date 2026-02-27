// experiment.run — Run experiment (train + evaluate) via Dagster pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const experimentRun: ToolDef = {
  name: "experiment.run_v1",
  description:
    "Run an experiment (train + evaluate) by triggering the Dagster experiment_run pipeline. Returns a run_id for tracking.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID (SHA256 hex) to run" },
      mode: { type: "string", enum: ["backtest", "paper"], description: "Run mode (default: backtest)" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const mode = (args.mode as string | undefined) ?? "backtest";

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("experiment_run", {
        ops: {
          run_experiment: {
            config: {
              experiment_id: experimentId,
              mode,
            },
          },
        },
      });
      return ok({ run_id: runId, experiment_id: experimentId, mode, job: "experiment_run" });
    } catch (e) {
      return err(`Failed to launch experiment run: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
