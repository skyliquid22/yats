// eval.run — Evaluate an experiment (deterministic) via Dagster pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const evalRun: ToolDef = {
  name: "eval.run_v1",
  description:
    "Run evaluation on an experiment by triggering the Dagster eval pipeline. Produces metrics.json and timeseries artifacts.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to evaluate" },
      force: { type: "boolean", description: "Force re-evaluation even if metrics exist (default: false)" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const force = (args.force as boolean | undefined) ?? false;

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("evaluate_experiment", {
        ops: {
          evaluate: {
            config: {
              experiment_id: experimentId,
              force,
            },
          },
        },
      });
      return ok({ run_id: runId, experiment_id: experimentId, job: "evaluate_experiment" });
    } catch (e) {
      return err(`Failed to launch evaluation: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
