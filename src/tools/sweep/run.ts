// sweep.run — Run a sweep config (multiple experiments) via Dagster pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const sweepRun: ToolDef = {
  name: "sweep.run_v1",
  description:
    "Run a parameter sweep across multiple experiment configurations. Triggers the Dagster experiment_sweep pipeline and returns run_ids for each experiment.",
  inputSchema: {
    type: "object",
    properties: {
      base_spec: {
        type: "object",
        description: "Base experiment spec shared across all sweep variants",
      },
      sweep_params: {
        type: "object",
        description:
          "Parameters to sweep over. Keys are spec field paths, values are arrays of values. E.g. {\"policy\": [\"sma\", \"ppo\"], \"seed\": [42, 123]}",
      },
      sweep_mode: {
        type: "string",
        enum: ["grid", "random"],
        description: "Sweep mode: grid (all combinations) or random (sampled). Default: grid",
      },
      max_experiments: {
        type: "number",
        description: "Max experiments to run (for random mode or to cap grid). Default: 100",
      },
    },
    required: ["base_spec", "sweep_params"],
  },
  async handler(args) {
    const baseSpec = args.base_spec as Record<string, unknown>;
    const sweepParams = args.sweep_params as Record<string, unknown[]>;
    const sweepMode = (args.sweep_mode as string | undefined) ?? "grid";
    const maxExperiments = (args.max_experiments as number | undefined) ?? 100;

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("experiment_sweep", {
        ops: {
          run_sweep: {
            config: {
              base_spec: baseSpec,
              sweep_params: sweepParams,
              sweep_mode: sweepMode,
              max_experiments: maxExperiments,
            },
          },
        },
      });
      return ok({
        run_id: runId,
        job: "experiment_sweep",
        sweep_mode: sweepMode,
        max_experiments: maxExperiments,
        sweep_params: Object.keys(sweepParams),
      });
    } catch (e) {
      return err(`Failed to launch sweep: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
