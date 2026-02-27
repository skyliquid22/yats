// experiment.create — Create experiment from spec (or base + overrides)
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const experimentCreate: ToolDef = {
  name: "experiment.create_v1",
  description:
    "Create an experiment from a JSON spec. Supports composable inheritance via 'extends' + 'overrides'. Returns the content-addressed experiment_id.",
  inputSchema: {
    type: "object",
    properties: {
      spec: {
        type: "object",
        description:
          "Experiment spec object. Required: experiment_name, symbols, start_date, end_date, feature_set, policy, cost_config. Optional: extends, overrides, policy_params, seed, evaluation_split, risk_config, execution_sim, regime_feature_set, regime_labeling, etc.",
      },
    },
    required: ["spec"],
  },
  async handler(args) {
    const spec = args.spec as Record<string, unknown>;
    const specJson = JSON.stringify(spec);

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule(
        "research.experiments.cli",
        ["create", "--spec-json", specJson],
        120_000,
      );
      if (result.exitCode !== 0) {
        return err(`experiment.create failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to create experiment: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
