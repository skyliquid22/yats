// experiment.compare — Side-by-side comparison of two experiments
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const experimentCompare: ToolDef = {
  name: "experiment.compare_v1",
  description:
    "Compare two experiments side-by-side: spec diffs and metric comparison (sharpe, calmar, etc.).",
  inputSchema: {
    type: "object",
    properties: {
      experiment_a: { type: "string", description: "First experiment ID" },
      experiment_b: { type: "string", description: "Second experiment ID" },
    },
    required: ["experiment_a", "experiment_b"],
  },
  async handler(args) {
    const idA = args.experiment_a as string;
    const idB = args.experiment_b as string;

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule(
        "research.experiments.cli",
        ["compare", idA, idB],
        60_000,
      );
      if (result.exitCode !== 0) {
        return err(`experiment.compare failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to compare experiments: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
