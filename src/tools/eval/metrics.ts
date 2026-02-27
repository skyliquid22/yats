// eval.metrics — Fetch evaluation metrics for an experiment
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const evalMetrics: ToolDef = {
  name: "eval.metrics_v1",
  description:
    "Fetch evaluation metrics for an experiment. Returns the full metrics.json content (performance, trading, safety, regime breakdown).",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID (SHA256 hex)" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule(
        "research.eval.cli",
        ["metrics", experimentId],
        30_000,
      );
      if (result.exitCode !== 0) {
        return err(`eval.metrics failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to fetch metrics: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
