// experiment.get — Get full experiment details (spec + metrics + status)
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const experimentGet: ToolDef = {
  name: "experiment.get_v1",
  description:
    "Get full experiment details including spec, metrics, and artifact paths. Reads from QuestDB index and filesystem.",
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
        "research.experiments.cli",
        ["get", experimentId],
        30_000,
      );
      if (result.exitCode !== 0) {
        return err(`experiment.get failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to get experiment: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
