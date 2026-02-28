// promote.history — Full promotion history for an experiment from QuestDB + filesystem
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const promoteHistory: ToolDef = {
  name: "promote.history_v1",
  description:
    "Fetch the full promotion history for an experiment. Returns all promotion records with qualification reports and tier progression from QuestDB and filesystem.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to fetch promotion history for" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule(
        "research.promotion.cli",
        ["history", experimentId],
        30_000,
      );
      if (result.exitCode !== 0) {
        return err(`promote.history failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to fetch promotion history: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
