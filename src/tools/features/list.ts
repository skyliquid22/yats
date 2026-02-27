// features.list — List registered feature sets and versions
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const featuresList: ToolDef = {
  name: "features.list_v1",
  description: "List registered feature sets, their versions, and the features they contain.",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  async handler() {
    const runner = new PythonRunner();
    try {
      const result = await runner.runModule("research.features.list_feature_sets", [], 30_000);
      if (result.exitCode !== 0) {
        return err(`Feature list failed (exit ${result.exitCode}): ${result.stderr}`);
      }
      const parsed = JSON.parse(result.stdout);
      return ok(parsed);
    } catch (e) {
      return err(`Failed to list feature sets: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
