// registry.feature_sets — List registered feature sets
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const registryFeatureSets: ToolDef = {
  name: "registry.feature_sets_v1",
  description: "List registered feature sets, their versions, and contained features.",
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
        return err(`Feature set listing failed (exit ${result.exitCode}): ${result.stderr}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to list feature sets: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
