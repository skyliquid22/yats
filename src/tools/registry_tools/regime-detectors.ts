// registry.regime_detectors — List registered regime detectors
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const registryRegimeDetectors: ToolDef = {
  name: "registry.regime_detectors_v1",
  description: "List registered regime detectors and their versions.",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  async handler() {
    const runner = new PythonRunner();
    try {
      const result = await runner.runModule("research.features.list_regime_detectors", [], 30_000);
      if (result.exitCode !== 0) {
        return err(`Regime detector listing failed (exit ${result.exitCode}): ${result.stderr}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to list regime detectors: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
