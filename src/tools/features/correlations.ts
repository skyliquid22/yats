// features.correlations — Correlation matrix across features
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const featuresCorrelations: ToolDef = {
  name: "features.correlations_v1",
  description: "Compute correlation matrix across features for a given feature set and optional filters.",
  inputSchema: {
    type: "object",
    properties: {
      feature_set: { type: "string", description: "Feature set name (e.g. 'core_v1')" },
      features: { type: "array", items: { type: "string" }, description: "Specific features to correlate (optional, defaults to all in set)" },
      symbol: { type: "string", description: "Filter by symbol (optional)" },
    },
    required: ["feature_set"],
  },
  async handler(args) {
    const featureSet = args.feature_set as string;
    const features = (args.features as string[] | undefined) ?? [];
    const symbol = (args.symbol as string | undefined) ?? "";

    const cliArgs = ["--feature-set", featureSet];
    if (features.length > 0) {
      cliArgs.push("--features", features.join(","));
    }
    if (symbol) {
      cliArgs.push("--symbol", symbol);
    }

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule("research.features.correlations", cliArgs, 60_000);
      if (result.exitCode !== 0) {
        return err(`Correlation computation failed (exit ${result.exitCode}): ${result.stderr}`);
      }
      const parsed = JSON.parse(result.stdout);
      return ok(parsed);
    } catch (e) {
      return err(`Failed to compute correlations: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
