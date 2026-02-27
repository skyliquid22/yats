// stats.ic_analysis — Information coefficient and decay
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const statsIcAnalysis: ToolDef = {
  name: "stats.ic_analysis_v1",
  description: "Compute information coefficient (IC) and IC decay across horizons for a feature set.",
  inputSchema: {
    type: "object",
    properties: {
      universe: { type: "string", description: "Universe name" },
      feature_set: { type: "string", description: "Feature set name" },
      start_date: { type: "string", description: "Start date ISO-8601" },
      end_date: { type: "string", description: "End date ISO-8601" },
      horizons: { type: "array", items: { type: "number" }, description: "Forward return horizons in days (default: [1,5,21])" },
    },
    required: ["universe", "feature_set", "start_date", "end_date"],
  },
  async handler(args) {
    const universe = args.universe as string;
    const featureSet = args.feature_set as string;
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;
    const horizons = args.horizons as number[] | undefined;

    const pyArgs = [
      "--universe", universe,
      "--feature-set", featureSet,
      "--start-date", startDate,
      "--end-date", endDate,
    ];
    if (horizons) pyArgs.push("--horizons", horizons.join(","));

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule("research.features.ic_analysis", pyArgs, 120_000);
      if (result.exitCode !== 0) {
        return err(`IC analysis failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run IC analysis: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
