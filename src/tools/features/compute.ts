// features.compute — Compute features for universe + date range (full recompute)
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const featuresCompute: ToolDef = {
  name: "features.compute_v1",
  description: "Compute features for a universe and date range using the Dagster feature pipeline (full recompute).",
  inputSchema: {
    type: "object",
    properties: {
      universe: { type: "string", description: "Universe name (e.g. 'sp500')" },
      feature_set: { type: "string", description: "Feature set name (e.g. 'core_v1')" },
      start_date: { type: "string", description: "Start date ISO-8601 (e.g. 2024-01-01). Empty = all data." },
      end_date: { type: "string", description: "End date ISO-8601 (e.g. 2024-12-31). Empty = all data." },
    },
    required: ["universe", "feature_set"],
  },
  async handler(args) {
    const universe = args.universe as string;
    const featureSet = args.feature_set as string;
    const startDate = (args.start_date as string | undefined) ?? "";
    const endDate = (args.end_date as string | undefined) ?? "";

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("feature_pipeline", {
        ops: {
          feature_pipeline_op: {
            config: {
              universe,
              feature_set: featureSet,
              start_date: startDate,
              end_date: endDate,
            },
          },
        },
      });
      return ok({ run_id: runId, job: "feature_pipeline", universe, feature_set: featureSet, start_date: startDate, end_date: endDate });
    } catch (e) {
      return err(`Failed to launch feature pipeline: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
