// features.compute_incremental — Routes to Dagster incremental feature pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const featuresComputeIncremental: ToolDef = {
  name: "features.compute_incremental_v1",
  description: "Compute only new features since the high-water mark (incremental). Uses watermark-based tracking to avoid recomputing existing data. Cross-sectional features are recomputed universe-wide per date.",
  inputSchema: {
    type: "object",
    properties: {
      universe: { type: "string", description: "Universe name (e.g. 'sp500')" },
      feature_set: { type: "string", description: "Feature set name (e.g. 'core_v1')" },
    },
    required: ["universe", "feature_set"],
  },
  async handler(args) {
    const universe = args.universe as string;
    const featureSet = args.feature_set as string;

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("feature_pipeline_incremental", {
        ops: {
          feature_pipeline_incremental_op: {
            config: {
              universe,
              feature_set: featureSet,
            },
          },
        },
      });
      return ok({ run_id: runId, job: "feature_pipeline_incremental", universe, feature_set: featureSet });
    } catch (e) {
      return err(`Failed to launch incremental feature pipeline: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
