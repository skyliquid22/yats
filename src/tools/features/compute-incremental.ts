// features.compute_incremental — STUB (full impl in P2.8)
import { err, type ToolDef } from "../../types/tools.js";

export const featuresComputeIncremental: ToolDef = {
  name: "features.compute_incremental_v1",
  description: "Compute only new features since the high-water mark (incremental). Not yet implemented — use features.compute for full recompute.",
  inputSchema: {
    type: "object",
    properties: {
      universe: { type: "string", description: "Universe name" },
      feature_set: { type: "string", description: "Feature set name" },
    },
    required: ["universe", "feature_set"],
  },
  async handler() {
    return err("features.compute_incremental is not yet implemented. Full implementation arrives in P2.8. Use features.compute_v1 for full recompute.");
  },
};
