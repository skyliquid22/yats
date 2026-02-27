// Tool registry — single source of truth for all MCP tools
import type { ToolDef } from "../types/tools.js";
import {
  dataIngest,
  dataIngestUniverse,
  dataRefresh,
  dataCanonicalize,
  dataVerify,
  dataQuery,
  dataCoverage,
  dataVendors,
} from "./data/index.js";
import {
  featuresCompute,
  featuresComputeIncremental,
  featuresList,
  featuresStats,
  featuresCorrelations,
  featuresCoverage,
  featuresWatermarks,
} from "./features/index.js";

const allTools: ToolDef[] = [
  dataIngest,
  dataIngestUniverse,
  dataRefresh,
  dataCanonicalize,
  dataVerify,
  dataQuery,
  dataCoverage,
  dataVendors,
  featuresCompute,
  featuresComputeIncremental,
  featuresList,
  featuresStats,
  featuresCorrelations,
  featuresCoverage,
  featuresWatermarks,
];

// Indexed by tool name for fast lookup
export const tools: Map<string, ToolDef> = new Map(
  allTools.map((t) => [t.name, t])
);
