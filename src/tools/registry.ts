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
import {
  statsAdfTest,
  statsBootstrapSharpe,
  statsDeflatedSharpe,
  statsPbo,
  statsRegimeDetect,
  statsConditionalSharpe,
  statsIcAnalysis,
  statsScmLeakage,
} from "./stats/index.js";
import {
  registryUniverses,
  registryFeatureSets,
  registryRegimeDetectors,
  registryPolicies,
} from "./registry_tools/index.js";

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
  statsAdfTest,
  statsBootstrapSharpe,
  statsDeflatedSharpe,
  statsPbo,
  statsRegimeDetect,
  statsConditionalSharpe,
  statsIcAnalysis,
  statsScmLeakage,
  registryUniverses,
  registryFeatureSets,
  registryRegimeDetectors,
  registryPolicies,
];

// Indexed by tool name for fast lookup
export const tools: Map<string, ToolDef> = new Map(
  allTools.map((t) => [t.name, t])
);
