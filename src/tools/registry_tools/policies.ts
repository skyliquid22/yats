// registry.policies — List available policy types
import { ok, type ToolDef } from "../../types/tools.js";

const POLICY_TYPES = [
  {
    name: "sma_weight",
    description: "Simple moving average crossover weight policy",
    parameters: ["fast_window", "slow_window", "max_weight"],
  },
  {
    name: "equal_weight",
    description: "Equal weight across all symbols in universe",
    parameters: ["rebalance_frequency"],
  },
];

export const registryPolicies: ToolDef = {
  name: "registry.policies_v1",
  description: "List available policy types and their configurable parameters.",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  async handler() {
    return ok({ policies: POLICY_TYPES });
  },
};
