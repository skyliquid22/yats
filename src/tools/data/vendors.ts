// data.vendors — List available vendors and capabilities
import { loadYaml } from "../../config/loader.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

interface VendorInfo {
  name: string;
  base_url: string;
  data_url?: string;
  capabilities: string[];
  configured: boolean;
}

const VENDOR_CAPABILITIES: Record<string, string[]> = {
  alpaca: ["equity_ohlcv"],
  financialdatasets: ["fundamentals", "financial_metrics"],
};

export const dataVendors: ToolDef = {
  name: "data.vendors_v1",
  description: "List available data vendors, their capabilities, and whether they are configured.",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  async handler() {
    try {
      const config = loadYaml("vendors.yml");
      const vendors: VendorInfo[] = [];

      for (const [name, raw] of Object.entries(config)) {
        const cfg = raw as Record<string, string>;
        const envKey = cfg.env_key;
        vendors.push({
          name,
          base_url: cfg.base_url ?? "",
          data_url: cfg.data_url,
          capabilities: VENDOR_CAPABILITIES[name] ?? [],
          configured: envKey ? !!process.env[envKey] : false,
        });
      }

      return ok({ vendors });
    } catch (e) {
      return err(`Failed to load vendor config: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
