// registry.universes — List available universe configurations
import { readdirSync, readFileSync } from "fs";
import { join } from "path";
import { ok, err, type ToolDef } from "../../types/tools.js";

interface UniverseInfo {
  name: string;
  description: string;
  ticker_count: number;
}

export const registryUniverses: ToolDef = {
  name: "registry.universes_v1",
  description: "List available universe configurations and their ticker counts.",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  async handler() {
    try {
      const dir = join(process.cwd(), "configs", "universes");
      const files = readdirSync(dir).filter((f) => f.endsWith(".yml"));
      const universes: UniverseInfo[] = [];

      for (const file of files) {
        const content = readFileSync(join(dir, file), "utf-8");
        const lines = content.split("\n");

        let name = file.replace(".yml", "");
        let description = "";
        let tickerCount = 0;

        for (const line of lines) {
          const nameMatch = line.match(/^name:\s*(.+)$/);
          if (nameMatch) name = nameMatch[1].trim();

          const descMatch = line.match(/^description:\s*(.+)$/);
          if (descMatch) description = descMatch[1].trim();

          if (line.match(/^\s+-\s+\S+/)) tickerCount++;
        }

        universes.push({ name, description, ticker_count: tickerCount });
      }

      return ok({ universes });
    } catch (e) {
      return err(`Failed to list universes: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
