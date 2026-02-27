// data.ingest_universe — Bulk ingest for entire universe
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

const VENDOR_JOBS: Record<string, string> = {
  alpaca: "ingest_alpaca",
  financialdatasets: "ingest_financialdatasets",
};

export const dataIngestUniverse: ToolDef = {
  name: "data.ingest_universe_v1",
  description: "Bulk ingest historical data for an entire universe of tickers from a vendor.",
  inputSchema: {
    type: "object",
    properties: {
      universe: { type: "string", description: "Universe name (must match a file in configs/universes/)" },
      vendor: { type: "string", enum: ["alpaca", "financialdatasets"], description: "Data vendor" },
      start_date: { type: "string", description: "Start date ISO-8601" },
      end_date: { type: "string", description: "End date ISO-8601" },
    },
    required: ["universe", "vendor", "start_date", "end_date"],
  },
  async handler(args) {
    const universe = args.universe as string;
    const vendor = args.vendor as string;
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;

    // Load universe tickers from config
    const { readFileSync } = await import("fs");
    const { join } = await import("path");
    let tickers: string[];
    try {
      const raw = readFileSync(join(process.cwd(), "configs", "universes", `${universe}.json`), "utf-8");
      const parsed = JSON.parse(raw);
      tickers = Array.isArray(parsed) ? parsed : parsed.tickers ?? [];
    } catch {
      return err(`Universe not found or invalid: ${universe}. Check configs/universes/${universe}.json`);
    }

    if (tickers.length === 0) {
      return err(`Universe '${universe}' has no tickers`);
    }

    const jobName = VENDOR_JOBS[vendor];
    if (!jobName) {
      return err(`Unknown vendor: ${vendor}`);
    }

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun(jobName, {
        ops: {
          [`fetch_${vendor}_bars`]: {
            config: {
              ticker_list: tickers,
              start_date: startDate,
              end_date: endDate,
            },
          },
        },
      });
      return ok({ run_id: runId, job: jobName, universe, ticker_count: tickers.length, vendor, start_date: startDate, end_date: endDate });
    } catch (e) {
      return err(`Failed to launch universe ingest: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
