// data.ingest — Ingest historical data for ticker(s) from vendor
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

const VENDOR_JOBS: Record<string, string> = {
  alpaca: "ingest_alpaca",
  financialdatasets: "ingest_financialdatasets",
};

export const dataIngest: ToolDef = {
  name: "data.ingest_v1",
  description: "Ingest historical data for ticker(s) from a vendor into QuestDB raw tables.",
  inputSchema: {
    type: "object",
    properties: {
      tickers: { type: "array", items: { type: "string" }, description: "List of ticker symbols (e.g. ['AAPL', 'MSFT'])" },
      vendor: { type: "string", enum: ["alpaca", "financialdatasets"], description: "Data vendor to ingest from" },
      start_date: { type: "string", description: "Start date ISO-8601 (e.g. 2024-01-01)" },
      end_date: { type: "string", description: "End date ISO-8601 (e.g. 2024-12-31)" },
    },
    required: ["tickers", "vendor", "start_date", "end_date"],
  },
  async handler(args) {
    const tickers = args.tickers as string[];
    const vendor = args.vendor as string;
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;

    const jobName = VENDOR_JOBS[vendor];
    if (!jobName) {
      return err(`Unknown vendor: ${vendor}. Valid: ${Object.keys(VENDOR_JOBS).join(", ")}`);
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
      return ok({ run_id: runId, job: jobName, tickers, vendor, start_date: startDate, end_date: endDate });
    } catch (e) {
      return err(`Failed to launch ingest: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
