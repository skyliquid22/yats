// data.refresh — Incremental update (latest data since last ingest)
import { DagsterClient } from "../../bridge/dagster-client.js";
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

const VENDOR_JOBS: Record<string, string> = {
  alpaca: "ingest_alpaca",
  financialdatasets: "ingest_financialdatasets",
};

const WATERMARK_QUERIES: Record<string, string> = {
  alpaca: "SELECT max(timestamp) as last_ts FROM raw_alpaca_equity_ohlcv",
  financialdatasets: "SELECT max(timestamp) as last_ts FROM raw_fd_financial_metrics",
};

export const dataRefresh: ToolDef = {
  name: "data.refresh_v1",
  description: "Incremental update — fetch latest data since the last ingest high-water mark.",
  inputSchema: {
    type: "object",
    properties: {
      tickers: { type: "array", items: { type: "string" }, description: "Tickers to refresh (empty = all known)" },
      vendor: { type: "string", enum: ["alpaca", "financialdatasets"], description: "Data vendor" },
    },
    required: ["vendor"],
  },
  async handler(args) {
    const vendor = args.vendor as string;
    const tickers = (args.tickers as string[] | undefined) ?? [];

    const jobName = VENDOR_JOBS[vendor];
    if (!jobName) {
      return err(`Unknown vendor: ${vendor}`);
    }

    // Find high-water mark
    const watermarkSql = WATERMARK_QUERIES[vendor];
    if (!watermarkSql) {
      return err(`No watermark query for vendor: ${vendor}`);
    }

    const qdb = new QuestDBClient();
    let startDate: string;
    try {
      const result = await qdb.query(watermarkSql);
      const lastTs = result.rows[0]?.last_ts;
      if (lastTs) {
        const d = new Date(lastTs as string | number);
        d.setDate(d.getDate() + 1);
        startDate = d.toISOString().slice(0, 10);
      } else {
        startDate = "2020-01-01";
      }
    } catch {
      startDate = "2020-01-01";
    } finally {
      await qdb.close();
    }

    const endDate = new Date().toISOString().slice(0, 10);

    if (startDate > endDate) {
      return ok({ message: "Already up to date", vendor, last_date: startDate });
    }

    const dagster = new DagsterClient();
    try {
      const config: Record<string, unknown> = {
        ops: {
          [`fetch_${vendor}_bars`]: {
            config: {
              start_date: startDate,
              end_date: endDate,
              ...(tickers.length > 0 ? { ticker_list: tickers } : {}),
            },
          },
        },
      };

      const runId = await dagster.launchRun(jobName, config);
      return ok({ run_id: runId, job: jobName, vendor, start_date: startDate, end_date: endDate });
    } catch (e) {
      return err(`Failed to launch refresh: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
