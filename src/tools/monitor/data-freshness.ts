// monitor.data_freshness — Last ingestion timestamps per vendor/domain from QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const monitorDataFreshness: ToolDef = {
  name: "monitor.data_freshness_v1",
  description:
    "Report data freshness by checking the most recent timestamp in each canonical table. Identifies stale data sources.",
  inputSchema: {
    type: "object",
    properties: {
      domain: {
        type: "string",
        enum: ["equity_ohlcv", "fundamentals", "financial_metrics", "all"],
        description: "Data domain to check (default: all)",
      },
    },
    required: [],
  },
  async handler(args) {
    const domain = (args.domain as string | undefined) ?? "all";

    const queries: Record<string, string> = {
      equity_ohlcv: `SELECT 'equity_ohlcv' as domain, max(timestamp) as latest, count() as total_rows FROM canonical_equity_ohlcv`,
      fundamentals: `SELECT 'fundamentals' as domain, max(report_date) as latest, count() as total_rows FROM canonical_fundamentals`,
      financial_metrics: `SELECT 'financial_metrics' as domain, max(timestamp) as latest, count() as total_rows FROM canonical_financial_metrics`,
    };

    const targets = domain === "all" ? Object.keys(queries) : [domain];

    const qdb = new QuestDBClient();
    try {
      const results = [];
      for (const d of targets) {
        const sql = queries[d];
        if (!sql) continue;
        try {
          const result = await qdb.query(sql);
          results.push(result.rows[0] ?? { domain: d, latest: null, total_rows: 0 });
        } catch {
          results.push({ domain: d, latest: null, total_rows: 0, error: "table not found or query failed" });
        }
      }
      return ok({ freshness: results });
    } catch (e) {
      return err(`Failed to check data freshness: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
