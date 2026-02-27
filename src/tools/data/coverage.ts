// data.coverage — Report data coverage gaps per symbol
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

const COVERAGE_QUERIES: Record<string, string> = {
  equity_ohlcv: `
    SELECT symbol,
           count() as bar_count,
           min(timestamp) as first_date,
           max(timestamp) as last_date,
           datediff('d', min(timestamp), max(timestamp)) + 1 as calendar_days,
           count() * 1.0 / (datediff('d', min(timestamp), max(timestamp)) + 1) as fill_ratio
    FROM canonical_equity_ohlcv
    GROUP BY symbol
    ORDER BY symbol
  `,
  fundamentals: `
    SELECT symbol,
           count() as record_count,
           min(report_date) as first_date,
           max(report_date) as last_date
    FROM canonical_fundamentals
    GROUP BY symbol
    ORDER BY symbol
  `,
  financial_metrics: `
    SELECT symbol,
           count() as record_count,
           min(timestamp) as first_date,
           max(timestamp) as last_date,
           datediff('d', min(timestamp), max(timestamp)) + 1 as calendar_days,
           count() * 1.0 / (datediff('d', min(timestamp), max(timestamp)) + 1) as fill_ratio
    FROM canonical_financial_metrics
    GROUP BY symbol
    ORDER BY symbol
  `,
};

export const dataCoverage: ToolDef = {
  name: "data.coverage_v1",
  description: "Report data coverage per symbol for a given domain, identifying gaps and fill ratios.",
  inputSchema: {
    type: "object",
    properties: {
      domain: {
        type: "string",
        enum: ["equity_ohlcv", "fundamentals", "financial_metrics"],
        description: "Data domain to check coverage for",
      },
      symbols: { type: "array", items: { type: "string" }, description: "Filter to specific symbols (optional)" },
    },
    required: ["domain"],
  },
  async handler(args) {
    const domain = args.domain as string;
    const symbols = (args.symbols as string[] | undefined) ?? [];

    const baseSql = COVERAGE_QUERIES[domain];
    if (!baseSql) {
      return err(`Unknown domain: ${domain}`);
    }

    // Add symbol filter if specified
    let sql = baseSql;
    if (symbols.length > 0) {
      const symbolList = symbols.map((s) => `'${s.replace(/'/g, "''")}'`).join(",");
      sql = sql.replace("GROUP BY", `WHERE symbol IN (${symbolList}) GROUP BY`);
    }

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql);
      return ok({ domain, symbols: symbols.length > 0 ? symbols : "all", coverage: result.rows });
    } catch (e) {
      return err(`Coverage query failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
