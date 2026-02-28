// risk.correlation — Strategy vs book correlation analysis
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskCorrelation: ToolDef = {
  name: "risk.correlation_v1",
  description:
    "Analyze correlation between a strategy/symbol and a benchmark. Returns overall correlation, beta, R-squared, rolling correlation, up/down market correlation, tracking error, and information ratio.",
  inputSchema: {
    type: "object",
    properties: {
      strategy_symbol: { type: "string", description: "Strategy or ticker symbol" },
      book_symbol: { type: "string", description: "Benchmark/book symbol (e.g. SPY)" },
      start_date: { type: "string", description: "Start date ISO-8601" },
      end_date: { type: "string", description: "End date ISO-8601" },
      rolling_window: { type: "number", description: "Rolling correlation window in days (default: 60)" },
    },
    required: ["strategy_symbol", "book_symbol", "start_date", "end_date"],
  },
  async handler(args) {
    const strategySymbol = args.strategy_symbol as string;
    const bookSymbol = args.book_symbol as string;
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;
    const rollingWindow = args.rolling_window as number | undefined;

    const pyArgs = [
      "--strategy-symbol", strategySymbol,
      "--book-symbol", bookSymbol,
      "--start-date", startDate,
      "--end-date", endDate,
    ];
    if (rollingWindow !== undefined) pyArgs.push("--rolling-window", String(rollingWindow));

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/risk/correlation.py", pyArgs, 300_000);
      if (result.exitCode !== 0) {
        return err(`Correlation analysis failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run correlation analysis: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
