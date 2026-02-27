// stats.adf_test — Augmented Dickey-Fuller stationarity test
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const statsAdfTest: ToolDef = {
  name: "stats.adf_test_v1",
  description: "Run Augmented Dickey-Fuller stationarity test on a time series.",
  inputSchema: {
    type: "object",
    properties: {
      symbol: { type: "string", description: "Ticker symbol" },
      column: { type: "string", description: "Column to test (default: close)", enum: ["open", "high", "low", "close", "volume"] },
      start_date: { type: "string", description: "Start date ISO-8601" },
      end_date: { type: "string", description: "End date ISO-8601" },
      max_lags: { type: "number", description: "Max lags for ADF test (default: auto)" },
    },
    required: ["symbol", "start_date", "end_date"],
  },
  async handler(args) {
    const symbol = args.symbol as string;
    const column = (args.column as string | undefined) ?? "close";
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;
    const maxLags = args.max_lags as number | undefined;

    const pyArgs = ["--symbol", symbol, "--column", column, "--start-date", startDate, "--end-date", endDate];
    if (maxLags !== undefined) pyArgs.push("--max-lags", String(maxLags));

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/stats/adf.py", pyArgs, 120_000);
      if (result.exitCode !== 0) {
        return err(`ADF test failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run ADF test: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
