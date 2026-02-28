// risk.tail_analysis — CVaR, worst drawdowns, bootstrap CIs
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskTailAnalysis: ToolDef = {
  name: "risk.tail_analysis_v1",
  description:
    "Tail risk analysis: VaR, CVaR (Expected Shortfall), worst drawdown periods, bootstrap confidence intervals, skewness, and kurtosis.",
  inputSchema: {
    type: "object",
    properties: {
      symbol: { type: "string", description: "Ticker symbol" },
      start_date: { type: "string", description: "Start date ISO-8601" },
      end_date: { type: "string", description: "End date ISO-8601" },
      alpha: { type: "number", description: "VaR/CVaR tail probability (default: 0.05 = 5th percentile)" },
      top_drawdowns: { type: "number", description: "Number of worst drawdown periods to return (default: 5)" },
      seed: { type: "number", description: "Random seed for reproducibility" },
    },
    required: ["symbol", "start_date", "end_date"],
  },
  async handler(args) {
    const symbol = args.symbol as string;
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;
    const alpha = args.alpha as number | undefined;
    const topDrawdowns = args.top_drawdowns as number | undefined;
    const seed = args.seed as number | undefined;

    const pyArgs = ["--symbol", symbol, "--start-date", startDate, "--end-date", endDate];
    if (alpha !== undefined) pyArgs.push("--alpha", String(alpha));
    if (topDrawdowns !== undefined) pyArgs.push("--top-drawdowns", String(topDrawdowns));
    if (seed !== undefined) pyArgs.push("--seed", String(seed));

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/risk/tail_analysis.py", pyArgs, 300_000);
      if (result.exitCode !== 0) {
        return err(`Tail analysis failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run tail analysis: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
