// stats.bootstrap_sharpe — Bootstrap confidence interval for Sharpe ratio
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const statsBootstrapSharpe: ToolDef = {
  name: "stats.bootstrap_sharpe_v1",
  description: "Compute bootstrap confidence interval for the Sharpe ratio of a symbol.",
  inputSchema: {
    type: "object",
    properties: {
      symbol: { type: "string", description: "Ticker symbol" },
      start_date: { type: "string", description: "Start date ISO-8601" },
      end_date: { type: "string", description: "End date ISO-8601" },
      n_bootstrap: { type: "number", description: "Number of bootstrap iterations (default: 10000)" },
      confidence: { type: "number", description: "Confidence level 0-1 (default: 0.95)" },
      seed: { type: "number", description: "Random seed for reproducibility" },
    },
    required: ["symbol", "start_date", "end_date"],
  },
  async handler(args) {
    const symbol = args.symbol as string;
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;
    const nBootstrap = args.n_bootstrap as number | undefined;
    const confidence = args.confidence as number | undefined;
    const seed = args.seed as number | undefined;

    const pyArgs = ["--symbol", symbol, "--start-date", startDate, "--end-date", endDate];
    if (nBootstrap !== undefined) pyArgs.push("--n-bootstrap", String(nBootstrap));
    if (confidence !== undefined) pyArgs.push("--confidence", String(confidence));
    if (seed !== undefined) pyArgs.push("--seed", String(seed));

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/stats/bootstrap.py", pyArgs, 300_000);
      if (result.exitCode !== 0) {
        return err(`Bootstrap Sharpe failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run bootstrap Sharpe: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
