// stats.deflated_sharpe — Deflated Sharpe Ratio (multiple testing correction)
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const statsDeflatedSharpe: ToolDef = {
  name: "stats.deflated_sharpe_v1",
  description: "Compute the Deflated Sharpe Ratio to correct for multiple testing (backtest overfitting).",
  inputSchema: {
    type: "object",
    properties: {
      observed_sharpe: { type: "number", description: "The observed (annualized) Sharpe ratio" },
      num_trials: { type: "number", description: "Number of backtest configurations tried" },
      skewness: { type: "number", description: "Skewness of return series" },
      kurtosis: { type: "number", description: "Excess kurtosis of return series" },
      n_observations: { type: "number", description: "Number of return observations" },
    },
    required: ["observed_sharpe", "num_trials", "skewness", "kurtosis", "n_observations"],
  },
  async handler(args) {
    const observedSharpe = args.observed_sharpe as number;
    const numTrials = args.num_trials as number;
    const skewness = args.skewness as number;
    const kurtosis = args.kurtosis as number;
    const nObs = args.n_observations as number;

    const pyArgs = [
      "--observed-sharpe", String(observedSharpe),
      "--num-trials", String(numTrials),
      "--skewness", String(skewness),
      "--kurtosis", String(kurtosis),
      "--n-observations", String(nObs),
    ];

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/stats/deflated_sharpe.py", pyArgs, 60_000);
      if (result.exitCode !== 0) {
        return err(`Deflated Sharpe failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run Deflated Sharpe: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
