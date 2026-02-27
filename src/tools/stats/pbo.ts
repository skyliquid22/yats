// stats.pbo — Probability of Backtest Overfitting
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const statsPbo: ToolDef = {
  name: "stats.pbo_v1",
  description: "Compute Probability of Backtest Overfitting (PBO) via combinatorially symmetric cross-validation.",
  inputSchema: {
    type: "object",
    properties: {
      returns_file: { type: "string", description: "Path to CSV file: rows=time periods, columns=strategy variants" },
      n_partitions: { type: "number", description: "Number of CSCV partitions (default: 16, must be even)" },
      seed: { type: "number", description: "Random seed for reproducibility" },
    },
    required: ["returns_file"],
  },
  async handler(args) {
    const returnsFile = args.returns_file as string;
    const nPartitions = args.n_partitions as number | undefined;
    const seed = args.seed as number | undefined;

    const pyArgs = ["--returns-file", returnsFile];
    if (nPartitions !== undefined) pyArgs.push("--n-partitions", String(nPartitions));
    if (seed !== undefined) pyArgs.push("--seed", String(seed));

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/stats/pbo.py", pyArgs, 300_000);
      if (result.exitCode !== 0) {
        return err(`PBO failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run PBO: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
