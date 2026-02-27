// stats.scm_leakage — Structural Causal Model leakage detection
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const statsScmLeakage: ToolDef = {
  name: "stats.scm_leakage_v1",
  description: "Detect potential look-ahead bias and data leakage in feature pipelines using lead-lag correlation analysis.",
  inputSchema: {
    type: "object",
    properties: {
      data_file: { type: "string", description: "Path to CSV with feature columns and a target column" },
      target_column: { type: "string", description: "Name of the target column (default: target)" },
      max_lag: { type: "number", description: "Max lag for cross-correlation analysis (default: 5)" },
    },
    required: ["data_file"],
  },
  async handler(args) {
    const dataFile = args.data_file as string;
    const targetColumn = (args.target_column as string | undefined) ?? "target";
    const maxLag = args.max_lag as number | undefined;

    const pyArgs = ["--data-file", dataFile, "--target-column", targetColumn];
    if (maxLag !== undefined) pyArgs.push("--max-lag", String(maxLag));

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/stats/scm_leakage.py", pyArgs, 120_000);
      if (result.exitCode !== 0) {
        return err(`SCM leakage detection failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run SCM leakage detection: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
