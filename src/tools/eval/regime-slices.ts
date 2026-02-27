// eval.regime_slices — Generate regime slice artifacts for an experiment
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const evalRegimeSlices: ToolDef = {
  name: "eval.regime_slices_v1",
  description:
    "Generate regime slice artifacts for an experiment. Shows per-regime performance (sharpe, return, drawdown) and identifies best/worst regimes.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID (SHA256 hex)" },
      labeling: { type: "string", enum: ["v1", "v2"], description: "Regime labeling version (default: v2)" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const labeling = (args.labeling as string | undefined) ?? "v2";

    const pyArgs = ["regime-slices", experimentId, "--labeling", labeling];

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule("research.eval.cli", pyArgs, 60_000);
      if (result.exitCode !== 0) {
        return err(`eval.regime_slices failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to generate regime slices: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
