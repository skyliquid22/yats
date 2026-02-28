// shadow.results — Fetch shadow execution results from QuestDB + filesystem
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const shadowResults: ToolDef = {
  name: "shadow.results_v1",
  description:
    "Fetch shadow execution results (metrics + logs) for a completed shadow run. Returns summary metrics and step-level execution logs from .yats_data/shadow/.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID" },
      run_id: { type: "string", description: "Dagster run ID of the shadow run" },
    },
    required: ["experiment_id", "run_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const runId = args.run_id as string;

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule(
        "research.shadow.cli",
        ["results", experimentId, runId],
        30_000,
      );
      if (result.exitCode !== 0) {
        return err(`shadow.results failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to fetch shadow results: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
