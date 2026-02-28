// qualify.report — Fetch qualification report from filesystem
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const qualifyReport: ToolDef = {
  name: "qualify.report_v1",
  description:
    "Fetch the qualification report for an experiment. Returns the full qualification_report.json with hard/soft/execution/regime gate results.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to fetch qualification report for" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule(
        "research.promotion.cli",
        ["report", experimentId],
        30_000,
      );
      if (result.exitCode !== 0) {
        return err(`qualify.report failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to fetch qualification report: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
