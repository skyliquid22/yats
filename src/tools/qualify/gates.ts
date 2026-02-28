// qualify.gates — List all qualification gates and their current thresholds
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const qualifyGates: ToolDef = {
  name: "qualify.gates_v1",
  description:
    "List all qualification gates and their current thresholds. Returns gate definitions from the static config including hard/soft/execution/regime gates.",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  async handler() {
    const runner = new PythonRunner();
    try {
      const result = await runner.runModule(
        "research.promotion.cli",
        ["gates"],
        15_000,
      );
      if (result.exitCode !== 0) {
        return err(`qualify.gates failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to fetch gate definitions: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
