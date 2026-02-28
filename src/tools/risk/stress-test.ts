// risk.stress_test — Run stress test against historical scenarios
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskStressTest: ToolDef = {
  name: "risk.stress_test_v1",
  description:
    "Run stress test against historical scenarios (GFC 2008, COVID 2020, dot-com bust, flash crash, volmageddon). Returns portfolio impact under each scenario.",
  inputSchema: {
    type: "object",
    properties: {
      symbols: {
        type: "array",
        items: { type: "string" },
        description: "List of ticker symbols in portfolio",
      },
      scenario: {
        type: "string",
        description: "Scenario key: gfc_2008, covid_2020, dotcom_2000, flash_crash_2010, vix_2018, or 'all' (default: all)",
      },
      weights: {
        type: "array",
        items: { type: "number" },
        description: "Portfolio weights per symbol (must sum to ~1.0). If omitted, equal weight.",
      },
    },
    required: ["symbols"],
  },
  async handler(args) {
    const symbols = args.symbols as string[];
    const scenario = (args.scenario as string | undefined) ?? "all";
    const weights = args.weights as number[] | undefined;

    if (symbols.length === 0) {
      return err("At least one symbol required");
    }

    const pyArgs = ["--symbols", symbols.join(","), "--scenario", scenario];
    if (weights) {
      if (weights.length !== symbols.length) {
        return err("weights length must match symbols length");
      }
      pyArgs.push("--weights", weights.join(","));
    }

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/risk/stress_test.py", pyArgs, 300_000);
      if (result.exitCode !== 0) {
        return err(`Stress test failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run stress test: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
