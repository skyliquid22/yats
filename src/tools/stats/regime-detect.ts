// stats.regime_detect — Regime detection using registered detector
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const statsRegimeDetect: ToolDef = {
  name: "stats.regime_detect_v1",
  description: "Run regime detection on a symbol's time series using a registered regime detector.",
  inputSchema: {
    type: "object",
    properties: {
      symbol: { type: "string", description: "Ticker symbol" },
      start_date: { type: "string", description: "Start date ISO-8601" },
      end_date: { type: "string", description: "End date ISO-8601" },
      detector: { type: "string", description: "Regime detector name (default: use configured default)" },
    },
    required: ["symbol", "start_date", "end_date"],
  },
  async handler(args) {
    const symbol = args.symbol as string;
    const startDate = args.start_date as string;
    const endDate = args.end_date as string;
    const detector = args.detector as string | undefined;

    const pyArgs = [symbol, "--start-date", startDate, "--end-date", endDate];
    if (detector) pyArgs.push("--detector", detector);

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule("research.features.regime_detect", pyArgs, 120_000);
      if (result.exitCode !== 0) {
        return err(`Regime detection failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run regime detection: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
