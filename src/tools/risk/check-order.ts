// risk.check_order — Pre-trade risk check (does not submit)
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskCheckOrder: ToolDef = {
  name: "risk.check_order_v1",
  description:
    "Pre-trade risk check against RISK_POLICY constraints. Does NOT submit the order — returns pass/reject with reasons. Use this to validate an order before submission.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID" },
      symbol: { type: "string", description: "Ticker symbol to trade" },
      side: { type: "string", enum: ["buy", "sell"], description: "Order side" },
      quantity: { type: "number", description: "Number of shares/units" },
      notional: { type: "number", description: "Notional value of the order in dollars" },
      mode: {
        type: "string",
        enum: ["paper", "live"],
        description: "Trading mode (default: paper)",
      },
      run_id: { type: "string", description: "Trading run ID (optional)" },
    },
    required: ["experiment_id", "symbol", "side", "quantity", "notional"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const symbol = args.symbol as string;
    const side = args.side as string;
    const quantity = args.quantity as number;
    const notional = args.notional as number;
    const mode = (args.mode as string | undefined) ?? "paper";
    const runId = args.run_id as string | undefined;

    const pyArgs = [
      "--experiment-id", experimentId,
      "--symbol", symbol,
      "--side", side,
      "--quantity", String(quantity),
      "--notional", String(notional),
      "--mode", mode,
    ];
    if (runId) pyArgs.push("--run-id", runId);

    const runner = new PythonRunner();
    try {
      const result = await runner.run("compute/risk/check_order.py", pyArgs, 60_000);
      if (result.exitCode !== 0) {
        return err(`Risk check failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }
      try {
        return ok(JSON.parse(result.stdout));
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run risk check: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
