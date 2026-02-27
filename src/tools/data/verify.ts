// data.verify — Data quality checks on canonical data
import { PythonRunner } from "../../bridge/python-runner.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const dataVerify: ToolDef = {
  name: "data.verify_v1",
  description: "Run data quality checks on canonical data, returning a validation report.",
  inputSchema: {
    type: "object",
    properties: {
      domain: {
        type: "string",
        enum: ["equity_ohlcv", "fundamentals", "financial_metrics"],
        description: "Data domain to verify",
      },
      symbols: { type: "array", items: { type: "string" }, description: "Symbols to check (empty = all)" },
      start_date: { type: "string", description: "Start date ISO-8601 (optional)" },
      end_date: { type: "string", description: "End date ISO-8601 (optional)" },
    },
    required: ["domain"],
  },
  async handler(args) {
    const domain = args.domain as string;
    const symbols = (args.symbols as string[] | undefined) ?? [];
    const startDate = (args.start_date as string | undefined) ?? "";
    const endDate = (args.end_date as string | undefined) ?? "";

    const pyArgs = ["--domain", domain];
    if (symbols.length > 0) pyArgs.push("--symbols", symbols.join(","));
    if (startDate) pyArgs.push("--start-date", startDate);
    if (endDate) pyArgs.push("--end-date", endDate);

    const runner = new PythonRunner();
    try {
      const result = await runner.runModule("research.verify_data", pyArgs, 120_000);
      if (result.exitCode !== 0) {
        return err(`Verification failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`);
      }

      try {
        const report = JSON.parse(result.stdout);
        return ok(report);
      } catch {
        return ok({ raw_output: result.stdout });
      }
    } catch (e) {
      return err(`Failed to run verify: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
