// execution.start_paper — Start paper trading for promoted experiment via Dagster pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionStartPaper: ToolDef = {
  name: "execution.start_paper_v1",
  description:
    "Start paper trading for a promoted experiment. Launches a Dagster pipeline that runs the experiment's policy against live market data with simulated execution.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to paper trade" },
      initial_capital: { type: "number", description: "Initial paper capital (default: 1_000_000)" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;
    const initialCapital = (args.initial_capital as number | undefined) ?? 1_000_000;

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("paper_trading", {
        ops: {
          paper_trading: {
            config: {
              experiment_id: experimentId,
              initial_capital: initialCapital,
            },
          },
        },
      });
      return ok({ run_id: runId, experiment_id: experimentId, initial_capital: initialCapital, job: "paper_trading" });
    } catch (e) {
      return err(`Failed to start paper trading: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
