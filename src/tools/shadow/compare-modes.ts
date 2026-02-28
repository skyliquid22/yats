// shadow.compare_modes — Compare shadow vs paper vs live for same experiment via QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const shadowCompareModes: ToolDef = {
  name: "shadow.compare_modes_v1",
  description:
    "Compare shadow vs paper vs live execution metrics for the same experiment. Queries the execution_metrics table in QuestDB for cross-mode comparison.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Experiment ID to compare across modes" },
    },
    required: ["experiment_id"],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(
        `SELECT mode, run_id, sharpe, max_drawdown, total_return,
                fill_rate, reject_rate, avg_slippage_bps, p95_slippage_bps,
                total_fees, total_turnover, execution_halts, timestamp
         FROM execution_metrics
         WHERE experiment_id = $1
         ORDER BY timestamp DESC`,
        [experimentId],
      );
      return ok({
        experiment_id: experimentId,
        modes: result.rows,
        columns: result.columns,
      });
    } catch (e) {
      return err(`Failed to compare modes: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
