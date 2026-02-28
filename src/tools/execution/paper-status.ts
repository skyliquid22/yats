// execution.paper_status — Status of paper trading run (QuestDB + Dagster)
import { DagsterClient } from "../../bridge/dagster-client.js";
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const executionPaperStatus: ToolDef = {
  name: "execution.paper_status_v1",
  description:
    "Get the status and metrics of a paper trading run. Combines Dagster pipeline status with QuestDB trading metrics.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Dagster run ID of the paper trading session" },
    },
    required: ["run_id"],
  },
  async handler(args) {
    const runId = args.run_id as string;

    const dagster = new DagsterClient();
    const qdb = new QuestDBClient();
    try {
      const dagsterStatus = await dagster.getRunStatus(runId);

      const metricsResult = await qdb.query(
        `SELECT experiment_id, total_trades, realized_pnl, unrealized_pnl,
                sharpe_ratio, max_drawdown, win_rate, last_heartbeat
         FROM paper_trading_metrics
         WHERE run_id = $1
         ORDER BY last_heartbeat DESC
         LIMIT 1`,
        [runId]
      );

      const metrics = metricsResult.rows.length > 0 ? metricsResult.rows[0] : null;

      return ok({
        run_id: runId,
        pipeline_status: dagsterStatus.status,
        end_time: dagsterStatus.endTime,
        metrics,
      });
    } catch (e) {
      return err(`Failed to get paper status: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
