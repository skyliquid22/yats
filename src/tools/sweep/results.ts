// sweep.results — Aggregate sweep results from QuestDB experiment_index
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const sweepResults: ToolDef = {
  name: "sweep.results_v1",
  description:
    "Aggregate results across a sweep by querying the experiment_index. Returns a comparison table sorted by the chosen metric.",
  inputSchema: {
    type: "object",
    properties: {
      dagster_run_id: { type: "string", description: "Sweep Dagster run_id to filter by" },
      experiment_ids: {
        type: "array",
        items: { type: "string" },
        description: "Specific experiment IDs to include (alternative to dagster_run_id filter)",
      },
      sort_by: { type: "string", enum: ["sharpe", "calmar", "total_return", "max_drawdown", "annualized_return"], description: "Metric to sort by (default: sharpe)" },
      limit: { type: "number", description: "Max results (default 50)" },
    },
    required: [],
  },
  async handler(args) {
    const dagsterRunId = args.dagster_run_id as string | undefined;
    const experimentIds = args.experiment_ids as string[] | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 50, 500);

    const VALID_SORT = new Set(["sharpe", "calmar", "total_return", "max_drawdown", "annualized_return"]);
    const sortBy = VALID_SORT.has(args.sort_by as string) ? (args.sort_by as string) : "sharpe";

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (dagsterRunId) {
      conditions.push(`dagster_run_id = $${paramIdx++}`);
      params.push(dagsterRunId);
    }

    if (experimentIds && experimentIds.length > 0) {
      const placeholders = experimentIds.map(() => `$${paramIdx++}`);
      conditions.push(`experiment_id IN (${placeholders.join(", ")})`);
      params.push(...experimentIds);
    }

    let sql = "SELECT experiment_id, feature_set, policy_type, universe, sharpe, calmar, max_drawdown, total_return, annualized_return, win_rate, turnover_1d_mean, qualification_status, created_at FROM experiment_index";
    if (conditions.length > 0) {
      sql += " WHERE " + conditions.join(" AND ");
    }
    sql += ` ORDER BY ${sortBy} DESC LIMIT ${limit}`;

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);

      // Compute summary statistics
      const rows = result.rows;
      const summary: Record<string, unknown> = {
        total_experiments: rows.length,
      };

      if (rows.length > 0) {
        const sharpes = rows.map((r) => r.sharpe as number).filter((v) => v != null);
        if (sharpes.length > 0) {
          summary.sharpe_mean = sharpes.reduce((a, b) => a + b, 0) / sharpes.length;
          summary.sharpe_best = Math.max(...sharpes);
          summary.sharpe_worst = Math.min(...sharpes);
        }
      }

      return ok({ results: rows, summary, row_count: rows.length });
    } catch (e) {
      return err(`Failed to get sweep results: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
