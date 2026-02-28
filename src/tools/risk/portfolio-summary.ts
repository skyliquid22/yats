// risk.portfolio_summary — Current portfolio risk snapshot from QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskPortfolioSummary: ToolDef = {
  name: "risk.portfolio_summary_v1",
  description:
    "Current portfolio risk snapshot. Returns gross/net exposure, drawdown, daily P&L, position count, concentration metrics, and kill switch status.",
  inputSchema: {
    type: "object",
    properties: {
      run_id: { type: "string", description: "Trading run ID (paper or live)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
    },
    required: [],
  },
  async handler(args) {
    const runId = args.run_id as string | undefined;
    const experimentId = args.experiment_id as string | undefined;

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (runId) {
      conditions.push(`run_id = $${paramIdx++}`);
      params.push(runId);
    }
    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const qdb = new QuestDBClient();
    try {
      // Latest NAV snapshot
      const navSql = `SELECT timestamp, run_id, experiment_id, cash, positions_value,
                             total_nav, daily_pnl, cumulative_pnl, drawdown
                      FROM portfolio_nav
                      ${where}
                      ORDER BY timestamp DESC
                      LIMIT 1`;
      const navResult = await qdb.query(navSql, [...params]);

      // Position count and concentration
      const posSql = `SELECT count() as position_count,
                             sum(abs(market_value)) as gross_exposure,
                             sum(market_value) as net_exposure
                      FROM positions
                      ${where}`;
      const posResult = await qdb.query(posSql, [...params]);

      // Kill switch status
      const ksSql = `SELECT timestamp, trigger, action, resolved_at
                     FROM kill_switches
                     ${where ? where + " AND" : "WHERE"} resolved_at IS NULL
                     ORDER BY timestamp DESC
                     LIMIT 1`;
      let killSwitchActive = false;
      let killSwitchInfo = null;
      try {
        const ksResult = await qdb.query(ksSql, [...params]);
        if (ksResult.rows.length > 0) {
          killSwitchActive = true;
          killSwitchInfo = ksResult.rows[0];
        }
      } catch {
        // kill_switches table may not exist yet
      }

      const nav = navResult.rows.length > 0 ? navResult.rows[0] : null;
      const posStats = posResult.rows.length > 0 ? posResult.rows[0] : null;
      const totalNav = (nav?.total_nav as number) || 0;

      return ok({
        nav_snapshot: nav,
        position_count: posStats?.position_count ?? 0,
        gross_exposure: posStats?.gross_exposure ?? 0,
        net_exposure: posStats?.net_exposure ?? 0,
        gross_exposure_pct: totalNav > 0 ? ((posStats?.gross_exposure as number) ?? 0) / totalNav : 0,
        net_exposure_pct: totalNav > 0 ? ((posStats?.net_exposure as number) ?? 0) / totalNav : 0,
        kill_switch_active: killSwitchActive,
        kill_switch_info: killSwitchInfo,
      });
    } catch (e) {
      return err(`Failed to query portfolio summary: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
