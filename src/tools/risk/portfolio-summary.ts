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
      run_id: { type: "string", description: "Dagster run ID of the trading run (optional)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
    },
    required: [],
  },
  async handler(args) {
    const runId = args.run_id as string | undefined;
    const experimentId = args.experiment_id as string | undefined;

    // Schema reference: create_tables.py — portfolio_state has dagster_run_id;
    // positions and kill_switches key on experiment_id/mode only.
    const navConditions: string[] = [];
    const navParams: unknown[] = [];
    let navIdx = 1;
    if (runId) {
      navConditions.push(`dagster_run_id = $${navIdx++}`);
      navParams.push(runId);
    }
    if (experimentId) {
      navConditions.push(`experiment_id = $${navIdx++}`);
      navParams.push(experimentId);
    }
    const navWhere = navConditions.length > 0 ? `WHERE ${navConditions.join(" AND ")}` : "";

    const expWhere = experimentId ? "WHERE experiment_id = $1" : "";
    const expParams: unknown[] = experimentId ? [experimentId] : [];

    const qdb = new QuestDBClient();
    try {
      // Latest NAV snapshot from portfolio_state
      const navSql = `SELECT timestamp, experiment_id, mode, nav, cash, gross_exposure,
                             net_exposure, leverage, num_positions, daily_pnl, peak_nav,
                             drawdown, dagster_run_id
                      FROM portfolio_state
                      ${navWhere}
                      ORDER BY timestamp DESC
                      LIMIT 1`;
      const navResult = await qdb.query(navSql, [...navParams]);

      // Position count and concentration
      const posSql = `SELECT count() as position_count,
                             sum(abs(notional)) as gross_exposure,
                             sum(notional) as net_exposure
                      FROM positions
                      ${expWhere}`;
      const posResult = await qdb.query(posSql, [...expParams]);

      // Kill switch status
      const ksSql = `SELECT timestamp, trigger, action, resolved_at
                     FROM kill_switches
                     ${expWhere ? expWhere + " AND" : "WHERE"} resolved_at IS NULL
                     ORDER BY timestamp DESC
                     LIMIT 1`;
      let killSwitchActive = false;
      let killSwitchInfo = null;
      try {
        const ksResult = await qdb.query(ksSql, [...expParams]);
        if (ksResult.rows.length > 0) {
          killSwitchActive = true;
          killSwitchInfo = ksResult.rows[0];
        }
      } catch {
        // kill_switches table may not exist yet
      }

      const nav = navResult.rows.length > 0 ? navResult.rows[0] : null;
      const posStats = posResult.rows.length > 0 ? posResult.rows[0] : null;
      const totalNav = (nav?.nav as number) || 0;

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
