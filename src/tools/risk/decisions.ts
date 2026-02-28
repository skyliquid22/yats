// risk.decisions — Query risk decision log from QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const riskDecisions: ToolDef = {
  name: "risk.decisions_v1",
  description:
    "Query the risk decision history log. Returns past risk decisions including pass, reject, halt, and size_reduce events with input metrics and actions taken.",
  inputSchema: {
    type: "object",
    properties: {
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      rule_id: { type: "string", description: "Filter by risk rule ID (optional)" },
      decision: {
        type: "string",
        enum: ["pass", "reject", "halt", "size_reduce"],
        description: "Filter by decision type (optional)",
      },
      mode: {
        type: "string",
        enum: ["paper", "live", "research"],
        description: "Filter by trading mode (optional)",
      },
      limit: { type: "number", description: "Max rows to return (default: 100, max: 1000)", minimum: 1, maximum: 1000 },
    },
    required: [],
  },
  async handler(args) {
    const experimentId = args.experiment_id as string | undefined;
    const ruleId = args.rule_id as string | undefined;
    const decision = args.decision as string | undefined;
    const mode = args.mode as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 100, 1000);

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }
    if (ruleId) {
      conditions.push(`rule_id = $${paramIdx++}`);
      params.push(ruleId);
    }
    if (decision) {
      conditions.push(`decision = $${paramIdx++}`);
      params.push(decision);
    }
    if (mode) {
      conditions.push(`mode = $${paramIdx++}`);
      params.push(mode);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT timestamp, rule_id, experiment_id, mode, input_metrics,
                        decision, action_taken, original_size, reduced_size, dagster_run_id
                 FROM risk_decisions
                 ${where}
                 ORDER BY timestamp DESC
                 LIMIT $${paramIdx}`;
    params.push(limit);

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      return ok({ decisions: result.rows, count: result.rows.length });
    } catch (e) {
      return err(`Failed to query risk decisions: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
