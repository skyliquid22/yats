// monitor.audit_log — Query audit trail from QuestDB
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const monitorAuditLog: ToolDef = {
  name: "monitor.audit_log_v1",
  description:
    "Query the audit trail for system events. Filter by action type, actor, or experiment.",
  inputSchema: {
    type: "object",
    properties: {
      action: { type: "string", description: "Filter by action type (e.g. 'promote', 'start_paper', 'halt') (optional)" },
      actor: { type: "string", description: "Filter by actor (user or agent ID) (optional)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      limit: { type: "number", description: "Max entries to return (default: 50, max: 1000)", minimum: 1, maximum: 1000 },
    },
    required: [],
  },
  async handler(args) {
    const action = args.action as string | undefined;
    const actor = args.actor as string | undefined;
    const experimentId = args.experiment_id as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 50, 1000);

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (action) {
      conditions.push(`action = $${paramIdx++}`);
      params.push(action);
    }
    if (actor) {
      conditions.push(`actor = $${paramIdx++}`);
      params.push(actor);
    }
    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT timestamp, action, actor, experiment_id, details, run_id
                 FROM audit_log
                 ${where}
                 ORDER BY timestamp DESC
                 LIMIT $${paramIdx}`;
    params.push(limit);

    const qdb = new QuestDBClient();
    try {
      const result = await qdb.query(sql, params);
      return ok({ entries: result.rows, count: result.rows.length });
    } catch (e) {
      return err(`Failed to query audit log: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
