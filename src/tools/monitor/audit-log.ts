// monitor.audit_log — Query the audit_trail table from QuestDB
// Schema reference: pipelines/yats_pipelines/utils/create_tables.py (AUDIT_TRAIL)
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const monitorAuditLog: ToolDef = {
  name: "monitor.audit_log_v1",
  description:
    "Query the audit trail of MCP tool invocations. Filter by tool name, invoker, experiment, result status, or query hash (read provenance).",
  inputSchema: {
    type: "object",
    properties: {
      tool_name: { type: "string", description: "Filter by tool name (e.g. 'data.query_v1') (optional)" },
      invoker: { type: "string", description: "Filter by invoker (agent ID) (optional)" },
      experiment_id: { type: "string", description: "Filter by experiment ID (optional)" },
      result_status: {
        type: "string",
        enum: ["success", "failure", "timeout"],
        description: "Filter by result status (optional)",
      },
      query_hash: { type: "string", description: "Filter to invocations whose query_hashes contain this sha256 hash (provenance lookup) (optional)" },
      limit: { type: "number", description: "Max entries to return (default: 50, max: 1000)", minimum: 1, maximum: 1000 },
    },
    required: [],
  },
  async handler(args) {
    const toolName = args.tool_name as string | undefined;
    const invoker = args.invoker as string | undefined;
    const experimentId = args.experiment_id as string | undefined;
    const resultStatus = args.result_status as string | undefined;
    const queryHashFilter = args.query_hash as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 50, 1000);

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (toolName) {
      conditions.push(`tool_name = $${paramIdx++}`);
      params.push(toolName);
    }
    if (invoker) {
      conditions.push(`invoker = $${paramIdx++}`);
      params.push(invoker);
    }
    if (experimentId) {
      conditions.push(`experiment_id = $${paramIdx++}`);
      params.push(experimentId);
    }
    if (resultStatus) {
      conditions.push(`result_status = $${paramIdx++}`);
      params.push(resultStatus);
    }
    if (queryHashFilter) {
      // query_hashes is a JSON array of sha256 hex hashes; validate to keep
      // the LIKE pattern free of user-supplied wildcards.
      if (!/^[0-9a-f]{64}$/.test(queryHashFilter)) {
        return err("query_hash must be a 64-char lowercase sha256 hex string");
      }
      conditions.push(`query_hashes LIKE '%${queryHashFilter}%'`);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const sql = `SELECT timestamp, tool_name, invoker, experiment_id, mode, parameters,
                        result_status, result_summary, duration_ms, query_hashes, dagster_run_id
                 FROM audit_trail
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
