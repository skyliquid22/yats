// Audit trail middleware — intercepts tool invocations and logs to QuestDB
import { writeAuditRow, type AuditRow } from "./bridge/questdb-ilp.js";
import type { RecordedQuery } from "./bridge/query-audit.js";
import type { ToolResult } from "./types/tools.js";

// Injectable audit sink — defaults to the QuestDB ILP writer. Tests (and the
// provenance suite) can capture audit rows without a live QuestDB.
export type AuditSink = (row: AuditRow) => Promise<void>;

let auditSink: AuditSink = writeAuditRow;

export function setAuditSink(sink: AuditSink | null): void {
  auditSink = sink ?? writeAuditRow;
}

// Max size for parameter/result JSON to avoid bloating the audit table
const MAX_JSON_LENGTH = 8192;

function truncateJson(obj: unknown): string {
  const json = JSON.stringify(obj ?? {});
  if (json.length <= MAX_JSON_LENGTH) return json;
  return json.slice(0, MAX_JSON_LENGTH - 3) + "...";
}

function extractExperimentId(args: Record<string, unknown>): string | null {
  const eid = args.experiment_id;
  return typeof eid === "string" ? eid : null;
}

function extractDagsterRunId(result: ToolResult): string | null {
  try {
    const text = result.content[0]?.text;
    if (!text) return null;
    const parsed = JSON.parse(text);
    if (typeof parsed.run_id === "string") return parsed.run_id;
    if (typeof parsed.dagster_run_id === "string") return parsed.dagster_run_id;
  } catch {
    // not JSON or no run_id
  }
  return null;
}

function buildResultSummary(result: ToolResult): string {
  try {
    const text = result.content[0]?.text;
    if (!text) return "{}";
    // Try to parse as JSON to get structured summary
    const parsed = JSON.parse(text);
    // Keep only scalar fields + row_count for summary
    const summary: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (v === null || typeof v !== "object") summary[k] = v;
    }
    return truncateJson(summary);
  } catch {
    // Plain text result — wrap it
    const text = result.content[0]?.text ?? "";
    return truncateJson({ message: text.slice(0, 500) });
  }
}

// Sanitize args before logging — strip role metadata, keep audit-relevant fields
function sanitizeParams(args: Record<string, unknown>): Record<string, unknown> {
  const { _invoker_role, ...rest } = args;
  return rest;
}

export async function logToolInvocation(
  toolName: string,
  invoker: string,
  args: Record<string, unknown>,
  result: ToolResult,
  durationMs: number,
  queries: readonly RecordedQuery[] = [],
): Promise<void> {
  const row: AuditRow = {
    tool_name: toolName,
    invoker,
    experiment_id: extractExperimentId(args),
    parameters: truncateJson(sanitizeParams(args)),
    result_status: result.isError ? "failure" : "success",
    result_summary: buildResultSummary(result),
    duration_ms: Math.round(durationMs),
    dagster_run_id: extractDagsterRunId(result),
    quanttown_molecule_id: typeof args.quanttown_molecule_id === "string" ? args.quanttown_molecule_id : null,
    quanttown_bead_id: typeof args.quanttown_bead_id === "string" ? args.quanttown_bead_id : null,
    query_hashes: queries.length > 0 ? JSON.stringify(queries.map((q) => q.hash)) : null,
  };

  // Awaited by the dispatcher (audit-before-respond) but never fails the tool
  // response: failures are logged to stderr only.
  try {
    await auditSink(row);
  } catch (err) {
    // Log to stderr but never fail the tool response
    console.error("[audit] Failed to write audit row:", (err as Error).message);
  }
}
