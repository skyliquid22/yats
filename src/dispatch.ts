// Tool-call dispatch pipeline — role resolution, permission + rate-limit
// checks, query-provenance recording, and audit logging. Extracted from
// server.ts so the full pipeline is testable without a stdio transport.
import { tools } from "./tools/registry.js";
import { type Role, isValidRole, canInvoke, deniedMessage, checkRateLimit } from "./auth/index.js";
import { logToolInvocation } from "./audit.js";
import { withQueryRecording } from "./bridge/query-audit.js";
import type { ToolResult } from "./types/tools.js";

const DEFAULT_ROLE: Role = "intern";

// _meta.invoker_role is client-supplied and therefore spoofable over stdio.
// The operator must explicitly allow elevated roles via YATS_ALLOWED_ROLES
// (comma-separated role names, or "*" to allow all valid roles). A requested
// role outside the allowlist falls back to the least-privileged role.
function roleAllowedByOperator(role: Role): boolean {
  const raw = process.env.YATS_ALLOWED_ROLES ?? "";
  if (raw.trim() === "*") return true;
  const allowed = raw.split(",").map((r) => r.trim()).filter((r) => r.length > 0);
  return allowed.includes(role);
}

// Extract invoker role from MCP request _meta, clamped to the operator allowlist
export function extractRole(meta: Record<string, unknown> | undefined): Role {
  const role = meta?.invoker_role;
  if (typeof role === "string" && isValidRole(role)) {
    if (role === DEFAULT_ROLE || roleAllowedByOperator(role)) return role;
    console.error(
      `[auth] Requested role "${role}" not in YATS_ALLOWED_ROLES; falling back to "${DEFAULT_ROLE}"`,
    );
  }
  return DEFAULT_ROLE;
}

export function extractAgentId(meta: Record<string, unknown> | undefined): string {
  const id = meta?.agent_id;
  return typeof id === "string" ? id : "unknown";
}

export interface DispatchOutcome {
  result: ToolResult;
  // Stable sha256 hashes of every SQL query executed during the invocation,
  // in execution order — the same values written to audit_trail.query_hashes.
  queryHashes: string[];
}

export async function handleToolCall(
  name: string,
  args: Record<string, unknown> | undefined,
  meta: Record<string, unknown> | undefined,
): Promise<DispatchOutcome> {
  const role = extractRole(meta);
  const agentId = extractAgentId(meta);

  // 1. Check tool exists
  const tool = tools.get(name);
  if (!tool) {
    return {
      result: { content: [{ type: "text", text: `Unknown tool: ${name}` }], isError: true },
      queryHashes: [],
    };
  }

  // 2. Check role-based permission
  if (!canInvoke(role, name)) {
    return {
      result: { content: [{ type: "text", text: deniedMessage(role, name) }], isError: true },
      queryHashes: [],
    };
  }

  // 3. Check rate limits
  const rateCheck = checkRateLimit(role, agentId, name);
  if (!rateCheck.allowed) {
    return {
      result: { content: [{ type: "text", text: rateCheck.message! }], isError: true },
      queryHashes: [],
    };
  }

  // 4. Pass role through to handler via args (for tools that need it, e.g. data.query)
  const argsWithRole = { ...(args ?? {}), _invoker_role: role };

  const startTime = performance.now();
  const { result, queries } = await withQueryRecording(() => tool.handler(argsWithRole));
  const durationMs = performance.now() - startTime;

  // 5. Audit trail — awaited so every read is logged (with its query hashes)
  // before the response is returned; audit failures never fail the response.
  await logToolInvocation(name, agentId, argsWithRole, result, durationMs, queries);

  return { result, queryHashes: queries.map((q) => q.hash) };
}
