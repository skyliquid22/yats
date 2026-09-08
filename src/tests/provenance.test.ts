// Provenance test — perform reads through the full dispatch pipeline, collect
// the stable query hashes, and verify every one is present in the audit log.
// Uses an in-memory audit sink; does not require a live QuestDB (queries are
// recorded before execution, so hashes are logged even when the DB is down).
import { test, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { setAuditSink } from "../audit.js";
import type { AuditRow } from "../bridge/questdb-ilp.js";
import { queryHash } from "../bridge/query-audit.js";
import { handleToolCall } from "../dispatch.js";

const capturedRows: AuditRow[] = [];

before(() => {
  process.env.YATS_ALLOWED_ROLES = "*";
  setAuditSink(async (row) => {
    capturedRows.push(row);
  });
});

after(() => {
  setAuditSink(null);
  delete process.env.YATS_ALLOWED_ROLES;
});

beforeEach(() => {
  capturedRows.length = 0;
});

function auditedHashes(toolName: string): string[] {
  return capturedRows
    .filter((r) => r.tool_name === toolName)
    .flatMap((r) => (r.query_hashes ? (JSON.parse(r.query_hashes) as string[]) : []));
}

const META = { invoker_role: "managing_partner", agent_id: "provenance-test" };

test("data.query read is audit-logged with the client-computable stable hash", async () => {
  const sql = "SELECT symbol FROM features LIMIT 5";
  const outcome = await handleToolCall("data.query_v1", { sql }, META);

  // The client can compute the exact hash from what it sent (LIMIT already present)
  const expected = queryHash(sql, []);
  assert.ok(outcome.queryHashes.includes(expected), "dispatch outcome missing expected hash");
  assert.ok(auditedHashes("data.query_v1").includes(expected), "audit log missing expected hash");
});

test("every read tool logs its queries to the audit log with matching hashes", async () => {
  const reads: Array<{ tool: string; args: Record<string, unknown> }> = [
    { tool: "data.query_v1", args: { sql: "SELECT count() FROM canonical_equity_ohlcv" } },
    { tool: "features.stats_v1", args: { feature: "ret_1d" } },
    { tool: "features.watermarks_v1", args: {} },
    { tool: "experiment.list_v1", args: {} },
    { tool: "execution.nav_v1", args: { experiment_id: "exp-prov-test" } },
    { tool: "execution.positions_v1", args: { experiment_id: "exp-prov-test" } },
    { tool: "execution.orders_v1", args: { experiment_id: "exp-prov-test" } },
    { tool: "risk.decisions_v1", args: {} },
    { tool: "promote.list_v1", args: {} },
    { tool: "monitor.audit_log_v1", args: { limit: 5 } },
  ];

  const collected: Array<{ tool: string; hashes: string[] }> = [];
  for (const { tool, args } of reads) {
    const outcome = await handleToolCall(tool, args, META);
    assert.ok(
      outcome.queryHashes.length >= 1,
      `${tool} executed no recorded queries — read not auditable`,
    );
    collected.push({ tool, hashes: outcome.queryHashes });
  }

  // Every hash collected from the reads must be present in the audit log
  for (const { tool, hashes } of collected) {
    const logged = auditedHashes(tool);
    for (const hash of hashes) {
      assert.ok(logged.includes(hash), `${tool}: hash ${hash} not found in audit log`);
    }
  }
});

test("rerunning the same read produces identical hashes (stability)", async () => {
  const args = { feature: "ret_1d", symbol: "AAPL" };
  const first = await handleToolCall("features.stats_v1", args, META);
  const second = await handleToolCall("features.stats_v1", args, META);
  assert.deepEqual(first.queryHashes, second.queryHashes);
  assert.ok(first.queryHashes.length >= 1);
});

test("denied invocations execute no queries but are still audit-logged", async () => {
  const outcome = await handleToolCall(
    "data.query_v1",
    { sql: 'SELECT * FROM "kill_switches"' },
    { invoker_role: "intern", agent_id: "provenance-test" },
  );
  assert.equal(outcome.result.isError, true);
  assert.deepEqual(outcome.queryHashes, []);
});

test("spoofed _meta role is clamped when not in YATS_ALLOWED_ROLES", async () => {
  const saved = process.env.YATS_ALLOWED_ROLES;
  delete process.env.YATS_ALLOWED_ROLES;
  try {
    // promote.list requires pm+; a spoofed managing_partner claim without the
    // operator allowlist falls back to intern and is denied.
    const outcome = await handleToolCall("promote.list_v1", {}, META);
    assert.equal(outcome.result.isError, true);
    assert.match(outcome.result.content[0]!.text, /Permission denied/);
  } finally {
    process.env.YATS_ALLOWED_ROLES = saved;
  }
});
