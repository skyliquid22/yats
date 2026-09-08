// Query provenance — records every SQL query executed during a tool invocation
// and computes a stable, deterministic hash for each so reads can later be
// verified against the audit_trail (v1.1 external-LLM-policy provenance).
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";

export interface RecordedQuery {
  sql: string;
  params: unknown[];
  hash: string;
}

const recordingStorage = new AsyncLocalStorage<RecordedQuery[]>();

// Canonical form of a query: whitespace-normalized SQL + JSON-encoded params.
// Formatting-only changes to a query string do not change its hash; any change
// to the effective SQL text or parameter values does.
export function canonicalizeQuery(sql: string, params: readonly unknown[] = []): string {
  const normalizedSql = sql.replace(/\s+/g, " ").trim();
  return JSON.stringify({ sql: normalizedSql, params });
}

// Stable deterministic hash: sha256 hex of the canonical form. No timestamps,
// no randomness — the same query + params always yields the same hash.
export function queryHash(sql: string, params: readonly unknown[] = []): string {
  return createHash("sha256").update(canonicalizeQuery(sql, params), "utf8").digest("hex");
}

// Record a query into the active invocation context (no-op outside one).
// Called by QuestDBClient before execution so even failed reads are auditable.
export function recordQuery(sql: string, params: readonly unknown[] = []): void {
  const queries = recordingStorage.getStore();
  if (queries) {
    queries.push({ sql, params: [...params], hash: queryHash(sql, params) });
  }
}

// Run fn with a fresh recording context; returns its result plus every query
// recorded while it ran (across all awaits and QuestDBClient instances).
export async function withQueryRecording<T>(
  fn: () => Promise<T>,
): Promise<{ result: T; queries: RecordedQuery[] }> {
  const queries: RecordedQuery[] = [];
  const result = await recordingStorage.run(queries, fn);
  return { result, queries };
}
