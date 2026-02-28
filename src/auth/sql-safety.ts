// SQL safety — role-based table whitelisting for data.query (PRD Section 21.2)
import { type Role, TABLE_WHITELISTS } from "./roles.js";

// Extract table names from a SQL SELECT query.
// Handles: FROM table, JOIN table, FROM schema.table
// Does NOT handle subqueries as table sources (those are validated recursively by QuestDB).
const TABLE_REF_PATTERN = /\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)/gi;

export function extractTableNames(sql: string): string[] {
  const tables: string[] = [];
  let match: RegExpExecArray | null;
  // Reset lastIndex for safety
  TABLE_REF_PATTERN.lastIndex = 0;
  while ((match = TABLE_REF_PATTERN.exec(sql)) !== null) {
    tables.push(match[1].toLowerCase());
  }
  return tables;
}

// Check if a table name matches a whitelist pattern.
// Patterns: exact match ("features") or prefix glob ("canonical_*")
function tableMatchesPattern(table: string, pattern: string): boolean {
  if (pattern === "*") return true;
  if (pattern.endsWith("*")) {
    const prefix = pattern.slice(0, -1); // "canonical_*" → "canonical_"
    return table.startsWith(prefix);
  }
  return table === pattern;
}

export function checkTableAccess(role: Role, sql: string): { allowed: boolean; denied?: string[] } {
  const whitelist = TABLE_WHITELISTS[role];
  const tables = extractTableNames(sql);

  if (tables.length === 0) {
    // No table references found — possibly a function-only query or malformed
    return { allowed: true };
  }

  const denied: string[] = [];
  for (const table of tables) {
    const ok = whitelist.some((pattern) => tableMatchesPattern(table, pattern));
    if (!ok) denied.push(table);
  }

  return denied.length === 0
    ? { allowed: true }
    : { allowed: false, denied };
}

export function tableDeniedMessage(role: Role, denied: string[]): string {
  return `Table access denied for role "${role}": ${denied.join(", ")}`;
}
