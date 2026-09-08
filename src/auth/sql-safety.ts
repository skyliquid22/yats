// SQL safety — role-based table whitelisting for data.query (PRD Section 21.2)
import { type Role, TABLE_WHITELISTS } from "./roles.js";

// Strip SQL comments so table references cannot be hidden behind them
// (e.g. `FROM/**/audit_trail` or `FROM -- \n audit_trail`).
function stripComments(sql: string): string {
  return sql.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/--[^\n]*/g, " ");
}

export interface TableExtraction {
  tables: string[];
  // True when a FROM/JOIN clause was found whose table reference could not be
  // parsed. Callers must deny in that case — an unparseable reference would
  // otherwise bypass the whitelist entirely.
  unparseable: boolean;
}

// Matches each FROM/JOIN keyword; the token that follows is inspected separately.
const FROM_JOIN_PATTERN = /\b(?:FROM|JOIN)\b/gi;
// Bare identifier: FROM table or FROM schema.table
const BARE_IDENT_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_.$]*/;
// Double-quoted identifier: FROM "table" ("" escapes a quote)
const DQUOTED_IDENT_PATTERN = /^"((?:[^"]|"")+)"/;
// Single-quoted table name (QuestDB allows FROM 'table')
const SQUOTED_IDENT_PATTERN = /^'((?:[^']|'')+)'/;

// Extract table names from a SQL SELECT query, including quoted identifiers.
// Subqueries `FROM (` are skipped here — their inner FROM/JOIN clauses are
// matched by the same scan.
export function extractTableReferences(sql: string): TableExtraction {
  const cleaned = stripComments(sql);
  const tables: string[] = [];
  let unparseable = false;

  FROM_JOIN_PATTERN.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = FROM_JOIN_PATTERN.exec(cleaned)) !== null) {
    const rest = cleaned.slice(match.index + match[0].length).replace(/^\s+/, "");
    if (rest.length === 0 || rest.startsWith("(")) continue; // subquery (or trailing keyword)

    const dquoted = DQUOTED_IDENT_PATTERN.exec(rest);
    if (dquoted) {
      tables.push(dquoted[1].replace(/""/g, '"').toLowerCase());
      continue;
    }
    const squoted = SQUOTED_IDENT_PATTERN.exec(rest);
    if (squoted) {
      tables.push(squoted[1].replace(/''/g, "'").toLowerCase());
      continue;
    }
    const bare = BARE_IDENT_PATTERN.exec(rest);
    if (bare) {
      tables.push(bare[0].toLowerCase());
      continue;
    }
    unparseable = true;
  }

  return { tables, unparseable };
}

// Back-compat helper: table names only.
export function extractTableNames(sql: string): string[] {
  return extractTableReferences(sql).tables;
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
  const { tables, unparseable } = extractTableReferences(sql);

  if (unparseable) {
    // A FROM/JOIN whose table reference cannot be identified must be denied —
    // allowing it would let obfuscated identifiers bypass the whitelist.
    return { allowed: false, denied: ["<unparseable table reference>"] };
  }

  if (tables.length === 0) {
    // No table references found — possibly a function-only query (e.g. SELECT 1)
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
