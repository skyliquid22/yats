// Role-based permissions — PRD Section 21.1
// Tool patterns: exact match or prefix glob (e.g., "registry.*")
// Tool names use _v1 suffix; matching strips version before checking.

export type Role = "intern" | "researcher" | "risk_officer" | "pm" | "managing_partner";

// --- Tool permission sets ---

const INTERN_TOOLS: readonly string[] = [
  "data.query", "data.coverage", "data.vendors",
  "features.list", "features.stats", "features.correlations",
  "features.coverage", "features.watermarks",
  "experiment.list", "experiment.get",
  "monitor.health", "monitor.data_freshness",
  "registry.*",
];

const RESEARCHER_EXTRA: readonly string[] = [
  "data.ingest", "data.ingest_universe", "data.refresh",
  "data.canonicalize", "data.verify",
  "features.compute", "features.compute_incremental",
  "experiment.create", "experiment.run", "experiment.compare",
  "eval.*",
  "shadow.*",
  "stats.*",
  "sweep.*",
];

const RISK_OFFICER_EXTRA: readonly string[] = [
  "risk.check_order", "risk.portfolio_summary", "risk.stress_test",
  "risk.correlation", "risk.tail_analysis", "risk.decisions",
  "qualify.*",
  "shadow.*",
  "risk.halt_trading",
];

const PM_EXTRA: readonly string[] = [
  "promote.to_research", "promote.to_candidate", "promote.list", "promote.history",
  "execution.start_paper", "execution.stop_paper", "execution.paper_status",
  "execution.positions", "execution.orders", "execution.nav",
  "risk.halt_trading",
];

const MANAGING_PARTNER_EXTRA: readonly string[] = [
  "promote.to_production",
  "execution.promote_live",
  "risk.resume_trading",
  "risk.flatten_positions",
];

// Build cumulative permission sets
export const ROLE_PERMISSIONS: Record<Role, readonly string[]> = {
  intern: INTERN_TOOLS,
  researcher: [...INTERN_TOOLS, ...RESEARCHER_EXTRA],
  risk_officer: [...INTERN_TOOLS, ...RISK_OFFICER_EXTRA],
  pm: [...INTERN_TOOLS, ...RESEARCHER_EXTRA, ...PM_EXTRA],
  managing_partner: ["*"], // full access
};

// --- Table whitelists for data.query (PRD Section 21.2) ---

const INTERN_TABLES: readonly string[] = [
  "canonical_*", "features", "experiment_index",
];

const RESEARCHER_TABLES: readonly string[] = [
  ...INTERN_TABLES,
  "reconciliation_log", "audit_trail", "execution_metrics",
  "execution_log", "risk_decisions", "orders", "portfolio_state", "promotions",
];

const RISK_OFFICER_TABLES: readonly string[] = [
  ...RESEARCHER_TABLES,
  "kill_switches", "raw_*",
];

export const TABLE_WHITELISTS: Record<Role, readonly string[]> = {
  intern: INTERN_TABLES,
  researcher: RESEARCHER_TABLES,
  risk_officer: RISK_OFFICER_TABLES,
  pm: RISK_OFFICER_TABLES,           // same as risk_officer per PRD
  managing_partner: ["*"],            // all tables
};

// --- Rate limits on destructive tools (PRD Section 21.4) ---

export interface RateLimitRule {
  maxInvocations: number;
  windowMs: number;  // milliseconds
}

export const RATE_LIMITS: Record<string, RateLimitRule> = {
  "risk.halt_trading":      { maxInvocations: 5, windowMs: 3_600_000 },   // 5/hour
  "risk.flatten_positions": { maxInvocations: 1, windowMs: 3_600_000 },   // 1/hour
  "risk.resume_trading":    { maxInvocations: 3, windowMs: 3_600_000 },   // 3/hour
  "promote.to_production":  { maxInvocations: 1, windowMs: 86_400_000 },  // 1/day
  "execution.promote_live": { maxInvocations: 1, windowMs: 86_400_000 },  // 1/day
};

export const VALID_ROLES: ReadonlySet<string> = new Set<string>([
  "intern", "researcher", "risk_officer", "pm", "managing_partner",
]);

export function isValidRole(role: string): role is Role {
  return VALID_ROLES.has(role);
}
