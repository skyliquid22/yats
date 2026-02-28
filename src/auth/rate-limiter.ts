// In-memory rate limiter for destructive tools — PRD Section 21.4
import { type Role, RATE_LIMITS } from "./roles.js";

// Key: "agentId:toolBase" → timestamps of recent invocations
const invocations = new Map<string, number[]>();

function stripVersion(toolName: string): string {
  return toolName.replace(/_v\d+$/, "");
}

export interface RateLimitResult {
  allowed: boolean;
  message?: string;
}

export function checkRateLimit(role: Role, agentId: string, toolName: string): RateLimitResult {
  // managing_partner is exempt from rate limits (PRD: all invocations still logged)
  if (role === "managing_partner") return { allowed: true };

  const toolBase = stripVersion(toolName);
  const rule = RATE_LIMITS[toolBase];
  if (!rule) return { allowed: true }; // no rate limit on this tool

  const key = `${agentId}:${toolBase}`;
  const now = Date.now();
  const windowStart = now - rule.windowMs;

  // Get existing timestamps and prune expired ones
  const timestamps = (invocations.get(key) ?? []).filter((t) => t > windowStart);

  if (timestamps.length >= rule.maxInvocations) {
    const windowDesc = rule.windowMs >= 86_400_000 ? "day" : "hour";
    return {
      allowed: false,
      message: `Rate limit exceeded: "${toolBase}" allows ${rule.maxInvocations} invocations per ${windowDesc} (agent: ${agentId})`,
    };
  }

  // Record this invocation
  timestamps.push(now);
  invocations.set(key, timestamps);
  return { allowed: true };
}
