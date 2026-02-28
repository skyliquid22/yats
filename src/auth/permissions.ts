// Permission checking — validates role against tool invocations
import { type Role, ROLE_PERMISSIONS } from "./roles.js";

// Strip version suffix from tool name: "data.query_v1" → "data.query"
function stripVersion(toolName: string): string {
  return toolName.replace(/_v\d+$/, "");
}

// Check if a tool name matches a permission pattern.
// Patterns: exact match ("data.query") or prefix glob ("registry.*")
function matchesPattern(toolBase: string, pattern: string): boolean {
  if (pattern === "*") return true;
  if (pattern.endsWith(".*")) {
    const prefix = pattern.slice(0, -2); // "registry.*" → "registry"
    return toolBase.startsWith(prefix + ".");
  }
  return toolBase === pattern;
}

export function canInvoke(role: Role, toolName: string): boolean {
  const toolBase = stripVersion(toolName);
  const allowed = ROLE_PERMISSIONS[role];
  return allowed.some((pattern) => matchesPattern(toolBase, pattern));
}

export function deniedMessage(role: Role, toolName: string): string {
  return `Permission denied: role "${role}" cannot invoke "${toolName}"`;
}
