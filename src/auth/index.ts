// Auth module barrel export
export { type Role, isValidRole, ROLE_PERMISSIONS, TABLE_WHITELISTS, RATE_LIMITS } from "./roles.js";
export { canInvoke, deniedMessage } from "./permissions.js";
export { checkRateLimit, type RateLimitResult } from "./rate-limiter.js";
export { checkTableAccess, tableDeniedMessage, extractTableNames } from "./sql-safety.js";
