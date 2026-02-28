import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { tools } from "./tools/registry.js";
import { type Role, isValidRole, canInvoke, deniedMessage, checkRateLimit } from "./auth/index.js";

const DEFAULT_ROLE: Role = "intern";

// Extract invoker role from MCP request _meta
function extractRole(meta: Record<string, unknown> | undefined): Role {
  const role = meta?.invoker_role;
  if (typeof role === "string" && isValidRole(role)) return role;
  return DEFAULT_ROLE;
}

function extractAgentId(meta: Record<string, unknown> | undefined): string {
  const id = meta?.agent_id;
  return typeof id === "string" ? id : "unknown";
}

const server = new Server(
  { name: "yats", version: "0.1.0" },
  { capabilities: { tools: {} } }
);

// List all registered tools
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: Array.from(tools.values()).map((t) => ({
    name: t.name,
    description: t.description,
    inputSchema: t.inputSchema,
  })),
}));

// Dispatch tool calls to handlers with permission + rate limit checks
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args, _meta } = request.params;
  const meta = _meta as Record<string, unknown> | undefined;
  const role = extractRole(meta);
  const agentId = extractAgentId(meta);

  // 1. Check tool exists
  const tool = tools.get(name);
  if (!tool) {
    return {
      content: [{ type: "text" as const, text: `Unknown tool: ${name}` }],
      isError: true,
    };
  }

  // 2. Check role-based permission
  if (!canInvoke(role, name)) {
    return {
      content: [{ type: "text" as const, text: deniedMessage(role, name) }],
      isError: true,
    };
  }

  // 3. Check rate limits
  const rateCheck = checkRateLimit(role, agentId, name);
  if (!rateCheck.allowed) {
    return {
      content: [{ type: "text" as const, text: rateCheck.message! }],
      isError: true,
    };
  }

  // 4. Pass role through to handler via args (for tools that need it, e.g. data.query)
  const argsWithRole = { ...(args ?? {}), _invoker_role: role };
  const result = await tool.handler(argsWithRole);
  return {
    content: result.content.map((c) => ({ type: "text" as const, text: c.text })),
    isError: result.isError,
  };
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch(console.error);
