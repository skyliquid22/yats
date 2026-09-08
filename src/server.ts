import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { tools } from "./tools/registry.js";
import { handleToolCall } from "./dispatch.js";

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

// Dispatch tool calls through the auth + audit pipeline (see dispatch.ts)
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args, _meta } = request.params;
  const meta = _meta as Record<string, unknown> | undefined;

  const { result } = await handleToolCall(name, args, meta);

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
