// Tool input/output types
export interface ToolResult {
  content: Array<{ type: string; text: string }>;
  isError?: boolean;
}

export function ok(data: unknown): ToolResult {
  return {
    content: [{ type: "text", text: typeof data === "string" ? data : JSON.stringify(data, null, 2) }],
  };
}

export function err(message: string): ToolResult {
  return {
    content: [{ type: "text", text: message }],
    isError: true,
  };
}

// Tool handler type
export type ToolHandler = (args: Record<string, unknown>) => Promise<ToolResult>;

// Tool definition for registry
export interface ToolDef {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  handler: ToolHandler;
}
