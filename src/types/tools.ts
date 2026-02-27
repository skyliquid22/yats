// Tool input/output types
export interface ToolResult {
  content: Array<{ type: string; text: string }>;
  isError?: boolean;
}
