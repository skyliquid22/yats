// monitor.pipeline_status — Dagster pipeline run statuses via GraphQL
import { ok, err, type ToolDef } from "../../types/tools.js";

export const monitorPipelineStatus: ToolDef = {
  name: "monitor.pipeline_status_v1",
  description:
    "Query Dagster for recent pipeline run statuses. Optionally filter by job name or status.",
  inputSchema: {
    type: "object",
    properties: {
      job_name: { type: "string", description: "Filter by Dagster job name (optional)" },
      status: {
        type: "string",
        enum: ["STARTED", "SUCCESS", "FAILURE", "CANCELED", "QUEUED", "NOT_STARTED"],
        description: "Filter by run status (optional)",
      },
      limit: { type: "number", description: "Max runs to return (default: 20, max: 100)", minimum: 1, maximum: 100 },
    },
    required: [],
  },
  async handler(args) {
    const jobName = args.job_name as string | undefined;
    const status = args.status as string | undefined;
    const limit = Math.min((args.limit as number | undefined) ?? 20, 100);

    const filterParts: string[] = [];
    if (jobName) filterParts.push(`pipelineName: "${jobName}"`);
    if (status) filterParts.push(`statuses: [${status}]`);
    const filter = filterParts.length > 0 ? `filter: { ${filterParts.join(", ")} }` : "";

    const query = `
      query PipelineRuns {
        runsOrError(${filter} limit: ${limit}) {
          __typename
          ... on Runs {
            results {
              runId
              jobName
              status
              startTime
              endTime
            }
          }
          ... on PythonError {
            message
          }
        }
      }
    `;

    try {
      const res = await fetch("http://localhost:3000/graphql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });

      if (!res.ok) {
        return err(`Dagster GraphQL error: ${res.status} ${res.statusText}`);
      }

      const data = (await res.json()) as Record<string, unknown>;
      const runsOrError = (data as { data?: { runsOrError?: Record<string, unknown> } }).data?.runsOrError;

      if (runsOrError?.__typename === "PythonError") {
        return err(`Dagster error: ${runsOrError.message}`);
      }

      const results = (runsOrError as { results?: unknown[] })?.results ?? [];
      return ok({ runs: results, count: results.length });
    } catch (e) {
      return err(`Failed to query pipeline status: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
