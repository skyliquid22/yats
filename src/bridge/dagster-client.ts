// Dagster GraphQL client — triggers pipeline jobs and checks run status
import { createHash } from "crypto";

export class DagsterClient {
  constructor(private readonly url: string = "http://localhost:3000/graphql") {}

  // Canonical JSON serialization: recursively sorts object keys so that
  // {a:1,b:2} and {b:2,a:1} produce the same string. Arrays preserve order.
  // Using Array.isArray replacer would silently drop nested keys — this is correct.
  private static canonicalize(val: unknown): string {
    if (val === null || typeof val !== "object" || Array.isArray(val)) {
      return JSON.stringify(val);
    }
    const obj = val as Record<string, unknown>;
    const parts = Object.keys(obj)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${DagsterClient.canonicalize(obj[k])}`);
    return `{${parts.join(",")}}`;
  }

  /**
   * Compute a deterministic run ID from job name, run config, and invoker.
   * Retrying the same call (same params) produces the same run ID, preventing
   * duplicate rows from partial-retry scenarios (PRD §20.3).
   */
  makeRunId(jobName: string, runConfig: Record<string, unknown>, invoker: string): string {
    const stable = DagsterClient.canonicalize({ invoker, jobName, runConfig });
    return createHash("sha256").update(stable).digest("hex").slice(0, 32);
  }

  async launchRun(
    jobName: string,
    runConfig: Record<string, unknown> = {},
    invoker: string = "mcp",
  ): Promise<string> {
    const deterministicRunId = this.makeRunId(jobName, runConfig, invoker);

    const mutation = `
      mutation LaunchRun($executionParams: ExecutionParams!) {
        launchRun(executionParams: $executionParams) {
          __typename
          ... on LaunchRunSuccess {
            run { runId }
          }
          ... on PythonError {
            message
          }
          ... on RunConfigValidationInvalid {
            errors { message }
          }
        }
      }
    `;

    const variables = {
      executionParams: {
        selector: { jobName, repositoryLocationName: "yats_pipelines", repositoryName: "__repository__" },
        runConfigData: JSON.stringify(runConfig),
        executionMetadata: { runId: deterministicRunId },
      },
    };

    const result = await this.gql(mutation, variables);
    const launch = result.data?.launchRun;

    if (launch?.__typename === "LaunchRunSuccess") {
      return launch.run.runId;
    }

    const errMsg = launch?.message ?? launch?.errors?.map((e: { message: string }) => e.message).join("; ") ?? "Unknown error";
    throw new Error(`Dagster launchRun failed: ${errMsg}`);
  }

  async getRunStatus(runId: string): Promise<{ status: string; endTime: number | null }> {
    const query = `
      query RunStatus($runId: ID!) {
        runOrError(runId: $runId) {
          __typename
          ... on Run {
            status
            endTime
          }
          ... on RunNotFoundError {
            message
          }
        }
      }
    `;

    const result = await this.gql(query, { runId });
    const run = result.data?.runOrError;

    if (run?.__typename === "Run") {
      return { status: run.status, endTime: run.endTime };
    }

    throw new Error(`Run not found: ${runId}`);
  }

  async listSuccessRuns(limit: number = 50): Promise<{ runId: string; jobName: string }[]> {
    const query = `
      query ListSuccessRuns($filter: RunsFilter, $limit: Int) {
        runsOrError(filter: $filter, limit: $limit) {
          __typename
          ... on Runs {
            results {
              runId
              jobName
            }
          }
          ... on PythonError {
            message
          }
        }
      }
    `;

    const result = await this.gql(query, {
      filter: { statuses: ["SUCCESS"] },
      limit,
    });
    const runsOrError = result.data?.runsOrError;

    if (runsOrError?.__typename === "Runs") {
      return (runsOrError.results as { runId: string; jobName: string }[]) ?? [];
    }

    const errMsg = (runsOrError as { message?: string })?.message ?? `unexpected type ${runsOrError?.__typename}`;
    throw new Error(`Dagster listSuccessRuns failed: ${errMsg}`);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async gql(query: string, variables: Record<string, unknown>): Promise<any> {
    const res = await fetch(this.url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
    });

    if (!res.ok) {
      throw new Error(`Dagster GraphQL HTTP error: ${res.status} ${res.statusText}`);
    }

    const json = (await res.json()) as { data?: Record<string, unknown>; errors?: { message: string }[] };
    if (json.errors?.length) {
      throw new Error(`Dagster GraphQL errors: ${json.errors.map((e) => e.message).join("; ")}`);
    }
    return json;
  }
}
