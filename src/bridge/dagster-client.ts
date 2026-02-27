// Dagster GraphQL client — triggers pipeline jobs and checks run status
export class DagsterClient {
  constructor(private readonly url: string = "http://localhost:3000/graphql") {}

  async launchRun(jobName: string, runConfig: Record<string, unknown> = {}): Promise<string> {
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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async gql(query: string, variables: Record<string, unknown>): Promise<any> {
    const res = await fetch(this.url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
    });

    if (!res.ok) {
      throw new Error(`Dagster GraphQL error: ${res.status} ${res.statusText}`);
    }

    return res.json() as Promise<Record<string, unknown>>;
  }
}
