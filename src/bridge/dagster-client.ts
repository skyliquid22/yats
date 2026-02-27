// Dagster GraphQL client
export class DagsterClient {
  constructor(private readonly url: string = "http://localhost:3000/graphql") {}
}
