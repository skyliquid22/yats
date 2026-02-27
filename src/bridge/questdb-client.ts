// QuestDB client (PG wire + ILP)
export class QuestDBClient {
  constructor(
    private readonly pgHost: string = "localhost",
    private readonly pgPort: number = 8812,
    private readonly ilpPort: number = 9009
  ) {}
}
