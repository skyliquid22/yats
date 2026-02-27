// financialdatasets.ai API client
export class FinancialDatasetsClient {
  constructor(
    private readonly apiKey: string,
    private readonly baseUrl: string = "https://api.financialdatasets.ai"
  ) {}
}
