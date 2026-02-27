// Alpaca Data + Trading API client
export class AlpacaClient {
  constructor(
    private readonly apiKey: string,
    private readonly apiSecret: string,
    private readonly baseUrl: string = "https://paper-api.alpaca.markets"
  ) {}
}
