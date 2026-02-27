// Order, position, signal types
export interface Order {
  id: string;
  symbol: string;
  side: "buy" | "sell";
  qty: number;
}
