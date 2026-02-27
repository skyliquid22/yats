// Risk config, decision types
export interface RiskConfig {
  dailyLossLimit: number;
  trailingDrawdownLimit: number;
  maxGrossExposure: number;
}
