// monitor.health — System health checks for QuestDB, Dagster, Alpaca
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

interface ServiceCheck {
  service: string;
  status: "healthy" | "unhealthy";
  latency_ms?: number;
  error?: string;
}

async function checkQuestDB(): Promise<ServiceCheck> {
  const start = Date.now();
  const qdb = new QuestDBClient();
  try {
    await qdb.query("SELECT 1 as health_check");
    return { service: "questdb", status: "healthy", latency_ms: Date.now() - start };
  } catch (e) {
    return { service: "questdb", status: "unhealthy", latency_ms: Date.now() - start, error: e instanceof Error ? e.message : String(e) };
  } finally {
    await qdb.close();
  }
}

async function checkDagster(): Promise<ServiceCheck> {
  const start = Date.now();
  try {
    const res = await fetch("http://localhost:3000/graphql", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: "{ version }" }),
    });
    if (!res.ok) {
      return { service: "dagster", status: "unhealthy", latency_ms: Date.now() - start, error: `HTTP ${res.status}` };
    }
    return { service: "dagster", status: "healthy", latency_ms: Date.now() - start };
  } catch (e) {
    return { service: "dagster", status: "unhealthy", latency_ms: Date.now() - start, error: e instanceof Error ? e.message : String(e) };
  }
}

async function checkAlpaca(): Promise<ServiceCheck> {
  const start = Date.now();
  try {
    const res = await fetch("https://paper-api.alpaca.markets/v2/clock", {
      headers: {
        "APCA-API-KEY-ID": process.env.ALPACA_API_KEY ?? "",
        "APCA-API-SECRET-KEY": process.env.ALPACA_SECRET_KEY ?? "",
      },
    });
    if (!res.ok) {
      return { service: "alpaca", status: "unhealthy", latency_ms: Date.now() - start, error: `HTTP ${res.status}` };
    }
    return { service: "alpaca", status: "healthy", latency_ms: Date.now() - start };
  } catch (e) {
    return { service: "alpaca", status: "unhealthy", latency_ms: Date.now() - start, error: e instanceof Error ? e.message : String(e) };
  }
}

export const monitorHealth: ToolDef = {
  name: "monitor.health_v1",
  description:
    "Check system health by testing connectivity to QuestDB, Dagster, and Alpaca. Returns status and latency for each service.",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  async handler() {
    const checks = await Promise.all([checkQuestDB(), checkDagster(), checkAlpaca()]);
    const allHealthy = checks.every((c) => c.status === "healthy");
    return ok({ overall: allHealthy ? "healthy" : "degraded", services: checks });
  },
};
