// data.canonicalize — Run reconciliation for domain + date range
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

const VALID_DOMAINS = ["equity_ohlcv", "fundamentals", "financial_metrics"];

export const dataCanonicalize: ToolDef = {
  name: "data.canonicalize_v1",
  description: "Run the canonicalization pipeline for one or more data domains over a date range.",
  inputSchema: {
    type: "object",
    properties: {
      domains: {
        type: "array",
        items: { type: "string", enum: VALID_DOMAINS },
        description: "Domains to canonicalize (default: all)",
      },
      start_date: { type: "string", description: "Start date ISO-8601 (optional)" },
      end_date: { type: "string", description: "End date ISO-8601 (optional)" },
    },
    required: [],
  },
  async handler(args) {
    const domains = (args.domains as string[] | undefined) ?? VALID_DOMAINS;
    const startDate = (args.start_date as string | undefined) ?? "";
    const endDate = (args.end_date as string | undefined) ?? "";

    for (const d of domains) {
      if (!VALID_DOMAINS.includes(d)) {
        return err(`Invalid domain: ${d}. Valid: ${VALID_DOMAINS.join(", ")}`);
      }
    }

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("canonicalize", {
        ops: {
          canonicalize_op: {
            config: {
              domains,
              start_date: startDate,
              end_date: endDate,
            },
          },
        },
      });
      return ok({ run_id: runId, job: "canonicalize", domains, start_date: startDate || "(all)", end_date: endDate || "(all)" });
    } catch (e) {
      return err(`Failed to launch canonicalize: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
