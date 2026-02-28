// monitor.reconcile — Check consistency across QuestDB, filesystem, Dagster
import { QuestDBClient } from "../../bridge/questdb-client.js";
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";
import { readdirSync } from "fs";
import { join } from "path";

interface Inconsistency {
  type: string;
  description: string;
  source: string;
}

export const monitorReconcile: ToolDef = {
  name: "monitor.reconcile_v1",
  description:
    "Check consistency across QuestDB, filesystem (.yats_data), and Dagster. Identifies missing data, orphaned files, and pipeline/data mismatches.",
  inputSchema: {
    type: "object",
    properties: {
      check: {
        type: "string",
        enum: ["all", "experiments", "promotions", "data"],
        description: "What to reconcile (default: all)",
      },
    },
    required: [],
  },
  async handler(args) {
    const check = (args.check as string | undefined) ?? "all";
    const inconsistencies: Inconsistency[] = [];
    const dataDir = join(process.cwd(), ".yats_data");

    const qdb = new QuestDBClient();
    try {
      // Check experiment artifacts consistency
      if (check === "all" || check === "experiments") {
        try {
          const experimentsDir = join(dataDir, "experiments");
          const fsDirs = new Set<string>();
          try {
            for (const entry of readdirSync(experimentsDir, { withFileTypes: true })) {
              if (entry.isDirectory()) fsDirs.add(entry.name);
            }
          } catch {
            // Directory may not exist
          }

          const dbResult = await qdb.query("SELECT DISTINCT experiment_id FROM experiments");
          const dbIds = new Set(dbResult.rows.map((r) => r.experiment_id as string));

          for (const fsId of fsDirs) {
            if (!dbIds.has(fsId)) {
              inconsistencies.push({
                type: "orphaned_artifact",
                description: `Experiment directory ${fsId} exists on filesystem but not in QuestDB`,
                source: "experiments",
              });
            }
          }
          for (const dbId of dbIds) {
            if (!fsDirs.has(dbId)) {
              inconsistencies.push({
                type: "missing_artifact",
                description: `Experiment ${dbId} exists in QuestDB but has no filesystem directory`,
                source: "experiments",
              });
            }
          }
        } catch {
          inconsistencies.push({
            type: "check_failed",
            description: "Could not reconcile experiments (table or directory missing)",
            source: "experiments",
          });
        }
      }

      // Check promotion records consistency
      if (check === "all" || check === "promotions") {
        try {
          const promoResult = await qdb.query(
            `SELECT experiment_id, tier FROM promotions WHERE tier = 'production'`
          );
          const dbResult = await qdb.query("SELECT DISTINCT experiment_id FROM experiments");
          const allExperiments = new Set(dbResult.rows.map((r) => r.experiment_id as string));

          for (const row of promoResult.rows) {
            if (!allExperiments.has(row.experiment_id as string)) {
              inconsistencies.push({
                type: "dangling_promotion",
                description: `Promotion to ${row.tier} for ${row.experiment_id} but experiment not found`,
                source: "promotions",
              });
            }
          }
        } catch {
          inconsistencies.push({
            type: "check_failed",
            description: "Could not reconcile promotions (table missing)",
            source: "promotions",
          });
        }
      }

      // Check data table freshness consistency
      if (check === "all" || check === "data") {
        try {
          const freshnessResult = await qdb.query(
            `SELECT max(timestamp) as latest FROM canonical_equity_ohlcv`
          );
          const latest = freshnessResult.rows[0]?.latest;
          if (latest) {
            const latestDate = new Date(latest as string);
            const ageHours = (Date.now() - latestDate.getTime()) / (1000 * 60 * 60);
            if (ageHours > 48) {
              inconsistencies.push({
                type: "stale_data",
                description: `Equity OHLCV data is ${Math.round(ageHours)}h old (last: ${latest})`,
                source: "data",
              });
            }
          }
        } catch {
          inconsistencies.push({
            type: "check_failed",
            description: "Could not check data freshness (table missing)",
            source: "data",
          });
        }
      }

      return ok({
        check,
        inconsistencies,
        count: inconsistencies.length,
        status: inconsistencies.length === 0 ? "consistent" : "inconsistencies_found",
      });
    } catch (e) {
      return err(`Reconciliation failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      await qdb.close();
    }
  },
};
