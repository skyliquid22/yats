// qualify.run — Run qualification for experiment vs baseline via Dagster pipeline
import { DagsterClient } from "../../bridge/dagster-client.js";
import { ok, err, type ToolDef } from "../../types/tools.js";

export const qualifyRun: ToolDef = {
  name: "qualify.run_v1",
  description:
    "Run qualification pipeline for an experiment against a baseline. Evaluates hard gates (regression, constraints, regime) and soft gates per PRD Appendix D.1.",
  inputSchema: {
    type: "object",
    properties: {
      candidate_id: { type: "string", description: "Candidate experiment ID to qualify" },
      baseline_id: { type: "string", description: "Baseline experiment ID to compare against" },
    },
    required: ["candidate_id", "baseline_id"],
  },
  async handler(args) {
    const candidateId = args.candidate_id as string;
    const baselineId = args.baseline_id as string;

    const dagster = new DagsterClient();
    try {
      const runId = await dagster.launchRun("qualify", {
        ops: {
          qualify: {
            config: {
              candidate_id: candidateId,
              baseline_id: baselineId,
            },
          },
        },
      });
      return ok({ run_id: runId, candidate_id: candidateId, baseline_id: baselineId, job: "qualify" });
    } catch (e) {
      return err(`Failed to launch qualification: ${e instanceof Error ? e.message : String(e)}`);
    }
  },
};
