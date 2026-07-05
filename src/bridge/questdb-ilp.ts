// QuestDB ILP (InfluxDB Line Protocol) writer for append-only audit trail
import { Sender } from "@questdb/nodejs-client";

let sender: Sender | null = null;
let connecting = false;

const ILP_HOST = process.env.QUESTDB_ILP_HOST ?? "localhost";
const ILP_PORT = process.env.QUESTDB_ILP_PORT ?? "9009";

async function getSender(): Promise<Sender> {
  if (sender) return sender;
  if (connecting) {
    // Wait briefly for in-flight connection
    await new Promise((r) => setTimeout(r, 100));
    if (sender) return sender;
  }
  connecting = true;
  try {
    const s = Sender.fromConfig(`tcp::addr=${ILP_HOST}:${ILP_PORT};`);
    await s.connect();
    sender = s;
    return s;
  } finally {
    connecting = false;
  }
}

export interface AuditRow {
  tool_name: string;
  invoker: string;
  experiment_id?: string | null;
  mode?: string | null;
  parameters: string; // JSON string
  result_status: "success" | "failure" | "timeout";
  result_summary: string; // JSON string
  duration_ms: number;
  dagster_run_id?: string | null;
  quanttown_molecule_id?: string | null;
  quanttown_bead_id?: string | null;
}

export async function writeAuditRow(row: AuditRow): Promise<void> {
  const s = await getSender();
  s.table("audit_trail")
    .symbol("tool_name", row.tool_name)
    .symbol("invoker", row.invoker)
    .symbol("result_status", row.result_status);

  if (row.experiment_id) s.symbol("experiment_id", row.experiment_id);
  if (row.mode) s.symbol("mode", row.mode);

  s.stringColumn("parameters", row.parameters)
    .stringColumn("result_summary", row.result_summary)
    .intColumn("duration_ms", row.duration_ms);

  if (row.dagster_run_id) s.stringColumn("dagster_run_id", row.dagster_run_id);
  if (row.quanttown_molecule_id) s.stringColumn("quanttown_molecule_id", row.quanttown_molecule_id);
  if (row.quanttown_bead_id) s.stringColumn("quanttown_bead_id", row.quanttown_bead_id);

  await s.atNow();
  await s.flush();
}

export async function closeIlp(): Promise<void> {
  if (sender) {
    await sender.close();
    sender = null;
  }
}
