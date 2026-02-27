// Config file loading + validation
import { readFileSync } from "fs";
import { join } from "path";

export function loadConfig(name: string): unknown {
  const path = join(process.cwd(), "configs", name);
  const raw = readFileSync(path, "utf-8");
  return JSON.parse(raw);
}
