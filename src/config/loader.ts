// Config file loading + validation
import { readFileSync } from "fs";
import { join } from "path";

export function loadYaml(name: string): Record<string, unknown> {
  const path = join(process.cwd(), "configs", name);
  const raw = readFileSync(path, "utf-8");
  return parseSimpleYaml(raw);
}

/**
 * Minimal YAML parser for flat/nested key-value configs.
 * Handles the vendor config structure without pulling in a full YAML lib.
 */
function parseSimpleYaml(text: string): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  let currentSection: string | null = null;
  let currentObj: Record<string, unknown> = {};

  for (const line of text.split("\n")) {
    const trimmed = line.trimEnd();
    if (!trimmed || trimmed.startsWith("#")) continue;

    // Top-level key (no indent)
    if (!line.startsWith(" ") && !line.startsWith("\t") && trimmed.endsWith(":")) {
      if (currentSection !== null) {
        result[currentSection] = currentObj;
      }
      currentSection = trimmed.slice(0, -1).trim();
      currentObj = {};
      continue;
    }

    // Indented key: value
    const match = trimmed.match(/^\s+(\S+):\s*(.+)$/);
    if (match && currentSection !== null) {
      currentObj[match[1]] = match[2];
    }
  }

  if (currentSection !== null) {
    result[currentSection] = currentObj;
  }

  return result;
}
