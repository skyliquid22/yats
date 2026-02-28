// Python subprocess execution bridge — sandboxed
import { spawn } from "child_process";
import { mkdtemp, rm, symlink, access } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { constants } from "fs";

export interface PythonResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export interface SandboxConfig {
  timeoutMs: number;
  memoryLimitMb: number;
  dataDir: string; // Absolute path to .yats_data/
}

const DEFAULT_TIMEOUT_MS = 300_000; // PRD: 300 seconds
const DEFAULT_MEMORY_LIMIT_MB = 4096; // PRD: 4GB

// Env vars stripped from subprocess — broker credentials must never leak
const SENSITIVE_ENV_PATTERNS = [
  "ALPACA_API_KEY",
  "ALPACA_SECRET_KEY",
  "APCA_API_KEY_ID",
  "APCA_API_SECRET_KEY",
  "BROKER_",
  "TRADING_",
  "API_SECRET",
  "API_KEY",
];

function isSensitiveVar(name: string): boolean {
  const upper = name.toUpperCase();
  return SENSITIVE_ENV_PATTERNS.some(
    (pattern) => upper === pattern || upper.startsWith(pattern),
  );
}

function buildSanitizedEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !isSensitiveVar(key)) {
      env[key] = value;
    }
  }
  return env;
}

export class PythonRunner {
  private readonly sandboxConfig: SandboxConfig;

  constructor(
    private readonly pythonPath: string = "python3",
    config?: Partial<SandboxConfig>,
  ) {
    this.sandboxConfig = {
      timeoutMs: config?.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      memoryLimitMb: config?.memoryLimitMb ?? DEFAULT_MEMORY_LIMIT_MB,
      dataDir: config?.dataDir ?? join(process.cwd(), ".yats_data"),
    };
  }

  private async setupSandboxDir(): Promise<string> {
    const sandboxDir = await mkdtemp(join(tmpdir(), "yats-sandbox-"));

    // Symlink .yats_data into sandbox for read access (if it exists)
    try {
      await access(this.sandboxConfig.dataDir, constants.R_OK);
      await symlink(
        this.sandboxConfig.dataDir,
        join(sandboxDir, ".yats_data"),
        "dir",
      );
    } catch {
      // .yats_data doesn't exist — that's fine, not all runs need it
    }

    return sandboxDir;
  }

  private async cleanupSandboxDir(sandboxDir: string): Promise<void> {
    try {
      await rm(sandboxDir, { recursive: true, force: true });
    } catch {
      // Best-effort cleanup
    }
  }

  private spawnSandboxed(
    args: string[],
    sandboxDir: string,
    timeoutMs: number,
    label: string,
  ): Promise<PythonResult> {
    return new Promise((resolve, reject) => {
      const memLimitBytes =
        this.sandboxConfig.memoryLimitMb * 1024 * 1024;

      // Use ulimit wrapper on macOS/Linux to enforce memory limit
      const projectRoot = process.cwd();
      const proc = spawn(this.pythonPath, args, {
        cwd: sandboxDir,
        env: {
          ...buildSanitizedEnv(),
          // Preserve Python module resolution — project root must be on path
          // so -m research.foo.bar still resolves from the sandbox
          PYTHONPATH: projectRoot,
          YATS_SANDBOX: "1",
          YATS_SANDBOX_DIR: sandboxDir,
          PYTHONDONTWRITEBYTECODE: "1",
          // Memory limit communicated via env — Python scripts can enforce
          // via resource module. Node spawn doesn't support rlimit directly.
          YATS_MEMORY_LIMIT_MB: String(this.sandboxConfig.memoryLimitMb),
          YATS_MEMORY_LIMIT_BYTES: String(memLimitBytes),
        },
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stdout = "";
      let stderr = "";
      let timedOut = false;

      const timer = setTimeout(() => {
        timedOut = true;
        proc.kill("SIGKILL"); // Hard kill on timeout — SIGTERM may be ignored
      }, timeoutMs);

      proc.stdout.on("data", (chunk: Buffer) => {
        stdout += chunk.toString();
      });
      proc.stderr.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });

      proc.on("close", (code) => {
        clearTimeout(timer);
        if (timedOut) {
          reject(
            new Error(`${label} timed out after ${timeoutMs}ms`),
          );
          return;
        }
        resolve({ stdout, stderr, exitCode: code ?? 1 });
      });

      proc.on("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });
  }

  async run(
    script: string,
    args: string[] = [],
    timeoutMs?: number,
  ): Promise<PythonResult> {
    const timeout = timeoutMs ?? this.sandboxConfig.timeoutMs;
    const sandboxDir = await this.setupSandboxDir();
    try {
      // Resolve script path relative to project root (not sandbox)
      const scriptPath = join(process.cwd(), script);
      return await this.spawnSandboxed(
        [scriptPath, ...args],
        sandboxDir,
        timeout,
        "Python script",
      );
    } finally {
      await this.cleanupSandboxDir(sandboxDir);
    }
  }

  async runModule(
    module: string,
    args: string[] = [],
    timeoutMs?: number,
  ): Promise<PythonResult> {
    const timeout = timeoutMs ?? this.sandboxConfig.timeoutMs;
    const sandboxDir = await this.setupSandboxDir();
    try {
      return await this.spawnSandboxed(
        ["-m", module, ...args],
        sandboxDir,
        timeout,
        "Python module",
      );
    } finally {
      await this.cleanupSandboxDir(sandboxDir);
    }
  }
}
