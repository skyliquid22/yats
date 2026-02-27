// Python subprocess execution bridge
import { spawn } from "child_process";

export interface PythonResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export class PythonRunner {
  constructor(private readonly pythonPath: string = "python3") {}

  async run(script: string, args: string[] = [], timeoutMs: number = 60_000): Promise<PythonResult> {
    return new Promise((resolve, reject) => {
      const proc = spawn(this.pythonPath, [script, ...args], {
        cwd: process.cwd(),
        env: { ...process.env },
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stdout = "";
      let stderr = "";
      let timedOut = false;

      const timer = setTimeout(() => {
        timedOut = true;
        proc.kill("SIGTERM");
      }, timeoutMs);

      proc.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
      proc.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });

      proc.on("close", (code) => {
        clearTimeout(timer);
        if (timedOut) {
          reject(new Error(`Python script timed out after ${timeoutMs}ms`));
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

  async runModule(module: string, args: string[] = [], timeoutMs: number = 60_000): Promise<PythonResult> {
    return new Promise((resolve, reject) => {
      const proc = spawn(this.pythonPath, ["-m", module, ...args], {
        cwd: process.cwd(),
        env: { ...process.env },
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stdout = "";
      let stderr = "";
      let timedOut = false;

      const timer = setTimeout(() => {
        timedOut = true;
        proc.kill("SIGTERM");
      }, timeoutMs);

      proc.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
      proc.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });

      proc.on("close", (code) => {
        clearTimeout(timer);
        if (timedOut) {
          reject(new Error(`Python module timed out after ${timeoutMs}ms`));
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
}
