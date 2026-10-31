import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { PaperclipPluginManifestV1 } from "@paperclipai/shared";
import { createPluginWorkerManager } from "../plugin-worker-manager.js";
import type { NativeExecutionInput } from "../../vendor/paperclip-runner/index.js";
import { NativeExecutionContext } from "./native-execution-context.js";

const execution = { binding: { companyId: "company", agentId: "agent", runId: "run" } } as NativeExecutionInput;
const manifest: PaperclipPluginManifestV1 = {
  id: "test.execution-context", apiVersion: 1, version: "1.0.0", displayName: "Execution context fixture",
  description: "Execution context fixture", author: "Paperclip", categories: ["automation"], capabilities: [],
  entrypoints: { worker: "worker.cjs" },
};
const params = {
  companyId: "company", agentId: "agent", runId: "run", environmentId: "environment", issueId: "issue",
  driverKey: "tasks", taskId: "task", config: {}, projectIds: [],
  lease: { providerLeaseId: "task" }, operation: { kind: "describe" as const },
};

let directory: string;
let entrypointPath: string;
beforeAll(async () => {
  directory = await mkdtemp(path.join(tmpdir(), "paperclip-startup-worker-"));
  entrypointPath = path.join(directory, "worker.cjs");
  await writeFile(entrypointPath, `
const readline = require("node:readline");
const held = [];
const send = (id, result) => process.stdout.write(JSON.stringify({jsonrpc:"2.0",id,result})+"\\n");
readline.createInterface({input:process.stdin,crlfDelay:Infinity}).on("line", line => {
  const message = JSON.parse(line);
  if (message.method === "initialize") send(message.id, {ok:true,supportedMethods:["environmentTask","configChanged","health"]});
  else if (message.method === "environmentTask") held.push(message.id);
  else if (message.method === "health") send(message.id, {status:"ok",details:{pending:held.length}});
  else if (message.method === "configChanged") {
    if (message.params.config.release) for (const id of held.splice(0)) send(id, {kind:"description",taskId:"task"});
    send(message.id, {});
  } else if (message.method === "shutdown") {send(message.id, {});setImmediate(()=>process.exit(0));}
});
`, { mode: 0o600 });
});
afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

async function startWorker() {
  const manager = createPluginWorkerManager();
  await manager.startWorker(manifest.id, { entrypointPath, manifest, config: {},
    instanceInfo: { instanceId: "instance", hostVersion: "1.0.0" }, apiVersion: 1, hostHandlers: {} });
  return manager;
}

describe("Core startup responsibility across real worker IPC", () => {
  it("keeps a timed-out caller pending until the actual late worker reply", async () => {
    const manager = await startWorker();
    const context = new NativeExecutionContext(execution, 5_000);
    const settled = vi.fn(context.trackWorker());
    try {
      await expect(manager.call(manifest.id, "environmentTask", params, 10, undefined, settled)).rejects.toThrow("timed out");
      expect(context.pending).toBe(true);
      expect(settled).not.toHaveBeenCalled();
      context.abort(new Error("Stop requested"));
      let drained = false;
      const drain = context.drain().then(result => { drained = true; return result; });
      expect(await manager.call(manifest.id, "health", {})).toMatchObject({ details: { pending: 1 } });
      expect(drained).toBe(false);
      await manager.call(manifest.id, "configChanged", { companyId: "company", config: { release: true } });
      expect(await drain).toBe(true);
      expect(context.pending).toBe(false);
      expect(settled).toHaveBeenCalledOnce();
      expect(() => context.assertOpen()).toThrow("Stop requested");
    } finally { context.finish(); await manager.stopAll(); }
  });

  it("does not treat the startup deadline as a worker completion receipt", async () => {
    const manager = await startWorker();
    const context = new NativeExecutionContext(execution, 100);
    const settled = vi.fn(context.trackWorker());
    try {
      await expect(manager.call(manifest.id, "environmentTask", params, 10, undefined, settled)).rejects.toThrow("timed out");
      if (!context.signal.aborted) await new Promise<void>(resolve => context.signal.addEventListener("abort", () => resolve(), { once: true }));
      expect(await context.drain()).toBe(false);
      expect(context.pending).toBe(true);
      expect(settled).not.toHaveBeenCalled();
      expect(await manager.call(manifest.id, "health", {})).toMatchObject({ details: { pending: 1 } });
      await manager.call(manifest.id, "configChanged", { companyId: "company", config: { release: true } });
      expect(settled).toHaveBeenCalledOnce();
      expect(context.pending).toBe(false);
      expect(await context.drain()).toBe(true);
      expect(() => context.assertOpen()).toThrow("startup_expired");
    } finally { context.finish(); await manager.stopAll(); }
  });
});
