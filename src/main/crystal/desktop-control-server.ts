import net from "node:net";
import { timingSafeEqual, createHash } from "node:crypto";
import { DEFAULT_PI_DESKTOP_PIPE, type DesktopControlRequest, type DesktopControlResponse,
  type DesktopProvisionRequest, type DesktopProvisionResult, type DesktopWorkspace,
  validateProvisionRequest } from "./desktop-control";

export interface NativeWorkspaceBridge {
  /** Native Pi Desktop main-process workspace manager, never an external git command. */
  createNativeWorktree(input: DesktopProvisionRequest): Promise<DesktopWorkspace>;
  listNativeWorkspaces(): Promise<DesktopWorkspace[]>;
  getSourceWorkspace(projectId: string, sourceWorkspaceId: string): Promise<{ id: string; repoRoot: string } | undefined>;
}
export interface DesktopControlServerOptions {
  bridge: NativeWorkspaceBridge;
  token: string;
  allowedProjects: ReadonlySet<string>;
  pipePath?: string;
  maxBytes?: number;
  /** Durable native journal. A committed creating entry must be reconciled after crashes. */
  journal: ProvisionJournal;
}
export interface ProvisionJournal {
  get(operationId: string): Promise<{ request: DesktopProvisionRequest; result: DesktopProvisionResult } | undefined>;
  putIfAbsent(request: DesktopProvisionRequest, result: DesktopProvisionResult): Promise<boolean>;
  setResult(operationId: string, result: DesktopProvisionResult): Promise<void>;
}
function signature(request: DesktopProvisionRequest): string {
  return createHash("sha256").update(JSON.stringify(request)).digest("hex");
}
function sameToken(left: string, right: string) {
  const a = Buffer.from(left), b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}
export function createPiDesktopControlServer(options: DesktopControlServerOptions): net.Server {
  if (options.token.length < 32 || options.allowedProjects.size === 0)
    throw new Error("Control server requires token and project allowlist");
  const pending = new Map<string, Promise<DesktopProvisionResult>>();
  const maxBytes = options.maxBytes ?? 65536;
  const handler = async (request: DesktopControlRequest): Promise<unknown> => {
    if (request.version !== 1 || !sameToken(String(request.token ?? ""), options.token))
      throw new Error("Unauthorized Pi Desktop control request");
    if (request.operation === "getCapabilities") return { version: 1, modelOverride: true };
    if (request.operation === "listWorkspaces") return await options.bridge.listNativeWorkspaces();
    if (request.operation === "getSourceWorkspace") {
      const payload = request.payload as { projectId?: unknown; sourceWorkspaceId?: unknown } | null;
      const projectId = payload?.projectId, sourceId = payload?.sourceWorkspaceId;
      if (typeof projectId !== "string" || !options.allowedProjects.has(projectId) ||
          typeof sourceId !== "string" || !/^[a-zA-Z0-9][a-zA-Z0-9:_.-]{0,159}$/.test(sourceId))
        throw new Error("Invalid or unauthorized source lookup");
      return await options.bridge.getSourceWorkspace(projectId, sourceId) ?? null;
    }
    if (request.operation === "getProvisionStatus") {
      const operationId = (request.payload as { operationId?: string })?.operationId;
      if (typeof operationId !== "string" || !operationId.trim()) throw new Error("Missing operation ID");
      return (await options.journal.get(operationId))?.result ?? null;
    }
    if (request.operation !== "createIsolatedTab") throw new Error("Unsupported operation");
    const input = request.payload as DesktopProvisionRequest;
    validateProvisionRequest(input);
    if (!options.allowedProjects.has(input.projectId)) throw new Error("Project not allowed");
    const existing = await options.journal.get(input.operationId);
    if (existing) {
      if (signature(existing.request) !== signature(input)) throw new Error("Operation ID reused with different request");
      return existing.result;
    }
    const inflight = pending.get(input.operationId);
    if (inflight) return await inflight;
    const operation = (async () => {
      const admitted = await options.journal.putIfAbsent(input, { state: "creating" });
      if (!admitted) {
        const concurrent = await options.journal.get(input.operationId);
        if (!concurrent || signature(concurrent.request) !== signature(input))
          throw new Error("Concurrent operation mismatch; reconcile");
        return concurrent.result;
      }
      // Native implementation must use startPi=true with NO taskPrompt;
      // Crystal's existing Messenger adapter dispatches the prompt exactly once.
      try {
        const workspace = await options.bridge.createNativeWorktree(input);
        const result: DesktopProvisionResult = { state: "created", workspace };
        await options.journal.setResult(input.operationId, result);
        return result;
      } catch {
        // An interrupted native create may have succeeded. Do NOT repeat blindly.
        await options.journal.setResult(input.operationId, { state: "unknown" });
        throw new Error("Native Pi tab creation uncertain; reconcile before retry");
      }
    })();
    pending.set(input.operationId, operation);
    try { return await operation; }
    finally { pending.delete(input.operationId); }
  };
  return net.createServer(socket => {
    let buffer = "";
    let handled = false;
    socket.setTimeout(5000, () => socket.destroy());
    socket.on("data", async chunk => {
      if (handled) return;
      buffer += chunk.toString("utf8");
      if (Buffer.byteLength(buffer) > maxBytes) { socket.destroy(); return; }
      const end = buffer.indexOf("\n");
      if (end < 0) return;
      handled = true;
      let id = "";
      let response: DesktopControlResponse;
      try {
        const request = JSON.parse(buffer.slice(0, end)) as DesktopControlRequest;
        id = typeof request.id === "string" ? request.id : "";
        const result = await handler(request);
        response = { version: 1, id, ok: true, result };
      } catch (error) {
        response = { version: 1, id, ok: false, error: (error as Error).message };
      }
      socket.end(JSON.stringify(response) + "\n");
    });
    socket.on("error", () => {});
  });
}
/** Start only inside the Pi Desktop main process, from an explicitly opted-in entry point. */
export function listenPiDesktopControl(server: net.Server, pipePath = DEFAULT_PI_DESKTOP_PIPE): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(pipePath, () => {
      server.off("error", reject);
      resolve();
    });
  });
}
