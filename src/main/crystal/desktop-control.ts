import net from "node:net";
import { randomUUID } from "node:crypto";

export const PI_DESKTOP_CONTROL_VERSION = 1;
export const DEFAULT_PI_DESKTOP_PIPE = String.raw`\\.\pipe\crystal-pi-desktop-v1`;
export type DesktopOperation = "listWorkspaces" | "getProvisionStatus" | "createIsolatedTab" | "getSourceWorkspace";
export interface DesktopSourceWorkspace { id: string; repoRoot: string; }
export interface DesktopWorkspace {
  id: string;
  path: string;
  branch: string;
  sourceWorkspaceId: string;
  operationId: string;
}
export interface DesktopProvisionRequest {
  operationId: string;
  sourceWorkspaceId: string;
  projectId: string;
  name: string;
}
export interface DesktopProvisionResult {
  state: "created" | "creating" | "unknown";
  workspace?: DesktopWorkspace;
}
export interface DesktopControlRequest {
  version: 1;
  id: string;
  token: string;
  operation: DesktopOperation;
  payload: unknown;
}
export interface DesktopControlResponse {
  version: 1;
  id: string;
  ok: boolean;
  result?: unknown;
  error?: string;
}
export interface DesktopControlOptions {
  pipePath?: string;
  token: string;
  timeoutMs?: number;
  maxBytes?: number;
  connect?: (path: string) => net.Socket;
}

function nonempty(v: unknown): v is string {
  return typeof v === "string" && v.trim().length > 0;
}
export function validateProvisionRequest(value: DesktopProvisionRequest): void {
  if (!value || !nonempty(value.operationId) || !nonempty(value.sourceWorkspaceId) ||
      !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value.projectId) ||
      !nonempty(value.name) || value.name.length > 120 || value.operationId.length > 160) {
    throw new TypeError("Invalid Pi Desktop provisioning request");
  }
}
function parseResponse(raw: string, id: string): DesktopControlResponse {
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new Error("Malformed Pi Desktop control response"); }
  const response = value as DesktopControlResponse;
  if (!response || response.version !== 1 || response.id !== id ||
      typeof response.ok !== "boolean") throw new Error("Uncorrelated Pi Desktop control response");
  if (!response.ok) throw new Error("Pi Desktop control rejected request: " + String(response.error ?? "unknown"));
  return response;
}

/** This is not controller authorization. Only an authorized Crystal controller can call this client. */
export class PiDesktopControlClient {
  private readonly options: Required<Pick<DesktopControlOptions, "pipePath" | "timeoutMs" | "maxBytes">> & DesktopControlOptions;
  constructor(options: DesktopControlOptions) {
    if (!nonempty(options.token) || options.token.length < 32)
      throw new TypeError("Pi Desktop control requires a strong local token");
    this.options = {
      ...options, pipePath: options.pipePath ?? DEFAULT_PI_DESKTOP_PIPE,
      timeoutMs: options.timeoutMs ?? 5000, maxBytes: options.maxBytes ?? 65536,
    };
    if (this.options.timeoutMs < 1 || this.options.maxBytes < 1024)
      throw new RangeError("Invalid Pi Desktop control limits");
  }
  async call(operation: DesktopOperation, payload: unknown): Promise<unknown> {
    const id = randomUUID();
    const request: DesktopControlRequest = {
      version: 1, id, token: this.options.token, operation, payload,
    };
    return await new Promise<unknown>((resolve, reject) => {
      let done = false;
      let buffer = "";
      const socket = this.options.connect?.(this.options.pipePath) ?? net.createConnection(this.options.pipePath);
      const finish = (error?: Error, value?: unknown) => {
        if (done) return;
        done = true;
        socket.destroy();
        if (error) reject(error); else resolve(value);
      };
      socket.setTimeout(this.options.timeoutMs, () => finish(new Error("Pi Desktop control timeout; reconcile operationId before retry")));
      socket.once("error", (e) => finish(new Error("Pi Desktop control unavailable: " + e.message)));
      socket.once("connect", () => socket.write(JSON.stringify(request) + "\n"));
      socket.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf8");
        if (Buffer.byteLength(buffer) > this.options.maxBytes)
          return finish(new Error("Pi Desktop control response too large"));
        const index = buffer.indexOf("\n");
        if (index >= 0) {
          try { finish(undefined, parseResponse(buffer.slice(0, index), id).result); }
          catch (error) { finish(error as Error); }
        }
      });
      socket.once("end", () => finish(new Error("Pi Desktop control connection closed without response")));
    });
  }
  async getProvisionStatus(operationId: string): Promise<DesktopProvisionResult | undefined> {
    if (!nonempty(operationId)) throw new TypeError("operationId required");
    const result = await this.call("getProvisionStatus", { operationId });
    return result === null ? undefined : result as DesktopProvisionResult;
  }
  async createIsolatedTab(input: DesktopProvisionRequest): Promise<DesktopProvisionResult> {
    validateProvisionRequest(input);
    return await this.call("createIsolatedTab", input) as DesktopProvisionResult;
  }
  async listWorkspaces(): Promise<DesktopWorkspace[]> {
    return await this.call("listWorkspaces", {}) as DesktopWorkspace[];
  }
  async getSourceWorkspace(projectId: string, sourceWorkspaceId: string): Promise<DesktopSourceWorkspace | undefined> {
    if (!/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(projectId) ||
        !/^[a-zA-Z0-9][a-zA-Z0-9:_.-]{0,159}$/.test(sourceWorkspaceId))
      throw new TypeError("Invalid project-bound source workspace lookup");
    const result = await this.call("getSourceWorkspace", { projectId, sourceWorkspaceId });
    return result === null ? undefined : result as DesktopSourceWorkspace;
  }
}

/** Stable across controller restarts, not regenerated on each network retry. */
export function desktopOperationId(attemptId: string): string {
  if (!nonempty(attemptId)) throw new TypeError("attemptId required");
  return "crystal-tab:" + attemptId;
}

export class PiDesktopWorkspaceProvisioner {
  private readonly client: PiDesktopControlClient;
  constructor(client: PiDesktopControlClient) { this.client = client; }
  async provision(request: DesktopProvisionRequest): Promise<DesktopWorkspace> {
    validateProvisionRequest(request);
    // Query before create. The native side MUST also deduplicate atomically.
    const previous = await this.client.getProvisionStatus(request.operationId);
    const result = previous ?? await this.client.createIsolatedTab(request);
    if (result.state !== "created" || !result.workspace)
      throw new Error("Pi Desktop creation is unconfirmed; reconcile operationId without creating another tab");
    const workspace = result.workspace;
    if (workspace.operationId !== request.operationId ||
        workspace.sourceWorkspaceId !== request.sourceWorkspaceId ||
        !nonempty(workspace.id) || !nonempty(workspace.path) || !nonempty(workspace.branch))
      throw new Error("Pi Desktop returned unrelated workspace");
    return workspace;
  }
}
