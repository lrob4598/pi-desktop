import fs from "node:fs/promises";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import type { DesktopProvisionRequest, DesktopProvisionResult } from "./desktop-control";
import type { ProvisionJournal } from "./desktop-control-server";

interface Entry { request: DesktopProvisionRequest; result: DesktopProvisionResult; }

/**
 * Fail-closed journal for the Pi Desktop main process.
 * The first intent is persisted before calling Pi's workspace manager.
 * Never clear unknown/creating intents automatically.
 */
export class FileProvisionJournal implements ProvisionJournal {
  private readonly directory: string;
  constructor(directory: string) {
    if (!path.isAbsolute(directory)) throw new TypeError("Journal path must be absolute");
    this.directory = directory;
  }
  private filename(operationId: string) {
    return path.join(this.directory, createHash("sha256").update(operationId).digest("hex") + ".json");
  }
  async get(operationId: string): Promise<Entry | undefined> {
    try {
      const raw = await fs.readFile(this.filename(operationId), "utf8");
      const entry = JSON.parse(raw) as Entry;
      if (entry.request?.operationId !== operationId) throw new Error("Provision journal mismatch");
      return entry;
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException | null)?.code === "ENOENT") return undefined;
      throw e;
    }
  }
  async listCreated(): Promise<Entry[]> {
    let files: string[];
    try { files = await fs.readdir(this.directory); }
    catch (error: unknown) {
      if ((error as NodeJS.ErrnoException | null)?.code === "ENOENT") return [];
      throw error;
    }
    const entries: Entry[] = [];
    for (const file of files.filter(name => /^[a-f0-9]{64}\.json$/.test(name))) {
      const entry = JSON.parse(await fs.readFile(path.join(this.directory, file), "utf8")) as Entry;
      if (entry.result?.state === "created" && entry.result.workspace) entries.push(entry);
    }
    return entries;
  }
  async putIfAbsent(request: DesktopProvisionRequest, result: DesktopProvisionResult) {
    await fs.mkdir(this.directory, { recursive: true });
    try {
      await fs.writeFile(this.filename(request.operationId), JSON.stringify({ request, result }), { flag: "wx", mode: 0o600 });
      return true;
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException | null)?.code === "EEXIST") return false;
      throw e;
    }
  }
  async setResult(operationId: string, result: DesktopProvisionResult) {
    const previous = await this.get(operationId);
    if (!previous) throw new Error("Provision intent not recorded");
    const filename = this.filename(operationId);
    const temporary = filename + "." + randomUUID() + ".tmp";
    await fs.writeFile(temporary, JSON.stringify({ request: previous.request, result }), { flag: "wx", mode: 0o600 });
    try { await fs.rename(temporary, filename); }
    catch (error) {
      await fs.unlink(temporary).catch(() => {});
      throw error;
    }
  }
}
