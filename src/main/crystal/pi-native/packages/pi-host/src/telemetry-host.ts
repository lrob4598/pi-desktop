import { attachPiDesktopTelemetry, type ObservableWorkspaceManager } from './pi-desktop-telemetry-adapter.ts'
import { PiTelemetryStore } from './telemetry-store.ts'
import { createPiNativeTelemetryHub, type PiNativeTelemetryHub } from './telemetry-hub.ts'
import {
  DEFAULT_TELEMETRY_PIPE, startPiNativeTelemetryPipe,
  type TelemetryCredential,
} from './telemetry-pipe.ts'

export interface PiNativeTelemetryHostOptions {
  /** Existing Pi Desktop main-process manager; no new Pi process is started. */
  source: ObservableWorkspaceManager
  credentials: readonly TelemetryCredential[]
  pipePath?: string
  store?: PiTelemetryStore
  /**
   * Host-only correlation from Crystal's authorized provision journal.
   * Must bind exact native workspace/runtime IDs; never derive them from prompts.
   */
  bindTrustedCorrelations?: (store: PiTelemetryStore) => void | Promise<void>
}

export interface PiNativeTelemetryHost {
  readonly store: PiTelemetryStore
  readonly hub: PiNativeTelemetryHub
  readonly pipePath: string
  refresh(): void
  close(): Promise<void>
}

/**
 * Composition root for a future Pi Native/Electron main process. A separate,
 * read-only local telemetry pipe leaves the existing Crystal control pipe and
 * all Pi sessions untouched. The host supplies project bindings and secrets.
 */
export async function startPiNativeTelemetryHost(
  options: PiNativeTelemetryHostOptions,
): Promise<PiNativeTelemetryHost> {
  const store = options.store ?? new PiTelemetryStore()
  const observer = attachPiDesktopTelemetry(options.source, store)
  try {
    await options.bindTrustedCorrelations?.(store)
    const hub = createPiNativeTelemetryHub(store, { refresh: observer.refresh })
    const pipe = await startPiNativeTelemetryPipe({
      path: options.pipePath ?? DEFAULT_TELEMETRY_PIPE,
      hub,
      credentials: options.credentials,
    })
    let closed = false
    return {
      store,
      hub,
      pipePath: pipe.path,
      refresh: observer.refresh,
      async close() {
        if (closed) return
        closed = true
        observer.detach()
        await pipe.close()
      },
    }
  } catch (error) {
    observer.detach()
    throw error
  }
}
