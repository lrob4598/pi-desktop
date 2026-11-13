import type { PiRpcObservation, PiRuntimeObservation, PiWorkspaceObservation } from './telemetry-store.ts'
import { PiTelemetryStore } from './telemetry-store.ts'

/**
 * Narrow structural interface already implemented by upstream Pi Desktop's
 * WorkspaceManager. No imports from Electron or the renderer are required.
 */
export interface ObservablePiManager {
  on(name: 'event', listener: (event: PiRpcObservation) => void): unknown
}

export interface ObservableWorkspaceManager {
  getWorkspaces(): PiWorkspaceObservation[]
  getSessionRuntimes(): PiRuntimeObservation[]
  onSessionRuntime(listener: (runtime: PiRuntimeObservation) => void): void
  onPiManager(listener: (manager: ObservablePiManager) => void): void
  runtimeIdFor(manager: ObservablePiManager): string | null
  onWorkspaceRemoved?(listener: (workspaceId: string) => void): void
  onActiveWorkspaceChanged?(listener: (workspaceId: string | null) => void): void
}

/**
 * Attach once in the Pi main/host process. All Pi managers are observed,
 * including INACTIVE tabs whose streaming events are filtered from the UI.
 * Closing this adapter disables processing; it does not stop Pi or alter tabs.
 */
export function attachPiDesktopTelemetry(
  source: ObservableWorkspaceManager,
  store: PiTelemetryStore,
): { refresh(): void; detach(): void } {
  let attached = true
  const observedManagers = new WeakSet<ObservablePiManager>()

  const refresh = (): void => {
    if (!attached) return
    store.syncWorkspaces(source.getWorkspaces())
  }

  refresh()
  for (const runtime of source.getSessionRuntimes()) store.upsertRuntime(runtime)
  source.onSessionRuntime((runtime) => {
    if (!attached) return
    store.syncWorkspaces(source.getWorkspaces())
    store.upsertRuntime(runtime)
  })
  source.onPiManager((manager) => {
    if (observedManagers.has(manager)) return
    observedManagers.add(manager)
    manager.on('event', (event) => {
      if (!attached) return
      const runtimeId = source.runtimeIdFor(manager)
      if (runtimeId) store.ingestRpcEvent(runtimeId, event)
    })
  })
  source.onWorkspaceRemoved?.(() => {
    if (attached) store.syncWorkspaces(source.getWorkspaces())
  })
  source.onActiveWorkspaceChanged?.(() => {
    if (attached) store.syncWorkspaces(source.getWorkspaces())
  })

  return {
    refresh,
    detach: () => { attached = false },
  }
}
