import type { WorkspaceManager } from '../workspace-manager'
import type { FileProvisionJournal } from './desktop-control-journal'
import { startPiNativeTelemetryHost } from './pi-native/packages/pi-host/src/telemetry-host.ts'
import { PiTelemetryStore } from './pi-native/packages/pi-host/src/telemetry-store.ts'
import type { TelemetryCorrelation } from './pi-native/packages/protocol/src/telemetry.ts'

/**
 * Observation-only mount in Pi Desktop's existing main process. Uses the
 * control server's authenticated provision journal as the ONLY source of
 * project/workspace association. Never reads prompts or changes Pi state.
 */
export async function startCrystalNativeTelemetry(
  workspaceManager: WorkspaceManager,
  journal: FileProvisionJournal,
  token: string,
  allowedProjects: ReadonlySet<string>,
  pipePath?: string,
): Promise<() => void> {
  const boundWorkspaces = new Map<string, TelemetryCorrelation>()
  const boundRuntimeKeys = new Map<string, string>()
  let closed = false
  let ongoing: Promise<void> | undefined

  const applyRuntime = (store: PiTelemetryStore, runtime: {
    runtimeId: string; workspaceId: string; closed?: boolean
  }): void => {
    if (closed) return
    if (runtime.closed) {
      boundRuntimeKeys.delete(runtime.runtimeId)
      return
    }
    const correlation = boundWorkspaces.get(runtime.workspaceId)
    if (!correlation) return
    const key = runtime.workspaceId + ':' + correlation.operationId
    if (boundRuntimeKeys.get(runtime.runtimeId) === key) return
    store.bindRuntime(runtime.runtimeId, correlation)
    boundRuntimeKeys.set(runtime.runtimeId, key)
  }

  const refreshBindings = (store: PiTelemetryStore): Promise<void> => {
    if (closed) return Promise.resolve()
    if (ongoing) return ongoing
    ongoing = (async () => {
      const existing = new Set(workspaceManager.getWorkspaces().map(w => w.id))
      for (const entry of await journal.listCreated()) {
        const workspaceId = entry.result.workspace?.id
        const { projectId, operationId } = entry.request
        if (entry.result.state !== 'created' || !workspaceId ||
          !existing.has(workspaceId) || !allowedProjects.has(projectId) ||
          typeof operationId !== 'string' || operationId.length === 0) continue
        const previous = boundWorkspaces.get(workspaceId)
        if (previous && (previous.projectId !== projectId || previous.operationId !== operationId)) {
          throw new Error('Conflicting native telemetry provision journal')
        }
        if (!previous) {
          const correlation: TelemetryCorrelation = {
            projectId, operationId,
            ...(operationId.startsWith('crystal-tab:') && operationId.length > 12
              ? { attemptId: operationId.slice('crystal-tab:'.length) } : {}),
          }
          store.bindWorkspace(workspaceId, correlation)
          boundWorkspaces.set(workspaceId, correlation)
        }
      }
      for (const runtime of workspaceManager.getSessionRuntimes()) applyRuntime(store, runtime)
    })()
    return ongoing.finally(() => { ongoing = undefined })
  }

  const host = await startPiNativeTelemetryHost({
    source: workspaceManager,
    credentials: [{ token, allowedProjectIds: new Set(allowedProjects) }],
    ...(pipePath === undefined ? {} : { pipePath }),
    bindTrustedCorrelations: refreshBindings,
  })

  // Pi Desktop exposes a native listener for *all* session runtimes, not
  // only the active tab. The base telemetry adapter is registered first.
  workspaceManager.onSessionRuntime(runtime => {
    try { applyRuntime(host.store, runtime) } catch { /* no Pi interruption */ }
  })
  const timer = setInterval(() => {
    void refreshBindings(host.store).catch(() => {
      // Keep the pipe running; a malformed/colliding journal never authorizes
      // a new binding. Retrying is observational only.
    })
  }, 2000)
  timer.unref()

  return () => {
    if (closed) return
    closed = true
    clearInterval(timer)
    void host.close().catch(() => {})
  }
}
