import type {
  TelemetryCapabilities, TelemetryEvent, TelemetryEventsPage, TelemetryEventsQuery,
  TelemetrySnapshot,
} from '../../protocol/src/telemetry.ts'

/** Authentication is established by the local transport, never supplied by request JSON. */
export interface TelemetryHubCaller {
  authenticated: boolean
  allowedProjectIds: ReadonlySet<string>
}

export interface TelemetryHubSource {
  capabilities(): TelemetryCapabilities
  snapshot(projectId?: string): TelemetrySnapshot
  events(query?: TelemetryEventsQuery, projectId?: string): TelemetryEventsPage
  subscribe(listener: (event: TelemetryEvent) => void): () => void
}

export interface TelemetrySubscription {
  replay: TelemetryEventsPage
  close(): void
}

export interface PiNativeTelemetryHub {
  capabilities(caller: TelemetryHubCaller, projectId: string): TelemetryCapabilities
  snapshot(caller: TelemetryHubCaller, projectId: string): TelemetrySnapshot
  events(caller: TelemetryHubCaller, projectId: string, query?: TelemetryEventsQuery): TelemetryEventsPage
  subscribe(
    caller: TelemetryHubCaller, projectId: string, query: TelemetryEventsQuery,
    onEvent: (event: TelemetryEvent) => void,
  ): TelemetrySubscription
}

function authorize(caller: TelemetryHubCaller, projectId: string): void {
  if (!caller?.authenticated || !(caller.allowedProjectIds instanceof Set) ||
      typeof projectId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(projectId) ||
      !caller.allowedProjectIds.has(projectId)) {
    throw new Error('Unauthorized telemetry project')
  }
}

function safeQuery(query: TelemetryEventsQuery, maxPageSize: number): TelemetryEventsQuery {
  if (!query || typeof query !== 'object' || Array.isArray(query) ||
      Object.keys(query).some(k => !['sinceSeq', 'streamId', 'limit'].includes(k)) ||
      (query.sinceSeq !== undefined &&
        (!Number.isSafeInteger(query.sinceSeq) || query.sinceSeq < 0)) ||
      (query.streamId !== undefined &&
        (typeof query.streamId !== 'string' || query.streamId.length < 1 ||
         query.streamId.length > 128)) ||
      (query.limit !== undefined &&
        (!Number.isSafeInteger(query.limit) || query.limit < 1 ||
         query.limit > maxPageSize))) {
    throw new Error('Invalid telemetry cursor or page size')
  }
  return query
}

/**
 * One observation hub shared by Crystal and Bridge. This has no scheduler,
 * workspace creation, prompt delivery, task receipts or durable transcript.
 */
export function createPiNativeTelemetryHub(
  source: TelemetryHubSource,
  options: { refresh?: () => void } = {},
): PiNativeTelemetryHub {
  const max = source.capabilities().maxPageSize
  if (!Number.isSafeInteger(max) || max < 1 || max > 1000) {
    throw new Error('Invalid source telemetry capabilities')
  }
  return {
    capabilities(caller, projectId) {
      authorize(caller, projectId)
      return structuredClone(source.capabilities())
    },
    snapshot(caller, projectId) {
      authorize(caller, projectId)
      options.refresh?.()
      return source.snapshot(projectId)
    },
    events(caller, projectId, query = {}) {
      authorize(caller, projectId)
      options.refresh?.()
      return source.events(safeQuery(query, max), projectId)
    },
    subscribe(caller, projectId, query, onEvent) {
      authorize(caller, projectId)
      if (typeof onEvent !== 'function') throw new Error('Telemetry callback required')
      const checked = safeQuery(query, max)
      options.refresh?.()
      // No cursor means live-only. Explicit cursors replay within retention.
      const watermark = checked.sinceSeq === undefined ? source.snapshot(projectId).currentSeq : checked.sinceSeq
      const actual = { ...checked, sinceSeq: watermark }
      let cursor = watermark
      let active = true
      let ready = false
      const pending: TelemetryEvent[] = []
      const deliver = (event: TelemetryEvent) => {
        if (!active || event.correlation?.projectId !== projectId ||
            event.seq <= cursor) return
        if (!ready) { pending.push(event); return }
        cursor = event.seq
        try { onEvent(structuredClone(event)) } catch { /* observers cannot interrupt Pi */ }
      }
      // Install listener first, then query history to avoid a race at handoff.
      const unsubscribe = source.subscribe(deliver)
      try {
        const replay = source.events(actual, projectId)
        if (replay.resetRequired || replay.hasGap) {
          throw new Error('Telemetry history changed or expired; resnapshot required')
        }
        if (replay.nextSeq < replay.currentSeq) {
          throw new Error('Telemetry replay backlog; page events before subscribing')
        }
        cursor = replay.nextSeq
        ready = true
        pending.sort((a, b) => a.seq - b.seq).forEach(deliver)
        return {
          replay,
          close() {
            if (!active) return
            active = false
            pending.length = 0
            unsubscribe()
          },
        }
      } catch (error) {
        active = false
        unsubscribe()
        throw error
      }
    },
  }
}
