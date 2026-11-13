import type {
  CrystalTelemetryRequest,
  CrystalTelemetryResult,
  TelemetryEventsQuery,
} from '../../protocol/src/telemetry.ts'
import { PiTelemetryStore } from '../../pi-host/src/telemetry-store.ts'

/**
 * Identity MUST come from the existing authenticated, OS-protected local pipe,
 * not fields supplied in the incoming JSON. This handler opens no listener.
 */
export interface TrustedCrystalCaller {
  authenticated: boolean
  allowedProjectIds: ReadonlySet<string>
}

export function createCrystalTelemetryHandler(
  store: PiTelemetryStore,
  options: { refresh?: () => void } = {},
): (input: unknown, caller: TrustedCrystalCaller) => CrystalTelemetryResult {
  return (input, caller) => {
    if (!caller?.authenticated || !(caller.allowedProjectIds instanceof Set) ||
        caller.allowedProjectIds.size === 0) {
      throw new Error('Unauthorized telemetry caller')
    }
    const request = parseRequest(input)
    if (!caller.allowedProjectIds.has(request.projectId)) {
      throw new Error('Telemetry project not allowed')
    }
    if (request.operation === 'telemetry.capabilities') return store.capabilities()

    options.refresh?.()
    if (request.operation === 'telemetry.snapshot') return store.snapshot(request.projectId)
    const query: TelemetryEventsQuery = {
      ...(request.sinceSeq !== undefined ? { sinceSeq: request.sinceSeq } : {}),
      ...(request.streamId !== undefined ? { streamId: request.streamId } : {}),
      ...(request.limit !== undefined ? { limit: request.limit } : {}),
    }
    return store.events(query, request.projectId)
  }
}

function parseRequest(input: unknown): CrystalTelemetryRequest {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw new Error('Malformed telemetry request')
  }
  const r = input as Record<string, unknown>
  if (r.version !== 1 || typeof r.projectId !== 'string' ||
      r.projectId.length < 1 || r.projectId.length > 128) {
    throw new Error('Invalid telemetry protocol or project')
  }
  if (r.operation === 'telemetry.capabilities' || r.operation === 'telemetry.snapshot') {
    return { version: 1, operation: r.operation, projectId: r.projectId }
  }
  if (r.operation !== 'telemetry.events') throw new Error('Unsupported telemetry operation')
  if (r.sinceSeq !== undefined &&
      (typeof r.sinceSeq !== 'number' || !Number.isSafeInteger(r.sinceSeq) || r.sinceSeq < 0)) {
    throw new Error('Invalid telemetry cursor')
  }
  if (r.limit !== undefined &&
      (typeof r.limit !== 'number' || !Number.isSafeInteger(r.limit) ||
       r.limit < 1 || r.limit > PiTelemetryStore.MAX_PAGE_SIZE)) {
    throw new Error('Invalid telemetry page size')
  }
  if (r.streamId !== undefined &&
      (typeof r.streamId !== 'string' || r.streamId.length < 1 || r.streamId.length > 128)) {
    throw new Error('Invalid telemetry stream')
  }
  return {
    version: 1, operation: 'telemetry.events', projectId: r.projectId,
    ...(r.sinceSeq !== undefined ? { sinceSeq: r.sinceSeq as number } : {}),
    ...(r.limit !== undefined ? { limit: r.limit as number } : {}),
    ...(r.streamId !== undefined ? { streamId: r.streamId as string } : {}),
  }
}
