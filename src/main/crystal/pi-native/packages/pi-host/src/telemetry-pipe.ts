import { createServer, type Server, type Socket } from 'node:net'
import { timingSafeEqual } from 'node:crypto'
import type { TelemetryEventsQuery, TelemetryStreamFrame } from '../../protocol/src/telemetry.ts'
import type { PiNativeTelemetryHub, TelemetryHubCaller } from './telemetry-hub.ts'

export const DEFAULT_TELEMETRY_PIPE = String.raw`\\.\pipe\pi-native-telemetry-v1`

export interface TelemetryCredential {
  /** Host-loaded secret (32+ chars); never commit or put into package manifests. */
  token: string
  allowedProjectIds: ReadonlySet<string>
}
export interface TelemetryPipeOptions {
  path: string
  hub: PiNativeTelemetryHub
  credentials: readonly TelemetryCredential[]
  maxRequestBytes?: number
  maxPendingBytes?: number
  heartbeatMs?: number
}
export interface TelemetryPipeHandle {
  path: string
  close(): Promise<void>
}

type WireOperation =
  | 'telemetry.capabilities' | 'telemetry.snapshot' | 'telemetry.events' | 'telemetry.subscribe'
interface WireRequest extends TelemetryEventsQuery {
  version: 1
  operation: WireOperation
  projectId: string
  token: string
}
const operations = new Set<WireOperation>([
  'telemetry.capabilities', 'telemetry.snapshot', 'telemetry.events', 'telemetry.subscribe',
])

function parseRequest(line: string): WireRequest {
  const obj: unknown = JSON.parse(line)
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) throw new Error('invalid_request')
  const req = obj as Record<string, unknown>
  if (Object.keys(req).some(key => ![
    'version', 'operation', 'projectId', 'token', 'sinceSeq', 'streamId', 'limit',
  ].includes(key)) ||
      req.version !== 1 || !operations.has(req.operation as WireOperation) ||
      typeof req.projectId !== 'string' || typeof req.token !== 'string') {
    throw new Error('invalid_request')
  }
  if ((req.operation === 'telemetry.capabilities' || req.operation === 'telemetry.snapshot') &&
      ['sinceSeq', 'streamId', 'limit'].some(key => req[key] !== undefined)) {
    throw new Error('invalid_request')
  }
  if (req.sinceSeq !== undefined &&
      (!Number.isSafeInteger(req.sinceSeq) || (req.sinceSeq as number) < 0) ||
      req.limit !== undefined &&
      (!Number.isSafeInteger(req.limit) || (req.limit as number) < 1) ||
      req.streamId !== undefined &&
      (typeof req.streamId !== 'string' || req.streamId.length === 0 || req.streamId.length > 128)) {
    throw new Error('invalid_request')
  }
  return req as unknown as WireRequest
}

function tokenEquals(a: string, b: string): boolean {
  const aa = Buffer.from(a), bb = Buffer.from(b)
  return aa.length === bb.length && timingSafeEqual(aa, bb)
}

/** Authenticated, project-scoped, metadata-only JSON-lines server. No TCP port. */
export async function startPiNativeTelemetryPipe(options: TelemetryPipeOptions): Promise<TelemetryPipeHandle> {
  if (!options.path || (process.platform === 'win32' &&
      !options.path.startsWith(DEFAULT_TELEMETRY_PIPE.slice(0, -'pi-native-telemetry-v1'.length)))) {
    throw new Error('Local telemetry pipe path required')
  }
  const maxRequestBytes = options.maxRequestBytes ?? 16_384
  const maxPendingBytes = options.maxPendingBytes ?? 262_144
  const heartbeatMs = options.heartbeatMs ?? 15_000
  if (![maxRequestBytes, maxPendingBytes, heartbeatMs].every(Number.isSafeInteger) ||
      maxRequestBytes < 1024 || maxRequestBytes > 65_536 ||
      maxPendingBytes < 1024 || maxPendingBytes > 1_048_576 ||
      heartbeatMs < 1000 || heartbeatMs > 60_000 ||
      !options.credentials.length) throw new Error('Invalid telemetry server limits or credentials')
  for (let i = 0; i < options.credentials.length; i++) {
    const item = options.credentials[i]
    if (typeof item.token !== 'string' || item.token.length < 32 ||
        !(item.allowedProjectIds instanceof Set) || item.allowedProjectIds.size === 0) {
      throw new Error('Invalid telemetry credential')
    }
    if (options.credentials.slice(0, i).some(previous => tokenEquals(previous.token, item.token))) {
      throw new Error('Duplicate telemetry credential')
    }
  }
  const callerFor = (token: string): TelemetryHubCaller | undefined => {
    const match = options.credentials.find(entry => tokenEquals(entry.token, token))
    return match === undefined ? undefined : {
      authenticated: true, allowedProjectIds: match.allowedProjectIds,
    }
  }

  const sockets = new Set<Socket>()
  const server: Server = createServer(socket => {
    sockets.add(socket)
    let buffer = ''
    let handled = false
    let unsubscribe: (() => void) | undefined
    let heartbeat: ReturnType<typeof setInterval> | undefined
    const cleanup = () => {
      unsubscribe?.()
      unsubscribe = undefined
      if (heartbeat) clearInterval(heartbeat)
      heartbeat = undefined
      sockets.delete(socket)
    }
    const send = (message: unknown): boolean => {
      if (socket.destroyed || !socket.writable) return false
      try {
        socket.write(JSON.stringify(message) + '\n')
        if (socket.writableLength > maxPendingBytes) {
          socket.destroy()
          return false
        }
        return true
      } catch { socket.destroy(); return false }
    }
    socket.setTimeout(heartbeatMs * 3, () => socket.destroy())
    socket.once('close', cleanup)
    socket.once('error', cleanup)
    socket.on('data', chunk => {
      if (handled) return
      buffer += chunk.toString('utf8')
      if (Buffer.byteLength(buffer) > maxRequestBytes) { socket.destroy(); return }
      const i = buffer.indexOf('\n')
      if (i < 0) return
      handled = true
      void (async () => {
        let caller: TelemetryHubCaller | undefined
        let request: WireRequest
        try {
          request = parseRequest(buffer.slice(0, i))
          caller = callerFor(request.token)
          if (!caller) throw new Error('unauthorized')
          const projectId = request.projectId
          const query: TelemetryEventsQuery = {
            ...(request.sinceSeq === undefined ? {} : { sinceSeq: request.sinceSeq }),
            ...(request.streamId === undefined ? {} : { streamId: request.streamId }),
            ...(request.limit === undefined ? {} : { limit: request.limit }),
          }
          if (request.operation === 'telemetry.capabilities') {
            socket.end(JSON.stringify({
              ok: true, data: options.hub.capabilities(caller, projectId),
            }) + '\n')
          } else if (request.operation === 'telemetry.snapshot') {
            socket.end(JSON.stringify({
              ok: true, data: options.hub.snapshot(caller, projectId),
            }) + '\n')
          } else if (request.operation === 'telemetry.events') {
            socket.end(JSON.stringify({
              ok: true, data: options.hub.events(caller, projectId, query),
            }) + '\n')
          } else {
            const subscription = options.hub.subscribe(caller, projectId, query, event => {
              if (!send({ type: 'event', event } satisfies TelemetryStreamFrame)) socket.destroy()
            })
            unsubscribe = subscription.close
            if (!send({ type: 'replay', page: subscription.replay } satisfies TelemetryStreamFrame)) return
            socket.setTimeout(0)
            heartbeat = setInterval(() => {
              try {
                const snap = options.hub.snapshot(caller!, projectId)
                if (!send({
                  type: 'heartbeat', streamId: snap.streamId,
                  currentSeq: snap.currentSeq, observedAt: snap.observedAt,
                } satisfies TelemetryStreamFrame)) socket.destroy()
              } catch { socket.destroy() }
            }, heartbeatMs)
            heartbeat.unref()
          }
        } catch (error) {
          const reason = error instanceof Error && /resnapshot|history changed|expired|backlog/i.test(error.message)
            ? 'resnapshot_or_replay_required'
            : error instanceof Error && error.message === 'unauthorized' ? 'unauthorized'
            : 'not_authorized_or_invalid_query'
          socket.end(JSON.stringify({ ok: false, error: reason }) + '\n')
        }
      })()
    })
  })
  server.on('error', () => { /* An Electron host must keep running if telemetry fails. */ })
  await new Promise<void>((resolve, reject) => {
    const failure = (error: Error) => { server.off('listening', success); reject(error) }
    const success = () => { server.off('error', failure); resolve() }
    server.once('error', failure)
    server.once('listening', success)
    server.listen(options.path)
  })
  return {
    path: options.path,
    close: () => new Promise<void>((resolve, reject) => {
      for (const socket of sockets) socket.destroy()
      server.close(error => error ? reject(error) : resolve())
    }),
  }
}
