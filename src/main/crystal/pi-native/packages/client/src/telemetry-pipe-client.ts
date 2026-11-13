import { createConnection, type Socket } from 'node:net'
import type {
  CrystalTelemetryRequest, CrystalTelemetryResult, TelemetryStreamFrame,
} from '../../protocol/src/telemetry.ts'
import { DEFAULT_TELEMETRY_PIPE } from '../../pi-host/src/telemetry-pipe.ts'

type UnaryRequest = Exclude<CrystalTelemetryRequest, { operation: 'telemetry.subscribe' }>
type SubscribeRequest = Extract<CrystalTelemetryRequest, { operation: 'telemetry.subscribe' }>
export interface TelemetryClientOptions {
  token: string
  pipePath?: string
  timeoutMs?: number
  maxFrameBytes?: number
}
export interface TelemetryStream {
  close(): void
}
export interface TelemetryPipeClient {
  request(request: UnaryRequest): Promise<CrystalTelemetryResult>
  subscribe(
    request: SubscribeRequest,
    onFrame: (frame: TelemetryStreamFrame) => void,
    onDisconnect?: (error?: Error) => void,
  ): Promise<TelemetryStream>
}

interface Connection {
  socket: Socket
  write(input: CrystalTelemetryRequest): void
  close(): void
}
export function createPiNativeTelemetryClient(options: TelemetryClientOptions): TelemetryPipeClient {
  if (typeof options.token !== 'string' || options.token.length < 32) {
    throw new Error('Telemetry client needs a protected 32+ character token')
  }
  const pipePath = options.pipePath ?? DEFAULT_TELEMETRY_PIPE
  const timeoutMs = options.timeoutMs ?? 5000
  const maxFrameBytes = options.maxFrameBytes ?? 1_048_576
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 ||
      !Number.isSafeInteger(maxFrameBytes) || maxFrameBytes < 4096 ||
      maxFrameBytes > 4_194_304) throw new Error('Invalid telemetry client limits')
  const connect = (): Connection => {
    const socket = createConnection(pipePath)
    return {
      socket,
      write(input) { socket.write(JSON.stringify({ ...input, token: options.token }) + '\n') },
      close() { socket.destroy() },
    }
  }
  return {
    request(input) {
      return new Promise<CrystalTelemetryResult>((resolve, reject) => {
        const connection = connect()
        let buffer = ''
        let settled = false
        const done = (error?: Error, result?: CrystalTelemetryResult) => {
          if (settled) return
          settled = true
          connection.close()
          if (error) reject(error)
          else resolve(result!)
        }
        connection.socket.setTimeout(timeoutMs, () => done(new Error('Telemetry request timed out')))
        connection.socket.once('error', error => done(error))
        connection.socket.once('close', () => done(new Error('Telemetry pipe closed')))
        connection.socket.once('connect', () => connection.write(input))
        connection.socket.on('data', chunk => {
          buffer += chunk.toString('utf8')
          if (Buffer.byteLength(buffer) > maxFrameBytes) return done(new Error('Telemetry response too large'))
          const end = buffer.indexOf('\n')
          if (end < 0) return
          try {
            const response = JSON.parse(buffer.slice(0, end)) as { ok?: boolean; data?: CrystalTelemetryResult; error?: string }
            if (response.ok !== true || response.data === undefined) {
              return done(new Error('Telemetry source rejected request: ' + String(response.error ?? 'unknown')))
            }
            done(undefined, response.data)
          } catch (error) { done(error as Error) }
        })
      })
    },
    subscribe(input, onFrame, onDisconnect) {
      if (input.operation !== 'telemetry.subscribe' || typeof onFrame !== 'function') {
        return Promise.reject(new Error('Invalid telemetry subscribe request'))
      }
      return new Promise<TelemetryStream>((resolve, reject) => {
        const connection = connect()
        let buffer = ''
        let accepted = false
        let closed = false
        let manualClose = false
        const failure = (error: Error) => {
          if (closed) return
          closed = true
          connection.close()
          if (!accepted) reject(error)
          else if (!manualClose) onDisconnect?.(error)
        }
        connection.socket.setTimeout(timeoutMs, () => failure(new Error('Telemetry stream timed out')))
        connection.socket.once('connect', () => connection.write(input))
        connection.socket.once('error', failure)
        connection.socket.once('close', () => failure(new Error('Telemetry stream disconnected')))
        connection.socket.on('data', chunk => {
          if (closed) return
          buffer += chunk.toString('utf8')
          if (Buffer.byteLength(buffer) > maxFrameBytes) {
            failure(new Error('Telemetry stream frame too large'))
            return
          }
          let end: number
          while ((end = buffer.indexOf('\n')) >= 0 && !closed) {
            const line = buffer.slice(0, end)
            buffer = buffer.slice(end + 1)
            try {
              const frame = JSON.parse(line) as TelemetryStreamFrame & { ok?: boolean; error?: string }
              if (!accepted) {
                if (frame.type !== 'replay') {
                  failure(new Error('Telemetry subscription refused: ' + String(frame.error ?? 'invalid first frame')))
                  return
                }
                accepted = true
                connection.socket.setTimeout(180_000, () => failure(new Error('Telemetry heartbeat lost')))
                resolve({
                  close() {
                    manualClose = true
                    if (!closed) { closed = true; connection.close() }
                  },
                })
              }
              onFrame(frame)
            } catch (error) { failure(error as Error); return }
          }
        })
      })
    },
  }
}
