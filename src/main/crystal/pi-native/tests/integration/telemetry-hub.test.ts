import test from 'node:test'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { createConnection, type Socket } from 'node:net'
import { EventEmitter } from 'node:events'
import { PiTelemetryStore, type PiRuntimeObservation, type PiWorkspaceObservation } from '../../packages/pi-host/src/telemetry-store.ts'
import { createPiNativeTelemetryHub } from '../../packages/pi-host/src/telemetry-hub.ts'
import { startPiNativeTelemetryPipe, DEFAULT_TELEMETRY_PIPE } from '../../packages/pi-host/src/telemetry-pipe.ts'
import { startPiNativeTelemetryHost } from '../../packages/pi-host/src/telemetry-host.ts'
import type { ObservablePiManager } from '../../packages/pi-host/src/pi-desktop-telemetry-adapter.ts'

const stamp = () => new Date('2026-11-12T17:00:00.000Z')
const alpha = { authenticated: true, allowedProjectIds: new Set(['alpha']) }
const beta = { authenticated: true, allowedProjectIds: new Set(['beta']) }
const token = 'telemetry-test-secret-with-forty-characters-12345'

function seeded(maxEvents = 50) {
  const store = new PiTelemetryStore({ streamId: 'stream-test', now: stamp, maxEvents })
  store.syncWorkspaces([
    { id: 'a-ws', name: 'Alpha', kind: 'worktree' },
    { id: 'b-ws', name: 'Beta', kind: 'folder' },
  ])
  store.bindWorkspace('a-ws', { projectId: 'alpha' })
  store.bindWorkspace('b-ws', { projectId: 'beta' })
  store.upsertRuntime({
    runtimeId: 'a-rt', workspaceId: 'a-ws', sessionId: 'a-session',
    status: 'running', activity: null, active: false, pid: 125,
  })
  store.upsertRuntime({
    runtimeId: 'b-rt', workspaceId: 'b-ws', sessionId: 'b-session',
    status: 'running', activity: null, active: true, pid: 126,
  })
  store.bindRuntime('a-rt', { projectId: 'alpha', taskId: 'task-1', attemptId: 'attempt-1' })
  store.bindRuntime('b-rt', { projectId: 'beta', taskId: 'task-2', attemptId: 'attempt-2' })
  return store
}

test('four operations, project authorization, metadata only, and subscriber isolation', () => {
  const store = seeded()
  const hub = createPiNativeTelemetryHub(store)
  assert.deepEqual(hub.capabilities(alpha, 'alpha').operations, [
    'telemetry.capabilities', 'telemetry.snapshot', 'telemetry.events', 'telemetry.subscribe',
  ])
  const shot = hub.snapshot(alpha, 'alpha')
  assert.equal(shot.workspaces.length, 1)
  assert.equal(shot.sessions[0].sessionId, 'a-session')
  assert.equal(hub.snapshot(beta, 'beta').sessions[0].sessionId, 'b-session')
  assert.throws(() => hub.snapshot(alpha, 'beta'), /Unauthorized/)
  assert.throws(() => hub.capabilities({ ...alpha, authenticated: false }, 'alpha'), /Unauthorized/)
  assert.throws(() => hub.events(alpha, 'alpha', { limit: 201 }), /Invalid/)
  const received: string[] = []
  const sub = hub.subscribe(alpha, 'alpha', {}, ev => received.push(ev.workspaceId))
  assert.equal(sub.replay.events.length, 0)
  store.ingestRpcEvent('b-rt', { type: 'agent_start', content: 'SECRET' })
  store.ingestRpcEvent('a-rt', { type: 'tool_execution_start', toolCallId: 'call', toolName: 'bash', args: { secret: 'SECRET' } })
  assert.deepEqual(received, ['a-ws'])
  assert.equal(JSON.stringify(hub.events(alpha, 'alpha')).includes('SECRET'), false)
  sub.close()
  store.ingestRpcEvent('a-rt', { type: 'agent_end' })
  assert.equal(received.length, 1)
})

test('runtime reassignment closes the old project stream without exposing its correlation to the new project', () => {
  const store = new PiTelemetryStore({ streamId: 'reassignment-test', now: stamp })
  store.syncWorkspaces([
    { id: 'old-ws', name: 'Old project', kind: 'worktree' },
    { id: 'new-ws', name: 'New project', kind: 'worktree' },
  ])
  store.bindWorkspace('old-ws', { projectId: 'alpha' })
  store.bindWorkspace('new-ws', { projectId: 'beta' })
  store.upsertRuntime({
    runtimeId: 'moved-rt', workspaceId: 'old-ws', sessionId: 'old-session',
    status: 'running', activity: null, active: true, pid: 300,
  })
  store.bindRuntime('moved-rt', { projectId: 'alpha', taskId: 'old-task', attemptId: 'old-attempt' })
  const hub = createPiNativeTelemetryHub(store)
  const alphaEvents: Array<{ workspaceId: string; sessionId: string | null; correlation?: unknown; detail: unknown }> = []
  const betaEvents: typeof alphaEvents = []
  const alphaSub = hub.subscribe(alpha, 'alpha', {}, event => alphaEvents.push(event))
  const betaSub = hub.subscribe(beta, 'beta', {}, event => betaEvents.push(event))
  try {
    store.upsertRuntime({
      runtimeId: 'moved-rt', workspaceId: 'new-ws', sessionId: 'new-session',
      status: 'running', activity: null, active: false, pid: 301,
    })

    const eventFields = ({ workspaceId, sessionId, correlation, detail }: typeof alphaEvents[number]) =>
      ({ workspaceId, sessionId, correlation, detail })
    assert.deepEqual(alphaEvents.map(eventFields), [{
      workspaceId: 'old-ws', sessionId: 'old-session',
      correlation: { projectId: 'alpha', taskId: 'old-task', attemptId: 'old-attempt' },
      detail: { type: 'runtime', process: 'stopped', activity: 'idle', active: false, closed: true },
    }])
    assert.deepEqual(betaEvents.map(eventFields), [{
      workspaceId: 'new-ws', sessionId: 'new-session', correlation: { projectId: 'beta' },
      detail: { type: 'runtime', process: 'running', activity: 'idle', active: false },
    }])

    const oldSnapshot = hub.snapshot(alpha, 'alpha')
    assert.deepEqual(oldSnapshot.workspaces.map(workspace => workspace.workspaceId), ['old-ws'])
    assert.equal(oldSnapshot.sessions.length, 0)
    const newSnapshot = hub.snapshot(beta, 'beta')
    assert.deepEqual(newSnapshot.workspaces.map(workspace => workspace.workspaceId), ['new-ws'])
    assert.equal(newSnapshot.sessions.length, 1)
    assert.equal(newSnapshot.sessions[0].sessionId, 'new-session')
    assert.equal(newSnapshot.sessions[0].correlation, undefined)
  } finally {
    alphaSub.close()
    betaSub.close()
  }
})

test('replay requires a gap-free cursor; backlog must be paged before subscribe', () => {
  const store = seeded(5)
  const hub = createPiNativeTelemetryHub(store)
  const start = hub.snapshot(alpha, 'alpha')
  for (let i = 0; i < 6; i++) store.ingestRpcEvent('a-rt', { type: 'auto_retry_start', attempt: i })
  assert.throws(() => hub.subscribe(alpha, 'alpha', {
    sinceSeq: start.currentSeq, streamId: 'stream-test', limit: 2,
  }, () => {}), /expired|backlog/)
  const page = hub.events(alpha, 'alpha', {
    sinceSeq: start.currentSeq, streamId: 'stream-test', limit: 2,
  })
  assert.equal(page.hasGap, true)
  const recentCursor = store.events({}, 'alpha').oldestAvailableSeq - 1
  assert.throws(() => hub.subscribe(alpha, 'alpha', {
    sinceSeq: recentCursor, streamId: 'wrong', limit: 20,
  }, () => {}), /changed|expired/)
  const stream = hub.subscribe(alpha, 'alpha', {
    sinceSeq: recentCursor, streamId: 'stream-test', limit: 20,
  }, () => {})
  assert.equal(stream.replay.events.length, 5)
  stream.close()
})

interface LineClient {
  socket: Socket
  next(): Promise<Record<string, unknown>>
  send(value: unknown): void
  close(): void
}
function lineClient(path: string): LineClient {
  const socket = createConnection(path)
  let buffer = ''
  const frames: Array<Record<string, unknown>> = []
  const waiters: Array<(value: Record<string, unknown>) => void> = []
  socket.on('data', chunk => {
    buffer += chunk.toString('utf8')
    let index: number
    while ((index = buffer.indexOf('\n')) >= 0) {
      const frame = JSON.parse(buffer.slice(0, index)) as Record<string, unknown>
      buffer = buffer.slice(index + 1)
      const resolve = waiters.shift()
      if (resolve) resolve(frame)
      else frames.push(frame)
    }
  })
  return {
    socket,
    send(value) { socket.write(JSON.stringify(value) + '\n') },
    next() {
      if (frames.length) return Promise.resolve(frames.shift()!)
      return new Promise<Record<string, unknown>>((resolve, reject) => {
        const timeout = setTimeout(() => {
          const i = waiters.indexOf(fulfill)
          if (i >= 0) waiters.splice(i, 1)
          reject(new Error('Timed out waiting for telemetry frame'))
        }, 2500)
        const fulfill = (frame: Record<string, unknown>) => {
          clearTimeout(timeout)
          resolve(frame)
        }
        waiters.push(fulfill)
      })
    },
    close() { socket.destroy() },
  }
}

test('real local named pipe serves snapshot/replay/live frames and rejects unauthorized calls', {
  skip: process.platform !== 'win32',
}, async () => {
  const store = seeded()
  const path = DEFAULT_TELEMETRY_PIPE + '-test-' + randomUUID()
  const pipe = await startPiNativeTelemetryPipe({
    path, hub: createPiNativeTelemetryHub(store),
    credentials: [{ token, allowedProjectIds: new Set(['alpha']) }],
    heartbeatMs: 1000,
  })
  const clients: LineClient[] = []
  const client = () => { const c = lineClient(path); clients.push(c); return c }
  try {
    const bad = client()
    bad.send({ version: 1, operation: 'telemetry.snapshot', projectId: 'alpha', token: 'wrong' })
    assert.equal((await bad.next()).error, 'unauthorized')

    const denied = client()
    denied.send({ version: 1, operation: 'telemetry.snapshot', projectId: 'beta', token })
    assert.equal((await denied.next()).ok, false)

    const cap = client()
    cap.send({ version: 1, operation: 'telemetry.capabilities', projectId: 'alpha', token })
    const caps = await cap.next()
    assert.equal(caps.ok, true)
    assert.equal((caps.data as {operations: string[]}).operations.length, 4)

    const snapshot = client()
    snapshot.send({ version: 1, operation: 'telemetry.snapshot', projectId: 'alpha', token })
    const shot = (await snapshot.next()).data as { currentSeq: number; streamId: string; sessions: unknown[] }
    assert.equal(shot.sessions.length, 1)

    const subscriber = client()
    subscriber.send({
      version: 1, operation: 'telemetry.subscribe', projectId: 'alpha', token,
      streamId: shot.streamId, sinceSeq: shot.currentSeq, limit: 20,
    })
    assert.equal((await subscriber.next()).type, 'replay')
    store.ingestRpcEvent('a-rt', { type: 'agent_start' })
    const frame = await subscriber.next()
    assert.equal(frame.type, 'event')
    assert.equal((frame.event as {workspaceId:string}).workspaceId, 'a-ws')
    let heartbeatSeen = false
    for (let i = 0; i < 4; i++) {
      const next = await subscriber.next()
      if (next.type === 'heartbeat') { heartbeatSeen = true; break }
      assert.equal(next.type, 'event')
    }
    assert.equal(heartbeatSeen, true)

    const replay = client()
    replay.send({
      version: 1, operation: 'telemetry.events', projectId: 'alpha', token,
      streamId: shot.streamId, sinceSeq: shot.currentSeq, limit: 10,
    })
    const page = (await replay.next()).data as {events: unknown[]}
    assert.equal(page.events.length >= 1, true)
  } finally {
    for (const c of clients) c.close()
    await pipe.close()
  }
})

class Manager extends EventEmitter {
  send(value: Record<string, unknown>) { this.emit('event', value) }
}
class Source {
  workspace: PiWorkspaceObservation = { id: 'ws', name: 'Native tab', kind: 'worktree' }
  runtime: PiRuntimeObservation = {
    runtimeId: 'rt', workspaceId: 'ws', sessionId: 'sess',
    status: 'running', activity: null, active: false, pid: 123,
  }
  manager = new Manager()
  getWorkspaces() { return [this.workspace] }
  getSessionRuntimes() { return [this.runtime] }
  onSessionRuntime(_callback: (runtime: PiRuntimeObservation) => void) {}
  onPiManager(callback: (manager: ObservablePiManager) => void) { callback(this.manager) }
  runtimeIdFor(_manager: ObservablePiManager) { return 'rt' }
}
test('host mount observes existing inactive Pi managers without creating sessions', {
  skip: process.platform !== 'win32',
}, async () => {
  const source = new Source()
  const path = DEFAULT_TELEMETRY_PIPE + '-host-test-' + randomUUID()
  const host = await startPiNativeTelemetryHost({
    source, pipePath: path,
    credentials: [{ token, allowedProjectIds: new Set(['alpha']) }],
    bindTrustedCorrelations(store) {
      store.bindWorkspace('ws', { projectId: 'alpha' })
      store.bindRuntime('rt', { projectId: 'alpha', attemptId: 'test-1' })
    },
  })
  try {
    source.manager.send({ type: 'agent_start', content: 'private' })
    const snap = host.hub.snapshot(alpha, 'alpha')
    assert.equal(snap.sessions[0].active, false)
    assert.equal(snap.sessions[0].activity, 'working')
    assert.equal(snap.sessions[0].correlation?.attemptId, 'test-1')
  } finally { await host.close() }
})
