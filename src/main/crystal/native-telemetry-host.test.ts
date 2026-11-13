import assert from 'node:assert/strict'
import { randomBytes, randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import { test } from 'node:test'
import type { WorkspaceManager } from '../workspace-manager'
import type { FileProvisionJournal } from './desktop-control-journal'
import { createPiNativeTelemetryClient } from './pi-native/packages/client/src/telemetry-pipe-client.ts'
import { startCrystalNativeTelemetry } from './native-telemetry-host.ts'

test('native telemetry mount binds only journal-created Pi workspaces and observes inactive sessions', async () => {
  const token = randomBytes(32).toString('hex')
  const pipePath = String.raw`\\.\pipe\pi-native-mount-test-` + randomUUID()
  const manager = new EventEmitter()
  const runtime = {
    runtimeId: 'runtime-one', workspaceId: 'workspace-one', sessionId: 'session-one',
    status: 'running' as const, activity: 'working' as const,
    active: false, pid: 221, error: null, sessionPath: null,
  }
  const source = {
    getWorkspaces: () => [
      { id: 'workspace-one', name: 'Registered task', kind: 'worktree' },
      { id: 'private-workspace', name: 'Personal scratch', kind: 'folder' },
    ],
    getSessionRuntimes: () => [runtime],
    onSessionRuntime: (callback: (entry: typeof runtime) => void) => callback(runtime),
    onPiManager: (callback: (manager: EventEmitter) => void) => callback(manager),
    runtimeIdFor: (candidate: EventEmitter) => candidate === manager ? 'runtime-one' : null,
  } as unknown as WorkspaceManager
  const journal = {
    async listCreated() {
      return [{ request: { projectId: 'crystal', operationId: 'crystal-tab:attempt-one' },
        result: { state: 'created', workspace: { id: 'workspace-one' } } }]
    },
  } as unknown as FileProvisionJournal
  const stop = await startCrystalNativeTelemetry(
    source, journal, token, new Set(['crystal']), pipePath,
  )
  try {
    const client = createPiNativeTelemetryClient({ token, pipePath })
    const snap = await client.request({
      version: 1, operation: 'telemetry.snapshot', projectId: 'crystal',
    })
    if (!('workspaces' in snap)) throw new Error('Expected telemetry snapshot')
    assert.deepEqual(snap.workspaces.map(w => w.workspaceId), ['workspace-one'])
    assert.equal(snap.sessions.length, 1)
    assert.equal(snap.sessions[0]?.active, false)
    assert.equal(snap.sessions[0]?.correlation?.attemptId, 'attempt-one')
    manager.emit('event', { type: 'agent_start' })
    const page = await client.request({
      version: 1, operation: 'telemetry.events', projectId: 'crystal',
      streamId: snap.streamId, sinceSeq: 0,
    })
    if (!('events' in page)) throw new Error('Expected telemetry page')
    assert.ok(page.events.some(e => e.detail.type === 'turn'))
    await assert.rejects(client.request({
      version: 1, operation: 'telemetry.snapshot', projectId: 'outside',
    }), /authorized|rejected/i)
  } finally {
    stop()
  }
})
