import test from 'node:test'
import assert from 'node:assert/strict'
import { PiTelemetryStore } from '../../packages/pi-host/src/telemetry-store.ts'
import { createCrystalTelemetryHandler } from '../../packages/client/src/crystal-telemetry.ts'

const workspace = { id: 'ws-one', name: 'Demo', kind: 'worktree' as const, branch: 'feat/demo' }
const runtime = {
  runtimeId: 'rt-one', workspaceId: 'ws-one', sessionId: 's-one',
  status: 'running' as const, activity: null, active: false, pid: 123,
}

function setup(maxEvents = 40) {
  const store = new PiTelemetryStore({
    streamId: 'run-1', maxEvents,
    now: () => new Date('2026-11-12T17:00:00.000Z'),
  })
  store.syncWorkspaces([workspace])
  store.bindWorkspace('ws-one', { projectId: 'alpha' })
  store.upsertRuntime(runtime)
  store.bindRuntime('rt-one', { projectId: 'alpha', taskId: 'T1', attemptId: 'A1' })
  return store
}

test('snapshot is scoped to Crystal project and cloned for callers', () => {
  const store = setup()
  const handle = createCrystalTelemetryHandler(store)
  const trusted = { authenticated: true, allowedProjectIds: new Set(['alpha', 'beta']) }
  const a = handle({ version: 1, operation: 'telemetry.snapshot', projectId: 'alpha' }, trusted)
  assert.ok('sessions' in a)
  assert.equal(a.sessions.length, 1)
  assert.equal(a.sessions[0].correlation?.attemptId, 'A1')
  assert.equal(a.workspaces[0].branch, 'feat/demo')
  a.sessions[0].runtimeId = 'changed'
  assert.equal(store.snapshot('alpha').sessions[0].runtimeId, 'rt-one')
  const b = handle({ version: 1, operation: 'telemetry.snapshot', projectId: 'beta' }, trusted)
  assert.ok('sessions' in b)
  assert.equal(b.sessions.length, 0)
  assert.equal(b.workspaces.length, 0)
})

test('metadata-only events redact raw prompts, tool args/results and errors', () => {
  const store = setup()
  const secret = 'SECRET_DO_NOT_EXPOSE_934'
  store.ingestRpcEvent('rt-one', {
    type: 'tool_execution_start', toolCallId: 'x', toolName: 'bash',
    args: { command: secret },
  })
  store.ingestRpcEvent('rt-one', {
    type: 'tool_execution_end', toolCallId: 'x', toolName: 'bash',
    result: { content: [{ text: secret }] }, isError: true,
  })
  store.ingestRpcEvent('rt-one', {
    type: 'extension_ui_request', method: 'confirm', id: 'x',
    title: secret, message: secret,
  })
  store.ingestRpcEvent('rt-one', {
    type: 'extension_error', extensionPath: secret, error: secret,
  })
  store.ingestRpcEvent('rt-one', {
    type: 'message_update', message: { content: secret }, assistantMessageEvent: { delta: secret },
  })
  const serialized = JSON.stringify(store.events({}, 'alpha'))
  assert.equal(serialized.includes(secret), false)
  assert.equal(store.snapshot('alpha').sessions[0].activity, 'needs-approval')
  assert.equal(store.events({}, 'alpha').events.filter((e) => e.detail.type === 'tool').length, 2)
})

test('model, usage and compaction telemetry preserve bounded counters', () => {
  const store = setup()
  store.ingestRpcEvent('rt-one', {
    type: 'message_end', message: {
      role: 'assistant', model: 'qwen3.8', provider: 'local',
      usage: { input: 100, output: 20, cacheRead: 30, cacheWrite: 5, cost: { total: 0.01 } },
      content: 'never exported',
    },
  })
  store.ingestRpcEvent('rt-one', { type: 'config_update', thinkingLevel: 'medium' })
  store.updateModel('rt-one', { contextTokens: 7200, contextWindow: 32768 })
  store.ingestRpcEvent('rt-one', { type: 'compaction_end', aborted: false, result: 'private summary' })
  store.ingestRpcEvent('rt-one', { type: 'compaction_end', aborted: true, errorMessage: 'private error' })
  const session = store.snapshot('alpha').sessions[0]
  assert.deepEqual(session.lastMessageUsage, {
    input: 100, output: 20, cacheRead: 30, cacheWrite: 5, costUsd: 0.01,
  })
  assert.equal(session.model?.modelId, 'qwen3.8')
  assert.equal(session.model?.thinkingLevel, 'medium')
  assert.equal(session.model?.contextTokens, 7200)
  assert.equal(session.compactions, 1)
  assert.equal(JSON.stringify(store.events({}, 'alpha')).includes('private'), false)
})

test('replay cursors report gaps, paging and restarted stream IDs', () => {
  const store = setup(3)
  for (let i = 1; i <= 6; i++) {
    store.ingestRpcEvent('rt-one', { type: 'auto_retry_start', attempt: i })
  }
  const first = store.events({ sinceSeq: 0, streamId: 'run-1', limit: 2 }, 'alpha')
  assert.equal(first.events.length, 2)
  assert.equal(first.hasGap, true)
  assert.equal(first.resetRequired, false)
  const second = store.events({ sinceSeq: first.nextSeq, streamId: 'run-1' }, 'alpha')
  assert.equal(second.events.length, 1)
  const changed = store.events({ streamId: 'old-run', sinceSeq: 1 }, 'alpha')
  assert.equal(changed.resetRequired, true)
  assert.equal(changed.events.length, 0)
  const future = store.events({ sinceSeq: 9999 }, 'alpha')
  assert.equal(future.resetRequired, true)
})

test('auth, project allowlist, malformed queries and commands fail closed', () => {
  const handle = createCrystalTelemetryHandler(setup())
  const trusted = { authenticated: true, allowedProjectIds: new Set(['alpha']) }
  const query = { version: 1, operation: 'telemetry.snapshot', projectId: 'alpha' }
  assert.throws(() => handle(query, { ...trusted, authenticated: false }), /Unauthorized/)
  assert.throws(() => handle(query, { authenticated: true, allowedProjectIds: new Set() }), /Unauthorized/)
  assert.throws(() => handle({ ...query, projectId: 'beta' }, trusted), /not allowed/)
  assert.throws(() => handle({ ...query, operation: 'prompt' }, trusted), /Unsupported/)
  assert.throws(() => handle({ ...query, version: 2 }, trusted), /Invalid/)
  assert.throws(() => handle({ ...query, operation: 'telemetry.events', limit: 999 }, trusted), /page size/)
  assert.throws(() => handle({ ...query, operation: 'telemetry.events', sinceSeq: -1 }, trusted), /cursor/)
  const capabilities = handle({ ...query, operation: 'telemetry.capabilities' }, trusted)
  assert.ok('authority' in capabilities)
  assert.equal(capabilities.authority, 'observation-only')
})

test('unexpected process loss stays unknown, never accepted', () => {
  const store = setup()
  store.ingestRpcEvent('rt-one', { type: 'agent_start' })
  store.upsertRuntime({ ...runtime, status: 'error', activity: 'working' })
  assert.equal(store.snapshot('alpha').sessions[0].activity, 'unknown')
  store.upsertRuntime({ ...runtime, status: 'stopped', activity: 'working' })
  assert.equal(store.snapshot('alpha').sessions[0].activity, 'unknown')
  store.upsertRuntime({ ...runtime, status: 'running', activity: 'needs-approval' })
  store.ingestRpcEvent('rt-one', { type: 'status_change', status: 'stopped' })
  assert.equal(store.snapshot('alpha').sessions[0].activity, 'unknown')
  assert.equal(JSON.stringify(store.snapshot('alpha')).includes('accepted'), false)
})

function seedRuntimeEvidence(store: PiTelemetryStore): void {
  store.ingestRpcEvent(runtime.runtimeId, {
    type: 'message_end', message: {
      role: 'assistant', model: 'old-model', provider: 'old-provider',
      usage: { input: 100, output: 20 },
    },
  })
  store.ingestRpcEvent(runtime.runtimeId, { type: 'compaction_end', aborted: false })
}

for (const destinationProject of ['beta', 'alpha', undefined]) {
  test(`runtime workspace reuse drops prior evidence (destination: ${destinationProject ?? 'unbound'})`, () => {
    const store = setup()
    store.syncWorkspaces([workspace, { id: 'ws-two', name: 'Other' }])
    if (destinationProject) store.bindWorkspace('ws-two', { projectId: destinationProject })
    seedRuntimeEvidence(store)
    const cursor = store.events().currentSeq
    const delivered: unknown[] = []
    store.subscribe((event) => delivered.push(event))

    store.upsertRuntime({ ...runtime, workspaceId: 'ws-two', sessionId: 's-two' })
    const session = store.snapshot().sessions[0]
    assert.equal(session.workspaceId, 'ws-two')
    assert.equal(session.sessionId, 's-two')
    assert.equal(session.correlation, undefined)
    assert.equal(session.model, undefined)
    assert.equal(session.lastMessageUsage, undefined)
    assert.equal(session.compactions, 0)
    const events = store.events({ sinceSeq: cursor }).events
    assert.equal(events.length, 2)
    assert.deepEqual(delivered, events)
    const closure = events.find((event) => event.workspaceId === 'ws-one')!
    assert.equal(closure.sessionId, 's-one')
    assert.deepEqual(closure.correlation, { projectId: 'alpha', taskId: 'T1', attemptId: 'A1' })
    assert.deepEqual(closure.detail, {
      type: 'runtime', process: 'stopped', activity: 'idle', active: false, closed: true,
    })
    const arrival = events.find((event) => event.workspaceId === 'ws-two')!
    assert.equal(arrival.sessionId, 's-two')
    assert.deepEqual(arrival.correlation, destinationProject ? { projectId: destinationProject } : undefined)
    assert.deepEqual(arrival.detail, { type: 'runtime', process: 'running', activity: 'idle', active: false })
    if (destinationProject !== 'alpha') {
      assert.equal(store.snapshot('alpha').sessions.length, 0)
      assert.deepEqual(store.snapshot('alpha').workspaces.map((w) => w.workspaceId), ['ws-one'])
      const oldEvents = store.events({ sinceSeq: cursor }, 'alpha').events
      assert.deepEqual(oldEvents, [closure])
    }
    // Old replay evidence remains scoped to its original workspace and attempt.
    const oldUsage = store.events({}, 'alpha').events.find((e) => e.detail.type === 'usage')!
    assert.equal(oldUsage.workspaceId, 'ws-one')
    assert.equal(oldUsage.sessionId, 's-one')
    assert.equal(oldUsage.correlation?.attemptId, 'A1')
    if (!destinationProject) store.bindWorkspace('ws-two', { projectId: 'beta' })
    const projectId = destinationProject ?? 'beta'
    store.bindRuntime(runtime.runtimeId, { projectId, taskId: 'T2', attemptId: 'A2' })
    assert.equal(store.snapshot(projectId).sessions[0].correlation?.attemptId, 'A2')
    assert.throws(() => store.bindRuntime(runtime.runtimeId, { projectId: 'other' }), /another/)
  })
}

test('same-workspace updates retain valid evidence and correlation without duplicate events', () => {
  const store = setup()
  seedRuntimeEvidence(store)
  const before = store.snapshot('alpha').sessions[0]
  store.upsertRuntime(runtime)
  const cursor = store.events().currentSeq
  store.upsertRuntime(runtime)
  assert.equal(store.events().currentSeq, cursor)
  store.upsertRuntime({ ...runtime, active: true, sessionId: 's-updated' })
  const after = store.snapshot('alpha').sessions[0]
  assert.deepEqual(after.correlation, before.correlation)
  assert.deepEqual(after.model, before.model)
  assert.deepEqual(after.lastMessageUsage, before.lastMessageUsage)
  assert.equal(after.compactions, 1)
  const events = store.events({ sinceSeq: cursor }, 'alpha').events
  assert.equal(events.length, 1)
  assert.equal(events[0].sessionId, 's-updated')
  assert.deepEqual(events[0].correlation, before.correlation)
})

test('late or ambiguous closes cannot remove or misattribute a reused runtime', () => {
  const store = setup()
  seedRuntimeEvidence(store)
  store.syncWorkspaces([workspace, { id: 'ws-two', name: 'Other' }])
  store.bindWorkspace('ws-two', { projectId: 'beta' })
  const replacement = { ...runtime, workspaceId: 'ws-two', sessionId: 's-two', pid: 456 }
  store.upsertRuntime(replacement)
  store.bindRuntime(runtime.runtimeId, { projectId: 'beta', attemptId: 'B1' })
  const cursor = store.events().currentSeq
  for (const stale of [
    runtime,
    { ...replacement, sessionId: 's-one' },
    { ...replacement, sessionId: null },
    { ...replacement, pid: 123 },
    { ...replacement, pid: null },
  ]) store.upsertRuntime({ ...stale, status: 'stopped', closed: true })
  assert.equal(store.events().currentSeq, cursor)
  assert.equal(store.snapshot('beta').sessions[0].sessionId, 's-two')

  store.upsertRuntime({ ...replacement, status: 'stopped', closed: true })
  assert.equal(store.snapshot('beta').sessions.length, 0)
  const closed = store.events({ sinceSeq: cursor }, 'beta').events
  assert.equal(closed.length, 1)
  assert.equal(closed[0].workspaceId, 'ws-two')
  assert.equal(closed[0].sessionId, 's-two')
  assert.deepEqual(closed[0].correlation, { projectId: 'beta', attemptId: 'B1' })
  assert.deepEqual(closed[0].detail, {
    type: 'runtime', process: 'stopped', activity: 'idle', active: false, closed: true,
  })
  store.upsertRuntime({ ...replacement, status: 'stopped', closed: true })
  assert.equal(store.events().currentSeq, cursor + 1)

  const rebound = { ...replacement, sessionId: 's-three', pid: 789 }
  store.upsertRuntime(rebound)
  store.bindRuntime(runtime.runtimeId, { projectId: 'beta', attemptId: 'B2' })
  const beforeLateClose = store.snapshot('beta')
  store.upsertRuntime({ ...replacement, closed: true })
  assert.deepEqual(store.snapshot('beta'), beforeLateClose)
  const session = store.snapshot('beta').sessions[0]
  assert.equal(session.correlation?.attemptId, 'B2')
  assert.equal(session.model, undefined)
  assert.equal(session.lastMessageUsage, undefined)
  assert.equal(session.compactions, 0)
})

test('subscribers cannot mutate stored events or break the Pi observer', () => {
  const store = setup()
  const out: unknown[] = []
  store.subscribe(() => { throw new Error('UI disconnected') })
  const unsubscribe = store.subscribe((event) => {
    out.push(event)
    event.workspaceId = 'tampered'
  })
  store.ingestRpcEvent('rt-one', { type: 'agent_start' })
  assert.equal(out.length > 0, true)
  assert.equal(store.events({}, 'alpha').events.at(-1)?.workspaceId, 'ws-one')
  unsubscribe()
})
