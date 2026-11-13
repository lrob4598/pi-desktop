import { randomUUID } from 'node:crypto'
import {
  TELEMETRY_PROTOCOL_VERSION,
  TELEMETRY_SOURCE,
  TELEMETRY_OPERATIONS,
  type PiActivityState,
  type PiProcessState,
  type TelemetryCapabilities,
  type TelemetryCorrelation,
  type TelemetryEvent,
  type TelemetryEventDetail,
  type TelemetryEventsPage,
  type TelemetryEventsQuery,
  type TelemetryModel,
  type TelemetrySession,
  type TelemetrySnapshot,
  type TelemetryUsage,
  type TelemetryWorkspace,
} from '../../protocol/src/telemetry.ts'

export interface PiWorkspaceObservation {
  id: string
  name: string
  kind?: 'folder' | 'worktree' | 'repoSet'
  branch?: string
  managed?: boolean
}

export interface PiRuntimeObservation {
  runtimeId: string
  workspaceId: string
  sessionId: string | null
  status: PiProcessState
  activity: 'working' | 'needs-approval' | 'completed' | 'failed' | null
  active: boolean
  pid: number | null
  engine?: string
  startupPhase?: 'spawning' | 'waiting-on-engine'
  closed?: boolean
}

/** The deliberately small subset of Pi/OMP RPC events this observer recognizes. */
export interface PiRpcObservation {
  type: string
  [key: string]: unknown
}

export interface TelemetryStoreOptions {
  maxEvents?: number
  streamId?: string
  now?: () => Date
}

/**
 * In-memory, metadata-only read model. Crystal owns tasks and their outcomes;
 * this store owns neither a scheduler nor a Pi process or session file.
 */
export class PiTelemetryStore {
  static readonly MAX_PAGE_SIZE = 200
  readonly streamId: string

  private readonly maxEvents: number
  private readonly now: () => Date
  private readonly workspaces = new Map<string, TelemetryWorkspace>()
  private readonly sessions = new Map<string, TelemetrySession>()
  private readonly workspaceBindings = new Map<string, TelemetryCorrelation>()
  private readonly subscribers = new Set<(event: TelemetryEvent) => void>()
  private readonly retained: TelemetryEvent[] = []
  private sequence = 0

  constructor(options: TelemetryStoreOptions = {}) {
    const size = options.maxEvents ?? 1024
    if (!Number.isSafeInteger(size) || size < 1 || size > 50_000) {
      throw new Error('Invalid telemetry event retention')
    }
    this.maxEvents = size
    this.streamId = options.streamId ?? randomUUID()
    if (!this.streamId || this.streamId.length > 128) throw new Error('Invalid telemetry stream ID')
    this.now = options.now ?? (() => new Date())
  }

  capabilities(): TelemetryCapabilities {
    return {
      protocolVersion: TELEMETRY_PROTOCOL_VERSION,
      source: TELEMETRY_SOURCE,
      operations: TELEMETRY_OPERATIONS,
      maxPageSize: PiTelemetryStore.MAX_PAGE_SIZE,
      payloadPolicy: 'metadata-only',
      authority: 'observation-only',
    }
  }

  /** Trusted host-only association after Crystal admission; never a wire command. */
  bindWorkspace(workspaceId: string, correlation: TelemetryCorrelation): void {
    if (!this.workspaces.has(workspaceId)) throw new Error('Unknown workspace')
    const safe = sanitizeCorrelation(correlation)
    const existing = this.workspaceBindings.get(workspaceId)
    if (existing && existing.projectId !== safe.projectId) {
      throw new Error('Workspace already belongs to another Crystal project')
    }
    if ([...this.sessions.values()].some((session) =>
      session.workspaceId === workspaceId && session.correlation &&
      session.correlation.projectId !== safe.projectId)) {
      throw new Error('Workspace has a runtime bound to another Crystal project')
    }
    this.workspaceBindings.set(workspaceId, safe)
    const workspace = this.workspaces.get(workspaceId)!
    this.workspaces.set(workspaceId, { ...workspace, correlation: safe })
    this.emit(workspaceId, null, { type: 'workspace', action: 'seen' })
  }

  /** Exact source snapshot. No filesystem scanning, workspace creation or mutation. */
  syncWorkspaces(observations: readonly PiWorkspaceObservation[]): void {
    const present = new Set<string>()
    for (const ws of observations) {
      if (typeof ws.id !== 'string' || !ws.id) continue
      present.add(ws.id)
      const current: TelemetryWorkspace = {
        workspaceId: ws.id,
        name: short(ws.name, 160),
        kind: ws.kind === 'worktree' || ws.kind === 'repoSet' ? ws.kind : 'folder',
        ...(typeof ws.branch === 'string' ? { branch: short(ws.branch, 160) } : {}),
        ...(typeof ws.managed === 'boolean' ? { managed: ws.managed } : {}),
        ...(this.workspaceBindings.has(ws.id) ? { correlation: this.workspaceBindings.get(ws.id)! } : {}),
      }
      const previous = this.workspaces.get(ws.id)
      this.workspaces.set(ws.id, current)
      if (!previous || JSON.stringify(previous) !== JSON.stringify(current)) {
        this.emit(ws.id, null, { type: 'workspace', action: 'seen' })
      }
    }
    for (const id of this.workspaces.keys()) {
      if (present.has(id)) continue
      this.emit(id, null, { type: 'workspace', action: 'removed' })
      this.workspaces.delete(id)
      this.workspaceBindings.delete(id)
    }
  }

  upsertRuntime(raw: PiRuntimeObservation): void {
    if (!raw.runtimeId || !raw.workspaceId) return
    const current = this.sessions.get(raw.runtimeId)
    if (raw.closed) {
      // A runtime ID alone is not an incarnation identity. An old workspace or
      // session may close after this ID has already been reused elsewhere.
      const sessionId = typeof raw.sessionId === 'string' ? short(raw.sessionId, 160) : null
      const matches = current && current.workspaceId === raw.workspaceId &&
        current.sessionId === sessionId &&
        (current.pid === null || raw.pid === current.pid)
      if (matches) {
        const prior = current
        this.emit(prior.workspaceId, prior.runtimeId, {
          type: 'runtime', process: 'stopped', activity: prior.activity, active: false, closed: true,
        })
        this.sessions.delete(raw.runtimeId)
      }
      return
    }
    // Workspace ownership is a trust boundary, even when the destination has
    // no binding yet. Close the old observation before replacing it so its
    // project subscribers can remove it using only the old correlation. Never
    // infer the destination project/attempt or evidence from a reused runtime
    // ID; only an unchanged workspace may retain the previous state.
    if (current && current.workspaceId !== raw.workspaceId) {
      this.emit(current.workspaceId, current.runtimeId, {
        type: 'runtime', process: 'stopped', activity: current.activity, active: false, closed: true,
      })
    }
    const prior = current?.workspaceId === raw.workspaceId ? current : undefined
    const session: TelemetrySession = {
      runtimeId: raw.runtimeId,
      workspaceId: raw.workspaceId,
      sessionId: typeof raw.sessionId === 'string' ? short(raw.sessionId, 160) : null,
      process: raw.status,
      activity: normalizeActivity(raw.status, raw.activity),
      active: raw.active === true,
      pid: typeof raw.pid === 'number' && Number.isSafeInteger(raw.pid) && raw.pid > 0 ? raw.pid : null,
      ...(typeof raw.engine === 'string' ? { engine: short(raw.engine, 60) } : {}),
      ...(raw.startupPhase ? { startupPhase: raw.startupPhase } : {}),
      ...(prior?.model ? { model: prior.model } : {}),
      ...(prior?.lastMessageUsage ? { lastMessageUsage: prior.lastMessageUsage } : {}),
      compactions: prior?.compactions ?? 0,
      lastObservedAt: this.now().toISOString(),
      ...(prior?.correlation ? { correlation: prior.correlation } : {}),
    }
    this.sessions.set(raw.runtimeId, session)
    const changed = !prior || JSON.stringify({ ...prior, lastObservedAt: '' }) !==
      JSON.stringify({ ...session, lastObservedAt: '' })
    if (changed) {
      this.emit(raw.workspaceId, raw.runtimeId, {
        type: 'runtime', process: session.process, activity: session.activity, active: session.active,
      })
    }
  }

  /** Trusted host-only attempt binding; never inferred from conversation text. */
  bindRuntime(runtimeId: string, correlation: TelemetryCorrelation): void {
    const session = this.sessions.get(runtimeId)
    if (!session) throw new Error('Unknown runtime')
    const safe = sanitizeCorrelation(correlation)
    if (session.correlation && session.correlation.projectId !== safe.projectId) {
      throw new Error('Runtime already belongs to another Crystal project')
    }
    if (this.workspaceBindings.has(session.workspaceId) &&
        this.workspaceBindings.get(session.workspaceId)!.projectId !== safe.projectId) {
      throw new Error('Runtime project differs from workspace project')
    }
    this.sessions.set(runtimeId, { ...session, correlation: safe })
    this.emit(session.workspaceId, runtimeId, {
      type: 'runtime', process: session.process, activity: session.activity, active: session.active,
    })
  }

  /**
   * Pi/OMP event projection, deliberately ignoring message content, tool
   * args/results, raw errors, extension paths and unknown event types.
   */
  ingestRpcEvent(runtimeId: string, event: PiRpcObservation): void {
    const session = this.sessions.get(runtimeId)
    if (!session || !event || typeof event.type !== 'string') return
    const publish = (detail: TelemetryEventDetail): void => this.emit(session.workspaceId, runtimeId, detail)
    switch (event.type) {
      case 'agent_start':
        this.setActivity(runtimeId, 'working')
        publish({ type: 'turn', phase: 'started' })
        break
      case 'agent_end':
        this.setActivity(runtimeId, 'completed')
        publish({ type: 'turn', phase: 'ended' })
        break
      case 'tool_execution_start':
      case 'tool_execution_end':
        publish({
          type: 'tool',
          phase: event.type === 'tool_execution_start' ? 'started' : 'ended',
          toolName: short(event.toolName, 128),
          toolCallId: short(event.toolCallId, 128),
          ...(event.type === 'tool_execution_end' ? { isError: event.isError === true } : {}),
        })
        break
      case 'extension_ui_request':
        if (['select', 'confirm', 'input', 'editor'].includes(String(event.method))) {
          this.setActivity(runtimeId, 'needs-approval')
          publish({ type: 'approval', phase: 'requested' })
        }
        break
      case 'message_end': {
        const message = asRecord(event.message)
        if (message?.role !== 'assistant') break
        const usage = toUsage(asRecord(message.usage))
        if (usage) {
          this.sessions.set(runtimeId, { ...this.sessions.get(runtimeId)!, lastMessageUsage: usage })
          publish({ type: 'usage', usage })
        }
        const model = toModel(message)
        if (model) this.updateModel(runtimeId, model)
        break
      }
      case 'config_update': {
        const model = toModel(asRecord(event.model))
        const thinkingLevel = typeof event.thinkingLevel === 'string'
          ? short(event.thinkingLevel, 60) : undefined
        if (model || thinkingLevel) this.updateModel(runtimeId, { ...model, ...(thinkingLevel ? { thinkingLevel } : {}) })
        break
      }
      case 'compaction_start':
        publish({ type: 'compaction', phase: 'started' })
        break
      case 'compaction_end':
        if (event.aborted !== true) {
          const current = this.sessions.get(runtimeId)!
          this.sessions.set(runtimeId, { ...current, compactions: current.compactions + 1 })
        }
        publish({
          type: 'compaction', phase: 'ended',
          aborted: event.aborted === true, willRetry: event.willRetry === true,
        })
        break
      case 'auto_retry_start':
        publish({ type: 'retry', phase: 'started', attempt: nonnegative(event.attempt) })
        break
      case 'auto_retry_end':
        publish({
          type: 'retry', phase: 'ended',
          attempt: nonnegative(event.attempt), success: event.success === true,
        })
        break
      case 'queue_update':
        publish({
          type: 'queue',
          steeringCount: Array.isArray(event.steering) ? event.steering.length : 0,
          followUpCount: Array.isArray(event.followUp) ? event.followUp.length : 0,
        })
        break
      case 'extension_error':
        publish({ type: 'diagnostic', code: 'extension-error' })
        break
      case 'prompt_result':
        if (event.status === 'error') publish({ type: 'diagnostic', code: 'prompt-error' })
        break
      case 'status_change': {
        const existing = this.sessions.get(runtimeId)
        if (!existing) break
        const status = event.status
        if (status === 'stopped' || status === 'starting' || status === 'running' || status === 'error') {
          const activity = (status === 'error' || status === 'stopped') &&
            (existing.activity === 'working' || existing.activity === 'needs-approval')
            ? 'unknown' : existing.activity
          this.sessions.set(runtimeId, { ...existing, process: status, activity })
          publish({ type: 'runtime', process: status, activity, active: existing.active })
          if (status === 'error') publish({ type: 'diagnostic', code: 'process-error' })
        }
        break
      }
      case 'session_info_update':
        if (typeof event.sessionId === 'string') {
          const current = this.sessions.get(runtimeId)!
          this.sessions.set(runtimeId, { ...current, sessionId: short(event.sessionId, 160) })
        }
        break
      default:
        break
    }
  }

  /** Host may supply read-only get_state/model context observations. */
  updateModel(runtimeId: string, observation: TelemetryModel): void {
    const session = this.sessions.get(runtimeId)
    if (!session) return
    const model: TelemetryModel = {
      ...(session.model ?? {}),
      ...(typeof observation.provider === 'string' ? { provider: short(observation.provider, 128) } : {}),
      ...(typeof observation.modelId === 'string' ? { modelId: short(observation.modelId, 192) } : {}),
      ...(typeof observation.thinkingLevel === 'string' ? { thinkingLevel: short(observation.thinkingLevel, 60) } : {}),
      ...(observation.contextTokens !== undefined ? { contextTokens: nonnegative(observation.contextTokens) } : {}),
      ...(observation.contextWindow !== undefined ? { contextWindow: nonnegative(observation.contextWindow) } : {}),
    }
    if (JSON.stringify(session.model) === JSON.stringify(model)) return
    this.sessions.set(runtimeId, { ...session, model })
    this.emit(session.workspaceId, runtimeId, { type: 'model', model })
  }

  /** Optional sanitized Git/test observations from the owning host, not completion proof. */
  observeGit(runtimeId: string, observation: { branch?: string; commit?: string; dirty?: boolean }): void {
    const session = this.sessions.get(runtimeId)
    if (!session) return
    this.emit(session.workspaceId, runtimeId, {
      type: 'git',
      ...(typeof observation.branch === 'string' ? { branch: short(observation.branch, 160) } : {}),
      ...(typeof observation.commit === 'string' ? { commit: short(observation.commit, 64) } : {}),
      ...(typeof observation.dirty === 'boolean' ? { dirty: observation.dirty } : {}),
    })
  }

  observeTests(runtimeId: string, observation: { outcome: 'passed' | 'failed' | 'unknown'; passed?: number; failed?: number }): void {
    const session = this.sessions.get(runtimeId)
    if (!session) return
    this.emit(session.workspaceId, runtimeId, {
      type: 'test', outcome: observation.outcome,
      ...(observation.passed !== undefined ? { passed: nonnegative(observation.passed) } : {}),
      ...(observation.failed !== undefined ? { failed: nonnegative(observation.failed) } : {}),
    })
  }

  snapshot(projectId?: string): TelemetrySnapshot {
    const sessions = [...this.sessions.values()].filter((s) => this.projectFor(s) === projectId || projectId === undefined)
    const visibleIds = new Set(sessions.map((s) => s.workspaceId))
    const workspaces = [...this.workspaces.values()].filter((w) =>
      projectId === undefined || w.correlation?.projectId === projectId || visibleIds.has(w.workspaceId))
    return clone({
      protocolVersion: TELEMETRY_PROTOCOL_VERSION,
      source: TELEMETRY_SOURCE,
      streamId: this.streamId,
      currentSeq: this.sequence,
      observedAt: this.now().toISOString(),
      workspaces,
      sessions,
    })
  }

  events(query: TelemetryEventsQuery = {}, projectId?: string): TelemetryEventsPage {
    const cursor = query.sinceSeq ?? 0
    const resetRequired = (query.streamId !== undefined && query.streamId !== this.streamId) ||
      !Number.isSafeInteger(cursor) || cursor < 0 || cursor > this.sequence
    const oldestAvailableSeq = this.retained[0]?.seq ?? this.sequence + 1
    const hasGap = !resetRequired && cursor < oldestAvailableSeq - 1
    const limit = Math.max(1, Math.min(PiTelemetryStore.MAX_PAGE_SIZE, query.limit ?? 100))
    const visible = resetRequired ? [] : this.retained
      .filter((ev) => ev.seq > cursor && (projectId === undefined || ev.correlation?.projectId === projectId))
      .slice(0, limit)
    const last = visible.at(-1)?.seq
    const unreturned = !resetRequired && this.retained.some((ev) =>
      ev.seq > (last ?? cursor) && (projectId === undefined || ev.correlation?.projectId === projectId))
    return clone({
      protocolVersion: TELEMETRY_PROTOCOL_VERSION,
      streamId: this.streamId,
      currentSeq: this.sequence,
      oldestAvailableSeq,
      nextSeq: resetRequired ? cursor : unreturned ? (last ?? cursor) : this.sequence,
      hasGap,
      resetRequired,
      events: visible,
    })
  }

  subscribe(listener: (event: TelemetryEvent) => void): () => void {
    this.subscribers.add(listener)
    return () => this.subscribers.delete(listener)
  }

  private projectFor(session: TelemetrySession): string | undefined {
    return session.correlation?.projectId ?? this.workspaceBindings.get(session.workspaceId)?.projectId
  }

  private setActivity(runtimeId: string, activity: PiActivityState): void {
    const session = this.sessions.get(runtimeId)
    if (!session || session.activity === activity) return
    const next = { ...session, activity, lastObservedAt: this.now().toISOString() }
    this.sessions.set(runtimeId, next)
    this.emit(session.workspaceId, runtimeId, {
      type: 'runtime', process: session.process, activity, active: session.active,
    })
  }

  private emit(workspaceId: string, runtimeId: string | null, detail: TelemetryEventDetail): void {
    const session = runtimeId ? this.sessions.get(runtimeId) : null
    const correlation = session?.correlation ?? this.workspaceBindings.get(workspaceId)
    const event: TelemetryEvent = {
      protocolVersion: TELEMETRY_PROTOCOL_VERSION,
      source: TELEMETRY_SOURCE,
      streamId: this.streamId,
      seq: ++this.sequence,
      observedAt: this.now().toISOString(),
      workspaceId,
      runtimeId,
      sessionId: session?.sessionId ?? null,
      ...(correlation ? { correlation } : {}),
      detail,
    }
    this.retained.push(clone(event))
    if (this.retained.length > this.maxEvents) this.retained.shift()
    for (const listener of this.subscribers) {
      try { listener(clone(event)) } catch { /* telemetry cannot break Pi event handling */ }
    }
  }
}

function normalizeActivity(status: PiProcessState, activity: PiRuntimeObservation['activity']): PiActivityState {
  if ((status === 'error' || status === 'stopped') &&
      (activity === 'working' || activity === 'needs-approval')) return 'unknown'
  if (activity === 'working' || activity === 'needs-approval' ||
      activity === 'completed' || activity === 'failed') return activity
  return status === 'error' ? 'unknown' : 'idle'
}

function short(value: unknown, max: number): string {
  return typeof value === 'string' ? value.slice(0, max) : ''
}

function nonnegative(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : 0
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown> : null
}

function toUsage(value: Record<string, unknown> | null): TelemetryUsage | null {
  if (!value) return null
  const cost = asRecord(value.cost)
  return {
    input: nonnegative(value.input),
    output: nonnegative(value.output),
    cacheRead: nonnegative(value.cacheRead),
    cacheWrite: nonnegative(value.cacheWrite),
    ...(typeof cost?.total === 'number' && Number.isFinite(cost.total) ?
      { costUsd: nonnegative(cost.total) } : {}),
  }
}

function toModel(message: Record<string, unknown> | null): TelemetryModel | null {
  if (!message) return null
  const model = asRecord(message.model)
  const id = typeof message.model === 'string' ? message.model : model?.id
  const provider = message.provider ?? model?.provider
  const output: TelemetryModel = {}
  if (typeof id === 'string') output.modelId = short(id, 192)
  if (typeof provider === 'string') output.provider = short(provider, 128)
  return Object.keys(output).length ? output : null
}

function sanitizeCorrelation(correlation: TelemetryCorrelation): TelemetryCorrelation {
  if (typeof correlation.projectId !== 'string' || !correlation.projectId.trim() ||
      correlation.projectId.length > 128) throw new Error('Invalid telemetry project identity')
  const result: TelemetryCorrelation = { projectId: correlation.projectId }
  for (const key of ['taskId', 'attemptId', 'operationId'] as const) {
    const value = correlation[key]
    if (value !== undefined) {
      if (typeof value !== 'string' || value.length < 1 || value.length > 160) {
        throw new Error('Invalid telemetry correlation')
      }
      result[key] = value
    }
  }
  return result
}

function clone<T>(value: T): T {
  return structuredClone(value)
}
