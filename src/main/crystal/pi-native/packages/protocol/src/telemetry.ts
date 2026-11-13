/**
 * Pi-Native -> Crystal telemetry v1.
 * Observation only. None of these states is a Crystal task acceptance receipt.
 * Message text, tool arguments/results, prompts, paths and credentials are excluded.
 */
export const TELEMETRY_PROTOCOL_VERSION = 1 as const
export const TELEMETRY_SOURCE = 'pi-native' as const
export const TELEMETRY_OPERATIONS = [
  'telemetry.capabilities', 'telemetry.snapshot', 'telemetry.events', 'telemetry.subscribe',
] as const

export type PiProcessState = 'stopped' | 'starting' | 'running' | 'error'
export type PiActivityState = 'idle' | 'working' | 'needs-approval' | 'completed' | 'failed' | 'unknown'
export type PiWorkspaceKind = 'folder' | 'worktree' | 'repoSet'

/** Bound only by the trusted host after Crystal authorizes a project/attempt. */
export interface TelemetryCorrelation {
  projectId: string
  taskId?: string
  attemptId?: string
  operationId?: string
}

export interface TelemetryWorkspace {
  workspaceId: string
  name: string
  kind: PiWorkspaceKind
  branch?: string
  managed?: boolean
  correlation?: TelemetryCorrelation
}

export interface TelemetryModel {
  provider?: string
  modelId?: string
  thinkingLevel?: string
  contextTokens?: number
  contextWindow?: number
}

export interface TelemetryUsage {
  input: number
  output: number
  cacheRead: number
  cacheWrite: number
  costUsd?: number
}

export interface TelemetrySession {
  runtimeId: string
  workspaceId: string
  sessionId: string | null
  process: PiProcessState
  activity: PiActivityState
  active: boolean
  pid: number | null
  engine?: string
  startupPhase?: 'spawning' | 'waiting-on-engine'
  model?: TelemetryModel
  lastMessageUsage?: TelemetryUsage
  compactions: number
  lastObservedAt: string
  correlation?: TelemetryCorrelation
}

export type TelemetryEventDetail =
  | { type: 'workspace'; action: 'seen' | 'removed' }
  | { type: 'runtime'; process: PiProcessState; activity: PiActivityState; active: boolean; closed?: boolean }
  | { type: 'turn'; phase: 'started' | 'ended' }
  | { type: 'tool'; phase: 'started' | 'ended'; toolName: string; toolCallId: string; isError?: boolean }
  | { type: 'approval'; phase: 'requested' }
  | { type: 'usage'; usage: TelemetryUsage }
  | { type: 'model'; model: TelemetryModel }
  | { type: 'compaction'; phase: 'started' | 'ended'; aborted?: boolean; willRetry?: boolean }
  | { type: 'retry'; phase: 'started' | 'ended'; attempt: number; success?: boolean }
  | { type: 'queue'; steeringCount: number; followUpCount: number }
  | { type: 'diagnostic'; code: 'process-error' | 'extension-error' | 'prompt-error' }
  | { type: 'git'; branch?: string; commit?: string; dirty?: boolean }
  | { type: 'test'; outcome: 'passed' | 'failed' | 'unknown'; passed?: number; failed?: number }

export interface TelemetryEvent {
  protocolVersion: typeof TELEMETRY_PROTOCOL_VERSION
  source: typeof TELEMETRY_SOURCE
  streamId: string
  seq: number
  observedAt: string
  workspaceId: string
  runtimeId: string | null
  sessionId: string | null
  correlation?: TelemetryCorrelation
  detail: TelemetryEventDetail
}

export interface TelemetrySnapshot {
  protocolVersion: typeof TELEMETRY_PROTOCOL_VERSION
  source: typeof TELEMETRY_SOURCE
  streamId: string
  currentSeq: number
  observedAt: string
  workspaces: TelemetryWorkspace[]
  sessions: TelemetrySession[]
}

export interface TelemetryEventsQuery {
  /** Exclusive cursor. Stream IDs change on host restart. */
  sinceSeq?: number
  streamId?: string
  limit?: number
}

export interface TelemetryEventsPage {
  protocolVersion: typeof TELEMETRY_PROTOCOL_VERSION
  streamId: string
  currentSeq: number
  oldestAvailableSeq: number
  nextSeq: number
  hasGap: boolean
  resetRequired: boolean
  events: TelemetryEvent[]
}

export interface TelemetryCapabilities {
  protocolVersion: typeof TELEMETRY_PROTOCOL_VERSION
  source: typeof TELEMETRY_SOURCE
  operations: typeof TELEMETRY_OPERATIONS
  maxPageSize: number
  payloadPolicy: 'metadata-only'
  authority: 'observation-only'
}

export type CrystalTelemetryRequest =
  | { version: 1; operation: 'telemetry.capabilities'; projectId: string }
  | { version: 1; operation: 'telemetry.snapshot'; projectId: string }
  | ({ version: 1; operation: 'telemetry.events'; projectId: string } & TelemetryEventsQuery)
  | ({ version: 1; operation: 'telemetry.subscribe'; projectId: string } & TelemetryEventsQuery)

/** Subscribe starts with a replay page, then ordered event frames and heartbeats. */
export type TelemetryStreamFrame =
  | { type: 'replay'; page: TelemetryEventsPage }
  | { type: 'event'; event: TelemetryEvent }
  | { type: 'heartbeat'; streamId: string; currentSeq: number; observedAt: string }

export type CrystalTelemetryResult = TelemetryCapabilities | TelemetrySnapshot | TelemetryEventsPage
