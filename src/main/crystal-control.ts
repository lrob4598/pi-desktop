import { app, BrowserWindow } from 'electron'
import { readFileSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import path from 'node:path'
import type { WorkspaceManager } from './workspace-manager'
import type { AppSettings } from '../shared/ipc-contracts'
import { startNewTabInBackground } from './ipc/workspace-handlers'
import { inspectGitRepository } from './git-worktree'
import { createPiDesktopControlServer, listenPiDesktopControl } from './crystal/desktop-control-server'
import { FileProvisionJournal } from './crystal/desktop-control-journal'
import { startCrystalNativeTelemetry } from './crystal/native-telemetry-host'
import { startHostedCrystal } from './crystal-controller-host'

function normalize(value: string): string {
  return path.win32.resolve(value).replace(/[\\/]+$/, '').toLowerCase()
}

function originBaseCommit(cwd: string): string | undefined {
  let defaultRef: string | undefined
  try {
    defaultRef = execFileSync('git', ['symbolic-ref', '--quiet', 'refs/remotes/origin/HEAD'],
      { cwd, encoding: 'utf8', windowsHide: true, stdio: ['ignore', 'pipe', 'ignore'] }).trim()
  } catch { /* A local repository can have no remote default branch. */ }
  for (const ref of [defaultRef, 'refs/remotes/origin/main', 'refs/remotes/origin/master']) {
    if (!ref) continue
    try {
      const commit = execFileSync('git', ['rev-parse', '--verify', ref + '^{commit}'],
        { cwd, encoding: 'utf8', windowsHide: true, stdio: ['ignore', 'pipe', 'ignore'] }).trim()
      if (/^[a-f0-9]{40}$/i.test(commit)) return commit
    } catch { /* Try the next available remote reference. */ }
  }
  return undefined
}
export async function startCrystalControl(
  workspaceManager: WorkspaceManager,
  settings: AppSettings,
  getWindow: () => BrowserWindow | null,
): Promise<() => Promise<void>> {
  if (process.env.CRYSTAL_PI_CONTROL !== '1') return async () => {}
  const tokenFile = process.env.CRYSTAL_PI_TOKEN_FILE
  const projectsFile = process.env.CRYSTAL_PI_PROJECTS_FILE
  if (!tokenFile || !projectsFile) throw new Error('Crystal control requires token and project map files')
  const token = readFileSync(tokenFile, 'utf8').trim()
  const projectRoots = JSON.parse(readFileSync(projectsFile, 'utf8')) as Record<string, string>
  const allowedProjects = new Set(Object.keys(projectRoots).filter(id =>
    /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(id) && path.win32.isAbsolute(projectRoots[id] ?? '')
  ))
  if (allowedProjects.size === 0) throw new Error('Crystal control project allowlist is empty')
  const journal = new FileProvisionJournal(path.join(app.getPath('userData'), 'crystal-provision-journal'))
  const sourceCreations = new Map<string, Promise<ReturnType<WorkspaceManager['getWorkspaces']>[number]>>()
  const resolveSource = async (projectId: string, sourceId: string) => {
    const root = projectRoots[projectId]
    if (!allowedProjects.has(projectId) || !root) return undefined
    let source = workspaceManager.getWorkspaces().find(w =>
      sourceId === `project:${projectId}`
        ? w.kind !== 'worktree' && normalize(w.path) === normalize(root)
        : w.id === sourceId)
    if (!source && sourceId === `project:${projectId}`) {
      let creating = sourceCreations.get(projectId)
      if (!creating) {
        creating = (async () => {
          const git = await inspectGitRepository(root)
          if (normalize(git.repoRoot) !== normalize(root)) throw new Error('Project root is not the enrolled repository')
          return workspaceManager.createWorkspace(projectId, root)
        })()
        sourceCreations.set(projectId, creating)
      }
      try { source = await creating } finally { sourceCreations.delete(projectId) }
    }
    if (!source) return undefined
    const git = await inspectGitRepository(source.path).catch(() => null)
    return git && normalize(git.repoRoot) === normalize(root) ? source : undefined
  }
  const server = createPiDesktopControlServer({
    token,
    journal,
    allowedProjects,
    bridge: {
      listNativeWorkspaces: async () => {
        // Only return journal-created workspaces that still exist in native Pi Desktop.
        const existing = new Set(workspaceManager.getWorkspaces().map(w => w.id))
        return (await journal.listCreated()).flatMap(entry => {
          const workspace = entry.result.workspace
          return workspace && existing.has(workspace.id) && allowedProjects.has(entry.request.projectId)
            ? [workspace] : []
        })
      },
      getSourceWorkspace: async (projectId, sourceId) => {
        const source = await resolveSource(projectId, sourceId)
        return source ? { id: sourceId, repoRoot: projectRoots[projectId]! } : undefined
      },
      createNativeWorktree: async input => {
        const source = await resolveSource(input.projectId, input.sourceWorkspaceId)
        if (!source)
          throw new Error('Source workspace is not enrolled under project root')
        // This calls the EXACT native path used by the Pi Desktop new-tab button.
        // No taskPrompt is supplied; Crystal delivers through Messenger once session is live.
        const workspace = await workspaceManager.createWorktreeWorkspace({
          sourceWorkspaceId: source.id,
          name: input.name,
          startPi: true,
          ...(input.sourceWorkspaceId === `project:${input.projectId}`
            ? { baseCommit: originBaseCommit(source.path) } : {}),
        })
        if (!workspace.branch || workspace.kind !== 'worktree') throw new Error('Native worktree metadata missing')
        startNewTabInBackground(workspaceManager, workspace, settings, undefined,
          input.provider && input.model ? { provider: input.provider, model: input.model } : undefined)
        getWindow()?.webContents.send('crystal:workspace-created', workspace.id)
        return {
          id: workspace.id, path: workspace.path, branch: workspace.branch,
          sourceWorkspaceId: input.sourceWorkspaceId, operationId: input.operationId,
        }
      },
    },
  })
  await listenPiDesktopControl(server)
  let stopTelemetry: () => void = () => {}
  if (process.env.CRYSTAL_PI_TELEMETRY === '1') {
    try {
      const telemetryTokenFile = process.env.CRYSTAL_PI_TELEMETRY_TOKEN_FILE
      const telemetryToken = telemetryTokenFile ? readFileSync(telemetryTokenFile, 'utf8').trim() : token
      stopTelemetry = await startCrystalNativeTelemetry(workspaceManager, journal, telemetryToken, allowedProjects,
        process.env.CRYSTAL_PI_TELEMETRY_PIPE)
    } catch (error) {
      console.error('Crystal Pi telemetry unavailable:', error)
    }
  }
  let stopController: () => Promise<void>
  try {
    stopController = await startHostedCrystal()
  } catch (error) {
    stopTelemetry()
    server.close()
    throw error
  }
  return async () => {
    await stopController()
    stopTelemetry()
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
  }
}
