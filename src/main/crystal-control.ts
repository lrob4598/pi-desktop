import { app, BrowserWindow } from 'electron'
import { readFileSync } from 'node:fs'
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
function isWithin(candidate: string, root: string): boolean {
  const absolute = normalize(candidate)
  const base = normalize(root)
  return absolute === base || absolute.startsWith(base + '\\')
}

export async function startCrystalControl(
  workspaceManager: WorkspaceManager,
  settings: AppSettings,
  getWindow: () => BrowserWindow | null,
): Promise<() => void> {
  if (process.env.CRYSTAL_PI_CONTROL !== '1') return () => {}
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
        const source = workspaceManager.getWorkspaces().find(w => w.id === sourceId)
        const allowedRoot = projectRoots[projectId]
        if (!source || !allowedProjects.has(projectId) || !allowedRoot) return undefined
        const git = await inspectGitRepository(source.path).catch(() => null)
        const repoRoot = git?.repoRoot
        return repoRoot && isWithin(repoRoot, allowedRoot)
          ? { id: source.id, repoRoot } : undefined
      },
      createNativeWorktree: async input => {
        const source = workspaceManager.getWorkspaces().find(w => w.id === input.sourceWorkspaceId)
        const root = projectRoots[input.projectId]
        if (!source || !root || !isWithin(source.repoRoot ?? source.path, root))
          throw new Error('Source workspace is not enrolled under project root')
        // This calls the EXACT native path used by the Pi Desktop new-tab button.
        // No taskPrompt is supplied; Crystal delivers through Messenger once session is live.
        const workspace = await workspaceManager.createWorktreeWorkspace({
          sourceWorkspaceId: input.sourceWorkspaceId,
          name: input.name,
          startPi: true,
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
      stopTelemetry = await startCrystalNativeTelemetry(workspaceManager, journal, token, allowedProjects,
        process.env.CRYSTAL_PI_TELEMETRY_PIPE)
    } catch (error) {
      console.error('Crystal Pi telemetry unavailable:', error)
    }
  }
  let stopController: () => void
  try {
    stopController = await startHostedCrystal()
  } catch (error) {
    stopTelemetry()
    server.close()
    throw error
  }
  return () => { stopController(); stopTelemetry(); server.close() }
}
