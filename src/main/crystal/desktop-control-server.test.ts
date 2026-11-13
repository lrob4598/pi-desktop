import assert from 'node:assert/strict'
import test from 'node:test'
import { randomUUID } from 'node:crypto'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createPiDesktopControlServer, listenPiDesktopControl } from './desktop-control-server'
import { PiDesktopControlClient } from './desktop-control'
import { FileProvisionJournal } from './desktop-control-journal'

test('native pipe discovers source and creates a single journaled Pi workspace', async () => {
  const temp = await mkdtemp(join(tmpdir(), 'crystal-pi-control-'))
  const pipePath = String.raw`\\.\pipe\crystal-test-${randomUUID()}`
  const token = 'crystal-control-test-secret-32-characters'
  const journal = new FileProvisionJournal(temp)
  let creates = 0
  const server = createPiDesktopControlServer({
    token, journal, allowedProjects: new Set(['cmms']),
    bridge: {
      listNativeWorkspaces: async () => [],
      getSourceWorkspace: async (project, id) =>
        project === 'cmms' && id === 'ws-cmms' ? { id, repoRoot: 'C:\\Fixtures\\Mini-CMMS' } : undefined,
      createNativeWorktree: async request => {
        creates++
        return {
          id: 'ws-created', path: 'C:\\Pi\\worktrees\\cmms',
          branch: 'pi/cmms-created', sourceWorkspaceId: request.sourceWorkspaceId,
          operationId: request.operationId,
        }
      },
    },
  })
  try {
    await listenPiDesktopControl(server, pipePath)
    const client = new PiDesktopControlClient({ token, pipePath })
    assert.deepEqual(await client.getSourceWorkspace('cmms', 'ws-cmms'),
      { id: 'ws-cmms', repoRoot: 'C:\\Fixtures\\Mini-CMMS' })
    assert.equal(await client.getSourceWorkspace('cmms', 'ws-missing'), undefined)
    const req = { operationId: 'cmms-001', sourceWorkspaceId: 'ws-cmms', projectId: 'cmms', name: 'CMMS equipment' }
    assert.deepEqual(await client.getCapabilities(), { version: 1, modelOverride: true })
    const first = await client.createIsolatedTab(req)
    assert.equal(first.state, 'created')
    assert.equal((await client.getProvisionStatus(req.operationId))?.state, 'created')
    assert.deepEqual(await client.createIsolatedTab(req), first)
    assert.equal(creates, 1, 'repeated operation must not create a second worktree')
    await assert.rejects(() => client.createIsolatedTab({ ...req, name: 'different' }), /different request/)
    await assert.rejects(() => client.createIsolatedTab({ ...req, provider: 'openai', model: 'different' }), /different request/)
    await assert.rejects(() => client.createIsolatedTab({ ...req, operationId: 'bad-model', provider: 'openai' }), /Invalid/)
    await assert.rejects(() => client.createIsolatedTab({ ...req, operationId: 'cmms-002', projectId: 'denied' }), /not allowed/)
    const badTokenClient = new PiDesktopControlClient({ token: 'not-the-correct-token-but-still-long', pipePath })
    await assert.rejects(() => badTokenClient.listWorkspaces(), /Unauthorized/)
    await assert.rejects(() => badTokenClient.getCapabilities(), /Unauthorized/)
    const reloaded = new FileProvisionJournal(temp)
    assert.equal((await reloaded.get(req.operationId))?.result.state, 'created')
  } finally {
    await new Promise<void>(resolve => server.close(() => resolve()))
    await rm(temp, { recursive: true, force: true })
  }
})
