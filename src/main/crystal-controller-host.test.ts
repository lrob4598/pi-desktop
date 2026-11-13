import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { startHostedCrystal } from './crystal-controller-host'

test('starts and stops the hosted controller with inherited configuration', async () => {
  const root = await mkdtemp(join(tmpdir(), 'crystal-host-'))
  const entry = join(root, 'controller.mjs')
  const receipt = join(root, 'started.json')
  await writeFile(entry, `import { writeFileSync } from 'node:fs';
writeFileSync(process.env.HOST_TEST_RECEIPT, JSON.stringify({ pid: process.pid, marker: process.env.HOST_TEST_MARKER }));
process.on('message', message => { if (message === 'crystal:shutdown') process.exit(0) });
process.on('disconnect', () => process.exit(0));
setInterval(() => {}, 1000);`)
  let stop: (() => void) | undefined
  try {
    stop = await startHostedCrystal({
      ...process.env,
      CRYSTAL_HOSTED_CONTROLLER_ENTRY: entry,
      CRYSTAL_HOSTED_NODE: process.execPath,
      HOST_TEST_RECEIPT: receipt,
      HOST_TEST_MARKER: 'forwarded',
    })
    let result: { pid: number; marker: string } | undefined
    for (let i = 0; i < 50 && !result; i++) {
      try { result = JSON.parse(await readFile(receipt, 'utf8')) } catch {
        await new Promise(resolve => setTimeout(resolve, 20))
      }
    }
    assert.equal(result?.marker, 'forwarded')
    assert.ok(result?.pid)
    stop()
    stop = undefined
    for (let i = 0; i < 50; i++) {
      try { process.kill(result.pid, 0) } catch { return }
      await new Promise(resolve => setTimeout(resolve, 20))
    }
    assert.fail('hosted controller did not exit')
  } finally {
    stop?.()
    await rm(root, { recursive: true, force: true })
  }
})

test('rejects relative hosted controller paths', async () => {
  await assert.rejects(() => startHostedCrystal({
    CRYSTAL_HOSTED_CONTROLLER_ENTRY: 'relative/controller.mjs',
    CRYSTAL_HOSTED_NODE: process.execPath,
  }), /absolute/)
})
