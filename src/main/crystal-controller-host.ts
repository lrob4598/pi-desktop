import { spawn, type ChildProcess } from 'node:child_process'
import { isAbsolute } from 'node:path'

/**
 * Pi Desktop owns this controller's lifetime. The child runs the existing
 * Crystal scheduler and its exclusive data-root lock remains authoritative.
 */
export async function startHostedCrystal(env: NodeJS.ProcessEnv = process.env): Promise<() => void> {
  const entry = env.CRYSTAL_HOSTED_CONTROLLER_ENTRY
  if (!entry) return () => {}
  const node = env.CRYSTAL_HOSTED_NODE
  if (!isAbsolute(entry) || !node || !isAbsolute(node)) {
    throw new Error('Hosted Crystal requires absolute Node and controller paths')
  }

  const child: ChildProcess = spawn(node, ['--experimental-strip-types', entry], {
    env,
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  })
  child.stdout?.on('data', chunk => console.log('[Crystal]', String(chunk).trimEnd()))
  child.stderr?.on('data', chunk => console.error('[Crystal]', String(chunk).trimEnd()))
  await new Promise<void>((resolve, reject) => {
    child.once('spawn', resolve)
    child.once('error', reject)
  })
  child.once('exit', (code, signal) => {
    if (code !== 0) console.error('Hosted Crystal exited', { code, signal })
  })
  return () => {
    if (child.exitCode === null && child.signalCode === null && child.connected) {
      child.send('crystal:shutdown', error => {
        if (error && child.exitCode === null && child.signalCode === null) child.kill()
      })
    }
  }
}
