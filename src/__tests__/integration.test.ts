/**
 * Opt-in integration suite exercising RedisJM against a REAL Redis server.
 *
 * Why this exists (a mock can't validate these): the real `LMPOP` reply shape through ioredis, the
 * Redis < 7 sequential-`LPOP` fallback trigger (which keys off the real "unknown command" error text),
 * the `HSCAN`/`SSCAN`/`ZRANGEBYSCORE` reply shapes, and genuine cross-connection visibility.
 *
 * HOW TO RUN:
 *   REDIS_URL=redis://localhost:6379 pnpm test
 *   # or via the dedicated script (defaults REDIS_URL to redis://localhost:6379):
 *   pnpm run test:integration
 *
 * When `REDIS_URL` is unset the whole suite is skipped (via `describe.skipIf`), so CI / local dev
 * without a Redis server stays green.
 *
 * ISOLATION: every test uses a unique target-group prefix (`itg-<ts>-<n>`, no ':' — the constructor
 * rejects that) so runs never collide. `afterEach` best-effort deletes only keys under `redisjm:itg-*`.
 *
 * REDIS VERSION: the `LMPOP` code path needs Redis >= 7. On 6.x the library falls back to sequential
 * `LPOP` automatically; the fallback-sensitive test (2) detects the server version and only logs which
 * path was taken rather than hard-asserting internals, so the suite passes on either.
 *
 * TIMERS: unlike the mock unit tests, this suite uses REAL timers (no `vi.useFakeTimers`). All
 * intervals are kept short (heartbeats 100–200ms, poll 50ms, delays 200–500ms) and every assertion of
 * an eventual state polls via `until()` with a generous timeout, so the suite is deterministic and runs
 * in a few seconds. Every manager is always `stop()`ped in `afterEach` so no timer leaks.
 */
import Redis from 'ioredis'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { RedisJM } from '../redisjm'
import type { RedisJMOptions } from '../types'

const REDIS_URL = process.env.REDIS_URL

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `fn` every `intervalMs` until it resolves truthy, or throws once `timeoutMs` elapses. Used for
 * every "eventually" assertion so the suite tolerates real-timer jitter without brittle exact sleeps.
 */
async function until(fn: () => boolean | Promise<boolean>, timeoutMs = 3000, intervalMs = 50): Promise<void> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    if (await fn()) return
    if (Date.now() > deadline) throw new Error(`until(): condition not met within ${timeoutMs}ms`)
    await sleep(intervalMs)
  }
}

describe.skipIf(!REDIS_URL)('integration (real Redis)', () => {
  let redis: Redis
  let redisMajorVersion = 0
  // Managers created within a test are tracked here so afterEach always stops them (no timer leak),
  // even if the test throws mid-way.
  const managers: RedisJM[] = []
  let groupCounter = 0
  /** Poll interval passed to every manager.start() in the suite. */
  const POLL_MS = 50

  /** Fresh, unique, ':'-free target group per test so concurrent/leftover keys never collide. */
  const newGroup = (): string => `itg-${Date.now()}-${++groupCounter}`

  /**
   * Creates a manager on a fresh target group (or a shared one, when passed) and registers it for
   * guaranteed teardown in afterEach.
   */
  const newManager = (options: RedisJMOptions, group = newGroup()): RedisJM => {
    const manager = new RedisJM(redis, group, options)
    managers.push(manager)
    return manager
  }

  beforeAll(async () => {
    redis = new Redis(REDIS_URL!, { maxRetriesPerRequest: null })
    // Wait for a live connection before any test runs.
    await redis.ping()
    const info = await redis.info('server')
    const match = /redis_version:(\d+)\./.exec(info)
    redisMajorVersion = match ? Number(match[1]) : 0
  })

  afterAll(async () => {
    if (redis) await redis.quit()
  })

  afterEach(async () => {
    // Stop every manager this test started (drains in-flight runs, clears poll/maintenance timers).
    await Promise.all(managers.splice(0).map((m) => m.stop().catch(() => {})))
    // Best-effort cleanup of only this suite's keys. KEYS is O(N) but fine in a test-only suite against
    // an isolated `itg-*` keyspace.
    const keys = await redis.keys('redisjm:itg-*')
    if (keys.length) await redis.del(...keys)
  })

  it('1. end-to-end lifecycle: queue → execute → finished, lock released, dup rejected', async () => {
    const manager = newManager({ heartbeatInterval: 200, maintenanceInterval: 0 })
    let received: unknown
    const job = manager.createJob<{ foo: string }>({ jobName: 'lifecycle' }, async (inputs) => {
      received = inputs
    })
    const jobId = job.getJobId('run1')

    expect(await job.queue('run1', { foo: 'bar' })).toBe(true)
    // Second queue of the same runId while it is still queued (lock held) → false.
    expect(await job.queue('run1', { foo: 'bar' })).toBe(false)

    manager.start(POLL_MS)
    await until(async () => (await manager.get(jobId))?.status === 'finished')

    expect(received).toEqual({ foo: 'bar' })
    // Default retention keeps the terminal record observable.
    const record = await manager.get(jobId)
    expect(record?.status).toBe('finished')
    expect(record?.progress).toBe(0)
    // Lock released once terminal.
    expect(await manager.isLocked(jobId)).toBe(false)
  })

  it('2. LMPOP path across two lanes (both execute); logs LMPOP vs LPOP-fallback path', async () => {
    const manager = newManager({ heartbeatInterval: 200, maintenanceInterval: 0 })
    const ran = new Set<string>()
    const jobA = manager.createJob({ jobName: 'lane-a', lane: 'alpha' }, async () => {
      ran.add('a')
    })
    const jobB = manager.createJob({ jobName: 'lane-b', lane: 'beta' }, async () => {
      ran.add('b')
    })

    expect(await jobA.queue('r', {})).toBe(true)
    expect(await jobB.queue('r', {})).toBe(true)

    manager.start(POLL_MS)
    await until(() => ran.has('a') && ran.has('b'))

    // Version-aware: assert behavior (both lanes drained), not internals. Log which pop path ran.
    const popPath = redisMajorVersion >= 7 ? 'LMPOP' : 'sequential LPOP fallback'
    console.log(`[integration] ${popPath} path exercised (redis major ${redisMajorVersion})`)
    expect(await manager.queueSize('alpha')).toBe(0)
    expect(await manager.queueSize('beta')).toBe(0)
  })

  it('3. delayed run: not executed before readyAt, executed after; delayed→finished visible', async () => {
    const manager = newManager({ heartbeatInterval: 200, maintenanceInterval: 0 })
    let ran = false
    const job = manager.createJob({ jobName: 'delayed' }, async () => {
      ran = true
    })
    const jobId = job.getJobId('d1')

    expect(await job.queue('d1', {}, undefined, { delay: 400 })).toBe(true)
    manager.start(POLL_MS)

    // Before readyAt: still delayed, not executed. (Promotion granularity is ~1s, so 200ms is safely
    // before the run becomes poppable.)
    await sleep(200)
    expect(ran).toBe(false)
    expect((await manager.get(jobId))?.status).toBe('delayed')

    // After promotion (~1s) it runs; generous timeout for real-timer jitter.
    await until(async () => (await manager.get(jobId))?.status === 'finished', 3000)
    expect(ran).toBe(true)
  })

  it('4. retries: fail twice then succeed → 2 retries, no final error, lock held during backoff', async () => {
    const manager = newManager({ heartbeatInterval: 200, maintenanceInterval: 0 })
    let attempts = 0
    const job = manager.createJob({ jobName: 'flaky', attempts: 3, backoff: 100 }, async () => {
      attempts++
      if (attempts < 3) throw new Error(`fail #${attempts}`)
    })
    const jobId = job.getJobId('run1')

    let retryCount = 0
    let errorFired = false
    let queueDuringBackoff: boolean | undefined
    manager.hook('retry', async () => {
      retryCount++
      // On the first retry (record staged on the delayed set, lock still held) a duplicate enqueue of
      // the same runId must be rejected.
      if (retryCount === 1) queueDuringBackoff = await job.queue('run1', {})
    })
    manager.hook('error', () => {
      errorFired = true
    })

    expect(await job.queue('run1', {})).toBe(true)
    manager.start(POLL_MS)

    await until(async () => (await manager.get(jobId))?.status === 'finished', 6000)

    expect(attempts).toBe(3)
    expect(retryCount).toBe(2)
    expect(errorFired).toBe(false)
    expect(queueDuringBackoff).toBe(false)
    expect((await manager.get(jobId))?.status).toBe('finished')
  })

  it('5. concurrency: two 300ms jobs finish in parallel (<550ms) at concurrency 2, serialized (>550ms) at 1', async () => {
    const runTwo = async (concurrency: number): Promise<number> => {
      const manager = newManager({ heartbeatInterval: 500, maintenanceInterval: 0, concurrency })
      let done = 0
      const job = manager.createJob({ jobName: 'slow' }, async () => {
        await sleep(300)
        done++
      })
      expect(await job.queue('a', {})).toBe(true)
      expect(await job.queue('b', {})).toBe(true)
      const t0 = Date.now()
      manager.start(POLL_MS)
      await until(() => done === 2, 4000)
      const elapsed = Date.now() - t0
      await manager.stop()
      return elapsed
    }

    const parallel = await runTwo(2)
    const serial = await runTwo(1)
    console.log(`[integration] concurrency 2 elapsed=${parallel}ms, concurrency 1 elapsed=${serial}ms`)
    expect(parallel).toBeLessThan(550)
    expect(serial).toBeGreaterThan(550)
  })

  it('6. stale reclaim + fencing end-to-end: zombie run cannot corrupt its successor', async () => {
    const group = newGroup()
    // Zombie manager: heartbeatInterval large enough that its heartbeat timer never fires within the
    // test window, so its running record goes stale from the SECOND manager's point of view.
    const zombie = newManager({ heartbeatInterval: 60_000, roundsToStale: 2, maintenanceInterval: 0 }, group)
    // Successor manager: tiny intervals → staleThreshold = 100 * 2 = 200ms.
    const successor = newManager({ heartbeatInterval: 100, roundsToStale: 2, maintenanceInterval: 0 }, group)

    // Both managers register the SAME jobName so the successor can execute the re-queued run.
    let releaseGate!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve
    })
    const zombieJob = zombie.createJob({ jobName: 'zjob' }, async () => {
      await gate // block until the test releases it
    })
    let successorRan = false
    let successorExecutionId: string | undefined
    const successorJob = successor.createJob({ jobName: 'zjob' }, async () => {
      successorRan = true
    })
    // Capture the successor execution's fencing token so we can prove it owns the terminal record.
    successor.hook('finish', (p) => {
      successorExecutionId = p.executionId
    })
    const jobId = zombieJob.getJobId('z1')

    // Zombie claims and starts the run, then blocks in its handler (do NOT await — it never finishes yet).
    expect(await zombieJob.queue('z1', {})).toBe(true)
    const zombieRun = zombie.popAndExecute()
    await until(async () => (await zombie.get(jobId))?.status === 'running')

    // Let the running heartbeat lapse past the successor's stale threshold, then have the successor's
    // maintenance reclaim it (marks stale, releases the lock).
    await sleep(400)
    const maint = await successor.performMaintenance()
    expect(maint.staleCount).toBeGreaterThanOrEqual(1)
    expect((await successor.get(jobId))?.status).toBe('stale')
    expect(await successor.isLocked(jobId)).toBe(false)

    // Re-queue the same runId (lock is free) and let the SUCCESSOR run it to a fresh terminal record.
    expect(await successorJob.queue('z1', {})).toBe(true)
    expect(await successor.popAndExecute()).toBe(true)
    expect(successorRan).toBe(true)
    await until(async () => (await successor.get(jobId))?.status === 'finished')
    const successorRecord = await successor.get(jobId)

    // Now unblock the zombie: its late `finish` hook fires against a record it no longer owns
    // (executionId mismatch), so the fencing guard must make it a no-op.
    releaseGate()
    await zombieRun

    // The zombie's completion must NOT have corrupted the successor's terminal record or re-locked it.
    const finalRecord = await successor.get(jobId)
    expect(finalRecord?.status).toBe('finished')
    expect(finalRecord?.executionId).toBe(successorExecutionId)
    expect(finalRecord?.executionId).toBe(successorRecord?.executionId)
    expect(await successor.isLocked(jobId)).toBe(false)
  })

  it('7. maintenance orphan-lock reclaim: record-less lock released, runId queueable after', async () => {
    const group = newGroup()
    // Tiny intervals → staleThreshold = 100 * 2 = 200ms.
    const manager = newManager({ heartbeatInterval: 100, roundsToStale: 2, maintenanceInterval: 0 }, group)
    const job = manager.createJob({ jobName: 'orphan' }, async () => {})
    const jobId = job.getJobId('o1')
    const locksKey = `redisjm:${group}:locks`

    // Manually seed a lock with NO backing log record (an enqueue that crashed between SADD and HSET).
    await redis.sadd(locksKey, jobId)
    expect(await manager.isLocked(jobId)).toBe(true)
    // queue() must be blocked while the orphan lock is held (and, having returned false, writes nothing).
    expect(await job.queue('o1', {})).toBe(false)

    // Pass 1: stamp the record-less lock as a suspect. Pass 2 (after the stale threshold) reclaims it.
    await manager.performMaintenance()
    await sleep(300)
    const result = await manager.performMaintenance()
    expect(result.staleCount).toBeGreaterThanOrEqual(1)

    expect(await manager.isLocked(jobId)).toBe(false)
    expect(await job.queue('o1', {})).toBe(true)
  })

  it('8. stats() / queueSize(): queued + delayed + running counted correctly', async () => {
    const manager = newManager({ heartbeatInterval: 60_000, maintenanceInterval: 0 })
    let releaseGate!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve
    })
    const job = manager.createJob({ jobName: 'stat' }, async (inputs: { block?: boolean }) => {
      if (inputs.block) await gate
    })

    // running: queue + pop (not awaited) a handler that blocks on the gate.
    expect(await job.queue('r1', { block: true })).toBe(true)
    const running = manager.popAndExecute()
    await until(async () => (await manager.get(job.getJobId('r1')))?.status === 'running')

    // queued: stays on the default lane (nothing pops it — we only popped once above).
    expect(await job.queue('q1', {})).toBe(true)
    // delayed: staged on the delayed set with a long delay so it never promotes during the test.
    expect(await job.queue('d1', {}, undefined, { delay: 60_000 })).toBe(true)

    const stats = await manager.stats()
    expect(stats.queues.default).toBe(1) // only q1 is on the live default queue
    expect(stats.delayed).toBe(1)
    expect(stats.locks).toBe(3) // running + queued + delayed all hold locks
    expect(stats.statuses.running).toBe(1)
    expect(stats.statuses.queued).toBe(1)
    expect(stats.statuses.delayed).toBe(1)

    expect(await manager.queueSize()).toBe(1) // default lane depth (delayed/running not on the list)

    // Release the blocked run so afterEach's stop() drains cleanly.
    releaseGate()
    await running
  })
})
