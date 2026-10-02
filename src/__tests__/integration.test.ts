/**
 * Opt-in integration suite exercising RedisJM against a REAL Redis server.
 *
 * Why this exists (a mock can't validate these): the real Lua scripts (atomic pop across lanes,
 * compare-and-set transitions) and their reply shapes through ioredis, the `HSCAN`/`SSCAN`/
 * `ZRANGEBYSCORE` reply shapes, and genuine cross-connection visibility.
 *
 * HOW TO RUN:
 *   REDIS_URL=redis://localhost:6379 pnpm test
 *   # or via the dedicated script (defaults REDIS_URL to redis://localhost:6379):
 *   pnpm run test:integration
 *
 * When `REDIS_URL` is unset the whole suite is skipped (via `describe.skipIf`), so CI / local dev
 * without a Redis server stays green. Requires Redis >= 7.0 (shebang scripts).
 *
 * ISOLATION: every test uses a unique target-group prefix (`itg-<ts>-<n>`) so runs never collide; only
 * keys under `redisjm:itg-*` are deleted after each test (see `integration-helpers.ts`).
 *
 * TIMERS: unlike the mock unit tests, this suite uses REAL timers (no `vi.useFakeTimers`). All
 * intervals are kept short (heartbeats 100–200ms, poll 50ms, delays 200–500ms) and every assertion of
 * an eventual state polls via `until()` with a generous timeout, so the suite is deterministic and runs
 * in a few seconds. Every manager is always `stop()`ped after each test so no timer leaks.
 */
import { describe, expect, it } from 'vitest'
import { REDIS_URL, sleep, until, useSharedRedis } from './integration-helpers'

describe.skipIf(!REDIS_URL)('integration (real Redis)', () => {
  const h = useSharedRedis('itg')
  const { newGroup, newManager } = h
  /** Poll interval passed to every manager.start() in the suite. */
  const POLL_MS = 50

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

  it('2. one pop script across two lanes: both lanes drain', async () => {
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
    await h.redis.sadd(locksKey, jobId)
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

  it('9. stale-then-recovered run self-heals: resurrects to running, re-locks, and finishes truthfully', async () => {
    // WHY (stale-then-recovered asymmetry): a pinned handler is staled by a peer's maintenance
    // (lock released), then recovers and writes again. Its updates used to be rejected (status 'stale')
    // while its finish (fenced on executionId only) still landed — a record reading `finished` with
    // frozen progress and dropped attrs. The fix resurrects the run on its next write.
    const group = newGroup()
    // Runner: heartbeatInterval large enough that its OWN heartbeat never fires in the test window, so
    // recovery here is driven purely by the handler's post-stale setProgress/setAttrs (the onUpdate path).
    const runner = newManager({ heartbeatInterval: 60_000, roundsToStale: 2, maintenanceInterval: 0 }, group)
    // Maintainer on the same group: tiny intervals → staleThreshold = 100 * 2 = 200ms.
    const maintainer = newManager({ heartbeatInterval: 100, roundsToStale: 2, maintenanceInterval: 0 }, group)

    let releaseGate1!: () => void
    let releaseGate2!: () => void
    const gate1 = new Promise<void>((r) => { releaseGate1 = r })
    const gate2 = new Promise<void>((r) => { releaseGate2 = r })
    const job = runner.createJob({ jobName: 'recover' }, async (_i: Record<string, unknown>, ctx) => {
      await ctx.setProgress(0.5)
      await gate1                        // pinned here while the maintainer stales the record + frees the lock
      await ctx.setProgress(1)           // was rejected under the bug; now resurrects the run + re-locks it
      await ctx.setAttrs({ done: true }) // was dropped under the bug; now lands
      await gate2                        // pause again so the test can observe the re-established lock
    })
    let finishExecutionId: string | undefined
    runner.hook('finish', (p) => { finishExecutionId = p.executionId })
    const jobId = job.getJobId('r1')

    // Runner claims + starts, reaches progress 0.5, then blocks on gate1 (do NOT await — it isn't done).
    expect(await job.queue('r1', {})).toBe(true)
    const runnerRun = runner.popAndExecute()
    await until(async () => (await runner.get(jobId))?.progress === 0.5)
    expect((await runner.get(jobId))?.status).toBe('running')

    // Let the runner's heartbeat lapse, then the maintainer stales it and releases its lock.
    await sleep(400)
    const maint = await maintainer.performMaintenance()
    expect(maint.staleCount).toBeGreaterThanOrEqual(1)
    expect((await maintainer.get(jobId))?.status).toBe('stale')
    expect(await maintainer.isLocked(jobId)).toBe(false)

    // Recovery: the handler resumes and writes again. The stale record self-heals back to running with
    // its final progress/attrs, observed from the independent maintainer connection.
    releaseGate1()
    await until(async () => {
      const r = await maintainer.get(jobId)
      return r?.status === 'running' && r?.progress === 1 && (r?.attrs as { done?: boolean })?.done === true
    })
    const resurrected = await maintainer.get(jobId)
    expect(resurrected?.finishedAt).toBeUndefined()     // maintenance's finishedAt cleared
    expect(await maintainer.isLocked(jobId)).toBe(true)  // lock re-established → dup enqueue blocked
    expect(await job.queue('r1', {})).toBe(false)

    // Finish: releasing gate2 lets the handler return; the record ends truthful and the lock is released.
    releaseGate2()
    await runnerRun
    await until(async () => (await maintainer.get(jobId))?.status === 'finished')
    const finalRecord = await maintainer.get(jobId)
    expect(finalRecord?.status).toBe('finished')
    expect(finalRecord?.progress).toBe(1)
    expect(finalRecord?.attrs).toEqual({ done: true })
    expect(finalRecord?.executionId).toBe(finishExecutionId)
    expect(await maintainer.isLocked(jobId)).toBe(false)
  })

  it('10. retry off a staled run re-locks the delayed record so the backoff window stays dedupe-protected', async () => {
    // WHY (stale-then-recovered asymmetry, retry branch): a pinned run is staled by a peer's
    // maintenance (lock released) and then throws a RETRYABLE error without writing an update first.
    // The retry branch stages a `delayed` record but assumes the lock is still held — so without the
    // self-heal the delayed record would sit UNLOCKED all through backoff (a producer could re-enqueue
    // and swallow the retry) and keep the maintenance-stamped finishedAt.
    const group = newGroup()
    // Runner: heartbeatInterval large so its own heartbeat never fires; a long backoff keeps the retry
    // delayed for the assertions. attempts:2 → the first failure retries rather than finalizing.
    const runner = newManager({ heartbeatInterval: 60_000, roundsToStale: 2, maintenanceInterval: 0 }, group)
    const maintainer = newManager({ heartbeatInterval: 100, roundsToStale: 2, maintenanceInterval: 0 }, group)

    let releaseGate!: () => void
    const gate = new Promise<void>((r) => { releaseGate = r })
    const job = runner.createJob({ jobName: 'retry-stale', attempts: 2, backoff: 60_000 }, async () => {
      await gate                          // pinned here while the maintainer stales the record + frees the lock
      throw new Error('boom after stale') // throws with NO intervening update → onError retry branch
    })
    let retries = 0
    let finalErrors = 0
    runner.hook('retry', () => { retries++ })
    runner.hook('error', () => { finalErrors++ })
    const jobId = job.getJobId('r1')

    // Runner claims + starts, then blocks on the gate (do NOT await — it isn't done).
    expect(await job.queue('r1', {})).toBe(true)
    const runnerRun = runner.popAndExecute()
    await until(async () => (await runner.get(jobId))?.status === 'running')

    // Let the runner's heartbeat lapse, then the maintainer stales it and releases its lock.
    await sleep(400)
    const maint = await maintainer.performMaintenance()
    expect(maint.staleCount).toBeGreaterThanOrEqual(1)
    expect((await maintainer.get(jobId))?.status).toBe('stale')
    expect(await maintainer.isLocked(jobId)).toBe(false)

    // Recovery via the error path: the handler throws, the retry stages a delayed record and RE-LOCKS
    // it — observed from the independent maintainer connection.
    releaseGate()
    await runnerRun
    await until(async () => (await maintainer.get(jobId))?.status === 'delayed')
    const delayed = await maintainer.get(jobId)
    expect(delayed?.finishedAt).toBeUndefined()          // stale finishedAt cleared (run isn't terminal)
    expect(delayed?.readyAt).toBeDefined()
    expect(await maintainer.isLocked(jobId)).toBe(true)   // lock re-established → delayed invariant restored
    expect(await job.queue('r1', {})).toBe(false)          // dup enqueue blocked during backoff
    expect(retries).toBe(1)
    expect(finalErrors).toBe(0)                            // a retry is not a final failure
  })
})
