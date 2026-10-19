/**
 * Real-Redis integration tests for the 0.5.0 features: named resources shared across jobs and
 * instances (the Lua enqueue / transition / purge / prune scripts), crash and stale-slot recovery, and
 * the `maxRunIdBytes` / `validate` refusals. Only this suite's own `redisjm:ires-*` keys are touched.
 *
 * Skipped when `REDIS_URL` is unset.
 *
 * HOW TO RUN: `REDIS_URL=redis://localhost:6379 pnpm test`
 */
import { describe, expect, it } from 'vitest'
import { RedisJMEnqueueError } from '../errors'
import { Job } from '../job'
import { REDIS_SERVER_BIN, REDIS_URL, sleep, startHung, until, useDedicatedRedis, useSharedRedis } from './integration-helpers'

/** A resource's set of holders in `group`. */
const resKey = (group: string, name: string): string => `redisjm:${group}:res:${name}:locks`
/** The resource registry of `group`. */
const resRegistry = (group: string): string => `redisjm:${group}:resources`
/** The job registry of `group`. */
const jobRegistry = (group: string): string => `redisjm:${group}:jobs`

describe.skipIf(!REDIS_URL)('0.5.0 integration (shared Redis)', () => {
  const h = useSharedRedis('ires', { logger: false, maintenanceInterval: 0 })
  const { newGroup, newManager } = h

  // WHY: the cap is only worth anything if it holds under concurrent producers on separate connections.
  it('several instances enqueueing concurrently never admit more than the cap across jobs', async () => {
    const group = newGroup()
    const options = { resourceCaps: { writes: 3 } }
    const managers = [newManager(options, group, h.newClient()), newManager(options, group, h.newClient()), newManager(options, group, h.newClient())]
    const jobs = managers.flatMap((m) => [
      new Job({ jobName: 'reconcile', resources: ['writes'] }, async () => {}),
      new Job({ jobName: 'evaluate', resources: ['writes'] }, async () => {}),
    ])
    const results = await Promise.all(Array.from({ length: 60 }, (_, i) => {
      const m = managers[i % managers.length]
      return m.enqueue(jobs[i % jobs.length], `r${i}`, null)
    }))
    const queued = results.filter((r) => r.status === 'queued')
    expect(queued).toHaveLength(3)
    expect(results.filter((r) => r.status === 'busy').every((r) => r.limitedBy === 'writes')).toBe(true)
    expect(await managers[0].resourceInFlightCount('writes')).toBe(3)
    expect((await h.redis.smembers(resKey(group, 'writes'))).sort()).toEqual(queued.map((r) => r.jobId).sort())
    // Batches from two instances at once: still exactly the cap.
    for (const r of queued) await managers[1].unqueue(r.jobId)
    const batches = await Promise.all(managers.slice(0, 2).map((m, k) =>
      m.enqueueMany(jobs[k], Array.from({ length: 10 }, (_, i) => ({ runId: `b${k}-${i}`, inputs: null })))))
    expect(batches.flat().filter((r) => r.status === 'queued')).toHaveLength(3)
  })

  it('workers on several instances never run more holders of a resource than its cap', async () => {
    const group = newGroup()
    const options = { resourceCaps: { writes: 2 }, concurrency: 4, heartbeatInterval: 200 }
    let running = 0
    let peak = 0
    let done = 0
    const handler = async (): Promise<void> => {
      running++
      peak = Math.max(peak, running)
      await sleep(15)
      running--
      done++
    }
    const managers = [newManager(options, group, h.newClient()), newManager(options, group, h.newClient())]
    const jobs = managers.map((m) => [
      m.createJob({ jobName: 'a', resources: ['writes'] }, handler),
      m.createJob({ jobName: 'b', resources: ['writes'] }, handler),
    ])
    for (const m of managers) m.start(5)
    // Producers retry on 'busy' (admission cap, no wait queue): 20 distinct runs eventually all execute.
    const pending = new Set(Array.from({ length: 20 }, (_, i) => i))
    await until(async () => {
      for (const i of [...pending]) {
        const { status } = await jobs[i % 2][i % 2].enqueue(`r${i}`, null, managers[i % 2])
        if (status === 'queued') pending.delete(i)
      }
      return pending.size === 0
    }, 10_000, 10)
    await until(() => done === 20, 10_000)
    expect(peak).toBeLessThanOrEqual(2)
    expect(peak).toBe(2)
    await until(async () => (await managers[0].resourceInFlightCount('writes')) === 0)
  })

  it('multi-resource enqueue is atomic in Lua: a full second resource takes nothing from the first', async () => {
    const group = newGroup()
    const m = newManager({ resourceCaps: { api: 5, writes: 1 } }, group)
    const w = new Job({ jobName: 'w', resources: ['writes'] }, async () => {})
    const both = new Job({ jobName: 'both', resources: ['api', 'writes'] }, async () => {})
    expect((await m.enqueue(w, '1', null)).status).toBe('queued')
    expect(await m.enqueue(both, '1', null)).toEqual({ status: 'busy', jobId: 'both#1', limitedBy: 'writes' })
    expect(await h.redis.scard(resKey(group, 'api'))).toBe(0)
    expect(await m.isLocked('both#1')).toBe(false)
    const limited = new Job({ jobName: 'one', resources: ['api'], maxInFlight: 1 }, async () => {})
    await m.enqueue(limited, '1', null)
    expect(await m.enqueue(limited, '2', null)).toEqual({ status: 'busy', jobId: 'one#2', limitedBy: 'job' })
    expect(await h.redis.smembers(resRegistry(group))).toEqual(expect.arrayContaining(['api', 'writes']))
  })

  // WHY: a pod that dies mid-run must not hold the shared slot beyond the stale threshold.
  it('a crashed holder (heartbeat lapsed) is staled by another instance\'s maintenance and its slot freed', async () => {
    const group = newGroup()
    const m = newManager({ resourceCaps: { writes: 1 }, heartbeatInterval: 50, roundsToStale: 2 }, group)
    const job = new Job({ jobName: 'w', resources: ['writes'] }, async () => {})
    expect((await m.enqueue(job, 'crashed', null)).status).toBe('queued')
    // The worker that claimed it died: record `running` with a long-dead heartbeat, lock + slot held.
    const record = JSON.parse((await h.redis.hget(`redisjm:${group}:log`, 'w#crashed'))!)
    await h.redis.lrem(`redisjm:${group}:queue`, 0, 'w#crashed')
    await h.redis.hset(`redisjm:${group}:log`, 'w#crashed', JSON.stringify({
      ...record, status: 'running', startedAt: Date.now() - 10_000, heartbeat: Date.now() - 10_000, executionId: 'dead',
    }))
    expect((await m.enqueue(job, 'next', null)).limitedBy).toBe('writes')

    const other = newManager({ resourceCaps: { writes: 1 }, heartbeatInterval: 50, roundsToStale: 2 }, group)
    expect((await other.performMaintenance()).staleCount).toBe(1)
    expect((await other.get('w#crashed'))?.status).toBe('stale')
    expect(await other.resourceInFlightCount('writes')).toBe(0)
    expect((await m.enqueue(job, 'next', null)).status).toBe('queued')
  })

  // WHY: maintenance prunes only registered sets; the Lua re-take of a resurrection must re-register the
  // job and its resources (the registries may have dropped them while empty), or a later record-less
  // release would leak the members forever.
  it('a resurrection re-registers its job and resources; a later record-less release is pruned without a new enqueue', async () => {
    const group = newGroup()
    const m = newManager({ resourceCaps: { writes: 1 }, heartbeatInterval: 50, roundsToStale: 2 }, group)
    const job = new Job({ jobName: 'w', resources: ['writes'] }, async () => {})
    await m.enqueue(job, '1', null)
    const log = `redisjm:${group}:log`
    const record = JSON.parse((await h.redis.hget(log, 'w#1'))!)
    await h.redis.lrem(`redisjm:${group}:queue`, 0, 'w#1')
    await h.redis.hset(log, 'w#1', JSON.stringify({
      ...record, status: 'running', startedAt: Date.now() - 10_000, heartbeat: Date.now() - 10_000, executionId: 'dead',
    }))
    expect((await m.performMaintenance()).staleCount).toBe(1)
    await m.performMaintenance()
    expect(await h.redis.smembers(jobRegistry(group))).toEqual([])
    expect(await h.redis.smembers(resRegistry(group))).toEqual([])

    const resurrect = (m as any).updateLog('w#1', (r: any) => {
      r.status = 'running'
      r.heartbeat = Date.now()
      delete r.finishedAt
      delete r.staleReason
    })
    expect((await resurrect).outcome).toBe('written')
    expect(await h.redis.smembers(resKey(group, 'writes'))).toEqual(['w#1'])
    expect(await h.redis.smembers(jobRegistry(group))).toEqual(['w'])
    expect(await h.redis.smembers(resRegistry(group))).toEqual(['writes'])

    await h.redis.srem(`redisjm:${group}:locks`, 'w#1')
    await h.redis.hdel(log, 'w#1')
    await m.performMaintenance()
    expect(await h.redis.scard(`redisjm:${group}:jobs:w:locks`)).toBe(0)
    expect(await h.redis.scard(resKey(group, 'writes'))).toBe(0)
    expect(await h.redis.smembers(jobRegistry(group))).toEqual([])
    expect(await h.redis.smembers(resRegistry(group))).toEqual([])
  })

  it('maxRunMs stales a hung holder and frees its slot', async () => {
    const group = newGroup()
    const m = newManager({ resourceCaps: { writes: 1 }, maxRunMs: 20, heartbeatInterval: 1000, abortGraceMs: 20 }, group)
    await startHung(m, { resources: ['writes'] })
    expect(await m.resourceInFlightCount('writes')).toBe(1)
    await sleep(30)
    await m.performMaintenance()
    expect((await m.get('hang#r1'))?.staleReason).toBe('maxRunMs')
    expect(await m.resourceInFlightCount('writes')).toBe(0)
  })

  it('an orphaned lock (no record) is reclaimed and its leaked slot pruned; unqueue frees a slot', async () => {
    const group = newGroup()
    const m = newManager({ resourceCaps: { writes: 2 }, heartbeatInterval: 20, roundsToStale: 1 }, group)
    // A run that lost its record (e.g. an orphan purge of a pre-0.5 write): lock + slot, no record.
    await h.redis.sadd(`redisjm:${group}:locks`, 'w#orphan')
    await h.redis.sadd(resKey(group, 'writes'), 'w#orphan')
    await h.redis.sadd(resRegistry(group), 'writes')
    const job = new Job({ jobName: 'w', resources: ['writes'] }, async () => {})
    await m.enqueue(job, 'live', null)
    expect(await m.resourceInFlightCount('writes')).toBe(2)
    await m.performMaintenance() // first sighting of the record-less lock
    await sleep(40)
    await m.performMaintenance() // reclaims the lock …
    await m.performMaintenance() // … and the resource prune drops the slot
    expect(await m.isLocked('w#orphan')).toBe(false)
    expect(await h.redis.smembers(resKey(group, 'writes'))).toEqual(['w#live'])
    await m.unqueue('w#live')
    expect(await m.resourceInFlightCount('writes')).toBe(0)
    await m.performMaintenance()
    expect(await h.redis.smembers(resRegistry(group))).toEqual([])
  })

  it('refusals: run-id-too-large and invalid throw before any write (single) and are per-entry results (many)', async () => {
    const group = newGroup()
    const m = newManager({ maxRunIdBytes: 16 }, group)
    const job = new Job<{ ids: string[] }>({
      jobName: 'v',
      validate: ({ inputs }) => (inputs.ids.length > 2 ? 'too-many-ids' : undefined),
    }, async () => {})
    await expect(m.enqueue(job, 'x'.repeat(17), { ids: [] })).rejects.toMatchObject({ reason: 'run-id-too-large' })
    const err = await m.enqueue(job, 'big', { ids: ['a', 'b', 'c'] }).catch((e: unknown) => e) as RedisJMEnqueueError
    expect(err).toMatchObject({ reason: 'invalid', detail: 'too-many-ids' })
    expect(await h.redis.exists(`redisjm:${group}:log`)).toBe(0)
    const results = await m.enqueueMany(job, [
      { runId: 'x'.repeat(17), inputs: { ids: [] } },
      { runId: 'big', inputs: { ids: ['a', 'b', 'c'] } },
      { runId: 'ok', inputs: { ids: ['a'] } },
    ])
    expect(results.map((r) => r.status)).toEqual(['run-id-too-large', 'invalid', 'queued'])
    expect(await m.isLocked('v#ok')).toBe(true)
  })
})

describe.skipIf(!REDIS_URL || !REDIS_SERVER_BIN)('0.5.0 integration (dedicated maxmemory Redis)', () => {
  const h = useDedicatedRedis('ires-oom')
  const { fillToOom, newGroup, newManager } = h

  // WHY: the enqueue script is flag-less, so Redis refuses it whole — no slot may be taken by a refused
  // run; the deletion-only purge (allow-oom) must still free a slot while Redis is full.
  it('under OOM an enqueue takes no resource slot and unqueue still frees one', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0, resourceCaps: { writes: 2 } }, group)
    const job = new Job({ jobName: 'w', resources: ['writes'] }, async () => {})
    await m.enqueue(job, 'held', null)
    await fillToOom()
    await expect(m.enqueue(job, 'refused', null)).rejects.toMatchObject({ reason: 'oom' })
    expect(await h.redis.smembers(resKey(group, 'writes'))).toEqual(['w#held'])
    await m.unqueue('w#held')
    expect(await m.resourceInFlightCount('writes')).toBe(0)
  })
})
