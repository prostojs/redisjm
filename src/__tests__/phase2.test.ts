/**
 * Unit tests (mock Redis) for the 0.2.0 phase-2 features: atomic script enqueue (+ enqueueMany,
 * maxInFlight → 'busy', lane caps → 'full', maxInputsBytes), O(1) inFlight via per-job lock sets, the
 * claiming set + atomic pop, old-lane draining with allow-lists, bounded/cursor-persisted maintenance,
 * HPEXPIRE retention, health()/memoryPressure/eviction-policy warning, every(), listPage(),
 * laneConcurrency, wake(), and popAndExecute() vs stop().
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { RedisJMEnqueueError } from '../errors'
import { Job } from '../job'
import { RedisJM } from '../redisjm'
import { createMockRedis } from './mock-redis'
import { CLAIMING, jobLanes, jobLocks, laneKey, LOCKS, LOG, mockHelpers, QUEUE } from './unit-helpers'

describe('phase 2', () => {
  let redis: ReturnType<typeof createMockRedis>
  beforeEach(() => {
    redis = createMockRedis()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  const { newManager, readRecord, seed, setOf, listOf } = mockHelpers(() => redis)

  // ---------------------------------------------------------------------------------------------
  describe('atomic enqueue', () => {
    it('writes lock, record, lane entry and the bookkeeping sets in one script call', async () => {
      const m = newManager()
      const job = new Job({ jobName: 'e', lane: 'a' }, vi.fn())
      vi.mocked(redis.evalsha).mockClear()
      vi.mocked(redis.eval).mockClear()
      expect(await m.enqueue(job, 'r1', { x: 1 })).toEqual({ status: 'queued', jobId: 'e#r1' })
      expect(vi.mocked(redis.evalsha).mock.calls.length + vi.mocked(redis.eval).mock.calls.length).toBe(2) // NOSCRIPT once, then EVAL
      expect(listOf(laneKey('a'))).toEqual(['e#r1'])
      expect(setOf(LOCKS)).toEqual(['e#r1'])
      expect(setOf(jobLocks('e'))).toEqual(['e#r1'])
      expect(setOf(jobLanes('e'))).toEqual([laneKey('a')])
      expect(setOf('redisjm:g:jobs')).toEqual(['e'])
      const record = await readRecord('e#r1')
      expect(record?.status).toBe('queued')
      expect(record?.enqueuedAt).toBeTypeOf('number')
    })

    it('maxInFlight: a job at its limit is busy (nothing written); frees up once a run finishes', async () => {
      const m = newManager()
      const job = m.createJob({ jobName: 'single', maxInFlight: 1 }, vi.fn())
      expect((await m.enqueue(job, 'a', null)).status).toBe('queued')
      expect((await m.enqueue(job, 'b', null)).status).toBe('busy')
      expect((await m.enqueue(job, 'a', null)).status).toBe('deduped') // same run id → deduped wins
      expect(await readRecord('single#b')).toBeNull()
      expect(await m.isLocked('single#b')).toBe(false)
      // A delayed run counts too.
      expect(await m.queue(job, 'c', null, { delay: 1000 })).toBe(false)
      await m.popAndExecute()
      expect((await m.enqueue(job, 'b', null)).status).toBe('queued')
    })

    it('lane caps: the smaller of maxQueued and laneCaps applies; full writes nothing; delayed runs are not counted', async () => {
      const m = newManager({ laneCaps: { a: 3 } })
      const capped = new Job({ jobName: 'c', lane: 'a', maxQueued: 2 }, vi.fn())
      const other = new Job({ jobName: 'o', lane: 'a' }, vi.fn())
      expect((await m.enqueue(capped, 'd1', null, { delay: 60_000 })).status).toBe('queued')
      expect((await m.enqueue(capped, 'd2', null, { delay: 60_000 })).status).toBe('queued')
      expect((await m.enqueue(capped, 'r1', null)).status).toBe('queued')
      expect((await m.enqueue(capped, 'r2', null)).status).toBe('queued')
      const full = await m.enqueue(capped, 'r3', null)
      expect(full).toEqual({ status: 'full', jobId: 'c#r3' })
      expect(await m.isLocked('c#r3')).toBe(false)
      expect(await readRecord('c#r3')).toBeNull()
      // The lane cap (3) still admits one more entry from another job, then refuses.
      expect((await m.enqueue(other, 'r1', null)).status).toBe('queued')
      expect((await m.enqueue(other, 'r2', null)).status).toBe('full')
      expect(await m.queue(other, 'r2', null)).toBe(false)
    })

    it('maxInputsBytes: oversized inputs throw before any Redis call; the job value wins, 0 opts out', async () => {
      const m = newManager({ maxInputsBytes: 10 })
      const failed = vi.fn()
      m.hook('enqueueFailed', failed)
      const small = new Job({ jobName: 's' }, vi.fn())
      const big = new Job({ jobName: 'b', maxInputsBytes: 100 }, vi.fn())
      const unlimited = new Job({ jobName: 'u', maxInputsBytes: 0 }, vi.fn())
      vi.mocked(redis.evalsha).mockClear()
      const err = await m.queue(small, 'r1', { text: 'way more than ten bytes' }).catch((e) => e)
      expect(err).toBeInstanceOf(RedisJMEnqueueError)
      expect(err.reason).toBe('inputs-too-large')
      expect(err.jobId).toBe('s#r1')
      expect(redis.evalsha).not.toHaveBeenCalled()
      expect(failed).toHaveBeenCalledWith(expect.objectContaining({ reason: 'inputs-too-large' }))
      expect(await m.queue(big, 'r1', { text: 'way more than ten bytes' })).toBe(true)
      expect(await m.queue(unlimited, 'r1', { text: 'x'.repeat(1000) })).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('enqueueMany', () => {
    it('returns per-entry statuses in entry order from one script call', async () => {
      const m = newManager()
      const job = m.createJob({ jobName: 'm', maxQueued: 3 }, vi.fn())
      await m.queue(job, 'r2', null)
      const results = await job.enqueueMany([
        { runId: 'r1', inputs: 1 },
        { runId: 'r2', inputs: 2 },
        { runId: 'r3', inputs: 3 },
        { runId: 'r4', inputs: 4 },
      ])
      expect(results).toEqual([
        { status: 'queued', jobId: 'm#r1' },
        { status: 'deduped', jobId: 'm#r2' },
        { status: 'queued', jobId: 'm#r3' },
        { status: 'full', jobId: 'm#r4' }, // the cap counts the entries the batch itself added
      ])
      expect(listOf(QUEUE)).toEqual(['m#r2', 'm#r1', 'm#r3'])
    })

    it('first: true lands the batch at the head in its given order; earlier entries win the cap', async () => {
      const m = newManager()
      const job = new Job({ jobName: 'm', maxQueued: 3 }, vi.fn())
      await m.queue(job, 'old', null)
      const results = await m.enqueueMany(job, [
        { runId: 'a', inputs: 1 }, { runId: 'a', inputs: 1 }, { runId: 'b', inputs: 2 }, { runId: 'c', inputs: 3 },
      ], { first: true })
      expect(results.map((r) => r.status)).toEqual(['queued', 'deduped', 'queued', 'full'])
      expect(listOf(QUEUE)).toEqual(['m#a', 'm#b', 'm#old'])
    })

    it('is all-or-nothing: OOM writes nothing; one oversized entry rejects the batch before any write', async () => {
      const m = newManager({ maxInputsBytes: 20 })
      const job = new Job({ jobName: 'm' }, vi.fn())
      await expect(m.enqueueMany(job, [{ runId: 'a', inputs: 1 }, { runId: 'b', inputs: 'x'.repeat(50) }]))
        .rejects.toMatchObject({ reason: 'inputs-too-large', jobId: 'm#b' })
      expect(await readRecord('m#a')).toBeNull()
      redis._setOom(true)
      await expect(m.enqueueMany(job, [{ runId: 'a', inputs: 1 }, { runId: 'c', inputs: 2 }]))
        .rejects.toMatchObject({ name: 'RedisJMEnqueueError', reason: 'oom' })
      expect(setOf(LOCKS)).toEqual([])
      expect(listOf(QUEUE)).toEqual([])
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('inFlight (per-job lock set)', () => {
    it('counts the job’s in-flight runs from its lock set and splits by status from exactly those records', async () => {
      const m = newManager()
      let release!: () => void
      const job = m.createJob({ jobName: 'f' }, () => new Promise<void>((r) => { release = r }))
      await m.queue(job, 'run', null)
      await m.queue(job, 'q', null)
      await m.queue(job, 'd', null, { delay: 60_000 })
      await m.queue(new Job({ jobName: 'other' }, vi.fn()), 'x', null)
      const running = m.popAndExecute()
      await vi.waitFor(async () => expect((await readRecord('f#run'))?.status).toBe('running'))
      vi.mocked(redis.hscan).mockClear()
      expect(await m.inFlight('f')).toEqual({ total: 3, queued: 1, delayed: 1, running: 1 })
      expect(redis.hscan).not.toHaveBeenCalled() // no log scan
      release()
      await running
      expect((await m.inFlight('f')).total).toBe(2)
      expect(setOf(jobLocks('f'))).toEqual(['f#d', 'f#q'])
      expect(await m.inFlight('nobody')).toEqual({ total: 0, queued: 0, delayed: 0, running: 0 })
    })

    it('maintenance prunes drifted job-lock-set members, empty lanes, and finished jobs from the registry', async () => {
      const m = newManager()
      const job = m.createJob({ jobName: 'p', lane: 'a' }, vi.fn())
      await m.queue(job, 'r1', null)
      await redis.sadd(jobLocks('p'), 'p#ghost') // drift: a member whose lock is gone
      await m.performMaintenance()
      expect(setOf(jobLocks('p'))).toEqual(['p#r1'])
      expect(setOf(jobLanes('p'))).toEqual([laneKey('a')]) // still holds an entry
      await m.popAndExecute()
      await m.performMaintenance()
      expect(setOf(jobLocks('p'))).toEqual([])
      expect(setOf(jobLanes('p'))).toEqual([])
      expect(setOf('redisjm:g:jobs')).toEqual([])
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('claiming set and old lanes', () => {
    it('a pop parks the id in claiming; the claim removes it atomically', async () => {
      const m = newManager()
      let seen: number | null = null
      const job = m.createJob({ jobName: 'c' }, async () => {
        seen = redis._dump().zsets.get(CLAIMING)?.size ?? 0
      })
      await m.queue(job, 'r1', null)
      vi.mocked(redis.zadd).mockClear()
      await m.popAndExecute()
      expect(redis.zadd).toHaveBeenCalledWith(CLAIMING, expect.any(Number), 'c#r1')
      expect(seen).toBe(0) // gone by the time the handler runs
    })

    it('maintenance requeues an overdue unclaimed run at the head, and drops claiming entries of runs that moved on', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 100, roundsToStale: 2 })
      const job = new Job({ jobName: 'c' }, vi.fn())
      await m.queue(job, 'parked', null)
      await m.queue(job, 'next', null)
      // Simulate a popper that died after the pop: parked in claiming, off the list.
      await redis.lrem(QUEUE, 1, 'c#parked')
      await redis.zadd(CLAIMING, Date.now(), 'c#parked')
      await redis.zadd(CLAIMING, Date.now(), 'c#gone') // no record at all
      expect((await m.performMaintenance()).requeuedCount).toBe(0) // not overdue yet
      await vi.advanceTimersByTimeAsync(300)
      const result = await m.performMaintenance()
      expect(result.requeuedCount).toBe(1)
      expect(listOf(QUEUE)).toEqual(['c#parked', 'c#next'])
      expect(redis._dump().zsets.get(CLAIMING)?.size).toBe(0)
      expect((await readRecord('c#parked'))?.status).toBe('queued')
      expect(await m.isLocked('c#parked')).toBe(true)
    })

    it('drains a job’s OLD lane (after its lane changed) without touching other jobs queued there', async () => {
      const m = newManager()
      // Before a deploy, `moved` lived on lane `a` (shared with `stay`, which this instance doesn't run).
      const producerView = new Job({ jobName: 'moved', lane: 'a' }, vi.fn())
      await m.queue(new Job({ jobName: 'stay', lane: 'a' }, vi.fn()), 's1', null)
      await m.queue(producerView, 'm1', null)
      // After the deploy, this instance runs `moved` on lane `b`.
      const fn = vi.fn()
      m.createJob({ jobName: 'moved', lane: 'b' }, fn)

      expect(await m.popAndExecute()).toBe(true)
      expect(fn).toHaveBeenCalledTimes(1)
      expect((await readRecord('moved#m1'))?.status).toBe('finished')
      // `stay` was never popped, requeued, or charged an unknown-job requeue.
      expect(listOf(laneKey('a'))).toEqual(['stay#s1'])
      expect((await readRecord('stay#s1'))?.requeueCount).toBeUndefined()
      expect(await m.popAndExecute()).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('bounded maintenance', () => {
    it('examines at most maxRecordsPerPass log records per pass and resumes from the persisted cursor', async () => {
      const m = newManager({ maxRecordsPerPass: 10 })
      const old = Date.now() - 120_000
      for (let i = 0; i < 35; i++) {
        await seed({ jobId: `x#${i}`, status: 'finished', progress: 1, finishedAt: old }, false)
      }
      const cleaned: number[] = []
      for (let pass = 0; pass < 4; pass++) cleaned.push((await m.performMaintenance()).cleanedCount)
      // ≤ 10 per pass; the shared cursor carries on where the previous pass stopped.
      expect(cleaned.every((c) => c <= 10)).toBe(true)
      expect(cleaned.reduce((a, b) => a + b, 0)).toBe(35)
      expect(await redis.hlen(LOG)).toBe(0)
      expect(await redis.get('redisjm:g:maintenance-cursor')).toBe('0')
    })

    it('needs no per-record list check for new-format queued records (legacy ones still get one)', async () => {
      const m = newManager()
      const job = new Job({ jobName: 'q' }, vi.fn())
      for (let i = 0; i < 20; i++) await m.queue(job, `r${i}`, null)
      // A legacy (0.1.x) queued record: no enqueuedAt.
      await seed({ jobId: 'q#legacy', status: 'queued' }, false)
      vi.mocked(redis.lpos).mockClear()
      await m.performMaintenance()
      expect(redis.lpos).toHaveBeenCalledTimes(1)
      expect(redis.lpos).toHaveBeenCalledWith(QUEUE, 'q#legacy')
    })

    it('never re-creates a record that was deleted between its read and the write', async () => {
      // WHY: a run finishing (keepFinishedInterval: 0 deletes its record) between maintenance's read
      // and its stale write used to be resurrected as a ghost `stale` record.
      const m = newManager({ heartbeatInterval: 100, roundsToStale: 2, keepFinishedInterval: 0 })
      const old = Date.now() - 10_000
      await seed({ jobId: 'g#r1', status: 'running', executionId: 'e1', startedAt: old, heartbeat: old })
      // The record disappears right after the scan read it (the run finished meanwhile).
      const realHscan = redis.hscan
      vi.mocked(redis.hscan).mockImplementationOnce(async (...args: any[]) => {
        const reply = await (realHscan as any)(...args)
        await redis.hdel(LOG, 'g#r1')
        await redis.srem(LOCKS, 'g#r1')
        return reply
      })
      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
      expect(await readRecord('g#r1')).toBeNull()
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('HPEXPIRE retention', () => {
    it('gives a terminal record a field TTL of keepFinishedInterval; a re-enqueue clears it', async () => {
      const m = newManager({ keepFinishedInterval: 5000 })
      const job = m.createJob({ jobName: 'h' }, vi.fn())
      await m.queue(job, 'r1', null)
      await m.popAndExecute()
      expect(redis.call).toHaveBeenCalledWith('HPEXPIRE', LOG, 5000, 'FIELDS', 1, 'h#r1')
      expect(redis._dump().fieldTtls.get(LOG)?.has('h#r1')).toBe(true)
      await m.queue(job, 'r1', null)
      expect(redis._dump().fieldTtls.get(LOG)?.has('h#r1')).toBe(false)
    })

    it('expires the record even when maintenance never runs', async () => {
      vi.useFakeTimers()
      const m = newManager({ keepFinishedInterval: 1000 })
      const job = m.createJob({ jobName: 'h' }, vi.fn())
      await m.queue(job, 'r1', null)
      await m.popAndExecute()
      expect(await readRecord('h#r1')).not.toBeNull()
      await vi.advanceTimersByTimeAsync(1001)
      expect(await readRecord('h#r1')).toBeNull()
    })

    it('on a server without HPEXPIRE the terminal write still lands (the field TTL is skipped)', async () => {
      // The TTL is set inside the transition script via `redis.pcall`, so an unknown command is ignored.
      redis._setHpexpireSupported(false)
      const m = newManager({ keepFinishedInterval: 5000 })
      const job = m.createJob({ jobName: 'h' }, vi.fn())
      for (const r of ['a', 'b', 'c']) {
        await m.queue(job, r, null)
        await m.popAndExecute()
      }
      expect((await readRecord('h#c'))?.status).toBe('finished')
      expect(redis._dump().fieldTtls.get(LOG)?.size ?? 0).toBe(0)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('health, memory pressure, eviction policy', () => {
    it('health() reads INFO memory and cheap cardinalities; { scan: true } counts exactly', async () => {
      redis._setInfo({ used_memory: 800, maxmemory: 1000, maxmemory_policy: 'noeviction' })
      const m = newManager()
      const job = new Job({ jobName: 'h' }, vi.fn())
      await m.queue(job, 'q1', null)
      await m.queue(job, 'd1', null, { delay: 60_000 })
      await seed({ jobId: 'h#s', status: 'stale', finishedAt: Date.now() }, false)
      vi.mocked(redis.hscan).mockClear()
      const h = await m.health()
      expect(redis.hscan).not.toHaveBeenCalled()
      expect(h).toMatchObject({
        usedMemory: 800, maxMemory: 1000, usedRatio: 0.8, maxmemoryPolicy: 'noeviction', oomRefusals: 0,
        delayed: 1, claiming: 0, locks: 2, running: 0, stale: null,
      })
      expect(h.queues.default).toBe(1)
      expect((await m.health({ scan: true })).stale).toBe(1)
    })

    it('memoryPressure fires once per upward crossing of memoryWarnRatio and re-arms below it', async () => {
      redis._setInfo({ used_memory: 500, maxmemory: 1000 })
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { maintenanceInterval: 100, memoryWarnRatio: 0.8, logger })
      const pressure = vi.fn()
      m.hook('memoryPressure', pressure)
      const tick = async () => {
        await (m as any).runMaintenance()
        await (m as any).checkMemoryPressure()
      }
      await tick()
      expect(pressure).not.toHaveBeenCalled()
      redis._setInfo({ used_memory: 900 })
      await tick()
      await tick()
      expect(pressure).toHaveBeenCalledTimes(1)
      expect(pressure.mock.calls[0][0]).toMatchObject({ usedRatio: 0.9 })
      redis._setInfo({ used_memory: 100 })
      await tick()
      redis._setInfo({ used_memory: 950 })
      await tick()
      expect(pressure).toHaveBeenCalledTimes(2)
      expect(logger.mock.calls.some(([msg]) => /memory at 90\.0%/.test(msg as string))).toBe(true)
    })

    it('a crossing whose health() snapshot failed is reported on the next tick, not swallowed', async () => {
      redis._setInfo({ used_memory: 900, maxmemory: 1000 })
      const m = new RedisJM(redis, 'g', { maintenanceInterval: 100, memoryWarnRatio: 0.8, logger: false })
      const pressure = vi.fn()
      m.hook('memoryPressure', pressure)
      vi.mocked(redis.zcard).mockRejectedValueOnce(new Error('Connection is closed.'))
      await (m as any).checkMemoryPressure()
      expect(pressure).not.toHaveBeenCalled()
      await (m as any).checkMemoryPressure()
      expect(pressure).toHaveBeenCalledTimes(1)
    })

    it.each([
      ['allkeys-lru', /may evict/],
      ['volatile-ttl', /cannot free them/],
    ])('start() warns about maxmemory-policy %s (read from INFO, never CONFIG)', async (policy, pattern) => {
      redis._setInfo({ maxmemory_policy: policy })
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { maintenanceInterval: 0, logger })
      m.start(10_000)
      await vi.waitFor(() => expect(logger.mock.calls.some(([msg]) => pattern.test(msg as string))).toBe(true))
      expect(redis.info).toHaveBeenCalledWith('memory')
      await m.stop()
    })

    it('start() stays quiet for noeviction', async () => {
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { maintenanceInterval: 0, logger })
      m.start(10_000)
      await vi.waitFor(() => expect(redis.info).toHaveBeenCalled())
      await m.stop()
      expect(logger.mock.calls.some(([msg]) => /maxmemory-policy/.test(msg as string))).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('every()', () => {
    it('skipIfInFlight (default) reuses one runId so the lock dedupes ticks; cancel and stop() clear it', async () => {
      vi.useFakeTimers()
      const m = newManager()
      const job = new Job({ jobName: 'tick' }, vi.fn())
      const cancel = m.every(job, 1000, { inputs: { n: 1 }, immediate: true })
      await vi.advanceTimersByTimeAsync(0)
      expect(listOf(QUEUE)).toEqual(['tick#every'])
      await vi.advanceTimersByTimeAsync(3000)
      expect(listOf(QUEUE)).toEqual(['tick#every']) // deduped while still queued
      cancel()
      const other = m.every(new Job({ jobName: 'fresh' }, vi.fn()), 1000, { inputs: null, skipIfInFlight: false, runId: 'f' })
      void other
      await vi.advanceTimersByTimeAsync(2000)
      expect(listOf(QUEUE).filter((id) => id.startsWith('fresh#f-'))).toHaveLength(2)
      await m.stop()
      await vi.advanceTimersByTimeAsync(5000)
      expect(listOf(QUEUE).filter((id) => id.startsWith('fresh#f-'))).toHaveLength(2)
    })

    it('enqueue failures are logged, never thrown from the timer', async () => {
      vi.useFakeTimers()
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { maintenanceInterval: 0, logger })
      redis._setOom(true)
      m.every(new Job({ jobName: 'tick' }, vi.fn()), 1000, { inputs: null })
      await vi.advanceTimersByTimeAsync(1000)
      expect(logger.mock.calls.some(([msg]) => /every\(\): enqueue of "tick#every" failed/.test(msg as string))).toBe(true)
      expect(() => m.every(new Job({ jobName: 'x' }, vi.fn()), 0, { inputs: null })).toThrow(TypeError)
      await m.stop()
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('listPage()', () => {
    it('pages through the log with filters and a cursor', async () => {
      const m = newManager()
      const a = new Job({ jobName: 'a', lane: 'x' }, vi.fn())
      const b = new Job({ jobName: 'b' }, vi.fn())
      for (let i = 0; i < 12; i++) await m.queue(a, `r${i}`, null)
      for (let i = 0; i < 5; i++) await m.queue(b, `r${i}`, null, { delay: 60_000 })
      const seen: string[] = []
      let cursor = '0'
      let pages = 0
      do {
        const page = await m.listPage({ jobName: 'a', lane: 'x', status: 'queued', limit: 5, cursor })
        seen.push(...page.records.map((r) => r.jobId))
        cursor = page.cursor
        pages++
      } while (cursor !== '0')
      expect(pages).toBeGreaterThan(1)
      expect(new Set(seen).size).toBe(12)
      expect((await m.listPage({ status: 'delayed', limit: 100 })).records).toHaveLength(5)
      expect((await m.listPage({ lane: 'default', limit: 100 })).records).toHaveLength(5)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('poll loop: laneConcurrency, wake(), popAndExecute() vs stop()', () => {
    it('laneConcurrency keeps capacity for other lanes while a heavy lane is at its cap', async () => {
      const m = newManager({ concurrency: 3, laneConcurrency: { heavy: 1 } })
      const started: string[] = []
      const gates: Array<() => void> = []
      const heavy = m.createJob({ jobName: 'heavy', lane: 'heavy' }, (input: string) => new Promise<void>((r) => {
        started.push(input)
        gates.push(r)
      }))
      const light = m.createJob({ jobName: 'light', lane: 'light' }, async (input: string) => { started.push(input) })
      await m.queue(heavy, 'h1', 'h1')
      await m.queue(heavy, 'h2', 'h2')
      await m.queue(light, 'l1', 'l1')
      m.start(5)
      await vi.waitFor(() => expect(started).toContain('l1'))
      await new Promise((r) => setTimeout(r, 30))
      expect(started.filter((s) => s.startsWith('h'))).toEqual(['h1']) // h2 waits for heavy's slot
      gates.shift()!()
      await vi.waitFor(() => expect(started).toContain('h2'))
      gates.shift()!()
      await m.stop()
    })

    it('a capped lane whose slot frees is re-polled right away, not after the idle interval', async () => {
      vi.useFakeTimers()
      const m = newManager({ laneConcurrency: { heavy: 1 }, concurrency: 2 })
      const started: string[] = []
      const gates: Array<() => void> = []
      const heavy = m.createJob({ jobName: 'heavy', lane: 'heavy' }, (input: string) => new Promise<void>((r) => {
        started.push(input)
        gates.push(r)
      }))
      await m.queue(heavy, 'h1', 'h1')
      await m.queue(heavy, 'h2', 'h2')
      m.start(60_000)
      await vi.advanceTimersByTimeAsync(10)
      expect(started).toEqual(['h1']) // h2 skipped at the lane cap; nothing else → idle for a minute
      gates.shift()!()
      await vi.advanceTimersByTimeAsync(10)
      expect(started).toEqual(['h1', 'h2'])
      gates.shift()!()
      await m.stop()
    })

    it('wake() cuts the idle wait short', async () => {
      vi.useFakeTimers()
      const m = newManager()
      const fn = vi.fn()
      const job = m.createJob({ jobName: 'w' }, fn)
      m.start(60_000)
      await vi.advanceTimersByTimeAsync(10) // first poll: empty → idle for a minute
      await m.queue(job, 'r1', null)
      await vi.advanceTimersByTimeAsync(10)
      expect(fn).not.toHaveBeenCalled()
      m.wake()
      await vi.advanceTimersByTimeAsync(5)
      expect(fn).toHaveBeenCalledTimes(1)
      await m.stop()
      m.wake() // no-op when stopped
    })

    it('popAndExecute() is drained by stop() and refuses to pop once stopped', async () => {
      const m = newManager()
      let release!: () => void
      const job = m.createJob({ jobName: 'p' }, () => new Promise<void>((r) => { release = r }))
      await m.queue(job, 'r1', null)
      await m.queue(job, 'r2', null)
      const run = m.popAndExecute()
      await vi.waitFor(async () => expect((await readRecord('p#r1'))?.status).toBe('running'))
      let stopped = false
      const stopping = m.stop().then(() => { stopped = true })
      await new Promise((r) => setTimeout(r, 10))
      expect(stopped).toBe(false)
      release()
      await stopping
      expect(await run).toBe(true)
      expect(await m.popAndExecute()).toBe(false)
      expect(listOf(QUEUE)).toEqual(['p#r2'])
      await m.unqueue('p#r2')
      m.start(60_000) // a new start() re-enables it
      await m.stop()
      expect(await m.popAndExecute()).toBe(false)
    })
  })
})
