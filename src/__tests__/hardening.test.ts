/**
 * Unit tests (mock Redis) for the 0.2.0 hardening: execution timeouts + maxRunMs, maintenance off the
 * queue (delete-first, guarded writes, OOM emergency mode), enqueue results / typed enqueue errors,
 * start-phase failure recovery, and manager-level hooks as isolated observers.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { JobTimeoutError, RedisJMEnqueueError } from '../errors'
import { Job } from '../job'
import { RedisJM } from '../redisjm'
import type { JobContext } from '../types'
import { connectionError, createMockRedis, oomError } from './mock-redis'
import { CLAIMING, LOCKS, LOG, mockHelpers, QUEUE } from './unit-helpers'

describe('hardening', () => {
  let redis: ReturnType<typeof createMockRedis>

  beforeEach(() => {
    redis = createMockRedis()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  const { readRecord, seed } = mockHelpers(() => redis)
  const hung = () => new Promise<void>(() => {})

  // ---------------------------------------------------------------------------------------------
  describe('execution timeout (manager)', () => {
    it('jobTimeout fails a hung run: record error with the timeout message, lock released, slot freed', async () => {
      // WHY: before 0.2.0 a hung handler held popAndExecute (and its concurrency slot) forever.
      const m = new RedisJM(redis, 'g', { jobTimeout: 30, keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 'h' }, hung)
      await m.queue(job, 'r1', null)

      expect(await m.popAndExecute()).toBe(true) // settles although the handler never does

      const record = await readRecord('h#r1')
      expect(record?.status).toBe('error')
      expect(record?.error).toMatch(/timed out after 30ms/)
      expect(await m.isLocked('h#r1')).toBe(false)
    })

    it('fires the manager timeout hook before the error hook', async () => {
      const m = new RedisJM(redis, 'g', { jobTimeout: 20, keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 'h' }, hung)
      const order: string[] = []
      let timeoutPayload: any
      m.hook('timeout', (p) => { order.push('timeout'); timeoutPayload = p })
      m.hook('error', () => { order.push('error') })
      await m.queue(job, 'r1', null)
      await m.popAndExecute()

      expect(order).toEqual(['timeout', 'error'])
      expect(timeoutPayload.timeoutMs).toBe(20)
      expect(timeoutPayload.runId).toBe('r1')
      expect(timeoutPayload.error).toBeInstanceOf(JobTimeoutError)
    })

    it('a timed-out attempt is retried when attempts remain (timeout → retry, no error)', async () => {
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 'h', timeoutMs: 20, attempts: 2, backoff: 10_000 }, hung)
      const events: string[] = []
      m.hook('timeout', () => { events.push('timeout') })
      m.hook('retry', () => { events.push('retry') })
      m.hook('error', () => { events.push('error') })
      await m.queue(job, 'r1', null)
      await m.popAndExecute()

      expect(events).toEqual(['timeout', 'retry'])
      const record = await readRecord('h#r1')
      expect(record?.status).toBe('delayed')
      expect(record?.error).toMatch(/timed out/)
      expect(await m.isLocked('h#r1')).toBe(true) // held through the backoff
    })

    it('the job timeout wins over the manager default; timeoutMs: 0 opts a job out', async () => {
      const m = new RedisJM(redis, 'g', { jobTimeout: 10_000, keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const short = m.createJob({ jobName: 'short', timeoutMs: 20 }, hung)
      let release!: () => void
      const optOut = m.createJob({ jobName: 'optout', timeoutMs: 0 }, () => new Promise<void>((r) => { release = r }))
      const execSpy = vi.spyOn(optOut, 'execute')

      await m.queue(short, 'r1', null)
      await m.popAndExecute()
      expect((await readRecord('short#r1'))?.error).toMatch(/after 20ms/)

      await m.queue(optOut, 'r1', null)
      const run = m.popAndExecute()
      await new Promise((r) => setTimeout(r, 50))
      expect(execSpy.mock.calls[0][1]?.timeoutMs).toBe(0)
      expect((await readRecord('optout#r1'))?.status).toBe('running')
      release()
      await run
      expect((await readRecord('optout#r1'))?.status).toBe('finished')
    })

    it("an abandoned handler's late setProgress / setAttrs never land", async () => {
      // WHY: the handler keeps running detached after its timeout; its writes must stay fenced out.
      const m = new RedisJM(redis, 'g', { jobTimeout: 20, keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      let ctxRef!: JobContext<{ late?: boolean }>
      const job = m.createJob<null, { late?: boolean }>({ jobName: 'h' }, (_i, ctx) => { ctxRef = ctx; return hung() })
      await m.queue(job, 'r1', null)
      await m.popAndExecute()
      const before = await readRecord('h#r1')
      expect(before?.status).toBe('error')

      await ctxRef.setProgress(0.9)
      await ctxRef.setAttrs({ late: true })

      const after = await readRecord('h#r1')
      expect(after).toEqual(before)
      expect(await m.isLocked('h#r1')).toBe(false)
    })

    it('poll loop: a hung handler at concurrency 1 times out and the next job runs', async () => {
      const m = new RedisJM(redis, 'g', { jobTimeout: 30, keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const ran: string[] = []
      const job = m.createJob({ jobName: 'p' }, async (input: string) => {
        ran.push(input)
        if (input === 'hang') await hung()
      })
      await m.queue(job, 'a', 'hang')
      await m.queue(job, 'b', 'ok')
      m.start(5)
      await vi.waitFor(async () => expect((await readRecord('p#b'))?.status).toBe('finished'), { timeout: 1000 })
      await m.stop()
      expect(ran).toEqual(['hang', 'ok'])
      expect((await readRecord('p#a'))?.status).toBe('error')
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('maxRunMs backstop', () => {
    it('stales a running record past maxRunMs even with a fresh heartbeat, and releases its lock', async () => {
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, maxRunMs: 5000, keepFinishedInterval: 60000, logger: false })
      await seed({ jobId: 'j#old', status: 'running', startedAt: Date.now() - 6000, heartbeat: Date.now(), executionId: 'e1' })
      await seed({ jobId: 'j#young', status: 'running', startedAt: Date.now() - 1000, heartbeat: Date.now(), executionId: 'e2' })

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(1)
      const old = await readRecord('j#old')
      expect(old?.status).toBe('stale')
      expect(old?.staleReason).toBe('maxRunMs')
      expect(old?.error).toMatch(/maxRunMs/)
      expect(await m.isLocked('j#old')).toBe(false)
      expect((await readRecord('j#young'))?.status).toBe('running')
    })

    it('is off by default: a long-running record with a fresh heartbeat stays running', async () => {
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000, logger: false })
      await seed({ jobId: 'j#old', status: 'running', startedAt: Date.now() - 3_600_000, heartbeat: Date.now() })
      await m.performMaintenance()
      expect((await readRecord('j#old'))?.status).toBe('running')
    })

    it('a maxRunMs stale is not resurrected by a heartbeat from the same execution; the run is aborted', async () => {
      // WHY: a hung handler's heartbeat timer keeps firing — resurrecting would undo the backstop.
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, logger: false, maintenanceInterval: 0 })
      const job = m.createJob({ jobName: 'j' }, vi.fn())
      await seed({ jobId: 'j#r1', status: 'stale', staleReason: 'maxRunMs', executionId: 'e1', finishedAt: Date.now() }, false)
      const abort = vi.fn()
      await job.callHook('heartbeat', { job, targetGroup: 'g', runId: 'r1', inputs: null, executionId: 'e1', attempt: 1, manager: m, abort })
      expect((await readRecord('j#r1'))?.status).toBe('stale')
      expect(await m.isLocked('j#r1')).toBe(false)
      expect(abort).toHaveBeenCalled()

      // A heartbeat-reason stale from the same execution still self-heals (unchanged behaviour).
      await seed({ jobId: 'j#r2', status: 'stale', staleReason: 'heartbeat', executionId: 'e2', finishedAt: Date.now() }, false)
      await job.callHook('heartbeat', { job, targetGroup: 'g', runId: 'r2', inputs: null, executionId: 'e2', attempt: 1, manager: m, abort: vi.fn() })
      const healed = await readRecord('j#r2')
      expect(healed?.status).toBe('running')
      expect(healed?.staleReason).toBeUndefined()
      expect(await m.isLocked('j#r2')).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('maintenance: delete-first, guarded writes, emergency mode', () => {
    const old = () => Date.now() - 120_000

    it('performs every deletion before the first write, with many fields per HDEL', async () => {
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000, logger: false })
      await seed({ jobId: 'j#f1', status: 'finished', finishedAt: old() }, false)
      await seed({ jobId: 'j#f2', status: 'error', finishedAt: old() }, false)
      await seed({ jobId: 'j#f3', status: 'stale', finishedAt: old() }, false)
      await seed({ jobId: 'j#run', status: 'running', startedAt: old(), heartbeat: old() })
      await redis.hset(LOG, 'j#junk', '{not json')
      await redis.sadd(LOCKS, 'j#junk')
      vi.mocked(redis.hdel).mockClear()
      vi.mocked(redis.hset).mockClear()
      vi.mocked(redis.srem).mockClear()

      const result = await m.performMaintenance()
      expect(result).toEqual({ staleCount: 1, cleanedCount: 4, requeuedCount: 0, mode: 'full' })

      // One batched compare-and-delete script call carries all four doomed records (EVALSHA, plus the
      // EVAL retry after the mock's first NOSCRIPT — same arguments).
      const doomed = ['j#f1', 'j#f2', 'j#f3', 'j#junk']
      const deleteCalls = [...vi.mocked(redis.evalsha).mock.calls, ...vi.mocked(redis.eval).mock.calls]
        .filter((call) => call.some((a) => a === 'j#f1'))
      expect(deleteCalls.length).toBeGreaterThanOrEqual(1)
      for (const call of deleteCalls) expect(doomed.every((id) => call.includes(id))).toBe(true)
      const logHdels = vi.mocked(redis.hdel).mock.calls.filter(([key]) => key === LOG)
      expect(logHdels.map(([, f]) => f).sort()).toEqual(doomed)
      // Deletions strictly before the first write.
      const lastDelete = Math.max(
        ...vi.mocked(redis.hdel).mock.invocationCallOrder,
        vi.mocked(redis.srem).mock.invocationCallOrder[0], // garbage-lock SREM (the stale SREM comes after its HSET)
      )
      const firstWrite = Math.min(...vi.mocked(redis.hset).mock.invocationCallOrder)
      expect(lastDelete).toBeLessThan(firstWrite)
      expect(await redis.sismember(LOCKS, 'j#junk')).toBe(0)
      expect((await readRecord('j#run'))?.status).toBe('stale')
    })

    it('one failed write does not abort the pass; failures are logged once with the classified reason', async () => {
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000, logger })
      await seed({ jobId: 'j#a', status: 'running', startedAt: old(), heartbeat: old() })
      await seed({ jobId: 'j#b', status: 'running', startedAt: old(), heartbeat: old() })
      vi.mocked(redis.hset).mockRejectedValueOnce(oomError())

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(1)
      const statuses = [(await readRecord('j#a'))?.status, (await readRecord('j#b'))?.status].sort()
      expect(statuses).toEqual(['running', 'stale'])
      // The refused record keeps its lock (no half-reclaim); the other one was released.
      expect([await m.isLocked('j#a'), await m.isLocked('j#b')].sort()).toEqual([false, true])
      const reports = logger.mock.calls.filter(([msg]) => /maintenance pass: 1 Redis operation\(s\) failed \(oom\)/.test(msg as string))
      expect(reports).toHaveLength(1)
    })

    it('never writes its scan snapshot over a record that changed since the scan', async () => {
      // WHY: a pass reads everything first and writes later (and now runs concurrently with the poll
      // loop). Writing the stale snapshot back would flip a just-claimed run to `queued` (fencing out
      // and aborting a healthy run) or stale a run that has heartbeated since.
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000, logger: false })
      // (a) queued in the snapshot, popped (not on the list), then CLAIMED before the write.
      await seed({ jobId: 'j#claimed', status: 'queued' })
      ;(redis.lpos as any).mockImplementationOnce(async () => {
        await seed({ jobId: 'j#claimed', status: 'running', executionId: 'e1', startedAt: Date.now(), heartbeat: Date.now() })
        return null
      })
      // (b) overdue `running` in the snapshot, but it heartbeated before the write.
      const stale = { jobId: 'j#beat', jobName: 'j', runId: 'beat', inputs: null, targetGroup: 'g', progress: 0, status: 'running', executionId: 'e2', startedAt: old(), heartbeat: old() }
      await seed({ ...stale, heartbeat: Date.now() } as any)
      const realHscan = redis.hscan
      vi.mocked(redis.hscan).mockImplementationOnce(async (key: any, ...args: any[]) => {
        const [cursor, flat] = await (realHscan as any)(key, ...args)
        const i = flat.indexOf('j#beat')
        flat[i + 1] = JSON.stringify(stale)
        return [cursor, flat]
      })

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
      const claimed = await readRecord('j#claimed')
      expect(claimed?.status).toBe('running')
      expect(claimed?.suspectedAt).toBeUndefined()
      expect((await readRecord('j#beat'))?.status).toBe('running')
      expect(await m.isLocked('j#beat')).toBe(true)
    })

    it('runMaintenance under OOM runs an emergency delete-only pass (no lock, no writes)', async () => {
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000, maintenanceInterval: 500, logger: false })
      await seed({ jobId: 'j#done', status: 'finished', finishedAt: old() }, false)
      await seed({ jobId: 'j#fresh', status: 'finished', finishedAt: Date.now() }, false)
      await seed({ jobId: 'j#run', status: 'running', startedAt: old(), heartbeat: old() })
      await redis.hset(LOG, 'j#junk', '"42"')
      await redis.sadd(LOCKS, 'j#junk')
      const fullPass = vi.spyOn(m, 'performMaintenance')
      redis._setOom(true)
      vi.mocked(redis.hset).mockClear()

      const result = await m.runMaintenance()

      expect(result).toEqual({ staleCount: 0, cleanedCount: 2, requeuedCount: 0, mode: 'emergency' })
      expect(fullPass).not.toHaveBeenCalled()
      expect(await readRecord('j#done')).toBeNull()
      expect(await readRecord('j#junk')).toBeNull()
      expect(await redis.sismember(LOCKS, 'j#junk')).toBe(0)
      expect(await readRecord('j#fresh')).not.toBeNull() // retention still honoured
      expect((await readRecord('j#run'))?.status).toBe('running') // no writes in emergency mode
      expect(redis.hset).not.toHaveBeenCalled()
      expect(await redis.get('redisjm:g:maintenance-lock')).toBeNull()
    })

    it('a full pass on an OOM Redis still frees memory (deletions land, writes are counted as failures)', async () => {
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000, logger })
      await seed({ jobId: 'j#done', status: 'finished', finishedAt: old() }, false)
      await seed({ jobId: 'j#run', status: 'running', startedAt: old(), heartbeat: old() })
      redis._setOom(true)
      const result = await m.performMaintenance()
      expect(result).toEqual({ staleCount: 0, cleanedCount: 1, requeuedCount: 0, mode: 'full' })
      expect(await readRecord('j#done')).toBeNull()
      expect(logger.mock.calls.some(([msg]) => /failed \(oom\)/.test(msg as string))).toBe(true)
    })

    it('maintenance keeps running on the timer while every concurrency slot is held by a hung handler', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, roundsToStale: 2, maintenanceInterval: 500, keepFinishedInterval: 60000, logger: false })
      const job = m.createJob({ jobName: 'h' }, hung)
      await m.queue(job, 'r1', null)
      m.start(10)
      await vi.advanceTimersByTimeAsync(20)
      expect((await readRecord('h#r1'))?.status).toBe('running') // the only slot is now hung

      // A crashed peer's stale record appears; maintenance must still reclaim it.
      await seed({ jobId: 'x#crashed', status: 'running', startedAt: Date.now() - 10_000, heartbeat: Date.now() - 10_000 })
      await vi.advanceTimersByTimeAsync(600)
      expect((await readRecord('x#crashed'))?.status).toBe('stale')
      expect(await m.isLocked('x#crashed')).toBe(false)
      m.stop() // the hung run never settles; don't await the drain
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('enqueue() result API and RedisJMEnqueueError', () => {
    it('reports queued / deduped with the jobId; first: true inserts at the head', async () => {
      const m = new RedisJM(redis, 'g', { logger: false })
      const job = new Job({ jobName: 'e' }, vi.fn())
      expect(await m.enqueue(job, 'r1', 1)).toEqual({ status: 'queued', jobId: 'e#r1' })
      expect(await m.enqueue(job, 'r1', 1)).toEqual({ status: 'deduped', jobId: 'e#r1' })
      expect(await m.enqueue(job, 'r2', 2, { first: true })).toEqual({ status: 'queued', jobId: 'e#r2' })
      expect(redis._dump().lists.get(QUEUE)).toEqual(['e#r2', 'e#r1'])
      expect(await job.enqueue('r3', 3, m, { delay: 1000 })).toEqual({ status: 'queued', jobId: 'e#r3' })
      expect((await readRecord('e#r3'))?.status).toBe('delayed')
    })

    it('rejects first + delay with a TypeError (programmer error, not wrapped)', async () => {
      const m = new RedisJM(redis, 'g', { logger: false })
      const job = new Job({ jobName: 'e' }, vi.fn())
      await expect(m.enqueue(job, 'r1', 1, { first: true, delay: 10 })).rejects.toThrow(TypeError)
    })

    it('queue() / queueFirst() return booleans derived from the result', async () => {
      const m = new RedisJM(redis, 'g', { logger: false })
      const job = new Job({ jobName: 'e' }, vi.fn())
      expect(await m.queue(job, 'r1', 1)).toBe(true)
      expect(await m.queue(job, 'r1', 1)).toBe(false)
      expect(await m.queueFirst(job, 'r1', 1)).toBe(false)
    })

    it('OOM: queue() throws RedisJMEnqueueError{reason:oom}, writes nothing, fires enqueueFailed', async () => {
      // WHY: an OOM-refused enqueue used to surface as a raw ReplyError (or, mid-write, leave partial state).
      const m = new RedisJM(redis, 'g', { logger: false })
      const job = new Job({ jobName: 'e' }, vi.fn())
      const failed = vi.fn()
      m.hook('enqueueFailed', failed)
      redis._setOom(true)

      const err = await m.queue(job, 'r1', { big: true }).catch((e) => e)
      expect(err).toBeInstanceOf(RedisJMEnqueueError)
      expect(err.reason).toBe('oom')
      expect(err.jobId).toBe('e#r1')
      expect(err.cause.message).toMatch(/^OOM/)
      expect(await m.isLocked('e#r1')).toBe(false)
      expect(await readRecord('e#r1')).toBeNull()
      expect(failed).toHaveBeenCalledWith(expect.objectContaining({
        jobId: 'e#r1', jobName: 'e', runId: 'r1', reason: 'oom', error: err,
      }))
      expect((m as any).oomRefusals).toBe(1)
    })

    it('a failed enqueue throws a classified error and writes nothing; a throwing observer cannot mask it', async () => {
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { logger })
      const job = new Job({ jobName: 'e' }, vi.fn())
      m.hook('enqueueFailed', () => { throw new Error('observer boom') })
      vi.mocked(redis.evalsha).mockRejectedValueOnce(connectionError())

      const err = await m.queue(job, 'r1', 1).catch((e) => e)
      expect(err).toBeInstanceOf(RedisJMEnqueueError)
      expect(err.reason).toBe('connection')
      expect(await m.isLocked('e#r1')).toBe(false)
      expect(await readRecord('e#r1')).toBeNull()
      expect(logger.mock.calls.some(([, e]) => e instanceof Error && e.message === 'observer boom')).toBe(true)
    })

    it('a delayed enqueue under OOM writes nothing (the script is refused before any write)', async () => {
      const m = new RedisJM(redis, 'g', { logger: false })
      const job = new Job({ jobName: 'e' }, vi.fn())
      redis._setOom(true)
      await expect(m.queue(job, 'r1', 1, { delay: 1000 })).rejects.toMatchObject({ name: 'RedisJMEnqueueError', reason: 'oom' })
      expect(await m.isLocked('e#r1')).toBe(false)
      expect(await readRecord('e#r1')).toBeNull()
      expect(redis._dump().zsets.get('redisjm:g:delayed')?.size ?? 0).toBe(0)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('start-phase failure recovery', () => {
    it('claim write fails → the jobId is pushed back to the head of its lane, lock kept; it runs later', async () => {
      // WHY: the popped entry used to be lost while its record stayed `queued` and locked until two
      // maintenance passes reclaimed it as stale (the run never executed).
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 's' }, fn)
      const startFailed = vi.fn()
      m.hook('startFailed', startFailed)
      await m.queue(job, 'r1', null)
      await m.queue(job, 'r2', null)
      vi.mocked(redis.hset).mockRejectedValueOnce(connectionError()) // the claim of r1

      expect(await m.popAndExecute()).toBe(true)
      expect(fn).not.toHaveBeenCalled()
      expect(redis._dump().lists.get(QUEUE)).toEqual(['s#r1', 's#r2']) // back at the head
      expect((await readRecord('s#r1'))?.status).toBe('queued')
      expect(await m.isLocked('s#r1')).toBe(true)
      expect(startFailed).toHaveBeenCalledWith(expect.objectContaining({
        jobId: 's#r1', jobName: 's', runId: 'r1', reason: 'connection', action: 'requeued',
      }))

      // A short back-off before popping again, then it runs normally.
      expect(await m.popAndExecute()).toBe(false)
      await vi.advanceTimersByTimeAsync(1000)
      expect(await m.popAndExecute()).toBe(true)
      expect(fn).toHaveBeenCalledTimes(1)
      expect((await readRecord('s#r1'))?.status).toBe('finished')
    })

    it('claim AND push-back refused under OOM → the run is deferred (kept in claiming, nothing lost), pops paused, then requeued by maintenance', async () => {
      // WHY: phase 1 had to DROP such a run (record + lock deleted). With the claiming set the popped
      // id stays parked, record `queued` and lock held, and maintenance puts it back on its lane.
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, roundsToStale: 2, keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 's' }, fn)
      const startFailed = vi.fn()
      m.hook('startFailed', startFailed)
      await m.queue(job, 'r1', null)
      await m.queue(job, 'r2', null)
      // Redis fills up right after the pop (while the popped record is being read).
      const realHget = redis.hget
      vi.mocked(redis.hget).mockImplementationOnce(async (...args: any[]) => {
        redis._setOom(true)
        return (realHget as any)(...args)
      })

      expect(await m.popAndExecute()).toBe(true)
      expect(fn).not.toHaveBeenCalled()
      expect((await readRecord('s#r1'))?.status).toBe('queued')
      expect(await m.isLocked('s#r1')).toBe(true)
      expect(await redis.zscore('redisjm:g:claiming', 's#r1')).not.toBeNull()
      expect(startFailed).toHaveBeenCalledWith(expect.objectContaining({ jobId: 's#r1', reason: 'oom', action: 'deferred' }))

      // Pops pause for a stale-threshold window; under OOM the pop would be refused anyway.
      redis._setOom(false)
      expect(await m.popAndExecute()).toBe(false)
      expect(redis._dump().lists.get(QUEUE)).toEqual(['s#r2'])
      await vi.advanceTimersByTimeAsync(2500)
      // Past the stale threshold: maintenance re-queues the parked run at the head of its lane.
      const result = await m.performMaintenance()
      expect(result.requeuedCount).toBe(1)
      expect(redis._dump().lists.get(QUEUE)).toEqual(['s#r1', 's#r2'])
      expect(await redis.zscore('redisjm:g:claiming', 's#r1')).toBeNull()
      expect(await m.popAndExecute()).toBe(true)
      expect(await m.popAndExecute()).toBe(true)
      expect(fn).toHaveBeenCalledTimes(2)
      expect((await readRecord('s#r1'))?.status).toBe('finished')
    })

    it('a claim that landed although its reply was lost is not pushed back (no second queue entry)', async () => {
      // WHY: recovery used to LPUSH the id back blindly. When the claim HAD landed (the connection broke
      // after the write), the run was then both `running` and queued again — a second execution.
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 's' }, fn)
      const startFailed = vi.fn()
      m.hook('startFailed', startFailed)
      await m.queue(job, 'r1', null)
      let lost = false
      for (const cmd of ['eval', 'evalsha'] as const) {
        const original = vi.mocked(redis[cmd]).getMockImplementation()!
        vi.mocked(redis[cmd]).mockImplementation(async (...args: any[]) => {
          const reply = await (original as any)(...args)
          if (!lost && args.some((a) => String(a).includes('"status":"running"'))) {
            lost = true
            throw connectionError()
          }
          return reply
        })
      }

      expect(await m.popAndExecute()).toBe(true)
      expect(lost).toBe(true)
      expect(fn).not.toHaveBeenCalled()
      expect((await readRecord('s#r1'))?.status).toBe('running') // claimed — left for its stale reclaim
      expect(redis._dump().lists.get(QUEUE)).toEqual([])
      expect(startFailed).not.toHaveBeenCalled()
      expect(logger.mock.calls.some(([msg]) => /no longer owns its record/.test(msg as string))).toBe(true)
    })

    it('a full Redis pops nothing: the pop script is refused before its LPOP', async () => {
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 's' }, fn)
      await m.queue(job, 'r1', null)
      redis._setOom(true)
      await expect(m.popAndExecute()).rejects.toMatchObject({ message: expect.stringMatching(/^OOM/) })
      expect(redis._dump().lists.get(QUEUE)).toEqual(['s#r1'])
      expect(redis._dump().zsets.get('redisjm:g:claiming')?.size ?? 0).toBe(0)
      redis._setOom(false)
      expect(await m.popAndExecute()).toBe(true)
      expect(fn).toHaveBeenCalledTimes(1)
    })

    it('a Redis failure reading the popped record → requeued, never lost-and-locked', async () => {
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 's' }, vi.fn())
      const startFailed = vi.fn()
      m.hook('startFailed', startFailed)
      await m.queue(job, 'r1', null)
      vi.mocked(redis.hget).mockRejectedValueOnce(connectionError())

      expect(await m.popAndExecute()).toBe(true)
      expect(redis._dump().lists.get(QUEUE)).toEqual(['s#r1'])
      expect(await m.isLocked('s#r1')).toBe(true)
      expect(startFailed).toHaveBeenCalledWith(expect.objectContaining({ action: 'requeued', reason: 'connection' }))
    })

    it('a job-level start hook throwing AFTER the claim fails the run through the normal failure path', async () => {
      // WHY: the claim had already flipped the record to `running`; the throw used to leave it running
      // + locked until maintenance staled it (and the manager-level retry/error logic never ran).
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 's', attempts: 2, backoff: 10_000 }, fn)
      job.hook('start', () => { throw new Error('start hook boom') })
      const jobError = vi.fn()
      job.hook('error', jobError)
      const startFailed = vi.fn()
      const retry = vi.fn()
      m.hook('startFailed', startFailed)
      m.hook('retry', retry)

      await m.queue(job, 'r1', null)
      await m.popAndExecute()
      expect(fn).not.toHaveBeenCalled()
      let record = await readRecord('s#r1')
      expect(record?.status).toBe('delayed') // attempt 1 of 2 → retry
      expect(record?.error).toBe('start hook boom')
      expect(retry).toHaveBeenCalledTimes(1)
      expect(jobError).toHaveBeenCalledTimes(1)
      expect(startFailed).toHaveBeenCalledWith(expect.objectContaining({ action: 'failed', reason: 'unknown' }))

      // Final attempt: terminal error, lock released.
      record!.status = 'queued'
      delete record!.readyAt
      await redis.hset(LOG, 's#r1', JSON.stringify(record))
      await redis.zrem('redisjm:g:delayed', 's#r1')
      await redis.rpush(QUEUE, 's#r1')
      await m.popAndExecute()
      expect((await readRecord('s#r1'))?.status).toBe('error')
      expect(await m.isLocked('s#r1')).toBe(false)
    })

    it('a job-level start hook throwing BEFORE the claim fails the queued record terminally (no spin)', async () => {
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = new Job({ jobName: 's' }, vi.fn())
      job.hook('start', () => { throw new Error('pre-claim boom') }) // registered before the manager's hook
      m.registerJob(job)
      const startFailed = vi.fn()
      m.hook('startFailed', startFailed)

      await m.queue(job, 'r1', null)
      await m.popAndExecute()
      const record = await readRecord('s#r1')
      expect(record?.status).toBe('error')
      expect(record?.error).toBe('pre-claim boom')
      expect(await m.isLocked('s#r1')).toBe(false)
      expect(redis._dump().lists.get(QUEUE)).toEqual([])
      expect(startFailed).toHaveBeenCalledWith(expect.objectContaining({ action: 'failed' }))
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('manager-level hooks are isolated observers', () => {
    it('a throwing manager start hook does not fail or strand the run', async () => {
      // WHY: it used to propagate out of the claim, leaving the record `running` + locked with the
      // handler never invoked.
      const logger = vi.fn()
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 'o' }, fn)
      m.hook('start', () => { throw new Error('observer boom') })
      const second = vi.fn()
      m.hook('start', second)

      await m.queue(job, 'r1', null)
      await m.popAndExecute()
      expect(fn).toHaveBeenCalledTimes(1)
      expect(second).toHaveBeenCalledTimes(1) // later observers still notified
      expect((await readRecord('o#r1'))?.status).toBe('finished')
      expect(await m.isLocked('o#r1')).toBe(false)
      expect(logger.mock.calls.some(([msg, e]) => /manager "start" hook threw/.test(msg as string) && (e as Error).message === 'observer boom')).toBe(true)
    })

    it('a rejecting retry observer cannot change a scheduled retry or silence later observers', async () => {
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 'o', attempts: 2, backoff: 10_000 }, () => { throw new Error('fail') })
      m.hook('retry', async () => { throw new Error('retry observer boom') })
      const later = vi.fn()
      m.hook('retry', later)
      await m.queue(job, 'r1', null)
      await m.popAndExecute()
      expect(later).toHaveBeenCalledTimes(1)
      expect((await readRecord('o#r1'))?.status).toBe('delayed')
      expect(await redis.zscore('redisjm:g:delayed', 'o#r1')).not.toBeNull()
      expect(await m.isLocked('o#r1')).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('refactor review regressions', () => {
    it('a claim re-sent after its reply was lost (reconnect) runs the job instead of reading as superseded', async () => {
      // WHY: the re-run claim lost its compare-and-set to the original and found the record `running`
      // under this very execution; the claim mutator only accepted `queued`, so the run was skipped as
      // superseded and its record sat `running` until maintenance staled it.
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      let ran = 0
      const job = m.createJob({ jobName: 'rs' }, async () => { ran++ })
      await m.queue(job, 'r1', null)
      let replayed = false
      for (const cmd of ['evalsha', 'eval'] as const) {
        const real = redis[cmd].bind(redis) as (...args: any[]) => Promise<unknown>
        ;(redis as any)[cmd] = async (...args: any[]) => {
          const reply = await real(...args)
          if (!replayed && args.some((a) => typeof a === 'string' && a.includes('"status":"running"'))) {
            replayed = true
            return real(...args) // the re-sent copy's reply replaces the lost original's
          }
          return reply
        }
      }
      expect(await m.popAndExecute()).toBe(true)
      expect(replayed).toBe(true)
      expect(ran).toBe(1)
      expect((await readRecord('rs#r1'))?.status).toBe('finished')
      expect(await m.isLocked('rs#r1')).toBe(false)
    })

    it('unregisterJob drops the job from the old-lane allow-lists at once', async () => {
      // WHY: allow-lists are refreshed at most every 5s; until then the unregistered job's old-lane entries
      // were popped as unknown jobs (requeue budget burned — or, with no budget, failed).
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false, unknownJobRequeueLimit: 0 })
      await m.enqueue(new Job({ jobName: 'moved' }, async () => {}), 'r1', null) // on the default lane
      const moved = m.createJob({ jobName: 'moved', lane: 'fresh' }, async () => {})
      const other = m.createJob({ jobName: 'other', lane: 'fresh' }, async () => {})
      await m.queue(other, 'x', null)
      expect(await m.popAndExecute()).toBe(true) // discovers the old lane, pops other#x
      m.unregisterJob(moved)
      expect(await m.popAndExecute()).toBe(false)
      expect((await readRecord('moved#r1'))?.status).toBe('queued')
      expect(await redis.lrange(QUEUE, 0, -1)).toEqual(['moved#r1'])
    })

    it("maintenance's claiming drop and promotion's stray-entry drop spare an entry re-scored since the read", async () => {
      // WHY: both were blind ZREMs decided on an earlier read — erasing a fresh pop's `claiming` mark /
      // a retry's `delayed` entry that landed in between. Now compare-and-delete on the score read.
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false, heartbeatInterval: 50 })
      m.createJob({ jobName: 'z' }, async () => {})
      await seed({ jobId: 'z#c', status: 'running', executionId: 'e1', startedAt: Date.now(), heartbeat: Date.now() })
      await seed({ jobId: 'z#d', status: 'running', executionId: 'e2', startedAt: Date.now(), heartbeat: Date.now() })
      await redis.zadd(CLAIMING, Date.now() - 60_000, 'z#c')
      await redis.zadd('redisjm:g:delayed', Date.now() - 1000, 'z#d')
      const realZrange = redis.zrangebyscore.bind(redis) as (...args: any[]) => Promise<string[]>
      ;(redis as any).zrangebyscore = async (...args: any[]) => {
        const reply = await realZrange(...args)
        // Re-scored right after the read: a fresh pop / a retry scheduled meanwhile.
        if (args[0] === CLAIMING) await redis.zadd(CLAIMING, 111, 'z#c')
        else await redis.zadd('redisjm:g:delayed', Date.now() + 60_000, 'z#d')
        return reply
      }
      await m.performMaintenance()
      expect(await redis.zscore(CLAIMING, 'z#c')).toBe('111')
      await m.popAndExecute()
      expect(await redis.zscore('redisjm:g:delayed', 'z#d')).not.toBeNull()
    })
  })
})
