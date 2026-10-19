/**
 * Unit tests (mock Redis) for the 0.5.0 features: named resources shared across jobs
 * (`JobMetadata.resources` + `RedisJMOptions.resourceCaps`, `limitedBy`, slot release on every lock path,
 * maintenance prune, `resourceInFlight*`, `health().resources`) and the enqueue payload contract
 * (`maxRunIdBytes` → `'run-id-too-large'`, `JobMetadata.validate` → `'invalid'`). The real-Redis
 * counterparts live in `integration-resources.test.ts`.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { RedisJMEnqueueError } from '../errors'
import { Job } from '../job'
import { PRUNE_RESOURCES_SCRIPT } from '../scripts'
import type { EnqueueFailedEventPayload } from '../types'
import { createMockRedis } from './mock-redis'
import { DELAYED, hung, jobLocks, JOBS, LOCKS, LOG, mockHelpers, QUEUE, RESOURCES, resKey } from './unit-helpers'

describe('0.5.0 features', () => {
  let redis: ReturnType<typeof createMockRedis>
  beforeEach(() => {
    redis = createMockRedis()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  const { newManager, readRecord, seed, setOf, zsetOf } = mockHelpers(() => redis)

  // ---------------------------------------------------------------------------------------------
  describe('FW-37 resources: configuration', () => {
    // WHY: a resource without a cap would be silently unenforced — the point of declaring it is the cap.
    it('a job declaring a resource missing from resourceCaps is refused at registerJob and at enqueue', () => {
      const m = newManager({ resourceCaps: { writes: 1 } })
      expect(() => m.createJob({ jobName: 'a', resources: ['wirtes'] }, vi.fn())).toThrow(/resourceCaps has no cap for it/)
      const unregistered = new Job({ jobName: 'b', resources: ['other'] }, vi.fn())
      return expect(m.enqueue(unregistered, 'r', null)).rejects.toThrow(/declares resource "other"/)
    })

    it('rejects bad resource names (reserved "job", "__" prefix, key-unsafe chars) and bad caps', () => {
      expect(() => newManager({ resourceCaps: { job: 1 } })).toThrow(TypeError)
      expect(() => newManager({ resourceCaps: { __x: 1 } })).toThrow(TypeError)
      expect(() => newManager({ resourceCaps: { 'a:b': 1 } })).toThrow(TypeError)
      expect(() => newManager({ resourceCaps: { ok: -1 } })).toThrow(/finite number >= 0/)
      expect(() => newManager({ resourceCaps: { ok: Number.NaN } })).toThrow(TypeError)
      expect(newManager({ resourceCaps: { ok: 2.9 } }).getOptions().resourceCaps).toEqual({ ok: 2 })
      const m = newManager({ resourceCaps: { ok: 1 } })
      expect(() => m.createJob({ jobName: 'a', resources: ['job'] }, vi.fn())).toThrow(TypeError)
      return expect(m.resourceInFlightCount('a#b')).rejects.toThrow(TypeError)
    })

    // WHY: the resolved list is cached per job; an in-place edit of the declared array must not be missed.
    it('a resources array mutated in place after a first enqueue is re-resolved', async () => {
      const m = newManager({ resourceCaps: { api: 5, writes: 5 } })
      const declared = ['api']
      const a = m.createJob({ jobName: 'a', resources: declared }, vi.fn())
      expect((await a.enqueue('1', null)).status).toBe('queued')
      declared.push('writes')
      expect((await a.enqueue('2', null)).status).toBe('queued')
      expect(setOf(resKey('writes'))).toEqual(['a#2'])
      declared.push('nope')
      await expect(a.enqueue('3', null)).rejects.toThrow(/declares resource "nope"/)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-37 resources: admission', () => {
    it('caps runs ACROSS jobs: the run past the cap is busy with limitedBy = the resource, nothing written', async () => {
      const m = newManager({ resourceCaps: { writes: 2 } })
      const a = m.createJob({ jobName: 'a', resources: ['writes'] }, vi.fn())
      const b = m.createJob({ jobName: 'b', resources: ['writes'] }, vi.fn())
      expect(await a.enqueue('1', null)).toEqual({ status: 'queued', jobId: 'a#1' })
      expect(await b.enqueue('1', null, undefined, { delay: 1000 })).toEqual({ status: 'queued', jobId: 'b#1' })
      expect(await b.enqueue('2', null)).toEqual({ status: 'busy', jobId: 'b#2', limitedBy: 'writes' })
      expect(await a.queue('2', null)).toBe(false)
      expect(await readRecord('b#2')).toBeNull()
      expect(setOf(LOCKS)).toEqual(['a#1', 'b#1'])
      expect(setOf(resKey('writes'))).toEqual(['a#1', 'b#1'])
      expect(setOf(RESOURCES)).toEqual(['writes'])
      expect((await readRecord('a#1'))?.resources).toEqual(['writes'])
      // dedupe still wins over a full resource
      expect((await a.enqueue('1', null)).status).toBe('deduped')
      expect(await m.resourceInFlightCount('writes')).toBe(2)
      expect(await m.resourceInFlight('writes')).toEqual({ total: 2, queued: 1, delayed: 1, running: 0, jobs: { a: 1, b: 1 } })
    })

    it('the job\'s own maxInFlight is checked first and reports limitedBy "job"', async () => {
      const m = newManager({ resourceCaps: { writes: 5 } })
      const a = m.createJob({ jobName: 'a', resources: ['writes'], maxInFlight: 1 }, vi.fn())
      expect((await a.enqueue('1', null)).status).toBe('queued')
      expect(await a.enqueue('2', null)).toEqual({ status: 'busy', jobId: 'a#2', limitedBy: 'job' })
      const plain = m.createJob({ jobName: 'p', maxInFlight: 0 }, vi.fn())
      expect(await plain.enqueue('1', null)).toEqual({ status: 'busy', jobId: 'p#1', limitedBy: 'job' })
    })

    // WHY: a job needing two contended things must never take one and wait on the other.
    it('several resources are checked and taken atomically: a full second resource leaves the first untouched', async () => {
      const m = newManager({ resourceCaps: { api: 4, writes: 1 } })
      const w = m.createJob({ jobName: 'w', resources: ['writes'] }, vi.fn())
      const both = m.createJob({ jobName: 'both', resources: ['api', 'writes', 'api'] }, vi.fn())
      expect((await w.enqueue('1', null)).status).toBe('queued')
      expect(await both.enqueue('1', null)).toEqual({ status: 'busy', jobId: 'both#1', limitedBy: 'writes' })
      expect(setOf(resKey('api'))).toEqual([])
      await m.unqueue('w#1')
      expect((await both.enqueue('1', null)).status).toBe('queued')
      expect(setOf(resKey('api'))).toEqual(['both#1'])
      expect(setOf(resKey('writes'))).toEqual(['both#1'])
      expect((await readRecord('both#1'))?.resources).toEqual(['api', 'writes']) // deduped
    })

    it('enqueueMany counts the batch against the cap in entry order; a lane cap is checked after resources', async () => {
      const m = newManager({ resourceCaps: { writes: 2 }, laneCaps: { default: 10 } })
      const a = m.createJob({ jobName: 'a', resources: ['writes'] }, vi.fn())
      const results = await a.enqueueMany([{ runId: '1', inputs: null }, { runId: '1', inputs: null }, { runId: '2', inputs: null }, { runId: '3', inputs: null }])
      expect(results.map((r) => r.status)).toEqual(['queued', 'deduped', 'queued', 'busy'])
      expect(results[3]).toMatchObject({ limitedBy: 'writes' })
      const full = newManager({ resourceCaps: { writes: 0 }, laneCaps: { default: 0 } })
      const f = full.createJob({ jobName: 'f', resources: ['writes'] }, vi.fn())
      expect((await f.enqueue('1', null)).limitedBy).toBe('writes')
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-37 resources: release on every lock path', () => {
    it('finish, final error, unqueue release the slot; a scheduled retry keeps it', async () => {
      const m = newManager({ resourceCaps: { writes: 3 } })
      const ok = m.createJob({ jobName: 'ok', resources: ['writes'] }, vi.fn())
      const bad = m.createJob({ jobName: 'bad', resources: ['writes'] }, () => { throw new Error('boom') })
      const retry = m.createJob({ jobName: 'retry', resources: ['writes'], attempts: 2, backoff: 60_000 }, () => { throw new Error('boom') })
      await ok.queue('1', null)
      await m.popAndExecute()
      expect(setOf(resKey('writes'))).toEqual([])
      await bad.queue('1', null)
      await m.popAndExecute()
      expect((await readRecord('bad#1'))?.status).toBe('error')
      expect(setOf(resKey('writes'))).toEqual([])
      await retry.queue('1', null)
      await m.popAndExecute()
      expect((await readRecord('retry#1'))?.status).toBe('delayed')
      expect(setOf(resKey('writes'))).toEqual(['retry#1'])
      await m.unqueue('retry#1')
      expect(setOf(resKey('writes'))).toEqual([])
      expect(zsetOf(DELAYED)).toEqual([])
    })

    it('keepFinishedInterval: 0 (drop) releases the slot too', async () => {
      const m = newManager({ resourceCaps: { writes: 1 }, keepFinishedInterval: 0 })
      const ok = m.createJob({ jobName: 'ok', resources: ['writes'] }, vi.fn())
      await ok.queue('1', null)
      await m.popAndExecute()
      expect(await readRecord('ok#1')).toBeNull()
      expect(setOf(resKey('writes'))).toEqual([])
    })

    // WHY: a pod that died mid-run must not hold the slot forever — the heartbeat-lapse stale frees it.
    it('a crashed run (heartbeat lapsed) is staled by maintenance and its slot freed; a resurrection re-takes it', async () => {
      const m = newManager({ resourceCaps: { writes: 1 }, heartbeatInterval: 50, roundsToStale: 2 })
      const job = m.createJob({ jobName: 'w', resources: ['writes'] }, vi.fn())
      await seed({ jobId: 'w#1', status: 'running', heartbeat: Date.now() - 10_000, startedAt: Date.now() - 10_000, resources: ['writes'], executionId: 'x' })
      await redis.sadd(resKey('writes'), 'w#1')
      expect((await job.enqueue('2', null)).limitedBy).toBe('writes')
      expect((await m.performMaintenance()).staleCount).toBe(1)
      expect((await readRecord('w#1'))?.status).toBe('stale')
      expect(setOf(resKey('writes'))).toEqual([])
      // The "dead" run proves itself alive: stale → running re-takes lock + slot (no cap check).
      const resurrect = (m as any).updateLog('w#1', (record: any) => {
        record.status = 'running'
        record.heartbeat = Date.now()
        delete record.finishedAt
        delete record.staleReason
      })
      expect((await resurrect).outcome).toBe('written')
      expect(setOf(resKey('writes'))).toEqual(['w#1'])
      expect(setOf(LOCKS)).toEqual(['w#1'])
    })

    it('maxRunMs stales a hung run and frees its slot', async () => {
      const m = newManager({ resourceCaps: { writes: 1 }, maxRunMs: 1, abortGraceMs: 10 })
      const job = m.createJob({ jobName: 'h', resources: ['writes'] }, hung)
      await job.queue('r1', null)
      m.start(5)
      await vi.waitFor(async () => expect((await readRecord('h#r1'))?.status).toBe('running'))
      expect(setOf(resKey('writes'))).toEqual(['h#r1'])
      await new Promise((r) => setTimeout(r, 5))
      await m.performMaintenance()
      expect(await readRecord('h#r1')).toMatchObject({ status: 'stale', staleReason: 'maxRunMs' })
      expect(setOf(resKey('writes'))).toEqual([])
      await m.stop({ abort: true })
    })

    // WHY: the releasing instance may run another deploy of the job definition (or not register it).
    it('the release follows the RECORD\'s resources, not this instance\'s job definition', async () => {
      const producer = newManager({ resourceCaps: { old: 1 } })
      await producer.enqueue(new Job({ jobName: 'w', resources: ['old'] }, vi.fn()), '1', null)
      const consumer = newManager({ resourceCaps: { new: 1 } })
      consumer.createJob({ jobName: 'w', resources: ['new'] }, vi.fn())
      await consumer.popAndExecute()
      expect(setOf(resKey('old'))).toEqual([])
      expect(setOf(resKey('new'))).toEqual([])
    })

    it('a foreign record\'s bad resource names never reach a key', async () => {
      const m = newManager()
      m.createJob({ jobName: 'w' }, vi.fn())
      await seed({ jobId: 'w#1', status: 'queued', enqueuedAt: Date.now(), resources: ['ok', 'x:y', 42 as any, '__r'] })
      await redis.rpush(QUEUE, 'w#1')
      await redis.sadd(resKey('ok'), 'w#1')
      await m.popAndExecute()
      expect(setOf(resKey('ok'))).toEqual([])
      expect([...redis._dump().sets.keys()].filter((k) => k.includes(':res:'))).toEqual([resKey('ok')])
    })

    // WHY: an orphan purge has no record to name the resources; maintenance must still free the slot.
    it('maintenance prunes slots whose lock is gone and drops an empty resource from the registry', async () => {
      const m = newManager({ resourceCaps: { writes: 1 } })
      const job = m.createJob({ jobName: 'w', resources: ['writes'] }, vi.fn())
      await job.queue('live', null)
      await redis.sadd(resKey('writes'), 'w#leaked')
      expect(await m.resourceInFlightCount('writes')).toBe(2)
      await m.performMaintenance()
      expect(setOf(resKey('writes'))).toEqual(['w#live'])
      expect(setOf(RESOURCES)).toEqual(['writes'])
      await m.unqueue('w#live')
      await m.performMaintenance()
      expect(setOf(RESOURCES)).toEqual([])
    })

    // WHY: maintenance only prunes REGISTERED sets. A resurrection re-takes the lock + slots of a run whose
    // sets the registries may already have dropped (empty after the stale release), so it must re-register
    // them — else a later record-less release would leak the job lock-set and resource-set members forever.
    it('a resurrection re-registers its job and resources, so a later record-less release is still pruned', async () => {
      const m = newManager({ resourceCaps: { writes: 1 }, heartbeatInterval: 50, roundsToStale: 2 })
      const job = m.createJob({ jobName: 'w', resources: ['writes'] }, vi.fn())
      await job.queue('1', null)
      // The worker that claimed it died: `running` with a long-dead heartbeat, off the lane.
      await redis.lrem(QUEUE, 0, 'w#1')
      const record = await readRecord('w#1')
      await seed({ ...record!, status: 'running', heartbeat: Date.now() - 10_000, startedAt: Date.now() - 10_000, executionId: 'x' })
      expect((await m.performMaintenance()).staleCount).toBe(1)
      await m.performMaintenance() // the empty sets drop out of both registries
      expect(setOf(JOBS)).toEqual([])
      expect(setOf(RESOURCES)).toEqual([])

      const resurrect = (m as any).updateLog('w#1', (r: any) => {
        r.status = 'running'
        r.heartbeat = Date.now()
        delete r.finishedAt
        delete r.staleReason
      })
      expect((await resurrect).outcome).toBe('written')
      expect(setOf(jobLocks('w'))).toEqual(['w#1'])
      expect(setOf(resKey('writes'))).toEqual(['w#1'])
      expect(setOf(JOBS)).toEqual(['w'])
      expect(setOf(RESOURCES)).toEqual(['writes'])

      // A release that cannot name the sets (lock + record gone), and no new enqueue: still pruned.
      await redis.srem(LOCKS, 'w#1')
      await redis.hdel(LOG, 'w#1')
      await m.performMaintenance()
      expect(setOf(jobLocks('w'))).toEqual([])
      expect(setOf(resKey('writes'))).toEqual([])
      expect(setOf(JOBS)).toEqual([])
      expect(setOf(RESOURCES)).toEqual([])
    })

    it('prunes every registered resource of a pass in one script call', async () => {
      const m = newManager({ resourceCaps: { a: 1, b: 1 } })
      await redis.sadd(resKey('a'), 'x#1')
      await redis.sadd(resKey('b'), 'x#2')
      await redis.sadd(RESOURCES, 'a', 'b')
      await m.performMaintenance()
      expect(setOf(resKey('a'))).toEqual([])
      expect(setOf(resKey('b'))).toEqual([])
      expect(setOf(RESOURCES)).toEqual([])
      const pruneCalls = vi.mocked(redis.evalsha).mock.calls.filter(([sha]) => sha === PRUNE_RESOURCES_SCRIPT.sha)
      expect(pruneCalls).toHaveLength(1)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-37 resources: observability', () => {
    it('health() lists each configured resource with its in-flight count and cap', async () => {
      const m = newManager({ resourceCaps: { writes: 3, api: 1 } })
      const job = m.createJob({ jobName: 'w', resources: ['writes'] }, vi.fn())
      await job.queue('1', null)
      expect((await m.health()).resources).toEqual({ writes: { inFlight: 1, cap: 3 }, api: { inFlight: 0, cap: 1 } })
      expect((await newManager().health()).resources).toEqual({})
    })

    it('resourceInFlight counts running runs and ignores drift (members without a live record)', async () => {
      const m = newManager({ resourceCaps: { writes: 3 } })
      await redis.sadd(resKey('writes'), 'gone#1')
      await seed({ jobId: 'w#1', status: 'running', heartbeat: Date.now(), resources: ['writes'] })
      await redis.sadd(resKey('writes'), 'w#1')
      expect(await m.resourceInFlight('writes')).toEqual({ total: 1, queued: 0, delayed: 0, running: 1, jobs: { w: 1 } })
      expect(await m.resourceInFlightCount('writes')).toBe(2)
      expect(await m.resourceInFlight('unused')).toEqual({ total: 0, queued: 0, delayed: 0, running: 0, jobs: {} })
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-38 maxRunIdBytes', () => {
    it('default 1024 bytes (UTF-8): a longer single enqueue throws run-id-too-large, fires enqueueFailed, writes nothing', async () => {
      const m = newManager()
      const failed: EnqueueFailedEventPayload[] = []
      m.hook('enqueueFailed', (p) => { failed.push(p) })
      const job = m.createJob({ jobName: 'j' }, vi.fn())
      expect(await job.queue('x'.repeat(1024), null)).toBe(true)
      const evalCalls = vi.mocked(redis.evalsha).mock.calls.length
      const err = await job.queue('é'.repeat(513), null).catch((e: unknown) => e) // 1026 bytes, 513 chars
      expect(err).toBeInstanceOf(RedisJMEnqueueError)
      expect((err as RedisJMEnqueueError).reason).toBe('run-id-too-large')
      expect((err as RedisJMEnqueueError).message).toMatch(/runId is 1026 bytes, limit is 1024/)
      expect((err as RedisJMEnqueueError).message.length).toBeLessThan(400) // the huge jobId is truncated in the message
      expect(vi.mocked(redis.evalsha).mock.calls.length).toBe(evalCalls)
      expect(failed.map((p) => p.reason)).toEqual(['run-id-too-large'])
      expect(setOf(LOCKS)).toEqual([`j#${'x'.repeat(1024)}`])
    })

    it('the job value wins; 0 disables (job or manager)', async () => {
      const long = 'r'.repeat(5000)
      const m = newManager({ maxRunIdBytes: 10 })
      expect(m.getOptions().maxRunIdBytes).toBe(10)
      const strict = m.createJob({ jobName: 's' }, vi.fn())
      const loose = m.createJob({ jobName: 'l', maxRunIdBytes: 0 }, vi.fn())
      const tight = newManager({ maxRunIdBytes: 0 }).createJob({ jobName: 't', maxRunIdBytes: 3 }, vi.fn())
      await expect(strict.queue('12345678901', null)).rejects.toMatchObject({ reason: 'run-id-too-large' })
      expect(await loose.queue(long, null)).toBe(true)
      expect(await newManager({ maxRunIdBytes: 0 }).queue(new Job({ jobName: 'z' }, vi.fn()), long, null)).toBe(true)
      await expect(tight.queue('abcd', null)).rejects.toMatchObject({ reason: 'run-id-too-large' })
    })

    // WHY: JS callers pass numeric ids (`o.id`); 0.4 accepted them, the size check must not crash on them.
    it('measures a non-string runId by its string form instead of throwing', async () => {
      const m = newManager({ maxRunIdBytes: 3 })
      const job = m.createJob({ jobName: 'n' }, vi.fn())
      expect(await job.enqueue(123 as any, null)).toEqual({ status: 'queued', jobId: 'n#123' })
      await expect(job.queue(1234 as any, null)).rejects.toMatchObject({ reason: 'run-id-too-large' })
    })

    it('enqueueMany reports it per entry (size, limit) and takes no slot for it', async () => {
      const m = newManager({ maxRunIdBytes: 4, resourceCaps: { writes: 1 } })
      const job = m.createJob({ jobName: 'j', resources: ['writes'] }, vi.fn())
      const results = await job.enqueueMany([{ runId: 'toolong', inputs: null }, { runId: 'ok', inputs: null }])
      expect(results).toEqual([
        { status: 'run-id-too-large', jobId: 'j#toolong', size: 7, limit: 4 },
        { status: 'queued', jobId: 'j#ok' },
      ])
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-38 validate', () => {
    it('a single enqueue refused by validate throws invalid with the reason as detail; enqueueFailed fires', async () => {
      const m = newManager()
      const failed: EnqueueFailedEventPayload[] = []
      m.hook('enqueueFailed', (p) => { failed.push(p) })
      const validate = vi.fn(({ inputs }: { runId: string; inputs: { ids: string[] } }) =>
        inputs.ids.length > 2 ? 'too-many-ids' : undefined)
      const job = m.createJob<{ ids: string[] }>({ jobName: 'v', validate }, vi.fn())
      expect(await job.queue('ok', { ids: ['a'] })).toBe(true)
      expect(validate).toHaveBeenCalledWith({ runId: 'ok', inputs: { ids: ['a'] } })
      const err = await job.queue('bad', { ids: ['a', 'b', 'c'] }).catch((e: unknown) => e) as RedisJMEnqueueError
      expect(err).toBeInstanceOf(RedisJMEnqueueError)
      expect(err.reason).toBe('invalid')
      expect(err.detail).toBe('too-many-ids')
      expect(err.jobId).toBe('v#bad')
      expect(failed).toHaveLength(1)
      expect(failed[0]).toMatchObject({ reason: 'invalid', jobId: 'v#bad' })
      expect(await readRecord('v#bad')).toBeNull()
    })

    it('enqueueMany reports { status: invalid, reason } per entry; the others enqueue; no slot taken', async () => {
      const m = newManager({ resourceCaps: { writes: 1 } })
      const job = m.createJob<number>({ jobName: 'v', resources: ['writes'], validate: ({ inputs }) => (inputs < 0 ? 'negative' : undefined) }, vi.fn())
      const results = await job.enqueueMany([{ runId: 'a', inputs: -1 }, { runId: 'b', inputs: 1 }])
      expect(results).toEqual([{ status: 'invalid', jobId: 'v#a', reason: 'negative' }, { status: 'queued', jobId: 'v#b' }])
    })

    it('runs after the built-in checks: an oversized runId or unserializable inputs never reach validate', async () => {
      const validate = vi.fn(() => undefined)
      const m = newManager({ maxRunIdBytes: 3, maxInputsBytes: 10 })
      const job = m.createJob<unknown>({ jobName: 'v', validate }, vi.fn())
      const results = await job.enqueueMany([
        { runId: 'long', inputs: null },
        { runId: 'a', inputs: 1n },
        { runId: 'b', inputs: 'x'.repeat(20) },
      ])
      expect(results.map((r) => r.status)).toEqual(['run-id-too-large', 'inputs-unserializable', 'inputs-too-large'])
      expect(validate).not.toHaveBeenCalled()
    })

    // WHY: only a reason refuses — an empty string or a non-string (a sloppy validator returning
    // `false` / `null`) must not refuse the entry with an empty reason.
    it('only a non-empty string refuses: \'\', undefined and non-strings accept', async () => {
      const m = newManager()
      const job = m.createJob<unknown>({ jobName: 'v', validate: ({ inputs }) => inputs as any }, vi.fn())
      const results = await job.enqueueMany([
        { runId: 'a', inputs: '' },
        { runId: 'b', inputs: undefined },
        { runId: 'c', inputs: false },
        { runId: 'd', inputs: 0 },
        { runId: 'e', inputs: 'nope' },
      ])
      expect(results.map((r) => r.status)).toEqual(['queued', 'queued', 'queued', 'queued', 'invalid'])
      expect(await job.queue('f', '')).toBe(true)
    })

    it('a throwing validator propagates as is and nothing of the batch is written', async () => {
      const m = newManager()
      const job = m.createJob<number>({ jobName: 'v', validate: ({ inputs }) => { if (inputs === 2) throw new Error('bug'); return undefined } }, vi.fn())
      await expect(job.enqueueMany([{ runId: 'a', inputs: 1 }, { runId: 'b', inputs: 2 }])).rejects.toThrow('bug')
      expect(setOf(LOCKS)).toEqual([])
      expect(redis._dump().hashes.get(LOG)?.size ?? 0).toBe(0)
    })
  })
})
