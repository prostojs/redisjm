/**
 * Unit tests (mock Redis) for the 0.3.0 features: settle-on-abort at the manager level (`abortGraceMs`,
 * `JobAbortedError`), `inFlightCount()`, the fleet registry (`presence` / `fleet()` / `instanceLabel` /
 * `getInstanceId()`), `listQueued()` + `getMany()`, and the `maintenance` hook. The job-level abort-grace
 * tests live in `job.test.ts`; the real-Redis counterparts in `integration-fw5.test.ts`.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { JobAbortedError } from '../errors'
import { Job } from '../job'
import { RedisJM } from '../redisjm'
import type { MaintenanceEventPayload, QueuedEntry } from '../types'
import { connectionError, createMockRedis } from './mock-redis'
import { CLAIMING, hung, laneKey, LOCKS, LOG, mockHelpers, popOnly, QUEUE } from './unit-helpers'

const DELAYED = 'redisjm:g:delayed'
const INSTANCES = 'redisjm:g:instances'
const INSTANCE_INFO = 'redisjm:g:instance-info'
const MAINTENANCE_LOCK = 'redisjm:g:maintenance-lock'

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

describe('0.3.0 features', () => {
  let redis: ReturnType<typeof createMockRedis>
  beforeEach(() => {
    redis = createMockRedis()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  const { newManager, readRecord, setOf, zsetOf, listOf, startHung } = mockHelpers(() => redis)

  // ---------------------------------------------------------------------------------------------
  describe('FW-09 abortGraceMs at the manager level', () => {
    // WHY: `stop({ abort: true })` used to wait for a handler that ignores its signal until its execution
    // timeout. With a grace the shutdown is bounded and the attempt goes through the normal failure path.
    it('stop({ abort: true }) settles a hung handler after the grace: attempts 1 → error, lock released', async () => {
      const m = newManager({ heartbeatInterval: 1000, abortGraceMs: 30 })
      const errors: Error[] = []
      const retries: unknown[] = []
      m.hook('error', (p) => { errors.push(p.error) })
      m.hook('retry', (p) => { retries.push(p) })
      await startHung(m)

      const startedAt = Date.now()
      await m.stop({ abort: true })
      expect(Date.now() - startedAt).toBeLessThan(1000)

      const record = await readRecord('h#r1')
      expect(record?.status).toBe('error')
      expect(record?.error).toMatch(/aborted \(manager stopped\) and did not settle within 30ms/)
      expect(setOf(LOCKS)).toEqual([])
      expect(errors).toHaveLength(1)
      expect(errors[0]).toBeInstanceOf(JobAbortedError)
      expect((errors[0] as JobAbortedError).reason).toBe('manager stopped')
      expect(retries).toHaveLength(0)
      expect((m as any).laneInFlight.get(QUEUE)).toBe(0)
      expect((m as any).inFlightRuns.size).toBe(0)
    })

    // WHY: the abandoned attempt consumed an attempt like any failure — with attempts left it is retried.
    it('stop({ abort: true }) with attempts 2 schedules the retry: delayed, lock kept, retry event', async () => {
      const m = newManager({ heartbeatInterval: 1000, abortGraceMs: 20 })
      const retries: Array<{ error: Error; attempt: number }> = []
      const errors: unknown[] = []
      m.hook('retry', (p) => { retries.push({ error: p.error, attempt: p.attempt }) })
      m.hook('error', (p) => { errors.push(p) })
      await startHung(m, { attempts: 2, backoff: 60_000 })
      await m.stop({ abort: true })

      const record = await readRecord('h#r1')
      expect(record?.status).toBe('delayed')
      expect(record?.attempt).toBe(1)
      expect(setOf(LOCKS)).toEqual(['h#r1'])
      expect(zsetOf(DELAYED)).toEqual(['h#r1'])
      expect(retries).toHaveLength(1)
      expect(retries[0].error).toBeInstanceOf(JobAbortedError)
      expect(errors).toHaveLength(0)
    })

    it('the detached handler resolving or rejecting later writes nothing', async () => {
      const m = newManager({ heartbeatInterval: 1000, abortGraceMs: 10, logger: vi.fn() })
      let release!: () => void
      let ctxRef: any
      const job = m.createJob({ jobName: 'h' }, (_i, ctx) => {
        ctxRef = ctx
        return new Promise<void>((r) => { release = r })
      })
      await m.queue(job, 'r1', null)
      m.start(5)
      await vi.waitFor(async () => expect((await readRecord('h#r1'))?.status).toBe('running'))
      await m.stop({ abort: true })

      const before = await redis.hget(LOG, 'h#r1')
      vi.mocked(redis.evalsha).mockClear()
      vi.mocked(redis.eval).mockClear()
      release()
      await ctxRef.setProgress(0.9)
      await ctxRef.setAttrs({ late: true })
      await sleep(20)
      expect(await redis.hget(LOG, 'h#r1')).toBe(before)
      expect(vi.mocked(redis.evalsha).mock.calls.length + vi.mocked(redis.eval).mock.calls.length).toBe(0)
    })

    // WHY: a superseded run (record re-enqueued under a new owner) must leave the successor untouched even
    // though its attempt is now settled by the grace.
    it('ownership loss by supersession: the run settles, the successor record, lock and entry are untouched', async () => {
      const m = newManager({ heartbeatInterval: 20, abortGraceMs: 20 })
      const events: string[] = []
      for (const name of ['error', 'retry', 'finish'] as const) m.hook(name, () => { events.push(name) })
      let started = false
      let release!: () => void
      const job = m.createJob({ jobName: 's' }, () => new Promise<void>((r) => { started = true; release = r }))
      await m.queue(job, 'r', 'first')
      const run = m.popAndExecute()
      await vi.waitFor(() => expect(started).toBe(true))
      await m.unqueue('s#r')
      await m.queue(job, 'r', 'successor')
      const successor = await redis.hget(LOG, 's#r')

      await run // settles after the next heartbeat notices the loss, plus the grace
      expect(await redis.hget(LOG, 's#r')).toBe(successor)
      expect(setOf(LOCKS)).toEqual(['s#r'])
      expect(listOf(QUEUE)).toEqual(['s#r'])
      expect(events).toEqual([])
      release()
      await sleep(20)
      expect(await redis.hget(LOG, 's#r')).toBe(successor)
    })

    it('ownership loss by unqueue mid-run: the run settles, the orphaned lock is released, error is emitted', async () => {
      const m = newManager({ heartbeatInterval: 20, abortGraceMs: 20 })
      const errors: Error[] = []
      m.hook('error', (p) => { errors.push(p.error) })
      let started = false
      const job = m.createJob({ jobName: 's' }, () => { started = true; return hung() })
      await m.queue(job, 'r', null)
      const run = m.popAndExecute()
      await vi.waitFor(() => expect(started).toBe(true))
      await m.unqueue('s#r')
      await run
      expect(await readRecord('s#r')).toBeNull()
      expect(setOf(LOCKS)).toEqual([])
      expect(errors).toHaveLength(1)
      expect(errors[0]).toBeInstanceOf(JobAbortedError)
    })

    it('a maxRunMs stale + grace + attempts 2 schedules the retry', async () => {
      const m = newManager({ heartbeatInterval: 20, abortGraceMs: 20, maxRunMs: 60 })
      const retries: unknown[] = []
      m.hook('retry', (p) => { retries.push(p) })
      let started = false
      const job = m.createJob({ jobName: 'x', attempts: 2, backoff: 60_000 }, () => { started = true; return hung() })
      await m.queue(job, 'r', null)
      const run = m.popAndExecute()
      await vi.waitFor(() => expect(started).toBe(true))
      await sleep(90)
      expect((await m.performMaintenance()).staleCount).toBe(1)
      expect((await readRecord('x#r'))?.staleReason).toBe('maxRunMs')

      await run // the next heartbeat is rejected (a maxRunMs stale is final), the grace elapses
      const record = await readRecord('x#r')
      expect(record?.status).toBe('delayed')
      expect(record?.staleReason).toBeUndefined()
      expect(setOf(LOCKS)).toEqual(['x#r'])
      expect(retries).toHaveLength(1)
    })

    // WHY: a fenced write (superseded / terminal / unqueued record) changes nothing, so it must not look like
    // an update to the manager's observers — an abandoned handler's late setProgress used to emit one.
    it('a fenced ctx write emits no `update` event; an owned one does', async () => {
      const m = newManager({ heartbeatInterval: 1000 })
      const updates: unknown[] = []
      m.hook('update', (p) => { updates.push(p.progress) })
      let started = false
      let release!: () => void
      let ctxRef: any
      const job = m.createJob({ jobName: 's' }, (_i, ctx) => new Promise<void>((r) => { ctxRef = ctx; started = true; release = r }))
      await m.queue(job, 'r', 'first')
      const run = m.popAndExecute()
      await vi.waitFor(() => expect(started).toBe(true))

      await ctxRef.setProgress(0.5)
      expect(updates).toEqual([0.5])
      expect((await readRecord('s#r'))?.progress).toBe(0.5)

      await m.unqueue('s#r')
      await m.queue(job, 'r', 'successor') // the run no longer owns its record
      const successor = await redis.hget(LOG, 's#r')
      await ctxRef.setProgress(0.9)
      expect(updates).toEqual([0.5])
      expect(await redis.hget(LOG, 's#r')).toBe(successor)

      release()
      await run
    })

    it('a job-level `false` overrides the manager default (stop() keeps waiting for that handler)', async () => {
      const m = newManager({ heartbeatInterval: 1000, abortGraceMs: 10 })
      let started = false
      let release!: () => void
      const job = m.createJob({ jobName: 'k', abortGraceMs: false }, () => new Promise<void>((r) => { started = true; release = r }))
      await m.queue(job, 'r', null)
      m.start(5)
      await vi.waitFor(() => expect(started).toBe(true))
      let stopped = false
      const stopping = m.stop({ abort: true }).then(() => { stopped = true })
      await sleep(100)
      expect(stopped).toBe(false)
      release()
      await stopping
      expect((await readRecord('k#r'))?.status).toBe('finished')
    })

    it('a job-level number overrides the manager default', async () => {
      const m = newManager({ heartbeatInterval: 1000, abortGraceMs: 60_000 })
      await startHung(m, { abortGraceMs: 10 })
      await m.stop({ abort: true })
      expect((await readRecord('h#r1'))?.status).toBe('error')
    })

    it('validates abortGraceMs: constructor, registerJob and getOptions', () => {
      for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 31, true, '5']) {
        expect(() => new RedisJM(redis, 'g', { abortGraceMs: bad as never })).toThrow(TypeError)
        expect(() => newManager().createJob({ jobName: 'v', abortGraceMs: bad as never }, vi.fn())).toThrow(TypeError)
      }
      expect(newManager().getOptions().abortGraceMs).toBe(false)
      expect(newManager({ abortGraceMs: 0 }).getOptions().abortGraceMs).toBe(0)
      expect(newManager({ abortGraceMs: false }).getOptions().abortGraceMs).toBe(false)
      const m = newManager()
      expect(() => m.createJob({ jobName: 'ok1', abortGraceMs: 0 }, vi.fn())).not.toThrow()
      expect(() => m.createJob({ jobName: 'ok2', abortGraceMs: false }, vi.fn())).not.toThrow()
      expect(m.getOptions().abortGraceMs).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-10 inFlightCount', () => {
    it('follows enqueue, delayed enqueue, pop-before-claim, claim, retry, finish, unqueue and maintenance stale', async () => {
      const m = newManager({ heartbeatInterval: 20 })
      let release!: () => void
      const job = m.createJob({ jobName: 'c' }, () => new Promise<void>((r) => { release = r }))
      const flaky = m.createJob({ jobName: 'f', attempts: 2, backoff: 60_000 }, () => { throw new Error('boom') })
      // Counts and the record-reading inFlight() agree at every step (no drift in a pure 0.3 group).
      const expectCount = async (jobName: string, expected: number) => {
        expect(await m.inFlightCount(jobName)).toBe(expected)
        expect((await m.inFlight(jobName)).total).toBe(expected)
      }

      await expectCount('c', 0)
      await m.queue(job, 'a', null)
      await expectCount('c', 1)
      await m.queue(job, 'd', null, { delay: 60_000 })
      await expectCount('c', 2)
      // Popped but not yet claimed: the run still holds its lock.
      expect((await popOnly(m))?.jobId).toBe('c#a')
      expect(zsetOf(CLAIMING)).toEqual(['c#a'])
      await expectCount('c', 2)
      await m.unqueue('c#a')
      await expectCount('c', 1)

      await m.queue(job, 'b', null)
      const run = m.popAndExecute()
      await vi.waitFor(async () => expect((await readRecord('c#b'))?.status).toBe('running'))
      await expectCount('c', 2)
      release()
      await run
      await expectCount('c', 1) // finished: lock released
      await m.unqueue('c#d')
      await expectCount('c', 0)

      // A retry keeps the lock through the backoff; the final failure releases it.
      await m.queue(flaky, 'x', null)
      await m.popAndExecute()
      expect((await readRecord('f#x'))?.status).toBe('delayed')
      await expectCount('f', 1)
      await m.unqueue('f#x')
      await expectCount('f', 0)

      // Maintenance staling a dead run releases its lock.
      await m.queue(job, 'z', null)
      const record = JSON.parse((await redis.hget(LOG, 'c#z'))!)
      await redis.hset(LOG, 'c#z', JSON.stringify({ ...record, status: 'running', startedAt: 1, heartbeat: 1, executionId: 'dead' }))
      await redis.lpop(QUEUE)
      await expectCount('c', 1)
      expect((await m.performMaintenance()).staleCount).toBe(1)
      await expectCount('c', 0)
    })

    // WHY: it is the number `maxInFlight` is enforced against.
    it('count >= maxInFlight exactly when the next enqueue is busy', async () => {
      const m = newManager()
      const job = m.createJob({ jobName: 'm', maxInFlight: 2 }, vi.fn())
      for (let i = 0; i < 4; i++) {
        const before = await m.inFlightCount('m')
        const { status } = await m.enqueue(job, `r${i}`, null)
        expect(status).toBe(before >= 2 ? 'busy' : 'queued')
      }
      expect(await m.inFlightCount('m')).toBe(2)
    })

    it('reads no records: one SCARD, no HGET / HMGET / HSCAN', async () => {
      const m = newManager()
      const job = m.createJob({ jobName: 'c' }, vi.fn())
      await m.queue(job, 'a', null)
      for (const cmd of ['hget', 'hmget', 'hscan', 'smembers', 'scard'] as const) vi.mocked(redis[cmd]).mockClear()
      expect(await m.inFlightCount('c')).toBe(1)
      expect(redis.scard).toHaveBeenCalledTimes(1)
      for (const cmd of ['hget', 'hmget', 'hscan', 'smembers'] as const) expect(redis[cmd]).not.toHaveBeenCalled()
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-11 fleet registry', () => {
    const startAndSettle = async (m: RedisJM) => {
      m.start(60_000)
      await vi.advanceTimersByTimeAsync(10)
    }

    it('start() registers the instance with its capacity; stop() removes the entry', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000, roundsToStale: 3, concurrency: 4, laneConcurrency: { images: 2 }, instanceLabel: 'pod-a' })
      m.createJob({ jobName: 'a' }, vi.fn())
      m.createJob({ jobName: 'i', lane: 'images' }, vi.fn())
      const t0 = Date.now()
      await startAndSettle(m)

      const fleet = await m.fleet()
      expect(fleet.instances).toHaveLength(1)
      const [instance] = fleet.instances
      expect(instance).toMatchObject({
        instanceId: m.getInstanceId(),
        label: 'pod-a',
        concurrency: 4,
        lanes: ['default', 'images'],
        laneConcurrency: { images: 2 },
        busy: 0,
      })
      expect(instance.startedAt).toBe(t0)
      expect(instance.seenAt).toBeGreaterThanOrEqual(t0)
      expect(instance.expiresAt - instance.seenAt).toBe(3000)
      expect(fleet.slots).toBe(4)
      expect(fleet.lanes).toEqual({ default: { instances: 1, slots: 4 }, images: { instances: 1, slots: 2 } })

      await m.stop()
      expect((await m.fleet()).instances).toEqual([])
      expect(redis._dump().hashes.get(INSTANCE_INFO)?.size ?? 0).toBe(0)
      expect(zsetOf(INSTANCES)).toEqual([])
    })

    it('rebuilds the entry on every beat: a job registered after start() and busy runs show up', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000, concurrency: 2, abortGraceMs: 0 })
      m.createJob({ jobName: 'a' }, hung)
      await startAndSettle(m)
      expect((await m.fleet()).instances[0]).toMatchObject({ lanes: ['default'], busy: 0 })

      m.createJob({ jobName: 'i', lane: 'images' }, vi.fn())
      await m.queue(new Job({ jobName: 'a' }, vi.fn()), 'r', null)
      m.wake()
      await vi.advanceTimersByTimeAsync(10)
      await vi.advanceTimersByTimeAsync(1000) // the next beat
      expect((await m.fleet()).instances[0]).toMatchObject({ lanes: ['default', 'images'], busy: 1 })
      const stopping = m.stop({ abort: true })
      await vi.advanceTimersByTimeAsync(10)
      await stopping
    })

    it('aggregates slots, busy and per-lane consumers across instances (lane slots use the laneConcurrency min)', async () => {
      vi.useFakeTimers()
      const m1 = newManager({ heartbeatInterval: 1000, concurrency: 4, laneConcurrency: { images: 2 } })
      m1.createJob({ jobName: 'a' }, vi.fn())
      m1.createJob({ jobName: 'i', lane: 'images' }, vi.fn())
      const m2 = newManager({ heartbeatInterval: 1000, concurrency: 3 })
      m2.createJob({ jobName: 'i', lane: 'images' }, vi.fn())
      await startAndSettle(m1)
      await startAndSettle(m2)

      const fleet = await m2.fleet() // any instance (or a non-consumer) reads the whole group
      expect(fleet.instances.map((i) => i.instanceId)).toEqual([m1.getInstanceId(), m2.getInstanceId()].sort())
      expect(fleet.slots).toBe(7)
      expect(fleet.busy).toBe(0)
      expect(fleet.lanes).toEqual({
        default: { instances: 1, slots: 4 },
        images: { instances: 2, slots: 2 + 3 },
      })
      await m1.stop()
      await m2.stop()
    })

    // WHY: a crashed instance can't deregister; its lease must lapse and the next registration prunes it.
    it('an instance that stops refreshing lapses after its ttl and is pruned by the next registration', async () => {
      vi.useFakeTimers()
      const crashed = newManager({ heartbeatInterval: 1000, roundsToStale: 3 })
      const alive = newManager({ heartbeatInterval: 1000, roundsToStale: 3 })
      await startAndSettle(crashed)
      await startAndSettle(alive)
      expect((await alive.fleet()).instances).toHaveLength(2)

      // Simulated crash: no more refreshes, no deregistration.
      clearInterval((crashed as any).presenceTimer)
      ;(crashed as any).presenceTimer = undefined
      await vi.advanceTimersByTimeAsync(2000)
      expect((await alive.fleet()).instances).toHaveLength(2) // lease (3000ms) not yet lapsed
      await vi.advanceTimersByTimeAsync(2000)
      expect((await alive.fleet()).instances.map((i) => i.instanceId)).toEqual([alive.getInstanceId()])
      // ...and the refresh of the live instance has pruned the dead one's entries.
      expect(zsetOf(INSTANCES)).toEqual([alive.getInstanceId()])
      expect([...(redis._dump().hashes.get(INSTANCE_INFO) ?? new Map()).keys()]).toEqual([alive.getInstanceId()])
      await alive.stop()
      await crashed.stop()
    })

    it('the presence key TTL is monotonic: a shorter heartbeat interval never shortens a longer one', async () => {
      vi.useFakeTimers()
      const slow = newManager({ heartbeatInterval: 10_000, roundsToStale: 3 })
      await startAndSettle(slow)
      const long = redis._dump().keyTtls.get(INSTANCES)!
      const fast = newManager({ heartbeatInterval: 100, roundsToStale: 3 })
      await startAndSettle(fast)
      expect(redis._dump().keyTtls.get(INSTANCES)!).toBeGreaterThanOrEqual(long)
      expect(redis._dump().keyTtls.get(INSTANCE_INFO)!).toBeGreaterThanOrEqual(long)
      await fast.stop()
      await slow.stop()
    })

    it('presence refresh sets a key-level expiry well beyond the lease, renewed on each write', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000, roundsToStale: 3 })
      await startAndSettle(m)
      const ttls = redis._dump().keyTtls
      const first = ttls.get(INSTANCES)!
      expect(ttls.get(INSTANCE_INFO)).toBe(first)
      expect(first - Date.now()).toBeGreaterThanOrEqual(3000 * 3)
      await vi.advanceTimersByTimeAsync(2000)
      expect(ttls.get(INSTANCES)!).toBeGreaterThan(first)
      await m.stop()
    })

    // WHY: roundsToStale 1 is valid for runs, but a presence lease equal to the refresh interval would flicker.
    it('clamps the presence lease to two refresh intervals when roundsToStale <= 1', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000, roundsToStale: 1 })
      await startAndSettle(m)
      const [entry] = (await m.fleet()).instances
      expect(entry.expiresAt - entry.seenAt).toBe(2000)
      await vi.advanceTimersByTimeAsync(1500)
      expect((await m.fleet()).instances).toHaveLength(1)
      await m.stop()
    })

    it('logs that presence is inactive when heartbeatInterval is 0 with presence on', async () => {
      const logger = vi.fn()
      const m = newManager({ heartbeatInterval: 0, logger })
      m.start(20)
      expect(logger.mock.calls.some(([msg]) => /needs heartbeatInterval > 0/.test(String(msg)))).toBe(true)
      await m.stop()
    })

    it('presence: false writes no fleet keys', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000, presence: false })
      await startAndSettle(m)
      await vi.advanceTimersByTimeAsync(5000)
      expect(redis._dump().zsets.has(INSTANCES)).toBe(false)
      expect(redis._dump().hashes.has(INSTANCE_INFO)).toBe(false)
      expect((await m.fleet()).instances).toEqual([])
      await m.stop()
      expect(m.getOptions().presence).toBe(false)
    })

    it('logs a failed refresh once per failure episode, and recovers', async () => {
      vi.useFakeTimers()
      const logger = vi.fn()
      const m = newManager({ heartbeatInterval: 1000, logger })
      const presenceLogs = () => logger.mock.calls.filter(([message]) => /fleet presence refresh failed/.test(message as string)).length
      redis._setOom(true) // the flag-less presence script is refused up front
      await startAndSettle(m)
      await vi.advanceTimersByTimeAsync(3000)
      expect(presenceLogs()).toBe(1)
      expect((await m.fleet()).instances).toEqual([]) // refused: nothing registered (the read still works)

      redis._setOom(false)
      await vi.advanceTimersByTimeAsync(1000)
      expect((await m.fleet()).instances).toHaveLength(1)
      redis._setOom(true)
      await vi.advanceTimersByTimeAsync(1000)
      expect(presenceLogs()).toBe(2) // a new episode is logged again
      redis._setOom(false)
      await m.stop()
    })

    it('sends the info only when it changed (a ZADD-only beat otherwise), and resends when Redis lost it', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000 })
      m.createJob({ jobName: 'a' }, vi.fn())
      const infoWrites = () => vi.mocked(redis.hset).mock.calls.filter(([key]) => key === INSTANCE_INFO).length
      await startAndSettle(m)
      expect(infoWrites()).toBe(1)
      await vi.advanceTimersByTimeAsync(3000) // three unchanged beats
      expect(infoWrites()).toBe(1)
      expect((await m.fleet()).instances).toHaveLength(1)

      m.createJob({ jobName: 'i', lane: 'images' }, vi.fn()) // a new lane: the info changed
      await vi.advanceTimersByTimeAsync(1000)
      expect(infoWrites()).toBe(2)
      expect((await m.fleet()).instances[0].lanes).toEqual(['default', 'images'])

      await redis.hdel(INSTANCE_INFO, m.getInstanceId()) // Redis lost the info (flush / lapse)
      await vi.advanceTimersByTimeAsync(1000)
      expect(infoWrites()).toBe(3)
      expect((await m.fleet()).instances).toHaveLength(1)
      await m.stop()
    })

    it('stop() then start() right away keeps the entry; stop() twice and stop-start-stop never overlap deregistrations', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000 })
      let active = 0
      let maxActive = 0
      let deregs = 0
      const realZrem = vi.mocked(redis.zrem).getMockImplementation()!
      vi.mocked(redis.zrem).mockImplementation(async (...args: unknown[]) => {
        deregs++
        maxActive = Math.max(maxActive, ++active)
        await sleep(0)
        await sleep(0)
        try {
          return await (realZrem as any)(...args)
        } finally {
          active--
        }
      })
      await startAndSettle(m)

      const stopping = m.stop()
      m.start(60_000) // re-armed before the deregistration ran
      await vi.advanceTimersByTimeAsync(10)
      await stopping
      expect((await m.fleet()).instances).toHaveLength(1)
      expect(deregs).toBe(0)

      const first = m.stop()
      const second = m.stop() // nothing left to deregister: awaits the one in flight
      m.start(60_000)
      const third = m.stop()
      await vi.advanceTimersByTimeAsync(10)
      await Promise.all([first, second, third])
      expect((await m.fleet()).instances).toEqual([])
      expect(maxActive).toBe(1)
      expect(deregs).toBeGreaterThanOrEqual(1)
    })

    it('fleet() is a read: it works under maxmemory, and stop() still deregisters', async () => {
      vi.useFakeTimers()
      const m = newManager({ heartbeatInterval: 1000 })
      await startAndSettle(m)
      redis._setOom(true)
      expect((await m.fleet()).instances).toHaveLength(1)
      await m.stop() // deletions are accepted under OOM
      expect((await m.fleet()).instances).toEqual([])
      redis._setOom(false)
    })

    it('skips (and logs) an entry whose info is missing or not an instance record', async () => {
      const logger = vi.fn()
      const m = newManager({ logger })
      const future = Date.now() + 60_000
      await redis.zadd(INSTANCES, future, 'ghost')
      await redis.hset(INSTANCE_INFO, 'ghost', 'not json')
      await redis.zadd(INSTANCES, future, 'no-info')
      await redis.zadd(INSTANCES, future, 'wrong-shape')
      await redis.hset(INSTANCE_INFO, 'wrong-shape', JSON.stringify({ concurrency: 'many' }))
      expect((await m.fleet()).instances).toEqual([])
      expect(logger.mock.calls.map(([message]) => message)).toEqual([
        'skipping unreadable fleet entry "ghost"',
        'skipping unreadable fleet entry "no-info"',
        'skipping unreadable fleet entry "wrong-shape"',
      ])
    })

    it('does not touch Redis on stop() for an instance that never started', async () => {
      const m = newManager()
      vi.mocked(redis.multi).mockClear()
      await m.stop()
      expect(redis.multi).not.toHaveBeenCalled()
    })

    it('getInstanceId is a stable UUID per manager (kept across stop/start)', async () => {
      vi.useFakeTimers()
      const a = newManager()
      const b = newManager()
      expect(a.getInstanceId()).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
      expect(a.getInstanceId()).not.toBe(b.getInstanceId())
      const id = a.getInstanceId()
      await startAndSettle(a)
      await a.stop()
      await startAndSettle(a)
      expect(a.getInstanceId()).toBe(id)
      expect((await a.fleet()).instances.map((i) => i.instanceId)).toEqual([id])
      await a.stop()
    })

    it('validates instanceLabel (string, at most 200 chars)', () => {
      expect(() => new RedisJM(redis, 'g', { instanceLabel: 'x'.repeat(201) })).toThrow(TypeError)
      expect(() => new RedisJM(redis, 'g', { instanceLabel: 5 as never })).toThrow(TypeError)
      expect(new RedisJM(redis, 'g', { instanceLabel: 'x'.repeat(200) }).getOptions().instanceLabel).toHaveLength(200)
      expect(new RedisJM(redis, 'g').getOptions().instanceLabel).toBe('')
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-12 listQueued', () => {
    /** The shared scenario: popped-not-claimed, two lanes (one with a priority insert), two delayed runs. */
    const scenario = async (m: RedisJM) => {
      const a = m.createJob({ jobName: 'a' }, vi.fn())
      const b = m.createJob({ jobName: 'b', lane: 'images' }, vi.fn())
      await m.queue(a, '1', null)
      await m.queue(a, '2', null)
      await m.queueFirst(a, '0', null) // head of the default lane
      await m.queue(b, '1', null)
      await m.queue(a, 'd', null, { delay: 5000 })
      await m.queue(b, 'd', null, { delay: 1000 })
      expect((await popOnly(m))?.jobId).toBe('a#0') // popped, not yet claimed
      return { a, b }
    }
    const ids = (entries: QueuedEntry[]) => entries.map((e) => e.jobId)

    it('lists in pop order: claiming, then the lane lists head to tail, then delayed by readyAt', async () => {
      const m = newManager()
      await scenario(m)
      const page = await m.listQueued()
      expect(ids(page.entries)).toEqual(['a#0', 'a#1', 'a#2', 'b#1', 'b#d', 'a#d'])
      expect(page.complete).toBe(true)
      expect(page.nextOffset).toBeUndefined()
      expect(page.entries.map((e) => [e.status, e.lane])).toEqual([
        ['queued', 'default'],
        ['queued', 'default'],
        ['queued', 'default'],
        ['queued', 'images'],
        ['delayed', 'images'],
        ['delayed', 'default'],
      ])
      const [popped, , , , delayedB, delayedA] = page.entries
      expect(popped).toMatchObject({ jobName: 'a', runId: '0', poppedAt: expect.any(Number) })
      expect(delayedB.readyAt).toBeLessThan(delayedA.readyAt!)
      expect(delayedA.readyAt).toBe(JSON.parse((await redis.hget(LOG, 'a#d'))!).readyAt)
      expect(delayedA.poppedAt).toBeUndefined()
      expect(page.entries[1].readyAt).toBeUndefined()
      // Never any record content.
      for (const entry of page.entries) {
        expect(Object.keys(entry).every((k) => ['jobId', 'jobName', 'runId', 'lane', 'status', 'poppedAt', 'readyAt'].includes(k))).toBe(true)
      }
    })

    it('a lane filter keeps delayed and popped entries only when their RECORD lane matches', async () => {
      const m = newManager()
      await scenario(m)
      expect(ids((await m.listQueued({ lane: 'images' })).entries)).toEqual(['b#1', 'b#d'])
      expect(ids((await m.listQueued({ lane: 'default' })).entries)).toEqual(['a#0', 'a#1', 'a#2', 'a#d'])
      expect(ids((await m.listQueued({ lane: 'nowhere' })).entries)).toEqual([])
    })

    it('a jobName filter matches the job exactly, not by name prefix', async () => {
      const m = newManager()
      const { b } = await scenario(m)
      await m.queue(m.createJob({ jobName: 'ab' }, vi.fn()), '1', null)
      expect(ids((await m.listQueued({ jobName: 'a' })).entries)).toEqual(['a#0', 'a#1', 'a#2', 'a#d'])
      expect(ids((await m.listQueued({ jobName: b.getName() })).entries)).toEqual(['b#1', 'b#d'])
      expect(ids((await m.listQueued({ jobName: 'b', lane: 'default' })).entries)).toEqual([])
    })

    it('pages with offset / limit / nextOffset, identically with and without filters', async () => {
      const m = newManager()
      await scenario(m)
      await m.queue(m.createJob({ jobName: 'ab' }, vi.fn()), '1', null)
      const optionSets = [{}, { lane: 'default' }, { lane: 'images' }, { jobName: 'a' }, { jobName: 'b', lane: 'images' }]
      for (const base of optionSets) {
        const whole = await m.listQueued(base)
        for (const limit of [1, 2, 4]) {
          const pages: string[] = []
          let offset: number | undefined = 0
          while (offset !== undefined) {
            const page: Awaited<ReturnType<RedisJM['listQueued']>> = await m.listQueued({ ...base, limit, offset })
            expect(page.complete).toBe(true)
            expect(page.entries.length).toBeLessThanOrEqual(limit)
            pages.push(...ids(page.entries))
            offset = page.nextOffset
          }
          expect(pages, JSON.stringify({ base, limit })).toEqual(ids(whole.entries))
        }
      }
    })

    it('a page that exactly exhausts the sequence has no nextOffset; a short one points at the rest', async () => {
      const m = newManager()
      await scenario(m)
      const exact = await m.listQueued({ limit: 6 })
      expect(exact.entries).toHaveLength(6)
      expect(exact.nextOffset).toBeUndefined()
      const short = await m.listQueued({ limit: 5 })
      expect(short.nextOffset).toBe(5)
      expect(ids((await m.listQueued({ limit: 5, offset: short.nextOffset })).entries)).toEqual(['a#d'])
      expect((await m.listQueued({ offset: 100 })).entries).toEqual([])
      // Out-of-range limits are clamped, not rejected.
      expect((await m.listQueued({ limit: 0 })).entries).toHaveLength(1)
      expect((await m.listQueued({ limit: 1e9 })).entries).toHaveLength(6)
    })

    // WHY: 0.1.x serialized `inputs` BEFORE `lane`, so a text search for "lane" would find an input key.
    it('resolves a 0.1.x-shaped record (inputs before lane, nested "lane" keys) to its top-level lane', async () => {
      const m = newManager()
      const old = (jobId: string, lane: string | undefined, readyAt: number) => {
        const [jobName, runId] = jobId.split('#')
        const tail = lane === undefined ? '' : `,"lane":"${lane}"`
        return `{"jobId":"${jobId}","jobName":"${jobName}","runId":"${runId}","inputs":{"lane":"decoy","deep":{"lane":"decoy2"}}`
          + `${tail},"targetGroup":"g","status":"delayed","progress":0,"readyAt":${readyAt}}`
      }
      await redis.hset(LOG, 'old#1', old('old#1', 'real', 100))
      await redis.zadd(DELAYED, 100, 'old#1')
      await redis.hset(LOG, 'old#2', old('old#2', undefined, 200)) // no top-level lane: the default lane
      await redis.zadd(DELAYED, 200, 'old#2')
      expect((await m.listQueued({ lane: 'real' })).entries.map((e) => [e.jobId, e.lane])).toEqual([['old#1', 'real']])
      expect(ids((await m.listQueued({ lane: 'decoy' })).entries)).toEqual([])
      expect(ids((await m.listQueued({ lane: 'default' })).entries)).toEqual(['old#2'])
      expect((await m.listQueued()).entries.map((e) => [e.jobId, e.lane])).toEqual([['old#1', 'real'], ['old#2', 'default']])
    })

    it('a delayed / popped entry without a readable record has no lane and is excluded under a lane filter', async () => {
      const m = newManager()
      await redis.zadd(DELAYED, 100, 'ghost#1')
      await redis.zadd(DELAYED, 200, 'junk#1')
      await redis.hset(LOG, 'junk#1', '{not json')
      await redis.zadd(DELAYED, 300, 'scalar#1')
      await redis.hset(LOG, 'scalar#1', '42')
      const all = (await m.listQueued()).entries
      expect(all.map((e) => [e.jobId, e.lane])).toEqual([['ghost#1', undefined], ['junk#1', undefined], ['scalar#1', undefined]])
      expect(all.every((e) => !('lane' in e))).toBe(true)
      expect((await m.listQueued({ lane: 'default' })).entries).toEqual([])
    })

    it('leaves the reserved __maintenance lane out unless it is asked for', async () => {
      const m = newManager()
      m.createJob({ jobName: 'a' }, vi.fn())
      await redis.rpush(laneKey('__maintenance'), '__redisjm_maintenance#legacy')
      await redis.rpush(QUEUE, 'a#1')
      expect(ids((await m.listQueued()).entries)).toEqual(['a#1'])
      const asked = (await m.listQueued({ lane: '__maintenance' })).entries
      expect(asked).toEqual([expect.objectContaining({ jobId: '__redisjm_maintenance#legacy', lane: '__maintenance' })])
    })

    it('finds lanes it has no local registration for through the per-job lane sets', async () => {
      const producer = newManager()
      const consumerSide = newManager() // no jobs registered here
      const job = new Job({ jobName: 'z', lane: 'zlane' }, vi.fn())
      await producer.enqueue(job, '1', null)
      await producer.enqueue(job, '2', null, { delay: 9000 })
      expect(ids((await consumerSide.listQueued()).entries)).toEqual(['z#1', 'z#2'])
      expect(ids((await consumerSide.listQueued({ jobName: 'z' })).entries)).toEqual(['z#1', 'z#2'])
    })

    it('is a single read-only script call (one atomic snapshot) after the lane discovery reads', async () => {
      const m = newManager()
      await scenario(m)
      for (const cmd of ['llen', 'lrange', 'zrangebyscore', 'hget'] as const) vi.mocked(redis[cmd]).mockClear()
      await m.listQueued({ lane: 'images' })
      // Everything is read inside the script emulation; the client issued no per-structure reads.
      for (const cmd of ['llen', 'lrange', 'zrangebyscore'] as const) expect(redis[cmd]).not.toHaveBeenCalled()
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-12 getMany', () => {
    it('returns records in input order, duplicates included, undefined for missing or garbage', async () => {
      const m = newManager()
      const job = m.createJob({ jobName: 'g' }, vi.fn())
      await m.queue(job, '1', { n: 1 })
      await m.queue(job, '2', { n: 2 })
      await redis.hset(LOG, 'g#junk', '{nope')
      await redis.hset(LOG, 'g#scalar', '7')
      const result = await m.getMany(['g#2', 'g#missing', 'g#1', 'g#2', 'g#junk', 'g#scalar'])
      expect(result.map((r) => r?.jobId)).toEqual(['g#2', undefined, 'g#1', 'g#2', undefined, undefined])
      expect(result[0]?.inputs).toEqual({ n: 2 }) // inputs included, as get()
      expect(result[0]).toEqual(await m.get('g#2'))
    })

    it('reads 1 200 ids in ONE pipeline of 500-field HMGETs; [] makes no Redis call', async () => {
      const m = newManager()
      const job = m.createJob({ jobName: 'g' }, vi.fn())
      await m.queue(job, '1', null)
      const many = Array.from({ length: 1200 }, (_, i) => (i % 2 ? 'g#1' : `g#none${i}`))
      for (const cmd of ['pipeline', 'hmget', 'hget'] as const) vi.mocked(redis[cmd]).mockClear()
      const result = await m.getMany(many)
      expect(result).toHaveLength(1200)
      expect(result.filter(Boolean)).toHaveLength(600)
      expect(redis.pipeline).toHaveBeenCalledTimes(1)
      expect(vi.mocked(redis.hmget).mock.calls.map((call) => call.length - 1)).toEqual([500, 500, 200])
      expect(redis.hget).not.toHaveBeenCalled()

      for (const cmd of ['pipeline', 'hmget', 'eval', 'evalsha'] as const) vi.mocked(redis[cmd]).mockClear()
      expect(await m.getMany([])).toEqual([])
      for (const cmd of ['pipeline', 'hmget', 'eval', 'evalsha'] as const) expect(redis[cmd]).not.toHaveBeenCalled()
    })

    it('throws when the read fails (a read error is not a missing record)', async () => {
      const m = newManager()
      vi.mocked(redis.hmget).mockRejectedValueOnce(connectionError())
      await expect(m.getMany(['g#1'])).rejects.toThrow('Connection is closed.')
    })
  })

  // ---------------------------------------------------------------------------------------------
  describe('FW-13 maintenance hook', () => {
    const collect = (m: RedisJM) => {
      const payloads: MaintenanceEventPayload[] = []
      m.hook('maintenance', (p) => { payloads.push(p) })
      return payloads
    }

    it('the timer pass emits its result, zero failed ops and a duration — before memoryPressure', async () => {
      vi.useFakeTimers()
      const m = newManager({ maintenanceInterval: 1000 })
      const order: string[] = []
      m.hook('maintenance', () => { order.push('maintenance') })
      m.hook('memoryPressure', () => { order.push('memoryPressure') })
      redis._setInfo({ used_memory: 900, maxmemory: 1000 })
      const payloads = collect(m)
      m.start(60_000)
      await vi.advanceTimersByTimeAsync(10)
      expect(payloads).toHaveLength(1)
      expect(payloads[0]).toMatchObject({ result: { mode: 'full', staleCount: 0, cleanedCount: 0, requeuedCount: 0 }, failedOps: 0 })
      expect(payloads[0].durationMs).toBeGreaterThanOrEqual(0)
      expect(payloads[0].error).toBeUndefined()
      expect(payloads[0].reason).toBeUndefined()
      expect(order).toEqual(['maintenance', 'memoryPressure'])
      await m.stop()
    })

    it('performMaintenance() and runMaintenance() emit; a pass skipped for a held lock does not', async () => {
      const m = newManager()
      const payloads = collect(m)
      await m.performMaintenance()
      expect(payloads).toHaveLength(1)
      await redis.set(MAINTENANCE_LOCK, 'other-instance', 'PX', 60_000)
      expect(await m.runMaintenance()).toBeNull()
      expect(payloads).toHaveLength(1)
      await redis.del(MAINTENANCE_LOCK)
      expect((await m.runMaintenance())?.mode).toBe('full')
      expect(payloads).toHaveLength(2)
    })

    it('an out-of-memory pass emits the emergency result', async () => {
      const m = newManager()
      const payloads = collect(m)
      redis._setOom(true)
      expect((await m.runMaintenance())?.mode).toBe('emergency')
      expect(payloads).toHaveLength(1)
      expect(payloads[0].result?.mode).toBe('emergency')
      expect(payloads[0].failedOps).toBe(0)
    })

    it('failed pipelined operations are counted with the first error and its reason; the pass still completes', async () => {
      const m = newManager()
      const payloads = collect(m)
      vi.mocked(redis.get).mockRejectedValue(connectionError()) // the cursor reads
      const result = await m.performMaintenance()
      vi.mocked(redis.get).mockReset()
      expect(result.mode).toBe('full')
      expect(payloads).toHaveLength(1)
      expect(payloads[0].result).toEqual(result)
      expect(payloads[0].failedOps).toBeGreaterThan(0)
      expect(payloads[0].reason).toBe('connection')
      expect(payloads[0].error?.message).toBe('Connection is closed.')
    })

    it('a lock connection error emits { result: null } with the error and its reason', async () => {
      const m = newManager()
      const payloads = collect(m)
      vi.mocked(redis.set).mockRejectedValueOnce(connectionError())
      expect(await m.runMaintenance()).toBeNull()
      expect(payloads).toHaveLength(1)
      expect(payloads[0]).toMatchObject({ result: null, failedOps: 0, reason: 'connection' })
      expect(payloads[0].error?.message).toBe('Connection is closed.')
    })

    it('a pass that throws is emitted with result null, then rethrown', async () => {
      const m = newManager()
      const payloads = collect(m)
      vi.mocked(redis.hscan).mockRejectedValueOnce(connectionError())
      await expect(m.runMaintenance()).rejects.toThrow('Connection is closed.')
      expect(payloads).toHaveLength(1)
      expect(payloads[0]).toMatchObject({ result: null, reason: 'connection' })
    })

    it('a direct performMaintenance() that throws emits too, then rethrows', async () => {
      const m = newManager()
      const payloads = collect(m)
      vi.mocked(redis.hscan).mockRejectedValueOnce(connectionError())
      await expect(m.performMaintenance()).rejects.toThrow('Connection is closed.')
      expect(payloads).toHaveLength(1)
      expect(payloads[0]).toMatchObject({ result: null, failedOps: 0, reason: 'connection' })
      expect(payloads[0].durationMs).toBeGreaterThanOrEqual(0)
    })

    it('a throwing observer is logged and affects neither the pass nor the other observers', async () => {
      const logger = vi.fn()
      const m = newManager({ logger })
      m.hook('maintenance', () => { throw new Error('observer bug') })
      const payloads = collect(m)
      const result = await m.performMaintenance()
      expect(result.mode).toBe('full')
      expect(payloads).toHaveLength(1)
      expect(logger.mock.calls.some(([message]) => /manager "maintenance" hook threw/.test(message as string))).toBe(true)
    })

    it('reports what the pass did (cleaned and stale counts)', async () => {
      const m = newManager({ keepFinishedInterval: 10 })
      const payloads = collect(m)
      await redis.hset(LOG, 'old#1', JSON.stringify({ jobId: 'old#1', jobName: 'old', runId: '1', inputs: null, targetGroup: 'g', progress: 0, status: 'finished', finishedAt: 1 }))
      await m.performMaintenance()
      expect(payloads[0].result?.cleanedCount).toBe(1)
    })
  })
})
