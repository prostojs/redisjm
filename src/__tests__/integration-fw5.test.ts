/**
 * Opt-in integration suite for the 0.3.0 features, against REAL Redis servers: settle-on-abort end to end
 * (`abortGraceMs`), `inFlightCount()`, the fleet registry (the `TIME`-based presence scripts), `listQueued()`
 * (the read-only script on real `cjson`, incl. a 0.1.x-shaped record) + `getMany()`, and the `maintenance`
 * hook. Two servers are involved:
 * - the shared server at `REDIS_URL`. Only this suite's own `redisjm:ifw5-*` keys are ever touched — never
 *   FLUSHALL / FLUSHDB / CONFIG SET on it: it is shared with other apps.
 * - a DEDICATED, throwaway `redis-server` spawned by the harness (`--maxmemory 2mb --maxmemory-policy
 *   noeviction`) for the out-of-memory behaviour of the fleet registry.
 *
 * Skipped when `REDIS_URL` is unset; the OOM block is additionally skipped without a `redis-server` binary.
 *
 * HOW TO RUN: `REDIS_URL=redis://localhost:6379 pnpm test`
 */
import { describe, expect, it, vi } from 'vitest'
import { JobAbortedError } from '../errors'
import type { RedisJM } from '../redisjm'
import type { MaintenanceEventPayload, QueuedEntry } from '../types'
import { PRESENCE_SCRIPT, runScript } from '../scripts'
import { popOnly, REDIS_SERVER_BIN, REDIS_URL, sleep, startHung, until, useDedicatedRedis, useSharedRedis } from './integration-helpers'

const ids = (entries: QueuedEntry[]): string[] => entries.map((e) => e.jobId)

describe.skipIf(!REDIS_URL)('0.3.0 integration (shared Redis)', () => {
  const h = useSharedRedis('ifw5')
  const { newGroup, newManager } = h

  describe('settle-on-abort (abortGraceMs)', () => {
    it('stop({ abort: true }) frees a hung handler after the grace: attempts 1 → error record, lock released', async () => {
      const group = newGroup()
      const manager = newManager({ heartbeatInterval: 1000, maintenanceInterval: 0, abortGraceMs: 50 }, group)
      const errors: Error[] = []
      manager.hook('error', (p) => { errors.push(p.error) })
      await startHung(manager)

      const startedAt = Date.now()
      await manager.stop({ abort: true })
      expect(Date.now() - startedAt).toBeLessThan(1500)

      const record = await manager.get('hang#r1')
      expect(record?.status).toBe('error')
      expect(record?.error).toMatch(/aborted \(manager stopped\) and did not settle within 50ms/)
      expect(await h.redis.sismember(`redisjm:${group}:locks`, 'hang#r1')).toBe(0)
      expect(await h.redis.sismember(`redisjm:${group}:jobs:hang:locks`, 'hang#r1')).toBe(0)
      expect(await h.redis.zscore(`redisjm:${group}:delayed`, 'hang#r1')).toBeNull()
      expect(errors).toHaveLength(1)
      expect(errors[0]).toBeInstanceOf(JobAbortedError)
    })

    it('stop({ abort: true }) with attempts 2: delayed (lock held, delayed-set entry), retry event', async () => {
      const group = newGroup()
      const manager = newManager({ heartbeatInterval: 1000, maintenanceInterval: 0, abortGraceMs: 50 }, group)
      const retries: unknown[] = []
      manager.hook('retry', (p) => { retries.push(p) })
      await startHung(manager, { attempts: 2, backoff: 60_000 })
      await manager.stop({ abort: true })

      const record = await manager.get('hang#r1')
      expect(record).toMatchObject({ status: 'delayed', attempt: 1 })
      expect(await h.redis.sismember(`redisjm:${group}:locks`, 'hang#r1')).toBe(1)
      expect(await h.redis.zscore(`redisjm:${group}:delayed`, 'hang#r1')).toBe(String(record!.readyAt))
      expect(retries).toHaveLength(1)
    })
  })

  describe('inFlightCount', () => {
    it('equals inFlight().total through a run, and gates the enqueue exactly like maxInFlight', async () => {
      const manager = newManager({ heartbeatInterval: 50, maintenanceInterval: 0 })
      let release!: () => void
      const job = manager.createJob({ jobName: 'c', maxInFlight: 2 }, () => new Promise<void>((r) => { release = r }))
      expect(await manager.inFlightCount('c')).toBe(0)
      expect((await job.enqueue('a', null)).status).toBe('queued')
      expect((await job.enqueue('b', null, undefined, { delay: 60_000 })).status).toBe('queued')
      expect(await manager.inFlightCount('c')).toBe(2)
      expect((await job.enqueue('x', null)).status).toBe('busy')
      expect((await manager.inFlight('c')).total).toBe(2)

      const run = manager.popAndExecute()
      await until(async () => (await manager.get('c#a'))?.status === 'running')
      expect(await manager.inFlightCount('c')).toBe(2)
      release()
      await run
      expect(await manager.inFlightCount('c')).toBe(1)
      expect((await manager.inFlight('c')).total).toBe(1)
      await manager.unqueue('c#b')
      expect(await manager.inFlightCount('c')).toBe(0)
    })
  })

  describe('fleet registry', () => {
    it('registers started instances on Redis server time, reports their capacity, and drops them on stop()', async () => {
      const group = newGroup()
      const options = { heartbeatInterval: 50, roundsToStale: 4, maintenanceInterval: 0 }
      const a = newManager({ ...options, concurrency: 4, laneConcurrency: { images: 2 }, instanceLabel: 'pod-a' }, group)
      a.createJob({ jobName: 'x' }, async () => {})
      a.createJob({ jobName: 'i', lane: 'images' }, async () => {})
      const b = newManager({ ...options, concurrency: 3 }, group)
      b.createJob({ jobName: 'i', lane: 'images' }, async () => {})
      a.start(20)
      b.start(20)
      await until(async () => (await a.fleet()).instances.length === 2)

      const fleet = await b.fleet()
      expect(fleet.instances.map((i) => i.instanceId)).toEqual([a.getInstanceId(), b.getInstanceId()].sort())
      expect(fleet.slots).toBe(7)
      expect(fleet.lanes).toEqual({ default: { instances: 1, slots: 4 }, images: { instances: 2, slots: 5 } })
      const entry = fleet.instances.find((i) => i.instanceId === a.getInstanceId())!
      expect(entry).toMatchObject({ label: 'pod-a', concurrency: 4, lanes: ['default', 'images'], laneConcurrency: { images: 2 }, busy: 0 })
      expect(entry.expiresAt - entry.seenAt).toBe(200)
      expect(entry.startedAt).toBeGreaterThan(0)

      await a.stop()
      expect((await b.fleet()).instances.map((i) => i.instanceId)).toEqual([b.getInstanceId()])
      await b.stop()
      expect((await b.fleet()).instances).toEqual([])
      expect(await h.redis.exists(`redisjm:${group}:instances`, `redisjm:${group}:instance-info`)).toBe(0)
    })

    it('an instance that stops refreshing (a crash) lapses after its ttl and is pruned by the next refresh', async () => {
      const group = newGroup()
      const options = { heartbeatInterval: 50, roundsToStale: 3, maintenanceInterval: 0 }
      const crashed = newManager(options, group)
      const alive = newManager(options, group)
      crashed.start(20)
      alive.start(20)
      await until(async () => (await alive.fleet()).instances.length === 2)

      clearInterval((crashed as any).presenceTimer) // no more refreshes, and no deregistration
      ;(crashed as any).presenceTimer = undefined
      await until(async () => (await alive.fleet()).instances.length === 1, 3000)
      expect((await alive.fleet()).instances[0].instanceId).toBe(alive.getInstanceId())
      // The live instance's next refresh prunes the lapsed entry from both keys.
      await until(async () => (await h.redis.zcard(`redisjm:${group}:instances`)) === 1)
      expect(await h.redis.hlen(`redisjm:${group}:instance-info`)).toBe(1)
    })

    it('the presence script renews the lease without info only while the info is stored; otherwise it asks for it', async () => {
      const group = newGroup()
      const keys = [`redisjm:${group}:instances`, `redisjm:${group}:instance-info`]
      const beat = (info: string) => runScript(h.redis, PRESENCE_SCRIPT, keys, ['x', 60_000, info, 180_000])
      expect(await beat('')).toBe(1) // no info stored, none sent: nothing registered
      expect(await h.redis.zscore(keys[0], 'x')).toBeNull()
      expect(await beat('{"v":1}')).toBe(0)
      const first = Number(await h.redis.zscore(keys[0], 'x'))
      await sleep(5)
      expect(await beat('')).toBe(0) // lease renewed, info untouched
      expect(Number(await h.redis.zscore(keys[0], 'x'))).toBeGreaterThan(first)
      expect(await h.redis.hget(keys[1], 'x')).toBe('{"v":1}')
      // Both keys carry a key-level expiry so an abandoned group cleans up.
      for (const key of keys) {
        const ttl = await h.redis.pttl(key)
        expect(ttl).toBeGreaterThan(170_000)
        expect(ttl).toBeLessThanOrEqual(180_000)
      }
    })

    it('presence: false registers nothing', async () => {
      const group = newGroup()
      const manager = newManager({ heartbeatInterval: 50, maintenanceInterval: 0, presence: false }, group)
      manager.start(20)
      await sleep(150)
      expect(await h.redis.exists(`redisjm:${group}:instances`, `redisjm:${group}:instance-info`)).toBe(0)
      expect((await manager.fleet()).instances).toEqual([])
    })
  })

  describe('listQueued / getMany', () => {
    it('lists claiming, then lane lists head to tail, then delayed by readyAt — on real Redis scripts', async () => {
      const group = newGroup()
      const m = newManager({ maintenanceInterval: 0 }, group)
      const a = m.createJob({ jobName: 'a' }, async () => {})
      const b = m.createJob({ jobName: 'b', lane: 'images' }, async () => {})
      await m.queue(a, '1', null)
      await m.queue(a, '2', null)
      await m.queueFirst(a, '0', null)
      await m.queue(b, '1', { big: 'x'.repeat(100) })
      await m.queue(a, 'd', null, { delay: 5000 })
      await m.queue(b, 'd', null, { delay: 1000 })
      expect((await popOnly(m, group))?.jobId).toBe('a#0')

      const page = await m.listQueued()
      expect(ids(page.entries)).toEqual(['a#0', 'a#1', 'a#2', 'b#1', 'b#d', 'a#d'])
      expect(page.complete).toBe(true)
      expect(page.entries.map((e) => e.lane)).toEqual(['default', 'default', 'default', 'images', 'images', 'default'])
      expect(page.entries[0].poppedAt).toBeGreaterThan(0)
      expect(page.entries[4].readyAt).toBe((await m.get('b#d'))!.readyAt)
      expect(page.entries.some((e) => 'inputs' in e)).toBe(false)

      expect(ids((await m.listQueued({ lane: 'images' })).entries)).toEqual(['b#1', 'b#d'])
      expect(ids((await m.listQueued({ lane: 'default' })).entries)).toEqual(['a#0', 'a#1', 'a#2', 'a#d'])
      expect(ids((await m.listQueued({ jobName: 'a', limit: 2, offset: 1 })).entries)).toEqual(['a#1', 'a#2'])
      const second = await m.listQueued({ limit: 4, offset: 0 })
      expect(second.nextOffset).toBe(4)
      expect(ids((await m.listQueued({ limit: 4, offset: second.nextOffset })).entries)).toEqual(['b#d', 'a#d'])
    })

    // WHY: the lane of a delayed / popped entry is only in its record, and 0.1.x wrote `inputs` BEFORE `lane`;
    // the real `cjson.decode` must pick the top-level key, never a nested input key.
    it('resolves a 0.1.x-shaped record with real cjson (nested "lane" keys in inputs)', async () => {
      const group = newGroup()
      const m = newManager({ maintenanceInterval: 0 }, group)
      const record = (jobId: string, tail: string, readyAt: number) => {
        const [jobName, runId] = jobId.split('#')
        return `{"jobId":"${jobId}","jobName":"${jobName}","runId":"${runId}","inputs":{"lane":"decoy","deep":{"lane":"decoy2"}}${tail},`
          + `"targetGroup":"${group}","status":"delayed","progress":0,"readyAt":${readyAt}}`
      }
      const log = `redisjm:${group}:log`
      const delayed = `redisjm:${group}:delayed`
      await h.redis.hset(log, 'old#1', record('old#1', ',"lane":"real"', 100))
      await h.redis.zadd(delayed, 100, 'old#1')
      await h.redis.hset(log, 'old#2', record('old#2', '', 200))
      await h.redis.zadd(delayed, 200, 'old#2')
      await h.redis.hset(log, 'junk#1', '{not json')
      await h.redis.zadd(delayed, 300, 'junk#1')
      await h.redis.zadd(delayed, 400, 'ghost#1') // no record at all

      expect((await m.listQueued({ lane: 'real' })).entries.map((e) => [e.jobId, e.lane])).toEqual([['old#1', 'real']])
      expect(ids((await m.listQueued({ lane: 'decoy' })).entries)).toEqual([])
      expect(ids((await m.listQueued({ lane: 'default' })).entries)).toEqual(['old#2'])
      const all = (await m.listQueued()).entries
      expect(all.map((e) => [e.jobId, e.lane])).toEqual([['old#1', 'real'], ['old#2', 'default'], ['junk#1', undefined], ['ghost#1', undefined]])
    })

    it('stops with complete: false at the decode bound, on real Redis', async () => {
      const m = newManager({ maintenanceInterval: 0 })
      const job = m.createJob({ jobName: 'cap', lane: 'x' }, async () => {})
      await m.enqueueMany(job, Array.from({ length: 560 }, (_, i) => ({ runId: String(i), inputs: null })), { delay: 60_000 })
      const page = await m.listQueued({ lane: 'x', limit: 1000 })
      expect(page.complete).toBe(false)
      expect(page.entries).toHaveLength(500)
      expect(page.nextOffset).toBeUndefined()
      // Without a lane filter only the entries that land in the page are decoded.
      const unfiltered = await m.listQueued({ limit: 400, offset: 100 })
      expect(unfiltered).toMatchObject({ complete: true, nextOffset: 500 })
      expect(unfiltered.entries).toHaveLength(400)
      const tooMany = await m.listQueued({ limit: 600 })
      expect(tooMany.complete).toBe(false) // landing in the page costs a decode each
      expect(tooMany.entries).toHaveLength(500)
    })

    it('stops with complete: false at the scan bound; a deep unfiltered offset skips by LLEN', async () => {
      const m = newManager({ maintenanceInterval: 0 })
      const job = m.createJob({ jobName: 'big', lane: 'x' }, async () => {})
      await m.enqueueMany(job, Array.from({ length: 10_050 }, (_, i) => ({ runId: String(i), inputs: null })))
      const page = await m.listQueued({ jobName: 'nomatch' })
      expect(page.complete).toBe(false)
      expect(page.entries).toEqual([])
      const deep = await m.listQueued({ offset: 10_040, limit: 100 })
      expect(deep.complete).toBe(true)
      expect(deep.entries).toHaveLength(10)
    })

    it('getMany reads many records in one call, in order, with undefined for missing ones', async () => {
      const m = newManager({ maintenanceInterval: 0 })
      const job = m.createJob({ jobName: 'g' }, async () => {})
      await job.enqueueMany(Array.from({ length: 700 }, (_, i) => ({ runId: String(i), inputs: { i } })))
      const wanted = ['g#699', 'g#none', 'g#0', 'g#699', ...Array.from({ length: 600 }, (_, i) => `g#${i + 50}`)]
      const result = await m.getMany(wanted)
      expect(result).toHaveLength(wanted.length)
      expect(result.slice(0, 4).map((r) => r?.jobId)).toEqual(['g#699', undefined, 'g#0', 'g#699'])
      expect(result[0]?.inputs).toEqual({ i: 699 })
      expect(result.slice(4).every((r, i) => r?.jobId === `g#${i + 50}`)).toBe(true)
      expect(await m.getMany([])).toEqual([])
    })
  })

  describe('maintenance hook', () => {
    it('the timer pass emits its result before memoryPressure; a held lock emits nothing', async () => {
      const group = newGroup()
      const manager = newManager({ heartbeatInterval: 50, maintenanceInterval: 100 }, group)
      const payloads: MaintenanceEventPayload[] = []
      manager.hook('maintenance', (p) => { payloads.push(p) })
      manager.start(20)
      await until(() => payloads.length >= 2, 3000)
      expect(payloads[0]).toMatchObject({ result: { mode: 'full' }, failedOps: 0 })
      expect(payloads[0].durationMs).toBeGreaterThanOrEqual(0)
      await manager.stop()

      const other = newManager({ maintenanceInterval: 0 }, group)
      const seen = vi.fn()
      other.hook('maintenance', seen)
      await h.redis.set(`redisjm:${group}:maintenance-lock`, 'someone-else', 'PX', 5000)
      expect(await other.runMaintenance()).toBeNull()
      expect(seen).not.toHaveBeenCalled()
    })
  })
})

describe.skipIf(!REDIS_URL || !REDIS_SERVER_BIN)('0.3.0 integration (dedicated maxmemory Redis)', () => {
  const h = useDedicatedRedis('ifw5oom')
  const { fillToOom, newGroup, newManager } = h

  it('fleet() still reads under OOM, a refused refresh logs and registers nothing, and stop() still deregisters', async () => {
    const group = newGroup()
    const options = { heartbeatInterval: 50, roundsToStale: 20, maintenanceInterval: 0 }
    const first = newManager(options, group)
    first.start(20)
    await until(async () => (await first.fleet()).instances.length === 1)

    await fillToOom()
    expect((await first.fleet()).instances.map((i) => i.instanceId)).toEqual([first.getInstanceId()]) // the read works

    // A refresh is a write (a flag-less script): refused up front, nothing is written.
    await expect(runScript(h.redis, PRESENCE_SCRIPT, [`redisjm:${group}:instances`, `redisjm:${group}:instance-info`], ['x', 1000, '{}']))
      .rejects.toMatchObject({ message: expect.stringMatching(/^OOM/) })
    const logs: string[] = []
    const second = newManager({ ...options, logger: (message) => { logs.push(message) } }, group)
    second.start(20)
    await until(() => logs.some((message) => /fleet presence refresh failed \(oom\)/.test(message)))
    expect(await h.redis.hexists(`redisjm:${group}:instance-info`, second.getInstanceId())).toBe(0)

    // Deletions are accepted under OOM: the first instance leaves the registry on stop().
    await first.stop()
    expect((await first.fleet()).instances.map((i) => i.instanceId)).not.toContain(first.getInstanceId())
  })

  it('listQueued() and getMany() are reads: they work under OOM', async () => {
    const group = newGroup()
    const manager = newManager({ maintenanceInterval: 0 }, group)
    const job = manager.createJob({ jobName: 'q' }, async () => {})
    await job.queue('r1', { a: 1 })
    await job.queue('r2', { a: 2 }, undefined, { delay: 60_000 })
    await fillToOom()
    expect(ids((await manager.listQueued()).entries)).toEqual(['q#r1', 'q#r2'])
    expect((await manager.getMany(['q#r2', 'q#nope'])).map((r) => r?.jobId)).toEqual(['q#r2', undefined])
  })
})
