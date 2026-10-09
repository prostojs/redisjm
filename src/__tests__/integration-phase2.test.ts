/**
 * Opt-in integration suite for the 0.2.0 phase-2 features against REAL Redis (>= 7.4 for the HPEXPIRE
 * assertions; the suite runs on the local 8.x). Exercises the real Lua scripts (enqueue, pop with
 * old-lane allow-lists, compare-and-set transitions, job pruning), bounded maintenance on a 50k-record
 * log, and the no-ghost-record guarantee at concurrency > 1.
 *
 * Shared server (`REDIS_URL`): only this suite's `redisjm:ith2-*` keys are touched — never FLUSHALL /
 * FLUSHDB / CONFIG SET / SCRIPT FLUSH there. Things that need those (script-cache flush, eviction
 * policy, maxmemory) run on a DEDICATED throwaway `redis-server` spawned here and killed in `afterAll`.
 *
 * HOW TO RUN: `REDIS_URL=redis://localhost:6379 pnpm test`
 */
import { afterEach, describe, expect, it } from 'vitest'
import { Job } from '../job'
import type { JobContext, RedisJMHealth, RedisJMOptions } from '../types'
import { REDIS_SERVER_BIN, REDIS_URL, sleep, until, useDedicatedRedis, useSharedRedis } from './integration-helpers'

describe.skipIf(!REDIS_URL)('phase-2 integration (shared Redis)', () => {
  const h = useSharedRedis('ith2', { logger: false })
  const { newClient, newGroup, newManager } = h

  it('atomic enqueue / enqueueMany: queued, deduped, busy, full — and inFlight without a log scan', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0 }, group)
    const single = new Job({ jobName: 'single', maxInFlight: 1 }, async () => {})
    const capped = new Job({ jobName: 'capped', lane: 'cap', maxQueued: 2 }, async () => {})
    expect(await m.enqueue(single, 'a', null)).toEqual({ status: 'queued', jobId: 'single#a' })
    expect((await m.enqueue(single, 'a', null)).status).toBe('deduped')
    expect((await m.enqueue(single, 'b', null)).status).toBe('busy')
    const batch = await m.enqueueMany(capped, [
      { runId: '1', inputs: 1 }, { runId: '2', inputs: 2 }, { runId: '3', inputs: 3 },
    ], { first: true })
    expect(batch.map((r) => r.status)).toEqual(['queued', 'queued', 'full'])
    expect(await h.redis.lrange(`redisjm:${group}:lane:cap:queue`, 0, -1)).toEqual(['capped#1', 'capped#2'])
    expect(await m.inFlight('capped')).toEqual({ total: 2, queued: 2, delayed: 0, running: 0 })
    expect(await h.redis.smembers(`redisjm:${group}:jobs:capped:lanes`)).toEqual([`redisjm:${group}:lane:cap:queue`])
    expect(await h.redis.sismember(`redisjm:${group}:locks`, 'capped#3')).toBe(0)
  })

  it('enqueueMany reports oversized / unserializable entries per entry; the rest enqueue through the real script', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0, maxInputsBytes: 20 }, group)
    const job = new Job<unknown>({ jobName: 'mixed', maxQueued: 2 }, async () => {})
    const results = await m.enqueueMany(job, [
      { runId: 'a', inputs: 1 },
      { runId: 'big', inputs: 'x'.repeat(50) },
      { runId: 'n', inputs: { n: 1n } },
      { runId: 'b', inputs: 2 },
      { runId: 'c', inputs: 3 },
    ])
    expect(results).toEqual([
      { status: 'queued', jobId: 'mixed#a' },
      { status: 'inputs-too-large', jobId: 'mixed#big', size: 52, limit: 20 },
      { status: 'inputs-unserializable', jobId: 'mixed#n', error: expect.any(TypeError) },
      { status: 'queued', jobId: 'mixed#b' },
      { status: 'full', jobId: 'mixed#c' }, // rejected entries took no cap slot
    ])
    expect(await h.redis.lrange(`redisjm:${group}:queue`, 0, -1)).toEqual(['mixed#a', 'mixed#b'])
    expect((await h.redis.smembers(`redisjm:${group}:locks`)).sort()).toEqual(['mixed#a', 'mixed#b'])
    expect(await h.redis.hexists(`redisjm:${group}:log`, 'mixed#big')).toBe(0)
    expect((await m.get('mixed#b'))?.inputs).toBe(2)
  })

  it('drains an old lane after a lane change without popping or charging other jobs queued there', async () => {
    const group = newGroup()
    const producer = newManager({ maintenanceInterval: 0 }, group)
    await producer.queue(new Job({ jobName: 'neighbor', lane: 'old' }, async () => {}), 'n1', null)
    await producer.queue(new Job({ jobName: 'mover', lane: 'old' }, async () => {}), 'm1', null)
    await producer.queue(new Job({ jobName: 'neighbor', lane: 'old' }, async () => {}), 'n2', null)
    // The consumer runs `mover` on its NEW lane only; it doesn't know `neighbor`.
    const consumer = newManager({ maintenanceInterval: 0, keepFinishedInterval: 60_000 }, group)
    let ran = 0
    consumer.createJob({ jobName: 'mover', lane: 'new' }, async () => { ran++ })

    expect(await consumer.popAndExecute()).toBe(true)
    expect(ran).toBe(1)
    expect((await consumer.get('mover#m1'))?.status).toBe('finished')
    expect(await consumer.popAndExecute()).toBe(false)
    expect(await h.redis.lrange(`redisjm:${group}:lane:old:queue`, 0, -1)).toEqual(['neighbor#n1', 'neighbor#n2'])
    expect((await consumer.get('neighbor#n1'))?.requeueCount).toBeUndefined()
  })

  it('HPEXPIRE: a finished record carries a field TTL (cleared by a re-enqueue) and expires without maintenance', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0, keepFinishedInterval: 300 }, group)
    const job = m.createJob({ jobName: 'ttl' }, async () => {})
    await m.queue(job, 'r1', null)
    await m.popAndExecute()
    const ttl = await h.redis.call('HPTTL', `redisjm:${group}:log`, 'FIELDS', 1, 'ttl#r1') as number[]
    expect(ttl[0]).toBeGreaterThan(0)
    // Re-enqueue the same runId: the HSET in the enqueue script clears the field TTL.
    await m.queue(job, 'r1', null)
    expect(await h.redis.call('HPTTL', `redisjm:${group}:log`, 'FIELDS', 1, 'ttl#r1')).toEqual([-1])
    await m.popAndExecute()
    await sleep(400)
    expect(await m.get('ttl#r1')).toBeUndefined() // gone with maintenance disabled
  })

  it('concurrency > 1: runs finishing between maintenance’s read and write leave no ghost records', async () => {
    // WHY: maintenance used to write back its scan-time copy of a record. A run that finished (and,
    // with keepFinishedInterval 0, deleted its record) in between was re-created as a ghost `stale`.
    const group = newGroup()
    // Runner: heartbeat far in the future → its running records look overdue to the maintainer.
    const runner = newManager({ heartbeatInterval: 60_000, maintenanceInterval: 0, concurrency: 2, keepFinishedInterval: 0 }, group)
    const gates: Array<() => void> = []
    const job = runner.createJob({ jobName: 'race' }, () => new Promise<void>((resolve) => { gates.push(resolve) }))
    await runner.queue(job, 'a', null)
    await runner.queue(job, 'b', null)
    runner.start(10)
    await until(() => gates.length === 2)
    await sleep(150)

    // Maintainer on its own connection. The race is forced into the narrowest gap: AFTER the pass read
    // a record (and decided to stale it) but BEFORE its write reaches Redis, both runs finish and their
    // records are deleted. A read-then-HSET write would re-create them as ghost `stale` records; the
    // compare-and-set transition script sees the record is gone and writes nothing.
    const client = newClient()
    const realEvalsha = client.evalsha.bind(client) as (...args: any[]) => Promise<unknown>
    let raced = 0
    ;(client as any).evalsha = async (...args: any[]) => {
      if (args.some((a) => String(a).includes('"status":"stale"'))) {
        raced++
        if (gates.length) {
          gates.splice(0).forEach((release) => release())
          await until(async () => (await h.redis.hlen(`redisjm:${group}:log`)) === 0)
        }
      }
      return realEvalsha(...args)
    }
    const maintainer = newManager({ heartbeatInterval: 50, roundsToStale: 2, maintenanceInterval: 0 }, group, client)
    const result = await maintainer.performMaintenance()

    expect(raced).toBeGreaterThanOrEqual(1) // a stale write was attempted after the deletions
    expect(result.staleCount).toBe(0)
    expect(await h.redis.hlen(`redisjm:${group}:log`)).toBe(0) // no ghost records
    expect(await h.redis.scard(`redisjm:${group}:locks`)).toBe(0)
  })

  it('maintenance on a 50k-record log stays bounded: ≤ maxRecordsPerPass examined, no per-record LPOS, cursor rotates', async () => {
    const group = newGroup()
    const logKey = `redisjm:${group}:log`
    const queueKey = `redisjm:${group}:queue`
    const now = Date.now()
    // Seed 50k new-format queued records (+ list entries + locks) with pipelines.
    for (let start = 0; start < 50_000; start += 5000) {
      const p = h.redis.pipeline()
      for (let i = start; i < start + 5000; i++) {
        const jobId = `bulk#${i}`
        p.hset(logKey, jobId, JSON.stringify({ jobId, jobName: 'bulk', runId: String(i), inputs: null, targetGroup: group, status: 'queued', progress: 0, enqueuedAt: now }))
        p.rpush(queueKey, jobId)
        p.sadd(`redisjm:${group}:locks`, jobId)
      }
      await p.exec()
    }
    const client = newClient()
    let examined = 0
    let lposCalls = 0
    const realHscan = client.hscan.bind(client) as (...args: any[]) => Promise<[string, string[]]>
    ;(client as any).hscan = async (...args: any[]) => {
      const reply = await realHscan(...args)
      if (String(args[0]) === logKey) examined += reply[1].length / 2
      return reply
    }
    const realPipeline = client.pipeline.bind(client)
    ;(client as any).pipeline = () => {
      const p = realPipeline()
      const realLpos = p.lpos.bind(p) as (...args: any[]) => unknown
      ;(p as any).lpos = (...args: any[]) => {
        lposCalls++
        return realLpos(...args)
      }
      return p
    }
    const m = newManager({ maintenanceInterval: 0, maxRecordsPerPass: 1000 }, group, client)

    const t0 = Date.now()
    await m.performMaintenance()
    const firstPassMs = Date.now() - t0
    const cursorAfterFirst = await h.redis.get(`redisjm:${group}:maintenance-cursor`)
    const examinedFirst = examined
    await m.performMaintenance()
    const cursorAfterSecond = await h.redis.get(`redisjm:${group}:maintenance-cursor`)

    console.log(`[integration] 50k log: first pass examined ${examinedFirst} records in ${firstPassMs}ms`)
    expect(examinedFirst).toBeGreaterThanOrEqual(1000)
    expect(examinedFirst).toBeLessThan(1600) // bounded (HSCAN COUNT is a hint, slight overshoot)
    expect(lposCalls).toBe(0)
    expect(firstPassMs).toBeLessThan(2000)
    expect(cursorAfterFirst).not.toBe('0')
    expect(cursorAfterSecond).not.toBe(cursorAfterFirst)
    expect(await h.redis.hlen(logKey)).toBe(50_000) // nothing healthy was touched
  }, 30_000)

  it('wake() makes an idle poller pick up new work immediately', async () => {
    const m = newManager({ maintenanceInterval: 0 })
    let ranAt = 0
    const job = m.createJob({ jobName: 'doorbell' }, async () => { ranAt = Date.now() })
    m.start(60_000)
    await sleep(50) // first poll found nothing → idle for a minute
    await job.queue('r1', null)
    const rungAt = Date.now()
    m.wake()
    await until(() => ranAt > 0, 1000)
    expect(ranAt - rungAt).toBeLessThan(500)
  })

  it('laneConcurrency: a busy heavy lane does not starve a light one', async () => {
    const m = newManager({ maintenanceInterval: 0, concurrency: 4, laneConcurrency: { heavy: 1 } })
    const started: string[] = []
    const release: Array<() => void> = []
    const heavy = m.createJob({ jobName: 'heavy', lane: 'heavy' }, (input: string, _ctx: JobContext) => new Promise<void>((r) => {
      started.push(input)
      release.push(r)
    }))
    const light = m.createJob({ jobName: 'light', lane: 'light' }, async (input: string) => { started.push(input) })
    await m.enqueueMany(heavy, [{ runId: '1', inputs: 'h1' }, { runId: '2', inputs: 'h2' }, { runId: '3', inputs: 'h3' }])
    await m.enqueueMany(light, [{ runId: '1', inputs: 'l1' }, { runId: '2', inputs: 'l2' }])
    m.start(10)
    await until(() => started.includes('l1') && started.includes('l2'))
    expect(started.filter((s) => s.startsWith('h'))).toHaveLength(1)
    release.shift()!()
    await until(() => started.filter((s) => s.startsWith('h')).length === 2)
    release.splice(0).forEach((r) => r())
    await until(() => started.filter((s) => s.startsWith('h')).length === 3)
    release.splice(0).forEach((r) => r())
  })
})

describe.skipIf(!REDIS_URL || !REDIS_SERVER_BIN)('phase-2 integration (dedicated Redis)', () => {
  const h = useDedicatedRedis('ith2d')
  const { fillToOom } = h
  const newManager = (options: RedisJMOptions, logger: RedisJMOptions['logger'] = false) => h.newManager({ ...options, logger })
  afterEach(async () => {
    // This server is dedicated to the suite: restoring its policy is fine (never done on the shared one).
    await h.redis.config('SET', 'maxmemory-policy', 'noeviction')
  })

  it('scripts survive a flushed script cache (NOSCRIPT → EVAL fallback)', async () => {
    const m = newManager({ maintenanceInterval: 0 })
    let ran = 0
    const job = m.createJob({ jobName: 'ns' }, async () => { ran++ })
    await job.queue('r1', null)
    await h.redis.script('FLUSH')
    await job.queue('r2', null)
    await h.redis.script('FLUSH')
    expect(await m.popAndExecute()).toBe(true)
    await h.redis.script('FLUSH')
    expect(await m.popAndExecute()).toBe(true)
    expect(ran).toBe(2)
  })

  it('enqueueMany under OOM writes nothing (one script, refused up front)', async () => {
    const m = newManager({ maintenanceInterval: 0 })
    const job = new Job({ jobName: 'batch' }, async () => {})
    await fillToOom()
    await expect(m.enqueueMany(job, [{ runId: 'a', inputs: 1 }, { runId: 'b', inputs: 2 }]))
      .rejects.toMatchObject({ name: 'RedisJMEnqueueError', reason: 'oom' })
    const group = m.getTargetGroup()
    expect(await h.redis.scard(`redisjm:${group}:locks`)).toBe(0)
    expect(await h.redis.hlen(`redisjm:${group}:log`)).toBe(0)
    expect(await h.redis.llen(`redisjm:${group}:queue`)).toBe(0)
  })

  it('health() reads INFO memory; memoryPressure fires from the maintenance timer when memory crosses the ratio', async () => {
    const m = newManager({ maintenanceInterval: 50, memoryWarnRatio: 0.9 })
    const before = await m.health()
    expect(before.maxMemory).toBe(2 * 1024 * 1024)
    expect(before.maxmemoryPolicy).toBe('noeviction')
    expect(before.usedRatio).toBeGreaterThan(0)
    expect(before.usedRatio!).toBeLessThan(0.9)
    const pressure: RedisJMHealth[] = []
    m.hook('memoryPressure', (snapshot) => { pressure.push(snapshot) })
    m.start(1000)
    await sleep(120)
    expect(pressure).toHaveLength(0)
    await fillToOom()
    await until(() => pressure.length === 1, 2000)
    expect(pressure[0].usedRatio!).toBeGreaterThanOrEqual(0.9)
    await sleep(200)
    expect(pressure).toHaveLength(1) // once per crossing
  })

  it('start() warns when the eviction policy can evict queue keys (read from INFO, not CONFIG)', async () => {
    await h.redis.config('SET', 'maxmemory-policy', 'allkeys-lru')
    const messages: string[] = []
    const m = newManager({ maintenanceInterval: 0 }, (msg) => { messages.push(msg) })
    m.start(1000)
    await until(() => messages.some((msg) => /allkeys-lru/.test(msg)), 2000)
  })
})
