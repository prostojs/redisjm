/**
 * Opt-in integration suite for the 0.2.0 hardening, against REAL Redis servers.
 *
 * Two servers are involved:
 * - the shared server at `REDIS_URL` (timeouts, maintenance-under-load, claim-failure recovery,
 *   maxRunMs). Only this suite's own `redisjm:ith-*` keys are ever touched — never FLUSHALL / FLUSHDB /
 *   CONFIG SET on it: it is shared with other apps.
 * - a DEDICATED, throwaway `redis-server` spawned by this file (`--maxmemory 2mb --maxmemory-policy
 *   noeviction`, no persistence) for the out-of-memory scenarios, killed in `afterAll`.
 *
 * Skipped when `REDIS_URL` is unset; the OOM block is additionally skipped when the `redis-server`
 * binary is not available.
 *
 * HOW TO RUN: `REDIS_URL=redis://localhost:6379 pnpm test`
 */
import { describe, expect, it } from 'vitest'
import { RedisJMEnqueueError } from '../errors'
import type { RedisJM } from '../redisjm'
import type { JobContext, JobLogRecord, StartFailedEventPayload } from '../types'
import { REDIS_SERVER_BIN, REDIS_URL, sleep, until, useDedicatedRedis, useSharedRedis } from './integration-helpers'

describe.skipIf(!REDIS_URL)('hardening integration (shared Redis)', () => {
  const h = useSharedRedis('ith')
  const { newClient, newGroup, newManager } = h
  const get = (m: RedisJM, jobId: string): Promise<JobLogRecord | undefined> => m.get(jobId)

  it('hung handler + timeoutMs at concurrency 1: the slot frees, other jobs keep running, record ends error', async () => {
    const manager = newManager({ heartbeatInterval: 100, maintenanceInterval: 0, concurrency: 1, logger: false })
    const ran: string[] = []
    const job = manager.createJob({ jobName: 'hang', timeoutMs: 200 }, async (input: string, ctx) => {
      ran.push(input)
      if (input === 'hang') await new Promise<void>(() => {}) // ignores ctx.signal on purpose
      void ctx
    })
    let timeouts = 0
    manager.hook('timeout', () => { timeouts++ })

    expect(await job.queue('a', 'hang')).toBe(true)
    expect(await job.queue('b', 'ok')).toBe(true)
    expect(await job.queue('c', 'ok')).toBe(true)
    manager.start(20)

    await until(async () => (await get(manager, job.getJobId('c')))?.status === 'finished', 3000)
    expect(ran).toEqual(['hang', 'ok', 'ok'])
    const hung = await get(manager, job.getJobId('a'))
    expect(hung?.status).toBe('error')
    expect(hung?.error).toMatch(/timed out after 200ms/)
    expect(timeouts).toBe(1)
    expect(await manager.isLocked(job.getJobId('a'))).toBe(false)
  })

  it('a timed-out attempt with attempts > 1 is delayed for retry (lock held)', async () => {
    const manager = newManager({ heartbeatInterval: 100, maintenanceInterval: 0, logger: false })
    let attempts = 0
    const job = manager.createJob({ jobName: 'retry-timeout', timeoutMs: 100, attempts: 2, backoff: 60_000 }, async () => {
      attempts++
      await new Promise<void>(() => {})
    })
    const events: string[] = []
    manager.hook('timeout', () => { events.push('timeout') })
    manager.hook('retry', () => { events.push('retry') })
    manager.hook('error', () => { events.push('error') })

    expect(await job.queue('r1', {})).toBe(true)
    manager.start(20)
    await until(async () => (await get(manager, job.getJobId('r1')))?.status === 'delayed')
    const record = await get(manager, job.getJobId('r1'))
    expect(record?.error).toMatch(/timed out/)
    expect(record?.attempt).toBe(1)
    expect(attempts).toBe(1)
    expect(events).toEqual(['timeout', 'retry'])
    expect(await manager.isLocked(job.getJobId('r1'))).toBe(true)
  })

  it('maintenance keeps running while every concurrency slot is held by a hung handler', async () => {
    const group = newGroup()
    // staleThreshold = 100 * 2 = 200ms; maintenance every 150ms; no timeout → the slot stays hung.
    const manager = newManager({ heartbeatInterval: 100, roundsToStale: 2, maintenanceInterval: 150, concurrency: 1, logger: false }, group)
    const job = manager.createJob({ jobName: 'hog' }, () => new Promise<void>(() => {}))
    expect(await job.queue('h', {})).toBe(true)
    manager.start(20)
    await until(async () => (await get(manager, job.getJobId('h')))?.status === 'running')

    // A peer crashed mid-run: its record is `running` with a long-dead heartbeat, lock still held.
    const crashedId = 'crashed#r1'
    await h.redis.sadd(`redisjm:${group}:locks`, crashedId)
    await h.redis.hset(`redisjm:${group}:log`, crashedId, JSON.stringify({
      jobId: crashedId, jobName: 'crashed', runId: 'r1', inputs: null, targetGroup: group,
      status: 'running', progress: 0, startedAt: Date.now() - 10_000, heartbeat: Date.now() - 10_000,
    }))

    await until(async () => (await get(manager, crashedId))?.status === 'stale', 2000)
    expect(await manager.isLocked(crashedId)).toBe(false)
    // The hung run is still alive and owned (its heartbeat keeps it fresh).
    expect((await get(manager, job.getJobId('h')))?.status).toBe('running')
    // Nothing was enqueued for maintenance.
    expect(await h.redis.llen(`redisjm:${group}:lane:__maintenance:queue`)).toBe(0)
  })

  it('maxRunMs stales a hung-but-heartbeating run, aborts it, and it cannot resurrect itself', async () => {
    const manager = newManager({ heartbeatInterval: 50, roundsToStale: 2, maintenanceInterval: 100, maxRunMs: 300, logger: false })
    let abortReason: unknown
    const job = manager.createJob({ jobName: 'long' }, async (_i: unknown, ctx: JobContext) => {
      await new Promise<void>((resolve) => ctx.signal.addEventListener('abort', () => resolve(), { once: true }))
      abortReason = ctx.signal.reason
      await sleep(250) // several more heartbeats fire while we linger after the abort
    })
    const jobId = job.getJobId('r1')
    expect(await job.queue('r1', {})).toBe(true)
    manager.start(20)

    await until(async () => (await get(manager, jobId))?.status === 'stale', 2000)
    const staled = await get(manager, jobId)
    expect(staled?.staleReason).toBe('maxRunMs')
    expect(await manager.isLocked(jobId)).toBe(false)
    await until(() => abortReason !== undefined, 1000)
    expect(String(abortReason)).toMatch(/lost ownership/)
    await sleep(120) // more heartbeats: still not resurrected
    expect((await get(manager, jobId))?.status).not.toBe('running')
    expect(await manager.isLocked(jobId)).toBe(false)
  })

  it('claim write fails after the pop → the run id is requeued at the head (lock held) and runs afterwards', async () => {
    const client = newClient()
    // Fail exactly one write: the claim (the first record transition that flips a record to running).
    const realEvalsha = client.evalsha.bind(client) as (...args: any[]) => Promise<unknown>
    let failedClaim = false
    ;(client as any).evalsha = (...args: any[]) => {
      if (!failedClaim && args.some((a) => String(a).includes('"status":"running"'))) {
        failedClaim = true
        return Promise.reject(new Error('Connection is closed.'))
      }
      return realEvalsha(...args)
    }
    const group = newGroup()
    const manager = newManager({ heartbeatInterval: 100, maintenanceInterval: 0, logger: false }, group, client)
    let ran = 0
    const job = manager.createJob({ jobName: 'claim' }, async () => { ran++ })
    const startFailed: StartFailedEventPayload[] = []
    manager.hook('startFailed', (p) => { startFailed.push(p) })
    const jobId = job.getJobId('r1')
    expect(await job.queue('r1', {})).toBe(true)
    expect(await job.queue('r2', {})).toBe(true)

    expect(await manager.popAndExecute()).toBe(true)
    expect(failedClaim).toBe(true)
    expect(ran).toBe(0)
    expect(await h.redis.lrange(`redisjm:${group}:queue`, 0, -1)).toEqual([jobId, job.getJobId('r2')])
    expect((await get(manager, jobId))?.status).toBe('queued')
    expect(await manager.isLocked(jobId)).toBe(true)
    expect(startFailed).toEqual([expect.objectContaining({ jobId, action: 'requeued', reason: 'connection' })])

    manager.start(20)
    await until(async () => (await get(manager, job.getJobId('r2')))?.status === 'finished', 3000)
    expect((await get(manager, jobId))?.status).toBe('finished')
    expect(ran).toBe(2)
    expect(await h.redis.scard(`redisjm:${group}:locks`)).toBe(0)
  })
})

describe.skipIf(!REDIS_URL || !REDIS_SERVER_BIN)('hardening integration (dedicated maxmemory Redis)', () => {
  const h = useDedicatedRedis('oom')
  const { fillToOom, freeMemory, newClient, newGroup, newManager } = h

  it('queue() under OOM throws RedisJMEnqueueError{reason:"oom"} and leaves no lock behind', async () => {
    const group = newGroup()
    const manager = newManager({ maintenanceInterval: 0 }, group)
    const job = manager.createJob({ jobName: 'p' }, async () => {})
    let failedEvent: unknown
    manager.hook('enqueueFailed', (p) => { failedEvent = p })
    await fillToOom()

    const err = await job.queue('r1', { a: 1 }).catch((e) => e)
    expect(err).toBeInstanceOf(RedisJMEnqueueError)
    expect(err.reason).toBe('oom')
    expect(err.cause.message).toMatch(/^OOM/)
    expect(failedEvent).toMatchObject({ jobId: 'p#r1', reason: 'oom' })
    expect(await manager.isLocked('p#r1')).toBe(false)
    expect(await manager.get('p#r1')).toBeUndefined()
    // enqueue() reports the same way (throws, not a status).
    await expect(job.enqueue('r2', {})).rejects.toMatchObject({ name: 'RedisJMEnqueueError', reason: 'oom' })
  })

  it('an emergency maintenance pass still deletes expired terminal records under OOM', async () => {
    const group = newGroup()
    const manager = newManager({ maintenanceInterval: 1000, keepFinishedInterval: 1000 }, group)
    const logKey = `redisjm:${group}:log`
    const old = Date.now() - 60_000
    for (let i = 0; i < 50; i++) {
      await h.redis.hset(logKey, `done#${i}`, JSON.stringify({
        jobId: `done#${i}`, jobName: 'done', runId: String(i), inputs: { pad: 'y'.repeat(200) }, targetGroup: group,
        status: i % 2 ? 'finished' : 'error', progress: 1, finishedAt: old,
      }))
    }
    await h.redis.hset(logKey, 'garbage#1', '{nope')
    await h.redis.sadd(`redisjm:${group}:locks`, 'garbage#1')
    await h.redis.hset(logKey, 'fresh#1', JSON.stringify({
      jobId: 'fresh#1', jobName: 'fresh', runId: '1', inputs: null, targetGroup: group, status: 'finished', progress: 1, finishedAt: Date.now(),
    }))
    await fillToOom()

    const result = await manager.runMaintenance()
    expect(result).toEqual({ staleCount: 0, cleanedCount: 51, requeuedCount: 0, mode: 'emergency' })
    expect(await h.redis.hlen(logKey)).toBe(1) // only the fresh (retained) record is left
    expect(await h.redis.sismember(`redisjm:${group}:locks`, 'garbage#1')).toBe(0)
    expect(await h.redis.exists(`redisjm:${group}:maintenance-lock`)).toBe(0) // no lock written under OOM
  })

  it('a full Redis pops nothing and loses nothing; processing resumes once memory frees', async () => {
    const group = newGroup()
    const manager = newManager({ maintenanceInterval: 0, heartbeatInterval: 100 }, group)
    let ran = 0
    const job = manager.createJob({ jobName: 'p' }, async () => { ran++ })
    expect(await job.queue('r1', {})).toBe(true)
    expect(await job.queue('r2', {})).toBe(true)
    await fillToOom()

    // The pop script is refused BEFORE its LPOP: nothing leaves the list, nothing is parked.
    await expect(manager.popAndExecute()).rejects.toMatchObject({ message: expect.stringMatching(/^OOM/) })
    expect(await h.redis.llen(`redisjm:${group}:queue`)).toBe(2)
    expect(await h.redis.zcard(`redisjm:${group}:claiming`)).toBe(0)
    expect((await manager.get('p#r1'))?.status).toBe('queued')

    await freeMemory()
    expect(await manager.popAndExecute()).toBe(true)
    expect(await manager.popAndExecute()).toBe(true)
    expect(ran).toBe(2)
    expect(await h.redis.scard(`redisjm:${group}:locks`)).toBe(0)
  })

  it('claim refused under OOM right after a pop → deferred (parked in claiming), then requeued by maintenance — never lost', async () => {
    const group = newGroup()
    const client = newClient()
    // Redis fills up between the pop and the claim: fill right before the first claim transition.
    const realEvalsha = client.evalsha.bind(client) as (...args: any[]) => Promise<unknown>
    let filled = false
    ;(client as any).evalsha = async (...args: any[]) => {
      if (!filled && args.some((a) => String(a).includes('"status":"running"'))) {
        filled = true
        await fillToOom()
      }
      return realEvalsha(...args)
    }
    const manager = newManager({ maintenanceInterval: 0, heartbeatInterval: 50, roundsToStale: 2 }, group, client)
    let ran = 0
    const job = manager.createJob({ jobName: 'c' }, async () => { ran++ })
    const startFailed: StartFailedEventPayload[] = []
    manager.hook('startFailed', (p) => { startFailed.push(p) })
    expect(await job.queue('r1', {})).toBe(true)

    expect(await manager.popAndExecute()).toBe(true)
    expect(ran).toBe(0)
    expect(startFailed).toEqual([expect.objectContaining({ jobId: 'c#r1', reason: 'oom', action: 'deferred' })])
    // Not lost: record queued, lock held, parked in claiming.
    expect((await manager.get('c#r1'))?.status).toBe('queued')
    expect(await manager.isLocked('c#r1')).toBe(true)
    expect(await h.redis.zscore(`redisjm:${group}:claiming`, 'c#r1')).not.toBeNull()

    await freeMemory()
    await sleep(150) // past the stale threshold (50 * 2)
    const result = await manager.performMaintenance()
    expect(result.requeuedCount).toBe(1)
    await sleep(100) // past the pop back-off
    expect(await manager.popAndExecute()).toBe(true)
    expect(ran).toBe(1)
    expect((await manager.get('c#r1'))?.status).toBe('finished')
  })

  it('after memory is freed, processing resumes with no stuck locks', async () => {
    const group = newGroup()
    const manager = newManager({ maintenanceInterval: 200, heartbeatInterval: 100, keepFinishedInterval: 60_000 }, group)
    const done: string[] = []
    const job = manager.createJob({ jobName: 'w' }, async (input: string) => { done.push(input) })
    expect(await job.queue('before', 'before')).toBe(true) // accepted before Redis filled up
    await fillToOom()
    await expect(job.queue('during', 'during')).rejects.toMatchObject({ reason: 'oom' })
    // Maintenance under OOM degrades to the emergency pass instead of failing.
    expect((await manager.runMaintenance())?.mode).toBe('emergency')

    await freeMemory()
    expect(await job.queue('during', 'during')).toBe(true) // the refused run id was never left locked
    manager.start(20)
    await until(() => done.length === 2, 3000)
    expect(done.sort()).toEqual(['before', 'during'])
    await until(async () => (await h.redis.scard(`redisjm:${group}:locks`)) === 0, 2000)
    const records = await manager.list()
    expect(records.map((r) => r.status)).toEqual(['finished', 'finished'])
  })
})
