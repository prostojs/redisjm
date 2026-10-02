/**
 * Regression tests from an independent correctness review of 0.2.0, against REAL Redis (the bugs are
 * races and Lua-side semantics the mock cannot prove). Shared server: only `redisjm:itr-*` keys.
 *
 * HOW TO RUN: `REDIS_URL=redis://localhost:6379 pnpm test`
 */
import type Redis from 'ioredis'
import { describe, expect, it } from 'vitest'
import { Job } from '../job'
import { MAINTENANCE_JOB_NAME, MAINTENANCE_LANE } from '../maintenance'
import { RedisJM } from '../redisjm'
import { REDIS_URL, sleep, until, useSharedRedis } from './integration-helpers'

describe.skipIf(!REDIS_URL)('review regressions (shared Redis)', () => {
  const h = useSharedRedis('itr', { logger: false })
  const { newClient, newGroup, newManager } = h

  it('many concurrent ctx.setProgress calls from one handler never fail the run (no CAS exhaustion)', async () => {
    const m = newManager({ maintenanceInterval: 0, heartbeatInterval: 50 })
    const job = m.createJob({ jobName: 'fanout' }, async (_inputs, ctx) => {
      await Promise.all(Array.from({ length: 40 }, (_, i) => ctx.setProgress(i / 40)))
    })
    await job.queue('r1', null)
    await m.popAndExecute()
    const record = await m.get('fanout#r1')
    expect(record?.status).toBe('finished')
    expect(record?.error).toBeUndefined()
  })

  it('an instance without the handler never flips a RUNNING record back to queued (no double execution)', async () => {
    const group = newGroup()
    const worker = newManager({ maintenanceInterval: 0, heartbeatInterval: 50 }, group)
    let runs = 0
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const job = worker.createJob({ jobName: 'once' }, async () => {
      runs++
      await gate
    })
    await job.queue('r1', null)
    const first = worker.popAndExecute()
    await until(async () => (await worker.get('once#r1'))?.status === 'running')
    // A stray duplicate entry (e.g. from a claiming requeue racing a slow claim) lands on the lane…
    await h.redis.rpush(`redisjm:${group}:queue`, 'once#r1')
    // …and is popped by an instance that does not register `once` (rolling deploy / other worker).
    const stranger = newManager({ maintenanceInterval: 0, heartbeatInterval: 50 }, group)
    stranger.createJob({ jobName: 'other' }, async () => {})
    await stranger.popAndExecute()
    expect((await worker.get('once#r1'))?.status).toBe('running')
    // Another worker with the handler must not be able to claim it a second time.
    const second = newManager({ maintenanceInterval: 0, heartbeatInterval: 50 }, group)
    second.createJob({ jobName: 'once' }, async () => { runs++ })
    while (await second.popAndExecute()) { /* drain */ }
    release()
    await first
    expect(runs).toBe(1)
    expect((await worker.get('once#r1'))?.status).toBe('finished')
  })

  /**
   * Re-enqueues `<jobName>#r1` from another manager the instant a script call on `client` leaves its lock
   * released — a producer racing the finish. The finish retires the record and releases the lock in ONE
   * transition, so the re-enqueue can only land after it; before 0.2's atomic retire it landed between
   * the terminal write and a separate SREM / HPEXPIRE / HDEL, which then hit the fresh record.
   */
  const reenqueueOnLockRelease = (client: Redis, group: string, jobName: string, onReenqueue: () => void) => {
    const jobId = `${jobName}#r1`
    let done = false
    for (const cmd of ['evalsha', 'eval'] as const) {
      const real = (client as any)[cmd].bind(client) as (...args: any[]) => Promise<unknown>
      ;(client as any)[cmd] = async (...args: any[]) => {
        const result = await real(...args)
        if (!done && args.includes(jobId) && (await h.redis.sismember(`redisjm:${group}:locks`, jobId)) === 0) {
          done = true
          onReenqueue()
          await new RedisJM(h.redis, group, { logger: false }).queue(new Job({ jobName }, async () => {}), 'r1', null)
        }
        return result
      }
    }
  }

  it('a runId re-enqueued right after its previous run finished keeps its record (no inherited field TTL)', async () => {
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0, keepFinishedInterval: 200 }, group, client)
    const job = m.createJob({ jobName: 'again' }, async () => {})
    let reenqueued = false
    reenqueueOnLockRelease(client, group, 'again', () => { reenqueued = true })
    await job.queue('r1', null)
    await m.popAndExecute()
    expect(reenqueued).toBe(true)
    await sleep(350) // past keepFinishedInterval: a TTL inherited from the finished run would have fired
    const record = await m.get('again#r1')
    expect(record?.status).toBe('queued')
    expect(await h.redis.call('HPTTL', `redisjm:${group}:log`, 'FIELDS', 1, 'again#r1')).toEqual([-1])
  })

  it('keepFinishedInterval 0: a runId re-enqueued right after the finish keeps its fresh record', async () => {
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0, keepFinishedInterval: 0 }, group, client)
    const job = m.createJob({ jobName: 'again0' }, async () => {})
    let reenqueued = false
    reenqueueOnLockRelease(client, group, 'again0', () => { reenqueued = true })
    await job.queue('r1', null)
    await m.popAndExecute()
    const record = await m.get('again0#r1')
    expect(record?.status).toBe('queued')
    expect(await m.isLocked('again0#r1')).toBe(true)
  })

  it('overlapping maintenance passes requeue an overdue claim exactly once', async () => {
    const group = newGroup()
    const a = newManager({ maintenanceInterval: 0, heartbeatInterval: 20, roundsToStale: 1 }, group)
    const b = newManager({ maintenanceInterval: 0, heartbeatInterval: 20, roundsToStale: 1 }, group)
    const job = new Job({ jobName: 'crashy' }, async () => {})
    await a.queue(job, 'r1', null)
    // Simulate a popper that died right after its pop: the id sits in `claiming`, off the lane.
    await h.redis.lpop(`redisjm:${group}:queue`)
    await h.redis.zadd(`redisjm:${group}:claiming`, Date.now() - 1000, 'crashy#r1')
    await sleep(30)
    await Promise.all([a.performMaintenance(), b.performMaintenance(), a.performMaintenance()])
    expect(await h.redis.lrange(`redisjm:${group}:queue`, 0, -1)).toEqual(['crashy#r1'])
  })

  it('a pre-claim job-level start hook that throws on a RETRY attempt fails the run instead of looping', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0, heartbeatInterval: 20, roundsToStale: 1 }, group)
    let attempt = 0
    const job = new Job({ jobName: 'hooky', attempts: 3 }, async () => {
      throw new Error('handler fails')
    })
    // Registered BEFORE the manager's hooks → runs before the claim.
    job.hook('start', () => {
      attempt++
      if (attempt >= 2) throw new Error('hook refuses')
    })
    m.registerJob(job)
    await m.queue(job, 'r1', null)
    await m.popAndExecute() // attempt 1: handler throws → retry scheduled
    expect((await m.get('hooky#r1'))?.status).toBe('delayed')
    await sleep(1100) // promotion is rate-limited to once per second
    await m.popAndExecute() // attempt 2: pre-claim hook throws
    const record = await m.get('hooky#r1')
    expect(record?.status).toBe('error')
    expect(await m.isLocked('hooky#r1')).toBe(false)
    expect(await h.redis.zcard(`redisjm:${group}:claiming`)).toBe(0)
  })

  it('mixed deploy: a run a 0.1.x consumer popped and died on before claiming is reclaimed, not locked forever', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0, heartbeatInterval: 20, roundsToStale: 1 }, group)
    const job = m.createJob({ jobName: 'v1pop' }, async () => {})
    await m.queue(job, 'r1', null)
    // A 0.1.x consumer pops with LMPOP (no `claiming` entry) and dies before its claim write.
    expect(await h.redis.lpop(`redisjm:${group}:queue`)).toBe('v1pop#r1')
    // 0.1.x instances announce themselves by enqueueing the maintenance job; a 0.2 instance consumes it.
    await m.queue(new Job({ jobName: MAINTENANCE_JOB_NAME, lane: MAINTENANCE_LANE }, async () => {}), 'legacy-tick', null)
    await m.popAndExecute()
    await sleep(40) // older than the stale threshold
    await m.performMaintenance() // pass 1: suspect
    await sleep(40)
    await m.performMaintenance() // pass 2: reclaim
    const record = await m.get('v1pop#r1')
    expect(record?.status).toBe('stale')
    expect(record?.staleReason).toBe('orphaned')
    expect(await m.isLocked('v1pop#r1')).toBe(false)
    expect(await m.queue(job, 'r1', null)).toBe(true) // the runId is usable again
  })

  it('without a legacy instance, new-format queued records are not LPOS-checked (cost stays bounded)', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0, heartbeatInterval: 20, roundsToStale: 1 }, group)
    const job = m.createJob({ jobName: 'fast' }, async () => {})
    await m.queue(job, 'r1', null)
    await sleep(40)
    const client = newClient()
    const lpos: unknown[] = []
    const m2 = newManager({ maintenanceInterval: 0, heartbeatInterval: 20, roundsToStale: 1 }, group, client)
    const realPipeline = client.pipeline.bind(client)
    ;(client as any).pipeline = (...args: any[]) => {
      const p = realPipeline(...args)
      const realLpos = p.lpos.bind(p) as (...a: any[]) => unknown
      ;(p as any).lpos = (...a: any[]) => { lpos.push(a); return realLpos(...a) }
      return p
    }
    await m2.performMaintenance()
    expect(lpos).toEqual([])
    expect((await m.get('fast#r1'))?.status).toBe('queued')
  })

  it('maintenance never deletes a record re-enqueued between its scan and its delete', async () => {
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0, keepFinishedInterval: 100 }, group, client)
    const job = new Job({ jobName: 'reuse' }, async () => {})
    // An expired terminal record with no field TTL (written by 0.1.x, or on Redis < 7.4).
    await h.redis.hset(`redisjm:${group}:log`, 'reuse#r1', JSON.stringify({
      jobId: 'reuse#r1', jobName: 'reuse', runId: 'r1', inputs: null, targetGroup: group,
      status: 'finished', progress: 1, finishedAt: Date.now() - 10_000,
    }))
    // A producer re-enqueues the (unlocked) runId while the pass is between its scan and its deletes.
    const realHscan = client.hscan.bind(client) as (...args: any[]) => Promise<unknown>
    let reenqueued = false
    ;(client as any).hscan = async (...args: any[]) => {
      const reply = await realHscan(...args)
      if (!reenqueued && String(args[0]).endsWith(':log')) {
        reenqueued = true
        await new RedisJM(h.redis, group, { logger: false }).queue(job, 'r1', null)
      }
      return reply
    }
    const result = await m.performMaintenance()
    expect(reenqueued).toBe(true)
    expect(result.cleanedCount).toBe(0)
    expect((await m.get('reuse#r1'))?.status).toBe('queued')
    expect(await m.isLocked('reuse#r1')).toBe(true)
  })

  it('stop() then start() before the old poll settled does not leave two poll loops running', async () => {
    const group = newGroup()
    const client = newClient()
    // Hold the FIRST pop in flight so stop() + start() happen while it is mid-pop.
    const realEvalsha = client.evalsha.bind(client) as (...args: any[]) => Promise<unknown>
    let delayed = false
    ;(client as any).evalsha = async (...args: any[]) => {
      if (!delayed && args.some((a) => String(a).endsWith(':claiming'))) {
        delayed = true
        await sleep(50)
      }
      return realEvalsha(...args)
    }
    const m = newManager({ maintenanceInterval: 0, concurrency: 1 }, group, client)
    let running = 0
    let maxAfterRestartSettled = 0
    let finished = 0
    const job = m.createJob({ jobName: 'slow' }, async () => {
      running++
      if (finished >= 2) maxAfterRestartSettled = Math.max(maxAfterRestartSettled, running)
      await sleep(60)
      running--
      finished++
    })
    for (let i = 0; i < 8; i++) await job.queue(`r${i}`, null)
    m.start(10)
    void m.stop()
    m.start(10)
    await until(() => finished === 8, 5000)
    // The in-flight pop of the old loop may overlap once; after that, concurrency 1 must hold.
    expect(maxAfterRestartSettled).toBe(1)
  })

  it('the job-prune script (real Lua) drops drifted lock-set members, empty lanes, and idle jobs', async () => {
    // WHY: until now only the mock's emulation of PRUNE_JOB_SCRIPT was exercised.
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0 }, group)
    const job = m.createJob({ jobName: 'p', lane: 'a' }, async () => {})
    await m.queue(job, 'r1', null)
    await h.redis.sadd(`redisjm:${group}:jobs:p:locks`, 'p#ghost')
    await h.redis.sadd(`redisjm:${group}:jobs:p:lanes`, `redisjm:${group}:lane:gone:queue`) // an empty (never used) lane
    await m.performMaintenance()
    expect(await h.redis.smembers(`redisjm:${group}:jobs:p:locks`)).toEqual(['p#r1'])
    expect(await h.redis.smembers(`redisjm:${group}:jobs:p:lanes`)).toEqual([`redisjm:${group}:lane:a:queue`])
    expect(await h.redis.smembers(`redisjm:${group}:jobs`)).toEqual(['p'])
    await m.popAndExecute()
    await m.performMaintenance()
    expect(await h.redis.exists(`redisjm:${group}:jobs:p:locks`, `redisjm:${group}:jobs:p:lanes`)).toBe(0)
    expect(await h.redis.smembers(`redisjm:${group}:jobs`)).toEqual([])
  })

  it('maintenance deletes garbage log values (including non-UTF-8 bytes) and releases their locks', async () => {
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0 }, group)
    const log = `redisjm:${group}:log`
    await h.redis.hset(log, 'g#text', '{not json')
    await h.redis.hset(log, 'g#scalar', '"42"')
    await h.redis.hset(log, 'g#binary', Buffer.from([0xff, 0xfe, 0x00, 0x81]))
    await h.redis.sadd(`redisjm:${group}:locks`, 'g#text', 'g#binary')
    const result = await m.performMaintenance()
    expect(result.cleanedCount).toBe(3)
    expect(await h.redis.hlen(log)).toBe(0)
    expect(await h.redis.scard(`redisjm:${group}:locks`)).toBe(0)
  })

  it('dropping a garbage popped entry never deletes a record re-enqueued in between', async () => {
    // WHY: the drop used to SREM the lock and then HDEL the record blindly, out of one atomic step: a
    // producer re-enqueuing the runId after the pop read the garbage lost its fresh record and lock.
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0 }, group, client)
    let ran = 0
    m.createJob({ jobName: 'junk' }, async () => { ran++ })
    await h.redis.hset(`redisjm:${group}:log`, 'junk#r1', '{not json') // garbage, holding no lock
    await h.redis.rpush(`redisjm:${group}:queue`, 'junk#r1')
    // Re-enqueue the runId right after the pop read the garbage record.
    const realHget = client.hget.bind(client) as (...args: any[]) => Promise<string | null>
    let reenqueued = false
    ;(client as any).hget = async (...args: any[]) => {
      const value = await realHget(...args)
      if (!reenqueued && args[1] === 'junk#r1') {
        reenqueued = true
        await new RedisJM(h.redis, group, { logger: false }).queue(new Job({ jobName: 'junk' }, async () => {}), 'r1', 'fresh')
      }
      return value
    }
    expect(await m.popAndExecute()).toBe(true) // the garbage entry, dropped
    expect(reenqueued).toBe(true)
    expect((await m.get('junk#r1'))?.status).toBe('queued')
    expect(await m.isLocked('junk#r1')).toBe(true)
    expect(await m.popAndExecute()).toBe(true) // the re-enqueued run
    expect(ran).toBe(1)
  })

  it('a run costs one round trip per record write: after the pop, one read, then one script call per claim / beat / update / finish', async () => {
    // WHY: every write used to re-read the record first (HGET + script), and a terminal write was
    // followed by separate SREMs (+ an HDEL under keepFinishedInterval: 0) — about 8 + 2 per beat/update.
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0, heartbeatInterval: 60, keepFinishedInterval: 0 }, undefined, client)
    const job = m.createJob({ jobName: 'rt' }, async (_inputs, ctx) => {
      await sleep(150) // a couple of heartbeats
      await ctx.setProgress(0.5)
    })
    // Warm-up run: loads every script into the server cache and does the pop path's periodic reads.
    await job.queue('warm', null)
    await m.popAndExecute()
    const sent: string[] = []
    const realSend = client.sendCommand.bind(client) as (...args: any[]) => unknown
    ;(client as any).sendCommand = (command: { name: string }, ...rest: any[]) => {
      sent.push(String(command.name))
      return realSend(command, ...rest)
    }
    await job.queue('r1', null)
    sent.length = 0
    expect(await m.popAndExecute()).toBe(true)
    const run = sent.splice(0)
    // Besides script calls: just the pop's record read (promotion / old-lane refresh reads are periodic).
    expect(run.filter((c) => !['evalsha', 'zrangebyscore', 'smembers'].includes(c))).toEqual(['hget'])
    // pop + claim + >= 1 heartbeat + update + finish
    expect(run.filter((c) => c === 'evalsha').length).toBeGreaterThanOrEqual(5)
    expect(await m.get('rt#r1')).toBeUndefined() // finished and dropped (keepFinishedInterval: 0)
  })

  it('a claim whose reply was lost and re-sent (ioredis reconnect) still runs the job instead of reading as superseded', async () => {
    // WHY: ioredis re-sends a command whose reply a dropped connection swallowed
    // (`autoResendUnfulfilledCommands`, on by default). The re-run claim finds the record already
    // `running` under this very execution, lost its compare-and-set, and the claim mutator (only
    // `queued` claimable) rejected it — the run was taken for superseded, never executed, and its record
    // sat `running` until maintenance staled it.
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0 }, group, client)
    let ran = 0
    const job = m.createJob({ jobName: 'resend' }, async () => { ran++ })
    await job.queue('r1', null)
    let replayed = false
    for (const cmd of ['evalsha', 'eval'] as const) {
      const real = (client as any)[cmd].bind(client) as (...args: any[]) => Promise<unknown>
      ;(client as any)[cmd] = async (...args: any[]) => {
        const reply = await real(...args)
        // The first record write that leaves the run `running` is the claim: deliver the reply of a
        // re-sent copy instead of the original's (exactly what a reconnect re-send yields).
        if (!replayed && args.some((a) => typeof a === 'string' && a.includes('"status":"running"'))) {
          replayed = true
          return real(...args)
        }
        return reply
      }
    }
    expect(await m.popAndExecute()).toBe(true)
    expect(replayed).toBe(true)
    expect(ran).toBe(1)
    expect((await m.get('resend#r1'))?.status).toBe('finished')
    expect(await m.isLocked('resend#r1')).toBe(false)
  })

  it('after unregisterJob the instance no longer pops that job off an old lane (stale allow-list)', async () => {
    // WHY: the old-lane allow-lists are refreshed at most every 5s; unregisterJob only reset the own-lane
    // cache, so for up to 5s the instance kept taking the unregistered job's entries off old lanes and
    // handling them as unknown jobs — burning their requeue budget, or failing them outright.
    const group = newGroup()
    const producer = newManager({ maintenanceInterval: 0 }, group)
    // Enqueued on the default lane before the job moved to lane `fresh`.
    await producer.enqueue(new Job({ jobName: 'moved' }, async () => {}), 'r1', null)
    const m = newManager({ maintenanceInterval: 0, unknownJobRequeueLimit: 0 }, group)
    const moved = m.createJob({ jobName: 'moved', lane: 'fresh' }, async () => {})
    const other = m.createJob({ jobName: 'other', lane: 'fresh' }, async () => {})
    await other.queue('x', null)
    // This poll discovers the old lane (allow-list `#moved#`) and pops `other#x` off the own lane.
    expect(await m.popAndExecute()).toBe(true)
    expect((await m.get('other#x'))?.status).toBe('finished')
    m.unregisterJob(moved)
    expect(await m.popAndExecute()).toBe(false)
    expect((await m.get('moved#r1'))?.status).toBe('queued')
    expect(await h.redis.lrange(`redisjm:${group}:queue`, 0, -1)).toEqual(['moved#r1'])
  })

  it("maintenance's drop of a stale claiming entry spares a fresh pop's mark set after its read", async () => {
    // WHY: the drop was a blind ZREM decided on an earlier read: a pop of the same jobId landing in
    // between (its fresh `claiming` mark) was erased, and had that popper then died before claiming,
    // nothing would ever requeue the run.
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0, heartbeatInterval: 50, roundsToStale: 2 }, group, client)
    const claiming = `redisjm:${group}:claiming`
    // A leftover mark (its pop was superseded): the record moved on, so maintenance drops the mark.
    await h.redis.hset(`redisjm:${group}:log`, 'cm#r1', JSON.stringify({
      jobId: 'cm#r1', jobName: 'cm', runId: 'cm', inputs: null, targetGroup: group, status: 'running',
      progress: 0, executionId: 'e1', startedAt: Date.now(), heartbeat: Date.now(),
    }))
    await h.redis.zadd(claiming, Date.now() - 60_000, 'cm#r1')
    const fresh = Date.now()
    const realZrange = client.zrangebyscore.bind(client) as (...args: any[]) => Promise<unknown>
    let repopped = false
    ;(client as any).zrangebyscore = async (...args: any[]) => {
      const reply = await realZrange(...args)
      if (!repopped && args[0] === claiming) {
        repopped = true
        await h.redis.zadd(claiming, fresh, 'cm#r1') // the same jobId popped again meanwhile
      }
      return reply
    }
    await m.performMaintenance()
    expect(repopped).toBe(true)
    expect(Number(await h.redis.zscore(claiming, 'cm#r1'))).toBe(fresh)
  })

  it('promotion dropping a stray delayed entry spares a retry re-scheduled after its read', async () => {
    // WHY: the stray-entry drop was a blind ZREM: a retry of the same run scheduled between the read
    // and the drop lost its delayed entry, and the orphan check later staled the run instead of retrying.
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0 }, group, client)
    m.createJob({ jobName: 'pd' }, async () => {})
    const log = `redisjm:${group}:log`
    const delayed = `redisjm:${group}:delayed`
    const base = { jobId: 'pd#r1', jobName: 'pd', runId: 'r1', inputs: null, targetGroup: group, progress: 0, executionId: 'e1' }
    await h.redis.hset(log, 'pd#r1', JSON.stringify({ ...base, status: 'running', startedAt: Date.now(), heartbeat: Date.now() }))
    await h.redis.sadd(`redisjm:${group}:locks`, 'pd#r1')
    await h.redis.zadd(delayed, Date.now() - 1000, 'pd#r1') // stray: the record is running
    const readyAt = Date.now() + 60_000
    const realHget = client.hget.bind(client) as (...args: any[]) => Promise<string | null>
    let rescheduled = false
    ;(client as any).hget = async (...args: any[]) => {
      const value = await realHget(...args)
      if (!rescheduled && args[0] === log && args[1] === 'pd#r1') {
        rescheduled = true
        // The running attempt fails right now and its retry is scheduled (record + entry, atomically).
        await h.redis.multi()
          .hset(log, 'pd#r1', JSON.stringify({ ...base, status: 'delayed', readyAt, attempt: 1 }))
          .zadd(delayed, readyAt, 'pd#r1')
          .exec()
      }
      return value
    }
    await m.popAndExecute()
    expect(rescheduled).toBe(true)
    expect(Number(await h.redis.zscore(delayed, 'pd#r1'))).toBe(readyAt)
  })

  it('values Lua cjson accepts but the manager does not (NaN, hex, "1.", raw tab) are cleaned as garbage', async () => {
    // WHY: the scripts judged "still garbage?" with cjson, which is laxer than JSON.parse: such a value
    // was never deleted by maintenance, and a popped entry over one kept its lock forever.
    const group = newGroup()
    const m = newManager({ maintenanceInterval: 0 }, group)
    m.createJob({ jobName: 'lx' }, async () => {})
    const log = `redisjm:${group}:log`
    const locks = `redisjm:${group}:locks`
    const values = ['{"status":"x","n":NaN}', '{"status":"x","n":0x10}', '{"status":"x","n":1.}', '{"status":"x","s":"a\tb"}']
    for (const [i, value] of values.entries()) {
      await h.redis.hset(log, `lx#m${i}`, value)
      await h.redis.hset(log, `lx#p${i}`, value)
      await h.redis.sadd(locks, `lx#m${i}`, `lx#p${i}`)
      await h.redis.rpush(`redisjm:${group}:queue`, `lx#p${i}`)
    }
    // Popped entries over such a value: dropped with their record and lock.
    for (let i = 0; i < values.length; i++) expect(await m.popAndExecute()).toBe(true)
    // Maintenance: deleted as garbage, locks released.
    const result = await m.performMaintenance()
    expect(result.cleanedCount).toBe(values.length)
    expect(await h.redis.hlen(log)).toBe(0)
    expect(await h.redis.scard(locks)).toBe(0)
    expect(await h.redis.zcard(`redisjm:${group}:claiming`)).toBe(0)
  })

  it('a record cjson rejects (nesting > 1000) re-enqueued over popped garbage is never deleted', async () => {
    // WHY: the orphan purge re-judged the stored value with cjson, which is STRICTER than JSON.parse
    // here: a valid record written in between by a producer was taken for garbage and deleted.
    const group = newGroup()
    const client = newClient()
    const m = newManager({ maintenanceInterval: 0 }, group, client)
    let ran = 0
    m.createJob({ jobName: 'deep' }, async () => { ran++ })
    await h.redis.hset(`redisjm:${group}:log`, 'deep#r1', '{not json')
    await h.redis.rpush(`redisjm:${group}:queue`, 'deep#r1')
    let inputs: unknown = 0
    for (let i = 0; i < 1100; i++) inputs = [inputs]
    const realHget = client.hget.bind(client) as (...args: any[]) => Promise<string | null>
    let reenqueued = false
    ;(client as any).hget = async (...args: any[]) => {
      const value = await realHget(...args)
      if (!reenqueued && args[1] === 'deep#r1') {
        reenqueued = true
        await new RedisJM(h.redis, group, { logger: false }).queue(new Job({ jobName: 'deep' }, async () => {}), 'r1', inputs)
      }
      return value
    }
    expect(await m.popAndExecute()).toBe(true) // the garbage entry
    expect(reenqueued).toBe(true)
    expect((await m.get('deep#r1'))?.status).toBe('queued')
    expect(await m.isLocked('deep#r1')).toBe(true)
  })
})
