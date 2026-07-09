import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { RedisJM } from '../redisjm'
import { Job } from '../job'
import { createMaintenanceJob, MAINTENANCE_JOB_NAME } from '../maintenance'
import type { JobContext, JobLogRecord } from '../types'
import { createMockRedis } from './mock-redis'

describe('RedisJM', () => {
  let redis: ReturnType<typeof createMockRedis>
  let manager: RedisJM

  beforeEach(() => {
    redis = createMockRedis()
    manager = new RedisJM(redis, 'test-group')
  })

  describe('getTargetGroup', () => {
    it('should return the target group', () => {
      expect(manager.getTargetGroup()).toBe('test-group')
    })
  })

  describe('getOptions', () => {
    it('should return defaults', () => {
      expect(manager.getOptions()).toEqual({
        heartbeatInterval: 5000,
        roundsToStale: 2,
        keepFinishedInterval: 0,
        maintenanceInterval: 10000, // heartbeatInterval * roundsToStale
        unknownJobRequeueLimit: 5,
        laneStrategy: 'roundRobin',
        lanePriority: [],
        concurrency: 1,
      })
    })

    it('should merge custom options', () => {
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000 })
      expect(m.getOptions()).toEqual({
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
        maintenanceInterval: 2000, // derived from custom heartbeatInterval
        unknownJobRequeueLimit: 5,
        laneStrategy: 'roundRobin',
        lanePriority: [],
        concurrency: 1,
      })
    })

    it('should accept explicit maintenanceInterval', () => {
      const m = new RedisJM(redis, 'g', { maintenanceInterval: 0 })
      expect(m.getOptions().maintenanceInterval).toBe(0)
    })

    it('should resolve custom lane options', () => {
      const m = new RedisJM(redis, 'g', { laneStrategy: 'priority', lanePriority: ['a', 'b'] })
      expect(m.getOptions().laneStrategy).toBe('priority')
      expect(m.getOptions().lanePriority).toEqual(['a', 'b'])
    })
  })

  describe('queue', () => {
    it('should queue a job and return true', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      const result = await manager.queue(job, 'run1', { key: 'value' })
      expect(result).toBe(true)
    })

    it('should add to locks set', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queue(job, 'run1', 'input1')
      expect(redis.sadd).toHaveBeenCalledWith('redisjm:test-group:locks', 'myJob#run1')
    })

    it('should push to queue list', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queue(job, 'run1', 'input1')
      expect(redis.rpush).toHaveBeenCalledWith('redisjm:test-group:queue', 'myJob#run1')
    })

    it('should create log record with status queued', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queue(job, 'run1', { key: 'value' })
      const logJson = await redis.hget('redisjm:test-group:log', 'myJob#run1')
      const record = JSON.parse(logJson!) as JobLogRecord
      expect(record.status).toBe('queued')
      expect(record.jobName).toBe('myJob')
      expect(record.runId).toBe('run1')
      expect(record.inputs).toEqual({ key: 'value' })
      expect(record.progress).toBe(0)
    })

    it('should reject duplicate runId and return false', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queue(job, 'run1', 'input1')
      const result = await manager.queue(job, 'run1', 'input1')
      expect(result).toBe(false)
    })

    it('should allow different runIds for same job', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      expect(await manager.queue(job, 'run1', 'input1')).toBe(true)
      expect(await manager.queue(job, 'run2', 'input2')).toBe(true)
    })
  })

  describe('queueFirst', () => {
    it('should push to front of queue list', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queueFirst(job, 'run1', 'input1')
      expect(redis.lpush).toHaveBeenCalledWith('redisjm:test-group:queue', 'myJob#run1')
    })

    it('should add to locks and log like queue', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queueFirst(job, 'run1', 'input1')
      expect(redis.sadd).toHaveBeenCalled()
      expect(redis.hset).toHaveBeenCalled()
    })
  })

  describe('isQueued', () => {
    it('should return true for locked jobIds', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queue(job, 'run1', 'input1')
      expect(await manager.isQueued('myJob#run1')).toBe(true)
    })

    it('should return false for non-locked jobIds', async () => {
      expect(await manager.isQueued('myJob#nonexistent')).toBe(false)
    })
  })

  describe('list', () => {
    it('should return all log records', async () => {
      const job1 = new Job({ jobName: 'job1' }, vi.fn())
      const job2 = new Job({ jobName: 'job2' }, vi.fn())
      await manager.queue(job1, 'run1', { a: 1 })
      await manager.queue(job2, 'run2', { b: 2 })

      const entries = await manager.list()
      expect(entries).toHaveLength(2)
      expect(entries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ jobName: 'job1', jobId: 'job1#run1', status: 'queued' }),
          expect.objectContaining({ jobName: 'job2', jobId: 'job2#run2', status: 'queued' }),
        ]),
      )
    })

    it('should return empty list when no jobs', async () => {
      expect(await manager.list()).toEqual([])
    })
  })

  describe('unqueue', () => {
    it('should remove from queue, locks, and log', async () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      await manager.queue(job, 'run1', 'input1')
      await manager.unqueue('myJob#run1')
      expect(redis.lrem).toHaveBeenCalledWith('redisjm:test-group:queue', 1, 'myJob#run1')
      expect(redis.srem).toHaveBeenCalledWith('redisjm:test-group:locks', 'myJob#run1')
      expect(redis.hdel).toHaveBeenCalledWith('redisjm:test-group:log', 'myJob#run1')
      expect(await manager.isQueued('myJob#run1')).toBe(false)
    })
  })

  describe('createJob', () => {
    it('should create and register a job', () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'newJob' }, fn)
      expect(job).toBeInstanceOf(Job)
      expect(job.getName()).toBe('newJob')
    })

    it('should infer generic types from function', () => {
      const fn = vi.fn((_inputs: { count: number }, _ctx: JobContext) => {})
      const job = manager.createJob({ jobName: 'typed' }, fn)
      expect(job.getName()).toBe('typed')
    })
  })

  describe('Job.getLane', () => {
    it('should return the declared lane', () => {
      expect(new Job({ jobName: 'x', lane: 'images' }, vi.fn()).getLane()).toBe('images')
    })

    it('should return undefined for a no-lane job', () => {
      expect(new Job({ jobName: 'x' }, vi.fn()).getLane()).toBeUndefined()
    })
  })

  describe('registerJob / unregisterJob', () => {
    it('should enforce unique jobName', () => {
      const job1 = new Job({ jobName: 'sameName' }, vi.fn())
      const job2 = new Job({ jobName: 'sameName' }, vi.fn())
      manager.registerJob(job1)
      expect(() => manager.registerJob(job2)).toThrow('Job with name "sameName" is already registered')
    })

    it('should not double-register the same job instance', () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      manager.registerJob(job)
      manager.registerJob(job) // should not throw
    })

    it('should stop re-dispatching events after unregister', async () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'eventJob' }, fn)
      const onStart = vi.fn()
      manager.hook('start', onStart)

      manager.unregisterJob(job)
      await job.execute('input', { targetGroup: 'test-group' })
      expect(onStart).not.toHaveBeenCalled()
    })

    it('should allow re-registering after unregister', () => {
      const job = new Job({ jobName: 'myJob' }, vi.fn())
      manager.registerJob(job)
      manager.unregisterJob(job)
      expect(() => manager.registerJob(job)).not.toThrow()
    })
  })

  describe('event re-dispatching', () => {
    it('should re-dispatch start event when targetGroup matches', async () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'eventJob' }, fn)
      const onStart = vi.fn()
      manager.hook('start', onStart)

      await job.execute('input', { targetGroup: 'test-group' })
      expect(onStart).toHaveBeenCalledWith(
        expect.objectContaining({
          job,
          targetGroup: 'test-group',
          inputs: 'input',
        }),
      )
    })

    it('should re-dispatch finish event when targetGroup matches', async () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'eventJob' }, fn)
      const onFinish = vi.fn()
      manager.hook('finish', onFinish)

      await job.execute('input', { targetGroup: 'test-group' })
      expect(onFinish).toHaveBeenCalled()
    })

    it('should re-dispatch error event when targetGroup matches', async () => {
      const fn = vi.fn(() => { throw new Error('fail') })
      const job = manager.createJob({ jobName: 'eventJob' }, fn)
      const onError = vi.fn()
      manager.hook('error', onError)

      await expect(job.execute('input', { targetGroup: 'test-group' })).rejects.toThrow('fail')
      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({ error: expect.any(Error) }),
      )
    })

    it('should NOT re-dispatch events when targetGroup does not match', async () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'eventJob' }, fn)
      const onStart = vi.fn()
      const onFinish = vi.fn()
      manager.hook('start', onStart)
      manager.hook('finish', onFinish)

      await job.execute('input', { targetGroup: 'other-group' })
      expect(onStart).not.toHaveBeenCalled()
      expect(onFinish).not.toHaveBeenCalled()
    })

    it('should re-dispatch update events', async () => {
      const fn = vi.fn(async (_input: string, ctx: JobContext) => {
        await ctx.setProgress(0.5)
      })
      const job = manager.createJob({ jobName: 'updateJob' }, fn)
      const onUpdate = vi.fn()
      manager.hook('update', onUpdate)

      await manager.queue(job, 'run1', 'input')
      await job.execute('input', { targetGroup: 'test-group', runId: 'run1' })
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ progress: 0.5 }),
      )
    })
  })

  describe('log record lifecycle', () => {
    it('should update log to running on start then remove on finish with keepFinishedInterval=0', async () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'logJob' }, fn)
      await manager.queue(job, 'run1', 'input')
      await job.execute('input', { targetGroup: 'test-group', runId: 'run1' })

      const logJson = await redis.hget('redisjm:test-group:log', 'logJob#run1')
      expect(logJson).toBeNull()
    })

    it('should keep finished log records when keepFinishedInterval > 0', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 'keepJob' }, fn)
      await m.queue(job, 'run1', 'input')
      await job.execute('input', { targetGroup: 'test-group', runId: 'run1' })

      const logJson = await redis.hget('redisjm:test-group:log', 'keepJob#run1')
      expect(logJson).not.toBeNull()
      const record = JSON.parse(logJson!) as JobLogRecord
      expect(record.status).toBe('finished')
      expect(record.finishedAt).toBeGreaterThan(0)
    })

    it('should remove lock on finish', async () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'lockJob' }, fn)
      await manager.queue(job, 'run1', 'input')
      await job.execute('input', { targetGroup: 'test-group', runId: 'run1' })
      expect(await manager.isQueued('lockJob#run1')).toBe(false)
    })

    it('should remove lock on error', async () => {
      const fn = vi.fn(() => { throw new Error('fail') })
      const job = manager.createJob({ jobName: 'errJob' }, fn)
      await manager.queue(job, 'run1', 'input')
      await expect(job.execute('input', { targetGroup: 'test-group', runId: 'run1' })).rejects.toThrow()
      expect(await manager.isQueued('errJob#run1')).toBe(false)
    })

    it('should update progress in log via setProgress', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
      const fn = vi.fn(async (_input: string, ctx: JobContext) => {
        await ctx.setProgress(0.75)
      })
      const job = m.createJob({ jobName: 'progJob' }, fn)
      await m.queue(job, 'run1', 'input')
      await job.execute('input', { targetGroup: 'test-group', runId: 'run1' })

      const logJson = await redis.hget('redisjm:test-group:log', 'progJob#run1')
      const record = JSON.parse(logJson!) as JobLogRecord
      expect(record.status).toBe('finished')
    })

    it('should update attrs in log via setAttrs', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
      const fn = vi.fn(async (_input: string, ctx: JobContext<{ step: string }>) => {
        await ctx.setAttrs({ step: 'done' })
      })
      const job = m.createJob<string, { step: string }>({ jobName: 'attrJob' }, fn)
      await m.queue(job, 'run1', 'input')
      await job.execute('input', { targetGroup: 'test-group', runId: 'run1' })

      const logJson = await redis.hget('redisjm:test-group:log', 'attrJob#run1')
      const record = JSON.parse(logJson!) as JobLogRecord
      expect(record.attrs).toEqual({ step: 'done' })
    })
  })

  describe('popAndExecute', () => {
    it('should return false when queue is empty', async () => {
      expect(await manager.popAndExecute()).toBe(false)
    })

    it('should pop from queue and execute matched job', async () => {
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'popJob' }, fn)
      await manager.queue(job, 'run1', { data: 'test' })

      const result = await manager.popAndExecute()
      expect(result).toBe(true)
      expect(fn).toHaveBeenCalledWith({ data: 'test' }, expect.any(Object))
    })

    it('should drop an unknown job name immediately when unknownJobRequeueLimit is 0', async () => {
      const m = new RedisJM(redis, 'test-group', {
        keepFinishedInterval: 60000,
        unknownJobRequeueLimit: 0,
        logger: false,
      })
      // Under lanes a manager with zero registered jobs subscribes to no work lane and would never
      // pop the default-queue entry below. Register a sentinel default-lane job so this manager
      // subscribes to the default lane and reaches the unknown-job path (its handler is irrelevant).
      m.createJob({ jobName: 'sentinel' }, vi.fn())
      const jobId = 'unknownJob#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.rpush('redisjm:test-group:queue', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'unknownJob', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'queued', progress: 0,
      }))

      const result = await m.popAndExecute()
      expect(result).toBe(true)

      const logJson = await redis.hget('redisjm:test-group:log', jobId)
      const record = JSON.parse(logJson!) as JobLogRecord
      expect(record.status).toBe('error')
      expect(record.error).toBe('Job name is unknown')
      expect(await m.isQueued(jobId)).toBe(false)
    })

    it('should re-queue an unknown job name up to the limit, keeping the lock, then drop it', async () => {
      const m = new RedisJM(redis, 'test-group', {
        keepFinishedInterval: 60000,
        unknownJobRequeueLimit: 2,
        logger: false,
      })
      // Register a sentinel default-lane job so the manager subscribes to the default lane and can
      // pop the seeded unknown job (a zero-job manager subscribes to no work lane under lanes).
      m.createJob({ jobName: 'sentinel' }, vi.fn())
      const jobId = 'unknownJob#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.rpush('redisjm:test-group:queue', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'unknownJob', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'queued', progress: 0,
      }))

      // Pop 1 + 2: re-queued each time (returns false = deferred, not executed), lock retained,
      // still back in the queue.
      for (let i = 1; i <= 2; i++) {
        expect(await m.popAndExecute()).toBe(false)
        const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
        expect(record.status).toBe('queued')
        expect(record.requeueCount).toBe(i)
        expect(await m.isQueued(jobId)).toBe(true)
        expect(await redis.lpos('redisjm:test-group:queue', jobId)).not.toBeNull()
      }

      // Pop 3: budget exhausted → mark error and release the lock.
      expect(await m.popAndExecute()).toBe(true)
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('error')
      expect(record.error).toBe('Job name is unknown')
      expect(await m.isQueued(jobId)).toBe(false)
    })

    it('should let a sibling instance claim a re-queued unknown job', async () => {
      // Instance A has no handler; instance B (sharing the same Redis/group) does.
      const a = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const b = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const handled: string[] = []
      b.createJob({ jobName: 'rolling' }, vi.fn(async (input: string) => { handled.push(input) }))
      // `rolling` is a default-lane job. A must subscribe to the default lane to pop it, but must NOT
      // register `rolling` (that would give it the handler): register a sentinel default-lane job.
      a.createJob({ jobName: 'sentinel' }, vi.fn())

      await a.queue(new Job({ jobName: 'rolling' }, vi.fn()), 'run1', 'payload')

      // A pops first and re-queues (no handler → returns false/deferred); B then pops and runs it.
      expect(await a.popAndExecute()).toBe(false)
      expect(handled).toEqual([])
      expect(await b.popAndExecute()).toBe(true)
      expect(handled).toEqual(['payload'])
      expect(await b.isQueued('rolling#run1')).toBe(false)
    })

    it('should process jobs in FIFO order', async () => {
      const order: string[] = []
      const job = manager.createJob({ jobName: 'fifo' }, vi.fn(async (input: string) => {
        order.push(input)
      }))
      await manager.queue(job, 'run1', 'first')
      await manager.queue(job, 'run2', 'second')

      await manager.popAndExecute()
      await manager.popAndExecute()
      expect(order).toEqual(['first', 'second'])
    })

    it('should process queueFirst jobs before normal queued jobs', async () => {
      const order: string[] = []
      const job = manager.createJob({ jobName: 'prio' }, vi.fn(async (input: string) => {
        order.push(input)
      }))
      await manager.queue(job, 'run1', 'normal')
      await manager.queueFirst(job, 'run2', 'priority')

      await manager.popAndExecute()
      await manager.popAndExecute()
      expect(order).toEqual(['priority', 'normal'])
    })
  })

  describe('start / stop', () => {
    afterEach(() => {
      manager.stop()
    })

    it('should poll and execute queued jobs', async () => {
      vi.useFakeTimers()
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'pollJob' }, fn)
      await manager.queue(job, 'run1', 'input1')

      manager.start(100)
      await vi.advanceTimersByTimeAsync(0)
      expect(fn).toHaveBeenCalledWith('input1', expect.any(Object))

      manager.stop()
      vi.useRealTimers()
    })

    it('should wait interval when queue is empty', async () => {
      vi.useFakeTimers()
      const fn = vi.fn()
      manager.createJob({ jobName: 'waitJob' }, fn)

      manager.start(100)
      await vi.advanceTimersByTimeAsync(0)
      expect(fn).not.toHaveBeenCalled()

      manager.stop()
      vi.useRealTimers()
    })

    it('should stop polling', async () => {
      vi.useFakeTimers()
      const fn = vi.fn()
      const job = manager.createJob({ jobName: 'stopJob' }, fn)

      manager.start(50)
      await vi.advanceTimersByTimeAsync(0) // first poll — empty
      manager.stop()

      await manager.queue(job, 'run1', 'input1')
      await vi.advanceTimersByTimeAsync(200)
      expect(fn).not.toHaveBeenCalled()

      vi.useRealTimers()
    })
  })

  describe('performMaintenance', () => {
    it('should mark stale jobs', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      const jobId = 'staleJob#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'staleJob', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'running', progress: 0.5,
        startedAt: Date.now() - 10000, heartbeat: Date.now() - 5000,
      }))

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(1)

      const logJson = await redis.hget('redisjm:test-group:log', jobId)
      const record = JSON.parse(logJson!) as JobLogRecord
      expect(record.status).toBe('stale')
      expect(record.finishedAt).toBeGreaterThan(0)

      const locked = await redis.sismember('redisjm:test-group:locks', jobId)
      expect(locked).toBe(0)
    })

    it('should not mark running jobs with recent heartbeat as stale', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      const jobId = 'activeJob#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'activeJob', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'running', progress: 0.5,
        startedAt: Date.now() - 500, heartbeat: Date.now() - 500,
      }))

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
    })

    it('should clean up expired finished records', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 1000,
      })
      const jobId = 'doneJob#run1'
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'doneJob', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'finished', progress: 1,
        finishedAt: Date.now() - 2000,
      }))

      const result = await m.performMaintenance()
      expect(result.cleanedCount).toBe(1)

      const logJson = await redis.hget('redisjm:test-group:log', jobId)
      expect(logJson).toBeNull()
    })

    it('should not clean up recently finished records', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      const jobId = 'recentJob#run1'
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'recentJob', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'finished', progress: 1,
        finishedAt: Date.now() - 1000,
      }))

      const result = await m.performMaintenance()
      expect(result.cleanedCount).toBe(0)
    })

    it('should clean up stale and error records after keepFinishedInterval', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 1000,
      })
      await redis.hset('redisjm:test-group:log', 'stale#r1', JSON.stringify({
        jobId: 'stale#r1', jobName: 'stale', runId: 'r1', inputs: null,
        targetGroup: 'test-group', status: 'stale', progress: 0,
        finishedAt: Date.now() - 2000,
      }))
      await redis.hset('redisjm:test-group:log', 'err#r1', JSON.stringify({
        jobId: 'err#r1', jobName: 'err', runId: 'r1', inputs: null,
        targetGroup: 'test-group', status: 'error', progress: 0, error: 'failed',
        finishedAt: Date.now() - 2000,
      }))

      const result = await m.performMaintenance()
      expect(result.cleanedCount).toBe(2)
    })

    it('should reclaim an orphaned queued record in two passes', async () => {
      // Simulates an instance that died between lpop and the 'start' event:
      // record is 'queued' + locked but absent from the queue list.
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      const jobId = 'orphan#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'orphan', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'queued', progress: 0,
      }))

      // Pass 1: marks the record as a suspect, keeps the lock
      let result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
      let record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('queued')
      expect(record.suspectedAt).toBeGreaterThan(0)
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(1)

      // Pass 2 within the threshold: still a suspect, nothing reclaimed
      result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)

      // Pass 3 past the threshold: reclaimed
      record.suspectedAt = Date.now() - 3000 // > heartbeatInterval * roundsToStale
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify(record))
      result = await m.performMaintenance()
      expect(result.staleCount).toBe(1)

      record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('stale')
      expect(record.suspectedAt).toBeUndefined()
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(0)
    })

    it('should not suspect a queued record that is still in the queue', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      const job = new Job({ jobName: 'waiting' }, vi.fn())
      await m.queue(job, 'run1', null)

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'waiting#run1'))!) as JobLogRecord
      expect(record.suspectedAt).toBeUndefined()
    })

    it('should clear suspectedAt when the record reappears in the queue', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      const jobId = 'requeued#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.rpush('redisjm:test-group:queue', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'requeued', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'queued', progress: 0,
        suspectedAt: Date.now() - 10000,
      }))

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.suspectedAt).toBeUndefined()
      expect(record.status).toBe('queued')
    })

    it('should clear suspectedAt when the job starts normally', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
      const job = m.createJob({ jobName: 'survivor' }, vi.fn())
      await m.queue(job, 'run1', null)

      // Maintenance stamped the record while it sat in the pop→start window
      const jobId = 'survivor#run1'
      const stamped = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      stamped.suspectedAt = Date.now()
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify(stamped))

      await m.popAndExecute()

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('finished')
      expect(record.suspectedAt).toBeUndefined()
    })
  })

  describe('orphaned-lock reclaim & corrupt-record hygiene', () => {
    // WHY: a lock left by an enqueue that crashed between SADD and HSET has no record and no queue
    // entry, so record-driven maintenance never sees it — the runId is blocked forever. Two-pass
    // reclaim must free it, and the user-visible proof is that queue() works again afterwards.
    it('reclaims an orphaned lock in two passes and frees the runId', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000, roundsToStale: 2, keepFinishedInterval: 60000, logger: false,
      })
      const jobId = 'ghost#run1'
      await redis.sadd('redisjm:test-group:locks', jobId) // lock with no record, no queue entry

      // A fresh Job for this runId can't be queued while the orphan lock is held.
      const job = new Job({ jobName: 'ghost' }, vi.fn())
      expect(await m.queue(job, 'run1', null)).toBe(false)

      // Pass 1: stamps a suspect, keeps the lock.
      let result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(1)
      expect(Number(await redis.hget('redisjm:test-group:suspects', jobId))).toBeGreaterThan(0)

      // Age the suspicion past the stale threshold (heartbeatInterval * roundsToStale = 2000ms).
      await redis.hset('redisjm:test-group:suspects', jobId, String(Date.now() - 3000))

      // Pass 2: releases the lock and clears the suspect, counted as stale.
      result = await m.performMaintenance()
      expect(result.staleCount).toBe(1)
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(0)
      expect(await redis.hget('redisjm:test-group:suspects', jobId)).toBeNull()

      // The actual fix: the runId is queueable again.
      expect(await m.queue(job, 'run1', null)).toBe(true)
    })

    // WHY: an enqueue in flight (SADD done, HSET landing a few ms later) is momentarily
    // indistinguishable from a permanent orphan. Once its record lands, pass 2 must exonerate the
    // suspect and keep the lock — never reclaim a live run.
    it('does not reclaim a lock once its log record lands (in-flight enqueue)', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000, roundsToStale: 2, keepFinishedInterval: 60000, logger: false,
      })
      const jobId = 'slow#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      // A suspect already stamped and aged past the threshold on an earlier pass...
      await redis.hset('redisjm:test-group:suspects', jobId, String(Date.now() - 3000))
      // ...but the enqueue's HSET has since landed (record exists and is queued).
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'slow', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'queued', progress: 0,
      }))
      await redis.rpush('redisjm:test-group:queue', jobId)

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(1) // lock kept
      expect(await redis.hget('redisjm:test-group:suspects', jobId)).toBeNull() // suspicion cleared
    })

    // WHY: previously the unparseable-record branch only HDEL'd under keepFinishedInterval 0, and
    // performMaintenance skipped it with `continue` — so under retention garbage accumulated forever.
    it('cleans an unparseable record and its lock under retention', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000, roundsToStale: 2, keepFinishedInterval: 60000, logger: false,
      })
      const jobId = 'corrupt#run1'
      await redis.hset('redisjm:test-group:log', jobId, 'not json{')
      await redis.sadd('redisjm:test-group:locks', jobId)

      const result = await m.performMaintenance()
      expect(result.cleanedCount).toBe(1)
      expect(await redis.hget('redisjm:test-group:log', jobId)).toBeNull()
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(0)
    })

    // WHY: valid JSON of the wrong shape (a bare `42`, `"true"`) parses fine but yields a record with
    // every field undefined. The shape guard must reject it everywhere it's read — list(), pop, sweep.
    it('treats valid-JSON-but-wrong-shape records as garbage across list, pop, and maintenance', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })

      // list() skips a bare-number record.
      await redis.hset('redisjm:test-group:log', 'foreign#run1', '42')
      expect(await m.list()).toHaveLength(0)

      // popAndExecute drops it without throwing.
      const job = m.createJob({ jobName: 'foreign' }, vi.fn())
      await redis.sadd('redisjm:test-group:locks', 'foreign#run1')
      await redis.rpush('redisjm:test-group:queue', 'foreign#run1')
      await expect(m.popAndExecute()).resolves.toBe(true)
      expect(job).toBeDefined()
      expect(await redis.hget('redisjm:test-group:log', 'foreign#run1')).toBeNull()
      expect(await redis.sismember('redisjm:test-group:locks', 'foreign#run1')).toBe(0)

      // maintenance cleans a fresh malformed record + lock.
      await redis.hset('redisjm:test-group:log', 'foreign#run2', '"true"')
      await redis.sadd('redisjm:test-group:locks', 'foreign#run2')
      const result = await m.performMaintenance()
      expect(result.cleanedCount).toBe(1)
      expect(await redis.hget('redisjm:test-group:log', 'foreign#run2')).toBeNull()
      expect(await redis.sismember('redisjm:test-group:locks', 'foreign#run2')).toBe(0)
    })

    // WHY: a manually removed run must not leave a pending suspects entry that a later maintenance
    // pass would act on (e.g. re-SREM a lock re-added under the same runId).
    it('unqueue clears a pending suspicion so maintenance reclaims nothing', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000, roundsToStale: 2, keepFinishedInterval: 60000, logger: false,
      })
      const jobId = 'manual#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.hset('redisjm:test-group:suspects', jobId, String(Date.now() - 3000))

      await m.unqueue(jobId)
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(0)
      expect(await redis.hget('redisjm:test-group:suspects', jobId)).toBeNull()

      const result = await m.performMaintenance()
      expect(result.staleCount).toBe(0)
    })
  })

  describe('auto-maintenance via start()', () => {
    afterEach(() => {
      manager.stop()
    })

    it('should enqueue and run maintenance on start', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      // Stale record left behind by a "crashed" instance
      const jobId = 'crashed#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'crashed', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'running', progress: 0,
        startedAt: Date.now() - 10000, heartbeat: Date.now() - 10000,
      }))

      m.start(100)
      // The async reclaim→enqueue bootstrap plus the pop+execute take several poll cycles to
      // settle; drain a few so the assertion is deterministic (no flake).
      for (let i = 0; i < 4; i++) await vi.advanceTimersByTimeAsync(100)

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('stale')
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(0)

      m.stop()
      vi.useRealTimers()
    })

    it('should re-enqueue maintenance every maintenanceInterval', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { maintenanceInterval: 500 })
      const spy = vi.spyOn(m, 'performMaintenance')

      m.start(100)
      // Drain the immediate enqueue (still well before the 500ms interval tick).
      for (let i = 0; i < 3; i++) await vi.advanceTimersByTimeAsync(100)
      expect(spy).toHaveBeenCalledTimes(1)

      await vi.advanceTimersByTimeAsync(700) // interval tick at 500 + a poll to execute it
      expect(spy.mock.calls.length).toBeGreaterThanOrEqual(2)

      m.stop()
      vi.useRealTimers()
    })

    it('should not enqueue maintenance when maintenanceInterval is 0', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { maintenanceInterval: 0 })
      const spy = vi.spyOn(m, 'performMaintenance')

      m.start(100)
      await vi.advanceTimersByTimeAsync(1000)
      expect(spy).not.toHaveBeenCalled()

      m.stop()
      vi.useRealTimers()
    })

    it('should reuse a consumer-registered maintenance job', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { maintenanceInterval: 500 })
      const job = createMaintenanceJob(m)
      expect(job.getName()).toBe('__redisjm_maintenance')

      // start() must not throw "already registered"
      expect(() => m.start(100)).not.toThrow()
      await vi.advanceTimersByTimeAsync(0)

      m.stop()
      vi.useRealTimers()
    })

    it('should stop enqueuing maintenance after stop()', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { maintenanceInterval: 500 })
      const spy = vi.spyOn(m, 'performMaintenance')

      m.start(100)
      await vi.advanceTimersByTimeAsync(0)
      const callsAtStop = spy.mock.calls.length
      m.stop()

      await vi.advanceTimersByTimeAsync(2000)
      expect(spy.mock.calls.length).toBe(callsAtStop)

      vi.useRealTimers()
    })
  })

  describe('multiple managers', () => {
    it('same job can be used with different managers', async () => {
      const redis2 = createMockRedis()
      const manager2 = new RedisJM(redis2, 'group2')
      const fn = vi.fn()
      const job = new Job({ jobName: 'shared' }, fn)

      manager.registerJob(job)
      manager2.registerJob(job)

      const onStart1 = vi.fn()
      const onStart2 = vi.fn()
      manager.hook('start', onStart1)
      manager2.hook('start', onStart2)

      await job.execute('input', { targetGroup: 'test-group' })
      expect(onStart1).toHaveBeenCalled()
      expect(onStart2).not.toHaveBeenCalled()

      onStart1.mockClear()
      onStart2.mockClear()

      await job.execute('input', { targetGroup: 'group2' })
      expect(onStart1).not.toHaveBeenCalled()
      expect(onStart2).toHaveBeenCalled()
    })
  })

  describe('blocking behavior', () => {
    it('running job should block queue then unblock on finish', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
      let resolveFn!: () => void
      const fn = vi.fn(() => new Promise<void>((resolve) => { resolveFn = resolve }))
      const job = m.createJob({ jobName: 'blockJob' }, fn)

      await m.queue(job, 'run1', 'input')
      const popPromise = m.popAndExecute()

      // Flush microtasks so popAndExecute reaches the fn call
      await new Promise((r) => setTimeout(r, 0))

      // While running, same runId should be blocked
      expect(await m.isQueued('blockJob#run1')).toBe(true)
      expect(await m.queue(job, 'run1', 'input')).toBe(false)

      resolveFn()
      await popPromise

      // After finish, lock is removed
      expect(await m.isQueued('blockJob#run1')).toBe(false)
      expect(await m.queue(job, 'run1', 'input')).toBe(true)
    })
  })

  describe('get', () => {
    it('should return a single record by jobId', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
      const job = new Job({ jobName: 'g' }, vi.fn())
      await m.queue(job, 'r1', { a: 1 })
      const record = await m.get('g#r1')
      expect(record?.jobName).toBe('g')
      expect(record?.status).toBe('queued')
      expect(record?.inputs).toEqual({ a: 1 })
    })

    it('should return undefined when the record is absent', async () => {
      expect(await manager.get('nope#r1')).toBeUndefined()
    })
  })

  describe('jobName validation', () => {
    it('should reject a job name containing "#"', () => {
      expect(() => manager.registerJob(new Job({ jobName: 'a#b' }, vi.fn()))).toThrow('must not contain "#"')
      expect(() => manager.createJob({ jobName: 'x#y' }, vi.fn())).toThrow('must not contain "#"')
    })
  })

  describe('enqueue ordering', () => {
    it('should write the log record before pushing the queue entry', async () => {
      const job = new Job({ jobName: 'order' }, vi.fn())
      await manager.queue(job, 'r1', 'x')
      const hsetOrder = (redis.hset as any).mock.invocationCallOrder[0]
      const rpushOrder = (redis.rpush as any).mock.invocationCallOrder[0]
      // The queue entry (what makes a job poppable) must never precede its log record.
      expect(hsetOrder).toBeLessThan(rpushOrder)
    })
  })

  describe('list resilience', () => {
    it('should skip an unparseable record instead of throwing', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const job = new Job({ jobName: 'ok' }, vi.fn())
      await m.queue(job, 'r1', { a: 1 })
      await redis.hset('redisjm:test-group:log', 'bad#r1', 'not json{')

      const records = await m.list()
      expect(records).toHaveLength(1)
      expect(records[0].jobName).toBe('ok')
    })
  })

  describe('start validation', () => {
    it('should throw on a non-positive interval', () => {
      expect(() => manager.start(0)).toThrow(TypeError)
      expect(() => manager.start(-5)).toThrow()
      expect(() => manager.start(Number.NaN)).toThrow()
    })
  })

  describe('graceful stop()', () => {
    it('should resolve only after the in-flight job settles', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0 })
      let release!: () => void
      const fn = vi.fn(() => new Promise<void>((r) => { release = r }))
      const job = m.createJob({ jobName: 'drain' }, fn)
      await m.queue(job, 'r1', 'x')

      m.start(50)
      await new Promise((r) => setTimeout(r, 0)) // let the poll pick up and start the job
      expect(fn).toHaveBeenCalled()

      let stopped = false
      const stopPromise = m.stop().then(() => { stopped = true })
      await new Promise((r) => setTimeout(r, 0))
      expect(stopped).toBe(false) // still draining the in-flight job

      release()
      await stopPromise
      expect(stopped).toBe(true)
    })

    // WHY: a stop() that lands while a poll is MID-POP must still drain the job that pop returns — the
    // queue entry is already off Redis, so the run WILL be dispatched by the in-flight poll; if stop()
    // snapshotted inFlightRuns before that dispatch it would resolve while the job still executes
    // (e.g. after the caller closed Redis). With {abort:true}, the late-dispatched run must also
    // receive the abort signal (its controller registers after the first abort pass).
    it('stop() during a mid-pop poll waits for the popped run, which still sees the abort signal', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      let handlerDone = false
      let sawAbort = false
      const job = m.createJob({ jobName: 'midpop' }, vi.fn(async (_i: string, ctx: JobContext) => {
        // Cooperative handler: wait for the shutdown abort to reach this late-dispatched run, then
        // finish. If the second abort pass were missing, this would hang and the test would time out.
        if (!ctx.signal.aborted) {
          await new Promise<void>((resolve) => ctx.signal.addEventListener('abort', () => resolve(), { once: true }))
        }
        sawAbort = ctx.signal.aborted
        handlerDone = true
      }))
      await m.queue(job, 'r1', 'x')

      // Defer the poll's pop: the first lmpop hangs until we release it with the queued jobId.
      let releasePop!: (v: unknown) => void
      ;(redis.lmpop as any).mockImplementationOnce(() => new Promise((resolve) => { releasePop = resolve }))

      m.start(50)
      await new Promise((r) => setTimeout(r, 0)) // let the poll reach the pending lmpop

      let stopped = false
      const stopPromise = m.stop({ abort: true }).then(() => { stopped = true })
      await new Promise((r) => setTimeout(r, 0))
      expect(stopped).toBe(false) // stop is awaiting the mid-pop poll, not resolving early

      // The pop now resolves with the entry it already removed — the final poll dispatches the run.
      releasePop(['redisjm:test-group:queue', ['midpop#r1']])
      await stopPromise
      expect(stopped).toBe(true)
      expect(handlerDone).toBe(true) // stop() resolved only after the popped run settled
      expect(sawAbort).toBe(true) // the second abort pass reached the late-dispatched run
    })
  })

  describe('concurrency', () => {
    afterEach(() => { vi.useRealTimers() })

    // The poll loop re-schedules found-work polls at delay 0; a zero-duration timer advance does not
    // fire such cascading same-tick timers under fake timers, so advance a tiny positive amount to
    // drain the poll cascade (still well under heartbeatInterval, so no heartbeats fire).
    const TICK = 1

    // Helper: a job whose handler blocks until its per-input gate is released, recording start order.
    const gatedJob = (m: RedisJM) => {
      const started: string[] = []
      const gates = new Map<string, () => void>()
      const job = m.createJob({ jobName: 'p' }, vi.fn((input: string) => new Promise<void>((resolve) => {
        started.push(input)
        gates.set(input, resolve)
      })))
      return { job, started, gates }
    }

    // WHY: with concurrency > 1 the poll loop must dispatch up to N runs simultaneously — both handlers
    // start before either resolves. The serial-default counterpart (below) is the regression guard.
    it('runs up to `concurrency` jobs in parallel', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false, concurrency: 2 })
      const { job, started, gates } = gatedJob(m)
      await m.queue(job, 'r1', 'a')
      await m.queue(job, 'r2', 'b')

      m.start(50)
      await vi.advanceTimersByTimeAsync(TICK)
      // Both handlers are in flight with neither gate released.
      expect([...started].sort()).toEqual(['a', 'b'])

      gates.get('a')!()
      gates.get('b')!()
      await m.stop()
    })

    // WHY: regression guard for the serial default — one long job blocks the instance, so the second
    // starts only after the first finishes and frees the single slot.
    it('with default concurrency 1, the second job starts only after the first finishes', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const { job, started, gates } = gatedJob(m)
      await m.queue(job, 'r1', 'a')
      await m.queue(job, 'r2', 'b')

      m.start(50)
      await vi.advanceTimersByTimeAsync(TICK)
      expect(started).toEqual(['a']) // only the first — the single slot is full

      gates.get('a')!()
      await vi.advanceTimersByTimeAsync(TICK)
      expect(started).toEqual(['a', 'b']) // slot freed → the second starts

      gates.get('b')!()
      await m.stop()
    })

    // WHY: the cap must never be exceeded — with 3 jobs and concurrency 2 the third waits for a slot to
    // free, then starts.
    it('never exceeds the concurrency cap (3 jobs, concurrency 2)', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false, concurrency: 2 })
      const { job, started, gates } = gatedJob(m)
      for (const [r, v] of [['r1', 'a'], ['r2', 'b'], ['r3', 'c']] as const) await m.queue(job, r, v)

      m.start(50)
      await vi.advanceTimersByTimeAsync(TICK)
      expect([...started].sort()).toEqual(['a', 'b']) // two in flight, third waits

      gates.get('a')!()
      await vi.advanceTimersByTimeAsync(TICK)
      expect([...started].sort()).toEqual(['a', 'b', 'c']) // a freed a slot → c starts

      gates.get('b')!()
      gates.get('c')!()
      await m.stop()
    })

    // WHY: stop() must drain EVERY in-flight run before resolving, not just one — the graceful contract
    // holds under concurrency too.
    it('graceful stop() drains multiple in-flight runs before resolving', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false, concurrency: 2 })
      const finished: string[] = []
      const gates: Array<() => void> = []
      const job = m.createJob({ jobName: 'd' }, vi.fn((input: string) => new Promise<void>((resolve) => {
        gates.push(() => { finished.push(input); resolve() })
      })))
      await m.queue(job, 'r1', 'a')
      await m.queue(job, 'r2', 'b')

      m.start(50)
      await vi.advanceTimersByTimeAsync(TICK)
      expect(gates).toHaveLength(2) // both in flight

      let stopped = false
      const stopPromise = m.stop().then(() => { stopped = true })
      await Promise.resolve()
      expect(stopped).toBe(false) // still draining both

      gates[0]()
      gates[1]()
      await stopPromise
      expect(stopped).toBe(true)
      expect(finished.sort()).toEqual(['a', 'b'])
    })

    // WHY: stop({ abort: true }) fires ctx.signal for a fast shutdown — a cooperative handler observes
    // aborted + the 'manager stopped' reason and can finish early; stop still resolves once it settles.
    it('stop({ abort: true }) aborts in-flight handlers so they can finish early', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      let observedAborted = false
      let observedReason: unknown
      const job = m.createJob({ jobName: 'ab' }, vi.fn((_input: string, ctx: JobContext) => new Promise<void>((resolve) => {
        ctx.signal.addEventListener('abort', () => {
          observedAborted = ctx.signal.aborted
          observedReason = ctx.signal.reason
          resolve() // cooperative: bail out of the remaining work
        }, { once: true })
      })))
      await m.queue(job, 'r1', 'x')

      m.start(50)
      await vi.advanceTimersByTimeAsync(TICK) // dispatch and start the handler

      await m.stop({ abort: true })
      expect(observedAborted).toBe(true)
      expect(observedReason).toBe('manager stopped')
    })

    // WHY: heartbeat-driven ownership-loss detection — when the guarded heartbeat write is rejected
    // (record staled/superseded under the run), onHeartbeat aborts ctx.signal with the ownership-loss
    // reason and fires NO manager-level heartbeat event for that lost beat.
    it('a heartbeat that detects ownership loss aborts the run and fires no heartbeat event', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 100, keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false,
      })
      const heartbeatEvents = vi.fn()
      m.hook('heartbeat', heartbeatEvents)
      let ctxRef!: JobContext
      const job = m.createJob({ jobName: 'hbloss' }, vi.fn((_i: string, ctx: JobContext) => new Promise<void>((resolve) => {
        ctxRef = ctx
        ctx.signal.addEventListener('abort', () => resolve(), { once: true })
      })))
      await m.queue(job, 'r1', 'x')

      const exec = job.execute('x', { targetGroup: 'test-group', runId: 'r1', heartbeatInterval: 100, manager: m })
      await vi.advanceTimersByTimeAsync(0) // claim settles, handler running

      // Simulate ownership loss: overwrite the record so the guarded heartbeat write is rejected
      // (executionId no longer matches), as maintenance-stale + a successor reclaim would leave it.
      const jobId = 'hbloss#r1'
      const rec = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      rec.status = 'stale'
      rec.executionId = 'someone-else'
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify(rec))

      // Advance one heartbeat interval → onHeartbeat runs, guarded write rejected → abort fires.
      await vi.advanceTimersByTimeAsync(100)
      await exec

      expect(ctxRef.signal.aborted).toBe(true)
      expect(String(ctxRef.signal.reason)).toContain('lost ownership')
      expect(heartbeatEvents).not.toHaveBeenCalled() // no manager heartbeat event for the lost beat
    })
  })

  describe('concurrency validation', () => {
    // WHY: concurrency must be a positive integer — reject 0/-1/NaN/Infinity, floor a fractional value.
    it('rejects non-positive / non-finite concurrency and floors a fractional value', () => {
      expect(() => new RedisJM(redis, 'g', { concurrency: 0 })).toThrow(TypeError)
      expect(() => new RedisJM(redis, 'g', { concurrency: -1 })).toThrow(TypeError)
      expect(() => new RedisJM(redis, 'g', { concurrency: Number.NaN })).toThrow(TypeError)
      expect(() => new RedisJM(redis, 'g', { concurrency: Number.POSITIVE_INFINITY })).toThrow(TypeError)
      expect(new RedisJM(redis, 'g', { concurrency: 2.7 }).getOptions().concurrency).toBe(2)
      expect(new RedisJM(redis, 'g', { concurrency: 1 }).getOptions().concurrency).toBe(1)
    })
  })

  describe('stale maintenance lock recovery', () => {
    it('should reclaim a maintenance lock orphaned by a hard kill mid-run', async () => {
      vi.useFakeTimers()
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
        maintenanceInterval: 500,
      })
      const spy = vi.spyOn(m, 'performMaintenance')

      // Maintenance was killed mid-run: lock held, status 'running', stale heartbeat, NOT in queue.
      // Without proactive reclaim this deadlocks — maintenance can't reclaim its own lock.
      const maintId = '__redisjm_maintenance#'
      await redis.sadd('redisjm:test-group:locks', maintId)
      await redis.hset('redisjm:test-group:log', maintId, JSON.stringify({
        jobId: maintId, jobName: '__redisjm_maintenance', runId: '', inputs: null,
        targetGroup: 'test-group', status: 'running', progress: 0,
        startedAt: Date.now() - 10000, heartbeat: Date.now() - 10000,
      }))

      m.start(100)
      // Drain the async reclaim→enqueue→pop→execute chain over a few poll cycles (deterministic).
      for (let i = 0; i < 4; i++) await vi.advanceTimersByTimeAsync(100)

      // The deadlock is broken: maintenance runs again and the orphaned lock is gone.
      expect(spy).toHaveBeenCalled()
      expect(await redis.sismember('redisjm:test-group:locks', maintId)).toBe(0)

      m.stop()
      vi.useRealTimers()
    })

    it('should reclaim a maintenance lock held with no backing record', async () => {
      // A lock with no log record is unambiguously orphaned (e.g. crash between sadd and hset).
      const m = new RedisJM(redis, 'test-group', { maintenanceInterval: 500 })
      const maintId = '__redisjm_maintenance#'
      await redis.sadd('redisjm:test-group:locks', maintId)
      expect(await redis.sismember('redisjm:test-group:locks', maintId)).toBe(1)

      vi.useFakeTimers()
      m.start(100)
      for (let i = 0; i < 3; i++) await vi.advanceTimersByTimeAsync(100)

      // Lock reclaimed so maintenance can be enqueued and run again.
      expect(await redis.sismember('redisjm:test-group:locks', maintId)).toBe(0)

      m.stop()
      vi.useRealTimers()
    })

    it('should relocate a maintenance job stranded on the legacy queue by a pre-lanes instance', async () => {
      // A 0.0.3 instance enqueued maintenance to the LEGACY default queue and holds the group-wide
      // lock, then the group cut over to lanes. A pure lane worker (below) never polls the legacy
      // key, so without relocation the entry strands there with the lock held and group-wide
      // maintenance deadlocks. reclaimStaleMaintenanceLock must move it onto the __maintenance lane.
      const maintId = '__redisjm_maintenance#'
      await redis.sadd('redisjm:test-group:locks', maintId)
      // 0.0.3-style record: status 'queued', NO `lane` field.
      await redis.hset('redisjm:test-group:log', maintId, JSON.stringify({
        jobId: maintId, jobName: '__redisjm_maintenance', runId: '', inputs: null,
        targetGroup: 'test-group', status: 'queued', progress: 0,
      }))
      await redis.rpush('redisjm:test-group:queue', maintId)

      // Pure lane worker: only a lane:'images' job → never subscribes to the legacy default lane.
      const m = new RedisJM(redis, 'test-group', {
        maintenanceInterval: 500,
        keepFinishedInterval: 60000,
        logger: false,
      })
      m.createJob({ jobName: 'store', lane: 'images' }, vi.fn())
      const spy = vi.spyOn(m, 'performMaintenance')

      vi.useFakeTimers()
      m.start(100)

      // First tick runs the bootstrap reclaim → relocate; assert the mid-state before the entry is
      // popped: it left the legacy queue and now sits on the __maintenance lane (relocated, not dropped).
      await vi.advanceTimersByTimeAsync(0)
      expect(await redis.lpos('redisjm:test-group:queue', maintId)).toBeNull()
      expect(await redis.lpos('redisjm:test-group:lane:__maintenance:queue', maintId)).not.toBeNull()

      // Drain the relocate→pop→execute chain over a few more poll cycles (deterministic). Stay
      // below the 500ms maintenanceInterval so the end-state lock reflects the reclaimed run, not a
      // fresh periodic re-enqueue.
      for (let i = 0; i < 3; i++) await vi.advanceTimersByTimeAsync(100)

      // The deadlock is broken: maintenance ran and released the lock (the entry already left the
      // legacy queue at the mid-state assert above, and nothing re-pushes it there).
      expect(spy).toHaveBeenCalled()
      expect(await redis.sismember('redisjm:test-group:locks', maintId)).toBe(0)

      m.stop()
      vi.useRealTimers()
    })
  })

  describe('observability', () => {
    it('should report a thrown handler to the logger by default', async () => {
      const logger = vi.fn()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger })
      m.createJob({ jobName: 'boom' }, vi.fn(() => { throw new Error('kaboom') }))
      await m.queue(new Job({ jobName: 'boom' }, vi.fn()), 'r1', 'x')

      await m.popAndExecute()

      expect(logger).toHaveBeenCalled()
      const [message, error] = logger.mock.calls[0]
      expect(message).toContain('boom#r1')
      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toBe('kaboom')
    })

    it('should report a missing log record to the logger', async () => {
      const logger = vi.fn()
      const m = new RedisJM(redis, 'test-group', { logger })
      m.createJob({ jobName: 'ghost' }, vi.fn())
      // Queue entry with no backing log record.
      await redis.sadd('redisjm:test-group:locks', 'ghost#r1')
      await redis.rpush('redisjm:test-group:queue', 'ghost#r1')

      expect(await m.popAndExecute()).toBe(true)
      expect(logger).toHaveBeenCalled()
      expect(logger.mock.calls[0][0]).toContain('ghost#r1')
      expect(await m.isQueued('ghost#r1')).toBe(false)
    })
  })

  describe('enqueue failure rollback', () => {
    it('should roll back both lock and log when the queue push fails', async () => {
      const job = new Job({ jobName: 'rollback' }, vi.fn())
      ;(redis.rpush as any).mockImplementationOnce(async () => { throw new Error('redis down') })

      await expect(manager.queue(job, 'r1', 'x')).rejects.toThrow('redis down')

      // Neither the lock nor the log record should survive a failed enqueue.
      expect(await manager.isQueued('rollback#r1')).toBe(false)
      expect(await redis.hget('redisjm:test-group:log', 'rollback#r1')).toBeNull()
    })
  })

  describe('heartbeat guard (onHeartbeat)', () => {
    it('should not refresh the heartbeat of a record that already left running', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
      const job = m.createJob({ jobName: 'hb' }, vi.fn())
      const jobId = 'hb#r1'
      const oldHeartbeat = Date.now() - 50000
      // A terminal record that a straggling heartbeat must not resurrect.
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'hb', runId: 'r1', inputs: null,
        targetGroup: 'test-group', status: 'stale', progress: 0,
        finishedAt: Date.now() - 50000, heartbeat: oldHeartbeat,
      }))

      // Fire the manager's heartbeat hook for this run (as a late/leaked timer would). The guarded
      // write is rejected (record already left running), so onHeartbeat now calls payload.abort — pass
      // a no-op abort since this hand-built payload has no real execution behind it.
      await job.callHook('heartbeat', { job, targetGroup: 'test-group', runId: 'r1', inputs: null, executionId: 'x', abort: () => {} })

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('stale')
      expect(record.heartbeat).toBe(oldHeartbeat) // unchanged — not refreshed
    })
  })

  describe('get after finish', () => {
    it('should return undefined for a finished run under keepFinishedInterval=0', async () => {
      const job = manager.createJob({ jobName: 'fin' }, vi.fn())
      await manager.queue(job, 'r1', 'x')
      await job.execute('x', { targetGroup: 'test-group', runId: 'r1' })
      // Default keepFinishedInterval:0 deletes the record on finish.
      expect(await manager.get('fin#r1')).toBeUndefined()
    })
  })

  describe('lanes (write side)', () => {
    it('should enqueue a laned job to its lane queue and NOT the default queue, stamping lane on the record', async () => {
      const job = new Job({ jobName: 'store-images', lane: 'images' }, vi.fn())
      await manager.queue(job, 'run1', { url: 'x' })

      // Routed to the named lane queue, absent from the legacy queue.
      expect(await redis.lpos('redisjm:test-group:lane:images:queue', 'store-images#run1')).not.toBeNull()
      expect(await redis.lpos('redisjm:test-group:queue', 'store-images#run1')).toBeNull()

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'store-images#run1'))!) as JobLogRecord
      expect(record.lane).toBe('images')
    })

    it('should keep a no-lane job on the legacy queue key with no "lane" field in the stored record (0.0.3 byte-for-byte)', async () => {
      const job = new Job({ jobName: 'plain' }, vi.fn())
      await manager.queue(job, 'run1', 'input')

      expect(await redis.lpos('redisjm:test-group:queue', 'plain#run1')).not.toBeNull()
      // Assert on the RAW stored string: a default-lane record must serialize with no `"lane"` key.
      const raw = (await redis.hget('redisjm:test-group:log', 'plain#run1'))!
      expect(raw).not.toContain('"lane"')
    })

    it('should treat lane "default" as an alias for the legacy queue key (no lane:default:queue key)', async () => {
      const job = new Job({ jobName: 'aliased', lane: 'default' }, vi.fn())
      await manager.queue(job, 'run1', 'input')

      expect(await redis.lpos('redisjm:test-group:queue', 'aliased#run1')).not.toBeNull()
      expect(await redis.lpos('redisjm:test-group:lane:default:queue', 'aliased#run1')).toBeNull()
    })

    it('should reject a targetGroup containing ":" (lane-infix collision-proofing)', () => {
      expect(() => new RedisJM(redis, 'a:b:c')).toThrow()
    })

    it('should reject invalid lane names on enqueue and registerJob, and accept a valid one', async () => {
      // Producer path (enqueue) is authoritative — validates before taking the lock.
      await expect(manager.queue(new Job({ jobName: 'j1', lane: 'bad:lane' }, vi.fn()), 'r', 'x')).rejects.toThrow()
      await expect(manager.queue(new Job({ jobName: 'j2', lane: '__nope' }, vi.fn()), 'r', 'x')).rejects.toThrow()
      expect(await manager.queue(new Job({ jobName: 'j3', lane: 'images-1' }, vi.fn()), 'r', 'x')).toBe(true)

      // registerJob validates too (early feedback).
      expect(() => manager.registerJob(new Job({ jobName: 'reg1', lane: 'bad#lane' }, vi.fn()))).toThrow()
      expect(() => manager.registerJob(new Job({ jobName: 'reg2', lane: '__reserved' }, vi.fn()))).toThrow()
      expect(() => manager.registerJob(new Job({ jobName: 'reg3', lane: 'images-1' }, vi.fn()))).not.toThrow()
    })

    it('should exempt the internal maintenance job from the "__" lane reservation', () => {
      // The built-in maintenance job owns the reserved `__maintenance` lane (assigned in a later
      // step); validation must let it through by jobName even though `__` is otherwise reserved.
      const job = new Job({ jobName: MAINTENANCE_JOB_NAME, lane: '__maintenance' }, vi.fn())
      expect(() => manager.registerJob(job)).not.toThrow()
    })

    it('should unqueue a laned job from its lane queue, locks, and log', async () => {
      const job = new Job({ jobName: 'store-images', lane: 'images' }, vi.fn())
      await manager.queue(job, 'run1', 'x')
      expect(await redis.lpos('redisjm:test-group:lane:images:queue', 'store-images#run1')).not.toBeNull()

      await manager.unqueue('store-images#run1')
      expect(await redis.lpos('redisjm:test-group:lane:images:queue', 'store-images#run1')).toBeNull()
      expect(await manager.isQueued('store-images#run1')).toBe(false)
      expect(await redis.hget('redisjm:test-group:log', 'store-images#run1')).toBeNull()
    })

    it('should LPOS the record lane during maintenance so a queued laned job is not false-orphaned', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
      const job = new Job({ jobName: 'store-images', lane: 'images' }, vi.fn())
      await m.queue(job, 'run1', null)

      // The entry sits in the images lane queue; maintenance must LPOS THAT lane, else it would
      // false-orphan the still-queued job. Run twice (the orphan check is two-pass).
      expect((await m.performMaintenance()).staleCount).toBe(0)
      expect((await m.performMaintenance()).staleCount).toBe(0)

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'store-images#run1'))!) as JobLogRecord
      expect(record.status).toBe('queued')
      expect(record.suspectedAt).toBeUndefined()
    })
  })

  describe('lanes (consumption)', () => {
    it('routing isolation: never pops a job on an unsubscribed lane (no requeue, no drop)', async () => {
      // Manager subscribes only to lane A (its one registered job). A lane-B job is enqueued.
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      m.createJob({ jobName: 'a-job', lane: 'A' }, vi.fn())
      await m.queue(new Job({ jobName: 'b-job', lane: 'B' }, vi.fn()), 'r1', 'x')

      // The §3 regression, asserted directly: nothing is popped from B.
      expect(await m.popAndExecute()).toBe(false)
      expect(await redis.lpos('redisjm:test-group:lane:B:queue', 'b-job#r1')).not.toBeNull()
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'b-job#r1'))!) as JobLogRecord
      expect(record.status).toBe('queued')
      expect(record.requeueCount).toBeUndefined()
    })

    it('auto-subscription: pops the subscribed lane, leaves others; default-lane manager serves the legacy key', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const aFn = vi.fn()
      const aJob = m.createJob({ jobName: 'a-job', lane: 'A' }, aFn)
      await m.queue(aJob, 'r1', 'x')
      await m.queue(new Job({ jobName: 'b-job', lane: 'B' }, vi.fn()), 'r1', 'y')

      expect(await m.popAndExecute()).toBe(true)
      expect(aFn).toHaveBeenCalledWith('x', expect.any(Object))
      // The B-lane job is untouched.
      expect(await redis.lpos('redisjm:test-group:lane:B:queue', 'b-job#r1')).not.toBeNull()

      // A default-lane manager pops a default-lane job off the legacy key.
      const def = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const defFn = vi.fn()
      const defJob = def.createJob({ jobName: 'plain' }, defFn)
      await def.queue(defJob, 'r1', 'z')
      expect(await redis.lpos('redisjm:test-group:queue', 'plain#r1')).not.toBeNull()
      expect(await def.popAndExecute()).toBe(true)
      expect(defFn).toHaveBeenCalledWith('z', expect.any(Object))
    })

    it('roundRobin: both lanes drain and pops interleave (no starvation)', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const order: string[] = []
      const aJob = m.createJob({ jobName: 'a-job', lane: 'A' }, vi.fn(async () => { order.push('A') }))
      const bJob = m.createJob({ jobName: 'b-job', lane: 'B' }, vi.fn(async () => { order.push('B') }))
      for (const r of ['r1', 'r2', 'r3']) await m.queue(aJob, r, 'x')
      for (const r of ['r1', 'r2', 'r3']) await m.queue(bJob, r, 'x')

      for (let i = 0; i < 6; i++) expect(await m.popAndExecute()).toBe(true)

      // Both lanes drained, and the per-poll rotation strictly interleaves them (not lane-at-a-time).
      expect(order.filter((l) => l === 'A')).toHaveLength(3)
      expect(order.filter((l) => l === 'B')).toHaveLength(3)
      expect(order).toEqual(['A', 'B', 'A', 'B', 'A', 'B'])
    })

    it('priority: drains lanes in lanePriority order (all A before any B)', async () => {
      const m = new RedisJM(redis, 'test-group', {
        keepFinishedInterval: 60000,
        logger: false,
        laneStrategy: 'priority',
        lanePriority: ['A', 'B'],
      })
      const order: string[] = []
      const aJob = m.createJob({ jobName: 'a-job', lane: 'A' }, vi.fn(async () => { order.push('A') }))
      const bJob = m.createJob({ jobName: 'b-job', lane: 'B' }, vi.fn(async () => { order.push('B') }))
      // Interleave the enqueues to prove ordering is by lane priority, not insertion order.
      await m.queue(bJob, 'r1', 'x')
      await m.queue(aJob, 'r1', 'x')
      await m.queue(bJob, 'r2', 'x')
      await m.queue(aJob, 'r2', 'x')

      for (let i = 0; i < 4; i++) expect(await m.popAndExecute()).toBe(true)
      expect(order).toEqual(['A', 'A', 'B', 'B'])
    })

    it('priority: lanes absent from lanePriority trail in registration order', async () => {
      const m = new RedisJM(redis, 'test-group', {
        keepFinishedInterval: 60000,
        logger: false,
        laneStrategy: 'priority',
        lanePriority: ['A'], // only A is listed; C and D must trail, in registration order
      })
      const order: string[] = []
      // Register C before D so the unlisted tail is C then D; listed A must still lead.
      const cJob = m.createJob({ jobName: 'c-job', lane: 'C' }, vi.fn(async () => { order.push('C') }))
      const dJob = m.createJob({ jobName: 'd-job', lane: 'D' }, vi.fn(async () => { order.push('D') }))
      const aJob = m.createJob({ jobName: 'a-job', lane: 'A' }, vi.fn(async () => { order.push('A') }))
      // Scrambled enqueue order to prove poll order comes from the strategy, not insertion order.
      await m.queue(dJob, 'r1', 'x')
      await m.queue(aJob, 'r1', 'x')
      await m.queue(cJob, 'r1', 'x')

      for (let i = 0; i < 3; i++) expect(await m.popAndExecute()).toBe(true)
      expect(order).toEqual(['A', 'C', 'D'])
    })

    it('__maintenance is polled first even under priority with a saturated work lane', async () => {
      const m = new RedisJM(redis, 'test-group', {
        keepFinishedInterval: 60000,
        logger: false,
        laneStrategy: 'priority',
        lanePriority: ['A'],
      })
      const aFn = vi.fn()
      const aJob = m.createJob({ jobName: 'a-job', lane: 'A' }, aFn)
      await m.queue(aJob, 'r1', 'x')
      await m.queue(aJob, 'r2', 'x')

      // A maintenance run lands on the reserved lane; the work lane A is non-empty and higher-listed.
      const maint = createMaintenanceJob(m)
      const maintSpy = vi.spyOn(m, 'performMaintenance')
      await maint.queue('', null)

      expect(await m.popAndExecute()).toBe(true)
      // The __maintenance entry was consumed FIRST, not the saturated A lane.
      expect(maintSpy).toHaveBeenCalledTimes(1)
      expect(aFn).not.toHaveBeenCalled()
      expect(await redis.lpos('redisjm:test-group:lane:__maintenance:queue', `${MAINTENANCE_JOB_NAME}#`)).toBeNull()
      expect(await redis.lpos('redisjm:test-group:lane:A:queue', 'a-job#r1')).not.toBeNull()
    })

    it('requeue stays on its lane end-to-end: a same-lane sibling then claims it (§5.5)', async () => {
      // X subscribes to `images` (registers `store`) but has no handler for a different same-lane
      // jobName `thumb` that gets enqueued on `images`.
      const x = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      x.createJob({ jobName: 'store', lane: 'images' }, vi.fn())
      await x.queue(new Job({ jobName: 'thumb', lane: 'images' }, vi.fn()), 'r1', 'payload')

      // X pops `thumb`, has no handler → requeues it back onto the images lane (deferred → false).
      expect(await x.popAndExecute()).toBe(false)
      expect(await redis.lpos('redisjm:test-group:lane:images:queue', 'thumb#r1')).not.toBeNull()
      expect(await redis.lpos('redisjm:test-group:queue', 'thumb#r1')).toBeNull()
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'thumb#r1'))!) as JobLogRecord
      expect(record.requeueCount).toBe(1)
      expect(record.lane).toBe('images')

      // Y registers `thumb` on `images` and pops + executes the requeued run from its lane.
      const y = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const thumbFn = vi.fn()
      y.createJob({ jobName: 'thumb', lane: 'images' }, thumbFn)
      expect(await y.popAndExecute()).toBe(true)
      expect(thumbFn).toHaveBeenCalledWith('payload', expect.any(Object))
    })

    it('maintenance runs on a pure worker and reclaims a default-lane orphan it never polls', async () => {
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
        logger: false,
      })
      // Pure image worker: only a lane:'images' job → never subscribes to the default work lane.
      m.createJob({ jobName: 'store', lane: 'images' }, vi.fn())

      // A stale `running` DEFAULT-lane record + lock left by a crashed instance.
      const jobId = 'crashed#run1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'crashed', runId: 'run1', inputs: null,
        targetGroup: 'test-group', status: 'running', progress: 0,
        startedAt: Date.now() - 10000, heartbeat: Date.now() - 10000,
      }))

      // Maintenance lands on the reserved lane, which every instance polls (§5.3).
      const maint = createMaintenanceJob(m)
      await maint.queue('', null)
      expect(await m.popAndExecute()).toBe(true)

      // The default-lane orphan was reclaimed even though this worker never polls the default lane.
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('stale')
      expect(await redis.sismember('redisjm:test-group:locks', jobId)).toBe(0)
    })

    it('sequential-LPOP fallback: same routing + roundRobin behavior when LMPOP is unavailable', async () => {
      // Simulate Redis < 7: LMPOP is an unknown command, forcing the sequential-LPOP fallback. The
      // manager probes once, catches the unknown-command error, and switches to LPOP permanently.
      ;(redis.lmpop as any).mockRejectedValue(new Error("ERR unknown command 'LMPOP'"))

      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false })
      const order: string[] = []
      const aJob = m.createJob({ jobName: 'a-job', lane: 'A' }, vi.fn(async () => { order.push('A') }))
      const bJob = m.createJob({ jobName: 'b-job', lane: 'B' }, vi.fn(async () => { order.push('B') }))
      // Routing isolation: a lane-C job (unsubscribed) must never be popped via the fallback either.
      await m.queue(new Job({ jobName: 'c-job', lane: 'C' }, vi.fn()), 'r1', 'x')
      for (const r of ['r1', 'r2', 'r3']) await m.queue(aJob, r, 'x')
      for (const r of ['r1', 'r2', 'r3']) await m.queue(bJob, r, 'x')

      for (let i = 0; i < 6; i++) expect(await m.popAndExecute()).toBe(true)

      // Identical fairness (interleaved) and routing (C untouched) via the fallback path.
      expect(order).toEqual(['A', 'B', 'A', 'B', 'A', 'B'])
      expect(await redis.lpos('redisjm:test-group:lane:C:queue', 'c-job#r1')).not.toBeNull()
      // LMPOP was probed exactly once, then abandoned for LPOP.
      expect((redis.lmpop as any).mock.calls.length).toBe(1)
    })
  })

  describe('execution fencing', () => {
    it('zombie finish cannot clobber a successor that reclaimed the same runId', async () => {
      // WHY: the deepest correctness flaw — a stalled handler is staled + unlocked by maintenance, a
      // producer re-queues the same runId, then the original "zombie" finishes. Its finish must NOT
      // overwrite/srem/delete the successor's freshly-queued record (which would silently drop it).
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false, maintenanceInterval: 0 })
      let release!: () => void
      const gate = new Promise<void>((r) => { release = r })
      const job = m.createJob({ jobName: 'z' }, vi.fn(async () => { await gate }))
      const managerFinish = vi.fn()
      m.hook('finish', managerFinish)

      await m.queue(job, 'r1', 'x')
      const zombie = m.popAndExecute() // claims the record (running + executionId), then blocks on the gate
      await new Promise((r) => setTimeout(r, 0))

      // Maintenance stales the zombie: mark the record stale and release its lock (as performMaintenance would).
      const jobId = 'z#r1'
      const staled = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      staled.status = 'stale'
      staled.finishedAt = Date.now()
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify(staled))
      await redis.srem('redisjm:test-group:locks', jobId)

      // Producer re-enqueues the same runId → fresh `queued` record (no executionId) + new lock.
      expect(await m.queue(job, 'r1', 'x2')).toBe(true)

      // Now let the zombie finish; its finish hook must be fenced out.
      release()
      await zombie

      const successor = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(successor.status).toBe('queued')
      expect(successor.executionId).toBeUndefined()
      expect(await m.isQueued(jobId)).toBe(true) // successor's lock survived
      expect(managerFinish).not.toHaveBeenCalled() // no finish event for the zombie
    })

    it('zombie error cannot clobber a successor that reclaimed the same runId', async () => {
      // WHY: same fence, but for a throwing zombie — its error hook must not flip/srem/delete the
      // successor's record either.
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false, maintenanceInterval: 0 })
      let release!: () => void
      const gate = new Promise<void>((r) => { release = r })
      const job = m.createJob({ jobName: 'ze' }, vi.fn(async () => { await gate; throw new Error('late boom') }))
      const managerError = vi.fn()
      m.hook('error', managerError)

      await m.queue(job, 'r1', 'x')
      const zombie = m.popAndExecute()
      await new Promise((r) => setTimeout(r, 0))

      const jobId = 'ze#r1'
      const staled = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      staled.status = 'stale'
      staled.finishedAt = Date.now()
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify(staled))
      await redis.srem('redisjm:test-group:locks', jobId)
      expect(await m.queue(job, 'r1', 'x2')).toBe(true)

      release()
      await zombie // popAndExecute swallows the thrown handler error internally

      const successor = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(successor.status).toBe('queued')
      expect(successor.executionId).toBeUndefined()
      expect(await m.isQueued(jobId)).toBe(true)
      expect(managerError).not.toHaveBeenCalled()
    })

    it('rejects a claim on an already-running record and skips without touching lock/log', async () => {
      // WHY: a popped entry whose record is already `running` (a concurrent claimant owns it) must be
      // skipped as superseded — the handler never runs and the owner's lock/record are left intact.
      const logger = vi.fn()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger, maintenanceInterval: 0 })
      const fn = vi.fn()
      m.createJob({ jobName: 'c' }, fn)
      const jobId = 'c#r1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.rpush('redisjm:test-group:queue', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'c', runId: 'r1', inputs: 'x',
        targetGroup: 'test-group', status: 'running', progress: 0,
        executionId: 'someone-else', startedAt: Date.now(), heartbeat: Date.now(),
      }))

      expect(await m.popAndExecute()).toBe(true)
      expect(fn).not.toHaveBeenCalled()
      expect(await m.isQueued(jobId)).toBe(true)
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(record.status).toBe('running')
      expect(record.executionId).toBe('someone-else')
      expect(logger.mock.calls.some(([msg]) => /supersed/i.test(msg as string))).toBe(true)
    })

    it('a throwing manager finish hook does not flip a finished run to error', async () => {
      // WHY: a user finish hook that throws is infra, not job outcome — the run stays `finished`, the
      // `error` hook never fires, and the failure is reported via the logger.
      const logger = vi.fn()
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger, maintenanceInterval: 0 })
      const job = m.createJob({ jobName: 'f' }, vi.fn())
      m.hook('finish', () => { throw new Error('finish hook boom') })
      const onError = vi.fn()
      m.hook('error', onError)

      await m.queue(job, 'r1', 'x')
      expect(await m.popAndExecute()).toBe(true)

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'f#r1'))!) as JobLogRecord
      expect(record.status).toBe('finished')
      expect(onError).not.toHaveBeenCalled()
      expect(logger.mock.calls.some(([, err]) => err instanceof Error && /finish hook boom/.test(err.message))).toBe(true)
    })

    it('error hook still fires on a job-function throw', async () => {
      // WHY: guard against over-fencing — a genuine fn failure must still record `error` and dispatch
      // the manager `error` event.
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false, maintenanceInterval: 0 })
      const job = m.createJob({ jobName: 'boom' }, vi.fn(() => { throw new Error('kaboom') }))
      const onError = vi.fn()
      m.hook('error', onError)

      await m.queue(job, 'r1', 'x')
      expect(await m.popAndExecute()).toBe(true)

      expect(onError).toHaveBeenCalledWith(expect.objectContaining({ error: expect.any(Error) }))
      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'boom#r1'))!) as JobLogRecord
      expect(record.status).toBe('error')
      expect(record.error).toBe('kaboom')
    })

    it('setAttrs merges keys across calls instead of replacing the attrs object', async () => {
      // WHY: setAttrs used to REPLACE the whole attrs object, silently dropping earlier keys.
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0 })
      const fn = vi.fn(async (_i: string, ctx: JobContext<{ a?: string; b?: string }>) => {
        await ctx.setAttrs({ a: '1' })
        await ctx.setAttrs({ b: '2' })
      })
      const job = m.createJob<string, { a?: string; b?: string }>({ jobName: 'merge' }, fn)
      await m.queue(job, 'r1', 'x')
      await job.execute('x', { targetGroup: 'test-group', runId: 'r1', manager: m })

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'merge#r1'))!) as JobLogRecord
      expect(record.attrs).toEqual({ a: '1', b: '2' })
    })

    it('only the driving manager reacts when two managers share a Job and targetGroup', async () => {
      // WHY: two managers in one process sharing a Job + targetGroup both used to react to every event
      // (double writes/events). The `manager` fencing token makes only the driver act.
      const a = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false, maintenanceInterval: 0 })
      const b = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false, maintenanceInterval: 0 })
      const job = new Job({ jobName: 'shared' }, vi.fn(async (_i: string, ctx: JobContext) => { await ctx.setProgress(0.5) }))
      a.registerJob(job)
      b.registerJob(job)
      const onStartB = vi.fn()
      const onUpdateB = vi.fn()
      const onFinishB = vi.fn()
      b.hook('start', onStartB)
      b.hook('update', onUpdateB)
      b.hook('finish', onFinishB)

      await a.queue(job, 'r1', 'x')
      const before = (redis.hset as any).mock.calls.length
      expect(await a.popAndExecute()).toBe(true)
      const writes = (redis.hset as any).mock.calls.length - before

      expect(onStartB).not.toHaveBeenCalled()
      expect(onUpdateB).not.toHaveBeenCalled()
      expect(onFinishB).not.toHaveBeenCalled()
      // Exactly three log writes (claim + one progress update + finish), each performed once — not
      // doubled by manager B reacting to the same events.
      expect(writes).toBe(3)
    })

    it('reports a failed heartbeat write to the logger and still completes the job', async () => {
      // WHY: heartbeat write failures used to be swallowed by `.catch(() => {})`; they must reach the
      // logger, and one failed heartbeat must not sink an otherwise healthy run.
      vi.useFakeTimers()
      const logger = vi.fn()
      const m = new RedisJM(redis, 'test-group', {
        heartbeatInterval: 100, keepFinishedInterval: 60000, logger, maintenanceInterval: 0,
      })
      const job = m.createJob({ jobName: 'hb' }, vi.fn(async () => { await new Promise((r) => setTimeout(r, 250)) }))
      await m.queue(job, 'r1', 'x')

      const exec = job.execute('x', { targetGroup: 'test-group', runId: 'r1', heartbeatInterval: 100, manager: m, logger })
      await vi.advanceTimersByTimeAsync(0) // let the claim settle

      // Fail exactly the next hset — the first heartbeat write.
      ;(redis.hset as any).mockImplementationOnce(async () => { throw new Error('redis unavailable') })
      await vi.advanceTimersByTimeAsync(100)
      expect(logger.mock.calls.some(([msg]) => /heartbeat/i.test(msg as string))).toBe(true)

      await vi.advanceTimersByTimeAsync(200) // drive the handler to completion
      await exec

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'hb#r1'))!) as JobLogRecord
      expect(record.status).toBe('finished')
      vi.useRealTimers()
    })
  })

  describe('delayed enqueue & promotion', () => {
    afterEach(() => { vi.useRealTimers() })

    // WHY: a delayed run must persist as `delayed` (holding the lock so dedupe still applies), sit on
    // the delayed zset (not the live queue), be non-poppable until due, then promote+execute on the
    // first poll after its readyAt — proving the promotion rate-limit doesn't starve it across a >1s gap.
    it('stages a delayed run, holds the lock, and promotes it once due', async () => {
      vi.useFakeTimers()
      const base = 1_700_000_000_000
      vi.setSystemTime(base)
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const fn = vi.fn()
      const job = m.createJob({ jobName: 'delayed' }, fn)

      expect(await m.queue(job, 'run1', { k: 'v' }, { delay: 5000 })).toBe(true)

      const record = JSON.parse((await redis.hget('redisjm:test-group:log', 'delayed#run1'))!) as JobLogRecord
      expect(record.status).toBe('delayed')
      expect(record.readyAt).toBe(base + 5000)
      expect(await m.isQueued('delayed#run1')).toBe(true)
      expect(await m.queue(job, 'run1', { k: 'v' })).toBe(false) // dedupe holds while waiting
      expect(await redis.zscore('redisjm:test-group:delayed', 'delayed#run1')).not.toBeNull()
      expect(await redis.lpos('redisjm:test-group:queue', 'delayed#run1')).toBeNull()

      // Not yet due → not poppable.
      expect(await m.popAndExecute()).toBe(false)
      expect(fn).not.toHaveBeenCalled()

      // Advance well past readyAt (and past the 1000ms promotion gap) → promoted and executed.
      vi.setSystemTime(base + 6000)
      expect(await m.popAndExecute()).toBe(true)
      expect(fn).toHaveBeenCalledWith({ k: 'v' }, expect.any(Object))
      expect(await redis.zscore('redisjm:test-group:delayed', 'delayed#run1')).toBeNull()
      expect(await m.isQueued('delayed#run1')).toBe(false)
    })

    // WHY: queueFirst + delay is contradictory (no "front" of a time-ordered set), and delay must be a
    // finite number ≥ 0; both are producer-input errors that must throw TypeError before taking the lock.
    it('rejects queueFirst+delay and invalid delay values with TypeError', async () => {
      const m = new RedisJM(redis, 'test-group', { logger: false })
      const job = new Job({ jobName: 'd' }, vi.fn())
      await expect(m.queueFirst(job, 'r1', 'x', { delay: 1000 })).rejects.toThrow(TypeError)
      await expect(m.queue(job, 'r2', 'x', { delay: -1 })).rejects.toThrow(TypeError)
      await expect(m.queue(job, 'r3', 'x', { delay: Number.NaN })).rejects.toThrow(TypeError)
      await expect(m.queue(job, 'r4', 'x', { delay: Number.POSITIVE_INFINITY })).rejects.toThrow(TypeError)
      // delay: 0 is valid and behaves like a normal (immediate) queue.
      expect(await m.queue(job, 'r5', 'x', { delay: 0 })).toBe(true)
      expect(await redis.lpos('redisjm:test-group:queue', 'd#r5')).not.toBeNull()
      // A rejected delayed enqueue must not leak a lock.
      expect(await m.isQueued('d#r1')).toBe(false)
    })
  })

  describe('retries', () => {
    afterEach(() => { vi.useRealTimers() })

    // WHY: the core retry story — a handler that fails twice then succeeds must schedule two backoff
    // retries (manager 'retry' fired with attempts 1 & 2 and the correct nextAttemptAt), hold the lock
    // across each backoff window, NEVER fire manager 'error', and end finished with record.attempt === 3.
    it('retries a failing handler through backoff, then succeeds, without firing error', async () => {
      vi.useFakeTimers()
      const base = 1_700_000_000_000
      vi.setSystemTime(base)
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      let calls = 0
      const job = m.createJob({ jobName: 'retry', attempts: 3, backoff: 1000 }, vi.fn(async () => {
        calls++
        if (calls < 3) throw new Error(`boom ${calls}`)
      }))
      const retrySpy = vi.fn()
      const errorSpy = vi.fn()
      m.hook('retry', retrySpy)
      m.hook('error', errorSpy)

      await m.queue(job, 'r1', 'x')

      // Attempt 1 fails → retry #1 scheduled at now + backoff; lock HELD through the backoff window.
      expect(await m.popAndExecute()).toBe(true)
      expect(retrySpy).toHaveBeenCalledTimes(1)
      expect(retrySpy).toHaveBeenLastCalledWith(
        expect.objectContaining({ attempt: 1, nextAttemptAt: base + 1000, error: expect.any(Error) }),
      )
      let rec = JSON.parse((await redis.hget('redisjm:test-group:log', 'retry#r1'))!) as JobLogRecord
      expect(rec.status).toBe('delayed')
      expect(rec.attempt).toBe(1)
      expect(await m.queue(job, 'r1', 'x')).toBe(false) // dedupe holds during backoff

      // Attempt 2 fails → retry #2; nextAttemptAt reflects the (advanced) now + backoff.
      vi.setSystemTime(base + 1500)
      expect(await m.popAndExecute()).toBe(true)
      expect(retrySpy).toHaveBeenCalledTimes(2)
      expect(retrySpy).toHaveBeenLastCalledWith(expect.objectContaining({ attempt: 2, nextAttemptAt: base + 2500 }))
      expect(await m.queue(job, 'r1', 'x')).toBe(false)

      // Attempt 3 succeeds → finished, no error event ever, attempt reached 3, lock released.
      vi.setSystemTime(base + 4000)
      expect(await m.popAndExecute()).toBe(true)
      expect(calls).toBe(3)
      expect(retrySpy).toHaveBeenCalledTimes(2)
      expect(errorSpy).not.toHaveBeenCalled()
      rec = JSON.parse((await redis.hget('redisjm:test-group:log', 'retry#r1'))!) as JobLogRecord
      expect(rec.status).toBe('finished')
      expect(rec.attempt).toBe(3)
      expect(await m.isQueued('retry#r1')).toBe(false)
    })

    // WHY: with attempts:2 an always-failing handler retries once, then on the final attempt fires the
    // manager 'error' event exactly once with the LAST error and leaves the record status 'error'.
    it('fires error only on final failure after exhausting attempts', async () => {
      vi.useFakeTimers()
      const base = 1_700_000_000_000
      vi.setSystemTime(base)
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      let calls = 0
      const job = m.createJob({ jobName: 'fail', attempts: 2, backoff: 1000 }, vi.fn(async () => {
        calls++
        throw new Error(`boom ${calls}`)
      }))
      const retrySpy = vi.fn()
      const errorSpy = vi.fn()
      m.hook('retry', retrySpy)
      m.hook('error', errorSpy)

      await m.queue(job, 'r1', 'x')

      expect(await m.popAndExecute()).toBe(true) // attempt 1 → retry, no error yet
      expect(retrySpy).toHaveBeenCalledTimes(1)
      expect(errorSpy).not.toHaveBeenCalled()

      vi.setSystemTime(base + 1500)
      expect(await m.popAndExecute()).toBe(true) // attempt 2 → final failure
      expect(retrySpy).toHaveBeenCalledTimes(1)
      expect(errorSpy).toHaveBeenCalledTimes(1)
      expect(errorSpy).toHaveBeenCalledWith(expect.objectContaining({ error: expect.objectContaining({ message: 'boom 2' }) }))
      const rec = JSON.parse((await redis.hget('redisjm:test-group:log', 'fail#r1'))!) as JobLogRecord
      expect(rec.status).toBe('error')
      expect(rec.error).toBe('boom 2')
      expect(await m.isQueued('fail#r1')).toBe(false)
    })

    // WHY: regression guard for legacy behavior — the default attempts:1 must make a single failure
    // immediately final (manager 'error', no 'retry', no delayed entry).
    it('treats attempts:1 (default) failure as immediately final', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 'once' }, vi.fn(() => { throw new Error('nope') }))
      const retrySpy = vi.fn()
      const errorSpy = vi.fn()
      m.hook('retry', retrySpy)
      m.hook('error', errorSpy)

      await m.queue(job, 'r1', 'x')
      expect(await m.popAndExecute()).toBe(true)

      expect(retrySpy).not.toHaveBeenCalled()
      expect(errorSpy).toHaveBeenCalledTimes(1)
      const rec = JSON.parse((await redis.hget('redisjm:test-group:log', 'once#r1'))!) as JobLogRecord
      expect(rec.status).toBe('error')
      expect(await redis.zscore('redisjm:test-group:delayed', 'once#r1')).toBeNull()
      expect(await m.isQueued('once#r1')).toBe(false)
    })

    // WHY: a superseded (zombie) execution's failure must be fenced by executionId — it schedules NO
    // retry, fires NO events, and leaves the successor's freshly-queued record and lock untouched.
    it('a superseded (zombie) failure schedules no retry and fires no events', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, logger: false, maintenanceInterval: 0 })
      let release!: () => void
      const gate = new Promise<void>((r) => { release = r })
      const job = m.createJob({ jobName: 'zr', attempts: 3, backoff: 1000 }, vi.fn(async () => { await gate; throw new Error('late boom') }))
      const retrySpy = vi.fn()
      const errorSpy = vi.fn()
      m.hook('retry', retrySpy)
      m.hook('error', errorSpy)

      await m.queue(job, 'r1', 'x')
      const zombie = m.popAndExecute() // claims the record, then blocks on the gate
      await new Promise((r) => setTimeout(r, 0))

      // Maintenance-style stale + unlock, then a successor re-enqueues the same runId (fresh record).
      const jobId = 'zr#r1'
      const staled = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      staled.status = 'stale'
      staled.finishedAt = Date.now()
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify(staled))
      await redis.srem('redisjm:test-group:locks', jobId)
      expect(await m.queue(job, 'r1', 'x2')).toBe(true)

      release()
      await zombie // popAndExecute swallows the thrown handler error internally

      expect(retrySpy).not.toHaveBeenCalled()
      expect(errorSpy).not.toHaveBeenCalled()
      const successor = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(successor.status).toBe('queued')
      expect(successor.executionId).toBeUndefined()
      expect(await redis.zscore('redisjm:test-group:delayed', jobId)).toBeNull() // no phantom retry entry
      expect(await m.isQueued(jobId)).toBe(true)
    })
  })

  describe('maintenance & delayed', () => {
    // WHY: an overdue-but-present delayed entry is owned by promotion, not maintenance — maintenance
    // must leave it (and its lock) untouched even across repeated passes.
    it('leaves a delayed record present on the zset untouched, even when overdue', async () => {
      const m = new RedisJM(redis, 'test-group', { heartbeatInterval: 1000, roundsToStale: 2, keepFinishedInterval: 60000, logger: false })
      const jobId = 'dm#r1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.zadd('redisjm:test-group:delayed', Date.now() - 10000, jobId) // overdue but present
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'dm', runId: 'r1', inputs: null, targetGroup: 'test-group',
        status: 'delayed', progress: 0, readyAt: Date.now() - 10000,
      }))

      expect((await m.performMaintenance()).staleCount).toBe(0)
      expect((await m.performMaintenance()).staleCount).toBe(0)
      const rec = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(rec.status).toBe('delayed')
      expect(rec.suspectedAt).toBeUndefined()
      expect(await m.isQueued(jobId)).toBe(true)
    })

    // WHY: a delayed record MISSING from the zset (e.g. an instance that ZREM'd then died before the
    // flip) is an orphan — the same two-pass suspectedAt reclaim as an orphaned queued record applies.
    it('reclaims a delayed record missing from the zset in two passes', async () => {
      const m = new RedisJM(redis, 'test-group', { heartbeatInterval: 1000, roundsToStale: 2, keepFinishedInterval: 60000, logger: false })
      const jobId = 'dorph#r1'
      await redis.sadd('redisjm:test-group:locks', jobId)
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
        jobId, jobName: 'dorph', runId: 'r1', inputs: null, targetGroup: 'test-group',
        status: 'delayed', progress: 0, readyAt: Date.now(),
      }))

      // Pass 1: suspect stamped, lock kept.
      expect((await m.performMaintenance()).staleCount).toBe(0)
      let rec = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(rec.status).toBe('delayed')
      expect(rec.suspectedAt).toBeGreaterThan(0)
      expect(await m.isQueued(jobId)).toBe(true)

      // Age the suspicion past the stale threshold → reclaimed on the next pass.
      rec.suspectedAt = Date.now() - 3000
      await redis.hset('redisjm:test-group:log', jobId, JSON.stringify(rec))
      expect((await m.performMaintenance()).staleCount).toBe(1)
      rec = JSON.parse((await redis.hget('redisjm:test-group:log', jobId))!) as JobLogRecord
      expect(rec.status).toBe('stale')
      expect(rec.suspectedAt).toBeUndefined()
      expect(await m.isQueued(jobId)).toBe(false)
    })
  })

  describe('unqueue & delayed', () => {
    // WHY: unqueue must clear the delayed-set entry too (a delayed run lives there, not on the queue
    // list), leaving zset entry, lock, and record all gone.
    it('removes a delayed run entirely', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 'u' }, vi.fn())
      await m.queue(job, 'r1', 'x', { delay: 5000 })
      expect(await redis.zscore('redisjm:test-group:delayed', 'u#r1')).not.toBeNull()

      await m.unqueue('u#r1')
      expect(await redis.zscore('redisjm:test-group:delayed', 'u#r1')).toBeNull()
      expect(await m.isQueued('u#r1')).toBe(false)
      expect(await redis.hget('redisjm:test-group:log', 'u#r1')).toBeNull()
    })
  })

  describe('promotion race', () => {
    afterEach(() => { vi.useRealTimers() })

    // WHY: two instances may sweep the same due entry; only the one whose ZREM returns 1 owns the
    // promotion. The loser (ZREM → 0) must skip without RPUSHing (no double-promote) and touch nothing.
    it('a ZREM race loser does not RPUSH the entry', async () => {
      vi.useFakeTimers()
      const base = 1_700_000_000_000
      vi.setSystemTime(base)
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const job = m.createJob({ jobName: 'race' }, vi.fn())
      await m.queue(job, 'r1', 'x', { delay: 1000 })

      // Due now, but a racing instance already claimed it: force this ZREM to report 0 (loser).
      vi.setSystemTime(base + 2000)
      ;(redis.zrem as any).mockResolvedValueOnce(0)

      expect(await m.popAndExecute()).toBe(false) // lost the race → nothing promoted, queue empty
      const rpushed = (redis.rpush as any).mock.calls.filter(([, v]: any[]) => v === 'race#r1').length
      expect(rpushed).toBe(0)
      const rec = JSON.parse((await redis.hget('redisjm:test-group:log', 'race#r1'))!) as JobLogRecord
      expect(rec.status).toBe('delayed') // the winner owns the flip; this instance touched nothing
    })
  })
})
