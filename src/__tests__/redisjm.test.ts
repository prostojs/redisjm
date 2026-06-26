import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { RedisJM } from '../redisjm'
import { Job } from '../job'
import { createMaintenanceJob } from '../maintenance'
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
      })
    })

    it('should accept explicit maintenanceInterval', () => {
      const m = new RedisJM(redis, 'g', { maintenanceInterval: 0 })
      expect(m.getOptions().maintenanceInterval).toBe(0)
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

      // Fire the manager's heartbeat hook for this run (as a late/leaked timer would).
      await job.callHook('heartbeat', { job, targetGroup: 'test-group', runId: 'r1', inputs: null })

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
})
