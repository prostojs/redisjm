import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { RedisJM } from '../redisjm'
import { Job } from '../job'
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
      })
    })

    it('should merge custom options', () => {
      const m = new RedisJM(redis, 'g', { heartbeatInterval: 1000, keepFinishedInterval: 60000 })
      expect(m.getOptions()).toEqual({
        heartbeatInterval: 1000,
        roundsToStale: 2,
        keepFinishedInterval: 60000,
      })
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

    it('should handle unknown job name gracefully', async () => {
      const m = new RedisJM(redis, 'test-group', { keepFinishedInterval: 60000 })
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
})
