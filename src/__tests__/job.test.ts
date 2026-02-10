import { describe, it, expect, vi } from 'vitest'
import { Job } from '../job'
import type { RedisJM } from '../redisjm'
import type { JobContext } from '../types'

describe('Job', () => {
  const metadata = { jobName: 'testJob', description: 'A test job' }

  describe('constructor and getters', () => {
    it('should return the job name', () => {
      const job = new Job(metadata, vi.fn())
      expect(job.getName()).toBe('testJob')
    })

    it('should return a copy of metadata', () => {
      const job = new Job(metadata, vi.fn())
      const result = job.getMetadata()
      expect(result).toEqual(metadata)
      expect(result).not.toBe(metadata)
    })

    it('should generate jobId from jobName and runId', () => {
      const job = new Job(metadata, vi.fn())
      expect(job.getJobId('run1')).toBe('testJob#run1')
      expect(job.getJobId('2024-01-01')).toBe('testJob#2024-01-01')
    })
  })

  describe('execute', () => {
    it('should call the job function with inputs and context', async () => {
      const fn = vi.fn()
      const job = new Job<{ value: number }>(metadata, fn)
      await job.execute({ value: 42 }, { targetGroup: 'group1' })
      expect(fn).toHaveBeenCalledWith({ value: 42 }, expect.objectContaining({
        setProgress: expect.any(Function),
        setAttrs: expect.any(Function),
      }))
    })

    it('should dispatch start event before execution', async () => {
      const order: string[] = []
      const fn = vi.fn(() => { order.push('fn') })
      const job = new Job<string>(metadata, fn)
      job.hook('start', () => { order.push('start') })
      await job.execute('input1', { targetGroup: 'group1' })
      expect(order).toEqual(['start', 'fn'])
    })

    it('should dispatch finish event after execution', async () => {
      const fn = vi.fn()
      const job = new Job<string>(metadata, fn)
      const onFinish = vi.fn()
      job.hook('finish', onFinish)
      await job.execute('input1', { targetGroup: 'group1' })
      expect(onFinish).toHaveBeenCalledWith(
        expect.objectContaining({
          job,
          targetGroup: 'group1',
          inputs: 'input1',
        }),
      )
    })

    it('should dispatch error event and rethrow on failure', async () => {
      const err = new Error('Job failed')
      const fn = vi.fn(() => { throw err })
      const job = new Job<string>(metadata, fn)
      const onError = vi.fn()
      job.hook('error', onError)

      await expect(job.execute('input1', { targetGroup: 'group1' })).rejects.toThrow('Job failed')
      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({
          job,
          targetGroup: 'group1',
          inputs: 'input1',
          error: err,
        }),
      )
    })

    it('should not dispatch finish event on failure', async () => {
      const fn = vi.fn(() => { throw new Error('fail') })
      const job = new Job<string>(metadata, fn)
      const onFinish = vi.fn()
      job.hook('finish', onFinish)

      await expect(job.execute('input1', { targetGroup: 'group1' })).rejects.toThrow()
      expect(onFinish).not.toHaveBeenCalled()
    })

    it('should handle async job functions', async () => {
      const fn = vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 10))
      })
      const job = new Job<string>(metadata, fn)
      const onFinish = vi.fn()
      job.hook('finish', onFinish)
      await job.execute('input1', { targetGroup: 'group1' })
      expect(onFinish).toHaveBeenCalled()
    })

    it('should use default manager targetGroup when none provided', async () => {
      const fn = vi.fn()
      const mockManager = { getTargetGroup: () => 'default-group' } as RedisJM
      const job = new Job<string>(metadata, fn, mockManager)
      const onStart = vi.fn()
      job.hook('start', onStart)
      await job.execute('input1')
      expect(onStart).toHaveBeenCalledWith(
        expect.objectContaining({ targetGroup: 'default-group' }),
      )
    })

    it('should use explicit runId from options', async () => {
      const fn = vi.fn()
      const job = new Job<{ x: number }>(metadata, fn)
      const onStart = vi.fn()
      job.hook('start', onStart)
      await job.execute({ x: 1 }, { targetGroup: 'group1', runId: 'custom-run-id' })
      expect(onStart).toHaveBeenCalledWith(
        expect.objectContaining({ runId: 'custom-run-id' }),
      )
    })

    it('should derive runId from serialized inputs when not provided', async () => {
      const fn = vi.fn()
      const job = new Job<{ x: number }>(metadata, fn)
      const onStart = vi.fn()
      job.hook('start', onStart)
      await job.execute({ x: 1 }, { targetGroup: 'group1' })
      expect(onStart).toHaveBeenCalledWith(
        expect.objectContaining({ runId: '{"x":1}' }),
      )
    })

    it('should use string itself as runId for string inputs', async () => {
      const fn = vi.fn()
      const job = new Job<string>(metadata, fn)
      const onStart = vi.fn()
      job.hook('start', onStart)
      await job.execute('my-run-id', { targetGroup: 'group1' })
      expect(onStart).toHaveBeenCalledWith(
        expect.objectContaining({ runId: 'my-run-id' }),
      )
    })
  })

  describe('heartbeat', () => {
    it('should dispatch heartbeat events at the configured interval', async () => {
      vi.useFakeTimers()
      const fn = vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 250))
      })
      const job = new Job<string>(metadata, fn)
      const onHeartbeat = vi.fn()
      job.hook('heartbeat', onHeartbeat)

      const executePromise = job.execute('input', { targetGroup: 'g', heartbeatInterval: 100 })

      await vi.advanceTimersByTimeAsync(100)
      expect(onHeartbeat).toHaveBeenCalledTimes(1)

      await vi.advanceTimersByTimeAsync(100)
      expect(onHeartbeat).toHaveBeenCalledTimes(2)

      await vi.advanceTimersByTimeAsync(100)
      await executePromise

      vi.useRealTimers()
    })

    it('should stop heartbeat after job finishes', async () => {
      vi.useFakeTimers()
      const fn = vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
      })
      const job = new Job<string>(metadata, fn)
      const onHeartbeat = vi.fn()
      job.hook('heartbeat', onHeartbeat)

      const executePromise = job.execute('input', { targetGroup: 'g', heartbeatInterval: 100 })
      await vi.advanceTimersByTimeAsync(50)
      await executePromise

      onHeartbeat.mockClear()
      await vi.advanceTimersByTimeAsync(200)
      expect(onHeartbeat).not.toHaveBeenCalled()

      vi.useRealTimers()
    })

    it('should stop heartbeat after job errors', async () => {
      vi.useFakeTimers()
      const fn = vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
        throw new Error('fail')
      })
      const job = new Job<string>(metadata, fn)
      const onHeartbeat = vi.fn()
      job.hook('heartbeat', onHeartbeat)

      let caughtError: Error | undefined
      const executePromise = job.execute('input', { targetGroup: 'g', heartbeatInterval: 100 })
        .catch((e: Error) => { caughtError = e })
      await vi.advanceTimersByTimeAsync(50)
      await executePromise
      expect(caughtError?.message).toBe('fail')

      onHeartbeat.mockClear()
      await vi.advanceTimersByTimeAsync(200)
      expect(onHeartbeat).not.toHaveBeenCalled()

      vi.useRealTimers()
    })

    it('should not set up heartbeat when interval is not provided', async () => {
      const fn = vi.fn()
      const job = new Job<string>(metadata, fn)
      const onHeartbeat = vi.fn()
      job.hook('heartbeat', onHeartbeat)
      await job.execute('input', { targetGroup: 'g' })
      expect(onHeartbeat).not.toHaveBeenCalled()
    })
  })

  describe('context callbacks', () => {
    it('should dispatch update event with progress via setProgress', async () => {
      const fn = vi.fn(async (_input: string, ctx: JobContext) => {
        await ctx.setProgress(0.5)
      })
      const job = new Job<string>(metadata, fn)
      const onUpdate = vi.fn()
      job.hook('update', onUpdate)
      await job.execute('input', { targetGroup: 'g' })
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ progress: 0.5 }),
      )
    })

    it('should dispatch update event with attrs via setAttrs', async () => {
      const fn = vi.fn(async (_input: string, ctx: JobContext<{ status: string }>) => {
        await ctx.setAttrs({ status: 'processing' })
      })
      const job = new Job<string, { status: string }>(metadata, fn)
      const onUpdate = vi.fn()
      job.hook('update', onUpdate)
      await job.execute('input', { targetGroup: 'g' })
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ attrs: { status: 'processing' } }),
      )
    })
  })

  describe('queue', () => {
    it('should call manager.queue and return its result', async () => {
      const mockManager = {
        queue: vi.fn().mockResolvedValue(true),
        getTargetGroup: () => 'group1',
      } as unknown as RedisJM
      const job = new Job<string>(metadata, vi.fn(), mockManager)
      const result = await job.queue('run1', 'input1')
      expect(result).toBe(true)
      expect(mockManager.queue).toHaveBeenCalledWith(job, 'run1', 'input1')
    })

    it('should use provided manager over default', async () => {
      const defaultManager = {
        queue: vi.fn().mockResolvedValue(false),
        getTargetGroup: () => 'group1',
      } as unknown as RedisJM
      const customManager = {
        queue: vi.fn().mockResolvedValue(true),
        getTargetGroup: () => 'group2',
      } as unknown as RedisJM

      const job = new Job<string>(metadata, vi.fn(), defaultManager)
      const result = await job.queue('run1', 'input1', customManager)
      expect(result).toBe(true)
      expect(customManager.queue).toHaveBeenCalled()
      expect(defaultManager.queue).not.toHaveBeenCalled()
    })

    it('should throw when no manager is available', async () => {
      const job = new Job<string>(metadata, vi.fn())
      await expect(job.queue('run1', 'input1')).rejects.toThrow(
        'No RedisJM instance provided and no default manager set',
      )
    })
  })
})
