import { describe, it, expect, vi } from 'vitest'
import { Job } from '../job'
import { RedisJM } from '../redisjm'
import type { JobContext } from '../types'
import { createMockRedis } from './mock-redis'

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

    // WHY: getAttempts defaults to 1 (no retries) and floors/clamps sub-1 or non-integer values so a
    // bad `attempts` config can never yield zero or partial attempts.
    it('getAttempts defaults to 1 and floors/clamps invalid values', () => {
      expect(new Job(metadata, vi.fn()).getAttempts()).toBe(1)
      expect(new Job({ jobName: 'a', attempts: 3 }, vi.fn()).getAttempts()).toBe(3)
      expect(new Job({ jobName: 'a', attempts: 2.9 }, vi.fn()).getAttempts()).toBe(2)
      expect(new Job({ jobName: 'a', attempts: 0 }, vi.fn()).getAttempts()).toBe(1)
      expect(new Job({ jobName: 'a', attempts: -5 }, vi.fn()).getAttempts()).toBe(1)
    })

    // WHY: getBackoffMs resolves a number or a function of the failed attempt and clamps
    // negative/non-finite results to 0 (immediate re-queue). Default (unset) is 0.
    it('getBackoffMs resolves fixed/functional backoff and clamps to 0', () => {
      expect(new Job(metadata, vi.fn()).getBackoffMs(1)).toBe(0)
      expect(new Job({ jobName: 'a', backoff: 1000 }, vi.fn()).getBackoffMs(1)).toBe(1000)
      expect(new Job({ jobName: 'a', backoff: (n) => n * 100 }, vi.fn()).getBackoffMs(3)).toBe(300)
      expect(new Job({ jobName: 'a', backoff: -500 }, vi.fn()).getBackoffMs(1)).toBe(0)
      expect(new Job({ jobName: 'a', backoff: () => Number.NaN }, vi.fn()).getBackoffMs(1)).toBe(0)
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

    it('should not leak the heartbeat timer when the start hook throws', async () => {
      // Regression: the timer used to be created before the (un-try/finally'd) start hook,
      // so a throwing start hook leaked a setInterval that fired phantom heartbeats forever,
      // defeating stale-reclaim and wedging the lock.
      vi.useFakeTimers()
      const fn = vi.fn()
      const job = new Job<string>(metadata, fn)
      job.hook('start', () => { throw new Error('start failed') })
      const onError = vi.fn()
      const onHeartbeat = vi.fn()
      job.hook('error', onError)
      job.hook('heartbeat', onHeartbeat)

      await expect(
        job.execute('input', { targetGroup: 'g', heartbeatInterval: 100 }),
      ).rejects.toThrow('start failed')
      expect(fn).not.toHaveBeenCalled()
      // A failed claim at start is NOT a job failure: the `error` hook must not fire (a start throw
      // propagates directly, so a superseded/failed claim can never be mistaken for a job error).
      expect(onError).not.toHaveBeenCalled()

      // No timer should be left running.
      await vi.advanceTimersByTimeAsync(1000)
      expect(onHeartbeat).not.toHaveBeenCalled()

      vi.useRealTimers()
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

    it('should reject non-finite progress and clamp out-of-range values', async () => {
      // WHY: setProgress used to accept garbage; a progress bar outside [0,1] (or NaN/Infinity) is
      // meaningless, so it must throw on non-finite input and clamp valid-but-out-of-range values.
      const progresses: number[] = []
      let ctx!: JobContext
      const fn = vi.fn(async (_input: string, c: JobContext) => { ctx = c })
      const job = new Job<string>(metadata, fn)
      job.hook('update', (p) => { if (p.progress !== undefined) progresses.push(p.progress) })
      await job.execute('input', { targetGroup: 'g' })

      expect(() => ctx.setProgress(Number.NaN)).toThrow(TypeError)
      expect(() => ctx.setProgress(Number.POSITIVE_INFINITY)).toThrow(TypeError)
      expect(() => (ctx.setProgress as (v: unknown) => unknown)('x')).toThrow(TypeError)

      await ctx.setProgress(1.5)
      await ctx.setProgress(-0.2)
      expect(progresses).toEqual([1, 0])
    })
  })

  describe('abort signal', () => {
    // WHY: every context exposes a cooperative AbortSignal, and the event payload's abort() aborts it
    // with the given reason (default 'aborted') — the manager's heartbeat hook uses this on loss.
    it('exposes ctx.signal and aborts it via payload.abort with a reason', async () => {
      let ctxRef!: JobContext
      let payloadAbort!: (reason?: string) => void
      const job = new Job<string>(metadata, vi.fn((_i: string, ctx: JobContext) => { ctxRef = ctx }))
      job.hook('start', (p) => { payloadAbort = p.abort })
      await job.execute('x', { targetGroup: 'g' })

      expect(ctxRef.signal).toBeInstanceOf(AbortSignal)
      expect(ctxRef.signal.aborted).toBe(false)
      payloadAbort('kill')
      expect(ctxRef.signal.aborted).toBe(true)
      expect(ctxRef.signal.reason).toBe('kill')
    })

    // WHY: an external signal (options.signal — e.g. shutdown) must be forwarded to ctx.signal,
    // carrying its reason, so the manager can drive aborts from outside the execution.
    it('forwards an external abort signal to ctx.signal, carrying the reason', async () => {
      const external = new AbortController()
      let ctxRef!: JobContext
      const job = new Job<string>(metadata, vi.fn((_i: string, ctx: JobContext) => new Promise<void>((resolve) => {
        ctxRef = ctx
        ctx.signal.addEventListener('abort', () => resolve(), { once: true })
      })))
      const exec = job.execute('x', { targetGroup: 'g', signal: external.signal })
      await new Promise((r) => setTimeout(r, 0)) // let the handler attach its listener
      external.abort('external stop')
      await exec

      expect(ctxRef.signal.aborted).toBe(true)
      expect(ctxRef.signal.reason).toBe('external stop')
    })

    // WHY: an external signal already aborted before execution must abort ctx.signal immediately.
    it('aborts ctx.signal immediately when the external signal is already aborted', async () => {
      const external = new AbortController()
      external.abort('pre-aborted')
      let ctxRef!: JobContext
      const job = new Job<string>(metadata, vi.fn((_i: string, ctx: JobContext) => { ctxRef = ctx }))
      await job.execute('x', { targetGroup: 'g', signal: external.signal })

      expect(ctxRef.signal.aborted).toBe(true)
      expect(ctxRef.signal.reason).toBe('pre-aborted')
    })

    // WHY: an execution must not leak a listener on a long-lived external signal — it detaches its
    // 'abort' listener when it settles, so a signal shared across many runs never accumulates listeners.
    it('detaches its listener from a long-lived external signal after settling', async () => {
      const external = new AbortController()
      const removeSpy = vi.spyOn(external.signal, 'removeEventListener')
      const job = new Job<string>(metadata, vi.fn())
      await job.execute('x', { targetGroup: 'g', signal: external.signal })

      expect(removeSpy).toHaveBeenCalledWith('abort', expect.any(Function))
      // A later abort of the external signal must not reach this settled run's detached listener.
      expect(() => external.abort('too late')).not.toThrow()
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
      // `options` is passed through (undefined here); the manager forwards it to enqueue.
      expect(mockManager.queue).toHaveBeenCalledWith(job, 'run1', 'input1', undefined)
    })

    it('should pass queue options (delay) through to the manager', async () => {
      const mockManager = {
        queue: vi.fn().mockResolvedValue(true),
        getTargetGroup: () => 'group1',
      } as unknown as RedisJM
      const job = new Job<string>(metadata, vi.fn(), mockManager)
      await job.queue('run1', 'input1', undefined, { delay: 5000 })
      expect(mockManager.queue).toHaveBeenCalledWith(job, 'run1', 'input1', { delay: 5000 })
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

  describe('queueFirst', () => {
    // WHY: queueFirst mirrors queue but delegates to manager.queueFirst (priority insert), forwarding options.
    it('should call manager.queueFirst and return its result', async () => {
      const mockManager = {
        queueFirst: vi.fn().mockResolvedValue(true),
        getTargetGroup: () => 'group1',
      } as unknown as RedisJM
      const job = new Job<string>(metadata, vi.fn(), mockManager)
      const result = await job.queueFirst('run1', 'input1')
      expect(result).toBe(true)
      expect(mockManager.queueFirst).toHaveBeenCalledWith(job, 'run1', 'input1', undefined)
    })

    // WHY: a priority insert must be popped before an already-queued run (front-of-queue insert).
    it('jumps the queue — the queueFirst run is popped before an earlier-queued run', async () => {
      const redis = createMockRedis()
      const m = new RedisJM(redis, 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false })
      const order: string[] = []
      const job = m.createJob({ jobName: 'j' }, async (input: string) => { order.push(input) })
      await job.queue('first', 'first')
      await job.queueFirst('urgent', 'urgent')
      await m.popAndExecute()
      await m.popAndExecute()
      expect(order).toEqual(['urgent', 'first'])
    })

    // WHY: same guard as queue — no manager (explicit or default) is an error, not a silent no-op.
    it('should throw when no manager is available', async () => {
      const job = new Job<string>(metadata, vi.fn())
      await expect(job.queueFirst('run1', 'input1')).rejects.toThrow(
        'No RedisJM instance provided and no default manager set',
      )
    })
  })
})
