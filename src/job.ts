import { Hookable } from 'hookable'
import type { RedisJM } from './redisjm'
import type {
  JobAttrs,
  JobAttrValue,
  JobContext,
  JobExecuteOptions,
  JobFunction,
  JobHooks,
  JobMetadata,
} from './types'

/**
 * Represents a named job with a function and event hooks.
 *
 * @typeParam TInputs - Type for job inputs (must be JSON-serializable)
 * @typeParam TAttrs - Type for custom attributes stored in the job log
 *
 * @example
 * ```ts
 * const job = new Job<{ orderId: string }, { step: string }>(
 *   { jobName: 'process-order' },
 *   async (inputs, ctx) => {
 *     await ctx.setAttrs({ step: 'processing' })
 *     await ctx.setProgress(1)
 *   }
 * )
 * ```
 */
export class Job<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> extends Hookable<JobHooks<TInputs, TAttrs>> {
  private readonly metadata: JobMetadata
  private readonly fn: JobFunction<TInputs, TAttrs>
  private defaultManager: RedisJM | undefined

  /**
   * @param metadata - Job name and optional description
   * @param fn - The job function to execute
   * @param manager - Optional default RedisJM instance for `queue()` calls
   */
  constructor(metadata: JobMetadata, fn: JobFunction<TInputs, TAttrs>, manager?: RedisJM) {
    super()
    this.metadata = metadata
    this.fn = fn
    this.defaultManager = manager
  }

  /**
   * Runs the job function with heartbeat timer and context callbacks.
   * Dispatches `start`, `finish`/`error`, `heartbeat`, and `update` events.
   *
   * @param inputs - The job inputs passed to the job function
   * @param options - Target group, heartbeat interval, and explicit runId
   *
   * @example
   * ```ts
   * await job.execute({ orderId: '123' }, { targetGroup: 'my-app', heartbeatInterval: 5000 })
   * ```
   */
  async execute(inputs: TInputs, options?: JobExecuteOptions): Promise<void> {
    const targetGroup = options?.targetGroup ?? this.defaultManager?.getTargetGroup() ?? ''
    const runId = options?.runId ?? (typeof inputs === 'string' ? inputs : JSON.stringify(inputs))
    const heartbeatInterval = options?.heartbeatInterval

    const payload = { job: this, targetGroup, runId, inputs }

    const ctx: JobContext<TAttrs> = {
      setProgress: (progress: number) => {
        return this.callHook('update', { ...payload, progress })
      },
      setAttrs: (attrs: TAttrs) => {
        return this.callHook('update', { ...payload, attrs })
      },
    }

    let heartbeatTimer: ReturnType<typeof setInterval> | undefined
    if (heartbeatInterval && heartbeatInterval > 0) {
      heartbeatTimer = setInterval(() => {
        this.callHook('heartbeat', payload).catch(() => {})
      }, heartbeatInterval)
    }

    await this.callHook('start', payload)
    try {
      await this.fn(inputs, ctx)
      if (heartbeatTimer) clearInterval(heartbeatTimer)
      await this.callHook('finish', payload)
    } catch (err) {
      if (heartbeatTimer) clearInterval(heartbeatTimer)
      const error = err instanceof Error ? err : new Error(String(err))
      await this.callHook('error', { ...payload, error })
      throw error
    }
  }

  /**
   * Convenience method to queue this job via a RedisJM instance.
   * Uses the provided manager or falls back to the default manager set in the constructor.
   *
   * @param runId - Unique identifier for this run (duplicates are rejected)
   * @param inputs - The job inputs to store and pass at execution time
   * @param manager - Optional RedisJM instance (overrides the default)
   * @returns `true` if queued, `false` if already locked
   *
   * @example
   * ```ts
   * const queued = await job.queue('order-123', { orderId: '123' })
   * ```
   */
  async queue(runId: string, inputs: TInputs, manager?: RedisJM): Promise<boolean> {
    const mgr = manager ?? this.defaultManager
    if (!mgr) {
      throw new Error('No RedisJM instance provided and no default manager set')
    }
    return mgr.queue(this as Job<any, any>, runId, inputs)
  }

  /**
   * Returns the composite job ID (`"jobName#runId"`).
   *
   * @example
   * ```ts
   * job.getJobId('run-1') // "process-order#run-1"
   * ```
   */
  getJobId(runId: string): string {
    return `${this.metadata.jobName}#${runId}`
  }

  /** Returns a copy of the job metadata. */
  getMetadata(): JobMetadata {
    return { ...this.metadata }
  }

  /** Returns the job name. */
  getName(): string {
    return this.metadata.jobName
  }

  /** Sets the default RedisJM instance used by `queue()` when no manager is explicitly provided. */
  setDefaultManager(manager: RedisJM): void {
    this.defaultManager = manager
  }
}
