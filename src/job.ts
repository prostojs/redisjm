import { randomUUID } from 'node:crypto'
import { Hookable } from 'hookable'
import type { RedisJM } from './redisjm'
import { toError } from './utils'
import type {
  JobAttrs,
  JobAttrValue,
  JobContext,
  JobExecuteOptions,
  JobFunction,
  JobHooks,
  JobMetadata,
  QueueOptions,
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
    // A fresh fencing token per execution: the record's owner is whoever's `start` stamped it.
    const executionId = randomUUID()

    const payload = { job: this, targetGroup, runId, inputs, executionId, manager: options?.manager }

    const ctx: JobContext<TAttrs> = {
      setProgress: (progress: number) => {
        // `Number.isFinite` does not coerce, so it already rejects non-numbers (NaN, Infinity, 'x', ...).
        if (!Number.isFinite(progress)) {
          throw new TypeError(`setProgress(progress): progress must be a finite number, got ${String(progress)}`)
        }
        // Clamp into [0, 1] rather than trusting the caller — a progress bar outside the range is meaningless.
        const clamped = Math.max(0, Math.min(1, progress))
        return this.callHook('update', { ...payload, progress: clamped })
      },
      setAttrs: (attrs: TAttrs) => {
        return this.callHook('update', { ...payload, attrs })
      },
    }

    // The heartbeat timer is created *after* the start hook resolves and torn down in
    // a `finally`, so a throwing/rejecting `start` (or `finish`) hook can never leak a
    // timer that keeps firing phantom heartbeats and defeats stale-reclaim.
    let heartbeatTimer: ReturnType<typeof setInterval> | undefined
    try {
      // `start` is the claim: a failed claim or Redis outage here is NOT a job failure, so it
      // propagates directly (via the `finally`) without ever dispatching the `error` hook.
      await this.callHook('start', payload)
      if (heartbeatInterval && heartbeatInterval > 0) {
        heartbeatTimer = setInterval(() => {
          // A failed heartbeat write is infra, not job outcome: report it instead of swallowing it.
          this.callHook('heartbeat', payload).catch((err) =>
            options?.logger?.('heartbeat update failed', toError(err)),
          )
        }, heartbeatInterval)
      }
      try {
        await this.fn(inputs, ctx)
      } catch (err) {
        // Only a job-function failure is a real job error. Dispatch the `error` hook, then rethrow
        // the ORIGINAL error. A throwing `error` hook is itself infra: report it and still rethrow
        // the job's own error so the true cause is never masked.
        const error = toError(err)
        try {
          await this.callHook('error', { ...payload, error })
        } catch (hookErr) {
          options?.logger?.('error hook failed', toError(hookErr))
        }
        throw error
      }
      // A throwing `finish` hook must never flip a successful run to `error`; let it propagate as-is.
      await this.callHook('finish', payload)
    } finally {
      if (heartbeatTimer) clearInterval(heartbeatTimer)
    }
  }

  /**
   * Convenience method to queue this job via a RedisJM instance.
   * Uses the provided manager or falls back to the default manager set in the constructor.
   *
   * @param runId - Unique identifier for this run (duplicates are rejected)
   * @param inputs - The job inputs to store and pass at execution time
   * @param manager - Optional RedisJM instance (overrides the default)
   * @param options - Optional queue options (e.g. `delay` to stage the run on the delayed set)
   * @returns `true` if queued, `false` if already locked
   *
   * @example
   * ```ts
   * const queued = await job.queue('order-123', { orderId: '123' })
   * await job.queue('order-456', { orderId: '456' }, undefined, { delay: 5000 })
   * ```
   */
  async queue(runId: string, inputs: TInputs, manager?: RedisJM, options?: QueueOptions): Promise<boolean> {
    const mgr = manager ?? this.defaultManager
    if (!mgr) {
      throw new Error('No RedisJM instance provided and no default manager set')
    }
    return mgr.queue(this as Job<any, any>, runId, inputs, options)
  }

  /**
   * Resolves the total number of attempts (including the first) for this job's runs. Floors and
   * clamps to a minimum of 1, so `attempts` values below 1 or non-integers never yield zero/partial
   * attempts. Default (`attempts` unset) is 1 — no retries.
   */
  getAttempts(): number {
    return Math.max(1, Math.floor(this.metadata.attempts ?? 1))
  }

  /**
   * Resolves the backoff delay (ms) before the retry that follows the given 1-based failed `attempt`.
   * Accepts a fixed number or a function of the failed attempt; negative / non-finite results are
   * clamped to 0 (immediate re-queue via the delayed set). Default (`backoff` unset) is 0.
   */
  getBackoffMs(attempt: number): number {
    const raw = typeof this.metadata.backoff === 'function' ? this.metadata.backoff(attempt) : this.metadata.backoff
    if (raw === undefined || !Number.isFinite(raw) || raw < 0) return 0
    return raw
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

  /** Returns the job's lane, or `undefined` for the default lane. */
  getLane(): string | undefined {
    return this.metadata.lane
  }

  /** Sets the default RedisJM instance used by `queue()` when no manager is explicitly provided. */
  setDefaultManager(manager: RedisJM): void {
    this.defaultManager = manager
  }
}
