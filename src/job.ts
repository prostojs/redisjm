import { randomUUID } from 'node:crypto'
import { Hookable } from 'hookable'
import { JobAbortedError, JobTimeoutError } from './errors'
import type { RedisJM } from './redisjm'
import { checkAbortGraceMs, positiveOrZero, toError, toTaggable } from './utils'
import type {
  EnqueueOptions,
  EnqueueManyResult,
  EnqueueResult,
  JobAttrs,
  JobAttrValue,
  JobContext,
  JobEventPayload,
  JobExecuteOptions,
  JobFunction,
  JobHooks,
  JobMetadata,
  QueueOptions,
} from './types'

/**
 * Errors thrown out of the START phase of an execution (the `start` hook chain, which contains the
 * manager's claim), mapped to that execution's event payload. Internal (not re-exported): the manager
 * uses it to tell a start-phase failure — which needs requeue/drop/fail recovery because the popped
 * entry is gone — apart from a handler or `finish`-hook failure, to fence that recovery on the right
 * executionId, and to dispatch the recovery's `error` hook with the execution's own payload. A WeakMap
 * so the mapping lives exactly as long as the error object.
 */
const startPhaseFailures = new WeakMap<object, JobEventPayload<any>>()

/**
 * Returns the event payload of the execution whose START phase threw `err`, or `undefined` when `err`
 * did not come out of a start phase. Internal helper for the manager's start-failure recovery.
 */
export function getStartPhasePayload(err: unknown): JobEventPayload<any> | undefined {
  // A primitive is never a key (WeakMap.get answers `undefined` for it).
  return startPhaseFailures.get(err as object)
}

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
   * With `options.timeoutMs > 0` the handler is raced against a timer: on expiry `ctx.signal` aborts
   * (reason `'timeout'`), the `error` hook fires with a `JobTimeoutError`, and `execute()` rejects with
   * it — WITHOUT waiting for the handler, so a hung handler cannot hold the caller (or a concurrency
   * slot) forever. If the abandoned handler later settles, that is reported once via `options.logger`
   * (never an unhandled rejection).
   *
   * With `options.abortGraceMs` (a number `>= 0`) the same settle-and-abandon applies to every OTHER abort
   * of `ctx.signal` (ownership loss, `payload.abort()`, an external `signal`): a handler still pending
   * `abortGraceMs` after the abort is abandoned and `execute()` rejects with a `JobAbortedError`; one that
   * settles within the grace decides the outcome itself. Once an attempt is abandoned (timeout or abort),
   * `ctx.setProgress` / `ctx.setAttrs` short-circuit (resolve, dispatch nothing).
   *
   * @param inputs - The job inputs passed to the job function
   * @param options - Target group, heartbeat interval, explicit runId, timeout, abort signal
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
    const timeoutMs = options?.timeoutMs
    const abortGraceMs = checkAbortGraceMs(options?.abortGraceMs, 'execute(options)')
    const attempt = options?.attempt ?? 1
    if (!Number.isInteger(attempt) || attempt < 1) {
      throw new TypeError(`execute(options.attempt): attempt must be an integer >= 1, got ${String(attempt)}`)
    }
    // A fresh fencing token per execution: the record's owner is whoever's `start` stamped it.
    const executionId = randomUUID()

    // Per-execution cooperative abort. `ctx.signal` is aborted when the run loses ownership of its
    // record (the manager's heartbeat hook calls `payload.abort`) or when an external signal — e.g.
    // shutdown via `options.signal` — fires. Nothing forcibly kills the handler; it observes the signal.
    const controller = new AbortController()
    const externalSignal = options?.signal
    let onExternalAbort: (() => void) | undefined
    if (externalSignal) {
      if (externalSignal.aborted) {
        // Already aborted before we started following it — mirror it immediately.
        controller.abort(externalSignal.reason)
      } else {
        onExternalAbort = () => controller.abort(externalSignal.reason)
        externalSignal.addEventListener('abort', onExternalAbort, { once: true })
      }
    }

    const payload: JobEventPayload<TInputs> = {
      job: this,
      targetGroup,
      runId,
      inputs,
      executionId,
      manager: options?.manager,
      // Replaced by the manager's claim with the attempt it wrote (see `JobEventPayload.attempt`).
      attempt,
      abort: (reason?: string) => controller.abort(reason ?? 'aborted'),
    }

    // Set when the attempt was settled WITHOUT the handler (timeout / abort grace elapsed): the handler
    // keeps running detached. A cheap short-circuit only — the fenced write (a rejected write emits no
    // `update`) is what actually keeps a detached handler's `ctx` calls out of the record.
    let abandoned = false

    const ctx: JobContext<TAttrs> = {
      setProgress: (progress: number) => {
        // `Number.isFinite` does not coerce, so it already rejects non-numbers (NaN, Infinity, 'x', ...).
        if (!Number.isFinite(progress)) {
          throw new TypeError(`setProgress(progress): progress must be a finite number, got ${String(progress)}`)
        }
        // Clamp into [0, 1] rather than trusting the caller — a progress bar outside the range is meaningless.
        const clamped = Math.max(0, Math.min(1, progress))
        if (abandoned) return Promise.resolve()
        return this.callHook('update', { ...payload, progress: clamped })
      },
      setAttrs: (attrs: TAttrs) => {
        if (abandoned) return Promise.resolve()
        return this.callHook('update', { ...payload, attrs })
      },
      signal: controller.signal,
    }

    // The heartbeat timer is created *after* the start hook resolves and torn down in
    // a `finally`, so a throwing/rejecting `start` (or `finish`) hook can never leak a
    // timer that keeps firing phantom heartbeats and defeats stale-reclaim.
    let heartbeatTimer: ReturnType<typeof setInterval> | undefined
    // The heartbeat dispatch currently in flight (never rejects). A heartbeat is a read-modify-write of
    // the record: if one is still in flight when the run settles, its write can land AFTER the terminal
    // `finish`/`error` write and flip the record back to `running` (stuck until maintenance stales it).
    // `stopHeartbeat` therefore clears the timer AND awaits the in-flight beat before any terminal hook.
    let heartbeatInFlight: Promise<void> | undefined
    const stopHeartbeat = async () => {
      if (heartbeatTimer) clearInterval(heartbeatTimer)
      heartbeatTimer = undefined
      await heartbeatInFlight
    }
    try {
      // `start` is the claim: a failed claim or Redis outage here is NOT a job failure, so it
      // propagates directly (via the `finally`) without ever dispatching the `error` hook. It is
      // tagged with this execution's payload (incl. its fencing token) so a manager can tell a
      // start-phase failure (the popped entry needs requeue/drop/fail recovery) from a handler failure.
      try {
        await this.callHook('start', payload)
      } catch (err) {
        const tagged = toTaggable(err)
        startPhaseFailures.set(tagged, payload)
        throw tagged
      }
      if (heartbeatInterval && heartbeatInterval > 0) {
        heartbeatTimer = setInterval(() => {
          // One beat at a time: a slow write skips beats instead of piling up overlapping writes.
          if (heartbeatInFlight) return
          heartbeatInFlight = this.callHook('heartbeat', payload)
            // A failed heartbeat write is infra, not job outcome: report it instead of swallowing it.
            .catch((err) => options?.logger?.('heartbeat update failed', toError(err)))
            .finally(() => {
              heartbeatInFlight = undefined
            })
        }, heartbeatInterval)
      }
      try {
        await this.runHandler(inputs, ctx, controller, runId, {
          timeoutMs,
          abortGraceMs,
          logger: options?.logger,
          onAbandon: () => {
            abandoned = true
          },
        })
      } catch (err) {
        // Only a job-function failure is a real job error. Dispatch the `error` hook, then rethrow
        // the ORIGINAL error. A throwing `error` hook is itself infra: report it and still rethrow
        // the job's own error so the true cause is never masked.
        const error = toError(err)
        await stopHeartbeat()
        try {
          await this.callHook('error', { ...payload, error })
        } catch (hookErr) {
          options?.logger?.('error hook failed', toError(hookErr))
        }
        throw error
      }
      await stopHeartbeat()
      // A throwing `finish` hook must never flip a successful run to `error`; let it propagate as-is.
      await this.callHook('finish', payload)
    } finally {
      if (heartbeatTimer) clearInterval(heartbeatTimer)
      // Detach the external-signal listener so a long-lived, shared shutdown signal doesn't accumulate
      // one dead listener per execution (a slow leak on a signal that outlives many runs).
      if (externalSignal && onExternalAbort) {
        externalSignal.removeEventListener('abort', onExternalAbort)
      }
    }
  }

  /**
   * Invokes the job function, racing it against `timeoutMs` and — once the run's signal aborts for another
   * reason — against `abortGraceMs`, when set. On expiry of the timeout: abort the run's signal with
   * reason `'timeout'` and reject with a `JobTimeoutError`. On expiry of the grace: reject with a
   * `JobAbortedError`. Either way the handler promise is abandoned (JS cannot kill it), so the caller's
   * `finally` clears the heartbeat and frees the slot right away. `onAbandon` runs BEFORE the rethrow (the
   * caller short-circuits its `ctx` writes). The abandoned promise gets a handler that logs ONCE if it later settles
   * or rejects, so a late rejection never becomes an unhandled rejection and a late completion is still
   * visible. A handler that settles on its own first (also within a grace) decides the outcome itself.
   */
  private async runHandler(
    inputs: TInputs,
    ctx: JobContext<TAttrs>,
    controller: AbortController,
    runId: string,
    opts: {
      timeoutMs: number | undefined
      abortGraceMs: number | false | undefined
      logger: JobExecuteOptions['logger']
      onAbandon: () => void
    },
  ): Promise<void> {
    // Invoke synchronously (a sync throw still becomes a rejection, exactly like `await this.fn(...)`).
    let handler: Promise<void>
    try {
      handler = Promise.resolve(this.fn(inputs, ctx))
    } catch (err) {
      handler = Promise.reject(err)
    }
    const timeoutMs = positiveOrZero(opts.timeoutMs)
    const grace = opts.abortGraceMs === false ? undefined : opts.abortGraceMs
    if (timeoutMs === 0 && grace === undefined) {
      await handler
      return
    }

    const jobId = this.getJobId(runId)
    let timer: ReturnType<typeof setTimeout> | undefined
    let graceTimer: ReturnType<typeof setTimeout> | undefined
    let onAbort: (() => void) | undefined
    // Set by whichever guard fires first; doubles as the "abandoned" flag checked after the race.
    let abandonError: JobTimeoutError | JobAbortedError | undefined
    const abandonment = new Promise<never>((_, reject) => {
      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          abandonError = new JobTimeoutError(timeoutMs, jobId)
          // Reject BEFORE aborting: an abort listener in the handler may reject the handler promise
          // synchronously; the `abandonError` check below makes the timeout win regardless of order.
          reject(abandonError)
          controller.abort('timeout')
        }, timeoutMs)
      }
      if (grace !== undefined) {
        onAbort = () => {
          // The timeout aborts the signal itself (reason 'timeout'): it has already won.
          if (abandonError || graceTimer) return
          graceTimer = setTimeout(() => {
            abandonError = new JobAbortedError(toError(controller.signal.reason ?? 'aborted').message, grace, jobId)
            reject(abandonError)
          }, grace)
        }
        // Aborted before (or while) the start phase ran: the grace begins as the handler starts.
        if (controller.signal.aborted) onAbort()
        else controller.signal.addEventListener('abort', onAbort, { once: true })
      }
    })
    try {
      await Promise.race([handler, abandonment])
    } catch (err) {
      if (!abandonError) throw err
      opts.onAbandon()
      // Abandoned handler: observe its eventual outcome once, so it is never an unhandled rejection.
      const what = abandonError instanceof JobTimeoutError
        ? `its ${timeoutMs}ms timeout`
        : `its abort (${abandonError.reason})`
      handler.then(
        () => opts.logger?.(`job "${jobId}" handler settled after ${what} (result discarded)`),
        (lateErr) => opts.logger?.(`job "${jobId}" handler rejected after ${what}`, toError(lateErr)),
      )
      throw abandonError
    } finally {
      clearTimeout(timer)
      clearTimeout(graceTimer)
      if (onAbort) controller.signal.removeEventListener('abort', onAbort)
    }
  }

  /**
   * Enqueues this job via a RedisJM instance and reports what happened (see `RedisJM.enqueue`).
   * Uses the provided manager or falls back to the default manager set in the constructor.
   *
   * @param runId - Unique identifier for this run (duplicates are deduped)
   * @param inputs - The job inputs to store and pass at execution time
   * @param manager - Optional RedisJM instance (overrides the default)
   * @param options - `delay`, `first` (priority insert)
   * @returns `{ status: 'queued' | 'deduped' | 'busy' | 'full', jobId }`; Redis failures throw `RedisJMEnqueueError`
   *
   * @example
   * ```ts
   * const { status } = await job.enqueue('order-123', { orderId: '123' })
   * if (status === 'deduped') console.log('already in flight')
   * ```
   */
  async enqueue(runId: string, inputs: TInputs, manager?: RedisJM, options?: EnqueueOptions): Promise<EnqueueResult> {
    return this.resolveManager(manager).enqueue(this as Job<any, any>, runId, inputs, options)
  }

  /**
   * Enqueues many runs of this job in one atomic call and returns a result per entry (see
   * `RedisJM.enqueueMany`). Uses the provided manager or the default manager set in the constructor.
   *
   * @example
   * ```ts
   * const results = await job.enqueueMany(orders.map((o) => ({ runId: o.id, inputs: o })))
   * ```
   */
  async enqueueMany(
    entries: Array<{ runId: string; inputs: TInputs }>,
    manager?: RedisJM,
    options?: EnqueueOptions,
  ): Promise<EnqueueManyResult[]> {
    return this.resolveManager(manager).enqueueMany(this as Job<any, any>, entries, options)
  }

  /**
   * Convenience method to queue this job via a RedisJM instance.
   * Uses the provided manager or falls back to the default manager set in the constructor.
   *
   * @param runId - Unique identifier for this run (duplicates are rejected)
   * @param inputs - The job inputs to store and pass at execution time
   * @param manager - Optional RedisJM instance (overrides the default)
   * @param options - Optional queue options (e.g. `delay` to stage the run on the delayed set)
   * @returns `true` if queued; `false` if not queued (deduped by a held lock, or a full lane) — use
   *   {@link enqueue} to tell those apart. Redis failures throw `RedisJMEnqueueError`.
   *
   * @example
   * ```ts
   * const queued = await job.queue('order-123', { orderId: '123' })
   * await job.queue('order-456', { orderId: '456' }, undefined, { delay: 5000 })
   * ```
   */
  async queue(runId: string, inputs: TInputs, manager?: RedisJM, options?: QueueOptions): Promise<boolean> {
    return this.resolveManager(manager).queue(this as Job<any, any>, runId, inputs, options)
  }

  /**
   * Convenience method to priority-insert this job (front of the queue) via a RedisJM instance.
   * Mirrors {@link queue} but delegates to `manager.queueFirst`. Uses the provided manager or the
   * default manager set in the constructor.
   *
   * @param runId - Unique identifier for this run (duplicates are rejected)
   * @param inputs - The job inputs to store and pass at execution time
   * @param manager - Optional RedisJM instance (overrides the default)
   * @param options - Optional queue options; a priority insert cannot be delayed (`delay > 0` throws)
   * @returns `true` if queued; `false` if not queued (deduped or full — see {@link enqueue}).
   *   Redis failures throw `RedisJMEnqueueError`.
   *
   * @example
   * ```ts
   * const queued = await job.queueFirst('urgent-order', { orderId: '456' })
   * ```
   */
  async queueFirst(runId: string, inputs: TInputs, manager?: RedisJM, options?: QueueOptions): Promise<boolean> {
    return this.resolveManager(manager).queueFirst(this as Job<any, any>, runId, inputs, options)
  }

  /**
   * Resolves the RedisJM instance to queue through: the explicit `manager` argument if given, else
   * the default manager set in the constructor. Throws if neither is available (a missing manager is
   * an error, not a silent no-op).
   */
  private resolveManager(manager?: RedisJM): RedisJM {
    const mgr = manager ?? this.defaultManager
    if (!mgr) {
      throw new Error('No RedisJM instance provided and no default manager set')
    }
    return mgr
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
   * Resolves this job's own execution timeout: `JobMetadata.timeoutMs` when set (`0` = explicitly no
   * timeout, overriding a manager default), else `undefined` (defer to the manager's `jobTimeout`).
   * Negative / non-finite values are treated as `0` (no timeout).
   */
  getTimeoutMs(): number | undefined {
    const raw = this.metadata.timeoutMs
    return raw === undefined ? undefined : positiveOrZero(raw)
  }

  /**
   * This job's own abort grace (`JobMetadata.abortGraceMs`): a number of ms, `false` (explicitly none,
   * overriding a manager default), or `undefined` (defer to the manager's `abortGraceMs`).
   */
  getAbortGraceMs(): number | false | undefined {
    return this.metadata.abortGraceMs
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
