import { Hookable } from 'hookable'
import type Redis from 'ioredis'
import { Job } from './job'
import { createMaintenanceJob, MAINTENANCE_JOB_NAME, MAINTENANCE_LANE } from './maintenance'
import { toError } from './utils'
import type {
  JobAttrs,
  JobAttrValue,
  JobErrorEventPayload,
  JobEventPayload,
  JobFunction,
  JobLogRecord,
  JobMetadata,
  JobRetryEventPayload,
  JobUpdateEventPayload,
  MaintenanceResult,
  QueueOptions,
  RedisJMHooks,
  RedisJMLogger,
  RedisJMOptions,
  ResolvedRedisJMOptions,
} from './types'

const DEFAULT_OPTIONS: Omit<ResolvedRedisJMOptions, 'maintenanceInterval'> = {
  heartbeatInterval: 5000,
  roundsToStale: 2,
  keepFinishedInterval: 0,
  unknownJobRequeueLimit: 5,
  laneStrategy: 'roundRobin',
  lanePriority: [],
}

/** Default logger: writes to `console.error`, prefers the error stack when present. */
const DEFAULT_LOGGER: RedisJMLogger = (message, error) => {
  if (error?.stack) {
    console.error(`[redisjm] ${message}\n${error.stack}`)
  } else if (error) {
    console.error(`[redisjm] ${message}`, error)
  } else {
    console.error(`[redisjm] ${message}`)
  }
}

const NOOP_LOGGER: RedisJMLogger = () => {}

/**
 * Thrown out of an execution whose popped queue entry no longer owns its log record — another run
 * (a successor re-enqueued under the same runId, or a concurrent claimant) has claimed it first.
 * Callers of `popAndExecute` inside the lib treat it as a benign skip: the record's new owner is
 * responsible for its lock and log, so the superseded execution must touch neither.
 */
export class RunSupersededError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'RunSupersededError'
  }
}

/**
 * Redis Job Manager for distributed job queues.
 *
 * Manages job scheduling, execution, and lifecycle using three Redis structures per target group:
 * a List (queue), a Set (locks), and a Hash (log).
 *
 * @example
 * ```ts
 * const manager = new RedisJM(redis, 'my-app', { heartbeatInterval: 5000 })
 * const job = manager.createJob({ jobName: 'send-email' }, async (inputs, ctx) => {
 *   await ctx.setProgress(0.5)
 *   // ... send email
 *   await ctx.setProgress(1)
 * })
 * await job.queue('daily-digest', { to: 'user@example.com' })
 * manager.start(1000)
 * ```
 */
export class RedisJM extends Hookable<RedisJMHooks> {
  private readonly redis: Redis
  private readonly targetGroup: string
  private readonly options: ResolvedRedisJMOptions
  private readonly logger: RedisJMLogger
  private readonly registeredJobs = new Set<Job<any, any>>()
  private readonly jobsByName = new Map<string, Job<any, any>>()
  private readonly jobHookCleanups = new Map<Job<any, any>, () => void>()

  private pollTimer: ReturnType<typeof setTimeout> | undefined
  private polling = false
  private maintenanceTimer: ReturnType<typeof setInterval> | undefined
  /** Promise of the job currently being executed by the poll loop, if any. Awaited by `stop()`. */
  private inFlight: Promise<unknown> | undefined
  /** Round-robin cursor rotating the work-lane poll order once per poll to prevent starvation. */
  private pollCursor = 0
  /** Lazy `LMPOP` capability flag: `undefined` = not yet probed, `false` = fall back to sequential `LPOP`. */
  private lmpopSupported: boolean | undefined
  /** Epoch ms of the last delayed-set promotion sweep; rate-limits `promoteDueDelayed` (see there). */
  private lastPromotionCheck = 0

  /**
   * @param redis - An ioredis client instance
   * @param targetGroup - String prefix for all Redis keys; only managers sharing the same target group share queues
   * @param options - Optional configuration for heartbeat, stale detection, log retention, and auto-maintenance
   */
  constructor(redis: Redis, targetGroup: string, options?: RedisJMOptions) {
    super()
    if (targetGroup.includes(':')) {
      // Keys embed a `:lane:` infix; a ':' in the group would let two distinct (group, lane)
      // pairs alias onto one Redis key (e.g. group "portal" + lane "images" vs. a group literally
      // named "portal:lane:images"). Reject it to keep the lane-key infix collision-proof.
      throw new Error(`Target group "${targetGroup}" must not contain ":" (it would collide with the lane-key infix)`)
    }
    this.redis = redis
    this.targetGroup = targetGroup
    const heartbeatInterval = options?.heartbeatInterval ?? DEFAULT_OPTIONS.heartbeatInterval
    const roundsToStale = options?.roundsToStale ?? DEFAULT_OPTIONS.roundsToStale
    this.options = {
      heartbeatInterval,
      roundsToStale,
      keepFinishedInterval: options?.keepFinishedInterval ?? DEFAULT_OPTIONS.keepFinishedInterval,
      maintenanceInterval: options?.maintenanceInterval ?? heartbeatInterval * roundsToStale,
      unknownJobRequeueLimit: options?.unknownJobRequeueLimit ?? DEFAULT_OPTIONS.unknownJobRequeueLimit,
      laneStrategy: options?.laneStrategy ?? DEFAULT_OPTIONS.laneStrategy,
      lanePriority: options?.lanePriority ?? DEFAULT_OPTIONS.lanePriority,
    }
    this.logger = options?.logger === false ? NOOP_LOGGER : (options?.logger ?? DEFAULT_LOGGER)
  }

  /** Returns the target group identifier for this manager. */
  getTargetGroup(): string {
    return this.targetGroup
  }

  /** Returns a copy of the resolved options with defaults applied. */
  getOptions(): ResolvedRedisJMOptions {
    return { ...this.options }
  }

  /**
   * Checks if a jobId is currently locked (queued, running, or stale).
   *
   * @example
   * ```ts
   * const locked = await manager.isQueued('send-email#daily-digest')
   * ```
   */
  async isQueued(jobId: string): Promise<boolean> {
    const result = await this.redis.sismember(this.getLocksKey(), jobId)
    return result === 1
  }

  /**
   * Adds a job run to the end of the queue. Returns `true` if queued, `false` if already locked.
   * Pass `options.delay` (ms) to stage the run on the delayed set instead of the live queue.
   *
   * @example
   * ```ts
   * const success = await manager.queue(job, 'order-123', { orderId: '123' })
   * await manager.queue(job, 'order-456', { orderId: '456' }, { delay: 5000 })
   * ```
   */
  async queue<TInputs>(job: Job<TInputs, any>, runId: string, inputs: TInputs, options?: QueueOptions): Promise<boolean> {
    return this.enqueue(job, runId, inputs, 'rpush', options)
  }

  /**
   * Adds a job run to the front of the queue (priority insert). Returns `true` if queued, `false` if already locked.
   * A priority insert cannot be delayed — passing `options.delay > 0` throws a TypeError.
   *
   * @example
   * ```ts
   * await manager.queueFirst(job, 'urgent-order', { orderId: '456' })
   * ```
   */
  async queueFirst<TInputs>(job: Job<TInputs, any>, runId: string, inputs: TInputs, options?: QueueOptions): Promise<boolean> {
    return this.enqueue(job, runId, inputs, 'lpush', options)
  }

  /**
   * Returns all job log records (all statuses).
   *
   * @example
   * ```ts
   * const records = await manager.list()
   * records.forEach(r => console.log(r.jobId, r.status, r.progress))
   * ```
   */
  async list(): Promise<JobLogRecord[]> {
    // Incremental HSCAN (not a single blocking HGETALL) so a large log hash is read in bounded
    // slices — matters once retention (`keepFinishedInterval > 0`) lets the hash grow.
    const entries = await this.scanHash(this.getLogKey())
    const records: JobLogRecord[] = []
    for (const [jobId, val] of entries) {
      // A single corrupt/foreign record must not take down the whole listing.
      const record = this.parseRecord(val, jobId)
      if (record) records.push(record)
    }
    return records
  }

  /**
   * Fetches a single job log record by `jobId` (`"jobName#runId"`), or `undefined` if absent.
   *
   * Note: with the default `keepFinishedInterval: 0`, finished/error records are deleted the
   * moment the job leaves `running`, so a successful run returns `undefined` here — `undefined`
   * means "no record", not "never ran". Set `keepFinishedInterval > 0` to observe terminal states.
   *
   * @example
   * ```ts
   * const record = await manager.get('send-email#daily-digest')
   * if (record?.status === 'finished') { ... }
   * ```
   */
  async get(jobId: string): Promise<JobLogRecord | undefined> {
    return (await this.readRecord(jobId)) ?? undefined
  }

  /**
   * Removes a job from the queue, locks, and log entirely.
   *
   * @example
   * ```ts
   * await manager.unqueue('send-email#daily-digest')
   * ```
   */
  async unqueue(jobId: string): Promise<void> {
    // Resolve the lane from the persisted record so we LREM the correct lane queue; if the record
    // is already gone, fall back to the default key (the locks/log removals below are group-wide).
    const record = await this.readRecord(jobId)
    await this.redis.lrem(this.getQueueKey(record?.lane), 1, jobId)
    // Also drop any delayed-set entry for this jobId (a `delayed`/retry-scheduled run lives there,
    // not on the queue list); harmless no-op for a non-delayed run.
    await this.redis.zrem(this.getDelayedKey(), jobId)
    await this.redis.srem(this.getLocksKey(), jobId)
    await this.redis.hdel(this.getLogKey(), jobId)
    // Also clear any pending orphan-suspicion for this jobId: a manually removed run must not leave
    // a suspects entry that a later maintenance pass would act on (e.g. re-SREM a re-added lock).
    await this.redis.hdel(this.getSuspectsKey(), jobId)
  }

  /**
   * Creates a new Job instance and registers it with this manager. Job names must be unique per manager.
   *
   * @example
   * ```ts
   * const job = manager.createJob(
   *   { jobName: 'process-order' },
   *   async (inputs: { orderId: string }, ctx) => {
   *     await ctx.setProgress(0.5)
   *     await ctx.setAttrs({ step: 'processing' })
   *   }
   * )
   * ```
   */
  createJob<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs>(
    metadata: JobMetadata,
    fn: JobFunction<TInputs, TAttrs>,
  ): Job<TInputs, TAttrs> {
    const job = new Job<TInputs, TAttrs>(metadata, fn, this)
    this.registerJob(job)
    return job
  }

  /**
   * Registers an existing Job instance by name. Hooks all job events to update Redis and re-dispatch.
   * Throws if a job with the same name is already registered.
   *
   * @example
   * ```ts
   * const job = new Job({ jobName: 'sync' }, syncFn)
   * manager.registerJob(job)
   * ```
   */
  registerJob<TInputs, TAttrs extends { [K in keyof TAttrs]: JobAttrValue }>(job: Job<TInputs, TAttrs>): void {
    if (this.registeredJobs.has(job)) return

    const jobName = job.getName()
    if (jobName.includes('#')) {
      // jobId is `${jobName}#${runId}` parsed on the first '#', so a '#' in the name
      // would alias distinct (jobName, runId) pairs onto the same lock/log key.
      throw new Error(`Job name "${jobName}" must not contain "#"`)
    }
    // Early feedback for a bad lane declaration (enqueue re-validates on the producer path).
    this.validateLane(job.getLane(), jobName)
    if (this.jobsByName.has(jobName)) {
      throw new Error(`Job with name "${jobName}" is already registered`)
    }

    this.registeredJobs.add(job)
    this.jobsByName.set(jobName, job)

    const getJobId = (runId: string) => job.getJobId(runId)

    const onStart = async (payload: JobEventPayload<TInputs>) => {
      if (!this.shouldHandle(payload)) return
      const jobId = getJobId(payload.runId)
      // `start` is the CLAIM: it only fires on a record that is still `queued`, flipping it to
      // `running` and stamping this execution's fencing token. A rejected claim means the entry we
      // popped no longer owns its record — a successor that re-enqueued the same runId (fresh
      // `queued` record) or a concurrent claimant owns it now — so this execution is superseded.
      const result = await this.updateLog(jobId, (record) => {
        if (record.status !== 'queued') return false
        record.status = 'running'
        record.startedAt = Date.now()
        record.heartbeat = Date.now()
        record.executionId = payload.executionId
        record.attempt = (record.attempt ?? 0) + 1
        delete record.suspectedAt
      })
      if (result === 'rejected') {
        throw new RunSupersededError(`run "${jobId}" was superseded before it could claim its record`)
      }
      // 'missing' → no backing record (direct `job.execute()` pattern); proceed silently.
      await this.callHook('start', payload as unknown as JobEventPayload)
    }

    const onFinish = async (payload: JobEventPayload<TInputs>) => {
      if (!this.shouldHandle(payload)) return
      const jobId = getJobId(payload.runId)
      const now = Date.now()
      // Fences the zombie-run scenario: a stalled handler is staled by maintenance (lock released),
      // a producer re-enqueues the same runId (fresh record, NO executionId), then the original
      // handler finally finishes. Its executionId no longer matches, so the mutator rejects — the
      // zombie can't overwrite the successor's record, srem its lock, or (under keepFinishedInterval=0)
      // delete it out from under a run that hasn't happened yet.
      const result = await this.updateLog(jobId, (record) => {
        if (record.executionId !== payload.executionId) return false
        record.status = 'finished'
        record.finishedAt = now
      })
      // 'rejected' → the record's owner changed under us: touch nothing, and skip the manager-level
      // event too (it doesn't describe the record's current owner).
      if (result === 'rejected') return
      // 'written' or 'missing' (record unqueued mid-run — the srem is a harmless no-op).
      await this.releaseLockAndMaybeDropLog(jobId)
      await this.callHook('finish', payload as unknown as JobEventPayload)
    }

    const onError = async (payload: JobErrorEventPayload<TInputs>) => {
      if (!this.shouldHandle(payload)) return
      const jobId = getJobId(payload.runId)
      const now = Date.now()
      const maxAttempts = job.getAttempts()
      // The mutator is synchronous, so compute the retry-vs-final decision inside it (setting the
      // record fields accordingly) and capture the outcome in this closure; then act on it after the
      // write. `scheduledRetry` set ⇒ we chose to retry.
      let scheduledRetry: { readyAt: number; attempt: number } | undefined
      const result = await this.updateLog(jobId, (record) => {
        // Same fencing as `onFinish`: a superseded execution's error must not clobber the successor's
        // record, schedule a phantom retry, or release its lock.
        if (record.executionId !== payload.executionId) return false
        // The 1-based attempt that just failed (stamped by onStart's claim).
        const attempt = record.attempt ?? 1
        // Last error kept for observability in BOTH branches.
        record.error = payload.error.message
        if (attempt < maxAttempts) {
          // RETRY: stage the run back on the delayed set and KEEP the lock (do not srem) — the held
          // lock is what prevents a duplicate enqueue of the same runId while the backoff elapses.
          // `finishedAt` stays unset (the run isn't terminal). Backoff 0 still routes through the
          // delayed set (readyAt = now) so promotion has a single code path; the next sweep picks it
          // up within ~1s.
          const readyAt = now + job.getBackoffMs(attempt)
          record.status = 'delayed'
          record.readyAt = readyAt
          scheduledRetry = { readyAt, attempt }
        } else {
          // FINAL failure: terminal error (lock released + `error` hook fired below).
          record.status = 'error'
          record.finishedAt = now
        }
      })
      // Zombie fencing: the record's owner changed under us — touch nothing, fire nothing.
      if (result === 'rejected') return
      if (result === 'written' && scheduledRetry) {
        // Write the zset entry BEFORE the retry hook (mirrors enqueue: the zset entry is what makes
        // the run promotable). Do NOT release the lock and do NOT fire the manager-level `error` hook
        // — the run isn't finally failed, so `retry` fires instead.
        await this.redis.zadd(this.getDelayedKey(), scheduledRetry.readyAt, jobId)
        await this.callHook('retry', {
          ...(payload as unknown as JobErrorEventPayload),
          attempt: scheduledRetry.attempt,
          nextAttemptAt: scheduledRetry.readyAt,
        } as JobRetryEventPayload)
        return
      }
      // Final failure (or a 'missing' record — unqueued mid-run): existing behavior exactly.
      await this.releaseLockAndMaybeDropLog(jobId)
      await this.callHook('error', payload as unknown as JobErrorEventPayload)
    }

    const onHeartbeat = async (payload: JobEventPayload<TInputs>) => {
      if (!this.shouldHandle(payload)) return
      const jobId = getJobId(payload.runId)
      await this.updateLog(jobId, (record) => {
        // Never refresh the heartbeat of a record that has left `running` or belongs to a different
        // execution: a late/straggling heartbeat from a finished or superseded run must not resurrect
        // (or keep alive) a record now owned by someone else.
        if (record.status !== 'running' || record.executionId !== payload.executionId) return false
        record.heartbeat = Date.now()
      })
      await this.callHook('heartbeat', payload as unknown as JobEventPayload)
    }

    const onUpdate = async (payload: JobUpdateEventPayload<TInputs, TAttrs>) => {
      if (!this.shouldHandle(payload)) return
      const jobId = getJobId(payload.runId)
      await this.updateLog(jobId, (record) => {
        // Same fencing as heartbeat: only the running execution that owns the record may write its
        // progress/attrs, so a superseded run's late update can't leak into the successor's record.
        if (record.status !== 'running' || record.executionId !== payload.executionId) return false
        if (payload.progress !== undefined) record.progress = payload.progress
        // Merge, not replace: successive setAttrs calls accumulate keys instead of clobbering.
        if (payload.attrs !== undefined) record.attrs = { ...record.attrs, ...payload.attrs }
      })
      await this.callHook('update', payload as unknown as JobUpdateEventPayload)
    }

    job.hook('start', onStart)
    job.hook('finish', onFinish)
    job.hook('error', onError)
    job.hook('heartbeat', onHeartbeat)
    job.hook('update', onUpdate)

    this.jobHookCleanups.set(job, () => {
      job.removeHooks({
        start: onStart,
        finish: onFinish,
        error: onError,
        heartbeat: onHeartbeat,
        update: onUpdate,
      })
    })
  }

  /**
   * Unregisters a Job and removes all event hooks.
   *
   * @example
   * ```ts
   * manager.unregisterJob(job)
   * ```
   */
  unregisterJob<TInputs, TAttrs extends { [K in keyof TAttrs]: JobAttrValue }>(job: Job<TInputs, TAttrs>): void {
    const cleanup = this.jobHookCleanups.get(job)
    if (cleanup) {
      cleanup()
      this.jobHookCleanups.delete(job)
    }
    this.jobsByName.delete(job.getName())
    this.registeredJobs.delete(job)
  }

  /**
   * Pops the next job from the queue, matches it to a registered Job by name, and executes it.
   * Returns `true` if a job was popped, `false` if the queue was empty.
   *
   * @example
   * ```ts
   * const hadWork = await manager.popAndExecute()
   * ```
   */
  async popAndExecute(): Promise<boolean> {
    // Promote any due delayed runs onto their lane queues before popping, so a run whose delay just
    // elapsed becomes poppable on this same pass.
    await this.promoteDueDelayed()

    // Poll the union of this instance's subscribed lanes (`__maintenance` first, then work lanes
    // ordered by strategy) atomically via `LMPOP`, with a sequential-`LPOP` fallback for Redis < 7.
    const keys = this.getSubscribedQueueKeys()
    const jobId = await this.popFromLanes(keys)
    if (!jobId) return false

    const separatorIndex = jobId.indexOf('#')
    if (separatorIndex === -1) {
      await this.redis.srem(this.getLocksKey(), jobId)
      await this.redis.hdel(this.getLogKey(), jobId)
      return true
    }

    const jobName = jobId.slice(0, separatorIndex)
    const runId = jobId.slice(separatorIndex + 1)

    const job = this.jobsByName.get(jobName)
    if (!job) {
      // This instance has no handler for the popped name. In a rolling deploy / blue-green
      // topology a sibling instance may have it, so re-queue (keeping the lock held) up to
      // `unknownJobRequeueLimit` times before giving up and recording the error. A successful
      // re-queue returns `false` (treated as "no work executed") so the poll loop applies its
      // idle interval rather than immediately re-popping — spacing retries by `interval` gives
      // a sibling that *does* have the handler real wall-clock time to claim it.
      if (await this.requeueUnknownJob(jobId)) return false

      const now = Date.now()
      await this.updateLog(jobId, (record) => {
        record.status = 'error'
        record.error = 'Job name is unknown'
        record.finishedAt = now
      })
      this.logger(`job "${jobId}" has no registered handler on this instance; dropping`)
      await this.releaseLockAndMaybeDropLog(jobId)
      return true
    }

    const logJson = await this.redis.hget(this.getLogKey(), jobId)
    if (!logJson) {
      // Popped a queue entry with no backing log record (desynced/cleaned state). Release
      // the lock and surface it rather than dropping the run completely silently.
      this.logger(`job "${jobId}" popped with no log record; dropping`)
      await this.redis.srem(this.getLocksKey(), jobId)
      return true
    }

    const logRecord = this.parseRecord(logJson, jobId)
    if (!logRecord) {
      // Corrupt/foreign record: drop the record and its lock unconditionally (garbage — retention
      // doesn't apply) rather than routing through the retention-aware release, which is what keeps
      // `keepFinishedInterval > 0` from hoarding it forever.
      await this.dropGarbageRecordAndLock(jobId)
      return true
    }
    try {
      await job.execute(logRecord.inputs, {
        targetGroup: this.targetGroup,
        heartbeatInterval: this.options.heartbeatInterval,
        runId,
        // Identify this manager as the driver (so only its hooks act) and route infra errors here.
        manager: this,
        logger: this.logger,
      })
    } catch (err) {
      if (err instanceof RunSupersededError) {
        // The entry we popped no longer owns its record — a successor/concurrent claimant does, and
        // is responsible for the lock and log. Skip without touching either (a re-push would
        // duplicate the successor's queued entry; an srem would free the successor's lock).
        this.logger(`job "${jobId}" superseded; skipping`)
        return true
      }
      // The job's `error` event already recorded the failure in Redis and re-broadcast it.
      // Surface it through the logger too, so a thrown handler is never fully silent when no
      // `error` hook is wired (and to catch infra/hook failures, which are NOT "already handled").
      const error = toError(err)
      this.logger(`job "${jobId}" failed: ${error.message}`, error)
    }

    return true
  }

  /**
   * Computes this instance's subscribed queue keys per poll (§5.6): the reserved `__maintenance`
   * lane first, then the DISTINCT work-lane queue keys of the registered jobs, ordered by strategy.
   *
   * Deduplication is on the resolved queue key (not the lane name) so a no-lane job and an explicit
   * `lane: 'default'` job don't double-weight the legacy key. Under `roundRobin` (default) the work
   * keys rotate by a per-manager cursor advanced once per poll (anti-starvation); under `priority`
   * the `lanePriority` order wins with unlisted work lanes trailing in registration order. The result
   * is always length ≥ 1 (`__maintenance`), so `numkeys` is never 0.
   */
  private getSubscribedQueueKeys(): string[] {
    const maintenanceKey = this.getQueueKey(MAINTENANCE_LANE)

    // Distinct work-lane keys in registration order, excluding the reserved maintenance key.
    const workKeys: string[] = []
    const seen = new Set<string>([maintenanceKey])
    for (const job of this.registeredJobs) {
      const key = this.getQueueKey(job.getLane())
      if (seen.has(key)) continue
      seen.add(key)
      workKeys.push(key)
    }

    let orderedWorkKeys: string[]
    if (this.options.laneStrategy === 'priority') {
      // Listed lanes first (in `lanePriority` order), then the remaining work keys in registration
      // order. `workKeys` is already distinct, so a single set consumed via `Set.delete` handles
      // both the "am I subscribed?" test and the "don't emit twice" guard in one step.
      const remaining = new Set(workKeys)
      const ordered: string[] = []
      for (const lane of this.options.lanePriority) {
        // `delete` returns true only when `key` is a still-unemitted work key.
        const key = this.getQueueKey(lane)
        if (remaining.delete(key)) ordered.push(key)
      }
      // `remaining` now holds only the non-priority work keys; emit them in registration order.
      for (const key of workKeys) {
        if (remaining.delete(key)) ordered.push(key)
      }
      orderedWorkKeys = ordered
    } else {
      // roundRobin: rotate the work-key list by the cursor, then advance it once per poll (only when
      // there is work to rotate) so no lane is starved by a saturated higher-order lane.
      const n = workKeys.length
      if (n > 0) {
        const offset = this.pollCursor % n
        orderedWorkKeys = [...workKeys.slice(offset), ...workKeys.slice(0, offset)]
        this.pollCursor++
      } else {
        orderedWorkKeys = workKeys
      }
    }

    return [maintenanceKey, ...orderedWorkKeys]
  }

  /**
   * Pops the next jobId from the first non-empty of `keys` (in order) via `LMPOP <n> <keys...> LEFT`,
   * falling back to a sequential `LPOP` per key for Redis < 7. The `LMPOP` capability is probed
   * lazily and cached: an "unknown command" error on the first attempt (raised BEFORE anything is
   * popped, so no work is lost) flips the flag and switches to the fallback permanently.
   */
  private async popFromLanes(keys: string[]): Promise<string | null> {
    if (this.lmpopSupported !== false) {
      try {
        const res = await this.redis.lmpop(keys.length, ...keys, 'LEFT')
        this.lmpopSupported = true
        return res ? (res[1][0] ?? null) : null
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err)
        if (this.lmpopSupported === undefined && message.toLowerCase().includes('unknown command')) {
          this.lmpopSupported = false
          // fall through to the sequential path
        } else {
          throw err
        }
      }
    }

    for (const key of keys) {
      const v = await this.redis.lpop(key)
      if (v) return v
    }
    return null
  }

  /**
   * Promotes delayed runs whose `readyAt` has elapsed onto their lane queues. For each due id: `ZREM`
   * first — a reply of 1 is the atomic claim (a racing instance may sweep the same entry; the loser
   * skips) — then flip the record `delayed`→`queued` (clearing `readyAt`) and `RPUSH` it onto its
   * persisted lane. A won ZREM whose record is missing/rejected is garbage: log it and release the lock.
   *
   * Rate-limited to at most once per 1000 ms via `lastPromotionCheck`: `popAndExecute` calls this on
   * every pop, and a busy queue re-polls immediately (interval 0), so without the cap each pop would
   * pay a ZRANGEBYSCORE. A delayed run still becomes poppable within ~1s of its `readyAt`, well inside
   * the coarse delay/backoff granularity this targets.
   */
  private async promoteDueDelayed(): Promise<void> {
    const now = Date.now()
    if (now - this.lastPromotionCheck < 1000) return
    this.lastPromotionCheck = now

    const delayedKey = this.getDelayedKey()
    // Bounded batch: each due id costs ~3 serial round trips (ZREM → updateLog → RPUSH), so cap the
    // sweep at 8 ids/pass to keep the promote step (awaited on the pop hot path) cheap. Combined with
    // the 1000 ms rate-limit this promotes ~8 delayed runs/sec/instance; anything still due is picked
    // up on the next sweep. Ids are independent (distinct hash fields / lane lists, and Redis resolves
    // the ZREM claim race per-member), so promote them concurrently — each id's own ZREM→flip→RPUSH
    // ordering is preserved within its chain.
    const dueIds = await this.redis.zrangebyscore(delayedKey, '-inf', now, 'LIMIT', 0, 8)
    await Promise.all(dueIds.map(async (jobId) => {
      // The ZREM is the claim. Crash window: an instance that ZREM'd then died before the flip/RPUSH
      // leaves the record `delayed` (or `queued`) but absent from the zset — maintenance's delayed
      // branch reclaims it via the same two-pass suspectedAt flow as an orphaned queued record.
      if ((await this.redis.zrem(delayedKey, jobId)) !== 1) return

      // Capture the lane inside the (synchronous) mutator so the RPUSH targets the record's own lane.
      let lane: string | undefined
      const result = await this.updateLog(jobId, (record) => {
        // Only a still-`delayed` record is promotable; anything else means the won claim points at a
        // record that no longer owns this delayed slot.
        if (record.status !== 'delayed') return false
        record.status = 'queued'
        delete record.readyAt
        lane = record.lane
      })
      if (result === 'written') {
        await this.redis.rpush(this.getQueueKey(lane), jobId)
      } else {
        // Missing or rejected after a won ZREM: a delayed entry without a healthy `delayed` record is
        // garbage. Release the lock so the runId isn't blocked forever.
        this.logger(`delayed job "${jobId}" had no promotable record; releasing lock`)
        await this.redis.srem(this.getLocksKey(), jobId)
      }
    }))
  }

  /**
   * Re-queues a job whose name is not registered on this instance, keeping its lock held so a
   * concurrent enqueue of the same runId can't duplicate it. Returns `true` if re-queued,
   * `false` if the retry budget is exhausted (or disabled) and the caller should record an error.
   */
  private async requeueUnknownJob(jobId: string): Promise<boolean> {
    const limit = this.options.unknownJobRequeueLimit
    if (limit <= 0) return false

    const record = await this.readRecord(jobId)
    if (!record) return false

    const count = record.requeueCount ?? 0
    if (count >= limit) return false

    record.requeueCount = count + 1
    record.status = 'queued'
    await this.redis.hset(this.getLogKey(), jobId, JSON.stringify(record))
    // Back of the queue (not the front) so this doesn't starve handleable work, and the lock
    // is intentionally left in place. RPUSH to the record's own lane so a laned unknown job stays
    // on its lane for a same-lane sibling to claim (end-to-end coverage lands with the step-3 consumer).
    await this.redis.rpush(this.getQueueKey(record.lane), jobId)
    return true
  }

  /**
   * Starts a polling loop that calls `popAndExecute()`. Polls immediately after a job executes;
   * waits `interval` ms when the queue is empty.
   *
   * Unless `maintenanceInterval` is `0`, also enqueues the built-in maintenance job — once
   * immediately (so locks orphaned by a crash are reclaimed soon after restart) and then every
   * `maintenanceInterval` ms. All instances enqueue concurrently; the lock dedupes the runs.
   * Before the first enqueue it also proactively reclaims a stale maintenance lock left by a
   * hard-killed instance, which would otherwise deadlock maintenance (it can't reclaim its own lock).
   *
   * @param interval - Milliseconds to wait between polls when idle (must be a positive number)
   *
   * @example
   * ```ts
   * manager.start(1000)
   * ```
   */
  start(interval: number): void {
    if (!Number.isFinite(interval) || interval <= 0) {
      throw new TypeError(`start(interval): interval must be a positive number, got ${interval}`)
    }
    if (this.polling) return
    this.polling = true

    if (this.options.maintenanceInterval > 0) {
      const job = this.jobsByName.get(MAINTENANCE_JOB_NAME) ?? createMaintenanceJob(this)
      const tryQueue = () => {
        // Don't enqueue after stop(): the bootstrap reclaim chain below is async, so a stop()
        // that lands before it resolves would otherwise leave an orphaned maintenance entry.
        if (!this.polling) return
        this.queue(job, '', null).catch(() => {})
      }
      // Reclaim a stale maintenance lock first (no-op when none), THEN enqueue — so a lock
      // orphaned by a hard kill can't permanently block maintenance from running.
      this.reclaimStaleMaintenanceLock().then(tryQueue, tryQueue)
      this.maintenanceTimer = setInterval(tryQueue, this.options.maintenanceInterval)
    }

    const poll = async () => {
      if (!this.polling) return
      let executed = false
      try {
        this.inFlight = this.popAndExecute()
        executed = (await this.inFlight) as boolean
      } catch (err) {
        // Keep the loop alive, but surface the failure instead of swallowing it silently.
        this.logger('poll loop error', toError(err))
      } finally {
        this.inFlight = undefined
      }
      if (!this.polling) return
      const delay = executed ? 0 : interval
      this.pollTimer = setTimeout(poll, delay)
    }
    poll()
  }

  /**
   * Stops the polling loop and returns a promise that resolves once the in-flight job (if any)
   * has settled, so callers (e.g. a SIGTERM handler) can await a graceful drain before exiting.
   *
   * @example
   * ```ts
   * process.on('SIGTERM', async () => { await manager.stop() })
   * ```
   */
  stop(): Promise<void> {
    this.polling = false
    if (this.pollTimer) {
      clearTimeout(this.pollTimer)
      this.pollTimer = undefined
    }
    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer)
      this.maintenanceTimer = undefined
    }
    // Swallow a rejected in-flight here — the poll loop already logs it; `stop()` should resolve.
    return Promise.resolve(this.inFlight).then(() => {}, () => {})
  }

  /**
   * Reclaims the built-in maintenance job's lock if it was orphaned by a hard-killed instance.
   * Because maintenance is the only thing that reclaims stale `running` locks, a maintenance run
   * killed mid-execution would hold its own lock forever and block all future reclamation for the
   * group — this breaks that deadlock at startup.
   *
   * Only reclaims locks that are *demonstrably* stale: a `running` record past the heartbeat
   * threshold, or a lock with no backing record. It intentionally does NOT reclaim a `queued`
   * record absent from the queue — that state is indistinguishable from a live instance's normal
   * pop→start window, so reclaiming it would race a healthy run. (A maintenance job orphaned in
   * that sub-millisecond window is the price of avoiding that race; prefer graceful `stop()` over
   * SIGKILL.)
   *
   * One cutover exception: a `queued` maintenance record still PRESENT on the legacy default queue
   * was enqueued by a pre-lanes (0.0.3) instance and would strand there — pure lane workers never
   * poll that key. It is relocated onto the `__maintenance` lane (lock kept held) so it can run.
   */
  private async reclaimStaleMaintenanceLock(): Promise<void> {
    const jobId = `${MAINTENANCE_JOB_NAME}#`
    const locksKey = this.getLocksKey()
    if ((await this.redis.sismember(locksKey, jobId)) !== 1) return

    const reclaim = async () => {
      await this.redis.srem(locksKey, jobId)
      await this.redis.hdel(this.getLogKey(), jobId)
    }

    const json = await this.redis.hget(this.getLogKey(), jobId)
    if (!json) {
      // Lock held with no backing record — unambiguously orphaned; reclaim it.
      await this.redis.srem(locksKey, jobId)
      return
    }

    const record = this.parseRecord(json, jobId)
    if (!record) {
      await reclaim()
      return
    }

    if (record.status === 'running') {
      const lastHeartbeat = record.heartbeat ?? record.startedAt ?? 0
      if (Date.now() - lastHeartbeat > this.getStaleThreshold()) await reclaim()
    } else if (record.status === 'queued') {
      // Cutover safety: a maintenance job enqueued by a pre-lanes (0.0.3) instance sits on the LEGACY
      // default queue, which pure lane workers never poll — it would strand there with the lock held and
      // deadlock group-wide maintenance. Relocate it onto the __maintenance lane (polled by every
      // lane-aware instance), keeping the lock held so no duplicate run is created. Safe against a
      // concurrent default-lane consumer: if that consumer already popped the entry, our LREM removes
      // nothing and we skip the re-push.
      const legacyKey = this.getQueueKey()
      if ((await this.redis.lpos(legacyKey, jobId)) !== null) {
        // Stamp the lane BEFORE moving the entry so a concurrent performMaintenance resolves the record
        // to the __maintenance lane (its two-pass orphan check tolerates the sub-ms move window).
        record.lane = MAINTENANCE_LANE
        await this.redis.hset(this.getLogKey(), jobId, JSON.stringify(record))
        if ((await this.redis.lrem(legacyKey, 1, jobId)) > 0) {
          await this.redis.rpush(this.getQueueKey(MAINTENANCE_LANE), jobId)
        }
      }
    }
  }

  /**
   * Scans the job log for stale and expired records:
   * 1. Marks running jobs as `"stale"` if heartbeat expired (`now - lastHeartbeat > heartbeatInterval * roundsToStale`)
   * 2. Marks orphaned queued jobs as `"stale"` — a `queued` record that is no longer in the queue
   *    list was popped by an instance that died before the `start` event fired. Detection is
   *    two-pass to avoid racing the normal pop→start window: the first scan stamps `suspectedAt`,
   *    a later scan reclaims the lock if the record is still orphaned past the stale threshold.
   * 3. Removes finished/error/stale records older than `keepFinishedInterval`, and deletes
   *    unparseable/foreign records (garbage that retention would otherwise hoard forever).
   * 4. Reclaims orphaned locks — a locks-set member with NO backing log record, which the
   *    record-driven loop above can never see. Same two-pass suspicion as stage 2 (via a
   *    `suspects` hash) so an in-flight enqueue isn't mistaken for a permanent orphan.
   *
   * @returns Counts of stale and cleaned records (orphaned locks count toward `staleCount`)
   *
   * @example
   * ```ts
   * const { staleCount, cleanedCount } = await manager.performMaintenance()
   * ```
   */
  async performMaintenance(): Promise<MaintenanceResult> {
    const logKey = this.getLogKey()
    const locksKey = this.getLocksKey()
    const suspectsKey = this.getSuspectsKey()
    // Incremental HSCAN over the log hash (bounded slices) rather than one blocking O(N) HGETALL.
    const entries = await this.scanHash(logKey)
    const now = Date.now()
    let staleCount = 0
    let cleanedCount = 0

    const staleThreshold = this.getStaleThreshold()

    // `entries` (a Map keyed by jobId) doubles as the "has a backing log record this scan" lookup
    // that stage 4 (below) uses to tell a lock that owns a record apart from a record-less orphan.
    for (const [jobId, json] of entries) {
      // Defense-in-depth: one corrupt/foreign record must not abort the whole sweep and
      // stall stale-reclaim + cleanup for every other job in the group.
      const record = this.parseRecord(json, jobId)
      if (!record) {
        // Unparseable/foreign record. Previously this branch `continue`d, so under
        // `keepFinishedInterval > 0` these accumulated in the hash forever. Drop it and its lock
        // unconditionally (garbage — retention doesn't apply); the jobId came straight from the hash
        // field, so releasing the matching lock is safe.
        await this.dropGarbageRecordAndLock(jobId)
        cleanedCount++
        continue
      }

      if (record.status === 'running') {
        const lastHeartbeat = record.heartbeat ?? record.startedAt ?? 0
        if (now - lastHeartbeat > staleThreshold) {
          record.status = 'stale'
          record.finishedAt = now
          await this.redis.hset(logKey, jobId, JSON.stringify(record))
          await this.redis.srem(locksKey, jobId)
          staleCount++
        }
      } else if (record.status === 'queued' || record.status === 'delayed') {
        // Liveness proof differs by status: a `queued` record must still be on its lane queue; a
        // `delayed` record must still be on the delayed zset. An overdue-but-present delayed entry is
        // healthy — promotion owns due entries, so maintenance must NOT stale it. The two-pass
        // suspectedAt reclaim below is identical for both (only the presence check above differs).
        const present = record.status === 'queued'
          ? (await this.redis.lpos(this.getQueueKey(record.lane), jobId)) !== null
          : (await this.redis.zscore(this.getDelayedKey(), jobId)) !== null
        if (!present) {
          if (record.suspectedAt === undefined) {
            record.suspectedAt = now
            await this.redis.hset(logKey, jobId, JSON.stringify(record))
          } else if (now - record.suspectedAt > staleThreshold) {
            record.status = 'stale'
            record.finishedAt = now
            delete record.suspectedAt
            await this.redis.hset(logKey, jobId, JSON.stringify(record))
            await this.redis.srem(locksKey, jobId)
            staleCount++
          }
        } else if (record.suspectedAt !== undefined) {
          delete record.suspectedAt
          await this.redis.hset(logKey, jobId, JSON.stringify(record))
        }
      } else if (record.status === 'finished' || record.status === 'error' || record.status === 'stale') {
        if (record.finishedAt !== undefined && now - record.finishedAt > this.options.keepFinishedInterval) {
          await this.redis.hdel(logKey, jobId)
          cleanedCount++
        }
      }
    }

    // Stage 4: orphaned-lock reclaim. A locks-set member with NO backing log record is invisible to
    // the record-driven loop above — it's created when `enqueue` crashes between its SADD and HSET
    // (or the rollback `.catch()` also fails): the runId stays locked forever (`queue()` returns
    // false, `isQueued()` true) with no record and no queue entry. Detect it two-pass, because an
    // enqueue mid-flight (SADD done, HSET a few ms later) is momentarily indistinguishable from a
    // permanent orphan. NOTE ON STAGE TIMING: a record enqueued *between* the log scan above and the
    // lock scan here would look orphaned — that transient false positive is exactly why detection is
    // two-pass (pass 1 suspects, pass 2 exonerates once the record lands), never single-pass.
    const lockedIds = await this.scanSet(locksKey)
    const suspects = await this.scanHash(suspectsKey)
    for (const member of lockedIds) {
      if (entries.has(member)) continue // has a record — reachable above, not an orphan
      const suspectedAt = suspects.get(member)
      if (suspectedAt === undefined) {
        // First sighting of a record-less lock: stamp when we first saw it and wait for a later pass.
        await this.redis.hset(suspectsKey, member, String(now))
      } else if (now - Number(suspectedAt) > staleThreshold) {
        // Still record-less past the stale threshold — a real queued-side loss, not an in-flight
        // enqueue. Release the lock and clear the suspicion. Counted as stale (same bucket as the
        // orphaned-queued reclaim in stage 2).
        await this.redis.srem(locksKey, member)
        await this.redis.hdel(suspectsKey, member)
        staleCount++
      }
    }

    // Exonerate suspects that no longer apply: the enqueue completed (a log record now exists) or the
    // lock was released elsewhere (unqueue / a concurrent reclaim). This is the second pass that makes
    // the false-positive window safe — an enqueue caught mid-flight on pass 1 is cleared here on pass 2.
    for (const [member] of suspects) {
      if (entries.has(member) || !lockedIds.has(member)) {
        await this.redis.hdel(suspectsKey, member)
      }
    }

    return { staleCount, cleanedCount }
  }

  // -- private helpers --

  /**
   * Decides whether a job event belongs to this manager. Two managers in one process can share one
   * Job (and even a targetGroup); this filters out events whose `manager` isn't this one, so only the
   * manager that DROVE the execution reacts (no double writes/events). Payloads without a `manager`
   * come from a direct `job.execute()` and keep the legacy targetGroup-only filtering.
   */
  private shouldHandle(payload: JobEventPayload<any>): boolean {
    if (payload.targetGroup !== this.targetGroup) return false
    if (payload.manager && payload.manager !== this) return false
    return true
  }

  /**
   * Releases a jobId's lock and, unless finished records are being retained
   * (`keepFinishedInterval > 0`), drops its log entry too. Shared by every terminal path
   * (finish, error, unknown-job drop, corrupt-record cleanup) so the retention policy lives in one spot.
   */
  private async releaseLockAndMaybeDropLog(jobId: string): Promise<void> {
    await this.redis.srem(this.getLocksKey(), jobId)
    if (this.options.keepFinishedInterval === 0) {
      await this.redis.hdel(this.getLogKey(), jobId)
    }
  }

  /**
   * Unconditionally drops a garbage record and its lock (SREM lock + HDEL record). Unlike
   * `releaseLockAndMaybeDropLog`, retention never applies here: the record is unparseable/foreign
   * garbage, so keeping it under `keepFinishedInterval > 0` would only hoard it forever. Shared by
   * the corrupt-record branches in `popAndExecute` and `performMaintenance` so that decision lives
   * in one spot.
   */
  private async dropGarbageRecordAndLock(jobId: string): Promise<void> {
    await this.redis.srem(this.getLocksKey(), jobId)
    await this.redis.hdel(this.getLogKey(), jobId)
  }

  /** Milliseconds a `running` heartbeat may lapse before the job is considered stale. */
  private getStaleThreshold(): number {
    return this.options.heartbeatInterval * this.options.roundsToStale
  }

  /**
   * Parses a stored log record, returning `null` (and logging) on corrupt/foreign JSON so a
   * single bad entry can never throw out of a scan, a hook, or the poll loop.
   */
  private parseRecord(json: string, jobId: string): JobLogRecord | null {
    let value: unknown
    try {
      value = JSON.parse(json)
    } catch {
      this.logger(`skipping unparseable log record "${jobId}"`)
      return null
    }
    // Shape guard: valid JSON that isn't a record object — a foreign field holding `"42"`,
    // `"true"`, `null`, or an array — would otherwise flow through `as JobLogRecord` as a record
    // with every property `undefined`. Require a non-null object with a string `status` before
    // trusting it; anything else is malformed and treated as garbage by the callers.
    if (typeof value !== 'object' || value === null || typeof (value as JobLogRecord).status !== 'string') {
      this.logger(`skipping malformed log record "${jobId}"`)
      return null
    }
    return value as JobLogRecord
  }

  /**
   * Incrementally reads a hash via `HSCAN` (COUNT 100) instead of one blocking O(N) `HGETALL`, so a
   * large log hash is loaded in bounded slices. HSCAN can return the same field across iterations
   * under concurrent writes, so results are deduped by field (last value wins).
   */
  private async scanHash(key: string): Promise<Map<string, string>> {
    const byField = new Map<string, string>()
    let cursor = '0'
    do {
      const [next, flat] = await this.redis.hscan(key, cursor, 'COUNT', 100)
      // HSCAN replies as a flat array alternating field, value, field, value, …
      for (let i = 0; i < flat.length; i += 2) {
        byField.set(flat[i], flat[i + 1])
      }
      cursor = next
    } while (cursor !== '0')
    return byField
  }

  /**
   * Incrementally reads a set via `SSCAN` (COUNT 100) instead of a single `SMEMBERS`. SSCAN may
   * repeat a member across iterations under concurrent writes, so members are deduped.
   */
  private async scanSet(key: string): Promise<Set<string>> {
    const members = new Set<string>()
    let cursor = '0'
    do {
      const [next, batch] = await this.redis.sscan(key, cursor, 'COUNT', 100)
      for (const m of batch) members.add(m)
      cursor = next
    } while (cursor !== '0')
    return members
  }

  /** Fetches and parses a single log record by `jobId`; `null` if absent or unparseable. */
  private async readRecord(jobId: string): Promise<JobLogRecord | null> {
    const json = await this.redis.hget(this.getLogKey(), jobId)
    return json ? this.parseRecord(json, jobId) : null
  }

  private async enqueue<TInputs>(
    job: Job<TInputs, any>,
    runId: string,
    inputs: TInputs,
    pushCmd: 'rpush' | 'lpush',
    options?: QueueOptions,
  ): Promise<boolean> {
    // Authoritative lane validation: the producer path never goes through registerJob, so validate
    // here (before taking the lock) as well as at registration.
    this.validateLane(job.getLane(), job.getName())

    const delay = options?.delay
    if (delay !== undefined) {
      if (!Number.isFinite(delay) || delay < 0) {
        throw new TypeError(`queue(options.delay): delay must be a finite number >= 0, got ${String(delay)}`)
      }
      // A priority insert stages the run at the FRONT of the live queue; there is no "front" of a
      // time-ordered delayed set, so combining the two is contradictory — reject it.
      if (delay > 0 && pushCmd === 'lpush') {
        throw new TypeError('queueFirst() cannot be combined with a delay — a priority insert cannot be delayed')
      }
    }

    const jobId = job.getJobId(runId)
    const locksKey = this.getLocksKey()

    const added = await this.redis.sadd(locksKey, jobId)
    if (added === 0) return false

    const isDelayed = delay !== undefined && delay > 0
    try {
      const record: JobLogRecord<TInputs> = {
        jobId,
        jobName: job.getName(),
        runId,
        inputs,
        targetGroup: this.targetGroup,
        // `undefined` for the default lane; JSON.stringify omits it, so a default-lane record
        // serializes byte-for-byte as in 0.0.3 (no `lane` key).
        lane: job.getLane(),
        // A delayed record holds the lock (dedupe still applies while waiting), like `queued`.
        status: isDelayed ? 'delayed' : 'queued',
        progress: 0,
      }
      if (isDelayed) {
        record.readyAt = Date.now() + delay!
      }
      // Write the log record BEFORE the queue/delayed entry: that entry is what makes the job
      // promotable/poppable, so if it landed first a concurrent poller could act on it before the
      // record exists. (A crash between these writes instead leaves a `queued`/`delayed` record
      // absent from its structure — reclaimed by maintenance.)
      await this.redis.hset(this.getLogKey(), jobId, JSON.stringify(record))
      if (isDelayed) {
        await this.redis.zadd(this.getDelayedKey(), record.readyAt!, jobId)
      } else {
        await this.redis[pushCmd](this.getQueueKey(job.getLane()), jobId)
      }
      return true
    } catch (err) {
      await this.redis.srem(locksKey, jobId).catch(() => {})
      await this.redis.hdel(this.getLogKey(), jobId).catch(() => {})
      // Roll back a possibly-written delayed entry too (mirrors the queue-list rollback).
      if (isDelayed) await this.redis.zrem(this.getDelayedKey(), jobId).catch(() => {})
      throw err
    }
  }

  /**
   * Read-modify-write of a single log record. Returns `'missing'` when there is no record,
   * `'rejected'` when the mutator returned `false` (a deliberate abort — the record isn't in a
   * state worth touching, or no longer belongs to this execution), and `'written'` otherwise.
   * The distinction lets fencing hooks tell "the record's owner changed" (rejected) apart from
   * "the record was unqueued/cleaned mid-run" (missing).
   */
  private async updateLog(
    jobId: string,
    mutate: (record: JobLogRecord) => void | boolean,
  ): Promise<'written' | 'rejected' | 'missing'> {
    const record = await this.readRecord(jobId)
    if (!record) return 'missing'
    if (mutate(record) === false) return 'rejected'
    await this.redis.hset(this.getLogKey(), jobId, JSON.stringify(record))
    return 'written'
  }

  /**
   * Resolves the queue-list key for a lane. The default lane (`undefined` or `'default'`) maps to
   * the exact legacy key `redisjm:<group>:queue` for backward compatibility; a named lane maps to
   * `redisjm:<group>:lane:<lane>:queue`. Locks and log stay group-wide (see `getLocksKey`/`getLogKey`).
   */
  private getQueueKey(lane?: string): string {
    if (lane === undefined || lane === 'default') {
      return `redisjm:${this.targetGroup}:queue`
    }
    return `redisjm:${this.targetGroup}:lane:${lane}:queue`
  }

  /**
   * Validates a lane name on the write paths. The default lane (`undefined`) and the internal
   * maintenance job are exempt (the latter owns the reserved `__maintenance` lane, assigned in a
   * later step); every other lane must match `^[A-Za-z0-9_-]+$` (no `#`, `:`, empty) and must not
   * start with `__` (reserved).
   */
  private validateLane(lane: string | undefined, jobName: string): void {
    if (lane === undefined) return
    if (jobName === MAINTENANCE_JOB_NAME) return
    if (!/^[A-Za-z0-9_-]+$/.test(lane)) {
      throw new Error(`Lane "${lane}" is invalid — lane names must match /^[A-Za-z0-9_-]+$/`)
    }
    if (lane.startsWith('__')) {
      throw new Error(`Lane "${lane}" is invalid — the "__" prefix is reserved`)
    }
  }

  private getLocksKey(): string {
    return `redisjm:${this.targetGroup}:locks`
  }

  /**
   * Hash mapping jobId → epoch-ms when maintenance first saw its lock held with no backing log
   * record. Backs the two-pass orphaned-lock reclaim in `performMaintenance` (stage 4), the same
   * way `suspectedAt` on a record backs the orphaned-queued reclaim.
   */
  private getSuspectsKey(): string {
    return `redisjm:${this.targetGroup}:suspects`
  }

  private getLogKey(): string {
    return `redisjm:${this.targetGroup}:log`
  }

  /**
   * Sorted-set key holding delayed/scheduled runs: member = jobId, score = epoch-ms when the run
   * becomes ready. Group-wide (not per-lane); the record's persisted `lane` routes it when promoted.
   */
  private getDelayedKey(): string {
    return `redisjm:${this.targetGroup}:delayed`
  }
}
