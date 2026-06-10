import { Hookable } from 'hookable'
import type Redis from 'ioredis'
import { Job } from './job'
import { createMaintenanceJob, MAINTENANCE_JOB_NAME } from './maintenance'
import type {
  JobAttrs,
  JobAttrValue,
  JobErrorEventPayload,
  JobEventPayload,
  JobFunction,
  JobLogRecord,
  JobMetadata,
  JobUpdateEventPayload,
  MaintenanceResult,
  RedisJMHooks,
  RedisJMOptions,
  ResolvedRedisJMOptions,
} from './types'

const DEFAULT_OPTIONS: Omit<ResolvedRedisJMOptions, 'maintenanceInterval'> = {
  heartbeatInterval: 5000,
  roundsToStale: 2,
  keepFinishedInterval: 0,
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
  private readonly registeredJobs = new Set<Job<any, any>>()
  private readonly jobsByName = new Map<string, Job<any, any>>()
  private readonly jobHookCleanups = new Map<Job<any, any>, () => void>()

  private pollTimer: ReturnType<typeof setTimeout> | undefined
  private polling = false
  private maintenanceTimer: ReturnType<typeof setInterval> | undefined

  /**
   * @param redis - An ioredis client instance
   * @param targetGroup - String prefix for all Redis keys; only managers sharing the same target group share queues
   * @param options - Optional configuration for heartbeat, stale detection, log retention, and auto-maintenance
   */
  constructor(redis: Redis, targetGroup: string, options?: RedisJMOptions) {
    super()
    this.redis = redis
    this.targetGroup = targetGroup
    const heartbeatInterval = options?.heartbeatInterval ?? DEFAULT_OPTIONS.heartbeatInterval
    const roundsToStale = options?.roundsToStale ?? DEFAULT_OPTIONS.roundsToStale
    this.options = {
      heartbeatInterval,
      roundsToStale,
      keepFinishedInterval: options?.keepFinishedInterval ?? DEFAULT_OPTIONS.keepFinishedInterval,
      maintenanceInterval: options?.maintenanceInterval ?? heartbeatInterval * roundsToStale,
    }
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
   *
   * @example
   * ```ts
   * const success = await manager.queue(job, 'order-123', { orderId: '123' })
   * ```
   */
  async queue<TInputs>(job: Job<TInputs, any>, runId: string, inputs: TInputs): Promise<boolean> {
    return this.enqueue(job, runId, inputs, 'rpush')
  }

  /**
   * Adds a job run to the front of the queue (priority insert). Returns `true` if queued, `false` if already locked.
   *
   * @example
   * ```ts
   * await manager.queueFirst(job, 'urgent-order', { orderId: '456' })
   * ```
   */
  async queueFirst<TInputs>(job: Job<TInputs, any>, runId: string, inputs: TInputs): Promise<boolean> {
    return this.enqueue(job, runId, inputs, 'lpush')
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
    const entries = await this.redis.hgetall(this.getLogKey())
    return Object.values(entries).map((val) => JSON.parse(val) as JobLogRecord)
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
    await this.redis.lrem(this.getQueueKey(), 1, jobId)
    await this.redis.srem(this.getLocksKey(), jobId)
    await this.redis.hdel(this.getLogKey(), jobId)
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
    if (this.jobsByName.has(jobName)) {
      throw new Error(`Job with name "${jobName}" is already registered`)
    }

    this.registeredJobs.add(job)
    this.jobsByName.set(jobName, job)

    const getJobId = (runId: string) => job.getJobId(runId)

    const onStart = async (payload: JobEventPayload<TInputs>) => {
      if (payload.targetGroup !== this.targetGroup) return
      const jobId = getJobId(payload.runId)
      await this.updateLog(jobId, (record) => {
        record.status = 'running'
        record.startedAt = Date.now()
        record.heartbeat = Date.now()
        delete record.suspectedAt
      })
      await this.callHook('start', payload as unknown as JobEventPayload)
    }

    const onFinish = async (payload: JobEventPayload<TInputs>) => {
      if (payload.targetGroup !== this.targetGroup) return
      const jobId = getJobId(payload.runId)
      const now = Date.now()
      await this.updateLog(jobId, (record) => {
        record.status = 'finished'
        record.finishedAt = now
      })
      await this.redis.srem(this.getLocksKey(), jobId)
      if (this.options.keepFinishedInterval === 0) {
        await this.redis.hdel(this.getLogKey(), jobId)
      }
      await this.callHook('finish', payload as unknown as JobEventPayload)
    }

    const onError = async (payload: JobErrorEventPayload<TInputs>) => {
      if (payload.targetGroup !== this.targetGroup) return
      const jobId = getJobId(payload.runId)
      const now = Date.now()
      await this.updateLog(jobId, (record) => {
        record.status = 'error'
        record.error = payload.error.message
        record.finishedAt = now
      })
      await this.redis.srem(this.getLocksKey(), jobId)
      if (this.options.keepFinishedInterval === 0) {
        await this.redis.hdel(this.getLogKey(), jobId)
      }
      await this.callHook('error', payload as unknown as JobErrorEventPayload)
    }

    const onHeartbeat = async (payload: JobEventPayload<TInputs>) => {
      if (payload.targetGroup !== this.targetGroup) return
      const jobId = getJobId(payload.runId)
      await this.updateLog(jobId, (record) => {
        record.heartbeat = Date.now()
      })
      await this.callHook('heartbeat', payload as unknown as JobEventPayload)
    }

    const onUpdate = async (payload: JobUpdateEventPayload<TInputs, TAttrs>) => {
      if (payload.targetGroup !== this.targetGroup) return
      const jobId = getJobId(payload.runId)
      await this.updateLog(jobId, (record) => {
        if (payload.progress !== undefined) record.progress = payload.progress
        if (payload.attrs !== undefined) record.attrs = payload.attrs
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
    const jobId = await this.redis.lpop(this.getQueueKey())
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
      const now = Date.now()
      await this.updateLog(jobId, (record) => {
        record.status = 'error'
        record.error = 'Job name is unknown'
        record.finishedAt = now
      })
      await this.redis.srem(this.getLocksKey(), jobId)
      if (this.options.keepFinishedInterval === 0) {
        await this.redis.hdel(this.getLogKey(), jobId)
      }
      return true
    }

    const logJson = await this.redis.hget(this.getLogKey(), jobId)
    if (!logJson) {
      await this.redis.srem(this.getLocksKey(), jobId)
      return true
    }

    const logRecord = JSON.parse(logJson) as JobLogRecord
    try {
      await job.execute(logRecord.inputs, {
        targetGroup: this.targetGroup,
        heartbeatInterval: this.options.heartbeatInterval,
        runId,
      })
    } catch {
      // Error already handled by the error event handler
    }

    return true
  }

  /**
   * Starts a polling loop that calls `popAndExecute()`. Polls immediately after a job executes;
   * waits `interval` ms when the queue is empty.
   *
   * Unless `maintenanceInterval` is `0`, also enqueues the built-in maintenance job — once
   * immediately (so locks orphaned by a crash are reclaimed soon after restart) and then every
   * `maintenanceInterval` ms. All instances enqueue concurrently; the lock dedupes the runs.
   *
   * @param interval - Milliseconds to wait between polls when idle
   *
   * @example
   * ```ts
   * manager.start(1000)
   * ```
   */
  start(interval: number): void {
    if (this.polling) return
    this.polling = true

    if (this.options.maintenanceInterval > 0) {
      const job = this.jobsByName.get(MAINTENANCE_JOB_NAME) ?? createMaintenanceJob(this)
      const tryQueue = () => {
        this.queue(job, '', null).catch(() => {})
      }
      tryQueue()
      this.maintenanceTimer = setInterval(tryQueue, this.options.maintenanceInterval)
    }

    const poll = async () => {
      if (!this.polling) return
      let executed = false
      try {
        executed = await this.popAndExecute()
      } catch {
        // Swallow poll errors to keep the loop alive
      }
      if (!this.polling) return
      const delay = executed ? 0 : interval
      this.pollTimer = setTimeout(poll, delay)
    }
    poll()
  }

  /**
   * Stops the polling loop. The currently executing job (if any) will finish.
   *
   * @example
   * ```ts
   * manager.stop()
   * ```
   */
  stop(): void {
    this.polling = false
    if (this.pollTimer) {
      clearTimeout(this.pollTimer)
      this.pollTimer = undefined
    }
    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer)
      this.maintenanceTimer = undefined
    }
  }

  /**
   * Scans the job log for stale and expired records:
   * 1. Marks running jobs as `"stale"` if heartbeat expired (`now - lastHeartbeat > heartbeatInterval * roundsToStale`)
   * 2. Marks orphaned queued jobs as `"stale"` — a `queued` record that is no longer in the queue
   *    list was popped by an instance that died before the `start` event fired. Detection is
   *    two-pass to avoid racing the normal pop→start window: the first scan stamps `suspectedAt`,
   *    a later scan reclaims the lock if the record is still orphaned past the stale threshold.
   * 3. Removes finished/error/stale records older than `keepFinishedInterval`
   *
   * @returns Counts of stale and cleaned records
   *
   * @example
   * ```ts
   * const { staleCount, cleanedCount } = await manager.performMaintenance()
   * ```
   */
  async performMaintenance(): Promise<MaintenanceResult> {
    const logKey = this.getLogKey()
    const locksKey = this.getLocksKey()
    const entries = await this.redis.hgetall(logKey)
    const now = Date.now()
    let staleCount = 0
    let cleanedCount = 0

    const staleThreshold = this.options.heartbeatInterval * this.options.roundsToStale

    for (const [jobId, json] of Object.entries(entries)) {
      const record = JSON.parse(json) as JobLogRecord

      if (record.status === 'running') {
        const lastHeartbeat = record.heartbeat ?? record.startedAt ?? 0
        if (now - lastHeartbeat > staleThreshold) {
          record.status = 'stale'
          record.finishedAt = now
          await this.redis.hset(logKey, jobId, JSON.stringify(record))
          await this.redis.srem(locksKey, jobId)
          staleCount++
        }
      } else if (record.status === 'queued') {
        const queuePosition = await this.redis.lpos(this.getQueueKey(), jobId)
        if (queuePosition === null) {
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

    return { staleCount, cleanedCount }
  }

  // -- private helpers --

  private async enqueue<TInputs>(
    job: Job<TInputs, any>,
    runId: string,
    inputs: TInputs,
    pushCmd: 'rpush' | 'lpush',
  ): Promise<boolean> {
    const jobId = job.getJobId(runId)
    const locksKey = this.getLocksKey()

    const added = await this.redis.sadd(locksKey, jobId)
    if (added === 0) return false

    try {
      await this.redis[pushCmd](this.getQueueKey(), jobId)

      const record: JobLogRecord<TInputs> = {
        jobId,
        jobName: job.getName(),
        runId,
        inputs,
        targetGroup: this.targetGroup,
        status: 'queued',
        progress: 0,
      }
      await this.redis.hset(this.getLogKey(), jobId, JSON.stringify(record))
      return true
    } catch (err) {
      await this.redis.srem(locksKey, jobId).catch(() => {})
      throw err
    }
  }

  private async updateLog(jobId: string, mutate: (record: JobLogRecord) => void): Promise<void> {
    const logKey = this.getLogKey()
    const json = await this.redis.hget(logKey, jobId)
    if (!json) return
    const record = JSON.parse(json) as JobLogRecord
    mutate(record)
    await this.redis.hset(logKey, jobId, JSON.stringify(record))
  }

  private getQueueKey(): string {
    return `redisjm:${this.targetGroup}:queue`
  }

  private getLocksKey(): string {
    return `redisjm:${this.targetGroup}:locks`
  }

  private getLogKey(): string {
    return `redisjm:${this.targetGroup}:log`
  }
}
