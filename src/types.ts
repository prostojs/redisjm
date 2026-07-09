import type { Job } from './job'
import type { RedisJM } from './redisjm'

/** Allowed value types for custom job attributes. */
export type JobAttrValue = string | number | boolean | null | undefined

/** Default attributes type for job log records. */
export type JobAttrs = Record<string, JobAttrValue>

/**
 * Sink for operational errors that would otherwise be silent: a job handler that
 * threw, a job popped with no registered handler, a job dropped because its log
 * record vanished, and poll-loop failures. Receives a message and, when available,
 * the underlying `Error` (so the stack can be logged).
 */
export type RedisJMLogger = (message: string, error?: Error) => void

/** Possible statuses of a job in the lifecycle. */
export type JobStatus = 'queued' | 'running' | 'finished' | 'error' | 'stale' | 'delayed'

/** Strategy for ordering subscribed lanes when polling with `LMPOP`. */
export type LaneStrategy = 'roundRobin' | 'priority'

/** Metadata associated with a job definition. */
export interface JobMetadata {
  /** Unique job name used as the key prefix in job IDs (`"jobName#runId"`) */
  jobName: string
  /** Optional human-readable description */
  description?: string
  /**
   * Optional lane (named sub-queue within the group). Omitted → default lane (legacy queue key);
   * a worker services only the lanes of its registered jobs.
   */
  lane?: string
  /**
   * Total number of attempts (including the first) before a failed run is considered finally failed.
   * Must be a positive integer; default `1` = no retries (a single failed attempt is terminal).
   * Resolved via `Job.getAttempts()` (floored, clamped to ≥ 1).
   */
  attempts?: number
  /**
   * Delay in ms before the next retry attempt, given the just-failed 1-based attempt number.
   * Either a fixed number of ms or a function of the failed attempt (e.g. exponential backoff).
   * Default `0` = re-queue immediately (still routed through the delayed set on the next promotion
   * pass). Negative / non-finite results are clamped to `0`. Resolved via `Job.getBackoffMs()`.
   */
  backoff?: number | ((attempt: number) => number)
}

/** Options for `queue` / `queueFirst` / `Job.queue`. */
export interface QueueOptions {
  /**
   * Milliseconds to stage the run on the delayed set before it becomes poppable. `> 0` stores the
   * record as `delayed` (holding the lock so dedupe still applies) and schedules it on the delayed
   * sorted set; `0`/omitted queues immediately. Must be a finite number ≥ 0. Incompatible with
   * `queueFirst` (a priority insert cannot be delayed).
   */
  delay?: number
}

/** Optional configuration for `RedisJM`. All fields have defaults. */
export interface RedisJMOptions {
  /** Milliseconds between heartbeat updates during job execution (default: `5000`) */
  heartbeatInterval?: number
  /** Number of missed heartbeat intervals before a job is considered stale (default: `2`) */
  roundsToStale?: number
  /** Milliseconds to keep finished/error/stale records in the log; `0` removes immediately (default: `0`) */
  keepFinishedInterval?: number
  /**
   * Milliseconds between automatic maintenance enqueues while `start()` is polling;
   * `0` disables auto-maintenance (default: `heartbeatInterval * roundsToStale`).
   * All instances enqueue concurrently — the lock ensures only one maintenance run executes.
   */
  maintenanceInterval?: number
  /**
   * Max number of times a job whose `jobName` is not registered on the popping
   * instance is re-queued (kept in the queue with its lock held) so another
   * instance — e.g. a freshly deployed pod that *does* register the handler — can
   * claim it. Once the budget is exhausted the job is marked
   * `error: 'Job name is unknown'` and dropped. `0` restores the legacy behavior
   * (drop immediately on the first pop). Default: `5`.
   */
  unknownJobRequeueLimit?: number
  /**
   * How `LMPOP` orders subscribed lanes: `roundRobin` (default) rotates the work-lane order each
   * poll to avoid starvation; `priority` uses the `lanePriority` order.
   */
  laneStrategy?: LaneStrategy
  /**
   * Explicit high→low lane order used when `laneStrategy: 'priority'`; lanes absent from the list
   * follow in registration order. The reserved `__maintenance` lane is always polled first
   * regardless.
   */
  lanePriority?: string[]
  /**
   * Sink for operational errors that are otherwise invisible — handler throws,
   * unknown/dropped jobs, and poll-loop failures. Defaults to a `console.error`
   * logger that includes the error stack. Pass `false` to silence default logging
   * (e.g. when you wire your own `manager.hook('error', …)` and don't want
   * duplicate console output).
   */
  logger?: RedisJMLogger | false
}

/** Resolved version of `RedisJMOptions` with all defaults applied. */
export interface ResolvedRedisJMOptions {
  heartbeatInterval: number
  roundsToStale: number
  keepFinishedInterval: number
  maintenanceInterval: number
  unknownJobRequeueLimit: number
  laneStrategy: LaneStrategy
  lanePriority: string[]
}

/** A full job state record stored in the Redis log hash. */
export interface JobLogRecord<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> {
  /** Composite ID: `"jobName#runId"` */
  jobId: string
  jobName: string
  runId: string
  /** The inputs that were passed when the job was queued */
  inputs: TInputs
  targetGroup: string
  /**
   * The lane the run was enqueued on; persisted so off-registry ops (maintenance, unqueue) resolve
   * the correct lane queue from the record.
   */
  lane?: string
  status: JobStatus
  /** Epoch ms when execution started */
  startedAt?: number
  /** Epoch ms when execution finished (success, error, or stale detection) */
  finishedAt?: number
  /** Epoch ms of the last heartbeat update */
  heartbeat?: number
  /** Progress value between 0 and 1 */
  progress: number
  /** Custom attributes set via `ctx.setAttrs()` */
  attrs?: TAttrs
  /** Error message if the job failed */
  error?: string
  /**
   * Epoch ms when maintenance first observed this `queued` record missing from the queue list
   * (orphan suspect — popped by an instance that died before the `start` event). Cleared when
   * the job starts normally; if still set and expired on a later scan, the record goes `stale`.
   */
  suspectedAt?: number
  /**
   * Number of times this run has been re-queued because the popping instance had no
   * registered handler for its `jobName` (see `RedisJMOptions.unknownJobRequeueLimit`).
   */
  requeueCount?: number
  /**
   * Fencing token stamped when an execution claims this record (the `start` hook flips it to
   * `running`). Lifecycle hooks refuse to mutate a record whose token doesn't match theirs, so a
   * zombie execution can't overwrite/clean a record now owned by a successor that reclaimed the runId.
   */
  executionId?: string
  /**
   * 1-based count of executions that have started (claimed) this queued run. Incremented on each
   * successful claim; groundwork for retries (a re-run of the same queued entry bumps it).
   */
  attempt?: number
  /**
   * Epoch ms when a `delayed` run becomes poppable (promotion flips it to `queued` once due).
   * Present only while `status === 'delayed'`; deleted when the run is promoted.
   */
  readyAt?: number
}

/** Options for `Job.execute()`. */
export interface JobExecuteOptions {
  /** Target group identifier (falls back to the default manager's target group) */
  targetGroup?: string
  /** Enables periodic heartbeat events at this interval (ms) */
  heartbeatInterval?: number
  /** Explicit runId (otherwise derived from serialized inputs) */
  runId?: string
  /**
   * The manager driving this execution. Stamped onto every event payload so that when two managers
   * in one process share a Job and a targetGroup, only the driving manager's hooks act on the events.
   */
  manager?: RedisJM
  /**
   * Sink for infrastructure errors that surface during execution (e.g. a failed heartbeat write or
   * a throwing `error` hook), which would otherwise be swallowed.
   */
  logger?: RedisJMLogger
}

/** Context object passed to the job function during execution. */
export interface JobContext<TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> {
  /** Updates the job's progress (0–1) in the log via an `update` event. */
  setProgress: (progress: number) => Promise<void>
  /** Updates the job's custom attributes in the log via an `update` event. */
  setAttrs: (attrs: TAttrs) => Promise<void>
}

/** The job function signature. Receives inputs and a context for progress/attrs updates. */
export type JobFunction<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> = (
  inputs: TInputs,
  ctx: JobContext<TAttrs>,
) => void | Promise<void>

/** Payload for `start`, `finish`, and `heartbeat` events. */
export interface JobEventPayload<TInputs = unknown> {
  job: Job<TInputs, any>
  targetGroup: string
  runId: string
  inputs: TInputs
  /** Fencing token unique to this execution; stamped onto the record when the run claims it. */
  executionId: string
  /** The manager driving this execution, when run through one (absent for direct `job.execute()`). */
  manager?: RedisJM
}

/** Payload for `error` events. Extends `JobEventPayload` with the caught error. */
export interface JobErrorEventPayload<TInputs = unknown> extends JobEventPayload<TInputs> {
  error: Error
}

/**
 * Payload for the manager-level `retry` event, fired for each scheduled retry (not final failure).
 * Extends `JobEventPayload` with the error that caused the retry, the 1-based attempt that failed,
 * and the epoch-ms time the next attempt becomes poppable.
 */
export interface JobRetryEventPayload<TInputs = unknown> extends JobEventPayload<TInputs> {
  error: Error
  /** The 1-based attempt number that just failed and triggered the retry. */
  attempt: number
  /** Epoch ms when the retried run becomes poppable (its delayed `readyAt`). */
  nextAttemptAt: number
}

/** Payload for `update` events. Extends `JobEventPayload` with optional progress and attrs. */
export interface JobUpdateEventPayload<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> extends JobEventPayload<TInputs> {
  progress?: number
  attrs?: TAttrs
}

/** Event hooks for `Job` instances. */
export interface JobHooks<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> {
  start: (payload: JobEventPayload<TInputs>) => void | Promise<void>
  finish: (payload: JobEventPayload<TInputs>) => void | Promise<void>
  /**
   * Fires on EVERY failed attempt (each throw of the job function), including attempts that will be
   * retried. Contrast the manager-level `error` event (`RedisJMHooks`), which fires only on FINAL
   * failure once the `attempts` budget is exhausted.
   */
  error: (payload: JobErrorEventPayload<TInputs>) => void | Promise<void>
  heartbeat: (payload: JobEventPayload<TInputs>) => void | Promise<void>
  update: (payload: JobUpdateEventPayload<TInputs, TAttrs>) => void | Promise<void>
}

/**
 * Event hooks for `RedisJM` instances. Re-dispatched from registered jobs matching the target group.
 *
 * Retry semantics: a job-level `error` hook (see `JobHooks`) fires on EVERY failed attempt, whereas
 * the manager-level `error` event here fires only on FINAL failure (the last attempt exhausted the
 * `attempts` budget). The manager-level `retry` event fires once for each scheduled retry in between.
 */
export interface RedisJMHooks {
  start: (payload: JobEventPayload) => void | Promise<void>
  finish: (payload: JobEventPayload) => void | Promise<void>
  error: (payload: JobErrorEventPayload) => void | Promise<void>
  /**
   * Fires when a failed attempt is scheduled for retry (attempt N failed, attempt N+1 pending on the
   * delayed set). Manager-level only — the retry decision lives in the manager, not the job. Does NOT
   * fire on final failure (the `error` event fires then instead).
   */
  retry: (payload: JobRetryEventPayload) => void | Promise<void>
  heartbeat: (payload: JobEventPayload) => void | Promise<void>
  update: (payload: JobUpdateEventPayload) => void | Promise<void>
}

/** Result returned by `RedisJM.performMaintenance()`. */
export interface MaintenanceResult {
  /**
   * Number of jobs reclaimed as stale: running jobs whose heartbeat lapsed, orphaned `queued`
   * records missing from the queue list, and orphaned locks with no backing log record (a lock
   * left behind by an `enqueue` that crashed between its SADD and HSET) — all counted here.
   */
  staleCount: number
  /** Number of log records removed: expired finished/error/stale records and unparseable garbage */
  cleanedCount: number
}
