import type { Job } from './job'

/** Allowed value types for custom job attributes. */
export type JobAttrValue = string | number | boolean | null | undefined

/** Default attributes type for job log records. */
export type JobAttrs = Record<string, JobAttrValue>

/** Possible statuses of a job in the lifecycle. */
export type JobStatus = 'queued' | 'running' | 'finished' | 'error' | 'stale'

/** Metadata associated with a job definition. */
export interface JobMetadata {
  /** Unique job name used as the key prefix in job IDs (`"jobName#runId"`) */
  jobName: string
  /** Optional human-readable description */
  description?: string
}

/** Optional configuration for `RedisJM`. All fields have defaults. */
export interface RedisJMOptions {
  /** Milliseconds between heartbeat updates during job execution (default: `5000`) */
  heartbeatInterval?: number
  /** Number of missed heartbeat intervals before a job is considered stale (default: `2`) */
  roundsToStale?: number
  /** Milliseconds to keep finished/error/stale records in the log; `0` removes immediately (default: `0`) */
  keepFinishedInterval?: number
}

/** Resolved version of `RedisJMOptions` with all defaults applied. */
export interface ResolvedRedisJMOptions {
  heartbeatInterval: number
  roundsToStale: number
  keepFinishedInterval: number
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
}

/** Options for `Job.execute()`. */
export interface JobExecuteOptions {
  /** Target group identifier (falls back to the default manager's target group) */
  targetGroup?: string
  /** Enables periodic heartbeat events at this interval (ms) */
  heartbeatInterval?: number
  /** Explicit runId (otherwise derived from serialized inputs) */
  runId?: string
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
}

/** Payload for `error` events. Extends `JobEventPayload` with the caught error. */
export interface JobErrorEventPayload<TInputs = unknown> extends JobEventPayload<TInputs> {
  error: Error
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
  error: (payload: JobErrorEventPayload<TInputs>) => void | Promise<void>
  heartbeat: (payload: JobEventPayload<TInputs>) => void | Promise<void>
  update: (payload: JobUpdateEventPayload<TInputs, TAttrs>) => void | Promise<void>
}

/** Event hooks for `RedisJM` instances. Re-dispatched from registered jobs matching the target group. */
export interface RedisJMHooks {
  start: (payload: JobEventPayload) => void | Promise<void>
  finish: (payload: JobEventPayload) => void | Promise<void>
  error: (payload: JobErrorEventPayload) => void | Promise<void>
  heartbeat: (payload: JobEventPayload) => void | Promise<void>
  update: (payload: JobUpdateEventPayload) => void | Promise<void>
}

/** Result returned by `RedisJM.performMaintenance()`. */
export interface MaintenanceResult {
  /** Number of running jobs marked as stale */
  staleCount: number
  /** Number of expired log records removed */
  cleanedCount: number
}
