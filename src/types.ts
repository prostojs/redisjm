import type { EnqueueErrorReason, RedisErrorReason, RedisJMEnqueueError } from './errors'
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

/** Strategy for ordering this instance's subscribed lanes when popping. */
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
  /**
   * Execution timeout in ms for each attempt of this job. When it elapses, the run's `ctx.signal` is
   * aborted (reason `'timeout'`) and the attempt fails with a `JobTimeoutError` — an ordinary failure,
   * so `attempts`/`backoff` apply. The concurrency slot is freed immediately even if the handler never
   * settles (a handler that ignores the signal keeps running detached; its late writes are fenced).
   * Overrides `RedisJMOptions.jobTimeout`; `0` disables the manager default for this job. Unset →
   * the manager default applies.
   */
  timeoutMs?: number
  /**
   * When the run's `ctx.signal` aborts for any reason other than its own execution timeout (ownership
   * loss, `stop({ abort: true })`, `payload.abort()`, an external `signal`), wait at most this many ms for
   * the handler to settle on its own, then settle the attempt with a `JobAbortedError` — freeing the slot
   * without waiting for the handler (it keeps running detached; its writes are fenced). `false` (default)
   * = wait for the handler (or its timeout) as before. `0` = settle on the next timer tick. Overrides `RedisJMOptions.abortGraceMs`; `false` opts this job out of the manager default.
   * The abandoned attempt is an ordinary failure and CONSUMES an attempt: set `attempts > 1` if an aborted
   * run must be re-run elsewhere.
   */
  abortGraceMs?: number | false
  /**
   * Backpressure cap: an enqueue of this job is refused with `{ status: 'full' }` (nothing written, no
   * lock taken) while its LANE's queue list already holds `>= maxQueued` entries. Combined with
   * `RedisJMOptions.laneCaps` — the smaller defined cap wins. Counts the lane list only (O(1) `LLEN`):
   * delayed runs and running runs are not counted. The check and the push happen in one atomic script,
   * so concurrent producers cannot overshoot it; entries pushed back by internal paths (an unknown-job
   * requeue, a retry promotion, maintenance re-queueing an abandoned pop) bypass the cap, so the list
   * can briefly exceed it. A full lane only ever
   * REJECTS new work — nothing already accepted is dropped to make room (silently discarding accepted
   * work is exactly the failure mode the cap exists to prevent).
   */
  maxQueued?: number
  /**
   * Max size in bytes of the JSON-serialized `inputs` of one run; a larger enqueue throws
   * `RedisJMEnqueueError` with reason `'inputs-too-large'` before anything is written. Overrides
   * `RedisJMOptions.maxInputsBytes`; `0` disables the manager default for this job.
   */
  maxInputsBytes?: number
  /**
   * Max number of this job's runs that may hold a lock at once (queued + delayed + running), across
   * all run ids and instances — `1` makes the job single-flight. Enforced atomically INSIDE the enqueue
   * script: an enqueue while the job is at its limit returns `{ status: 'busy' }` and writes nothing.
   * Applies to every producer (timers, `every()`, manual triggers). Unset = unlimited.
   */
  maxInFlight?: number
}

/** Options for `queue` / `queueFirst` / `Job.queue` (and the base of `EnqueueOptions`). */
export interface QueueOptions {
  /**
   * Milliseconds to stage the run on the delayed set before it becomes poppable. `> 0` stores the
   * record as `delayed` (holding the lock so dedupe still applies) and schedules it on the delayed
   * sorted set; `0`/omitted queues immediately. Must be a finite number ≥ 0. Incompatible with
   * `queueFirst` (a priority insert cannot be delayed).
   */
  delay?: number
}

/** Options for `RedisJM.enqueue` / `Job.enqueue`. */
export interface EnqueueOptions extends QueueOptions {
  /**
   * Priority insert at the FRONT of the lane queue (what `queueFirst` does). Same restriction as
   * `queueFirst`: cannot be combined with `delay > 0` (throws a `TypeError`).
   */
  first?: boolean
}

/**
 * Outcome of `RedisJM.enqueue` / `Job.enqueue`:
 * - `'queued'`  — the run was written (on its lane queue, or on the delayed set when `delay > 0`).
 * - `'deduped'` — the runId already holds a lock (queued/delayed/running); nothing was written.
 * - `'busy'`    — the job already has `>= maxInFlight` runs holding a lock; nothing was written.
 * - `'full'`    — the lane was at its cap (`maxQueued` / `laneCaps`); nothing was written.
 *
 * Redis failures are NOT a status: they throw `RedisJMEnqueueError`.
 */
export interface EnqueueResult {
  status: 'queued' | 'deduped' | 'busy' | 'full'
  /** The `"jobName#runId"` the enqueue addressed. */
  jobId: string
}

/** Optional configuration for `RedisJM`. All fields have defaults. */
export interface RedisJMOptions {
  /** Milliseconds between heartbeat updates during job execution (default: `5000`) */
  heartbeatInterval?: number
  /** Number of missed heartbeat intervals before a job is considered stale (default: `2`) */
  roundsToStale?: number
  /**
   * Milliseconds to keep finished/error/stale records in the log after they reach a terminal state,
   * so `get()`/`list()` can observe the outcome. Default `60000` (60s) — kept as the default because
   * callers commonly poll `get()` for a run's result; terminal records linger a minute. MEMORY COST:
   * every terminal record (inputs included) stays in Redis that long, so a high-throughput group holds
   * roughly `throughput × keepFinishedInterval` records. They are reclaimed twice over: each terminal
   * record gets a hash-field TTL (`HPEXPIRE`, Redis >= 7.4) so it expires even when maintenance can't
   * run, and maintenance sweeps expired ones too. Set `0` to opt into the legacy write-only behavior,
   * where a record is deleted the instant the job leaves `running` (`get()` then returns `undefined`
   * for a finished run).
   */
  keepFinishedInterval?: number
  /**
   * Milliseconds between automatic maintenance passes while `start()` is running; `0` disables
   * auto-maintenance (default: `heartbeatInterval * roundsToStale`). Maintenance runs on the manager's
   * own timer — NOT through the job queue — so it keeps running when every concurrency slot is busy
   * or the queue is backed up. Every instance ticks; a short-lived Redis lock (see
   * `RedisJM.runMaintenance`) lets roughly one pass per interval run across the whole group.
   */
  maintenanceInterval?: number
  /**
   * Default execution timeout (ms) for every job run by this manager; `0`/unset = no timeout.
   * A job's own `JobMetadata.timeoutMs` wins (including `0`, which opts that job out). See
   * `JobMetadata.timeoutMs` for the semantics.
   */
  jobTimeout?: number
  /**
   * Default abort grace for every job run by this manager: when a run's `ctx.signal` aborts for a reason
   * other than its execution timeout (ownership loss, `stop({ abort: true })`, `payload.abort()`), wait at
   * most this many ms for the handler to settle, then abandon it and fail the attempt with a
   * `JobAbortedError` — so a handler that ignores its signal can't hold its concurrency slot, or
   * `stop()`, hostage. `false`/unset (default) = wait for the handler. A job's own
   * `JobMetadata.abortGraceMs` wins. Must be `false` or a finite number `>= 0` (else `TypeError`). An
   * abandoned attempt goes through the normal failure path, so with the default `attempts: 1` it ends
   * `error`; set `attempts > 1` if an aborted run must be re-run elsewhere.
   */
  abortGraceMs?: number | false
  /**
   * Register this instance in the group's fleet registry while `start()` runs, so `fleet()` can report
   * the live consumers and their capacity (default `true`). Costs one small Redis write (a lease `ZADD`) per
   * instance per `heartbeatInterval` — the info is rewritten only when it changes — and two small keys per
   * group; set `false` to opt out.
   */
  presence?: boolean
  /** Free-text label stored with this instance's fleet entry (e.g. a pod name; max 200 chars). */
  instanceLabel?: string
  /**
   * Backstop for hung handlers that have no timeout: maintenance marks a `running` record `stale`
   * (and releases its lock) once `now - startedAt > maxRunMs`, REGARDLESS of its heartbeat — a hung
   * handler's heartbeat timer keeps the record looking alive forever otherwise. The staled run can no
   * longer resurrect itself through heartbeats/updates (its `ctx.signal` is aborted on the next
   * heartbeat). Off by default (`0`/unset). Pick a value comfortably above your slowest healthy run.
   */
  maxRunMs?: number
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
   * How a pop orders the subscribed lanes: `roundRobin` (default) rotates the work-lane order each
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
   * Per-lane concurrency caps for THIS instance: max simultaneous poll-loop runs of a lane (`'default'`
   * key = the default lane), within the global `concurrency`. The poll loop does not pop from a lane at
   * its cap; the lane's slot frees when one of its runs settles. Use it to keep capacity for a
   * latency-sensitive lane while a heavy lane is busy. Lanes without an entry are limited only by
   * `concurrency`. Runs started via `popAndExecute()` are not counted.
   */
  laneConcurrency?: Record<string, number>
  /**
   * Backpressure caps per lane (`'default'` key = the default lane): an enqueue onto a lane whose
   * queue list already holds `>= cap` entries returns `{ status: 'full' }`. See `JobMetadata.maxQueued`
   * (the smaller defined cap wins) for the exact semantics — list length only, reject-only.
   */
  laneCaps?: Record<string, number>
  /**
   * Default max size in bytes of a run's JSON-serialized inputs (see `JobMetadata.maxInputsBytes`,
   * which wins). `0`/unset = unlimited.
   */
  maxInputsBytes?: number
  /**
   * Upper bound on the work of one maintenance pass, per stage: at most about this many log records,
   * claiming entries and locks are examined per pass (default `1000`). The log and lock scans resume
   * where the previous pass — on any instance — stopped (cursors persisted in Redis), so a large log is
   * covered over several passes instead of one O(n) sweep. Deletions and checks are batched/pipelined.
   */
  maxRecordsPerPass?: number
  /**
   * `used_memory / maxmemory` ratio (from `INFO memory`) at which the maintenance timer fires the
   * `memoryPressure` hook and logs a warning — once per crossing, re-armed when the ratio drops back
   * below. Default `0.8`; `0` disables. Ignored when Redis has no `maxmemory` limit. Evaluated only after
   * each pass of the maintenance TIMER, so `memoryPressure` never fires without `start()` or with
   * `maintenanceInterval: 0` (poll `health()` yourself then).
   */
  memoryWarnRatio?: number
  /**
   * Max number of jobs a single instance executes simultaneously. Default `1` — today's serial
   * behavior, where one long-running job blocks the instance from popping anything else (maintenance is
   * not affected: it runs on its own timer). Set `> 1` for I/O-bound workloads that need N runs in flight. Must
   * be a positive integer; the constructor floors it and throws a `TypeError` if it is `< 1` or not
   * finite.
   */
  concurrency?: number
  /**
   * Sink for operational errors that are otherwise invisible — handler throws and timeouts,
   * unknown/dropped jobs, start failures (requeued/dropped runs), maintenance write failures,
   * throwing manager-level hooks, and poll-loop failures. Defaults to a `console.error`
   * logger that includes the error stack. Pass `false` to silence default logging
   * (e.g. when you wire your own `manager.hook('error', …)` and don't want
   * duplicate console output).
   */
  logger?: RedisJMLogger | false
}

/** Options for `RedisJM.stop()`. */
export interface StopOptions {
  /**
   * When `true`, abort every in-flight run's `ctx.signal` (reason `'manager stopped'`) before draining
   * — a fast shutdown that lets cooperative handlers bail out of wasted work early. Abort is
   * cooperative: nothing forcibly kills a handler; `stop()` still awaits all in-flight runs to settle —
   * unless `abortGraceMs` is set, in which case a handler still pending that many ms after the abort is
   * abandoned (its attempt fails with a `JobAbortedError`, consuming an attempt) and `stop()` resolves.
   * Default `false` — a graceful drain that lets in-flight runs finish on their own.
   */
  abort?: boolean
}

/**
 * Resolved version of `RedisJMOptions` with all defaults applied (every option but `logger`; limits that
 * are off resolve to `0`, lane limits to normalized records). See `RedisJMOptions` for each field.
 */
export type ResolvedRedisJMOptions = Required<Omit<RedisJMOptions, 'logger'>>

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
   * Epoch ms when maintenance first observed this record missing from its structure — a `delayed`
   * record off the delayed set, or a LEGACY (no `enqueuedAt`) `queued` record off its lane list (orphan
   * suspect). Cleared when it shows up again or starts; if still set past the stale threshold on a
   * later pass, the record goes `stale`.
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
  /**
   * Epoch ms when the run was (last) put on its lane list — stamped by every 0.2+ write that queues a
   * run. Its presence also marks a record as written by a version that keeps the "a `queued` record is
   * on its lane list or in the `claiming` set" invariant, so maintenance can find orphans from the
   * `claiming` set alone; `queued` records WITHOUT it (written by 0.1.x) get the older per-record list
   * check until they drain.
   */
  enqueuedAt?: number
  /**
   * Why maintenance marked this record `stale`: `'heartbeat'` (running heartbeat lapsed),
   * `'orphaned'` (a queued/delayed record missing from its queue/delayed set), or `'maxRunMs'`
   * (running longer than `RedisJMOptions.maxRunMs`). Unlike a heartbeat/orphan stale, a `'maxRunMs'`
   * stale is never resurrected by a later heartbeat/update of the same execution (they are rejected and
   * its `ctx.signal` aborts). The execution's own outcome still lands, as for any stale-but-owned run: a
   * later finish records `finished`, and a throw schedules a retry when attempts remain (else `error`).
   */
  staleReason?: 'heartbeat' | 'orphaned' | 'maxRunMs'
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
  /**
   * External abort signal plumbed into `ctx.signal`. When it aborts, this execution's context signal
   * aborts with the same reason. The manager passes one per execution to wire ownership-loss and
   * shutdown aborts. The execution follows the signal for its lifetime and detaches its listener when
   * it settles, so a long-lived external signal never accumulates a listener per execution.
   */
  signal?: AbortSignal
  /**
   * Execution timeout in ms (`0`/unset = none). On expiry `ctx.signal` aborts with reason
   * `'timeout'` and `execute()` rejects with a `JobTimeoutError` (after dispatching the `error` hook),
   * without waiting for the handler to settle. The manager passes the resolved value
   * (`JobMetadata.timeoutMs` ?? `RedisJMOptions.jobTimeout`); a direct `job.execute()` honors it as given.
   */
  timeoutMs?: number
  /**
   * Abort grace in ms (`false`/unset = none): once `ctx.signal` aborts for a reason other than the
   * timeout, a handler still pending after this many ms is abandoned and `execute()` rejects with a
   * `JobAbortedError` (after dispatching the `error` hook). A handler that settles within the grace
   * decides the outcome itself. Must be `false` or a finite number `>= 0` (else `TypeError`). The manager
   * passes the resolved value (`JobMetadata.abortGraceMs` ?? `RedisJMOptions.abortGraceMs`).
   */
  abortGraceMs?: number | false
  /**
   * 1-based attempt number (an integer `>= 1`, default `1`) stamped on this execution's event payloads
   * (`JobEventPayload.attempt`); a manager's claim replaces it with the attempt it wrote.
   */
  attempt?: number
}

/** Context object passed to the job function during execution. */
export interface JobContext<TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> {
  /** Updates the job's progress (0–1) in the log via an `update` event. */
  setProgress: (progress: number) => Promise<void>
  /** Updates the job's custom attributes in the log via an `update` event. */
  setAttrs: (attrs: TAttrs) => Promise<void>
  /**
   * Aborted when this run loses ownership of its record — staled by maintenance, superseded by a
   * re-enqueue of the same runId, or unqueued (detected by the heartbeat hook's guarded write being
   * rejected) — or when the manager shuts down with `stop({ abort: true })`. Abort is COOPERATIVE:
   * nothing forcibly stops the handler, so check `signal.aborted` (or listen for `'abort'`) at natural
   * checkpoints to stop wasted work whose writes would only be fenced out. `signal.reason` carries a
   * short string cause. With `abortGraceMs` (or `timeoutMs`) the attempt is settled without the handler
   * once the grace (timeout) elapses; from then on this context is INERT: `setProgress` / `setAttrs`
   * resolve without writing anything.
   */
  signal: AbortSignal
}

/** The job function signature. Receives inputs and a context for progress/attrs updates. */
export type JobFunction<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> = (
  inputs: TInputs,
  ctx: JobContext<TAttrs>,
) => void | Promise<void>

/** Payload for `start`, `finish`, and `heartbeat` events (and the base of every other run-event payload). */
export interface JobEventPayload<TInputs = unknown> {
  job: Job<TInputs, any>
  targetGroup: string
  runId: string
  inputs: TInputs
  /** Fencing token unique to this execution; stamped onto the record when the run claims it. */
  executionId: string
  /** The manager driving this execution, when run through one (absent for direct `job.execute()`). */
  manager?: RedisJM
  /**
   * 1-based attempt of THIS execution (`1` = first run, `N` = (N-1)-th retry) as its claim wrote it to
   * the run record — right even when another instance ran the earlier attempts. Job-level `start` hooks
   * registered before `registerJob` run pre-claim and see the predicted value; a direct `job.execute()`
   * uses `JobExecuteOptions.attempt`.
   */
  attempt: number
  /**
   * Aborts THIS execution's context signal (`ctx.signal`) with the given reason (default `'aborted'`).
   * The manager's heartbeat hook calls it on ownership loss (stale/superseded/unqueued); it is also
   * exposed to user hooks as a custom kill-switch. Cooperative — the handler must observe the signal
   * to actually stop.
   */
  abort: (reason?: string) => void
}

/** Payload for `error` events. Extends `JobEventPayload` with the caught error. */
export interface JobErrorEventPayload<TInputs = unknown> extends JobEventPayload<TInputs> {
  error: Error
}

/**
 * Payload for the manager-level `retry` event, fired for each scheduled retry (not final failure).
 * Extends `JobEventPayload` (whose `attempt` is the 1-based attempt that just failed) with the error
 * that caused the retry and the epoch-ms time the next attempt becomes poppable.
 */
export interface JobRetryEventPayload<TInputs = unknown> extends JobEventPayload<TInputs> {
  error: Error
  /** Epoch ms when the retried run becomes poppable (its delayed `readyAt`). */
  nextAttemptAt: number
}

/** Payload for the manager-level `timeout` event. */
export interface JobTimeoutEventPayload<TInputs = unknown> extends JobErrorEventPayload<TInputs> {
  /** The timeout that elapsed, in ms (`error` is the `JobTimeoutError` the attempt failed with). */
  timeoutMs: number
}

/** Payload for the manager-level `enqueueFailed` event (an enqueue that threw `RedisJMEnqueueError`). */
export interface EnqueueFailedEventPayload {
  jobId: string
  jobName: string
  runId: string
  reason: EnqueueErrorReason
  error: RedisJMEnqueueError
}

/**
 * Payload for the manager-level `startFailed` event: a popped run failed between the pop and a fully
 * established execution. `action` says how it was recovered:
 * - `'requeued'` — the claim never landed; the jobId was pushed back to the head of its lane.
 *                  (When the record is no longer `queued` — the claim did land despite the error, or the
 *                  run moved on — there is nothing of this pop's to recover: no event fires.)
 * - `'deferred'` — the claim never landed and the push-back failed too (e.g. Redis out of memory). The
 *                  run is NOT lost: it stays in the `claiming` set (record `queued`, lock held) and
 *                  maintenance puts it back on its lane once it has sat there past the stale threshold.
 * - `'failed'`   — the claim landed (or a job-level `start` hook threw); the failure was routed through
 *                  the run's normal failure path (retry when attempts remain, else terminal `error`).
 */
export interface StartFailedEventPayload {
  jobId: string
  jobName: string
  runId: string
  reason: RedisErrorReason
  action: 'requeued' | 'deferred' | 'failed'
  error: Error
  /**
   * The 1-based attempt the claim wrote — set only for `'failed'` when the claim had landed (a
   * job-level `start` hook threw after it); `undefined` when the run was never claimed.
   */
  attempt?: number
}

/** Payload for `update` events. Extends `JobEventPayload` with optional progress and attrs. */
export interface JobUpdateEventPayload<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> extends JobEventPayload<TInputs> {
  progress?: number
  attrs?: TAttrs
}

/**
 * Event hooks for `Job` instances (`job.hook(...)`). These are the run's LIFECYCLE, not observers:
 * they are awaited in registration order, and a registered manager drives Redis state through its own
 * job-level hooks. A throwing job-level `start` hook therefore FAILS the run (when it throws after the
 * manager's claim, the failure goes through the normal retry-or-final path and the `error` hooks fire;
 * when it was registered before the job was registered with a manager and throws before the claim,
 * the queued record is failed terminally). Contrast manager-level hooks (`RedisJMHooks`), which are
 * isolated observers.
 */
export interface JobHooks<TInputs = unknown, TAttrs extends { [K in keyof TAttrs]: JobAttrValue } = JobAttrs> {
  start: (payload: JobEventPayload<TInputs>) => void | Promise<void>
  finish: (payload: JobEventPayload<TInputs>) => void | Promise<void>
  /**
   * Fires on EVERY failed attempt (each throw of the job function, an execution timeout, or a
   * job-level `start` hook that threw after the manager claimed the run), including attempts that
   * will be retried. Contrast the manager-level `error` event (`RedisJMHooks`), which fires only on FINAL
   * failure once the `attempts` budget is exhausted.
   */
  error: (payload: JobErrorEventPayload<TInputs>) => void | Promise<void>
  heartbeat: (payload: JobEventPayload<TInputs>) => void | Promise<void>
  update: (payload: JobUpdateEventPayload<TInputs, TAttrs>) => void | Promise<void>
}

/**
 * Event hooks for `RedisJM` instances. Re-dispatched from registered jobs matching the target group.
 *
 * OBSERVERS, not lifecycle: manager-level hooks (`manager.hook(...)`) are notified AFTER the
 * manager has made its Redis write for the transition, and are isolated — a throwing or rejecting
 * manager hook is reported to the logger and can never change the run's outcome or Redis state (nor
 * stop the other manager hooks for the same event). Contrast job-level hooks (`job.hook(...)`, see
 * `JobHooks`), which ARE the lifecycle: they are awaited in order and a throwing job-level `start`
 * hook fails the run.
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
  /**
   * Fires when an attempt exceeded its execution timeout, BEFORE the `retry` / `error` event for the
   * same attempt (a timeout is an ordinary failure, so one of those follows).
   */
  timeout: (payload: JobTimeoutEventPayload) => void | Promise<void>
  /** Fires when an enqueue failed with `RedisJMEnqueueError` (right before it is thrown to the caller). */
  enqueueFailed: (payload: EnqueueFailedEventPayload) => void | Promise<void>
  /** Fires when a popped run failed to start and was requeued, deferred, or failed (see the payload). */
  startFailed: (payload: StartFailedEventPayload) => void | Promise<void>
  /**
   * Fires (from the maintenance timer) when Redis memory use crosses `memoryWarnRatio` — once per
   * crossing; re-armed when it drops back below. Payload: a `health()` snapshot.
   */
  memoryPressure: (payload: RedisJMHealth) => void | Promise<void>
  /**
   * Fires after each maintenance pass THIS instance ran (the timer, `runMaintenance()`,
   * `performMaintenance()`, the legacy maintenance job), or when a pass could not run because of an error.
   * Not fired for a pass skipped because another instance holds the maintenance lock. On the timer it
   * fires before `memoryPressure`.
   */
  maintenance: (payload: MaintenanceEventPayload) => void | Promise<void>
}

/**
 * Snapshot returned by `RedisJM.health()`. Memory figures come from `INFO memory` (works on managed
 * Redis services that block `CONFIG`, and under OOM). All counts are cheap O(1)/O(lanes) cardinality
 * reads unless `{ scan: true }` is passed (see `running` / `stale`).
 */
export interface RedisJMHealth {
  /** `used_memory` in bytes. */
  usedMemory: number
  /** `maxmemory` in bytes; `0` = no limit configured. */
  maxMemory: number
  /** `usedMemory / maxMemory`, or `null` when there is no limit. */
  usedRatio: number | null
  /** `maxmemory_policy` (`noeviction` recommended), or `''` if INFO did not report it. */
  maxmemoryPolicy: string
  /** Enqueues refused with an OOM error by THIS manager instance since it was created. */
  oomRefusals: number
  /** Queue length per lane this instance knows (default, `__maintenance`, registered, capped lanes). */
  queues: Record<string, number>
  /** Runs on the delayed set. */
  delayed: number
  /** Runs popped but not yet claimed (the `claiming` set) — normally ~0; a growing value means stuck pops. */
  claiming: number
  /**
   * Running runs. Without `{ scan: true }` this is an ESTIMATE from cardinalities —
   * `locks − Σqueues − delayed − claiming`, floored at 0 — which also counts orphaned locks and runs
   * queued on lanes this instance doesn't know. With `{ scan: true }` it is an exact log-scan count (O(n)).
   */
  running: number
  /** Stale records still in the log. Needs a log scan: `null` unless `{ scan: true }`. */
  stale: number | null
  /** Held locks (queued + delayed + running + claiming). */
  locks: number
}

/**
 * Per-job in-flight counts returned by `RedisJM.inFlight()`: one `SMEMBERS` of the job's lock set plus
 * one `HMGET` of those records — O(runs in flight of this job), independent of the log size and of
 * other jobs. `total = queued + delayed + running`. It can be LOWER than the lock-set size `maxInFlight`
 * compares against while that set holds drift (members whose run already ended — e.g. a lock released
 * by a pre-0.2 instance — until maintenance prunes them), so an enqueue can briefly report `'busy'`
 * with `total < maxInFlight`. For hot paths that only need the number, `RedisJM.inFlightCount()` is one
 * O(1) `SCARD` and reads no records.
 */
export interface InFlightCounts {
  /** Runs of this job in flight: `queued + delayed + running` (queued includes popped-not-yet-claimed). */
  total: number
  queued: number
  delayed: number
  running: number
}

/** Options for `RedisJM.listPage()`. Filters are applied to each scanned record. */
export interface ListPageOptions {
  status?: JobStatus
  /** Lane name; `'default'` matches the default lane. */
  lane?: string
  jobName?: string
  /** Target number of records per page (default `100`); a page may hold slightly more or fewer. */
  limit?: number
  /** Cursor from the previous page; omit (or `'0'`) to start. */
  cursor?: string
}

/** One page from `RedisJM.listPage()`. `cursor === '0'` means the scan is complete. */
export interface ListPage {
  records: JobLogRecord[]
  cursor: string
}

/** Options for `RedisJM.every()`. */
export interface EveryOptions<TInputs = unknown> {
  /** Inputs passed to every run. */
  inputs: TInputs
  /**
   * Base runId (default `'every'`). With `skipIfInFlight` (default) it is used AS IS for every tick, so
   * the run lock dedupes a tick while the previous run is still queued/delayed/running — on every
   * instance. With `skipIfInFlight: false` each tick enqueues `<runId>-<tickEpochMs>`.
   */
  runId?: string
  /** Enqueue once right away instead of waiting a full interval (default `false`). */
  immediate?: boolean
  /** See `runId`. Default `true`. */
  skipIfInFlight?: boolean
}

/**
 * Snapshot returned by `RedisJM.stats()` for dashboards/introspection. All counts are best-effort
 * point-in-time reads (no cross-structure transaction), so a run mid-transition may be double- or
 * un-counted for one poll.
 */
export interface RedisJMStats {
  /** Queue depth per lane, keyed by lane name ('default' for the legacy/no-lane queue). */
  queues: Record<string, number>
  /** Number of runs staged on the delayed set (backoff retries + delay-queued runs). */
  delayed: number
  /** Total held locks (queued + delayed + running — a terminal run, `stale` included, holds none). */
  locks: number
  /** Log record counts by status. */
  statuses: Record<JobStatus, number>
}

/** Result returned by `RedisJM.performMaintenance()` / `RedisJM.runMaintenance()`. */
export interface MaintenanceResult {
  /**
   * Number of jobs reclaimed as stale: running jobs whose heartbeat lapsed (or that exceeded
   * `maxRunMs`), orphaned `delayed` records missing from the delayed set, legacy (<= 0.1.x) `queued`
   * records missing from their lane list, and orphaned locks with no backing log record — all counted
   * here.
   */
  staleCount: number
  /** Number of log records removed: expired finished/error/stale records and unparseable garbage */
  cleanedCount: number
  /**
   * Number of runs put back on their lane: popped (in the `claiming` set) longer than the stale
   * threshold without being claimed — the popping instance died, or its claim failed and could not be
   * pushed back. The run never started, so re-queueing it loses nothing and cannot double-execute (a
   * late claim of the old pop is fenced). Always `0` in emergency mode.
   */
  requeuedCount: number
  /**
   * `'full'` — a normal pass (deletions first, then stale/orphan writes). `'emergency'` — Redis refused
   * the maintenance lock with an OOM error, so a lock-free, delete-only pass ran instead: expired
   * terminal records and garbage were deleted (deletions are allowed under OOM), nothing was written,
   * and `staleCount` is always `0`.
   */
  mode: 'full' | 'emergency'
}

/** Payload of the manager-level `maintenance` event: the outcome of one pass this instance ran. */
export interface MaintenanceEventPayload {
  /** The pass result (`mode: 'full' | 'emergency'`); `null` when the lock read failed or the pass threw. */
  result: MaintenanceResult | null
  /** Redis operations that failed and were skipped inside the pass (the pass still completed). */
  failedOps: number
  /** The first failure: the thrown error, the lock error, or the first failed operation. */
  error?: Error
  /** Classified cause of `error`. */
  reason?: RedisErrorReason
  /** Wall-clock ms of the pass itself (lock acquisition excluded); for a lock failure, of the failed lock attempt. */
  durationMs: number
}

/** One live consumer instance of a group, as listed by `RedisJM.fleet()`. */
export interface FleetInstance {
  instanceId: string
  /** The `instanceLabel` option, when set. */
  label?: string
  /** The instance's `concurrency`. */
  concurrency: number
  /** Lane labels it consumes (`'default'` for the default lane). */
  lanes: string[]
  /** Its normalized `laneConcurrency` option. */
  laneConcurrency: Record<string, number>
  /** Poll-loop runs in flight at its last refresh. */
  busy: number
  /** Epoch ms (instance clock) when it started. */
  startedAt: number
  /** Redis server ms of its last refresh (`expiresAt - ttl`). */
  seenAt: number
  /** Redis server ms after which, absent a refresh, it counts as gone. */
  expiresAt: number
}

/** Snapshot returned by `RedisJM.fleet()`. */
export interface RedisJMFleet {
  /** Live instances, sorted by `instanceId`. */
  instances: FleetInstance[]
  /** Sum of the instances' `concurrency`. */
  slots: number
  /** Sum of the instances' `busy`. */
  busy: number
  /**
   * Per lane: how many instances consume it and the sum of `min(concurrency, laneConcurrency[lane] ??
   * concurrency)`. Lanes share each instance's `concurrency`, so lane slots do NOT add up across lanes.
   */
  lanes: Record<string, { instances: number; slots: number }>
}

/** Options for `RedisJM.listQueued()`. */
export interface ListQueuedOptions {
  /** Lane label (`'default'` = the default lane). Omitted → every lane the group has entries on. */
  lane?: string
  /** Only runs of this job. */
  jobName?: string
  /** Max entries (default `100`, max `1000`). */
  limit?: number
  /** Skip this many matching entries of the ordered sequence (default `0`). Cost grows with the offset. */
  offset?: number
}

/** One run listed by `RedisJM.listQueued()`. */
export interface QueuedEntry {
  jobId: string
  jobName: string
  runId: string
  /** Lane label; absent only for a delayed / popped entry whose record is missing or unreadable. */
  lane?: string
  /** `'queued'` (on a lane list, or popped and not yet claimed) or `'delayed'`. */
  status: 'queued' | 'delayed'
  /** Epoch ms of the pop — present for a run popped but not yet claimed (its record is still `queued`). */
  poppedAt?: number
  /** Epoch ms the run becomes poppable — present for `'delayed'`. */
  readyAt?: number
}

/** One page of `RedisJM.listQueued()`. */
export interface QueuedPage {
  entries: QueuedEntry[]
  /** Offset for the next page; absent when the sequence is exhausted. */
  nextOffset?: number
  /** `false` when a per-call scan / decode bound stopped the listing early (narrow it with `lane` / `jobName`). */
  complete: boolean
}
