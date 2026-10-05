import { randomUUID } from 'node:crypto'
import { Hookable } from 'hookable'
import type Redis from 'ioredis'
import { classifyRedisError, JobTimeoutError, RedisJMEnqueueError } from './errors'
import type { EnqueueErrorReason, RedisErrorReason } from './errors'
import { getStartPhasePayload, Job } from './job'
import { createMaintenanceJob, MAINTENANCE_JOB_NAME, MAINTENANCE_LANE } from './maintenance'
import {
  DELETE_IF_UNCHANGED_SCRIPT,
  ENQUEUE_SCRIPT,
  FLEET_SCRIPT,
  LIST_QUEUED_SCRIPT,
  POP_SCRIPT,
  PRESENCE_SCRIPT,
  PRUNE_JOB_SCRIPT,
  PURGE_SCRIPT,
  runScript,
  sha1Hex,
  TRANSITION_SCRIPT,
} from './scripts'
import { checkAbortGraceMs, nonNegativeInt, positiveOrZero, toError, toTaggable } from './utils'
import type {
  EnqueueOptions,
  EnqueueResult,
  EveryOptions,
  FleetInstance,
  InFlightCounts,
  ListPage,
  ListPageOptions,
  ListQueuedOptions,
  RedisJMFleet,
  RedisJMHealth,
  JobAttrs,
  JobAttrValue,
  JobErrorEventPayload,
  JobEventPayload,
  JobFunction,
  JobLogRecord,
  JobMetadata,
  JobRetryEventPayload,
  JobStatus,
  JobUpdateEventPayload,
  MaintenanceEventPayload,
  MaintenanceResult,
  QueuedEntry,
  QueuedPage,
  QueueOptions,
  RedisJMHooks,
  RedisJMLogger,
  RedisJMOptions,
  RedisJMStats,
  ResolvedRedisJMOptions,
  StartFailedEventPayload,
  StopOptions,
} from './types'

const DEFAULT_OPTIONS: Omit<ResolvedRedisJMOptions, 'maintenanceInterval'> = {
  heartbeatInterval: 5000,
  roundsToStale: 2,
  // Observe outcomes out of the box: terminal records linger 60s so `get()`/`list()` can read a
  // finished/error/stale run. `0` opts into the legacy write-only behavior (drop on leaving running).
  keepFinishedInterval: 60_000,
  unknownJobRequeueLimit: 5,
  laneStrategy: 'roundRobin',
  lanePriority: [],
  concurrency: 1,
  jobTimeout: 0,
  abortGraceMs: false,
  presence: true,
  instanceLabel: '',
  maxRunMs: 0,
  laneConcurrency: {},
  laneCaps: {},
  maxInputsBytes: 0,
  maxRecordsPerPass: 1000,
  memoryWarnRatio: 0.8,
}

/** One lane of a pop: its list key and spec (`'*'` = own lane, or an allow-list `#jobA#jobB#`). */
type PollPlan = Array<{ key: string; spec: string }>

/** How often consumers re-read their jobs' lane sets to discover old lanes to drain. */
const OLD_LANE_REFRESH_MS = 5000

/** How many entries from the head of an OLD lane the pop script inspects for an allow-listed job. */
const OLD_LANE_SCAN_LIMIT = 100

/** Max length of `instanceLabel`. */
const MAX_INSTANCE_LABEL = 200

/** Records per `HMGET` of `getMany` (one pipeline carries every chunk). */
const GET_MANY_CHUNK = 500

/** `listQueued` bounds: default / max page size, and the per-call decode and scan caps of the script. */
const LIST_QUEUED_DEFAULT_LIMIT = 100
const LIST_QUEUED_MAX_LIMIT = 1000
const LIST_QUEUED_DECODE_CAP = 500
const LIST_QUEUED_SCAN_CAP = 10_000

/** Max jobs whose bookkeeping sets one maintenance pass prunes (one small script call each). */
const JOB_PRUNE_BATCH = 100

/** Job-lock-set members sampled per prune call (drift self-heals over passes). */
const JOB_LOCK_PRUNE_SAMPLE = 200

/**
 * How long one sighting of a pre-0.2 instance (see `noteLegacyInstance`) keeps maintenance's legacy
 * orphan check on for NEW-format `queued` records — floored at 10 stale thresholds. Long enough to cover
 * the two passes past the threshold the check needs after the last old instance is gone.
 */
const LEGACY_WINDOW_MS = 10 * 60_000

/** How many times `updateLog` re-reads and retries when its compare-and-set loses a race. */
const CAS_ATTEMPTS = 5

/**
 * What an `updateLog` mutator returns. `false` or a reason string REJECTS the write (nothing is touched;
 * the string comes back as `LogUpdate.reason`). Otherwise the record is written, and the queue-structure
 * side effects are DERIVED from the status change (see `RedisJM.transition`) — a mutator only states
 * what the status change can't: a queued → queued requeue's push side, and whether it must take the
 * run's `claiming` entry as a precondition (so two overlapping requeues can't both push it back).
 */
type Mutation = void | false | string | { push: 'L' | 'R'; takeFromClaiming?: boolean }

/** Result of `updateLog`. */
interface LogUpdate {
  outcome: 'written' | 'rejected' | 'missing'
  /** The record as written (`'written'`), or as read when the mutator rejected it. */
  record?: JobLogRecord
  /** The mutator's rejection reason, or `'precondition'` when a take-from-set precondition failed. */
  reason?: string
}

/** Per-pass failure tally of maintenance (see `RedisJM.createOpGuard`). */
interface OpGuard {
  /** Runs one guarded Redis operation; resolves whether it succeeded. */
  run: (op: () => Promise<unknown>) => Promise<boolean>
  /** Records a failure caught elsewhere. */
  fail: (err: unknown) => void
  /** Logs ONCE per pass: the failure count and the classified reason of the first failure. */
  report: (label: string) => void
  /** The failures so far: how many, and the first one (`undefined` when none). */
  summary: () => { failures: number; firstError: unknown }
}

/** One scanned log batch of a maintenance pass, split by `RedisJM.planLogCleanup`. */
interface LogBatch {
  /** Scanned jobId → raw JSON. */
  entries: Map<string, string>
  /** Cursor to resume the scan from (`'0'` = completed). */
  cursor: string
  garbage: string[]
  expired: string[]
  live: Array<[string, JobLogRecord]>
}

/** The `INFO memory` part of a health snapshot. */
type MemoryInfo = Pick<RedisJMHealth, 'usedMemory' | 'maxMemory' | 'usedRatio' | 'maxmemoryPolicy'>

/** Parses `field:value` lines of an `INFO` reply. */
function parseInfo(info: string): Map<string, string> {
  const map = new Map<string, string>()
  for (const line of info.split(/\r?\n/)) {
    const i = line.indexOf(':')
    if (i > 0 && !line.startsWith('#')) map.set(line.slice(0, i), line.slice(i + 1).trim())
  }
  return map
}

/** A persisted scan cursor as read back (`'0'` — start over — when absent, unreadable or malformed). */
function toCursor(value: unknown): string {
  return typeof value === 'string' && /^\d+$/.test(value) ? value : '0'
}

/** Normalizes a `Record<lane, number>` option: drops non-finite / negative values, floors the rest. */
function normalizeLaneLimits(limits: Record<string, number> | undefined): Record<string, number> {
  const out: Record<string, number> = {}
  for (const [lane, value] of Object.entries(limits ?? {})) {
    const limit = nonNegativeInt(value)
    if (limit !== undefined) out[lane] = limit
  }
  return out
}

/**
 * Max fields/members per batched HDEL/SREM in maintenance. Big enough that cleaning a backlog of
 * expired records is a handful of round trips, small enough that one command never blocks Redis
 * for long (each removal is O(1)).
 */
const DELETE_BATCH_SIZE = 500

/** Splits `"jobName#runId"` on its FIRST '#'; a separator-less id yields `{ jobName: id, runId: '' }`. */
function splitJobId(jobId: string): { jobName: string; runId: string } {
  const i = jobId.indexOf('#')
  return i === -1 ? { jobName: jobId, runId: '' } : { jobName: jobId.slice(0, i), runId: jobId.slice(i + 1) }
}

/** The 1-based attempt a claim of `record` writes (a record without `attempt` has had no claims). */
function nextAttempt(record: JobLogRecord): number {
  return (record.attempt ?? 0) + 1
}

/** Whether a status is terminal (the run is over: its lock is released, its record only kept as history). */
function isTerminal(status: JobStatus): boolean {
  return status === 'finished' || status === 'error' || status === 'stale'
}

/**
 * Whether a parsed value is a log record: a non-null object with a string `status`. Valid JSON that
 * isn't one — a foreign field holding `"42"`, `"true"`, `null`, or an array — would otherwise flow
 * through `as JobLogRecord` as a record with every property `undefined`.
 */
function isRecordShape(value: unknown): value is JobLogRecord {
  return typeof value === 'object' && value !== null && typeof (value as JobLogRecord).status === 'string'
}

/** `RedisJM.parseRecord`'s verdict, without its logging: whether stored `json` is a log record. */
function isRecordJson(json: string): boolean {
  try {
    return isRecordShape(JSON.parse(json))
  } catch {
    return false
  }
}

/** Pairs a `ZRANGEBYSCORE … WITHSCORES` reply (`[member, score, member, score, …]`) up. */
function scorePairs(flat: string[]): Array<[string, string]> {
  const pairs: Array<[string, string]> = []
  for (let i = 0; i + 1 < flat.length; i += 2) pairs.push([flat[i], flat[i + 1]])
  return pairs
}

/**
 * The mutator that fails a run which never started — still `queued` (popped, never claimed) — with a
 * terminal `error`; the lock release and history TTL ride on the transition. Rejects any other status.
 */
function failQueuedRun(message: string, now: number): (record: JobLogRecord) => Mutation {
  return (record) => {
    if (record.status !== 'queued') return false
    record.status = 'error'
    record.error = message
    record.finishedAt = now
  }
}

/**
 * Serializes a record whose inputs are already serialized (`inputsJson`; `undefined` = no inputs key, as
 * `JSON.stringify` omits it), appending them last — so an enqueue stringifies the inputs exactly once,
 * whether or not it also measured them against `maxInputsBytes`.
 */
function serializeRecord(record: Omit<JobLogRecord, 'inputs'>, inputsJson: string | undefined): string {
  const json = JSON.stringify(record)
  return inputsJson === undefined ? json : `${json.slice(0, -1)},"inputs":${inputsJson}}`
}

/**
 * Errors thrown by the manager's CLAIM write (the `start` hook's read-modify-write of the record).
 * Tagged so start-failure recovery knows the claim never landed (record still `queued`, popped entry
 * gone) and can push the entry back instead of routing it through the run's failure path.
 */
const claimWriteFailures = new WeakSet<object>()

/**
 * Shared pre-resolved execution thunk for the pop branches that already did their Redis cleanup
 * inline (garbage drop, unknown-job drop, missing-record drop, non-jobId cleanup). Returning it from
 * `popNext` makes those branches count as "work found" for pacing (immediate re-poll) exactly like the
 * old `return true`, without dispatching any actual execution.
 */
const NOOP_THUNK = (): Promise<void> => Promise.resolve()

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
 * Manages job scheduling, execution, and lifecycle with a few Redis structures per target group: a
 * List per lane (queue), a Set (locks), a Hash (log), a sorted set of delayed runs, and a sorted set of
 * popped-but-not-yet-claimed runs (`claiming`).
 *
 * REQUIRES Redis >= 7.0: pops and record transitions are `#!lua` shebang scripts (so that a Redis at
 * `maxmemory` refuses a pop up front instead of losing the popped run). Field-level history expiry
 * (`HPEXPIRE`) is used when the server has it (>= 7.4) and skipped otherwise.
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
  /**
   * The maintenance pass currently running from this instance's timer, if any. The timer skips a tick
   * while it is set (passes never overlap on one instance), and `stop()` awaits it. Never rejects.
   */
  private maintenancePass: Promise<void> | undefined
  /**
   * Running count of enqueues Redis refused with an OOM error (`RedisJMEnqueueError` reason `'oom'`)
   * since this manager was created — a cheap pressure signal for operational health reporting.
   */
  private oomRefusals = 0
  /**
   * Epoch ms until which the poll loop does not pop, set by start-failure recovery (see
   * `recoverUnclaimed`). After a requeue it is a short back-off (≤ 1s) so a persistent failure can't
   * spin pop → fail → requeue at full speed. After an OOM / a deferral it is a whole stale-threshold
   * window: every further pop would hit the same wall and park its run in `claiming` for maintenance to
   * requeue later, so the loop backs off instead. Maintenance keeps running meanwhile (its emergency
   * pass frees memory).
   */
  private popsPausedUntil = 0
  /**
   * Promises of the runs currently executing in the poll loop (up to `concurrency`). The poll loop
   * dispatches into this set without awaiting so multiple runs can be in flight at once; `stop()`
   * awaits them all to drain. Each promise swallows its own errors, so members never reject.
   */
  private readonly inFlightRuns = new Set<Promise<void>>()
  /**
   * Abort controllers of the in-flight runs — one per dispatched execution. `stop({ abort: true })`
   * aborts every one (cooperative fast shutdown); each is removed when its run settles.
   */
  private readonly abortControllers = new Set<AbortController>()
  /**
   * Promise of the poll invocation currently executing, assigned synchronously at EVERY invocation
   * (the initial `poll()` and each timer callback). `stop()` must await it BEFORE snapshotting
   * `inFlightRuns`: a poll caught mid-pop has already removed the queue entry from Redis, so the run
   * it is about to dispatch must be part of the drain — snapshotting earlier would let `stop()`
   * resolve while that popped job still executes (e.g. after the caller has closed Redis). Never
   * rejects (all of `poll`'s awaits are caught).
   */
  private currentPoll: Promise<void> | undefined
  /**
   * Incremented by every `start()`. A poll loop only keeps scheduling itself while its own generation is
   * current: a poll still in flight from before a `stop()` must not resume scheduling when `start()` is
   * called again before it settles — that would run TWO loops (exceeding `concurrency`, and leaving a
   * timer the next `stop()` doesn't know about).
   */
  private pollGeneration = 0
  /** Round-robin cursor rotating the work-lane poll order once per poll to prevent starvation. */
  private pollCursor = 0
  /**
   * Set by `stop()`, cleared by `start()`: once stopped, `popAndExecute()` refuses to pop (returns
   * `false`), so nothing new starts while the caller is shutting down.
   */
  private stopped = false
  /** In-flight `popAndExecute()` calls (pop + run), drained by `stop()` like poll-loop runs. Never reject. */
  private readonly manualRuns = new Set<Promise<unknown>>()
  /** Poll-loop runs in flight per lane queue key — enforces `laneConcurrency`. */
  private readonly laneInFlight = new Map<string, number>()
  /** `laneConcurrency` resolved to lane queue keys (the poll loop only ever sees keys). */
  private readonly laneConcurrencyByKey = new Map<string, number>()
  /**
   * This instance's own lanes, rebuilt after a job is (un)registered (see `getSubscribedQueueKeys`):
   * the distinct work-lane keys in base order (`lanePriority` first under `priority`) and the set of
   * every own key (work lanes + the maintenance lane).
   */
  private ownLanes: { workKeys: string[]; keys: Set<string> } | undefined
  /** True while the poll loop sleeps its idle `interval` (the only wait `wake()` may cut short). */
  private idleWaiting = false
  /** A `wake()` that arrived while a poll was in flight: the next scheduling re-polls immediately. */
  private wakePending = false
  /** Re-poll entry point of the running loop (set by `start()`), used by `wake()`. */
  private schedulePoll: ((delay: number) => void) | undefined
  /** Timers created by `every()`, cleared by `stop()`. */
  private readonly everyTimers = new Set<ReturnType<typeof setInterval>>()
  /** `memoryPressure` edge trigger: fires when crossing upward while armed; re-armed below the ratio. */
  private memoryPressureArmed = true
  /** Local log-scan cursor for EMERGENCY passes (the shared cursor can't be written under OOM). */
  private emergencyCursor = '0'
  /** Whether the current OOM pop-refusal episode was already logged (logged once per episode). */
  private popOomLogged = false
  /** Old lanes this instance's jobs still have entries on, with their allow-lists (see `getPollPlan`). */
  private oldLanes: PollPlan = []
  /** Epoch ms of the last `refreshOldLanes` read. */
  private lastOldLaneRefresh = 0
  /** Epoch ms of the last delayed-set promotion sweep; rate-limits `promoteDueDelayed` (see there). */
  private lastPromotionCheck = 0
  /**
   * Tail of the per-record write queue (see `updateLog`): this instance's writes to one record run one
   * after another, so they never race each other's compare-and-set. Entries are removed once idle.
   */
  private readonly recordWrites = new Map<string, Promise<unknown>>()
  /**
   * The JSON this instance last saw of a record it is about to write again — read on the pop (the
   * claim's input) or written as the run's owner while `running` (the next heartbeat/update/finish's
   * input). It seeds `updateLog`'s first compare-and-set, saving the re-read; a stale seed just costs
   * one compare-and-set miss and a re-read. Dropped on any write that isn't an owner's `running` write,
   * so it holds about one entry per run executing here.
   */
  private readonly recordJson = new Map<string, string>()
  /** This manager's id in the group's fleet registry (stable across `stop()` / `start()`). */
  private readonly instanceId = randomUUID()
  /** Fleet-presence refresh timer, alive between `start()` and `stop()`. */
  private presenceTimer: ReturnType<typeof setInterval> | undefined
  /**
   * Every presence write (refresh, deregistration) runs through this chain, one at a time, each op
   * re-checking its intent when it runs (see {@link queuePresence}). Never rejects.
   */
  private presenceTail: Promise<void> = Promise.resolve()
  /** A refresh is queued and has not started yet: further timer ticks coalesce into it. */
  private presenceRefreshQueued = false
  /** The info JSON stored in Redis by the last successful write (`undefined` = none is stored). */
  private presenceSent: string | undefined
  /** The static part of the fleet entry's info (lanes, label, …), cached until the registrations change. */
  private presenceBase: Record<string, unknown> | undefined
  /** Whether the current presence-refresh failure episode was already logged. */
  private presenceFailLogged = false
  /** Epoch ms of the last `start()` (the fleet entry's `startedAt`). */
  private startedAt = 0

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
    const concurrency = options?.concurrency ?? DEFAULT_OPTIONS.concurrency
    const abortGraceMs = checkAbortGraceMs(options?.abortGraceMs, 'RedisJMOptions') ?? DEFAULT_OPTIONS.abortGraceMs
    const instanceLabel = options?.instanceLabel ?? DEFAULT_OPTIONS.instanceLabel
    if (typeof instanceLabel !== 'string' || instanceLabel.length > MAX_INSTANCE_LABEL) {
      throw new TypeError(`instanceLabel must be a string of at most ${MAX_INSTANCE_LABEL} characters`)
    }
    // Validate before flooring: reject NaN/Infinity/< 1 outright, then floor a valid value (2.7 → 2)
    // so a single instance never runs zero or a fractional number of jobs at once.
    if (!Number.isFinite(concurrency) || concurrency < 1) {
      throw new TypeError(`concurrency must be a positive integer >= 1, got ${String(concurrency)}`)
    }
    this.options = {
      heartbeatInterval,
      roundsToStale,
      keepFinishedInterval: options?.keepFinishedInterval ?? DEFAULT_OPTIONS.keepFinishedInterval,
      maintenanceInterval: options?.maintenanceInterval ?? heartbeatInterval * roundsToStale,
      unknownJobRequeueLimit: options?.unknownJobRequeueLimit ?? DEFAULT_OPTIONS.unknownJobRequeueLimit,
      laneStrategy: options?.laneStrategy ?? DEFAULT_OPTIONS.laneStrategy,
      lanePriority: options?.lanePriority ?? DEFAULT_OPTIONS.lanePriority,
      concurrency: Math.floor(concurrency),
      jobTimeout: positiveOrZero(options?.jobTimeout),
      abortGraceMs,
      presence: options?.presence ?? DEFAULT_OPTIONS.presence,
      instanceLabel,
      maxRunMs: positiveOrZero(options?.maxRunMs),
      laneConcurrency: normalizeLaneLimits(options?.laneConcurrency),
      laneCaps: normalizeLaneLimits(options?.laneCaps),
      maxInputsBytes: positiveOrZero(options?.maxInputsBytes),
      maxRecordsPerPass: Math.max(1, Math.floor(positiveOrZero(options?.maxRecordsPerPass) || DEFAULT_OPTIONS.maxRecordsPerPass)),
      memoryWarnRatio: options?.memoryWarnRatio === undefined
        ? DEFAULT_OPTIONS.memoryWarnRatio
        : Math.max(0, Number.isFinite(options.memoryWarnRatio) ? options.memoryWarnRatio : 0),
    }
    for (const [lane, cap] of Object.entries(this.options.laneConcurrency)) {
      this.laneConcurrencyByKey.set(this.getQueueKey(lane), cap)
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
   * This manager's id in the group's fleet registry (see {@link fleet}): a random UUID, fixed for the
   * manager's lifetime (it survives `stop()` / `start()`), so a log line or a `fleet()` entry can be tied
   * back to the process.
   */
  getInstanceId(): string {
    return this.instanceId
  }

  /**
   * Checks whether a jobId currently holds a lock. A lock is held for the WHOLE active lifecycle:
   * `queued` (waiting on a lane list), `delayed` (staged on the delayed set) and `running` (executing)
   * — NOT just "queued". Returns `false` once the run reaches a terminal state, whose write releases the
   * lock in the same atomic step: `finished`, `error`, and `stale` (maintenance releases the lock as it
   * marks the run stale; a stale run that proves itself alive re-takes it).
   *
   * @example
   * ```ts
   * const locked = await manager.isLocked('send-email#daily-digest')
   * ```
   */
  async isLocked(jobId: string): Promise<boolean> {
    const result = await this.redis.sismember(this.getLocksKey(), jobId)
    return result === 1
  }

  /**
   * @deprecated Use {@link isLocked}. The name is misleading: it returns `true` for a lock held in
   * ANY active state (queued, delayed or running), not only `queued`. Thin delegating alias.
   */
  async isQueued(jobId: string): Promise<boolean> {
    return this.isLocked(jobId)
  }

  /**
   * Point-in-time snapshot for dashboards/introspection: per-lane queue depths, the delayed-set size,
   * total held locks, and log-record counts by status. All reads are best-effort and independent (no
   * cross-structure transaction), so a run mid-transition may be double- or un-counted for one poll.
   *
   * SCOPE (best-effort lane visibility): queue depths are reported for the lanes this instance can
   * name — the default lane, the reserved `__maintenance` lane, its registered jobs' lanes, plus any
   * lane discovered on a scanned log record (so a producer-only instance still sees a consumer lane's
   * backlog once that lane has records). A lane with queue entries but NO log records AND no local
   * registration is invisible here (nothing names it).
   *
   * @example
   * ```ts
   * const { queues, delayed, locks, statuses } = await manager.stats()
   * ```
   */
  async stats(): Promise<RedisJMStats> {
    // Status counts come from a single log scan; initialize every status to 0 so the shape is stable
    // regardless of which statuses are currently present. That scan also surfaces lanes (below).
    const statuses: Record<JobStatus, number> = {
      queued: 0,
      running: 0,
      finished: 0,
      error: 0,
      stale: 0,
      delayed: 0,
    }
    const labels = this.knownLaneLabels()
    // Count statuses (single log scan, reusing list()) and pick up any lane seen on a record (a
    // producer-only instance still observes a consumer lane's backlog once that lane has records —
    // see the SCOPE note above).
    for (const record of await this.list()) {
      // A foreign-but-shape-valid record can carry a status string outside the known set; guard the
      // increment so it can't seed a NaN bucket in the histogram.
      if (record.status in statuses) statuses[record.status]++
      labels.add(this.laneLabel(record.lane))
    }
    // Independent, non-transactional reads in one batch — see the doc note above.
    const [delayed, locks, queues] = await Promise.all([
      this.redis.zcard(this.getDelayedKey()),
      this.redis.scard(this.getLocksKey()),
      this.laneDepths(labels),
    ])
    return { queues, delayed, locks, statuses }
  }

  /**
   * Operational health snapshot (see `RedisJMHealth`): Redis memory use and eviction policy from
   * `INFO memory` (works on managed Redis services that block `CONFIG`, and while Redis is full), this
   * instance's OOM-refused enqueue count, and cheap cardinalities — per-lane queue lengths, delayed,
   * claiming, locks. `running` is an estimate and `stale` is `null` unless `{ scan: true }`, which adds
   * one O(n) log scan to count them exactly — keep that for occasional/dashboard use.
   *
   * @example
   * ```ts
   * const h = await manager.health()
   * if (h.usedRatio !== null && h.usedRatio > 0.9) alert(h)
   * ```
   */
  async health(options?: { scan?: boolean }): Promise<RedisJMHealth> {
    return this.collectHealth(await this.readMemoryInfo(), options?.scan ?? false)
  }

  /** {@link health} with the `INFO memory` part already read (the memory-pressure check has it). */
  private async collectHealth(memory: MemoryInfo, scan: boolean): Promise<RedisJMHealth> {
    const labels = this.knownLaneLabels()
    for (const lane of Object.keys(this.options.laneCaps)) labels.add(lane)
    for (const lane of Object.keys(this.options.laneConcurrency)) labels.add(lane)
    const [delayed, claiming, locks, queues] = await Promise.all([
      this.redis.zcard(this.getDelayedKey()),
      this.redis.zcard(this.getClaimingKey()),
      this.redis.scard(this.getLocksKey()),
      this.laneDepths(labels),
    ])
    const queued = Object.values(queues).reduce((a, b) => a + b, 0)
    let running = Math.max(0, locks - queued - delayed - claiming)
    let stale: number | null = null
    if (scan) {
      running = 0
      stale = 0
      for (const { status } of await this.list()) {
        if (status === 'running') running++
        else if (status === 'stale') stale++
      }
    }
    return { ...memory, oomRefusals: this.oomRefusals, queues, delayed, claiming, running, stale, locks }
  }

  /**
   * The lane labels this instance can name on its own: the default lane, the reserved maintenance lane
   * and every registered job's lane (deduped by label, which collapses a no-lane job and an explicit
   * `lane: 'default'` job onto the single legacy queue — see `laneLabel`).
   */
  private knownLaneLabels(): Set<string> {
    const labels = new Set<string>(['default', MAINTENANCE_LANE])
    for (const job of this.registeredJobs) labels.add(this.laneLabel(job.getLane()))
    return labels
  }

  /** Queue depth (LLEN) of each lane, keyed by label — independent reads issued together. */
  private async laneDepths(labels: Iterable<string>): Promise<Record<string, number>> {
    const list = [...labels]
    const lens = await Promise.all(list.map((label) => this.redis.llen(this.getQueueKey(label))))
    return Object.fromEntries(list.map((label, i) => [label, lens[i]]))
  }

  /**
   * In-flight runs of one job name, without scanning the log: reads the job's lock set and exactly those
   * records (one `HMGET`) — O(runs in flight of this job), independent of the log size and of every other
   * job. Popped-not-yet-claimed runs count as `queued`. See `InFlightCounts` for how `total` relates to
   * the lock-set size `maxInFlight` is enforced against. When only the number is needed, use
   * {@link inFlightCount} (one O(1) `SCARD`).
   *
   * @example
   * ```ts
   * const { total, running } = await manager.inFlight('send-email')
   * ```
   */
  async inFlight(jobName: string): Promise<InFlightCounts> {
    const ids = await this.redis.smembers(this.getJobLocksKey(jobName))
    const counts: InFlightCounts = { total: 0, queued: 0, delayed: 0, running: 0 }
    if (ids.length === 0) return counts
    const values = await this.redis.hmget(this.getLogKey(), ...ids)
    values.forEach((json, i) => {
      const status = this.decodeRecord(json, ids[i])?.status
      if (status === 'queued' || status === 'delayed' || status === 'running') {
        counts[status]++
        counts.total++
      }
    })
    return counts
  }

  /**
   * Number of runs of `jobName` holding a lock — queued (including popped-not-yet-claimed), delayed and
   * running — as ONE O(1) `SCARD` of the job's lock set, reading no records. It is exactly what
   * `maxInFlight` is enforced against: an enqueue sees `'busy'` iff this is `>= maxInFlight` at that
   * instant. Prefer it to {@link inFlight} on hot paths that only gate on the number. In a group where a
   * pre-0.2 instance still writes, the count drifts (that instance neither adds to nor removes from the
   * per-job set) until those runs drain and maintenance prunes the set — see Upgrading.
   *
   * @example
   * ```ts
   * if (await manager.inFlightCount('send-email') >= 10) return // shed load
   * ```
   */
  async inFlightCount(jobName: string): Promise<number> {
    return this.redis.scard(this.getJobLocksKey(jobName))
  }

  /**
   * One page of log records, HSCAN-based so a large log can be listed incrementally (unlike `list()`,
   * which reads it all). Filters (`status`, `lane`, `jobName`) are applied to the scanned records, and
   * one call keeps scanning until it has about `limit` matches or reaches the end of the log — so only
   * the last page comes back short (it may slightly exceed `limit`: the last HSCAN slice is kept whole),
   * and a very selective filter may scan most of the log in a single call. Keep calling with the
   * returned `cursor` until it is `'0'`. HSCAN semantics apply: a record may appear on two pages if the
   * log changes meanwhile.
   *
   * @example
   * ```ts
   * let cursor = '0'
   * do {
   *   const page = await manager.listPage({ status: 'error', cursor })
   *   render(page.records)
   *   cursor = page.cursor
   * } while (cursor !== '0')
   * ```
   */
  async listPage(options: ListPageOptions = {}): Promise<ListPage> {
    const limit = Math.max(1, Math.floor(options.limit ?? 100))
    const lane = options.lane === undefined ? undefined : this.laneLabel(options.lane)
    const records: JobLogRecord[] = []
    const cursor = await this.scan('hscan', this.getLogKey(), options.cursor ?? '0', limit, (flat) => {
      for (let i = 0; i < flat.length; i += 2) {
        const record = this.parseRecord(flat[i + 1], flat[i])
        if (!record) continue
        if (options.status !== undefined && record.status !== options.status) continue
        if (options.jobName !== undefined && record.jobName !== options.jobName) continue
        if (lane !== undefined && this.laneLabel(record.lane) !== lane) continue
        records.push(record)
      }
      return records.length < limit
    })
    return { records, cursor }
  }

  /**
   * Records for many jobIds in ONE round trip (a pipeline of `HMGET`s, 500 fields per command), in input
   * order — duplicates included; `undefined` for a missing or unparseable record. Inputs are included, as
   * with {@link get}. `[]` makes no Redis call.
   *
   * @example
   * ```ts
   * const records = await manager.getMany(['send-email#a', 'send-email#b'])
   * ```
   */
  async getMany(jobIds: string[]): Promise<Array<JobLogRecord | undefined>> {
    if (jobIds.length === 0) return []
    const pipeline = this.redis.pipeline()
    for (let i = 0; i < jobIds.length; i += GET_MANY_CHUNK) {
      pipeline.hmget(this.getLogKey(), ...jobIds.slice(i, i + GET_MANY_CHUNK))
    }
    const jsons: Array<string | null> = []
    for (const [err, values] of (await pipeline.exec()) ?? []) {
      if (err) throw err
      jsons.push(...(values as Array<string | null>))
    }
    return jobIds.map((jobId, i) => this.decodeRecord(jsons[i], jobId) ?? undefined)
  }

  /**
   * Lists runs waiting to run, in the order they leave the queue — one atomic, read-only snapshot, no
   * inputs ever leaving Redis. The sequence is: runs popped but not yet claimed (by pop time), then the
   * lane lists head → tail (a priority insert is at the head), then the delayed set by `readyAt` (each is
   * pushed to its lane's tail when it falls due). With `lane` this is the order in which the current
   * entries leave that lane (absent new head inserts); across lanes there is NO global pop order —
   * consumers interleave lanes per `laneStrategy`. Without `lane`, every lane the group has entries on is
   * listed: `default` first, then the others alphabetically (the reserved `__maintenance` lane only when
   * asked for explicitly). A run whose lane lists are only in a pre-0.2 producer's lane set is found only
   * when its lane is named or registered locally.
   *
   * A `lane` filter on the delayed / popped entries resolves each one's lane from its record, inside the
   * script (`cjson`, at most 500 decodes per call — decode cost scales with the record's size, on the Redis
   * thread). A per-call decode / scan bound sets `complete: false`: narrow the listing with `lane` /
   * `jobName`. `complete: false` is EXPECTED when a page needs more than 500 record decodes — no `lane`
   * filter, many delayed / popped entries inside the page. Without `lane`, the lanes to read are
   * discovered client-side first (a registry `SMEMBERS`, then one pipelined read of the lane sets; with
   * `jobName` only that one job's lane set, in the pipeline) — two small round trips before the script.
   * Same exposure as {@link listPage} / {@link get}: runIds may carry entity ids — gate any
   * admin endpoint built on it.
   *
   * @example
   * ```ts
   * let page = await manager.listQueued({ lane: 'images', limit: 50 })
   * while (page.nextOffset !== undefined) page = await manager.listQueued({ lane: 'images', limit: 50, offset: page.nextOffset })
   * ```
   */
  async listQueued(options: ListQueuedOptions = {}): Promise<QueuedPage> {
    const limit = Math.min(LIST_QUEUED_MAX_LIMIT, Math.max(1, nonNegativeInt(options.limit) ?? LIST_QUEUED_DEFAULT_LIMIT))
    const offset = nonNegativeInt(options.offset) ?? 0
    const filter = options.lane === undefined ? '' : this.laneLabel(options.lane)
    const labels = filter ? [filter] : await this.listedLaneLabels(options.jobName)
    const res = await runScript(
      this.redis,
      LIST_QUEUED_SCRIPT,
      [this.getClaimingKey(), this.getDelayedKey(), this.getLogKey(), ...labels.map((label) => this.getQueueKey(label))],
      [
        filter,
        options.jobName === undefined ? '' : `${options.jobName}#`,
        offset,
        limit,
        LIST_QUEUED_DECODE_CAP,
        LIST_QUEUED_SCAN_CAP,
        ...labels,
      ],
    )
    const flat = Array.isArray(res) ? res : []
    const entries: QueuedEntry[] = []
    for (let i = 2; i + 3 < flat.length; i += 4) {
      const kind = String(flat[i])
      const jobId = String(flat[i + 1])
      const score = Number(flat[i + 2])
      const lane = String(flat[i + 3])
      const entry: QueuedEntry = { jobId, ...splitJobId(jobId), status: kind === 'd' ? 'delayed' : 'queued' }
      if (lane) entry.lane = lane
      if (kind === 'c') entry.poppedAt = score
      if (kind === 'd') entry.readyAt = score
      entries.push(entry)
    }
    const page: QueuedPage = { entries, complete: Number(flat[0]) === 1 }
    if (Number(flat[1]) === 1) page.nextOffset = offset + entries.length
    return page
  }

  /**
   * The lane labels `listQueued` walks when no lane is given: `default`, then every other lane — the
   * lists named by the jobs' lane sets (all jobs, or just `jobName`'s) and the ones this instance knows
   * (registered jobs' lanes) — alphabetically; the reserved maintenance lane is left out.
   */
  private async listedLaneLabels(jobName?: string): Promise<string[]> {
    const others = new Set<string>()
    const add = (label: string) => {
      if (label !== 'default' && label !== MAINTENANCE_LANE) others.add(label)
    }
    for (const job of this.registeredJobs) add(this.laneLabel(job.getLane()))
    const names = jobName === undefined ? await this.redis.smembers(this.getJobRegistryKey()) : [jobName]
    for (const keys of await this.readJobLaneSets(names)) {
      for (const key of keys) {
        const label = this.laneLabelOfKey(key)
        if (label !== undefined) add(label)
      }
    }
    return ['default', ...[...others].sort()]
  }

  /** The lane sets (lane list KEYS) of each named job, in one pipeline; `[]` for a failed read. */
  private async readJobLaneSets(names: string[]): Promise<string[][]> {
    if (names.length === 0) return []
    const pipeline = this.redis.pipeline()
    for (const name of names) pipeline.smembers(this.getJobLanesKey(name))
    return ((await pipeline.exec()) ?? []).map(([err, keys]) => (!err && Array.isArray(keys) ? (keys as string[]) : []))
  }

  /**
   * Enqueues `job` every `intervalMs` from this instance's own timer (works with or without `start()`;
   * cleared by `stop()`), and returns a function that stops it. With `skipIfInFlight` (default) every
   * tick uses the same runId, so the run lock dedupes a tick while the previous run is still
   * queued/delayed/running — across ALL instances running the same `every()`; with `false` each tick
   * enqueues a distinct run. Enqueue failures are logged (and fire `enqueueFailed`), never thrown from
   * the timer.
   *
   * @example
   * ```ts
   * const cancel = manager.every(cleanupJob, 60_000, { inputs: null, immediate: true })
   * ```
   */
  every<TInputs>(job: Job<TInputs, any>, intervalMs: number, options: EveryOptions<TInputs>): () => void {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new TypeError(`every(intervalMs): interval must be a positive number, got ${intervalMs}`)
    }
    const base = options.runId ?? 'every'
    const skipIfInFlight = options.skipIfInFlight ?? true
    const tick = () => {
      const runId = skipIfInFlight ? base : `${base}-${Date.now()}`
      this.enqueue(job, runId, options.inputs).catch((err) => {
        this.logger(`every(): enqueue of "${job.getJobId(runId)}" failed`, toError(err))
      })
    }
    const timer = setInterval(tick, intervalMs)
    this.everyTimers.add(timer)
    if (options.immediate) tick()
    return () => {
      clearInterval(timer)
      this.everyTimers.delete(timer)
    }
  }

  /**
   * The live consumer instances of this group — those running `start()` with `presence` on — and their
   * capacity (see `RedisJMFleet`): per instance its `concurrency`, lanes, `laneConcurrency` and how busy
   * it was at its last refresh, and the group totals (`slots`, `busy`, per-lane consumers and slots).
   * One read-only script, O(instances); works under maxmemory and on replicas. Liveness is a lease on
   * REDIS server time, refreshed every `heartbeatInterval` and lapsing after `heartbeatInterval ×
   * roundsToStale` (so a pinned event loop lapses like its runs do).
   *
   * NOT counted: drain workers that only call `popAndExecute()`, producer-only processes, instances with
   * `presence: false`, and instances older than 0.3.0 — during a rolling deploy `fleet()` under-counts
   * until the rollout completes.
   *
   * @example
   * ```ts
   * const { slots, lanes } = await manager.fleet()
   * const wave = lanes['images']?.slots ?? 0
   * ```
   */
  async fleet(): Promise<RedisJMFleet> {
    const res = await runScript(this.redis, FLEET_SCRIPT, [this.getInstancesKey(), this.getInstanceInfoKey()], [])
    const flat = Array.isArray(res) ? res : []
    const instances: FleetInstance[] = []
    for (let i = 1; i + 2 < flat.length; i += 3) {
      const instanceId = String(flat[i])
      const expiresAt = Number(flat[i + 1])
      const info = this.parseInstanceInfo(flat[i + 2], instanceId)
      if (!info) continue
      const { ttlMs, ...rest } = info
      instances.push({ ...rest, seenAt: expiresAt - ttlMs, expiresAt })
    }
    instances.sort((a, b) => (a.instanceId < b.instanceId ? -1 : a.instanceId > b.instanceId ? 1 : 0))
    const fleet: RedisJMFleet = { instances, slots: 0, busy: 0, lanes: {} }
    for (const instance of instances) {
      fleet.slots += instance.concurrency
      fleet.busy += instance.busy
      for (const lane of instance.lanes) {
        const entry = (fleet.lanes[lane] ??= { instances: 0, slots: 0 })
        entry.instances++
        entry.slots += Math.min(instance.concurrency, instance.laneConcurrency[lane] ?? instance.concurrency)
      }
    }
    return fleet
  }

  /** A fleet entry's stored info JSON, or `null` (logged) when it is missing or not an instance record. */
  private parseInstanceInfo(
    json: unknown,
    instanceId: string,
  ): (Omit<FleetInstance, 'seenAt' | 'expiresAt'> & { ttlMs: number }) | null {
    let value: any
    try {
      value = typeof json === 'string' ? JSON.parse(json) : undefined
    } catch {
      value = undefined
    }
    const valid = typeof value === 'object' && value !== null
      && typeof value.concurrency === 'number' && typeof value.busy === 'number'
      && typeof value.startedAt === 'number' && typeof value.ttlMs === 'number'
      && Array.isArray(value.lanes) && value.lanes.every((lane: unknown) => typeof lane === 'string')
      && typeof value.laneConcurrency === 'object' && value.laneConcurrency !== null
    if (!valid) {
      this.logger(`skipping unreadable fleet entry "${instanceId}"`)
      return null
    }
    return {
      instanceId,
      ...(typeof value.label === 'string' && value.label ? { label: value.label } : {}),
      concurrency: value.concurrency,
      lanes: value.lanes,
      laneConcurrency: value.laneConcurrency,
      busy: value.busy,
      startedAt: value.startedAt,
      ttlMs: value.ttlMs,
    }
  }

  /**
   * Runs a presence write after every earlier one: refreshes and deregistrations never overlap, so a
   * refresh can't re-add an entry a deregistration just removed, and a quick `stop()` / `start()` can't
   * race. `op` must not reject and re-checks the intent (timer armed or not) when it runs.
   */
  private queuePresence(op: () => Promise<void>): Promise<void> {
    return (this.presenceTail = this.presenceTail.then(op))
  }

  /** Queues a refresh of this instance's fleet entry (see {@link refreshPresence}); coalesces while one is queued. */
  private schedulePresenceRefresh(): void {
    if (this.presenceRefreshQueued) return
    this.presenceRefreshQueued = true
    void this.queuePresence(() => {
      this.presenceRefreshQueued = false
      return this.refreshPresence()
    })
  }

  /** The info the fleet entry carries besides `busy`: rebuilt only when jobs are (un)registered or on `start()`. */
  private presenceBaseInfo(): Record<string, unknown> {
    return (this.presenceBase ??= {
      ...(this.options.instanceLabel ? { label: this.options.instanceLabel } : {}),
      concurrency: this.options.concurrency,
      lanes: [...new Set([...this.registeredJobs].map((job) => this.laneLabel(job.getLane())))]
        .filter((label) => label !== MAINTENANCE_LANE)
        .sort(),
      laneConcurrency: this.options.laneConcurrency,
      startedAt: this.startedAt,
      ttlMs: this.getPresenceLease(),
    })
  }

  /** Fleet lease length: the stale threshold, but at least two refresh intervals (no flicker at `roundsToStale <= 1`). */
  private getPresenceLease(): number {
    return this.options.heartbeatInterval * Math.max(this.options.roundsToStale, 2)
  }

  /**
   * Refreshes this instance's fleet entry (see {@link fleet}) — a no-op unless `start()` armed the timer.
   * The lease (ZADD) is renewed every beat; the info JSON is sent only when it differs from what Redis
   * holds (or Redis lost it). Never rejects; a failure is logged once per episode (under maxmemory the
   * refresh is refused and the entry lapses after its ttl — truthful: a Redis at maxmemory refuses every
   * pop anyway).
   */
  private async refreshPresence(): Promise<void> {
    if (!this.presenceTimer) return
    const info = JSON.stringify({ ...this.presenceBaseInfo(), busy: this.inFlightRuns.size })
    const keys = [this.getInstancesKey(), this.getInstanceInfoKey()]
    // The lease is at least two refresh intervals: `roundsToStale <= 1` (valid for run staleness) would
    // otherwise lapse the entry between refreshes and make it flicker in `fleet()`.
    const ttlMs = this.getPresenceLease()
    // Key-level expiry so an abandoned group's presence keys clean up: well beyond any live lease.
    const keyTtlMs = Math.max(ttlMs * 3, 60_000)
    try {
      const unchanged = info === this.presenceSent
      // `1` = no info was sent and Redis holds none for this instance (lapsed, flushed): send it now.
      const needInfo = await runScript(this.redis, PRESENCE_SCRIPT, keys, [this.instanceId, ttlMs, unchanged ? '' : info, keyTtlMs])
      if (needInfo === 1) await runScript(this.redis, PRESENCE_SCRIPT, keys, [this.instanceId, ttlMs, info, keyTtlMs])
      this.presenceSent = info
      this.presenceFailLogged = false
    } catch (err) {
      if (this.presenceFailLogged) return
      this.presenceFailLogged = true
      this.logger(`fleet presence refresh failed (${classifyRedisError(err)}); this instance may lapse from fleet()`, toError(err))
    }
  }

  /**
   * Removes this instance's fleet entry (best effort, logged on failure): one `MULTI ZREM + HDEL`
   * (deletions, accepted under maxmemory). A no-op when a later `start()` re-armed the timer meanwhile.
   */
  private async deregisterPresence(): Promise<void> {
    if (this.presenceTimer) return
    try {
      const results = await this.redis.multi()
        .zrem(this.getInstancesKey(), this.instanceId)
        .hdel(this.getInstanceInfoKey(), this.instanceId)
        .exec()
      const failed = (results ?? []).find(([err]) => err)
      if (failed) throw failed[0]
      this.presenceSent = undefined
    } catch (err) {
      this.logger('fleet presence deregistration failed (the entry lapses on its own)', toError(err))
    }
  }

  /** Reads `used_memory` / `maxmemory` / `maxmemory_policy` from `INFO memory`. */
  private async readMemoryInfo(): Promise<MemoryInfo> {
    const info = parseInfo(await this.redis.info('memory'))
    const usedMemory = Number(info.get('used_memory') ?? 0)
    const maxMemory = Number(info.get('maxmemory') ?? 0)
    return {
      usedMemory,
      maxMemory,
      usedRatio: maxMemory > 0 ? usedMemory / maxMemory : null,
      maxmemoryPolicy: info.get('maxmemory_policy') ?? '',
    }
  }

  /**
   * Run once by `start()`: warns when Redis' eviction policy can hurt a job queue. Read from
   * `INFO memory` — never `CONFIG GET`, which managed Redis services commonly block.
   * - `allkeys-*`: Redis may evict queue/lock/log keys under memory pressure → lost or duplicated work.
   * - `volatile-*`: redisjm keys carry no key TTL, so the policy cannot free them — Redis behaves like
   *   `noeviction` for them while other volatile keys get evicted.
   * Recommended: `noeviction`, a memory alarm (see `memoryWarnRatio` / `health()`), and ideally a
   * dedicated instance for the queue.
   */
  private async checkEvictionPolicy(): Promise<void> {
    try {
      const { maxmemoryPolicy } = await this.readMemoryInfo()
      if (maxmemoryPolicy.startsWith('allkeys-')) {
        this.logger(`Redis maxmemory-policy is "${maxmemoryPolicy}": under memory pressure Redis may evict queue, lock and log keys, losing or duplicating jobs. Use "noeviction" (ideally on a dedicated instance) with a memory alarm.`)
      } else if (maxmemoryPolicy.startsWith('volatile-')) {
        this.logger(`Redis maxmemory-policy is "${maxmemoryPolicy}": redisjm keys carry no TTL, so this policy cannot free them — Redis will refuse writes once full. Prefer "noeviction" with a memory alarm.`)
      }
    } catch (err) {
      this.logger('could not read INFO memory to check the eviction policy', toError(err))
    }
  }

  /**
   * Run after each timer maintenance pass: fires `memoryPressure` (with a `health()` snapshot) and logs
   * when `used_memory / maxmemory` crosses `memoryWarnRatio` upward; re-arms once it drops below. Never
   * throws.
   */
  private async checkMemoryPressure(): Promise<void> {
    const warnRatio = this.options.memoryWarnRatio
    if (warnRatio <= 0) return
    try {
      const memory = await this.readMemoryInfo()
      const { usedRatio } = memory
      if (usedRatio === null) return
      if (usedRatio < warnRatio) {
        this.memoryPressureArmed = true
        return
      }
      if (!this.memoryPressureArmed) return
      // Snapshot BEFORE disarming: if `health()` fails, the crossing is retried on the next tick instead
      // of being swallowed until memory drops below the ratio and climbs back.
      const snapshot = await this.collectHealth(memory, false)
      this.memoryPressureArmed = false
      this.logger(
        `Redis memory at ${(usedRatio * 100).toFixed(1)}% of maxmemory (warn ratio ${warnRatio}); writes will be refused at 100% under noeviction`,
      )
      await this.emit('memoryPressure', snapshot)
    } catch (err) {
      this.logger('memory pressure check failed', toError(err))
    }
  }

  /**
   * Queue depth (LLEN) of a single lane's queue list — the default lane when `lane` is omitted (or
   * `'default'`). Read-only introspection: any lane string resolves to its key (no lane-name
   * validation). NOTE: delayed/scheduled runs are NOT on a lane list until promoted, so they are not
   * counted here — use `stats().delayed` for those.
   *
   * @example
   * ```ts
   * const pending = await manager.queueSize('images')
   * ```
   */
  async queueSize(lane?: string): Promise<number> {
    return this.redis.llen(this.getQueueKey(lane))
  }

  /**
   * Enqueues a job run and reports what happened: `{ status, jobId }` with status
   * `'queued'` (written), `'deduped'` (the runId already holds a lock — queued/delayed/running —
   * nothing written), `'busy'` (the job already has `>= maxInFlight` runs locked) or `'full'` (its lane
   * is at its cap). Atomic: one server-side script takes the lock, writes the record and pushes it. `options.first` inserts at the FRONT of the lane queue (like
   * `queueFirst`); `options.delay` stages the run on the delayed set (the two cannot be combined).
   *
   * Every Redis failure throws `RedisJMEnqueueError` (with a classified `reason`, e.g. `'oom'` when
   * Redis is at `maxmemory` — then nothing at all was written) and fires the manager-level
   * `enqueueFailed` hook. Validation errors (bad lane / delay) throw a plain `TypeError`/`Error`.
   *
   * @example
   * ```ts
   * const { status } = await manager.enqueue(job, 'order-123', { orderId: '123' })
   * await manager.enqueue(job, 'urgent', { orderId: '9' }, { first: true })
   * ```
   */
  async enqueue<TInputs>(job: Job<TInputs, any>, runId: string, inputs: TInputs, options?: EnqueueOptions): Promise<EnqueueResult> {
    const [result] = await this.enqueueRun(job, [{ runId, inputs }], options?.first ? 'lpush' : 'rpush', options)
    return result
  }

  /**
   * Enqueues many runs of one job in ONE atomic script call and returns a result per entry, in entry
   * order — so batch producers don't serialize one round trip per run. Each entry is checked like a
   * single `enqueue` (dedupe, `maxInFlight`, lane cap — counted as the batch fills the lane). With
   * `first: true` the batch lands at the head of the lane in its given order.
   *
   * All-or-nothing on failure: a Redis failure (e.g. OOM — Redis refuses the whole script) throws one
   * `RedisJMEnqueueError` (its `jobId` is the first entry's) and NOTHING was written; an entry with
   * oversized inputs rejects the whole batch before any write. Keep batches to a sensible size
   * (hundreds to a few thousand): one script call blocks Redis for its duration.
   *
   * @example
   * ```ts
   * const results = await manager.enqueueMany(job, orders.map((o) => ({ runId: o.id, inputs: o })))
   * const skipped = results.filter((r) => r.status !== 'queued')
   * ```
   */
  async enqueueMany<TInputs>(
    job: Job<TInputs, any>,
    entries: Array<{ runId: string; inputs: TInputs }>,
    options?: EnqueueOptions,
  ): Promise<EnqueueResult[]> {
    return this.enqueueRun(job, entries, options?.first ? 'lpush' : 'rpush', options)
  }

  /**
   * Adds a job run to the end of the queue. Returns `true` if queued, `false` if it was not queued —
   * deduped by a held lock, `busy` (`maxInFlight`) or `full` (lane cap); use {@link enqueue} to tell
   * those apart.
   * Pass `options.delay` (ms) to stage the run on the delayed set instead of the live queue. Redis
   * failures throw `RedisJMEnqueueError` (see {@link enqueue}).
   *
   * @example
   * ```ts
   * const success = await manager.queue(job, 'order-123', { orderId: '123' })
   * await manager.queue(job, 'order-456', { orderId: '456' }, { delay: 5000 })
   * ```
   */
  async queue<TInputs>(job: Job<TInputs, any>, runId: string, inputs: TInputs, options?: QueueOptions): Promise<boolean> {
    return (await this.enqueueRun(job, [{ runId, inputs }], 'rpush', options))[0].status === 'queued'
  }

  /**
   * Adds a job run to the front of the queue (priority insert). Returns `true` if queued, `false` if
   * not queued (deduped, busy or full — see {@link enqueue}). A priority insert cannot be delayed — passing
   * `options.delay > 0` throws a TypeError. Redis failures throw `RedisJMEnqueueError`.
   *
   * @example
   * ```ts
   * await manager.queueFirst(job, 'urgent-order', { orderId: '456' })
   * ```
   */
  async queueFirst<TInputs>(job: Job<TInputs, any>, runId: string, inputs: TInputs, options?: QueueOptions): Promise<boolean> {
    return (await this.enqueueRun(job, [{ runId, inputs }], 'lpush', options))[0].status === 'queued'
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
    const { entries } = await this.scanHashBatch(this.getLogKey(), '0', Infinity)
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
   * Note: by default (`keepFinishedInterval: 60000`) a terminal record lingers ~60s, so a run that
   * just finished IS observable here (status `finished`/`error`/`stale`). Opting into
   * `keepFinishedInterval: 0` deletes the record the moment the job leaves `running`, and a finished
   * run then returns `undefined` — `undefined` means "no record", not "never ran".
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
    // is already gone, fall back to the default key (the other removals are group-wide).
    const record = await this.readRecord(jobId)
    // One atomic purge: record + lock together, plus its lane entry, any delayed-set entry (a
    // `delayed`/retry-scheduled run lives there), any `claiming` entry (popped, not yet claimed), and any
    // pending orphan-suspicion — a manually removed run must not leave a suspects entry that a later
    // maintenance pass would act on (e.g. re-SREM a re-added lock).
    await this.purge(jobId, 'force', { lane: this.getQueueKey(record?.lane), claiming: true, delayed: true, suspects: true })
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
    checkAbortGraceMs(job.getAbortGraceMs(), `job "${jobName}" (JobMetadata)`)
    if (this.jobsByName.has(jobName)) {
      throw new Error(`Job with name "${jobName}" is already registered`)
    }

    this.registeredJobs.add(job)
    this.jobsByName.set(jobName, job)
    this.ownLanes = undefined
    this.presenceBase = undefined
    // Discover the new job's old lanes on the next poll, not up to OLD_LANE_REFRESH_MS later.
    this.lastOldLaneRefresh = 0

    /**
     * The prologue every job-level hook shares: act only on events of executions THIS manager drives
     * (see `shouldHandle`), addressed by the run's jobId.
     */
    const own = <P extends JobEventPayload<TInputs>>(handler: (payload: P, jobId: string) => Promise<void>) =>
      async (payload: P): Promise<void> => {
        if (this.shouldHandle(payload)) await handler(payload, job.getJobId(payload.runId))
      }
    /**
     * Writes the record of `payload`'s execution only while that execution OWNS it — its executionId
     * still stamped on the record. That fences the zombie-run scenario: a stalled handler is staled by
     * maintenance (lock released), a producer re-enqueues the same runId (fresh record, NO executionId),
     * then the original handler finally reports. Its executionId no longer matches, so the write is
     * rejected — the zombie can't overwrite the successor's record, release its lock, or (under
     * keepFinishedInterval=0) delete it out from under a run that hasn't happened yet. With `resurrect`
     * the record must also be `running`, or `stale` but self-healable (see `acceptRunningOrStale`).
     */
    const updateOwned = (
      payload: JobEventPayload<TInputs>,
      mutate: (record: JobLogRecord) => Mutation,
      resurrect = false,
    ): Promise<LogUpdate> => this.updateLog(job.getJobId(payload.runId), (record) => {
      if (record.executionId !== payload.executionId) return false
      if (resurrect && !this.acceptRunningOrStale(record)) return false
      return mutate(record)
    })

    const onStart = own(async (payload: JobEventPayload<TInputs>, jobId) => {
      // `start` is the CLAIM: it only fires on a record that is still `queued`, flipping it to
      // `running` and stamping this execution's fencing token (the transition also takes the run out
      // of `claiming`, where the pop parked it). A rejected claim means the entry we popped no longer
      // owns its record — a successor that re-enqueued the same runId (fresh `queued` record) or a
      // concurrent claimant owns it now — so this execution is superseded.
      let claim: LogUpdate
      try {
        claim = await this.updateLog(jobId, (record) => {
          // Our OWN claim already landed: its reply was lost and the client re-sent it after a reconnect
          // (ioredis `autoResendUnfulfilledCommands`), so the re-run lost its compare-and-set. The
          // executionId is fresh per execution — nobody else can have stamped it. Not a supersession.
          if (record.status === 'running' && record.executionId === payload.executionId) return 'claimed'
          if (record.status !== 'queued') return false
          const now = Date.now()
          record.status = 'running'
          record.startedAt = now
          record.heartbeat = now
          record.executionId = payload.executionId
          record.attempt = nextAttempt(record)
          delete record.suspectedAt
        })
      } catch (err) {
        // The claim's read or write failed (Redis down / out of memory): the record is still `queued`
        // but its queue entry was already popped. Tag the error so the pop's start-failure recovery
        // pushes the entry back (or defers it) instead of failing a run that never started.
        const tagged = toTaggable(err)
        claimWriteFailures.add(tagged)
        throw tagged
      }
      if (claim.outcome === 'rejected' && claim.reason !== 'claimed') {
        throw new RunSupersededError(`run "${jobId}" was superseded before it could claim its record`)
      }
      // Stamp the attempt the claim wrote (right even when another instance ran the earlier attempts)
      // on the payload every later hook of this execution shares. 'missing' → no backing record
      // (direct `job.execute()` pattern): keep the caller's value.
      payload.attempt = claim.record?.attempt ?? payload.attempt
      await this.emit('start', payload as unknown as JobEventPayload)
    })

    const onFinish = own(async (payload: JobEventPayload<TInputs>, jobId) => {
      const now = Date.now()
      // The terminal write releases the lock and retires the record (history TTL, or deletion under
      // keepFinishedInterval=0) atomically — see `transition`. Trade-off: the fence is the executionId
      // alone (a stale-but-ours record is finished), so a re-sent copy of this write whose reply was lost
      // writes `finished` again — clearing its field TTL (maintenance still expires it by `finishedAt`).
      const { outcome } = await updateOwned(payload, (record) => {
        record.status = 'finished'
        record.finishedAt = now
      })
      // 'rejected' → the record's owner changed under us: touch nothing, and skip the manager-level
      // event too (it doesn't describe the record's current owner).
      if (outcome === 'rejected') return
      // 'missing' (record unqueued mid-run): release a lock nothing backs any more.
      if (outcome === 'missing') await this.purge(jobId, 'orphan')
      await this.emit('finish', payload as unknown as JobEventPayload)
    })

    const onError = own(async (payload: JobErrorEventPayload<TInputs>, jobId) => {
      const now = Date.now()
      const maxAttempts = job.getAttempts()
      // Same fencing as `onFinish`: a superseded execution's error must not clobber the successor's
      // record, schedule a phantom retry, or release its lock. A `stale`-but-ours record is accepted:
      // a retry off it re-takes the lock maintenance released (derived by the transition from
      // stale → delayed), so the backoff window is dedupe-protected like a retry off `running`.
      // Trade-off (executionId is the only fence): a re-sent copy of this write whose reply was lost
      // re-applies it — a second `retry` event, a `readyAt` past the delayed-set score, or (if the retry
      // was promoted meanwhile) a `queued` record moved back to `delayed` while its lane entry stays,
      // whose next pop is superseded (the record is not `queued`). Never a double execution.
      const { outcome, record } = await updateOwned(payload, (r) => {
        // The 1-based attempt that just failed (stamped by onStart's claim).
        const attempt = r.attempt ?? 1
        // Last error kept for observability in BOTH branches.
        r.error = payload.error.message
        if (attempt < maxAttempts) {
          // RETRY: stage the run back on the delayed set and KEEP the lock — the held lock is what
          // prevents a duplicate enqueue of the same runId while the backoff elapses. Backoff 0 still
          // routes through the delayed set (readyAt = now) so promotion has a single code path; the
          // next sweep picks it up within ~1s. A retry off a staled run clears the stale `finishedAt`
          // and `staleReason` (the run isn't terminal).
          r.status = 'delayed'
          r.readyAt = now + job.getBackoffMs(attempt)
          delete r.finishedAt
          delete r.staleReason
        } else {
          // FINAL failure: terminal error (lock released with the write, `error` hook fired below).
          r.status = 'error'
          r.finishedAt = now
        }
      })
      // Zombie fencing: the record's owner changed under us — touch nothing, fire nothing.
      if (outcome === 'rejected') return
      // A timed-out attempt is an ordinary failure (retry-or-final below); observers hear about the
      // timeout itself first.
      if (payload.error instanceof JobTimeoutError) {
        await this.emit('timeout', {
          ...(payload as unknown as JobErrorEventPayload),
          timeoutMs: payload.error.timeoutMs,
        })
      }
      if (outcome === 'written' && record?.status === 'delayed') {
        // The delayed-set entry landed atomically with the record. The run isn't finally failed, so
        // `retry` fires instead of the manager-level `error` hook.
        await this.emit('retry', {
          ...(payload as unknown as JobErrorEventPayload),
          nextAttemptAt: record.readyAt!,
        } as JobRetryEventPayload)
        return
      }
      // Final failure (or a 'missing' record — unqueued mid-run: release a lock nothing backs any more).
      if (outcome === 'missing') await this.purge(jobId, 'orphan')
      await this.emit('error', payload as unknown as JobErrorEventPayload)
    })

    const onHeartbeat = own(async (payload: JobEventPayload<TInputs>) => {
      // A pinned-but-alive run self-heals: a `stale` record that is still ours flips back to `running`
      // (and re-takes its lock). A record whose owner changed or that reached a terminal state is
      // rejected — a straggling heartbeat from a finished or superseded run must never keep it alive.
      const { outcome } = await updateOwned(payload, (record) => {
        record.heartbeat = Date.now()
      }, true)
      // Heartbeat-driven ownership-loss detection: the heartbeat is the ONLY periodic read the executor
      // already performs, so when its guarded write is not 'written' — 'rejected' (record now owned by a
      // successor, or terminal) or 'missing' (unqueued mid-run) — this execution has lost ownership of
      // its record. Abort the run's cooperative signal (detected within one heartbeatInterval, zero
      // extra Redis traffic) and SKIP the manager-level heartbeat event: it doesn't describe a live,
      // owned run. Abort is cooperative — the handler must observe the signal.
      if (outcome !== 'written') {
        payload.abort('run lost ownership of its record (superseded, terminal, or unqueued)')
        return
      }
      await this.emit('heartbeat', payload as unknown as JobEventPayload)
    })

    const onUpdate = own(async (payload: JobUpdateEventPayload<TInputs, TAttrs>) => {
      // Same self-heal + fencing as heartbeat: the running execution that owns the record may write
      // its progress/attrs, and a `stale`-but-ours record is resurrected so a pinned-then-recovered
      // handler's final setProgress/setAttrs land instead of being silently dropped. A superseded
      // run's late update still can't leak into the successor's record (executionId mismatch).
      const { outcome } = await updateOwned(payload, (record) => {
        if (payload.progress !== undefined) record.progress = payload.progress
        // Merge, not replace: successive setAttrs calls accumulate keys instead of clobbering.
        if (payload.attrs !== undefined) record.attrs = { ...record.attrs, ...payload.attrs }
      }, true)
      // A fenced (superseded / terminal / unqueued) write changed nothing, so it is not an `update`. No
      // abort here — the heartbeat owns ownership-loss detection.
      if (outcome !== 'written') return
      await this.emit('update', payload as unknown as JobUpdateEventPayload)
    })

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
    this.ownLanes = undefined
    this.presenceBase = undefined
    // The old-lane allow-lists name the job too: drop them and re-read on the next poll, or for up to
    // OLD_LANE_REFRESH_MS this instance would keep taking its entries off old lanes as unknown jobs
    // (burning their requeue budget, or failing them).
    this.oldLanes = []
    this.lastOldLaneRefresh = 0
  }

  /**
   * Pops the next job from this instance's subscribed lanes, matches it to a registered Job by name,
   * and executes it, resolving once that run has settled. Returns `true` if a job was popped, `false`
   * if the queues were empty — or if popping is briefly paused after a start failure (see the
   * `startFailed` hook), or once `stop()` has been called (until the next `start()`).
   *
   * Independent of the poll loop's slots: it always executes one pop (for drain-until-empty workers),
   * not counted against `concurrency` / `laneConcurrency`. It IS tracked: `stop()` waits for an
   * in-flight `popAndExecute()` to settle. Rejects when the pop itself fails (e.g. Redis out of memory
   * refuses the pop script — in which case nothing was popped).
   *
   * @example
   * ```ts
   * while (await manager.popAndExecute()) {}
   * ```
   */
  async popAndExecute(): Promise<boolean> {
    if (this.stopped) return false
    const run = (async () => {
      const popped = await this.popNext(await this.getPollPlan())
      if (!popped) return false
      // Preserve the public contract: resolve only after the popped run completes.
      await popped.run()
      return true
    })()
    const tracked = run.then(() => {}, () => {})
    this.manualRuns.add(tracked)
    void tracked.finally(() => this.manualRuns.delete(tracked))
    return run
  }

  /**
   * Promotes due delayed runs, pops ONE job from the lanes of `plan` (in order), and resolves the pop
   * into either `null` (nothing to do at idle pacing: empty queues, popping paused, or an unknown job
   * re-queued for a sibling) or `{ run, queueKey }`. The pop's Redis-touching cleanup (unknown-job drop,
   * missing/corrupt-record drop, non-jobId cleanup) happens inline here; those branches return the
   * pre-resolved `NOOP_THUNK` so the caller still counts them as "work found" for pacing. Only a real
   * execution defers into a thunk the poll loop can run concurrently without awaiting.
   */
  private async popNext(plan: PollPlan): Promise<null | { run: () => Promise<void>; queueKey: string }> {
    // Start-failure back-off (see `popsPausedUntil`).
    if (Date.now() < this.popsPausedUntil) return null

    // Promote any due delayed runs onto their lane queues before popping, so a run whose delay just
    // elapsed becomes poppable on this same pass. A failed promotion must not stop popping.
    try {
      await this.promoteDueDelayed()
    } catch (err) {
      this.logPopError('delayed-run promotion failed', err)
    }
    if (plan.length === 0) return null

    // One atomic script: pop from the first lane with an eligible entry AND park the id in `claiming`.
    // Under maxmemory Redis refuses the whole script, so a full Redis pops (and loses) nothing.
    const popped = await this.popFromLanes(plan)
    this.popOomLogged = false
    if (!popped) return null

    // From here on the entry is OFF its lane list (parked in `claiming`). Any Redis failure while
    // resolving it must not strand it: push it back to the head of its lane (see `recoverUnclaimed`).
    try {
      const run = await this.resolvePopped(popped.jobId, popped.key)
      return run ? { run, queueKey: popped.key } : null
    } catch (err) {
      await this.recoverUnclaimed(popped.jobId, err)
      return { run: NOOP_THUNK, queueKey: popped.key }
    }
  }

  /**
   * Logs a pop-path Redis failure; an OOM refusal is logged once per episode (every poll would hit it).
   */
  private logPopError(message: string, err: unknown): void {
    if (classifyRedisError(err) === 'oom') {
      if (this.popOomLogged) return
      this.popOomLogged = true
      this.logger(`${message}: Redis is out of memory — nothing is popped until memory frees`, toError(err))
      return
    }
    this.logger(message, toError(err))
  }

  /**
   * Resolves a popped `jobId` (from queue list `queueKey`) into an execution thunk, `null` (unknown job
   * re-queued for a sibling), or `NOOP_THUNK` (cleanup done inline). The drop branches purge the run's
   * leftovers — `claiming` entry included — only while no valid record backs it (see `purge`); a stray
   * duplicate entry leaves everything to the record's owner. See {@link popNext}.
   */
  private async resolvePopped(jobId: string, queueKey: string): Promise<null | (() => Promise<void>)> {
    if (!jobId.includes('#')) {
      // Not a jobId at all: nothing can ever own it.
      await this.purge(jobId, 'force', { claiming: true })
      return NOOP_THUNK
    }

    const { jobName, runId } = splitJobId(jobId)
    // 0.2+ never enqueues the maintenance job; an entry of it means a pre-0.2 instance is alive here.
    if (jobName === MAINTENANCE_JOB_NAME) this.noteLegacyInstance()

    const job = this.jobsByName.get(jobName)
    if (!job) {
      // This instance has no handler for the popped name. In a rolling deploy / blue-green
      // topology a sibling instance may have it, so re-queue (keeping the lock held) up to
      // `unknownJobRequeueLimit` times before giving up and recording the error. A successful
      // re-queue returns `null` (treated as "no work executed") so the poll loop applies its
      // idle interval rather than immediately re-popping — spacing retries by `interval` gives
      // a sibling that *does* have the handler real wall-clock time to claim it.
      const requeue = await this.requeueUnknownJob(jobId)
      if (requeue === 'requeued') return null
      if (requeue === 'stray') {
        // The record is not `queued`: this entry is a stray duplicate of a run that is already
        // claimed (running), scheduled or finished elsewhere — its owner holds the record and lock.
        // Touching the record here (re-queueing or failing it) would hand a RUNNING run to a second
        // worker (double execution) or clobber it. Drop the entry only; its `claiming` mark is left
        // for maintenance (removing it here could erase a concurrent legitimate pop's mark).
        this.logger(`job "${jobId}" popped as a stray entry (its record is not queued); skipping`)
        return NOOP_THUNK
      }

      // Same guard as the requeue: only a still-`queued` record is this pop's to fail.
      const { outcome } = await this.updateLog(jobId, failQueuedRun('Job name is unknown', Date.now()))
      if (outcome === 'rejected') return NOOP_THUNK
      this.logger(`job "${jobId}" has no registered handler on this instance; dropping`)
      if (outcome === 'missing') await this.purge(jobId, 'orphan', { claiming: true })
      return NOOP_THUNK
    }

    const logJson = await this.redis.hget(this.getLogKey(), jobId)
    const logRecord = logJson ? this.parseRecord(logJson, jobId) : null
    if (!logJson || !logRecord) {
      // Popped a queue entry with no (parseable) backing record — desynced/cleaned state, or a
      // corrupt/foreign record. Drop it with its lock (garbage: retention doesn't apply, and keeping it
      // under `keepFinishedInterval > 0` would only hoard it), surfacing a missing record rather than
      // dropping the run completely silently.
      if (!logJson) this.logger(`job "${jobId}" popped with no log record; dropping`)
      await this.purge(jobId, 'orphan', { claiming: true })
      return NOOP_THUNK
    }
    // The claim's compare-and-set starts from this read (see `recordJson`).
    this.recordJson.set(jobId, logJson)

    // Defer the execution into a thunk so the poll loop can run it WITHOUT awaiting (concurrency): the
    // thunk owns a per-execution AbortController (registered so `stop({ abort: true })` can signal it,
    // removed when the run settles) and reproduces the serial path's terminal error handling exactly.
    // Capture only `inputs` and the predicted attempt, not the whole parsed record: the thunk lives in
    // `inFlightRuns` for the run's lifetime, so closing over just those lets the record wrapper be
    // collected sooner. The prediction is what the claim should write (the claim stamps the real one);
    // only job-level `start` hooks registered before `registerJob` (they run pre-claim) ever see it.
    const { inputs } = logRecord
    const attempt = nextAttempt(logRecord)
    const timeoutMs = job.getTimeoutMs() ?? this.options.jobTimeout
    // The job's own value wins, `false` included (an explicit opt-out of the manager default).
    const abortGraceMs = job.getAbortGraceMs() ?? this.options.abortGraceMs
    return async () => {
      const controller = new AbortController()
      this.abortControllers.add(controller)
      try {
        await job.execute(inputs, {
          targetGroup: this.targetGroup,
          heartbeatInterval: this.options.heartbeatInterval,
          runId,
          // Resolved execution timeout (job value wins; 0 = none). On expiry execute() settles with a
          // JobTimeoutError even if the handler never does, so this slot frees.
          timeoutMs,
          // Resolved abort grace: after `ctx.signal` aborts (not by the timeout), a handler still pending
          // this long is abandoned and execute() settles with a JobAbortedError (default: wait for it).
          abortGraceMs,
          attempt,
          // Identify this manager as the driver (so only its hooks act) and route infra errors here.
          manager: this,
          logger: this.logger,
          // Cooperative abort: the heartbeat hook fires this on ownership loss; stop({abort:true}) fires
          // it on shutdown. WHY (abort meets retries): if an aborted attempt still throws, the error
          // path may schedule a retry — and that's CORRECT. If the abort came from staleness/supersession
          // the retry's writes are fenced out by the executionId claim/mutator guards (it never lands);
          // if it came from local shutdown the retry legitimately belongs to another instance. No
          // special-casing is needed here.
          signal: controller.signal,
        })
      } catch (err) {
        if (err instanceof RunSupersededError) {
          // The entry we popped no longer owns its record — a successor/concurrent claimant does, and
          // is responsible for the lock and log. Skip without touching either (a re-push would
          // duplicate the successor's queued entry; an srem would free the successor's lock).
          this.logger(`job "${jobId}" superseded; skipping`)
          return
        }
        // A failure in the START phase (claim write, or a job-level `start` hook) happened after the
        // pop removed the entry and before the run was established — recover it explicitly.
        const startPayload = getStartPhasePayload(err)
        if (startPayload) {
          await this.recoverStartPhaseFailure(jobId, startPayload, err)
          return
        }
        // The job's `error` event already recorded the failure in Redis and re-broadcast it.
        // Surface it through the logger too, so a thrown handler is never fully silent when no
        // `error` hook is wired (and to catch infra/hook failures, which are NOT "already handled").
        const error = toError(err)
        this.logger(`job "${jobId}" failed: ${error.message}`, error)
      } finally {
        this.abortControllers.delete(controller)
        // Normally gone with the terminal write already; this covers the paths that never write (a
        // superseded or unrecoverable start), so the cache never outlives the run.
        this.recordJson.delete(jobId)
      }
    }
  }

  /**
   * The ordered list of lanes one pop considers (see `POP_SCRIPT`): this instance's own lanes (spec
   * `'*'`, see {@link getSubscribedQueueKeys}) followed by OLD lanes its jobs still have entries on —
   * lanes a job was enqueued on before its lane changed across a deploy — each with the allow-list of
   * this instance's job names that may be taken from it. The old-lane set comes from the per-job lane
   * sets in Redis, refreshed at most every {@link OLD_LANE_REFRESH_MS}.
   */
  private async getPollPlan(): Promise<PollPlan> {
    const own = this.getSubscribedQueueKeys()
    const ownKeys = this.ownLanes!.keys
    await this.refreshOldLanes(ownKeys)
    return [
      ...own.map((key) => ({ key, spec: '*' })),
      ...this.oldLanes.filter(({ key }) => !ownKeys.has(key)),
    ]
  }

  /**
   * Refreshes `oldLanes`: for every registered job, the lane keys in its Redis lane set that are not one
   * of this instance's own lanes, mapped to an allow-list of the registered job names found there. Rate
   * limited; a failed read keeps the previous plan (and is logged).
   */
  private async refreshOldLanes(ownKeys: Set<string>): Promise<void> {
    const now = Date.now()
    if (now - this.lastOldLaneRefresh < OLD_LANE_REFRESH_MS) return
    this.lastOldLaneRefresh = now
    const jobs = [...this.jobsByName.keys()].filter((name) => name !== MAINTENANCE_JOB_NAME)
    if (jobs.length === 0) {
      this.oldLanes = []
      return
    }
    try {
      const allow = new Map<string, Set<string>>()
      ;(await this.readJobLaneSets(jobs)).forEach((keys, i) => {
        for (const key of keys) {
          if (ownKeys.has(key)) continue
          if (!allow.has(key)) allow.set(key, new Set())
          allow.get(key)!.add(jobs[i])
        }
      })
      this.oldLanes = [...allow].map(([key, names]) => ({ key, spec: `#${[...names].join('#')}#` }))
    } catch (err) {
      this.logPopError('could not refresh old lanes', err)
    }
  }

  /**
   * This instance's subscribed queue keys for one poll (§5.6): the reserved `__maintenance` lane first,
   * then the DISTINCT work-lane queue keys of the registered jobs, ordered by strategy.
   *
   * Deduplication is on the resolved queue key (not the lane name) so a no-lane job and an explicit
   * `lane: 'default'` job don't double-weight the legacy key. Under `roundRobin` (default) the work
   * keys rotate by a per-manager cursor advanced once per poll (anti-starvation); under `priority`
   * the `lanePriority` order wins with unlisted work lanes trailing in registration order. The base
   * order is cached until a job is (un)registered (`ownLanes`); only the rotation is per poll.
   */
  private getSubscribedQueueKeys(): string[] {
    const maintenanceKey = this.getQueueKey(MAINTENANCE_LANE)
    this.ownLanes ??= this.buildOwnLanes(maintenanceKey)
    const { workKeys } = this.ownLanes
    const n = workKeys.length
    if (this.options.laneStrategy === 'priority' || n === 0) return [maintenanceKey, ...workKeys]
    // roundRobin: rotate the work-key list by the cursor, then advance it once per poll so no lane is
    // starved by a saturated higher-order lane.
    const offset = this.pollCursor++ % n
    return [maintenanceKey, ...workKeys.slice(offset), ...workKeys.slice(0, offset)]
  }

  /** Builds `ownLanes` (see there) from the registered jobs. */
  private buildOwnLanes(maintenanceKey: string): { workKeys: string[]; keys: Set<string> } {
    // Distinct work-lane keys in registration order, excluding the reserved maintenance key.
    const keys = new Set<string>([maintenanceKey])
    const workKeys: string[] = []
    for (const job of this.registeredJobs) {
      const key = this.getQueueKey(job.getLane())
      if (keys.has(key)) continue
      keys.add(key)
      workKeys.push(key)
    }
    if (this.options.laneStrategy !== 'priority') return { workKeys, keys }
    // Listed lanes first (in `lanePriority` order), then the remaining work keys in registration
    // order. `workKeys` is already distinct, so a single set consumed via `Set.delete` handles both the
    // "am I subscribed?" test and the "don't emit twice" guard in one step.
    const remaining = new Set(workKeys)
    const ordered: string[] = []
    for (const lane of this.options.lanePriority) {
      const key = this.getQueueKey(lane)
      if (remaining.delete(key)) ordered.push(key)
    }
    for (const key of workKeys) {
      if (remaining.delete(key)) ordered.push(key)
    }
    return { workKeys: ordered, keys }
  }

  /**
   * Runs the pop script (see `POP_SCRIPT`) over the plan in order: the popped `{ key, jobId }` (also
   * parked in `claiming`), or `null` when no lane has an eligible entry.
   */
  private async popFromLanes(plan: PollPlan): Promise<{ key: string; jobId: string } | null> {
    const res = await runScript(
      this.redis,
      POP_SCRIPT,
      [this.getClaimingKey(), ...plan.map((p) => p.key)],
      [Date.now(), OLD_LANE_SCAN_LIMIT, ...plan.map((p) => p.spec)],
    )
    if (!Array.isArray(res) || res.length < 2) return null
    return { key: String(res[0]), jobId: String(res[1]) }
  }

  /**
   * Start-failure recovery for a popped entry whose claim never landed (record still `queued`, id
   * parked in `claiming`): put it back at the HEAD of its lane (see `requeueUnclaimed`). If that fails
   * too (e.g. Redis at maxmemory refuses the script), the run is NOT dropped: it stays parked in
   * `claiming` (record `queued`, lock held) and maintenance puts it back on its lane once it has sat
   * there past the stale threshold (action `'deferred'`). A record that is no longer `queued` (the claim
   * did land despite the error, or the run moved on) is not this pop's to recover: nothing is touched
   * and no `startFailed` fires. Trade-off: a claim that landed although its call threw leaves the record
   * `running` under an execution that never started; it is not retried — maintenance stales it once its
   * heartbeat lapses (a lost reply that the client RE-SENDS is handled: the claim is idempotent). Either way popping pauses briefly — for a whole stale-threshold window
   * after an OOM or a deferral (see `popsPausedUntil`). Never throws.
   */
  private async recoverUnclaimed(jobId: string, err: unknown): Promise<void> {
    const error = toError(err)
    const reason = classifyRedisError(err)
    let action: 'requeued' | 'deferred' | undefined
    try {
      if (await this.requeueUnclaimed(jobId)) action = 'requeued'
    } catch (requeueErr) {
      action = 'deferred'
      this.logger(
        `requeue of "${jobId}" failed (${classifyRedisError(requeueErr)}); it stays in the claiming set and maintenance will requeue it`,
        toError(requeueErr),
      )
    }
    // Back off before popping again. The entry may be right back at the head of the lane, and a
    // persistent failure would otherwise spin pop → fail → requeue at full speed; under OOM every pop
    // is refused anyway, so back off for a whole stale-threshold window.
    const severe = reason === 'oom' || action === 'deferred'
    this.pausePops(severe ? this.getStaleThreshold() : Math.min(1000, this.getStaleThreshold()))
    if (!action) {
      this.logger(`job "${jobId}" failed to start (${reason}) and no longer owns its record; skipping`, error)
      return
    }
    this.logger(`job "${jobId}" failed to start (${reason}); ${action}`, error)
    await this.emitStartFailed(jobId, reason, action, error)
  }

  /**
   * Puts a popped-but-never-claimed run back at the HEAD of its lane (it was popped first), atomically
   * with taking it out of `claiming` — only while its record is still `queued` and its `claiming` entry
   * still parked, so an overlapping requeue (start-failure recovery here, maintenance on another
   * instance) is a no-op instead of a duplicate entry. Shared by start-failure recovery and maintenance's
   * claiming stage. Resolves whether it requeued; throws on a Redis error (e.g. OOM refuses the script).
   */
  private async requeueUnclaimed(jobId: string, seed?: string): Promise<boolean> {
    const { outcome } = await this.updateLog(jobId, (record) => {
      if (record.status !== 'queued') return false
      record.enqueuedAt = Date.now()
      return { push: 'L', takeFromClaiming: true }
    }, seed)
    return outcome === 'written'
  }

  /**
   * Start-phase failure of a run that reached `job.execute()` (see the thunk in `resolvePopped`):
   * - the claim write itself failed → the claim never landed → {@link recoverUnclaimed};
   * - the claim landed (record `running` under this executionId) and a later start-phase step threw
   *   (a job-level `start` hook) → route it through the run's NORMAL failure path: dispatch the job's
   *   `error` hook, so the manager applies the fenced retry-or-final logic (`attempts`/`backoff`);
   * - a job-level `start` hook registered BEFORE the manager's claim threw → the record is still
   *   `queued` (never claimed): fail it terminally (`error`, lock released). Not requeued — a
   *   deterministic hook failure would otherwise spin pop→requeue forever.
   * Never throws.
   */
  private async recoverStartPhaseFailure(jobId: string, payload: JobEventPayload<any>, err: unknown): Promise<void> {
    // A primitive is never a key (WeakSet.has answers `false` for it).
    if (claimWriteFailures.has(err as object)) {
      await this.recoverUnclaimed(jobId, err)
      return
    }
    const error = toError(err)
    const reason = classifyRedisError(err)
    // The attempt this execution's claim wrote; stays `undefined` when the run was never claimed.
    let attempt: number | undefined
    try {
      const record = await this.readRecord(jobId)
      if (record?.executionId === payload.executionId) {
        // Claimed by this execution (whose payload already carries the claimed attempt): the normal
        // (fenced) failure path decides retry vs. final.
        attempt = payload.attempt
        try {
          await payload.job.callHook('error', { ...payload, error })
        } catch (hookErr) {
          this.logger('error hook failed', toError(hookErr))
        }
      } else if (record?.status === 'queued') {
        // Never claimed by this execution (a pre-claim job-level start hook threw): terminal error,
        // lock released. NOTE `executionId` is not checked: a retry attempt's record is `queued` but
        // still carries the PREVIOUS attempt's token, and treating that as "someone else's" left the
        // run parked in `claiming` → requeued by maintenance → the hook threw again, forever.
        await this.updateLog(jobId, failQueuedRun(error.message, Date.now()))
      } else {
        // Someone else owns the record now (or it is gone): nothing of ours to recover.
        this.logger(`job "${jobId}" failed to start and no longer owns its record; skipping`, error)
        return
      }
    } catch (recoverErr) {
      this.logger(`job "${jobId}" failed to start; recovery failed (maintenance will reclaim it)`, toError(recoverErr))
      return
    }
    this.logger(`job "${jobId}" failed to start (${reason}); failed`, error)
    await this.emitStartFailed(jobId, reason, 'failed', error, attempt)
  }

  /** Fires the `startFailed` observers for `jobId`. */
  private async emitStartFailed(
    jobId: string,
    reason: RedisErrorReason,
    action: StartFailedEventPayload['action'],
    error: Error,
    attempt?: number,
  ): Promise<void> {
    const { jobName, runId } = splitJobId(jobId)
    await this.emit('startFailed', { jobId, jobName, runId, reason, action, error, attempt })
  }

  /** Pauses popping for `ms` (see `popsPausedUntil`); never shortens an already longer pause. */
  private pausePops(ms: number): void {
    this.popsPausedUntil = Math.max(this.popsPausedUntil, Date.now() + ms)
  }

  /**
   * Promotes delayed runs whose `readyAt` has elapsed onto their lane queues. Each promotion is ONE
   * atomic transition (see `transition`): take the id off the delayed set (the claim — a racing
   * instance that already took it makes this a no-op), flip the record `delayed` → `queued`, and RPUSH
   * it onto its persisted lane. There is no crash window in which a promoted run is on neither
   * structure. A due entry whose record is missing (or garbage) is purged with its lock.
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
    // Bounded batch (8 ids/pass, ~2 round trips each) keeps this step — awaited on the pop hot path —
    // cheap; anything still due is picked up on the next sweep. Ids are independent, so promote them
    // concurrently.
    // With scores: a stray entry is dropped only while its score is still the one read (see below).
    const due = scorePairs(await this.redis.zrangebyscore(delayedKey, '-inf', now, 'WITHSCORES', 'LIMIT', 0, 8))
    await Promise.all(due.map(async ([jobId, score]) => {
      const { outcome, reason } = await this.updateLog(jobId, (record) => {
        if (record.status !== 'delayed') return 'not-delayed'
        record.status = 'queued'
        delete record.readyAt
        record.enqueuedAt = Date.now()
        return { push: 'R' }
      })
      if (outcome === 'missing') {
        // A delayed entry without a record is garbage: drop it and release the lock so the runId isn't
        // blocked forever.
        this.logger(`delayed job "${jobId}" had no promotable record; releasing lock`)
        await this.purge(jobId, 'orphan', { delayed: true })
      } else if (reason === 'not-delayed') {
        // A stray delayed entry for a record that moved on (owns its own lock state): drop just the entry
        // — and only while its score is the one read. A blind ZREM could erase the entry of a retry the
        // run scheduled meanwhile, and the orphan check would then stale the run instead of retrying it.
        await this.deleteIfUnchanged(delayedKey, 'z', [[jobId, score]])
      }
      // else: promoted, or a racing instance promoted it first (precondition failed) — nothing to do.
    }))
  }

  /**
   * Re-queues a job whose name is not registered on this instance, keeping its lock held so a
   * concurrent enqueue of the same runId can't duplicate it. Returns `'requeued'`; `'stray'` when the
   * record is not `queued` (the popped entry is a stray duplicate — leave the record alone); or
   * `'exhausted'` when the budget is used up / disabled or the record is gone, and the caller should
   * record an error.
   */
  private async requeueUnknownJob(jobId: string): Promise<'requeued' | 'stray' | 'exhausted'> {
    const limit = this.options.unknownJobRequeueLimit
    const { outcome, reason } = await this.updateLog(jobId, (record) => {
      // Only a `queued` record is this pop's run. Flipping a `running` record back to `queued` (what an
      // unguarded requeue of a stray entry did) let another worker claim it while it was running.
      if (record.status !== 'queued') return 'stray'
      const count = record.requeueCount ?? 0
      if (limit <= 0 || count >= limit) return false
      record.requeueCount = count + 1
      record.enqueuedAt = Date.now()
      // Back of the queue (not the front) so this doesn't starve handleable work; the lock stays held.
      // RPUSH to the record's own lane so a laned unknown job stays on its lane for a same-lane sibling.
      return { push: 'R' }
    })
    if (outcome === 'written') return 'requeued'
    return reason === 'stray' ? 'stray' : 'exhausted'
  }

  /**
   * Starts a polling loop that calls `popAndExecute()`. Polls immediately after a job executes;
   * waits `interval` ms when the queue is empty.
   *
   * Unless `maintenanceInterval` is `0`, also runs maintenance on this manager's own timer — once
   * immediately (so locks orphaned by a crash are reclaimed soon after restart) and then every
   * `maintenanceInterval` ms — via {@link runMaintenance}. The timer is independent of the poll loop
   * and the concurrency slots (maintenance still runs when every slot is held by a hung handler or the
   * queue is backed up), passes never overlap on one instance, and the Redis lock inside
   * `runMaintenance` lets about one pass per interval run across all instances.
   *
   * The built-in maintenance JOB is still registered (and its reserved `__maintenance` lane still
   * polled) so maintenance entries enqueued by older instances during a rolling deploy are consumed;
   * it now just calls `runMaintenance()`.
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
    this.stopped = false
    const generation = ++this.pollGeneration
    const current = () => this.polling && generation === this.pollGeneration
    // One-time eviction-policy sanity check (INFO memory — works on managed Redis that blocks CONFIG).
    void this.checkEvictionPolicy()

    if (this.options.maintenanceInterval > 0) {
      // Rolling-deploy compatibility: older instances still ENQUEUE the maintenance job, so keep its
      // handler registered here (it runs the same lock-guarded pass as the timer).
      if (!this.jobsByName.has(MAINTENANCE_JOB_NAME)) createMaintenanceJob(this)
      // Maintenance lives OFF the queue: a timer of its own, so a full set of busy/hung concurrency
      // slots or a deep backlog can never starve it (it is what reclaims those slots' records).
      this.maintenanceTimer = setInterval(() => this.maintenanceTick(), this.options.maintenanceInterval)
      this.maintenanceTick()
    }

    if (this.options.presence && !(this.options.heartbeatInterval > 0)) {
      this.logger('fleet presence is off for this instance: it needs heartbeatInterval > 0 (set presence: false to silence this)')
    }
    if (this.options.presence && this.options.heartbeatInterval > 0) {
      // Fleet presence (see `fleet()`): a lease refreshed on its own timer — outside the poll loop and the
      // concurrency slots — so a full set of busy slots never lapses the instance.
      this.startedAt = Date.now()
      this.presenceBase = undefined
      this.presenceTimer = setInterval(() => this.schedulePresenceRefresh(), this.options.heartbeatInterval)
      this.schedulePresenceRefresh()
    }

    const concurrency = this.options.concurrency
    const laneConcurrency = this.laneConcurrencyByKey
    // Schedules the next poll. `delay > 0` is the idle wait — the only wait `wake()` may cut short.
    const schedule = (delay: number) => {
      if (!current()) return
      if (this.wakePending) {
        this.wakePending = false
        delay = 0
      }
      this.idleWaiting = delay > 0
      // Track every invocation in `currentPoll` (assigned synchronously inside the timer callback) so
      // `stop()` can await a poll caught mid-pop before it drains `inFlightRuns` (see `currentPoll`).
      this.pollTimer = setTimeout(() => {
        this.idleWaiting = false
        this.currentPoll = poll()
      }, delay)
    }
    // Waits for any in-flight run to settle (a slot frees), then re-polls; with nothing in flight there
    // is nothing to wait for, so fall back to the idle interval.
    const waitForSlot = async () => {
      if (this.inFlightRuns.size === 0) return schedule(interval)
      // In-flight runs swallow their own errors, so the race never rejects (belt-and-suspenders catch).
      await Promise.race(this.inFlightRuns).catch(() => {})
      schedule(0)
    }
    const poll = async () => {
      if (!current()) return

      // All execution slots busy: wait for one run to free a slot. Do NOT pop while full — that would
      // exceed the concurrency cap.
      if (this.inFlightRuns.size >= concurrency) return waitForSlot()

      // Per-lane caps: only lanes below their `laneConcurrency` cap are offered to the pop.
      let popped: Awaited<ReturnType<RedisJM['popNext']>> = null
      try {
        const plan = (await this.getPollPlan()).filter(({ key }) => {
          const cap = laneConcurrency.get(key)
          return cap === undefined || (this.laneInFlight.get(key) ?? 0) < cap
        })
        if (plan.length === 0) return waitForSlot()
        popped = await this.popNext(plan)
      } catch (err) {
        // Keep the loop alive, but surface the failure instead of swallowing it silently.
        this.logPopError('poll loop error', err)
      }

      if (popped) {
        // Dispatch WITHOUT awaiting so the loop can pop the next job concurrently. The thunk already
        // swallows RunSupersededError and handler errors internally (identical to the serial path); the
        // outer `.catch` guards against an unexpected infra fault becoming an unhandled rejection. Track
        // the run so `stop()` can drain it and `concurrency` / `laneConcurrency` are enforced.
        const lane = popped.run === NOOP_THUNK ? undefined : popped.queueKey
        if (lane !== undefined) this.laneInFlight.set(lane, (this.laneInFlight.get(lane) ?? 0) + 1)
        const run = popped.run().catch((err) => {
          this.logger('in-flight run error', toError(err))
        })
        this.inFlightRuns.add(run)
        void run.finally(() => {
          this.inFlightRuns.delete(run)
          if (lane === undefined) return
          this.laneInFlight.set(lane, (this.laneInFlight.get(lane) ?? 1) - 1)
          // A capped lane's slot just freed: if the loop is idling (it skipped this lane at its cap and
          // found nothing elsewhere), re-poll now instead of after a full `interval`.
          if (laneConcurrency.has(lane)) this.wake()
        })
      }

      // Pacing: work found → re-poll immediately; idle (empty queues / requeued unknown) → `interval`.
      schedule(popped ? 0 : interval)
    }
    this.schedulePoll = schedule
    this.currentPoll = poll()
  }

  /**
   * Cuts the poll loop's idle wait short so it polls NOW — e.g. from a pub/sub "doorbell" that a
   * producer rings after enqueueing. Respects slot accounting (`concurrency` / `laneConcurrency`), so
   * it is the safe replacement for calling `popAndExecute()` ad hoc. No-op when the loop is not running
   * or every slot is busy (the loop re-polls as soon as a slot frees anyway); a wake that arrives while
   * a poll is in flight makes the next poll immediate.
   *
   * @example
   * ```ts
   * subscriber.on('message', () => manager.wake())
   * ```
   */
  wake(): void {
    if (!this.polling || !this.schedulePoll) return
    if (this.inFlightRuns.size >= this.options.concurrency) return
    if (this.idleWaiting) {
      if (this.pollTimer) clearTimeout(this.pollTimer)
      this.schedulePoll(0)
    } else {
      this.wakePending = true
    }
  }

  /**
   * Stops the polling loop and returns a promise that resolves once all in-flight runs have settled,
   * so callers (e.g. a SIGTERM handler) can await a drain before exiting.
   *
   * Two modes: the default is a GRACEFUL drain — new work stops being popped and `stop()` awaits the
   * runs already in flight to finish on their own. `stop({ abort: true })` is a FAST abort-and-drain —
   * it additionally aborts every in-flight run's `ctx.signal` (reason `'manager stopped'`) so
   * cooperative handlers can bail out of wasted work early; `stop()` still awaits them to settle
   * (abort is cooperative — nothing forcibly kills a handler). A handler that ignores its signal is only
   * awaited until its execution timeout (`jobTimeout` / `JobMetadata.timeoutMs`) when one is set — or,
   * with `abortGraceMs`, until that many ms after the abort: then it is abandoned, its attempt fails with
   * a `JobAbortedError` (consuming an attempt: with the default `attempts: 1` the run ends `error`) and
   * `stop()` resolves. This instance also leaves the fleet registry (see {@link fleet}) first thing.
   * The maintenance timer and `every()` timers are cleared too, an in-flight maintenance pass is
   * awaited, in-flight `popAndExecute()` calls are drained, and `popAndExecute()` refuses to pop
   * (returns `false`) until the next `start()`.
   *
   * @example
   * ```ts
   * process.on('SIGTERM', async () => { await manager.stop() })            // graceful
   * process.on('SIGINT',  async () => { await manager.stop({ abort: true }) }) // fast
   * ```
   */
  stop(options?: StopOptions): Promise<void> {
    this.polling = false
    this.stopped = true
    this.schedulePoll = undefined
    this.idleWaiting = false
    this.wakePending = false
    if (this.pollTimer) {
      clearTimeout(this.pollTimer)
      this.pollTimer = undefined
    }
    for (const timer of this.everyTimers) clearInterval(timer)
    this.everyTimers.clear()
    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer)
      this.maintenanceTimer = undefined
    }
    // Leave the fleet registry right away (this instance stops popping now); awaited by the drain below.
    if (this.presenceTimer) {
      clearInterval(this.presenceTimer)
      this.presenceTimer = undefined
      void this.queuePresence(() => this.deregisterPresence())
    }
    const abortAll = () => {
      for (const controller of this.abortControllers) controller.abort('manager stopped')
    }
    if (options?.abort) {
      // Fast shutdown: signal every in-flight run that it should stop wasting resources. Cooperative —
      // the handler must observe `ctx.signal`; we still await settlement below. NOTE: a run dispatched
      // by the still-in-flight final poll registers its controller AFTER this loop and would miss the
      // signal — the second (idempotent) abortAll after the currentPoll await below closes that gap.
      abortAll()
    }
    const drain = async () => {
      await this.presenceTail
      // A maintenance pass in flight finishes on its own (it is short and bounded by the log size);
      // awaiting it keeps `stop()`'s promise meaning "nothing of this manager is touching Redis".
      await Promise.resolve(this.maintenancePass)
      // Await the in-flight poll invocation FIRST: a poll caught mid-pop has already removed the queue
      // entry from Redis, so the run it dispatches must be included in the drain — snapshotting
      // `inFlightRuns` before that dispatch would let `stop()` resolve while the popped job still
      // executes. (The poll cannot be skipped either: dropping the popped entry would strand the run
      // until maintenance stales it.) `poll` never rejects (its awaits are caught) — the guard is
      // belt-and-suspenders so `stop()` keeps its no-reject promise.
      await Promise.resolve(this.currentPoll).then(() => {}, () => {})
      // Re-abort so a run the final poll dispatched (controller registered after the early abortAll)
      // also gets the signal; aborting an already-aborted controller is a no-op.
      if (options?.abort) abortAll()
      // Await settlement of ALL in-flight runs. Each run's promise already swallows its own errors (the
      // poll loop logged them), so `allSettled` just waits without surfacing — `stop()` always resolves.
      // `popAndExecute()` calls in flight (their pop and their run) are drained the same way.
      await Promise.allSettled([...this.inFlightRuns, ...this.manualRuns])
    }
    return drain()
  }

  /**
   * One timer tick: start a lock-guarded maintenance pass unless this instance already has one in
   * flight (passes never overlap on one instance), then check memory pressure. Failures are logged,
   * never thrown.
   */
  private maintenanceTick(): void {
    if (this.maintenancePass) return
    const pass = this.runMaintenance().then(
      () => this.checkMemoryPressure(),
      (err) => this.logger('maintenance pass failed', toError(err)),
    )
    this.maintenancePass = pass
    void pass.finally(() => {
      if (this.maintenancePass === pass) this.maintenancePass = undefined
    })
  }

  /**
   * Runs one maintenance pass if this instance wins the group's maintenance lock — what the
   * `start()` timer calls every `maintenanceInterval`.
   *
   * Lock: `SET redisjm:<group>:maintenance-lock <token> NX PX <ttl>`, deliberately NOT released after
   * the pass: its expiry spaces passes ~one interval apart across the whole fleet, however many
   * instances tick. (`ttl` is `maintenanceInterval` minus a small margin so an instance's own next tick
   * is not blocked by its own previous lock; `heartbeatInterval * roundsToStale` when auto-maintenance
   * is disabled.)
   *
   * - lock acquired → full pass ({@link performMaintenance}), result `mode: 'full'`;
   * - lock held by another instance → `null`;
   * - the lock SET refused because Redis is OUT OF MEMORY → an EMERGENCY pass instead: no lock and no
   *   writes, only deletions (expired terminal records, unparseable garbage records and their locks),
   *   which Redis accepts at maxmemory. Deletions are idempotent, so concurrent emergency passes on
   *   several instances are safe. Bounded like a full pass (`maxRecordsPerPass`), resuming from a
   *   per-instance cursor. Result `mode: 'emergency'`, `staleCount: 0`.
   * - any other lock error (connection, timeout, read-only replica…) → logged, `null`.
   *
   * @example
   * ```ts
   * const result = await manager.runMaintenance() // null when another instance holds the lock
   * ```
   */
  async runMaintenance(): Promise<MaintenanceResult | null> {
    const startedAt = Date.now()
    const interval = this.options.maintenanceInterval
    const ttl = interval > 0
      // Margin: the lock is set a few ms AFTER this instance's tick fires, so a TTL of exactly one
      // interval would still be held at its next tick and halve a lone instance's cadence.
      ? Math.max(1, interval - Math.min(1000, Math.floor(interval / 10)))
      : this.getStaleThreshold()
    let acquired: string | null
    try {
      acquired = await this.redis.set(this.getMaintenanceLockKey(), randomUUID(), 'PX', ttl, 'NX')
    } catch (err) {
      const reason = classifyRedisError(err)
      if (reason === 'oom') {
        this.logger('Redis is out of memory; running an emergency (delete-only) maintenance pass', toError(err))
        return this.performEmergencyMaintenance()
      }
      this.logger(`maintenance lock could not be acquired (${reason}); skipping this pass`, toError(err))
      await this.emitMaintenanceFailure(err, startedAt)
      return null
    }
    if (acquired !== 'OK') return null
    return this.performMaintenance()
  }

  /**
   * Fires the `maintenance` observers for a pass that did not complete (`result: null`): it threw, or its
   * lock could not be acquired. `durationMs` is the wall-clock since `startedAt` (the pass start; for a
   * lock failure, the start of the attempt).
   */
  private async emitMaintenanceFailure(err: unknown, startedAt: number, failedOps = 0): Promise<void> {
    await this.emit('maintenance', {
      result: null,
      failedOps,
      error: toError(err),
      reason: classifyRedisError(err),
      durationMs: Date.now() - startedAt,
    })
  }

  /** Fires the `maintenance` observers for a completed pass (see `RedisJMHooks.maintenance`); `durationMs` runs from `startedAt`. */
  private async emitMaintenance(result: MaintenanceResult, ops: OpGuard, startedAt: number): Promise<void> {
    const { failures, firstError } = ops.summary()
    const payload: MaintenanceEventPayload = { result, failedOps: failures, durationMs: Date.now() - startedAt }
    if (failures > 0) {
      payload.error = toError(firstError)
      payload.reason = classifyRedisError(firstError)
    }
    await this.emit('maintenance', payload)
  }

  /**
   * One full, UNGUARDED maintenance pass (no lock — prefer {@link runMaintenance}, which the
   * `start()` timer uses; this stays public for manual/one-off use).
   *
   * COST IS BOUNDED, not proportional to the log size: each stage examines at most
   * `maxRecordsPerPass` items, and the log and lock scans resume from cursors persisted in Redis
   * (`redisjm:<group>:maintenance-cursor` / `…:maintenance-locks-cursor`), so consecutive passes — on
   * any instance — rotate through a large log. A failed cursor write just restarts that scan at 0.
   * Presence checks are pipelined per batch; no per-record round trips except for the (rare) records
   * that actually change.
   *
   * DELETIONS COME FIRST: every deletion runs before any write, so a pass on a Redis at maxmemory still
   * frees what it can before the first write is refused. Every Redis operation is individually guarded:
   * one failed write is counted and skipped instead of aborting the pass; failures are logged once per
   * pass with their classified reason. Every record write is a compare-and-set against the record as
   * read (see `updateLog`), so the pass can never overwrite a newer write (a claim, a heartbeat…).
   *
   * Stages:
   *  - log batch: delete finished/error/stale records older than `keepFinishedInterval` and
   *    unparseable/foreign records (and their locks); mark `running` records `stale` (lock released)
   *    when their heartbeat lapsed or they ran longer than `maxRunMs`; two-pass orphan check of
   *    `delayed` records missing from the delayed set, and of LEGACY `queued` records (no `enqueuedAt`,
   *    written by <= 0.1.x) missing from their lane list and from `claiming` — first pass stamps
   *    `suspectedAt`, a later one marks them `stale`. While a pre-0.2 instance was seen in the group
   *    recently (it enqueues the maintenance job, which 0.2+ never does), new-format `queued` records
   *    older than the stale threshold get the same check: an old consumer pops without a `claiming`
   *    entry, so a crash between its pop and its claim would otherwise hold the run's lock forever;
   *  - claiming: runs popped longer than the stale threshold ago and never claimed (the popping
   *    instance died, or its claim failed) are put back at the head of their lane (`requeuedCount`); the
   *    run never started, so nothing is lost or executed twice. Entries whose record moved on are removed;
   *  - locks batch: a lock with no backing record is suspected on one pass and released on a later one
   *    past the stale threshold (a pre-0.2 instance's enqueue is momentarily record-less);
   *  - jobs batch: per-job bookkeeping upkeep — job-lock-set members whose lock is gone, lanes whose
   *    list is empty, and jobs with nothing left are pruned.
   *
   * Fires the manager-level `maintenance` event when the pass completes.
   *
   * @returns Counts of stale, cleaned and requeued records (orphaned locks count toward `staleCount`), `mode: 'full'`
   *
   * @example
   * ```ts
   * const { staleCount, cleanedCount, requeuedCount } = await manager.performMaintenance()
   * ```
   */
  async performMaintenance(): Promise<MaintenanceResult> {
    const startedAt = Date.now()
    try {
      return await this.runFullPass(startedAt)
    } catch (err) {
      // A pass that THROWS is observed too (`result: null`), then rethrown for the caller to log.
      await this.emitMaintenanceFailure(err, startedAt)
      throw err
    }
  }

  /** The body of {@link performMaintenance}; `now` is the pass start. Emits `maintenance` on completion. */
  private async runFullPass(now: number): Promise<MaintenanceResult> {
    const ops = this.createOpGuard()
    const cursorKeys = [
      this.getMaintenanceCursorKey(),
      this.getMaintenanceLocksCursorKey(),
      this.getMaintenanceJobsCursorKey(),
    ]

    // ---- READ phase -------------------------------------------------------------------------------
    // The scan cursors (each pass resumes where the last one — on any instance — stopped) and the
    // legacy-instance marker in one round trip; then the independent stages read concurrently.
    const legacyKey = this.getLegacySeenKey()
    const [[logCursor], [locksCursor], [jobsCursor], [legacySeen]] = await this.pipelined(
      [...cursorKeys, legacyKey],
      (p, key) => (key === legacyKey ? p.exists(key) : p.get(key)),
      ops,
    )
    const [log, claims, locks, jobs] = await Promise.all([
      this.readLogStage(toCursor(logCursor), legacySeen === 1, now, ops),
      this.readClaimingStage(now, ops),
      this.readLockStage(toCursor(locksCursor), now, ops),
      this.scanSetBatch(this.getJobRegistryKey(), toCursor(jobsCursor), Math.min(this.options.maxRecordsPerPass, JOB_PRUNE_BATCH)),
    ])

    // ---- DELETE phase — before any write (deletions still succeed when Redis is at maxmemory) ------
    const cleanedCount = await this.cleanLogBatch(log, ops)
    await this.inBatches(claims.drop, ops, (batch) => this.deleteIfUnchanged(this.getClaimingKey(), 'z', batch))
    // Orphaned locks reclaimed — counted as stale (same bucket as the orphaned-queued reclaim).
    let staleCount = await this.releaseLocks(locks.orphans, ops)
    await this.batchDelete(this.getSuspectsKey(), locks.clear, 'hdel', ops)
    // Per-job sets: drop lock-set members whose lock is gone (drift self-heal), empty lanes, and jobs
    // with nothing left. Deletion-only script, one call per job, all in flight together.
    await Promise.all(jobs.members.map((jobName) => ops.run(() => runScript(
      this.redis,
      PRUNE_JOB_SCRIPT,
      [this.getLocksKey(), this.getJobLocksKey(jobName), this.getJobLanesKey(jobName), this.getJobRegistryKey()],
      [jobName, JOB_LOCK_PRUNE_SAMPLE],
    ))))

    // ---- WRITE phase — compare-and-set per changed record, each guarded on its own ---------------
    staleCount += await this.reclaimLiveRecords(log, now, ops)
    // Overdue claims whose record is still `queued`: back to the HEAD of their lane (they were popped
    // first). The compare-and-set guarantees the record is still `queued` at that instant — a late claim
    // by the original popper either landed first (we skip) or will be fenced (its claim sees a re-queued
    // record and a later pop's claim wins).
    let requeuedCount = 0
    for (const [jobId, json] of claims.requeue) {
      await ops.run(async () => {
        if (await this.requeueUnclaimed(jobId, json)) requeuedCount++
      })
    }
    // First sighting of a record-less lock: stamp when we first saw it and wait for a later pass.
    await this.inBatches(locks.stamp, ops, (batch) =>
      this.redis.hset(this.getSuspectsKey(), Object.fromEntries(batch.map((member) => [member, String(now)]))))
    // Persist the scan cursors so the next pass (on any instance) continues where this one stopped.
    const nextCursors = [log.cursor, locks.cursor, jobs.cursor]
    await this.pipelined(cursorKeys, (p, key, i) => p.set(key, nextCursors[i]), ops)

    ops.report('maintenance pass')
    const result: MaintenanceResult = { staleCount, cleanedCount, requeuedCount, mode: 'full' }
    await this.emitMaintenance(result, ops, now)
    return result
  }

  /**
   * Log stage reads: one bounded log batch (see {@link readLogBatch}) plus the presence checks of its
   * `delayed` records (ZSCORE delayed) and LEGACY `queued` records (LPOS on their lane + ZSCORE claiming
   * — a queued record parked in `claiming` is present; the claiming stage owns it), in one pipeline.
   * `presence` maps each checked jobId to whether it is on its structure (`undefined` = read failed).
   *
   * New-format queued records normally need no check at all: their invariant (on the lane list or in
   * `claiming`) makes the claiming stage the orphan detector. EXCEPT while a pre-0.2 instance is around
   * (`legacyActive`, mixed rolling deploy, see `noteLegacyInstance`): its consumer pops with a plain
   * LMPOP — no `claiming` entry — so if it dies before its claim, a new-format record is left `queued` on
   * neither structure, holding its lock forever. During that window, new-format queued records older
   * than the stale threshold get the legacy LPOS check too (the cost the old instances pay anyway).
   */
  private async readLogStage(
    cursor: string,
    legacyActive: boolean,
    now: number,
    ops: OpGuard,
  ): Promise<LogBatch & { presence: Map<string, boolean | undefined> }> {
    const batch = await this.readLogBatch(cursor, now)
    const staleThreshold = this.getStaleThreshold()
    const checked = batch.live.filter(([, r]) => r.status === 'delayed' || (r.status === 'queued' && (
      r.enqueuedAt === undefined || (legacyActive && now - r.enqueuedAt > staleThreshold)
    )))
    const replies = await this.pipelined(checked, (p, [jobId, r]) => {
      if (r.status === 'delayed') {
        p.zscore(this.getDelayedKey(), jobId)
      } else {
        p.lpos(this.getQueueKey(r.lane), jobId)
        p.zscore(this.getClaimingKey(), jobId)
      }
    }, ops)
    const presence = new Map(checked.map(([jobId], i) => [
      jobId,
      replies[i].includes(undefined) ? undefined : replies[i].some((reply) => reply !== null),
    ]))
    return { ...batch, presence }
  }

  /**
   * Claiming stage reads: runs popped longer than the stale threshold ago and still not claimed (the
   * popping instance died, or its claim failed). Those whose record is still `queued` are to be
   * `requeue`d (with the JSON read, the compare-and-set's seed); the rest (claimed since, gone, or
   * garbage) leave a stale claiming entry to `drop` (`[jobId, score read]`).
   */
  private async readClaimingStage(now: number, ops: OpGuard): Promise<{
    drop: Array<[string, string]>
    requeue: Array<[string, string]>
  }> {
    let overdue: Array<[string, string]> = []
    try {
      overdue = scorePairs(await this.redis.zrangebyscore(
        this.getClaimingKey(), '-inf', now - this.getStaleThreshold(), 'WITHSCORES', 'LIMIT', 0, this.options.maxRecordsPerPass,
      ))
    } catch (err) {
      ops.fail(err)
    }
    const replies = await this.pipelined(overdue, (p, [jobId]) => p.hget(this.getLogKey(), jobId), ops)
    const drop: Array<[string, string]> = []
    const requeue: Array<[string, string]> = []
    overdue.forEach(([jobId, score], i) => {
      const [json] = replies[i]
      if (json === undefined) return // read failed: leave it for the next pass
      if (typeof json === 'string' && this.parseRecord(json, jobId)?.status === 'queued') requeue.push([jobId, json])
      // Dropped with the score read: a pop of the same jobId since then (a fresh mark) is spared.
      else drop.push([jobId, score])
    })
    return { drop, requeue }
  }

  /**
   * Locks stage reads, resuming from its own cursor — works without the full log map. A lock with no
   * backing record is suspected on one pass (`stamp`) and released on a later one past the stale
   * threshold (`orphans`): a pre-0.2 instance's enqueue is momentarily record-less. Concurrently, suspicions
   * that no longer apply (record landed, or lock released elsewhere) are collected to `clear` — the
   * suspects hash only ever holds record-less locks, so one bounded HSCAN covers it in practice.
   */
  private async readLockStage(cursor: string, now: number, ops: OpGuard): Promise<{
    cursor: string
    orphans: string[]
    stamp: string[]
    clear: string[]
  }> {
    const logKey = this.getLogKey()
    const locksKey = this.getLocksKey()
    const suspectsKey = this.getSuspectsKey()
    const staleThreshold = this.getStaleThreshold()
    const max = this.options.maxRecordsPerPass
    const readLocks = async () => {
      const batch = await this.scanSetBatch(locksKey, cursor, max)
      const members = batch.members
      const hasRecord = await this.pipelined(members, (p, m) => p.hexists(logKey, m), ops)
      const recordless = members.filter((_, i) => hasRecord[i][0] === 0)
      const suspicion = await this.pipelined(recordless, (p, m) => p.hget(suspectsKey, m), ops)
      const orphans: string[] = []
      const stamp: string[] = []
      recordless.forEach((member, i) => {
        const [suspectedAt] = suspicion[i]
        if (suspectedAt === undefined) return // read failed
        if (suspectedAt === null) stamp.push(member)
        else if (now - Number(suspectedAt) > staleThreshold) orphans.push(member)
      })
      return { cursor: batch.cursor, orphans, stamp }
    }
    const readSuspects = async () => {
      let suspects: string[] = []
      try {
        suspects = [...(await this.scanHashBatch(suspectsKey, '0', max, true)).entries.keys()]
      } catch (err) {
        ops.fail(err)
      }
      const state = await this.pipelined(suspects, (p, m) => {
        p.sismember(locksKey, m)
        p.hexists(logKey, m)
      }, ops)
      return suspects.filter((_, i) => {
        const [locked, hasRecord] = state[i]
        return locked !== undefined && hasRecord !== undefined && (hasRecord === 1 || locked === 0)
      })
    }
    const [lockState, staleSuspicions] = await Promise.all([readLocks(), readSuspects()])
    return { ...lockState, clear: [...new Set([...lockState.orphans, ...staleSuspicions])] }
  }

  /**
   * Write stage of the log batch: marks `running` records `stale` when their heartbeat lapsed or they
   * ran longer than `maxRunMs`, and runs the two-pass orphan check of the presence-checked records — a
   * first pass stamps `suspectedAt`, a later one past the stale threshold marks them `stale` (the
   * pop→claim window looks the same as an orphan, hence two passes). Every write is a compare-and-set
   * seeded with the scanned JSON (the lock release and history TTL ride on it — see `transition`).
   * Returns how many records were marked stale.
   */
  private async reclaimLiveRecords(
    log: LogBatch & { presence: Map<string, boolean | undefined> },
    now: number,
    ops: OpGuard,
  ): Promise<number> {
    const staleThreshold = this.getStaleThreshold()
    let staled = 0
    for (const [jobId, snapshot] of log.live) {
      let mutate: (record: JobLogRecord) => Mutation
      if (snapshot.status === 'running') {
        if (!this.runOverdue(snapshot, now)) continue // cheap pre-filter on the snapshot
        mutate = (record) => {
          // Still the same execution, still running, still overdue on its CURRENT heartbeat?
          if (record.status !== 'running' || record.executionId !== snapshot.executionId) return false
          const reason = this.runOverdue(record, now)
          if (!reason) return false
          this.markStale(record, reason, now)
        }
      } else {
        if (!log.presence.has(jobId)) continue // new-format queued: covered by the claiming stage
        const present = log.presence.get(jobId)
        if (present === undefined) continue // presence read failed
        if (present && snapshot.suspectedAt === undefined) continue
        mutate = (record) => {
          // Claimed / promoted / reclaimed since the scan → not ours to touch.
          if (record.status !== snapshot.status) return false
          if (present) {
            // Back on its structure: clear a pending suspicion.
            if (record.suspectedAt === undefined) return false
            delete record.suspectedAt
          } else if (record.suspectedAt === undefined) {
            // Two-pass orphan detection, pass 1: suspect.
            record.suspectedAt = now
          } else if (now - record.suspectedAt > staleThreshold) {
            // Pass 2: still orphaned past the threshold → reclaim.
            this.markStale(record, 'orphaned', now)
          } else {
            return false
          }
        }
      }
      await ops.run(async () => {
        const { outcome, record } = await this.updateLog(jobId, mutate, log.entries.get(jobId))
        if (outcome === 'written' && record?.status === 'stale') staled++
      })
    }
    return staled
  }

  /**
   * Whether a `running` record is overdue for reclaim, and why: `'maxRunMs'` when it has run longer
   * than `maxRunMs` (checked against `startedAt`, independent of the heartbeat — a hung handler's
   * heartbeat timer keeps the record looking alive forever), else `'heartbeat'` when its heartbeat
   * lapsed past the stale threshold, else `null`.
   */
  private runOverdue(record: JobLogRecord, now: number): 'maxRunMs' | 'heartbeat' | null {
    const maxRunMs = this.options.maxRunMs
    if (maxRunMs > 0 && record.startedAt !== undefined && now - record.startedAt > maxRunMs) return 'maxRunMs'
    const lastHeartbeat = record.heartbeat ?? record.startedAt ?? 0
    return now - lastHeartbeat > this.getStaleThreshold() ? 'heartbeat' : null
  }

  /**
   * Marks `record` stale for `reason`. The lock release and history TTL ride on the transition that
   * writes it (see `transition`); the counterpart that undoes a heartbeat/orphan stale is
   * `acceptRunningOrStale`.
   */
  private markStale(record: JobLogRecord, reason: NonNullable<JobLogRecord['staleReason']>, now: number): void {
    record.status = 'stale'
    record.finishedAt = now
    record.staleReason = reason
    delete record.suspectedAt
    if (reason === 'maxRunMs') record.error = `Run exceeded maxRunMs (${this.options.maxRunMs}ms)`
  }

  /**
   * The EMERGENCY pass {@link runMaintenance} runs when Redis refuses the maintenance lock with an OOM
   * error: lock-free and delete-only — expired terminal records and unparseable garbage records are
   * HDEL'd and the garbage records' locks SREM'd (in batches), nothing is written. Deletions are
   * idempotent, so several instances running it concurrently is safe. Bounded by `maxRecordsPerPass`;
   * the shared cursor can't be written under OOM, so it resumes from a per-instance cursor instead.
   */
  private async performEmergencyMaintenance(): Promise<MaintenanceResult> {
    const startedAt = Date.now()
    try {
      const ops = this.createOpGuard()
      const batch = await this.readLogBatch(this.emergencyCursor, startedAt)
      this.emergencyCursor = batch.cursor
      const cleanedCount = await this.cleanLogBatch(batch, ops)
      ops.report('emergency maintenance pass')
      const result: MaintenanceResult = { staleCount: 0, cleanedCount, requeuedCount: 0, mode: 'emergency' }
      await this.emitMaintenance(result, ops, startedAt)
      return result
    } catch (err) {
      await this.emitMaintenanceFailure(err, startedAt)
      throw err
    }
  }

  /**
   * Records that a pre-0.2 instance is active in this group (seen: one of its maintenance-job entries
   * was popped — 0.2+ never enqueues that job). Opens the window in which maintenance also orphan-checks
   * new-format `queued` records (see `performMaintenance`). Fire-and-forget: a failed write only means
   * the window opens on the next sighting.
   */
  private noteLegacyInstance(): void {
    const ttl = Math.max(LEGACY_WINDOW_MS, 10 * this.getStaleThreshold())
    this.redis.set(this.getLegacySeenKey(), String(Date.now()), 'PX', ttl).catch(() => {})
  }

  /**
   * Scans `key` with `cmd` from `cursor`, `count` per call, handing each page to `onPage` until it
   * returns `false` or the scan completes. Returns the cursor to resume from (`'0'` = completed).
   */
  private async scan(
    cmd: 'hscan' | 'sscan',
    key: string,
    cursor: string,
    count: number,
    onPage: (items: string[]) => boolean,
  ): Promise<string> {
    let next = cursor
    let more: boolean
    do {
      const [c, items] = await this.redis[cmd](key, next, 'COUNT', count)
      next = c
      more = onPage(items)
    } while (next !== '0' && more)
    return next
  }

  /**
   * HSCANs `key` from `cursor` until about `max` entries were collected or the scan completed — with
   * `onePage`, a single HSCAN call. Returns the entries (deduped by field: HSCAN can return a field
   * twice under concurrent writes; last value wins) and the cursor to resume from (`'0'` = completed).
   */
  private async scanHashBatch(key: string, cursor: string, max: number, onePage = false): Promise<{ entries: Map<string, string>; cursor: string }> {
    const entries = new Map<string, string>()
    const next = await this.scan('hscan', key, cursor, Math.min(max, 500), (flat) => {
      // HSCAN replies as a flat array alternating field, value, field, value, …
      for (let i = 0; i < flat.length; i += 2) entries.set(flat[i], flat[i + 1])
      return !onePage && entries.size < max
    })
    return { entries, cursor: next }
  }

  /** SSCAN counterpart of {@link scanHashBatch}. */
  private async scanSetBatch(key: string, cursor: string, max: number): Promise<{ members: string[]; cursor: string }> {
    const members = new Set<string>()
    const next = await this.scan('sscan', key, cursor, Math.min(max, 500), (batch) => {
      for (const m of batch) members.add(m)
      return members.size < max
    })
    return { members: [...members], cursor: next }
  }

  /**
   * Queues `add(pipeline, item, i)`'s commands (one or more) per item in a single pipeline and returns
   * each item's replies, in order; a per-command error, or a failed pipeline, yields `undefined` for
   * that reply (counted by `ops`).
   */
  private async pipelined<T>(
    items: T[],
    add: (pipeline: ReturnType<Redis['pipeline']>, item: T, i: number) => void,
    ops: OpGuard,
  ): Promise<unknown[][]> {
    if (items.length === 0) return []
    const pipeline = this.redis.pipeline()
    const ends = items.map((item, i) => {
      add(pipeline, item, i)
      return pipeline.length
    })
    let replies: unknown[]
    try {
      replies = ((await pipeline.exec()) ?? []).map(([err, value]) => {
        if (!err) return value
        ops.fail(err)
        return undefined
      })
    } catch (err) {
      ops.fail(err)
      replies = []
    }
    return ends.map((end, i) => {
      const start = i === 0 ? 0 : ends[i - 1]
      return Array.from({ length: end - start }, (_, k) => replies[start + k])
    })
  }

  /**
   * Scans one bounded log batch from `cursor` and splits it (see {@link planLogCleanup}) — the read
   * half shared by the full and the emergency pass.
   */
  private async readLogBatch(cursor: string, now: number): Promise<LogBatch> {
    const { entries, cursor: next } = await this.scanHashBatch(this.getLogKey(), cursor, this.options.maxRecordsPerPass)
    return { entries, cursor: next, ...this.planLogCleanup(entries, now) }
  }

  /**
   * The deletion half shared by the full and the emergency pass: compare-and-deletes the batch's
   * garbage and expired records (a record replaced since the scan — e.g. the runId was re-enqueued — is
   * kept), then releases the locks of the garbage records actually deleted (a replaced one owns its lock
   * now). Returns how many records were deleted.
   */
  private async cleanLogBatch(batch: LogBatch, ops: OpGuard): Promise<number> {
    const deleted = await this.deleteRecordsIfUnchanged(batch, ops)
    await this.releaseLocks(batch.garbage.filter((id) => deleted.has(id)), ops)
    return deleted.size
  }

  /**
   * Splits scanned log entries into deletable `garbage` (unparseable/foreign — retention never
   * applies), `expired` terminal records (finished/error/stale older than `keepFinishedInterval`), and
   * `live` non-terminal records (running/queued/delayed) for the write stages.
   */
  private planLogCleanup(entries: Map<string, string>, now: number): Pick<LogBatch, 'garbage' | 'expired' | 'live'> {
    const garbage: string[] = []
    const expired: string[] = []
    const live: Array<[string, JobLogRecord]> = []
    for (const [jobId, json] of entries) {
      // Defense-in-depth: one corrupt/foreign record must not abort the whole sweep.
      const record = this.parseRecord(json, jobId)
      if (!record) {
        garbage.push(jobId)
      } else if (isTerminal(record.status)) {
        if (record.finishedAt !== undefined && now - record.finishedAt > this.options.keepFinishedInterval) {
          expired.push(jobId)
        }
      } else if (record.status === 'running' || record.status === 'queued' || record.status === 'delayed') {
        live.push([jobId, record])
      }
    }
    return { garbage, expired, live }
  }

  /** Runs `fn` per batch of {@link DELETE_BATCH_SIZE} items, in order, each batch guarded by `ops`. */
  private async inBatches<T>(items: T[], ops: OpGuard, fn: (batch: T[]) => Promise<unknown>): Promise<void> {
    for (let i = 0; i < items.length; i += DELETE_BATCH_SIZE) {
      const batch = items.slice(i, i + DELETE_BATCH_SIZE)
      await ops.run(() => fn(batch))
    }
  }

  /**
   * Deletes `members` from `key` in batches (one HDEL/SREM/ZREM per batch, see {@link inBatches}).
   * Returns how many were actually removed (per Redis' replies).
   */
  private async batchDelete(key: string, members: string[], cmd: 'hdel' | 'srem' | 'zrem', ops: OpGuard): Promise<number> {
    let removed = 0
    await this.inBatches(members, ops, async (batch) => {
      removed += await this.redis[cmd](key, ...batch)
    })
    return removed
  }

  /**
   * Deletes a batch's scanned log records with a compare-and-delete (see `DELETE_IF_UNCHANGED_SCRIPT`),
   * in batches: `expired` only while unchanged since the scan; `garbage` only while it is still the very
   * value judged garbage — re-read as RAW bytes (it need not be valid UTF-8, so the scanned decoded copy
   * needn't hash like the stored bytes) and re-judged by the manager's own parser, the single source of
   * truth for "is a record". Returns the ids actually deleted.
   */
  private async deleteRecordsIfUnchanged(batch: LogBatch, ops: OpGuard): Promise<Set<string>> {
    const logKey = this.getLogKey()
    const pairs: Array<[string, string]> = batch.expired.map((id) => [id, sha1Hex(batch.entries.get(id) ?? '')])
    await this.inBatches(batch.garbage, ops, async (ids) => {
      const raws = await this.redis.hmgetBuffer(logKey, ...ids)
      raws.forEach((raw, i) => {
        if (raw && !isRecordJson(raw.toString())) pairs.push([ids[i], sha1Hex(raw)])
      })
    })
    const deleted = new Set<string>()
    await this.inBatches(pairs, ops, async (slice) => {
      for (const id of await this.deleteIfUnchanged(logKey, 'h', slice)) deleted.add(id)
    })
    return deleted
  }

  /**
   * One compare-and-delete call (see `DELETE_IF_UNCHANGED_SCRIPT`): `'h'` pairs are (field, SHA-1 of the
   * value read), `'z'` pairs (member, score read). Resolves the fields / members actually deleted.
   */
  private async deleteIfUnchanged(key: string, kind: 'h' | 'z', pairs: Array<[string, string]>): Promise<string[]> {
    if (pairs.length === 0) return []
    const reply = await runScript(this.redis, DELETE_IF_UNCHANGED_SCRIPT, [key], [kind, ...pairs.flat()])
    return Array.isArray(reply) ? reply.map(String) : []
  }

  /**
   * Releases maintenance-reclaimed run locks (garbage records' and orphaned ones) in batches: per batch
   * ONE transaction SREMs them from the global locks set and from each job's lock set (deletions —
   * accepted under OOM). Returns how many global locks were removed.
   */
  private async releaseLocks(jobIds: string[], ops: OpGuard): Promise<number> {
    let released = 0
    await this.inBatches(jobIds, ops, async (batch) => {
      const byJob = new Map<string, string[]>()
      for (const jobId of batch) {
        const { jobName } = splitJobId(jobId)
        const ids = byJob.get(jobName)
        if (ids) ids.push(jobId)
        else byJob.set(jobName, [jobId])
      }
      const tx = this.redis.multi().srem(this.getLocksKey(), ...batch)
      for (const [jobName, ids] of byJob) tx.srem(this.getJobLocksKey(jobName), ...ids)
      const [[err, removed] = [null, 0]] = (await tx.exec()) ?? []
      if (err) throw err
      released += Number(removed)
    })
    return released
  }

  /**
   * Per-pass failure tally for maintenance (see `OpGuard`): `run` executes one guarded Redis operation,
   * `fail` records a failure caught elsewhere, `report` logs ONCE per pass with the count and the
   * classified reason of the first failure.
   */
  private createOpGuard(): OpGuard {
    let failures = 0
    let firstError: unknown
    const fail = (err: unknown) => {
      failures++
      if (failures === 1) firstError = err
    }
    return {
      fail,
      run: async (op) => {
        try {
          await op()
          return true
        } catch (err) {
          fail(err)
          return false
        }
      },
      report: (label) => {
        if (failures === 0) return
        this.logger(
          `${label}: ${failures} Redis operation(s) failed (${classifyRedisError(firstError)}); continued with the rest`,
          toError(firstError),
        )
      },
      summary: () => ({ failures, firstError }),
    }
  }

  /**
   * Dispatches a MANAGER-level hook as an isolated observer: each registered handler is awaited in
   * turn and a throwing/rejecting one is logged and skipped — it can neither change the run's outcome
   * or Redis state nor prevent the other handlers from being notified. (Job-level hooks are NOT routed
   * through here: they are the lifecycle and keep their throw semantics.)
   */
  private async emit<N extends keyof RedisJMHooks>(name: N, payload: Parameters<RedisJMHooks[N]>[0]): Promise<void> {
    const caller = async (hooks: Array<(...args: any[]) => any>, args: any[]) => {
      for (const hook of hooks) {
        try {
          await hook(...args)
        } catch (err) {
          this.logger(`manager "${String(name)}" hook threw (ignored — manager hooks are observers)`, toError(err))
        }
      }
    }
    await (this.callHookWith as (c: typeof caller, n: N, ...a: any[]) => Promise<void>)(caller, name, payload)
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
   * Self-heal decision shared by the heartbeat and update hooks (after their executionId fence, see
   * `updateOwned` in `registerJob`) — the recovery side of the stale-then-recovered execution-fencing
   * asymmetry.
   *
   * A live handler whose heartbeat lapsed — maintenance staled its record and released its lock while
   * the event loop was pinned — still OWNS that record as long as its `executionId` matches. The
   * moment it writes again (a heartbeat, `setProgress`, or `setAttrs`) it proves it's alive, so we
   * resurrect: flip `stale` → `running` and clear the maintenance-stamped `finishedAt`; the transition
   * re-takes the lock with the write (stale → running) so a producer can't double-enqueue the runId now
   * that the run is demonstrably still going. This closes the old asymmetry where such a run's updates
   * were rejected yet its finish (fenced on executionId only) still landed, freezing progress and
   * dropping attrs.
   *
   * Returns `true` (apply the write) for a `running` record or a resurrected `stale` one; `false`
   * (reject) for a record that reached a terminal/non-running state a live run can't own
   * (finished/error/queued/delayed, or a `maxRunMs` stale). Once a successor re-enqueues the runId
   * (fresh record, no/other executionId), the old execution's writes keep failing the fence.
   */
  private acceptRunningOrStale(record: JobLogRecord): boolean {
    if (record.status === 'running') return true
    // A `maxRunMs` stale is maintenance's verdict that this execution ran too long — final for its
    // heartbeats and updates. A hung handler's heartbeat timer keeps firing, so resurrecting here would
    // undo the backstop on the next heartbeat (and re-lock the runId forever). Rejecting also aborts the
    // run's signal.
    if (record.status === 'stale' && record.staleReason !== 'maxRunMs') {
      record.status = 'running'
      delete record.finishedAt
      delete record.staleReason
      return true
    }
    return false
  }

  /**
   * Purges one run's state in one atomic, deletion-only step (see `PURGE_SCRIPT` — accepted under OOM):
   * its record and run lock, plus the requested `lane` list entry / `claiming` / `delayed` / `suspects`
   * entries. `'force'` purges unconditionally; `'orphan'` only while no valid record backs the jobId: the
   * stored value is read here as RAW bytes and judged by the manager's own parser — a record means
   * "skip" — and the script then purges only while the value is still exactly what was judged (absent,
   * or that garbage). So a record re-enqueued since is never deleted or unlocked, and Lua never has to
   * agree with `JSON.parse` on what a record is. Resolves whether it purged.
   */
  private async purge(
    jobId: string,
    mode: 'force' | 'orphan',
    also: { lane?: string; claiming?: boolean; delayed?: boolean; suspects?: boolean } = {},
  ): Promise<boolean> {
    const flags = (also.lane ? 'q' : '') + (also.claiming ? 'c' : '') + (also.delayed ? 'd' : '') + (also.suspects ? 's' : '')
    let witness = ''
    if (mode === 'orphan') {
      const raw = await this.redis.hgetBuffer(this.getLogKey(), jobId)
      if (raw) {
        if (isRecordJson(raw.toString())) return false
        witness = sha1Hex(raw)
      }
    }
    const keys = [
      this.getLogKey(),
      this.getLocksKey(),
      this.getJobLocksKey(splitJobId(jobId).jobName),
      this.getClaimingKey(),
      this.getDelayedKey(),
      this.getSuspectsKey(),
      also.lane ?? this.getLogKey(),
    ]
    return Number(await runScript(this.redis, PURGE_SCRIPT, keys, [jobId, mode, flags, witness])) === 1
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
    // Shape guard (see `isRecordShape`): anything else is malformed and treated as garbage by the callers.
    if (!isRecordShape(value)) {
      this.logger(`skipping malformed log record "${jobId}"`)
      return null
    }
    return value
  }

  /** Fetches and parses a single log record by `jobId`; `null` if absent or unparseable. */
  private async readRecord(jobId: string): Promise<JobLogRecord | null> {
    return this.decodeRecord(await this.redis.hget(this.getLogKey(), jobId), jobId)
  }

  /** {@link parseRecord} of a possibly absent stored value: `null` when there is none or it is unreadable. */
  private decodeRecord(json: string | null | undefined, jobId: string): JobLogRecord | null {
    return json ? this.parseRecord(json, jobId) : null
  }

  /**
   * The single enqueue implementation behind `enqueue` / `enqueueMany` / `queue` / `queueFirst`:
   * validation and the inputs-size guard in JS, then ONE `ENQUEUE_SCRIPT` call for all entries (dedupe
   * via the run lock, `maxInFlight` → `'busy'`, lane cap → `'full'`, record + queue/delayed entry +
   * bookkeeping sets). The script is atomic and, under maxmemory, refused before any write — so a failed
   * enqueue leaves nothing behind and there is nothing to roll back. A Redis failure throws
   * `RedisJMEnqueueError` for the whole call.
   */
  private async enqueueRun<TInputs>(
    job: Job<TInputs, any>,
    entries: Array<{ runId: string; inputs: TInputs }>,
    pushCmd: 'rpush' | 'lpush',
    options?: QueueOptions,
  ): Promise<EnqueueResult[]> {
    // Authoritative lane validation: the producer path never goes through registerJob, so validate
    // here (before any Redis write) as well as at registration.
    this.validateLane(job.getLane(), job.getName())

    const delay = options?.delay
    if (delay !== undefined) {
      if (!Number.isFinite(delay) || delay < 0) {
        throw new TypeError(`queue(options.delay): delay must be a finite number >= 0, got ${String(delay)}`)
      }
      // A priority insert stages the run at the FRONT of the live queue; there is no "front" of a
      // time-ordered delayed set, so combining the two is contradictory — reject it.
      if (delay > 0 && pushCmd === 'lpush') {
        throw new TypeError('queueFirst() / enqueue({ first: true }) cannot be combined with a delay — a priority insert cannot be delayed')
      }
    }
    if (entries.length === 0) return []

    const meta = job.getMetadata()
    const { jobName, lane } = meta
    const isDelayed = delay !== undefined && delay > 0
    const now = Date.now()
    // Effective max serialized inputs size: the job's value wins, `0` = unlimited.
    const maxInputsBytes = meta.maxInputsBytes !== undefined ? positiveOrZero(meta.maxInputsBytes) : this.options.maxInputsBytes
    const args: Array<string | number> = []
    for (const { runId, inputs } of entries) {
      const jobId = job.getJobId(runId)
      const inputsJson = JSON.stringify(inputs) as string | undefined
      // Size guard BEFORE any Redis traffic: one oversized payload must not be what fills Redis. In a
      // batch, one oversized entry rejects the whole batch (nothing written).
      if (maxInputsBytes > 0) {
        const size = Buffer.byteLength(inputsJson ?? '', 'utf8')
        if (size > maxInputsBytes) {
          throw await this.enqueueFailure(
            jobName, runId, jobId,
            new Error(`serialized inputs are ${size} bytes, limit is ${maxInputsBytes}`),
            'inputs-too-large',
          )
        }
      }
      const record: Omit<JobLogRecord, 'inputs'> = {
        jobId,
        jobName,
        runId,
        targetGroup: this.targetGroup,
        // `undefined` for the default lane; JSON.stringify omits it, so a default-lane record carries
        // no `lane` key (as in 0.0.3).
        lane,
        // A delayed record holds the lock (dedupe still applies while waiting), like `queued`.
        status: isDelayed ? 'delayed' : 'queued',
        progress: 0,
      }
      if (isDelayed) record.readyAt = now + delay!
      else record.enqueuedAt = now
      args.push(jobId, serializeRecord(record, inputsJson), record.readyAt ?? 0)
    }

    const keys = [
      this.getLocksKey(),
      this.getLogKey(),
      this.getDelayedKey(),
      this.getQueueKey(lane),
      this.getJobLocksKey(jobName),
      this.getJobLanesKey(jobName),
      this.getJobRegistryKey(),
    ]
    const header = [
      jobName,
      isDelayed ? 'D' : pushCmd === 'lpush' ? 'L' : 'R',
      this.resolveLaneCap(meta) ?? -1,
      nonNegativeInt(meta.maxInFlight) ?? -1,
    ]
    let statuses: unknown
    try {
      statuses = await runScript(this.redis, ENQUEUE_SCRIPT, keys, [...header, ...args])
    } catch (err) {
      // One script call: nothing was written (OOM refuses it up front). Report it per the first entry.
      const { runId } = entries[0]
      throw await this.enqueueFailure(jobName, runId, job.getJobId(runId), err)
    }
    const list = Array.isArray(statuses) ? statuses : []
    return entries.map(({ runId }, i) => ({
      status: (list[i] ?? 'deduped') as EnqueueResult['status'],
      jobId: job.getJobId(runId),
    }))
  }

  /**
   * Effective enqueue cap of a job's lane: the smaller of `JobMetadata.maxQueued` and
   * `laneCaps[lane]` when either is defined, else `undefined` (no cap, no LLEN round trip).
   */
  private resolveLaneCap(meta: JobMetadata): number | undefined {
    const caps = [nonNegativeInt(meta.maxQueued), this.options.laneCaps[this.laneLabel(meta.lane)]]
      .filter((c): c is number => c !== undefined)
    return caps.length ? Math.min(...caps) : undefined
  }

  /**
   * Builds the `RedisJMEnqueueError` for a failed enqueue write, counts OOM refusals, and notifies the
   * `enqueueFailed` observers (isolated — an observer can't mask the error). Returns the error for the
   * caller to throw.
   */
  private async enqueueFailure(
    jobName: string,
    runId: string,
    jobId: string,
    err: unknown,
    forcedReason?: EnqueueErrorReason,
  ): Promise<RedisJMEnqueueError> {
    const reason = forcedReason ?? classifyRedisError(err)
    if (reason === 'oom') this.oomRefusals++
    const error = new RedisJMEnqueueError(reason, jobId, err)
    await this.emit('enqueueFailed', { jobId, jobName, runId, reason, error })
    return error
  }

  /**
   * Atomic read-modify-write of a single log record: read it, let `mutate` change it (see `Mutation`),
   * then write it with a compare-and-set (`TRANSITION_SCRIPT`) that only lands if the stored JSON is
   * still exactly what was read, together with the queue-structure side effects the status change
   * implies (see {@link transition}). On a lost race the record is re-read and `mutate` re-applied (up to
   * `CAS_ATTEMPTS` times), so a concurrent writer — a claim, a heartbeat, a finish, another instance's
   * maintenance — is never overwritten by a stale copy. `mutate` must therefore be re-runnable: it gets
   * a fresh copy each attempt, and what it decided is read back from the returned `LogUpdate`.
   *
   * The first attempt starts from `seed` (JSON the caller already read), else from the JSON this
   * instance last saw of the record (`recordJson`) — no read at all on the hot path. A stale seed just
   * loses its compare-and-set (not counted as an attempt) and the record is re-read; a rejection decided
   * on a seed is likewise re-checked on a fresh read, so outcomes are exactly those of a read-first
   * write.
   *
   * Writes from THIS instance to the same record are queued and run one at a time. Without that, a
   * handler firing many `ctx.setProgress()`/`setAttrs()` calls concurrently (or one racing a heartbeat
   * or the finish) would have its own writes lose each other's compare-and-set: with N concurrent local
   * writers the last needs N attempts, so beyond `CAS_ATTEMPTS` the update — or the terminal write —
   * threw. Serialized, local writers never conflict; the CAS retry budget is left for genuinely
   * concurrent writers on OTHER instances (maintenance, a promotion), which are rare per record.
   *
   * Outcome `'missing'` when there is no (parseable) record, `'rejected'` when the mutator rejected
   * (the record isn't in a state worth touching, or no longer belongs to this execution) or a take-from-
   * set precondition failed, and `'written'` otherwise. Throws on a Redis error, or when every attempt
   * lost its race.
   */
  private updateLog(
    jobId: string,
    mutate: (record: JobLogRecord) => Mutation,
    seed?: string,
  ): Promise<LogUpdate> {
    const previous = this.recordWrites.get(jobId) ?? Promise.resolve()
    const run = () => this.updateLogNow(jobId, mutate, seed)
    const result = previous.then(run, run)
    const tail = result.then(() => {}, () => {})
    this.recordWrites.set(jobId, tail)
    void tail.then(() => {
      if (this.recordWrites.get(jobId) === tail) this.recordWrites.delete(jobId)
    })
    return result
  }

  /** One compare-and-set read-modify-write of a record (see {@link updateLog}, which serializes these). */
  private async updateLogNow(
    jobId: string,
    mutate: (record: JobLogRecord) => Mutation,
    seed?: string,
  ): Promise<LogUpdate> {
    let next = seed ?? this.recordJson.get(jobId)
    // Forgotten unless this write leaves the record `running` under this instance (re-set below).
    this.recordJson.delete(jobId)
    for (let attempt = 0; attempt < CAS_ATTEMPTS;) {
      const seeded = next !== undefined
      let json: string | null | undefined = next
      next = undefined
      if (!seeded) {
        json = await this.redis.hget(this.getLogKey(), jobId)
        attempt++
      }
      const record = json ? this.parseRecord(json, jobId) : null
      // Only a compare-and-set proves a seed current: a verdict reached on a seed alone (no record,
      // a rejection) is re-checked against a fresh read.
      if (!json || !record) {
        if (seeded) continue
        return { outcome: 'missing' }
      }
      const before = record.status
      const mutation = mutate(record)
      if (mutation === false || typeof mutation === 'string') {
        if (seeded) continue
        return { outcome: 'rejected', record, reason: mutation || undefined }
      }
      const written = JSON.stringify(record)
      const result = await this.transition(jobId, json, written, before, record, mutation || undefined)
      if (result === 1) {
        if (record.status === 'running') this.recordJson.set(jobId, written)
        return { outcome: 'written', record }
      }
      if (result === -1) return { outcome: 'missing' }
      if (result === -2) return { outcome: 'rejected', reason: 'precondition' }
      // 0: someone else wrote the record since we read it → re-read and re-apply.
    }
    throw new Error(`record "${jobId}" kept changing under concurrent writers; gave up after ${CAS_ATTEMPTS} attempts`)
  }

  /**
   * Runs `TRANSITION_SCRIPT` for one record write (see there for the ops and result codes), DERIVING
   * the queue-structure side effects from the status change, so the structure invariant — "a `queued`
   * record is on its lane list or in `claiming`; a `delayed` one is on the delayed set; a live run holds
   * its lock, a terminal one doesn't" — is stated once, here, instead of by every caller:
   * - leaving `queued` (or a queued record pushed back) → out of `claiming` (or, with
   *   `takeFromClaiming`, only if still there);
   * - `delayed` → `queued` (promotion) → taken off the delayed set, aborting unless still there;
   *   entering `delayed` → onto the delayed set at `readyAt`;
   * - entering a terminal status → the lock is released and the record retired with the write: a
   *   history TTL of `keepFinishedInterval`, or — `finished`/`error` with `keepFinishedInterval: 0` —
   *   deleted (a `stale` record is always kept: a live run may still resurrect it);
   * - `stale` → `running`/`delayed` (a resurrected run, a retry off a staled run) → the lock is re-taken;
   * - `extra.push` → pushed onto the record's lane (head `'L'` / tail `'R'`).
   */
  private async transition(
    jobId: string,
    expected: string,
    next: string,
    before: JobStatus,
    record: JobLogRecord,
    extra?: { push: 'L' | 'R'; takeFromClaiming?: boolean },
  ): Promise<number> {
    const after = record.status
    const keepFinished = this.options.keepFinishedInterval
    const claiming = extra?.takeFromClaiming
      ? 'take'
      : before === 'queued' && (after !== 'queued' || extra) ? 'remove' : ''
    const delayed = before === 'delayed' && after === 'queued'
      ? 'take'
      : before !== 'delayed' && after === 'delayed' ? 'add' : ''
    let lock = ''
    if (isTerminal(after) && after !== before) lock = keepFinished === 0 && after !== 'stale' ? 'drop' : 'retire'
    else if (before === 'stale' && (after === 'running' || after === 'delayed')) lock = 'take'
    const { jobName } = splitJobId(jobId)
    const pushKey = extra ? this.getQueueKey(record.lane) : this.getLogKey()
    const keys = [
      this.getLogKey(),
      this.getClaimingKey(),
      this.getLocksKey(),
      this.getDelayedKey(),
      pushKey,
      this.getJobLocksKey(jobName),
      this.getJobLanesKey(jobName),
    ]
    const args = [jobId, sha1Hex(expected), next, extra?.push ?? '', claiming, delayed, record.readyAt ?? 0, lock, keepFinished]
    return Number(await runScript(this.redis, TRANSITION_SCRIPT, keys, args))
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

  /** Inverse of {@link getQueueKey}: the lane label of a lane list key, `undefined` for a foreign key. */
  private laneLabelOfKey(key: string): string | undefined {
    if (key === this.getQueueKey()) return 'default'
    const prefix = `redisjm:${this.targetGroup}:lane:`
    return key.startsWith(prefix) && key.endsWith(':queue') && key.length > prefix.length + ':queue'.length
      ? key.slice(prefix.length, -':queue'.length)
      : undefined
  }

  /**
   * Display label for a lane, mirroring the default-lane rule in {@link getQueueKey}: the default
   * lane (`undefined` or `'default'`) reports as `'default'`; a named lane reports under its own name.
   */
  private laneLabel(lane?: string): string {
    return lane === undefined || lane === 'default' ? 'default' : lane
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
   * record. Backs the two-pass orphaned-lock reclaim in `performMaintenance` (locks stage), the same
   * way `suspectedAt` on a record backs the orphaned-queued reclaim.
   */
  private getSuspectsKey(): string {
    return `redisjm:${this.targetGroup}:suspects`
  }

  private getLogKey(): string {
    return `redisjm:${this.targetGroup}:log`
  }

  /**
   * Sorted set of popped-but-not-yet-claimed runs (member = jobId, score = pop time). The pop script
   * adds to it atomically with the LPOP; the claim removes it. An entry older than the stale threshold
   * is a run whose popping instance died or whose claim failed — maintenance puts it back on its lane.
   */
  private getClaimingKey(): string {
    return `redisjm:${this.targetGroup}:claiming`
  }

  /** Set of the job names that have (or recently had) runs in this group — maintenance's job index. */
  private getJobRegistryKey(): string {
    return `redisjm:${this.targetGroup}:jobs`
  }

  /**
   * Per-job set of the jobIds currently holding a run lock — `inFlight()` / `maxInFlight` read its size
   * in O(1). Kept in step with the global locks set by the scripts; drift self-heals (maintenance
   * prunes members whose lock is gone).
   */
  private getJobLocksKey(jobName: string): string {
    return `redisjm:${this.targetGroup}:jobs:${jobName}:locks`
  }

  /**
   * Per-job set of the lane list KEYS the job has (had) entries on. Every push registers its lane here
   * atomically; consumers also drain a job's old lanes (after its lane changed across a deploy), and
   * maintenance prunes lanes whose list is empty.
   */
  private getJobLanesKey(jobName: string): string {
    return `redisjm:${this.targetGroup}:jobs:${jobName}:lanes`
  }

  /** Sorted set of the live consumer instances: member = instanceId, score = lease expiry (Redis server ms). */
  private getInstancesKey(): string {
    return `redisjm:${this.targetGroup}:instances`
  }

  /** Hash instanceId → the instance's info JSON (capacity, lanes, label…), the companion of `instances`. */
  private getInstanceInfoKey(): string {
    return `redisjm:${this.targetGroup}:instance-info`
  }

  /** String key (with a TTL) marking that a pre-0.2 instance was recently seen in this group. */
  private getLegacySeenKey(): string {
    return `redisjm:${this.targetGroup}:legacy-seen`
  }

  /** String key holding the persisted log-scan cursor of maintenance (rotates passes across instances). */
  private getMaintenanceCursorKey(): string {
    return `redisjm:${this.targetGroup}:maintenance-cursor`
  }

  /** String key holding the persisted job-registry scan cursor of maintenance's job-upkeep stage. */
  private getMaintenanceJobsCursorKey(): string {
    return `redisjm:${this.targetGroup}:maintenance-jobs-cursor`
  }

  /** String key holding the persisted lock-scan cursor of maintenance's orphaned-lock stage. */
  private getMaintenanceLocksCursorKey(): string {
    return `redisjm:${this.targetGroup}:maintenance-locks-cursor`
  }

  /**
   * String key of the group-wide maintenance lock (see {@link runMaintenance}). Expires on its own;
   * never deleted, so its TTL is what spaces maintenance passes across instances.
   */
  private getMaintenanceLockKey(): string {
    return `redisjm:${this.targetGroup}:maintenance-lock`
  }

  /**
   * Sorted-set key holding delayed/scheduled runs: member = jobId, score = epoch-ms when the run
   * becomes ready. Group-wide (not per-lane); the record's persisted `lane` routes it when promoted.
   */
  private getDelayedKey(): string {
    return `redisjm:${this.targetGroup}:delayed`
  }
}
