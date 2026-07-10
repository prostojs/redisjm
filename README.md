# @prostojs/redisjm

Redis Job Manager for distributed job queues in Kubernetes-like multi-instance environments.

When running multiple instances of the same application, `@prostojs/redisjm` ensures that job runs are queued exactly once and picked up by only one instance. It uses Redis for queue management, distributed locking, heartbeat monitoring, and job lifecycle tracking.

## Features

- Redis-backed job queue with atomic distributed locking (Redis Set + `SADD`)
- Ensures only one job `runId` is scheduled — running, delayed, and stale jobs also block the queue
- Priority queue support (`queueFirst` for urgent jobs)
- Delayed runs and automatic retries with configurable backoff
- Execution fencing & self-heal — a staled-but-alive run resurrects on its next write (keeping its progress/attrs and lock); a genuinely superseded one can't clobber its successor's record
- Cooperative cancellation via `ctx.signal` (ownership loss or `stop({ abort: true })`)
- Per-instance concurrency (`concurrency` — N runs in flight per instance)
- Automatic heartbeat monitoring to detect stale/abandoned jobs
- Progress tracking (0-1) and custom attributes per job
- Event system (start, finish, error, retry, heartbeat, update) built on [hookable](https://github.com/unjs/hookable)
- Built-in maintenance job for stale detection, orphaned-lock reclaim, and log cleanup
- Polling-based job execution with `start` / awaitable `stop` (graceful drain or fast abort)
- Introspection — `stats()`, `queueSize()`, and terminal records observable for 60s by default
- Failures visible by default — handler errors are logged (configurable / silenceable)
- Rolling-deploy resilient — a job whose handler isn't registered yet is re-queued for a sibling instance instead of dropped
- Target group isolation — multiple app groups can share the same Redis instance
- Lanes — heterogeneous workers share one group, log, and maintenance loop while each instance pops only the lanes it can handle
- TypeScript with full generic type inference for job inputs and custom attributes

## Installation

```bash
pnpm add @prostojs/redisjm
```

## Quick Start

```typescript
import Redis from 'ioredis'
import { RedisJM } from '@prostojs/redisjm'

const redis = new Redis()
const manager = new RedisJM(redis, 'my-app')

// Create a job
const emailJob = manager.createJob(
  { jobName: 'send-email' },
  async (inputs: { to: string; subject: string }, ctx) => {
    await ctx.setProgress(0.5)
    // ... send email logic
    await ctx.setProgress(1)
  }
)

// Queue a job run
const queued = await emailJob.queue('daily-digest-2024-01-15', {
  to: 'user@example.com',
  subject: 'Daily Digest',
})
console.log(queued) // true if queued, false if already locked

// Start processing the queue
manager.start(1000) // poll every 1 second
```

## Redis Key Structure

Redis structures per target group:

| Key pattern | Redis type | Purpose |
|---|---|---|
| `redisjm:{tg}:queue` | List | Ordered queue of jobId strings for the default lane (RPUSH/LPUSH + LMPOP/LPOP) |
| `redisjm:{tg}:lane:{lane}:queue` | List | Ordered queue for a named lane (see [Lanes](#lanes)) |
| `redisjm:{tg}:locks` | Set | Locked jobIds (queued + delayed + running + stale). Atomic via SADD |
| `redisjm:{tg}:log` | Hash | jobId -> JSON record with full job state |
| `redisjm:{tg}:delayed` | Sorted Set | jobId -> ready-at epoch-ms; staged delayed runs and scheduled retries (see [Retries & delayed runs](#retries--delayed-runs)) |
| `redisjm:{tg}:suspects` | Hash | jobId -> first-seen epoch-ms; backs maintenance's two-pass orphaned-lock reclaim |

> A job with **no lane** keeps the exact legacy key `redisjm:{tg}:queue`; only a named lane gets its own `:lane:{lane}:queue` list. The locks set, log hash, delayed set, and suspects hash stay **group-wide** across all lanes — one lock namespace and a single monitoring pane for every worker type.

## API Reference

### `RedisJM`

The main job manager class. Uses Redis to manage job queues, locks, and the job log.

#### Constructor

```typescript
new RedisJM(redis: Redis, targetGroup: string, options?: RedisJMOptions)
```

- `redis` -- An [ioredis](https://github.com/redis/ioredis) client instance
- `targetGroup` -- A string prefix for all Redis keys; only clients sharing the same target group share queues and locks. Must not contain `:` (reserved for the lane-key infix)
- `options` -- Optional configuration:

| Option | Default | Description |
|---|---|---|
| `heartbeatInterval` | `5000` | Milliseconds between heartbeat updates during job execution |
| `roundsToStale` | `2` | Number of missed heartbeat intervals before a job is considered stale |
| `keepFinishedInterval` | `60000` | Milliseconds to keep finished/error/stale records in the log so `get()`/`list()` can observe the outcome. `0` opts into the legacy write-only behavior (record deleted the instant the job leaves `running`) — see the caveat under [Job Statuses](#job-statuses) |
| `maintenanceInterval` | `heartbeatInterval * roundsToStale` | Milliseconds between automatic maintenance enqueues while `start()` is polling (0 = disable auto-maintenance) |
| `unknownJobRequeueLimit` | `5` | Times a job whose name isn't registered on the popping instance is re-queued (lock held) for a sibling instance before being dropped as an error. `0` restores the legacy drop-on-first-pop behavior |
| `concurrency` | `1` | Max runs a single instance executes simultaneously. `1` is the serial default (one long run blocks the instance from popping anything, including `__maintenance`); set `> 1` for I/O-bound workloads. Must be a positive integer — the constructor floors it and throws `TypeError` if `< 1` or non-finite. See [Concurrency](#concurrency) |
| `laneStrategy` | `'roundRobin'` | How subscribed lanes are ordered each poll: `'roundRobin'` rotates the work-lane order to avoid starvation, `'priority'` uses `lanePriority`. See [Lanes](#lanes) |
| `lanePriority` | `[]` | Explicit high→low lane order used when `laneStrategy: 'priority'`; lanes not listed trail in registration order |
| `logger` | `console.error` | Sink `(message, error?) => void` for operational errors (handler throws, unknown/dropped jobs, poll-loop failures). Pass `false` to silence default logging |

#### Methods

##### `createJob<TInputs, TAttrs>(metadata, fn): Job<TInputs, TAttrs>`

Creates a new `Job` instance and registers it with this manager. Job names must be unique per manager.

```typescript
const job = manager.createJob(
  { jobName: 'process-order' },
  async (inputs: { orderId: string }, ctx) => {
    await ctx.setProgress(0.5)
    await ctx.setAttrs({ step: 'processing' })
    // ...
  }
)
```

Pass an optional `lane` in the metadata to route the job to a named sub-queue serviced only by consumers that register it — see [Lanes](#lanes). The metadata also accepts `attempts` and `backoff` to enable automatic retries — see [Retries & delayed runs](#retries--delayed-runs).

##### `queue<TInputs>(job, runId, inputs, options?): Promise<boolean>`

Adds a job run to the end of the queue. Returns `true` if successfully queued, `false` if the jobId is already locked (queued, delayed, running, or stale).

Pass `options.delay` (ms) to stage the run on the [delayed set](#retries--delayed-runs) instead of the live queue — it holds the lock (so dedupe still applies) and becomes poppable once the delay elapses. `delay` must be a finite number ≥ 0.

```typescript
const success = await manager.queue(job, 'order-123', { orderId: '123' })
await manager.queue(job, 'order-456', { orderId: '456' }, { delay: 5000 }) // poppable in ~5s
```

> **Dedupe is on the lock, not the log record.** `queue()` dedupes on `jobName#runId` in the locks set, which releases on finish/error (or when maintenance reclaims an orphan). Two consequences for integrators:
> - **Always check the boolean return.** `false` means "already locked" — a caller that ignores it reports false success.
> - **A reused `runId` can be swallowed by a stale/orphaned lock after a hard kill** until maintenance reclaims it (bounded by `maintenanceInterval`, or indefinitely if auto-maintenance is off). For idempotent *manual* triggers where you want a fresh run regardless, prefer a unique `runId` per trigger (e.g. append a timestamp). This does **not** apply to the maintenance job, which deliberately uses a stable empty `runId` to self-dedupe.

##### `queueFirst<TInputs>(job, runId, inputs, options?): Promise<boolean>`

Same as `queue` but adds to the front of the queue (priority insert). A priority insert cannot be delayed — passing `options.delay > 0` throws a `TypeError` (there is no "front" of a time-ordered delayed set).

```typescript
await manager.queueFirst(job, 'urgent-order', { orderId: '456' })
```

##### `isLocked(jobId): Promise<boolean>`

Checks whether a jobId currently holds a lock. The lock is held for the whole active lifecycle — `queued`, `delayed`, `running`, and `stale` — and released once the run reaches a terminal state (`finished`/`error`, or a reclaimed stale).

```typescript
const locked = await manager.isLocked('process-order#order-123')
```

##### `isQueued(jobId): Promise<boolean>` (deprecated)

Deprecated alias for `isLocked` — the name is misleading (it returns `true` for a lock held in **any** active state, not just `queued`). Prefer `isLocked`.

##### `stats(): Promise<RedisJMStats>`

Point-in-time snapshot for dashboards/introspection: per-lane queue depths, the delayed-set size, total held locks, and log-record counts by status.

```typescript
const { queues, delayed, locks, statuses } = await manager.stats()
```

> **Best-effort, not transactional.** All reads are independent (no cross-structure transaction), so a run mid-transition may be double- or un-counted for one poll. **Lane visibility is scoped:** `queues` reports the lanes this instance can name — the default lane, `__maintenance`, its registered jobs' lanes, plus any lane seen on a scanned log record. A lane with queue entries but no log records **and** no local registration is invisible here (nothing names it).

##### `queueSize(lane?): Promise<number>`

Queue depth (`LLEN`) of a single lane's queue list — the default lane when `lane` is omitted (or `'default'`). Delayed/scheduled runs are **not** on a lane list until promoted, so they aren't counted here — use `stats().delayed` for those.

```typescript
const pending = await manager.queueSize('images')
```

##### `list(): Promise<JobLogRecord[]>`

Returns all job log records (all statuses). A single corrupt/foreign hash field is skipped (and logged) rather than throwing.

```typescript
const records = await manager.list()
```

##### `get(jobId): Promise<JobLogRecord | undefined>`

Fetches a single record by `jobId` (`"jobName#runId"`), or `undefined` if absent — an O(1) `HGET` instead of scanning `list()`. Handy for "did my trigger finish?" endpoints.

```typescript
const record = await manager.get('process-order#order-123')
```

> With the default `keepFinishedInterval: 60000`, a terminal record lingers ~60s, so a run that just finished **is** observable here (status `finished`/`error`/`stale`). Opting into `keepFinishedInterval: 0` deletes the record the moment the job leaves `running`, and a finished run then returns `undefined` — `undefined` means "no record", **not** "never ran". Under `0`, wire the `finish`/`error` hooks to observe outcomes instead.

##### `unqueue(jobId): Promise<void>`

Removes a job from the queue, locks, and log entirely.

```typescript
await manager.unqueue('process-order#order-123')
```

##### `popAndExecute(): Promise<boolean>`

Pops the next job from the queue, matches it to a registered Job instance by name, and executes it. Returns `true` if a job was popped (even if execution failed), `false` if the queue was empty. Each pop first promotes any due [delayed](#retries--delayed-runs) runs onto their lane queues.

If the job name is not registered on this instance, it is re-queued (lock held) up to `unknownJobRequeueLimit` times so a sibling instance that *does* register the handler — e.g. a freshly deployed pod — can claim it. Once the budget is exhausted the record is set to status `"error"` with error `"Job name is unknown"` and the lock is released. A run that lost ownership of its record mid-execution throws `RunSupersededError`, which `popAndExecute` treats as a benign skip (it touches neither the lock nor the log — the new owner does). Handler failures, unknown-name drops, and missing-record drops are reported through the configured `logger`.

##### `start(interval): void`

Starts a polling loop that calls `popAndExecute()`. When a job is executed, the next poll fires immediately. When the queue is empty, waits `interval` ms before the next poll. Throws `TypeError` if `interval` is not a positive number.

With `concurrency > 1` the loop keeps up to `concurrency` runs in flight — it dispatches a popped run without awaiting it and pops again immediately, waiting only when all slots are full (see [Concurrency](#concurrency)).

Unless `maintenanceInterval` is `0`, `start()` also auto-wires maintenance: it first proactively reclaims a maintenance lock orphaned by a hard-killed instance (which would otherwise deadlock — maintenance can't reclaim its own lock), then enqueues the built-in maintenance job once immediately and every `maintenanceInterval` ms thereafter. All instances enqueue concurrently — the lock ensures only one maintenance run executes at a time.

```typescript
manager.start(1000) // poll every 1 second when idle
```

##### `stop(options?): Promise<void>`

Stops the polling loop and the auto-maintenance timer, and resolves once **all** in-flight runs have settled — so a shutdown handler can `await` a drain before exiting. Two modes:

- **Graceful (default)** — `stop()` stops popping new work and awaits the runs already in flight to finish on their own.
- **Fast (`stop({ abort: true })`)** — additionally aborts every in-flight run's `ctx.signal` (reason `'manager stopped'`) so cooperative handlers can bail out of wasted work early. Abort is cooperative: nothing forcibly kills a handler; `stop()` still awaits every run to settle. See [Cancellation](#cancellation).

For locks to release cleanly, prefer graceful shutdown (`stop()` on `SIGTERM`); a hard `SIGKILL` leaves orphaned locks that maintenance reclaims after the stale threshold.

```typescript
process.on('SIGTERM', async () => { await manager.stop() })              // graceful drain
process.on('SIGINT',  async () => { await manager.stop({ abort: true }) }) // fast abort-and-drain
```

##### `performMaintenance(): Promise<MaintenanceResult>`

Scans the job log and:
1. Marks running jobs as `"stale"` if `now - lastHeartbeat > heartbeatInterval * roundsToStale`, removes their lock.
2. Marks orphaned `queued`/`delayed` jobs as `"stale"` and removes their lock — a `queued` record no longer on its lane queue (or a `delayed` record no longer on the delayed set) was popped/promoted by an instance that died before the `start` event fired. Detection is two-pass to avoid racing the normal pop→start window: the first scan stamps `suspectedAt` on the record; a later scan reclaims it if it is still orphaned after the stale threshold.
3. Removes finished/error/stale log records older than `keepFinishedInterval`, and deletes unparseable/foreign records (garbage that retention would otherwise hoard forever).
4. Reclaims **orphaned locks** — a locks-set member with no backing log record (a lock left behind when an `enqueue` crashed between its `SADD` and `HSET`). Same two-pass suspicion (via the `suspects` hash) so an in-flight enqueue isn't mistaken for a permanent orphan.

Returns `{ staleCount, cleanedCount }` (orphaned locks count toward `staleCount`; unparseable records toward `cleanedCount`). Both the log and locks set are scanned incrementally (`HSCAN`/`SSCAN`), not with a single blocking `HGETALL`/`SMEMBERS`.

##### `registerJob(job): void`

Registers a `Job` instance by name (must be unique). Hooks on all events to update Redis and re-dispatch.

##### `unregisterJob(job): void`

Unregisters a `Job` and removes all event hooks.

##### `getTargetGroup(): string`

Returns the target group identifier.

##### `getOptions(): ResolvedRedisJMOptions`

Returns a copy of the resolved options with defaults applied.

---

### `Job<TInputs, TAttrs>`

Represents a named job with a function and event hooks. Extends [Hookable](https://github.com/unjs/hookable).

- `TInputs` -- Type for job inputs (must be JSON-serializable)
- `TAttrs` -- Type for custom attributes, extends `Record<string, string | number | boolean | null | undefined>`

#### Constructor

```typescript
new Job<TInputs, TAttrs>(metadata: JobMetadata, fn: JobFunction<TInputs, TAttrs>, manager?: RedisJM)
```

#### Job Function Signature

```typescript
type JobFunction<TInputs, TAttrs> = (
  inputs: TInputs,
  ctx: {
    setProgress: (progress: number) => Promise<void>  // finite; clamped to 0–1
    setAttrs: (attrs: TAttrs) => Promise<void>         // merges into existing attrs
    signal: AbortSignal                                // cooperative cancellation
  }
) => void | Promise<void>
```

- `setProgress` throws a `TypeError` on a non-finite value and clamps the result into `[0, 1]`.
- `setAttrs` **merges** into the record's existing attributes (successive calls accumulate keys); it does not replace them.
- `signal` aborts on ownership loss or `stop({ abort: true })` — see [Cancellation](#cancellation).

#### Methods

##### `execute(inputs, options?): Promise<void>`

Runs the job function with heartbeat timer and context callbacks. Options:

```typescript
interface JobExecuteOptions {
  targetGroup?: string
  heartbeatInterval?: number  // enables automatic heartbeat events
  runId?: string              // explicit runId (otherwise derived from inputs)
  manager?: RedisJM           // driving manager, stamped on every event payload
  logger?: RedisJMLogger      // sink for infra errors (failed heartbeat write, throwing error hook)
  signal?: AbortSignal        // external abort plumbed into ctx.signal (e.g. shutdown)
}
```

`manager`, `logger`, and `signal` are wired automatically when a run is dispatched by `start()`/`popAndExecute()`; you set them only when calling `execute()` directly.

##### `queue(runId, inputs, manager?, options?): Promise<boolean>`

Convenience method -- delegates to `RedisJM.queue`. Pass `options.delay` to stage the run on the delayed set (see [Retries & delayed runs](#retries--delayed-runs)).

```typescript
await job.queue('order-456', { orderId: '456' }, undefined, { delay: 5000 })
```

##### `queueFirst(runId, inputs, manager?, options?): Promise<boolean>`

Convenience method -- delegates to `RedisJM.queueFirst` (priority insert). A priority insert cannot be delayed (`delay > 0` throws).

##### `getJobId(runId): string`

Returns `"jobName#runId"`.

##### `getMetadata(): JobMetadata` / `getName(): string` / `getLane(): string | undefined`

---

### `createMaintenanceJob(manager): Job<null, never>`

Factory function that creates and registers a pre-defined maintenance job. When executed, it calls `manager.performMaintenance()` to detect stale jobs and clean up expired log records.

> **Note:** `manager.start()` wires maintenance automatically (see the `maintenanceInterval` option). Manual wiring as shown below is only needed when auto-maintenance is disabled (`maintenanceInterval: 0`) or when maintenance must run on a separate schedule. `start()` reuses a maintenance job you registered yourself.

Maintenance is idempotent -- each run scans the full log regardless of prior state. Use an empty `runId` (`''`) so that at most one maintenance job is queued or running at any time. The lock is released on completion, allowing the next `queue` call to succeed. There is no need for time-based runIds.

```typescript
import { createMaintenanceJob } from '@prostojs/redisjm'

const maintenanceJob = createMaintenanceJob(manager)

// Periodically try to queue maintenance (all instances, only one succeeds)
setInterval(() => maintenanceJob.queue('', null), 30000)
```

## Events

Both `Job` and `RedisJM` emit events via [hookable](https://github.com/unjs/hookable). Every payload also carries `executionId` (the per-run fencing token), `abort(reason?)` (a cooperative kill-switch for this run — see [Cancellation](#cancellation)), and `manager` (the driving `RedisJM`, absent for a direct `job.execute()`):

| Event | Extra payload fields | Emitted by | Description |
|---|---|---|---|
| `start` | — | Job + manager | Before job function runs |
| `finish` | — | Job + manager | After successful completion |
| `error` | `error` | Job + manager | See retry semantics below |
| `retry` | `error, attempt, nextAttemptAt` | **manager only** | A failed attempt was scheduled for retry |
| `heartbeat` | — | Job + manager | Periodic heartbeat tick |
| `update` | `progress?, attrs?` | Job + manager | On setProgress/setAttrs |

`RedisJM` re-dispatches events only when `targetGroup` matches (and, when a `manager` is stamped, only on the manager that drove the run) and updates the Redis log accordingly.

**`error` vs `retry` (retry semantics):**

- The **job-level** `error` hook (`job.hook('error', …)`) fires on **every** failed attempt, including ones that will be retried.
- The **manager-level** `error` event (`manager.hook('error', …)`) fires only on **final** failure — the last attempt exhausted the `attempts` budget.
- The **manager-level** `retry` event fires once per scheduled retry in between (payload adds `error`, the 1-based `attempt` that failed, and `nextAttemptAt` — epoch-ms the retry becomes poppable). See [Retries & delayed runs](#retries--delayed-runs).

Only a **job-function** failure dispatches `error`. A throwing `start` or `finish` hook is infrastructure, not a job outcome — it propagates without flipping the record's status or firing `error`.

> `setProgress` and `setAttrs` each emit a **separate** `update` event carrying only its own field — a single `update` payload never contains both `progress` and `attrs`.

```typescript
manager.hook('start', (payload) => {
  console.log(`Job ${payload.job.getName()} started`)
})

// Optional: custom handling. Job errors are ALSO logged by the default `logger`
// (set `logger: false` to suppress that if you handle errors yourself here).
manager.hook('error', (payload) => {
  console.error(`Job failed:`, payload.error)
})
```

### Observability & error handling

- **Failures are visible by default.** A thrown handler is recorded in the log (`status: 'error'`), re-broadcast on the `error` event, **and** written to the `logger` (default `console.error`, including the stack). Unknown-name drops, missing-record drops, and poll-loop errors also go to the `logger`. Pass `logger: false` to silence, or a custom function to redirect.
- **Don't blanket-`DEL` the Redis keys.** The `:queue`, `:lane:*:queue`, `:locks`, `:log`, `:delayed`, and `:suspects` keys are shared by **every** job in the target group — deleting one wipes all runs in the group, not just yours. Use `unqueue(jobId)` to remove a single run (it clears its queue/delayed/lock/log/suspect entries).

### Long-running handlers & heartbeats

Heartbeats are emitted from a timer on the single-threaded event loop. A long, tightly synchronous handler that never `await`s starves that timer, so heartbeats stop landing and maintenance may mark the run `stale` and release its lock — opening a window in which another instance can pick up the **same** `runId` while the original handler is still running.

**Self-heal (the common case).** Staleness detection can't tell a dead handler from one that merely pinned its event loop, so recovery is symmetric: the moment the handler breathes again — its next heartbeat, `setProgress`, or `setAttrs` — the run **resurrects** its own record (`stale` → `running`), clears the maintenance-stamped `finishedAt`, and **re-acquires its lock**. It keeps its progress/attrs stream and re-establishes dedupe, so its eventual `finish`/`error` records the truth instead of a frozen mid-run snapshot. To an observer the `stale` badge was transient, not a point of no return.

**Zombie fencing (a successor already took over).** If, during the released-lock window, a producer re-enqueued the `runId` and a successor claimed it, the original ("zombie") run is genuinely superseded: its `executionId` no longer matches the record, so its finish/error/heartbeat/update writes are all **rejected** (it can't clobber, clean, or resurrect the successor's record), and its `ctx.signal` **aborts** within one `heartbeatInterval` (the heartbeat hook's guarded write detects the ownership loss). But cancellation is cooperative — the zombie only stops if the handler observes `signal`. A handler that ignores the signal keeps burning CPU and, worse, keeps performing its own **external side effects** (writes to other systems) concurrently with the successor. Chunk long work, `await` between chunks so heartbeats fire, and check `ctx.signal` at each checkpoint:

```typescript
manager.createJob({ jobName: 'backfill' }, async (inputs: { ids: string[] }, ctx) => {
  for (let i = 0; i < inputs.ids.length; i += 100) {
    if (ctx.signal.aborted) return                     // lost ownership → stop side effects
    await processChunk(inputs.ids.slice(i, i + 100))   // yields to the loop → heartbeat lands
    await ctx.setProgress(Math.min(1, (i + 100) / inputs.ids.length))
  }
})
```

> **Clock-skew note.** Staleness compares a record's `heartbeat`/`suspectedAt` timestamp (written by one machine) against `Date.now()` on the machine running maintenance. If instance clocks drift, the effective stale threshold shifts by the skew — keep NTP sane across the fleet.

## Retries & delayed runs

A run can be **staged for the future** two ways, both backed by the same `redisjm:{tg}:delayed` sorted set (member = jobId, score = ready-at epoch-ms):

- **Delayed enqueue** — `queue(job, runId, inputs, { delay })` (or `job.queue(runId, inputs, manager?, { delay })`) stores the record as `delayed`, **holding the lock** (so dedupe still applies while it waits) and schedules it on the delayed set. It becomes poppable once `delay` ms elapse. `queueFirst` cannot be delayed (`delay > 0` throws).
- **Automatic retries** — set `attempts` (and optionally `backoff`) on the job metadata.

### Retries

```typescript
const job = manager.createJob(
  {
    jobName: 'charge-card',
    attempts: 4,                          // total tries incl. the first (default 1 = no retries)
    backoff: (attempt) => attempt * 2000, // ms before the retry after the N-th failed attempt
  },
  async (inputs, ctx) => { /* ... */ },
)
```

- `attempts` — total number of attempts including the first; must be a positive integer, floored and clamped to ≥ 1. Default `1` means a single failed attempt is terminal (no retries).
- `backoff` — ms before the next retry, as a fixed number **or** a function of the just-failed 1-based attempt (e.g. exponential backoff). Negative/non-finite results are clamped to `0`. Default `0` re-queues immediately (still routed through the delayed set).

When an attempt below the budget fails, the run goes **back through the delayed set with its lock still held** (so no duplicate enqueue of the same `runId` can slip in during the backoff), and the manager fires the **`retry`** event (payload: `error`, `attempt`, `nextAttemptAt`). The final failure keeps the normal terminal path: status `error`, lock released, manager `error` event. Note the job-level `error` hook fires on *every* attempt; the manager-level `error` only on the last (see [retry semantics](#events)).

### Promotion

Each polling instance sweeps due delayed entries onto their lane queues on every pop, rate-limited to at most once per second and at most 8 ids per pass. A delayed run therefore becomes poppable within ~1s of its ready time — fine for the coarse delay/backoff granularity this targets, but don't rely on delayed runs firing to sub-second precision. Use `stats().delayed` to see how many runs are currently staged.

## Concurrency

By default (`concurrency: 1`) an instance runs **one** job at a time — a long run blocks it from popping anything else, including the `__maintenance` lane. Set `concurrency > 1` for I/O-bound workloads that can keep several runs in flight:

```typescript
const manager = new RedisJM(redis, 'my-app', { concurrency: 8 })
```

The poll loop then dispatches a popped run **without awaiting it** and immediately pops again, keeping up to `concurrency` runs in flight; it waits only when all slots are full. `stop()` drains **all** in-flight runs (including one being popped at that instant). `concurrency` must be a positive integer — the constructor floors a fractional value and throws `TypeError` on `< 1` or non-finite. Concurrency is per **instance**; the distributed lock still guarantees each `runId` runs on only one instance at a time.

## Cancellation

Every execution's `ctx.signal` is an `AbortSignal` that aborts when the run should stop wasting work:

- **Ownership loss** — the run was superseded by a re-enqueue of the same `runId`, reached a terminal state, or was unqueued. Detected via the heartbeat hook's guarded write, so it fires within one `heartbeatInterval`. (A run merely staled by maintenance but still owned by this execution **self-heals** instead of aborting — see [Long-running handlers](#long-running-handlers--heartbeats).)
- **Shutdown** — `stop({ abort: true })` aborts every in-flight run (reason `'manager stopped'`).

Cancellation is **cooperative** — nothing forcibly kills a handler. Check `signal.aborted` (or listen for `'abort'`) at natural checkpoints; `signal.reason` carries a short string cause. A handler that never checks the signal runs to completion, but its record writes are fenced out (see [Long-running handlers](#long-running-handlers--heartbeats)) — the danger is only its **external side effects**.

```typescript
manager.createJob({ jobName: 'export' }, async (inputs: { rows: Row[] }, ctx) => {
  for (const row of inputs.rows) {
    if (ctx.signal.aborted) return          // stop early — cooperative
    await writeRow(row)                      // external side effect worth avoiding after abort
  }
})
```

User hooks can also trigger this signal via `payload.abort(reason?)` as a custom kill-switch.

## Job Statuses

| Status | Description | Blocks queue? | In log? |
|---|---|---|---|
| `queued` | Waiting on a lane queue | Yes | Yes |
| `delayed` | Staged on the delayed set (scheduled run or pending retry), lock held | Yes | Yes |
| `running` | Currently executing with active heartbeat | Yes | Yes |
| `stale` | Heartbeat expired, detected by maintenance (a still-alive run self-heals back to `running` on its next heartbeat/`setProgress`/`setAttrs`) | Yes (until maintenance cleans it) | Yes |
| `finished` | Completed successfully | No | Kept for `keepFinishedInterval` |
| `error` | Failed with an error (final failure) | No | Kept for `keepFinishedInterval` |

> **Terminal states are observable by default.** With the default `keepFinishedInterval: 60000`, `finished`/`error`/`stale` records linger ~60s, so `list()`/`get()` return them for a minute after the run settles. Set `keepFinishedInterval: 0` to opt into the legacy **write-only** behavior — records are deleted the instant they're set, `list()`/`get()` then only ever return `queued`/`delayed`/`running`, and terminal outcomes are observable only via the `finish`/`error` hooks.

## Lanes

A **lane** is a named sub-queue within a target group. The group keeps **one** shared log, **one** lock set, and **one** maintenance loop — only the poppable work list splits by lane. This lets heterogeneous workers share a single job system while each instance pops only the work it can handle: e.g. light web pods and a dedicated heavy image worker enqueuing and monitoring in one place, with no web pod ever popping the heavy image job.

Jobs with **no lane** use the **default lane**, which maps to the exact legacy queue key `redisjm:<group>:queue`. Lanes are fully opt-in — code that never declares a lane behaves exactly as before.

### Declaring a lane

Pass an optional `lane` on the job metadata:

```typescript
const storeImages = manager.createJob(
  { jobName: 'store-images', lane: 'images' },
  async (inputs: { url: string }, ctx) => {
    // fetch → resize → upload
  }
)
```

Lane names must match `^[A-Za-z0-9_-]+$` and must not start with `__` (reserved). The target group must not contain `:` (reserved for the lane-key infix).

### Auto-subscription

A consumer that calls `start()` polls exactly the **union of its registered jobs' lanes** (plus the reserved `__maintenance` lane) — there is no separate subscription config. A dedicated worker that registers only `store-images` services only the `images` lane and never pops default-lane work:

```typescript
const worker = new RedisJM(redis, 'portal')
worker.createJob({ jobName: 'store-images', lane: 'images' }, storeImagesFn)
worker.start(1000) // polls `images` (+ `__maintenance`) — never the default lane
```

### Produce vs. consume

**Producing a job needs only a `Job` instance and the manager — no registration.** Registration is what auto-subscribes a consumer to a lane. So a pod that must **enqueue** a laned job but must **not run** it (e.g. a web pod that also `start()`s to consume its own default-lane jobs) must **not** register that job — registering it would subscribe the pod to the lane, and it would then pop the heavy work.

To produce without consuming, construct an **unregistered** `Job` and queue through it:

```typescript
import { RedisJM, Job } from '@prostojs/redisjm'

const manager = new RedisJM(redis, 'portal')
manager.createJob({ jobName: 'render-page' }, renderFn) // default-lane job this web pod consumes
manager.start(1000)

// Enqueue image work WITHOUT registering it, so this pod never pops the heavy handler:
const imageJob = new Job<{ url: string }>(
  { jobName: 'store-images', lane: 'images' },
  async () => {}, // never runs on this pod
  manager,
)
await imageJob.queue('img-42', { url: 'https://example.com/img.png' })
```

Rule of thumb: **register to consume, construct-without-register to only produce.**

### Lane ordering (`laneStrategy`)

Each poll checks lanes in order and takes the first available job. The reserved `__maintenance` lane is always polled first; the work lanes are ordered by `laneStrategy`:

- `'roundRobin'` (default) — rotates the work-lane order each poll, so no lane is starved by a busier one.
- `'priority'` — uses `lanePriority`, an explicit high→low list of lane names. Work lanes absent from the list trail in registration order.

```typescript
new RedisJM(redis, 'portal', {
  laneStrategy: 'priority',
  lanePriority: ['images', 'thumbnails'],
})
```

### One lane per handler set

Lanes isolate at **lane** granularity, not per-jobName. If two consumers share a lane but register disjoint job names, each keeps popping names it can't handle and requeues them — reintroducing wasted contention within that lane. The supported pattern is **one lane per disjoint handler set**: every subscriber of a lane should register every jobName on that lane.

### Redis requirement

Lane polling uses `LMPOP`, which requires **Redis ≥ 7**. On older servers the library automatically falls back to sequential `LPOP` with the same routing and ordering, so lanes stay usable everywhere.

### Adopting lanes on an existing group

Roll the lane-aware version out to **every** instance in the group **before** any producer starts declaring non-default lanes. A pre-lane instance's maintenance loop mishandles laned records — it looks for them on the legacy queue and can wrongly mark them stale — so no laned work should exist until the whole group is lane-aware. Rollback is the mirror image: stop emitting laned work and let the lanes drain before rolling back to a lane-unaware binary.

## Full Example: Distributed Job Processing

```typescript
import Redis from 'ioredis'
import { RedisJM, createMaintenanceJob } from '@prostojs/redisjm'

const redis = new Redis(process.env.REDIS_URL)
const manager = new RedisJM(redis, 'my-service', {
  heartbeatInterval: 5000,
  roundsToStale: 2,
  keepFinishedInterval: 60000,
})

// Define jobs
const reportJob = manager.createJob(
  { jobName: 'daily-report' },
  async (inputs: { date: string }, ctx) => {
    await ctx.setAttrs({ step: 'fetching data' })
    await ctx.setProgress(0.3)
    // ... fetch data

    await ctx.setAttrs({ step: 'generating report' })
    await ctx.setProgress(0.7)
    // ... generate report

    await ctx.setProgress(1)
  }
)

// Maintenance job (auto-registered with manager)
const maintenanceJob = createMaintenanceJob(manager)

// Start processing the queue (one job at a time per instance)
manager.start(1000)

// CRON handler (runs on every instance)
async function onCron() {
  const today = new Date().toISOString().slice(0, 10)
  await reportJob.queue(today, { date: today })

  // Schedule maintenance -- empty runId ensures at most one in the queue
  await maintenanceJob.queue('', null)
}

// Graceful shutdown — await the drain so the in-flight job finishes and its lock releases
process.on('SIGTERM', async () => {
  await manager.stop()
  await redis.quit()
})
```

## Using a Job with Multiple Managers

A single `Job` instance can be attached to different `RedisJM` instances:

```typescript
const managerA = new RedisJM(redis, 'group-a')
const managerB = new RedisJM(redis, 'group-b')

const job = managerA.createJob(
  { jobName: 'sync-data' },
  async (inputs: { source: string }, ctx) => { /* ... */ }
)

// Also register with second manager
managerB.registerJob(job)

// Queue on specific manager
await job.queue('run-1', { source: 'api' }, managerB)
```

Each event payload carries the `manager` that drove the execution, so when two managers in one process share a Job (and even a target group), only the driving manager's hooks act on a given run's events — no double writes or double-dispatched events.

## Types

```typescript
type JobAttrValue = string | number | boolean | null | undefined
type JobAttrs = Record<string, JobAttrValue>
type JobStatus = 'queued' | 'running' | 'finished' | 'error' | 'stale' | 'delayed'
type LaneStrategy = 'roundRobin' | 'priority'

interface JobMetadata {
  jobName: string
  description?: string
  lane?: string                    // named sub-queue; omitted → default (legacy) lane
  attempts?: number                // total tries incl. first; default 1 (no retries), floored/clamped ≥ 1
  backoff?: number | ((attempt: number) => number)  // ms before retry; default 0; negatives → 0
}

type RedisJMLogger = (message: string, error?: Error) => void

interface QueueOptions {
  delay?: number                   // ms to stage on the delayed set; ≥ 0; incompatible with queueFirst
}

interface RedisJMOptions {
  heartbeatInterval?: number       // default 5000
  roundsToStale?: number           // default 2
  keepFinishedInterval?: number    // default 60000; 0 = legacy write-only (drop on leaving running)
  maintenanceInterval?: number     // default heartbeatInterval * roundsToStale; 0 disables
  unknownJobRequeueLimit?: number  // default 5; 0 = drop unknown names immediately
  concurrency?: number             // default 1; max simultaneous runs per instance; integer ≥ 1
  laneStrategy?: LaneStrategy      // default 'roundRobin'
  lanePriority?: string[]          // default []; high→low lane order for 'priority'
  logger?: RedisJMLogger | false   // default console.error; false to silence
}

interface StopOptions {
  abort?: boolean                  // default false; true aborts every in-flight ctx.signal before draining
}

// Resolved options (returned by getOptions()); every field above except `logger` with defaults applied.
interface ResolvedRedisJMOptions {
  heartbeatInterval: number
  roundsToStale: number
  keepFinishedInterval: number
  maintenanceInterval: number
  unknownJobRequeueLimit: number
  laneStrategy: LaneStrategy
  lanePriority: string[]
  concurrency: number
}

interface JobLogRecord<TInputs = unknown, TAttrs extends JobAttrs = JobAttrs> {
  jobId: string
  jobName: string
  runId: string
  inputs: TInputs
  targetGroup: string
  lane?: string                 // lane the run was enqueued on (persisted for maintenance/unqueue)
  status: JobStatus
  startedAt?: number
  finishedAt?: number
  heartbeat?: number
  progress: number
  attrs?: TAttrs
  error?: string
  executionId?: string          // fencing token stamped by the run that claimed this record
  attempt?: number              // 1-based count of executions that have claimed this run
  readyAt?: number              // epoch-ms a `delayed` run becomes poppable (present only while delayed)
  suspectedAt?: number          // orphaned-queued/delayed suspect timestamp (maintenance)
  requeueCount?: number         // times re-queued for an unknown job name
}

interface JobContext<TAttrs extends JobAttrs = JobAttrs> {
  setProgress: (progress: number) => Promise<void>  // throws on non-finite; clamps to [0,1]
  setAttrs: (attrs: TAttrs) => Promise<void>         // merges into existing attrs
  signal: AbortSignal                                // cooperative cancellation (ownership loss / stop abort)
}

interface JobExecuteOptions {
  targetGroup?: string
  heartbeatInterval?: number
  runId?: string
  manager?: RedisJM
  logger?: RedisJMLogger
  signal?: AbortSignal
}

type JobFunction<TInputs, TAttrs extends JobAttrs> = (
  inputs: TInputs,
  ctx: JobContext<TAttrs>,
) => void | Promise<void>

// Event payloads. All events carry these; `error`/`retry`/`update` extend them.
interface JobEventPayload<TInputs = unknown> {
  job: Job<TInputs, any>
  targetGroup: string
  runId: string
  inputs: TInputs
  executionId: string
  manager?: RedisJM
  abort: (reason?: string) => void
}
interface JobErrorEventPayload<TInputs = unknown> extends JobEventPayload<TInputs> { error: Error }
interface JobRetryEventPayload<TInputs = unknown> extends JobEventPayload<TInputs> {
  error: Error
  attempt: number        // 1-based attempt that just failed
  nextAttemptAt: number  // epoch-ms the retry becomes poppable
}
interface JobUpdateEventPayload<TInputs = unknown, TAttrs extends JobAttrs = JobAttrs>
  extends JobEventPayload<TInputs> { progress?: number; attrs?: TAttrs }

interface RedisJMStats {
  queues: Record<string, number>          // queue depth per lane ('default' for the legacy queue)
  delayed: number                          // runs staged on the delayed set
  locks: number                            // total held locks (queued + delayed + running + stale)
  statuses: Record<JobStatus, number>      // log-record counts by status
}

interface MaintenanceResult {
  staleCount: number    // stale-reclaimed: lapsed running + orphaned queued/delayed + orphaned locks
  cleanedCount: number  // removed: expired terminal records + unparseable garbage
}
```

Exported classes and values: `RedisJM`, `Job`, `RunSupersededError` (thrown out of a superseded execution; `popAndExecute` skips it), `createMaintenanceJob`, `MAINTENANCE_JOB_NAME`, and the reserved-lane constant `MAINTENANCE_LANE` (`'__maintenance'`). Exported types include `JobHooks`, `RedisJMHooks`, and all of the above.

## License

MIT
