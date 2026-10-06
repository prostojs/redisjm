# @prostojs/redisjm

Redis Job Manager for distributed job queues in Kubernetes-like multi-instance environments.

When running multiple instances of the same application, `@prostojs/redisjm` ensures that job runs are queued exactly once and picked up by only one instance. It uses Redis for queue management, distributed locking, heartbeat monitoring, and job lifecycle tracking.

## Features

- Redis-backed job queue with atomic enqueue and pop (server-side Lua scripts) and distributed run locks
- Ensures only one job `runId` is in flight — a queued, delayed, or running run blocks a duplicate enqueue
- Explicit enqueue outcomes — `enqueue()` / `enqueueMany()` report `queued` / `deduped` / `busy` / `full`; Redis failures throw a typed `RedisJMEnqueueError` instead of looking like a dedupe
- Priority queue support (`queueFirst` / `enqueue(…, { first: true })` for urgent jobs)
- Delayed runs and automatic retries with configurable backoff
- Execution timeouts (`jobTimeout` / `timeoutMs`) and a hung-handler backstop (`maxRunMs`)
- Backpressure — per-lane queue caps (`maxQueued` / `laneCaps`) and per-job in-flight limits (`maxInFlight`)
- Execution fencing & self-heal — a staled-but-alive run resurrects on its next write (keeping its progress/attrs and lock); a genuinely superseded one can't clobber its successor's record
- Cooperative cancellation via `ctx.signal` (ownership loss, timeout, or `stop({ abort: true })`) — and, with `abortGraceMs`, a handler that ignores an abort is abandoned after a grace period (`JobAbortedError`) so it can't hold a slot or a shutdown hostage
- Per-instance concurrency (`concurrency`) and per-lane concurrency (`laneConcurrency`)
- Automatic heartbeat monitoring to detect stale/abandoned jobs
- Progress tracking (0-1) and custom attributes per job
- Event system (start, finish, error, retry, timeout, heartbeat, update, enqueueFailed, startFailed, memoryPressure, maintenance) built on [hookable](https://github.com/unjs/hookable)
- Maintenance on its own timer — bounded, lock-guarded, compare-and-set, and still able to free memory when Redis is full
- Memory guardrails — eviction-policy check, `health()` snapshot, `memoryPressure` alarm, per-record history TTL (Redis ≥ 7.4), inputs-size limit
- Polling-based job execution with `start` / awaitable `stop` (graceful drain or fast abort) and `wake()` for push-style doorbells
- Introspection — `stats()`, `health()`, `inFlight()` / `inFlightCount()`, `listPage()`, `queueSize()`, and terminal records observable for 60s by default
- Queue listing in pop order (`listQueued()`) and batch record reads (`getMany()`) — no private key names needed
- Worker fleet registry — `fleet()` reports the live consumer instances and their capacity (`presence`, `instanceLabel`)
- Failures visible by default — handler errors, timeouts, and start failures are logged (configurable / silenceable)
- Rolling-deploy resilient — a job whose handler isn't registered yet is re-queued for a sibling instance instead of dropped; queued runs survive a job's lane change
- Target group isolation — multiple app groups can share the same Redis instance
- Lanes — heterogeneous workers share one group, log, and maintenance loop while each instance pops only the lanes it can handle
- TypeScript with full generic type inference for job inputs and custom attributes

## Requirements

| Requirement | Why |
|---|---|
| **Redis ≥ 7.0** | Enqueue, pop, and every record transition are `#!lua` shebang scripts (`EVALSHA` with an `EVAL` fallback). A flag-less shebang script is refused *up front* when Redis is at `maxmemory`, which is what guarantees that a full Redis neither half-writes an enqueue nor pops (and loses) a run. Older servers are not supported. |
| **Redis ≥ 7.4** (recommended) | Terminal records get a hash-field TTL (`HPEXPIRE`), so history expires even when maintenance can't run. Below 7.4 this is silently skipped and maintenance's sweep reclaims expired records instead. |
| **No Redis Cluster** | Keys are not hash-tagged and the scripts touch several keys of a group (one of them accesses lane keys it reads from a set instead of declaring them), so they can't be routed to one slot. Use a single primary (replication / failover is fine). The 0.3 scripts (fleet presence, queue listing) keep this constraint. |
| **`maxmemory-policy noeviction`** | `allkeys-*` policies may evict queue, lock, and log keys under pressure → lost or duplicated jobs. `volatile-*` policies can't free redisjm keys (they carry no key TTL), so Redis ends up refusing writes anyway while evicting your other volatile keys. `start()` reads the policy from `INFO memory` and logs a warning for both. See [Memory & eviction](#memory--eviction). |

The client is [ioredis](https://github.com/redis/ioredis) (v5).

## Installation

```bash
pnpm add @prostojs/redisjm
```

## Quick Start

```typescript
import Redis from 'ioredis'
import { RedisJM, RedisJMEnqueueError } from '@prostojs/redisjm'

const redis = new Redis()
const manager = new RedisJM(redis, 'my-app', {
  jobTimeout: 5 * 60_000, // no attempt may hold a slot longer than 5 minutes
})

// Create a job
const emailJob = manager.createJob(
  { jobName: 'send-email' },
  async (inputs: { to: string; subject: string }, ctx) => {
    await ctx.setProgress(0.5)
    // ... send email logic (pass ctx.signal to cancellable I/O)
    await ctx.setProgress(1)
  }
)

// Enqueue a job run and look at what happened
try {
  const { status } = await emailJob.enqueue('daily-digest-2024-01-15', {
    to: 'user@example.com',
    subject: 'Daily Digest',
  })
  console.log(status) // 'queued' | 'deduped' (already in flight) | 'busy' | 'full'
} catch (err) {
  if (err instanceof RedisJMEnqueueError) {
    console.error(`enqueue failed (${err.reason})`) // Redis refused/failed — NOT a duplicate
  }
  throw err
}

// Start processing the queue (also starts maintenance on its own timer)
manager.start(1000) // poll every 1 second when idle

process.on('SIGTERM', async () => {
  await manager.stop()
  await redis.quit()
})
```

## Redis Key Structure

Redis structures per target group:

| Key pattern | Redis type | Purpose |
|---|---|---|
| `redisjm:{tg}:queue` | List | Ordered queue of jobIds for the default lane (pushed by the enqueue script, popped by the pop script) |
| `redisjm:{tg}:lane:{lane}:queue` | List | Ordered queue for a named lane (see [Lanes](#lanes)) |
| `redisjm:{tg}:locks` | Set | jobIds holding a run lock (queued + delayed + running, including popped-not-yet-claimed) |
| `redisjm:{tg}:log` | Hash | jobId -> JSON record with full job state (terminal fields get a TTL on Redis ≥ 7.4) |
| `redisjm:{tg}:delayed` | Sorted Set | jobId -> ready-at epoch-ms; staged delayed runs and scheduled retries (see [Retries & delayed runs](#retries--delayed-runs)) |
| `redisjm:{tg}:claiming` | Sorted Set | jobId -> pop epoch-ms; runs popped but not yet claimed by their executor. Maintenance re-queues entries older than the stale threshold |
| `redisjm:{tg}:suspects` | Hash | jobId -> first-seen epoch-ms; backs maintenance's two-pass orphaned-lock reclaim |
| `redisjm:{tg}:jobs` | Set | Job names that have (or recently had) runs in the group — maintenance's index of per-job sets |
| `redisjm:{tg}:jobs:{jobName}:locks` | Set | This job's jobIds holding a lock — backs `inFlight()` and `maxInFlight` |
| `redisjm:{tg}:jobs:{jobName}:lanes` | Set | Lane list keys (`redisjm:{tg}:queue`, `redisjm:{tg}:lane:{lane}:queue`) this job has queue entries on — lets consumers drain a job's old lane after a lane change |
| `redisjm:{tg}:maintenance-lock` | String (PX TTL) | Group-wide maintenance lock. Never deleted — its expiry spaces passes about one `maintenanceInterval` apart across the fleet |
| `redisjm:{tg}:maintenance-cursor`, `…:maintenance-locks-cursor`, `…:maintenance-jobs-cursor` | String | Persisted scan cursors, so consecutive maintenance passes (on any instance) continue where the last one stopped |
| `redisjm:{tg}:instances` | Sorted Set | instanceId -> lease expiry (Redis server ms): the live consumer instances (see [`fleet()`](#fleet-promiseredisjmfleet)). Written by every started instance with `presence` on |
| `redisjm:{tg}:instance-info` | Hash | instanceId -> JSON `{ label?, concurrency, lanes, laneConcurrency, busy, startedAt, ttlMs }`, companion of `instances` |
| `redisjm:{tg}:legacy-seen` | String (PX TTL) | Set while a pre-0.2 instance was recently seen in the group (mixed rolling deploy, see [Upgrading](#upgrading-from-01x)) |

> A job with **no lane** keeps the exact legacy key `redisjm:{tg}:queue`; only a named lane gets its own `:lane:{lane}:queue` list. Everything else stays **group-wide** across all lanes — one lock namespace and a single monitoring pane for every worker type.

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
| `roundsToStale` | `2` | Number of missed heartbeat intervals before a job is considered stale (the fleet presence lease is clamped to at least two refresh intervals, so `1` never makes an instance flicker in `fleet()`). The **stale threshold** used throughout is `heartbeatInterval * roundsToStale` |
| `keepFinishedInterval` | `60000` | Milliseconds to keep finished/error/stale records in the log so `get()`/`list()` can observe the outcome. Costs memory — see [Memory & eviction](#memory--eviction). `0` opts into the legacy write-only behavior (a finished/error record is deleted in the same atomic step that ends the run; `stale` records stay until maintenance's next pass) |
| `maintenanceInterval` | `heartbeatInterval * roundsToStale` | Milliseconds between maintenance passes on the manager's own timer while `start()` is running (`0` disables). See [Maintenance](#maintenance) |
| `jobTimeout` | `0` (none) | Default execution timeout (ms) per attempt for every job; a job's own `timeoutMs` wins. See [Timeouts](#timeouts) |
| `abortGraceMs` | `false` (wait) | After a run's `ctx.signal` aborts for a reason other than its timeout (ownership loss, `stop({ abort: true })`, `payload.abort()`), wait at most this many ms for the handler, then abandon it and fail the attempt with a `JobAbortedError`. `false` = wait for the handler; `0` = settle on the next tick. A job's own `abortGraceMs` wins (`false` opts a job out). `TypeError` unless `false` or a finite number `0..2147483647` (Node's timer limit). See [Settling aborted runs](#settling-aborted-runs) |
| `presence` | `true` | While `start()` runs, register this instance in the group's fleet registry so `fleet()` can report live consumers and their capacity. **Costs one small Redis write (a `ZADD`) per instance per `heartbeatInterval`** — the info is rewritten only when it changes — and two small keys per group that expire on their own (a key-level TTL of `max(3 × lease, 60s)`, renewed on every refresh) once no instance is left; `false` opts out. Presence needs `heartbeatInterval > 0`; with `0`, `start()` logs that presence is inactive. See [`fleet()`](#fleet-promiseredisjmfleet) |
| `instanceLabel` | `''` | Free-text label stored with this instance's fleet entry (e.g. a pod name); a string of at most 200 characters, else `TypeError` |
| `maxRunMs` | `0` (off) | Maintenance marks a `running` record `stale` once it has run longer than this, regardless of its heartbeat — a backstop for hung handlers without a timeout. See [Timeouts](#timeouts) |
| `concurrency` | `1` | Max runs a single instance executes simultaneously (maintenance is not affected — it has its own timer). Must be a positive integer — the constructor floors it and throws `TypeError` if `< 1` or non-finite. See [Concurrency](#concurrency) |
| `laneConcurrency` | `{}` | Per-lane cap on this instance's simultaneous poll-loop runs (`'default'` key = default lane), within `concurrency`. See [Concurrency](#concurrency) |
| `laneCaps` | `{}` | Per-lane enqueue caps (`'default'` key = default lane): an enqueue onto a lane whose list already holds `>= cap` entries returns `'full'`. See [Backpressure](#backpressure) |
| `maxInputsBytes` | `0` (unlimited) | Max size of a run's JSON-serialized inputs; a larger enqueue throws `RedisJMEnqueueError` with reason `'inputs-too-large'` before anything is written. A job's own `maxInputsBytes` wins |
| `maxRecordsPerPass` | `1000` | Upper bound on the items each maintenance stage examines per pass. See [Maintenance](#maintenance) |
| `memoryWarnRatio` | `0.8` | `used_memory / maxmemory` ratio at which the maintenance timer fires `memoryPressure` and logs a warning (once per crossing). `0` disables; ignored when Redis has no `maxmemory` |
| `unknownJobRequeueLimit` | `5` | Times a job whose name isn't registered on the popping instance is re-queued (lock held) for a sibling instance before being dropped as an error. `0` restores the legacy drop-on-first-pop behavior |
| `laneStrategy` | `'roundRobin'` | How subscribed lanes are ordered each poll: `'roundRobin'` rotates the work-lane order to avoid starvation, `'priority'` uses `lanePriority`. See [Lanes](#lanes) |
| `lanePriority` | `[]` | Explicit high→low lane order used when `laneStrategy: 'priority'`; lanes not listed trail in registration order |
| `logger` | `console.error` | Sink `(message, error?) => void` for operational errors (handler throws and timeouts, unknown/dropped jobs, start failures, maintenance write failures, throwing manager hooks, poll-loop failures, eviction-policy and memory warnings). Pass `false` to silence default logging |

#### Methods

##### `createJob<TInputs, TAttrs>(metadata, fn): Job<TInputs, TAttrs>`

Creates a new `Job` instance and registers it with this manager. Job names must be unique per manager and must not contain `#`.

```typescript
const job = manager.createJob(
  { jobName: 'process-order', attempts: 3, timeoutMs: 60_000 },
  async (inputs: { orderId: string }, ctx) => {
    await ctx.setProgress(0.5)
    await ctx.setAttrs({ step: 'processing' })
    // ...
  }
)
```

See [Job metadata](#job-metadata) for every field (`lane`, `attempts`, `backoff`, `timeoutMs`, `maxQueued`, `maxInFlight`, `maxInputsBytes`).

##### `enqueue<TInputs>(job, runId, inputs, options?): Promise<EnqueueResult>`

Enqueues a job run and reports what happened as `{ status, jobId }`. The whole enqueue — dedupe check, `maxInFlight` check, lane-cap check, lock, record, and queue (or delayed-set) entry — is **one atomic server-side script**: there is never a half-written run, and a refused enqueue wrote nothing.

Options:
- `delay` (ms) — stage the run on the [delayed set](#retries--delayed-runs) instead of the live queue (finite number ≥ 0). The run holds its lock while it waits, so dedupe still applies.
- `first: true` — priority insert at the **front** of the lane (what `queueFirst` does). Cannot be combined with `delay > 0` (throws `TypeError`).

| `status` | Meaning | What to do |
|---|---|---|
| `'queued'` | The run was written (lane queue, or delayed set when `delay > 0`). | Done. |
| `'deduped'` | A run with this `runId` already holds a lock (queued, delayed, or running). Nothing was written. | This is the dedupe working: for idempotent triggers (cron on every instance) treat it as success. Need a fresh run regardless? Use a distinct `runId`. |
| `'busy'` | The job already has `>= maxInFlight` runs holding a lock. Nothing was written. | Skip this trigger, or try again later. See [Backpressure](#backpressure). |
| `'full'` | The lane's queue is at its cap (`maxQueued` / `laneCaps`). Nothing was written. | Shed load upstream (e.g. respond 429/503) and retry later with backoff. |

Redis failures are **not** a status: they throw [`RedisJMEnqueueError`](#redisjmenqueueerror) (and fire the manager-level `enqueueFailed` hook). Validation errors (bad lane, bad delay, `first` + `delay`) throw a plain `TypeError`/`Error`. See [Failure handling](#failure-handling).

```typescript
const { status } = await manager.enqueue(job, 'order-123', { orderId: '123' })
await manager.enqueue(job, 'urgent', { orderId: '9' }, { first: true })
await manager.enqueue(job, 'later', { orderId: '10' }, { delay: 30_000 })
```

##### `enqueueMany<TInputs>(job, entries, options?): Promise<EnqueueResult[]>`

Enqueues many runs of **one** job in a single atomic script call and returns one `EnqueueResult` per entry, in entry order — batch producers don't pay one round trip per run. Each entry is checked like a single `enqueue` (dedupe — including duplicates within the batch — `maxInFlight`, and the lane cap as the batch fills the lane); earlier entries win a cap/`maxInFlight` slot. With `first: true` the batch lands at the head of the lane **in its given order**.

All-or-nothing on failure: a Redis failure throws one `RedisJMEnqueueError` (its `jobId` is the first entry's) and nothing was written; one entry with oversized inputs rejects the whole batch before any write.

```typescript
const results = await manager.enqueueMany(job, orders.map((o) => ({ runId: o.id, inputs: o })))
const notQueued = results.filter((r) => r.status !== 'queued')
```

> Keep batches to hundreds — at most a few thousand — entries: the script blocks Redis for its duration.

##### `queue<TInputs>(job, runId, inputs, options?): Promise<boolean>`

Shorthand for `enqueue()` that returns `true` when the status is `'queued'`, `false` otherwise. **`false` means deduped _or_ busy _or_ full** — use `enqueue()` when you need to tell them apart. Redis failures throw `RedisJMEnqueueError`. Accepts `options.delay`.

```typescript
const success = await manager.queue(job, 'order-123', { orderId: '123' })
await manager.queue(job, 'order-456', { orderId: '456' }, { delay: 5000 }) // poppable in ~5s
```

> **Dedupe is on the lock, not the log record.** Enqueue dedupes on `jobName#runId` in the locks set, which is released when the run finishes or fails finally, or when maintenance reclaims it as stale. Consequences:
> - **Always check the result.** `false`/`'deduped'` means "already in flight" — a caller that ignores it reports false success. And never turn an enqueue **error** into "already in flight" (see [Failure handling](#failure-handling)).
> - **A reused `runId` is blocked by the lock of a hard-killed run** until maintenance reclaims it (about one stale threshold plus up to one `maintenanceInterval`; longer on a very large log, see [Maintenance](#maintenance)). For manual triggers where you want a fresh run regardless, use a unique `runId` per trigger (e.g. append a timestamp).

##### `queueFirst<TInputs>(job, runId, inputs, options?): Promise<boolean>`

Same as `queue` but adds to the front of the queue (priority insert). A priority insert cannot be delayed — passing `options.delay > 0` throws a `TypeError` (there is no "front" of a time-ordered delayed set).

```typescript
await manager.queueFirst(job, 'urgent-order', { orderId: '456' })
```

##### `every<TInputs>(job, intervalMs, options): () => void`

Enqueues `job` every `intervalMs` from this instance's own timer and returns a function that cancels it. Works with or without `start()`; `stop()` clears every `every()` timer.

- `inputs` (required) — inputs for every run.
- `runId` — base runId (default `'every'`).
- `skipIfInFlight` (default `true`) — every tick uses the same `runId`, so the run lock dedupes a tick while the previous run is still queued, delayed, or running — **on every instance** running the same `every()`. With `false`, each tick enqueues a distinct run `<runId>-<tickEpochMs>`.
- `immediate` (default `false`) — also enqueue once right away.

Enqueue failures are logged (and fire `enqueueFailed`), never thrown from the timer; `deduped`/`busy`/`full` ticks are skipped silently.

```typescript
const cancel = manager.every(cleanupJob, 60_000, { inputs: null, immediate: true })
```

##### `isLocked(jobId): Promise<boolean>`

Checks whether a jobId currently holds a lock. The lock is held while the run is `queued` (including popped-but-not-yet-claimed), `delayed`, or `running`, and released in the same atomic step that writes the run's terminal state (`finished`, final `error`, or `stale` when maintenance reclaims it).

```typescript
const locked = await manager.isLocked('process-order#order-123')
```

##### `isQueued(jobId): Promise<boolean>` (deprecated)

Deprecated alias for `isLocked` — the name is misleading (it returns `true` for a lock held in **any** active state, not just `queued`). Prefer `isLocked`.

##### `stats(): Promise<RedisJMStats>`

Point-in-time snapshot for dashboards/introspection: per-lane queue depths, the delayed-set size, total held locks, and log-record counts by status. **Reads the whole log** (`HSCAN`) to count statuses — for frequent polling prefer `health()`.

```typescript
const { queues, delayed, locks, statuses } = await manager.stats()
```

> **Best-effort, not transactional.** All reads are independent (no cross-structure transaction), so a run mid-transition may be double- or un-counted for one poll. **Lane visibility is scoped:** `queues` reports the lanes this instance can name — the default lane, `__maintenance`, its registered jobs' lanes, plus any lane seen on a scanned log record. A lane with queue entries but no log records **and** no local registration is invisible here.

##### `health(options?): Promise<RedisJMHealth>`

Operational health snapshot, cheap enough for a liveness/metrics endpoint:

| Field | Source / meaning |
|---|---|
| `usedMemory`, `maxMemory`, `usedRatio`, `maxmemoryPolicy` | From `INFO memory` (works on managed Redis that blocks `CONFIG`, and while Redis is full). `usedRatio` is `null` when there is no `maxmemory` |
| `oomRefusals` | Enqueues refused with an OOM error by **this** instance since it was created |
| `queues` | `LLEN` per lane this instance knows (default, `__maintenance`, registered jobs' lanes, lanes in `laneCaps` / `laneConcurrency`) |
| `delayed`, `claiming`, `locks` | Cardinalities of the delayed set, the `claiming` set (normally ~0 — a growing value means stuck pops), and the locks set |
| `running` | **Estimate** `locks − Σqueues − delayed − claiming` (floored at 0) — also counts orphaned locks and runs queued on lanes this instance doesn't know. Exact with `{ scan: true }` |
| `stale` | `null` unless `{ scan: true }` |

`{ scan: true }` adds one O(n) log scan to count `running`/`stale` exactly — keep it for occasional use.

```typescript
const h = await manager.health()
if (h.usedRatio !== null && h.usedRatio > 0.9) alert(h)
```

##### `inFlight(jobName): Promise<InFlightCounts>`

In-flight runs of one job name: `{ total, queued, delayed, running }` (popped-not-yet-claimed runs count as `queued`). Reads the job's own lock set plus exactly those records (`SMEMBERS` + one `HMGET`) — O(runs in flight of this job), independent of the log size and of other jobs.

```typescript
const { total, running } = await manager.inFlight('send-email')
```

> `total` can be **lower** than the lock-set size that `maxInFlight` is enforced against while that set holds drift (e.g. a lock released by a pre-0.2 instance, until maintenance prunes it) — so an enqueue can briefly report `'busy'` with `total < maxInFlight`. For hot paths that only need the number, use [`inFlightCount()`](#inflightcountjobname-promisenumber).

##### `inFlightCount(jobName): Promise<number>`

The number of runs of one job holding a lock — queued (including popped-not-yet-claimed), delayed and running — as **one O(1) `SCARD`** of the job's lock set; no records are read. It is exactly what `maxInFlight` is enforced against: an enqueue answers `'busy'` iff this is `>= maxInFlight` at that instant. Use it instead of `inFlight()` to gate work on a hot path; call it for several jobs with `Promise.all`.

```typescript
if (await manager.inFlightCount('send-email') >= 10) return // shed load
```

> In a group where a pre-0.2 instance still writes, the count drifts — see [Upgrading to 0.3](#upgrading-to-03).

##### `queueSize(lane?): Promise<number>`

Queue depth (`LLEN`) of a single lane's queue list — the default lane when `lane` is omitted (or `'default'`). Delayed/scheduled runs are **not** on a lane list until promoted, so they aren't counted here — use `stats().delayed` / `health().delayed` for those.

```typescript
const pending = await manager.queueSize('images')
```

##### `list(): Promise<JobLogRecord[]>`

Returns all job log records (all statuses), read incrementally with `HSCAN`. A single corrupt/foreign hash field is skipped (and logged) rather than throwing. For a large log use `listPage()`.

```typescript
const records = await manager.list()
```

##### `listPage(options?): Promise<ListPage>`

One page of log records via `HSCAN`, for listing a large log incrementally. Options: `status`, `lane` (`'default'` = default lane), `jobName`, `limit` (target page size, default `100`), `cursor` (from the previous page; omit or `'0'` to start). Returns `{ records, cursor }`; `cursor === '0'` means the scan is complete.

```typescript
let cursor = '0'
do {
  const page = await manager.listPage({ status: 'error', cursor })
  render(page.records)
  cursor = page.cursor
} while (cursor !== '0')
```

> Filters are applied to the scanned records, and one call keeps scanning until it has about `limit` matches or reaches the end — a very selective filter can make a single call scan much of the log. `HSCAN` semantics apply: a record may appear on two pages if the log changes meanwhile.

##### `listQueued(options?): Promise<QueuedPage>`

The runs waiting to run, **in the order they leave the queue**, from one atomic read-only snapshot (a single script; works under `maxmemory` and on replicas). Inputs never leave Redis. Options: `lane` (`'default'` = default lane; omitted → every lane the group has entries on), `jobName`, `limit` (default `100`, max `1000`), `offset` (default `0`; cost grows with it). Returns `{ entries, nextOffset?, complete }`; `nextOffset` is absent once the sequence is exhausted.

The order is: runs **popped but not yet claimed** (by pop time), then the **lane lists head to tail** (a `queueFirst` insert is at the head), then the **delayed** set by `readyAt` (each is pushed to the tail of its lane when it falls due). With a `lane` this is the order in which the current entries leave that lane (absent new head inserts); **across lanes there is no global pop order** — consumers interleave lanes per `laneStrategy`. Without `lane`: `default` first, then the other lanes alphabetically; the reserved `__maintenance` lane only when asked for explicitly.

```typescript
let offset: number | undefined = 0
while (offset !== undefined) {
  const page = await manager.listQueued({ lane: 'images', limit: 50, offset })
  render(page.entries) // { jobId, jobName, runId, lane?, status: 'queued' | 'delayed', poppedAt?, readyAt? }
  if (!page.complete) console.warn('page cut short by a scan bound; narrow with lane / jobName')
  offset = page.nextOffset // absent once the sequence is exhausted
}
```

- The lane of a delayed or popped entry lives only in its record, so a `lane` filter (and the `lane` of each returned delayed/popped entry) is resolved **inside the script** with `cjson` — at most 500 record decodes and 10 000 walked entries per call, on the Redis thread (decode cost grows with record size; keep inputs small or pointer-sized). Hitting a bound stops the listing early with **`complete: false`** — narrow it with `lane` / `jobName`. Expect it when a page needs more than 500 decodes (no `lane` filter, many delayed or popped entries inside the page). An entry whose record is missing or unreadable has no `lane` and is excluded by a lane filter.
- Without `lane`, the lanes to read are discovered client-side first — from this instance's registered jobs and the per-job lane sets (a registry `SMEMBERS`, then one pipelined read; with `jobName`, only that job's set): two small round trips before the script. A lane written only by a pre-0.2 producer is listed only when you pass it as `lane`.
- Pre-0.2 consumers pop without a `claiming` entry, so runs they popped are invisible until claimed (then `running`, not listed).
- Same exposure as `listPage()` / `get()`: runIds can carry entity ids — gate any admin endpoint built on it.

##### `getMany(jobIds): Promise<Array<JobLogRecord | undefined>>`

Records for many jobIds in **one round trip** (a pipeline of `HMGET`s, 500 fields per command), in input order — duplicates included; `undefined` for a missing or unparseable record. Inputs are included, as with `get()`. `[]` makes no Redis call; a Redis error rejects.

```typescript
const records = await manager.getMany(page.entries.map((e) => e.jobId))
```

##### `get(jobId): Promise<JobLogRecord | undefined>`

Fetches a single record by `jobId` (`"jobName#runId"`), or `undefined` if absent — an O(1) `HGET` instead of scanning `list()`. Handy for "did my trigger finish?" endpoints.

```typescript
const record = await manager.get('process-order#order-123')
```

> With the default `keepFinishedInterval: 60000`, a terminal record lingers ~60s, so a run that just finished **is** observable here (status `finished`/`error`/`stale`). Opting into `keepFinishedInterval: 0` deletes a finished/error record in the same step that ends the run, and a finished run then returns `undefined` — `undefined` means "no record", **not** "never ran". Under `0`, wire the `finish`/`error` hooks to observe outcomes instead.

##### `unqueue(jobId): Promise<void>`

Removes a run in one atomic step: its record, its lock, its lane-queue entry, and any delayed-set, `claiming` and suspects entries. Also the manual fix for a run whose lock is stuck (see [Upgrading](#upgrading-from-01x)).

```typescript
await manager.unqueue('process-order#order-123')
```

##### `popAndExecute(): Promise<boolean>`

Pops the next job from this instance's lanes, matches it to a registered Job by name, executes it, and resolves once that run settled. Returns `true` if something was popped (even if execution failed), `false` if the queues were empty — or while popping is briefly paused after a [start failure](#start-failure-recovery), or once `stop()` has been called (until the next `start()`). Each pop first promotes due [delayed](#retries--delayed-runs) runs (at most once per second). Rejects when the pop itself fails (e.g. Redis at `maxmemory` refuses the pop script — then nothing was popped).

It is independent of the poll loop's slots (always executes one pop; not counted against `concurrency` / `laneConcurrency`), but it **is** tracked: `stop()` waits for an in-flight `popAndExecute()`. To trigger an immediate poll from a running loop, use `wake()` instead.

If the job name is not registered on this instance, the run is re-queued (lock held) up to `unknownJobRequeueLimit` times so a sibling instance that *does* register the handler — e.g. a freshly deployed pod — can claim it. Once the budget is exhausted the record is set to status `"error"` with error `"Job name is unknown"` and the lock is released. A run that lost ownership of its record before it could claim it throws `RunSupersededError` internally, which is treated as a benign skip (it touches neither the lock nor the log — the new owner does). Handler failures, unknown-name drops, and missing-record drops are reported through the configured `logger`.

```typescript
while (await manager.popAndExecute()) {} // drain-until-empty worker
```

##### `start(interval): void`

Starts a polling loop. When a job is popped, the next poll fires immediately; when the queues are empty it waits `interval` ms. Throws `TypeError` if `interval` is not a positive number. With `concurrency > 1` the loop keeps up to `concurrency` runs in flight (see [Concurrency](#concurrency)).

Unless `maintenanceInterval` is `0`, `start()` also runs [maintenance](#maintenance) on its own timer — once immediately, then every `maintenanceInterval` ms — independent of the poll loop and the concurrency slots, and registers the built-in maintenance job so maintenance entries enqueued by **older** instances during a rolling deploy are still consumed. Every `start()` also reads `maxmemory_policy` once from `INFO memory` and logs a warning for an `allkeys-*` or `volatile-*` policy.

```typescript
manager.start(1000) // poll every 1 second when idle
```

##### `wake(): void`

Cuts the poll loop's idle wait short so it polls **now** — e.g. from a pub/sub "doorbell" a producer rings after enqueueing. Respects `concurrency` / `laneConcurrency`, so it is the safe replacement for calling `popAndExecute()` ad hoc. No-op when the loop isn't running or every slot is busy (the loop re-polls as soon as a slot frees anyway); a wake that arrives while a poll is in flight makes the next poll immediate.

```typescript
subscriber.subscribe('jobs:doorbell')
subscriber.on('message', () => manager.wake())

// producer side
await job.enqueue(runId, inputs)
await publisher.publish('jobs:doorbell', '1')
```

##### `stop(options?): Promise<void>`

Stops the polling loop, the maintenance timer, and every `every()` timer, and resolves once **all** in-flight runs have settled — so a shutdown handler can `await` a drain before exiting. It also awaits an in-flight maintenance pass and in-flight `popAndExecute()` calls, and makes `popAndExecute()` return `false` until the next `start()`. This instance also leaves the [fleet registry](#fleet-promiseredisjmfleet) first thing. Two modes:

- **Graceful (default)** — `stop()` stops popping new work and awaits the runs already in flight to finish on their own.
- **Fast (`stop({ abort: true })`)** — additionally aborts every in-flight run's `ctx.signal` (reason `'manager stopped'`) so cooperative handlers can bail out early. Abort is cooperative: nothing forcibly kills a handler; `stop()` still awaits every run to settle. A handler that ignores its signal is awaited only until its [execution timeout](#timeouts), when one is set — or, with [`abortGraceMs`](#settling-aborted-runs), only until that many ms after the abort: it is then abandoned, its attempt fails with a `JobAbortedError` and `stop()` resolves (see [Settling aborted runs](#settling-aborted-runs) for what that does to the run). See [Cancellation](#cancellation).

For locks to release cleanly, prefer graceful shutdown (`stop()` on `SIGTERM`); a hard `SIGKILL` leaves locks that maintenance reclaims after the stale threshold.

```typescript
process.on('SIGTERM', async () => { await manager.stop() })              // graceful drain
process.on('SIGINT',  async () => { await manager.stop({ abort: true }) }) // fast abort-and-drain
```

##### `runMaintenance(): Promise<MaintenanceResult | null>`

Runs one maintenance pass **if this instance wins the group's maintenance lock** — what the `start()` timer calls. Returns `null` when another instance holds the lock (or the lock could not be read for a non-memory reason, which is logged). If Redis refuses the lock with an out-of-memory error, it runs an **emergency** delete-only pass instead (`mode: 'emergency'`). See [Maintenance](#maintenance).

```typescript
// Workers that never call start() (e.g. popAndExecute-only drainers) schedule it themselves:
setInterval(() => { manager.runMaintenance().catch(() => {}) }, 10_000)
```

##### `performMaintenance(): Promise<MaintenanceResult>`

One full, **unguarded** maintenance pass (no lock). Kept for manual/one-off use (scripts, tests); on a timer, prefer `runMaintenance()` so the fleet runs about one pass per interval instead of one per instance. The pass itself is safe to overlap (every write is compare-and-set), just redundant. Returns `{ staleCount, cleanedCount, requeuedCount, mode: 'full' }` — see [Maintenance](#maintenance) for what each stage does.

##### `fleet(): Promise<RedisJMFleet>`

The live **consumer instances** of the group — those running `start()` with `presence` on — and their capacity, from one read-only script (O(instances); works under `maxmemory` and on replicas):

```typescript
const { instances, slots, busy, lanes } = await manager.fleet()
const wave = lanes['images']?.slots ?? 0   // how many image runs the fleet can execute at once
```

| Field | Meaning |
|---|---|
| `instances[]` | `{ instanceId, label?, concurrency, lanes, laneConcurrency, busy, startedAt, seenAt, expiresAt }`, sorted by `instanceId`. `lanes` = the lane labels it consumes (`'default'` for the default lane); `busy` = its poll-loop runs in flight at its last refresh; `seenAt` / `expiresAt` are Redis **server** ms |
| `slots`, `busy` | Σ `concurrency`, Σ `busy` |
| `lanes` | Per lane `{ instances, slots }` where `slots` = Σ `min(concurrency, laneConcurrency[lane] ?? concurrency)`. Lanes **share** each instance's `concurrency`, so lane slots do not add up across lanes |

Liveness is a lease: a started instance refreshes its entry every `heartbeatInterval` and it lapses after `heartbeatInterval × roundsToStale` — **on Redis server time**, so client clock skew is irrelevant — exactly like its runs go stale. (An instance whose event loop is pinned lapses too; raise `roundsToStale` for CPU-heavy work and both widen.) `stop()` removes the entry at its start; the refresh also prunes lapsed entries. The entry is rebuilt on every refresh, so jobs registered after `start()` show up on the next one. A refresh refused under `maxmemory` is logged once per episode and the instance lapses after its ttl — truthful, since a full Redis refuses every pop anyway.

**Not counted:** drain workers that only call `popAndExecute()`, producer-only processes, instances with `presence: false`, and **instances older than 0.3.0** — during a rolling deploy `fleet()` under-counts until the rollout completes.

##### `getInstanceId(): string`

This manager's id in the fleet registry: a random UUID, fixed for the manager's lifetime (it survives `stop()` / `start()`), for tying a log line or a `fleet()` entry back to a process. Pair it with the `instanceLabel` option (e.g. the pod name).

##### `registerJob(job): void`

Registers a `Job` instance by name (must be unique; must not contain `#`; lane validated). Hooks on all job events to update Redis and re-dispatch. Registering is what subscribes a consumer to the job's lane — see [Produce vs. consume](#produce-vs-consume).

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

`manager` is the default manager used by `enqueue`/`queue`/… when none is passed explicitly. Constructing a `Job` does **not** register it — see [Produce vs. consume](#produce-vs-consume).

#### Job metadata

| Field | Default | Description |
|---|---|---|
| `jobName` | — | Unique name; key prefix of job IDs (`"jobName#runId"`). Must not contain `#` |
| `description` | — | Human-readable description |
| `lane` | default lane | Named sub-queue; see [Lanes](#lanes) |
| `attempts` | `1` | Total attempts including the first; floored, clamped ≥ 1. See [Retries](#retries) |
| `backoff` | `0` | Ms before the next retry: a number or `(failedAttempt) => ms`; negatives/non-finite → `0` |
| `timeoutMs` | manager `jobTimeout` | Execution timeout per attempt; `0` disables the manager default for this job. See [Timeouts](#timeouts) |
| `abortGraceMs` | manager `abortGraceMs` | Grace after an abort (not a timeout) before the attempt is abandoned with a `JobAbortedError`; `false` opts this job out of the manager default. See [Settling aborted runs](#settling-aborted-runs) |
| `maxQueued` | — | Enqueue cap on this job's lane list (combined with `laneCaps`; the smaller wins) → `'full'`. See [Backpressure](#backpressure) |
| `maxInFlight` | unlimited | Max runs of this job holding a lock at once (queued + delayed + running, any runId, all instances) → `'busy'`. `1` = single-flight |
| `maxInputsBytes` | manager `maxInputsBytes` | Max JSON-serialized inputs size; `0` disables the manager default for this job |

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
- `signal` aborts on ownership loss, execution timeout (reason `'timeout'`), or `stop({ abort: true })` — see [Cancellation](#cancellation).
- Once an attempt was **abandoned** (its timeout or `abortGraceMs` elapsed), the detached handler's `setProgress` / `setAttrs` no longer reach the record or the `update` event — see [Timeouts](#timeouts).

#### Methods

##### `execute(inputs, options?): Promise<void>`

Runs the job function with heartbeat timer and context callbacks. Options:

```typescript
interface JobExecuteOptions {
  targetGroup?: string
  heartbeatInterval?: number  // enables automatic heartbeat events
  runId?: string              // explicit runId (otherwise derived from inputs)
  manager?: RedisJM           // driving manager, stamped on every event payload
  logger?: RedisJMLogger      // sink for infra errors (failed heartbeat write, throwing error hook, late handler settle)
  signal?: AbortSignal        // external abort plumbed into ctx.signal (e.g. shutdown)
  timeoutMs?: number          // execution timeout; rejects with JobTimeoutError on expiry
  abortGraceMs?: number | false // after an abort (not the timeout): rejects with JobAbortedError once the handler is still pending after this many ms
  attempt?: number            // 1-based attempt stamped on every event payload (default 1)
}
```

All of these are wired automatically when a run is dispatched by `start()`/`popAndExecute()` (`timeoutMs` = the job's `timeoutMs` ?? the manager's `jobTimeout`, `abortGraceMs` likewise); you set them only when calling `execute()` directly.

##### `enqueue(runId, inputs, manager?, options?): Promise<EnqueueResult>`

Delegates to [`RedisJM.enqueue`](#enqueuetinputsjob-runid-inputs-options-promiseenqueueresult) through the given or default manager.

```typescript
const { status } = await job.enqueue('order-123', { orderId: '123' })
if (status === 'deduped') console.log('already in flight')
```

##### `enqueueMany(entries, manager?, options?): Promise<EnqueueResult[]>`

Delegates to [`RedisJM.enqueueMany`](#enqueuemanytinputsjob-entries-options-promiseenqueueresult).

##### `queue(runId, inputs, manager?, options?): Promise<boolean>` / `queueFirst(runId, inputs, manager?, options?): Promise<boolean>`

Delegate to `RedisJM.queue` / `RedisJM.queueFirst` (`false` = deduped, busy, or full).

```typescript
await job.queue('order-456', { orderId: '456' }, undefined, { delay: 5000 })
```

All enqueue helpers throw `Error('No RedisJM instance provided and no default manager set')` when there is neither an explicit nor a default manager.

##### `getJobId(runId): string`

Returns `"jobName#runId"`.

##### Other accessors

`getMetadata()` (copy), `getName()`, `getLane()`, `getAttempts()` (resolved, ≥ 1), `getBackoffMs(attempt)`, `getTimeoutMs()` (the job's own timeout, `0` = explicitly none, `undefined` = defer to the manager), `setDefaultManager(manager)`.

---

### Errors

#### `RedisJMEnqueueError`

Thrown by `enqueue` / `enqueueMany` / `queue` / `queueFirst` (and their `Job` counterparts) when Redis refuses or fails the enqueue, or before any write when the inputs are too large. Fields: `reason`, `jobId` (`"jobName#runId"`; the first entry's for a batch), `cause` (the underlying error).

| `reason` | Meaning | Was anything written? |
|---|---|---|
| `'oom'` | Redis is at `maxmemory` (`noeviction`) and refused the enqueue script | No — retrying the same `runId` is safe |
| `'readonly'` | The client is talking to a read-only replica (e.g. mid-failover) | No |
| `'connection'` | The connection is gone or never came up | **Unknown** — the script may still have run. Retrying the same `runId` is safe (it may come back `'deduped'`) |
| `'timeout'` | The client gave up waiting (ioredis `MaxRetriesPerRequestError`, `commandTimeout`) | **Unknown** — as above |
| `'inputs-too-large'` | Serialized inputs exceed `maxInputsBytes` | No — rejected before any Redis traffic |
| `'unknown'` | Anything else | Treat as unknown |

#### `JobTimeoutError`

The error an attempt fails with when it exceeds its execution timeout (`name === 'JobTimeoutError'`, field `timeoutMs`). It is an ordinary failure: it reaches the `error` hooks and `attempts`/`backoff` apply. See [Timeouts](#timeouts).

#### `JobAbortedError`

The error an attempt fails with when its `ctx.signal` aborted — for a reason other than its execution timeout — and the handler did not settle within `abortGraceMs` (`name === 'JobAbortedError'`, fields `reason` = the abort reason, e.g. `'manager stopped'`, and `graceMs`). An ordinary failure: it reaches the `error` hooks, `attempts`/`backoff` apply, and the attempt **is consumed**. A handler that settles on its own inside the grace decides the outcome itself. See [Settling aborted runs](#settling-aborted-runs).

#### `RunSupersededError`

Thrown out of an execution whose popped entry no longer owns its log record (a successor or concurrent claimant claimed it first). The manager treats it as a benign skip.

#### `classifyRedisError(err): RedisErrorReason`

The shared classifier behind the reasons above (`'oom' | 'readonly' | 'connection' | 'timeout' | 'unknown'`). Pure, synchronous, never throws — use it in your own Redis error handling for consistent decisions.

```typescript
import { classifyRedisError } from '@prostojs/redisjm'

try { await redis.set('k', 'v') } catch (err) {
  if (classifyRedisError(err) === 'oom') shedLoad()
}
```

---

### `createMaintenanceJob(manager): Job<null, never>`

Creates and registers the built-in maintenance job (`MAINTENANCE_JOB_NAME`, on the reserved `MAINTENANCE_LANE` `'__maintenance'`), whose handler calls `manager.runMaintenance()`.

**You normally don't need it.** Since 0.2, `start()` runs maintenance on its own timer and registers this job itself, only so that maintenance entries enqueued by **pre-0.2 instances during a rolling deploy** are still consumed. Don't enqueue it yourself: a queued pass waits behind (and occupies) a concurrency slot, and every popped entry of this job is taken as a sign that a pre-0.2 instance is still running, which keeps maintenance's costlier legacy orphan check switched on (see [Upgrading](#upgrading-from-01x)). For manual scheduling without `start()`, call `manager.runMaintenance()` from your own timer.

The one case to call it: a worker that never calls `start()` (e.g. a `popAndExecute()`-only drainer) during a mixed 0.1.x/0.2 deploy, so it can consume the old instances' maintenance entries.

## Events

Both `Job` and `RedisJM` emit events via [hookable](https://github.com/unjs/hookable). Run-event payloads (`start`, `finish`, `error`, `retry`, `timeout`, `heartbeat`, `update`) also carry `executionId` (the per-run fencing token), `attempt` (the 1-based attempt of this execution — see [Attempt numbers](#attempt-numbers)), `abort(reason?)` (a cooperative kill-switch for this run — see [Cancellation](#cancellation)), and `manager` (the driving `RedisJM`, absent for a direct `job.execute()`):

| Event | Extra payload fields | Emitted by | Description |
|---|---|---|---|
| `start` | — | Job + manager | Run claimed, before the job function runs |
| `finish` | — | Job + manager | After successful completion |
| `error` | `error` | Job + manager | See retry semantics below |
| `retry` | `error, attempt, nextAttemptAt` | **manager only** | A failed attempt was scheduled for retry |
| `timeout` | `error, timeoutMs` | **manager only** | An attempt exceeded its execution timeout; fires before that attempt's `retry`/`error` |
| `heartbeat` | — | Job + manager | Periodic heartbeat tick |
| `update` | `progress?, attrs?` | Job + manager | On setProgress/setAttrs |
| `enqueueFailed` | `{ jobId, jobName, runId, reason, error }` | **manager only** | An enqueue threw `RedisJMEnqueueError` (fired right before it is thrown) |
| `startFailed` | `{ jobId, jobName, runId, reason, action, error, attempt? }` | **manager only** | A popped run failed to start; `action` says how it was recovered — see [Start-failure recovery](#start-failure-recovery) |
| `memoryPressure` | a `health()` snapshot | **manager only** | Redis memory crossed `memoryWarnRatio` (from the maintenance timer; once per crossing) |
| `maintenance` | `{ result, failedOps, error?, reason?, durationMs }` | **manager only** | A maintenance pass THIS instance ran finished (timer, `runMaintenance()`, `performMaintenance()`, the legacy job), or could not run because of an error. Not fired when another instance holds the lock. On the timer it fires before `memoryPressure` — see [Maintenance](#maintenance-hook) |

`RedisJM` re-dispatches run events only when `targetGroup` matches (and, when a `manager` is stamped, only on the manager that drove the run) and updates the Redis log accordingly.

### Manager hooks are observers; job hooks are the lifecycle

- **Manager-level hooks** (`manager.hook(...)`) are **isolated observers**. They are notified *after* the manager has made its Redis write for the transition; a throwing or rejecting manager hook is reported to the `logger` and can never change the run's outcome or Redis state, nor stop the other manager hooks for the same event. Use them for metrics, alerts, and logging.
- **Job-level hooks** (`job.hook(...)`) **are the lifecycle**: they are awaited in registration order (the manager drives Redis state through its own job-level hooks). A throwing job-level `start` hook therefore **fails the run**: thrown after the manager's claim, the failure goes through the normal retry-or-final path (the `error` hooks fire); registered before the job was registered with a manager and thrown before the claim, the queued run is failed terminally. A throwing `finish` hook never turns a successful run into `error`.

**`error` vs `retry` (retry semantics):**

- The **job-level** `error` hook (`job.hook('error', …)`) fires on **every** failed attempt (handler throw, execution timeout, an abandoned abort — `JobAbortedError` — or a job-level `start` hook that threw after the claim), including ones that will be retried.
- The **manager-level** `error` event (`manager.hook('error', …)`) fires only on **final** failure — the last attempt exhausted the `attempts` budget.
- The **manager-level** `retry` event fires once per scheduled retry in between (payload adds `error`, the 1-based `attempt` that failed, and `nextAttemptAt` — epoch-ms the retry becomes poppable). See [Retries & delayed runs](#retries--delayed-runs).

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

manager.hook('enqueueFailed', ({ jobId, reason }) => metrics.increment('enqueue_failed', { reason }))
manager.hook('memoryPressure', (health) => pageOnCall(`redis at ${health.usedRatio}`))
```

### Attempt numbers

`payload.attempt` tells a first run (`1`) from a retry (`2`, `3`, …) on every run event, job- and manager-level:

```typescript
manager.hook('start', ({ job, runId, attempt }) => {
  if (attempt > 1) console.log(`${job.getJobId(runId)}: retry #${attempt - 1}`)
})
```

- It is the run's attempt count as stored with the run, so it is correct when a retry is picked up by a different instance than the one whose attempt failed. Every event of the same execution carries the same number; for `retry` it is the attempt that just failed.
- Job-level `start` hooks registered **before** `registerJob` run before the run is claimed and see the expected attempt (the same number unless the run was changed in between).
- A direct `job.execute()` uses `options.attempt` (an integer `>= 1`, default `1`).
- `startFailed` carries `attempt` only for `action: 'failed'` when a job-level `start` hook threw after the claim.

### Observability & error handling

- **Failures are visible by default.** A thrown handler is recorded in the log (`status: 'error'`), re-broadcast on the `error` event, **and** written to the `logger` (default `console.error`, including the stack). Timeouts, start failures, unknown-name drops, missing-record drops, maintenance write failures, throwing manager hooks, and poll-loop errors also go to the `logger`. Pass `logger: false` to silence, or a custom function to redirect.
- **Don't blanket-`DEL` the Redis keys.** Every key in the [key table](#redis-key-structure) is shared by **every** job in the target group — deleting one wipes or corrupts all runs in the group, not just yours. Use `unqueue(jobId)` to remove a single run.

### Long-running handlers & heartbeats

Heartbeats are emitted from a timer on the single-threaded event loop. A long, tightly synchronous handler that never `await`s starves that timer, so heartbeats stop landing and maintenance may mark the run `stale` and release its lock — opening a window in which a producer can re-enqueue the **same** `runId` while the original handler is still running.

**Self-heal (the common case).** Staleness detection can't tell a dead handler from one that merely pinned its event loop, so recovery is symmetric: the moment the handler breathes again — its next heartbeat, `setProgress`, or `setAttrs` — the run **resurrects** its own record (`stale` → `running`), clears the maintenance-stamped `finishedAt`, and **re-acquires its lock**. It keeps its progress/attrs stream and re-establishes dedupe, so its eventual `finish`/`error` records the truth. (A run staled by [`maxRunMs`](#timeouts) is the exception: it is never resurrected.)

**Zombie fencing (a successor already took over).** If, during the released-lock window, a producer re-enqueued the `runId` and a successor claimed it, the original ("zombie") run is genuinely superseded: its `executionId` no longer matches the record, so its finish/error/heartbeat/update writes are all **rejected**, and its `ctx.signal` **aborts** within one `heartbeatInterval`. But cancellation is cooperative — the zombie only stops if the handler observes `signal`. A handler that ignores the signal keeps performing its own **external side effects** concurrently with the successor. Chunk long work, `await` between chunks so heartbeats fire, and check `ctx.signal` at each checkpoint:

```typescript
manager.createJob({ jobName: 'backfill' }, async (inputs: { ids: string[] }, ctx) => {
  for (let i = 0; i < inputs.ids.length; i += 100) {
    if (ctx.signal.aborted) return                     // lost ownership → stop side effects
    await processChunk(inputs.ids.slice(i, i + 100))   // yields to the loop → heartbeat lands
    await ctx.setProgress(Math.min(1, (i + 100) / inputs.ids.length))
  }
})
```

> **Clock-skew note.** Staleness and the `claiming` re-queue compare timestamps written by one machine (heartbeats, pop times, `suspectedAt`) against `Date.now()` on the machine running maintenance. Skew larger than the stale threshold causes premature stale marks and re-queues — keep NTP sane across the fleet.

## Timeouts

Without a timeout, a handler that never settles (a hung socket, a lost promise) holds its concurrency slot forever while its heartbeat timer keeps the record looking alive. Two independent guards:

```typescript
const manager = new RedisJM(redis, 'my-app', {
  jobTimeout: 5 * 60_000,  // default per-attempt timeout for every job
  maxRunMs: 30 * 60_000,   // maintenance backstop: stale any run older than this
})

const syncJob = manager.createJob(
  { jobName: 'sync-account', timeoutMs: 60_000, attempts: 3, backoff: 10_000 }, // job value wins
  async (inputs: { accountId: string }, ctx) => {
    const res = await fetch(`https://api.example.com/accounts/${inputs.accountId}`, { signal: ctx.signal })
    // ...
  },
)

manager.hook('timeout', ({ runId, timeoutMs }) => console.warn(`sync ${runId} timed out after ${timeoutMs}ms`))
```

**Execution timeout** (`JobMetadata.timeoutMs`, else `RedisJMOptions.jobTimeout`; `0`/unset = none; a job's `timeoutMs: 0` opts it out of the manager default):

- On expiry the run's `ctx.signal` aborts with reason `'timeout'` and the attempt fails with a `JobTimeoutError` — an ordinary failure, so `attempts`/`backoff` apply. The manager fires `timeout`, then `retry` or `error` for that attempt.
- The slot frees **immediately**, even if the handler never settles.
- **The handler is not killed** — JavaScript can't. An abandoned attempt keeps running detached; its record writes (`setProgress`/`setAttrs`) are fenced out — a rejected write changes nothing and fires no `update` event; if it later settles or rejects that is logged once (never an unhandled rejection). A **retry can overlap the abandoned attempt**, so handlers must observe `ctx.signal` (pass it to `fetch`, DB drivers, …) and be idempotent.

The same settle-and-abandon mechanism can be applied to every *other* abort reason — see [Settling aborted runs](#settling-aborted-runs).

**`maxRunMs` backstop** (off by default): maintenance marks a `running` record `stale` (`staleReason: 'maxRunMs'`, lock released) once `now - startedAt > maxRunMs`, **regardless of its heartbeat**. Unlike a heartbeat stale, the run cannot resurrect itself: its heartbeats and updates are rejected and its `ctx.signal` aborts on its next heartbeat. Its own outcome still lands, as for any stale-but-owned run — a later finish records `finished`, and a throw schedules a retry when attempts remain (else `error`). Pick a value comfortably above your slowest healthy run; it measures a single attempt (from that attempt's `startedAt`), so it doesn't need to cover retries. Detection happens when maintenance's scan reaches the record (see [Maintenance](#maintenance)).

## Retries & delayed runs

A run can be **staged for the future** two ways, both backed by the same `redisjm:{tg}:delayed` sorted set (member = jobId, score = ready-at epoch-ms):

- **Delayed enqueue** — `enqueue(job, runId, inputs, { delay })` (or `queue`, or the `Job` variants) stores the record as `delayed`, **holding the lock** (so dedupe still applies while it waits) and schedules it on the delayed set. It becomes poppable once `delay` ms elapse. A priority insert cannot be delayed (`first`/`queueFirst` with `delay > 0` throws).
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

When an attempt below the budget fails — a handler throw or an [execution timeout](#timeouts) — the run goes **back through the delayed set with its lock still held** (so no duplicate enqueue of the same `runId` can slip in during the backoff), and the manager fires **`retry`**. The final failure takes the terminal path: status `error`, lock released, manager `error` event.

> Retries cover failures the manager *observes*. A run whose instance died mid-execution is detected by maintenance and ends **`stale`** — it is not retried automatically. See [Delivery guarantees](#delivery-guarantees).

### Promotion

Each polling instance moves due delayed entries onto their lane queues before popping, rate-limited to at most once per second and at most 8 ids per sweep. Each promotion is one atomic step (off the delayed set, record `delayed` → `queued`, pushed onto its lane), so two instances can't both promote the same run. A delayed run therefore becomes poppable within ~1s of its ready time — don't rely on sub-second precision. Use `health().delayed` / `stats().delayed` to see how many runs are staged.

## Concurrency

By default (`concurrency: 1`) an instance runs **one** job at a time. Set `concurrency > 1` for I/O-bound workloads that can keep several runs in flight:

```typescript
const manager = new RedisJM(redis, 'my-app', { concurrency: 8 })
```

The poll loop then dispatches a popped run **without awaiting it** and immediately pops again, keeping up to `concurrency` runs in flight; it waits only when all slots are full. `stop()` drains **all** in-flight runs (including one being popped at that instant). Concurrency is per **instance**; the distributed lock still guarantees each `runId` runs on only one instance at a time. Maintenance does not use a slot — it runs on its own timer even when every slot is held by a hung handler.

### Per-lane concurrency

`laneConcurrency` caps how many poll-loop runs of a lane this instance executes at once, within the global `concurrency` — e.g. keep capacity for a latency-sensitive lane while a heavy lane is busy:

```typescript
const manager = new RedisJM(redis, 'my-app', {
  concurrency: 8,
  laneConcurrency: { images: 2 }, // at most 2 image runs here; the other slots stay free for other lanes
})
```

The poll loop doesn't pop from a lane at its cap; when one of its runs settles, the loop re-polls right away. Lanes without an entry are limited only by `concurrency`. Runs started by `popAndExecute()` are not counted.

### Fleet capacity

Concurrency and lane caps are per instance; to size work against the whole group (how many runs of a lane can execute at once across every pod) read [`fleet()`](#fleet-promiseredisjmfleet) — it knows each live instance's real `concurrency` and `laneConcurrency` and the lanes it actually consumes, which an environment constant times a pod count does not.

## Backpressure

Without limits, a producer that outruns its consumers grows the queue until Redis runs out of memory. Two atomic, reject-only limits are enforced **inside the enqueue script**, so concurrent producers can't race past them:

```typescript
const manager = new RedisJM(redis, 'my-app', {
  laneCaps: { default: 10_000, images: 500 }, // max queue length per lane
})

const reportJob = manager.createJob(
  { jobName: 'rebuild-report', maxInFlight: 1 }, // single-flight across all runIds and instances
  rebuildReport,
)

const { status } = await reportJob.enqueue(`manual-${Date.now()}`, null)
if (status === 'busy') return reply(409, 'a rebuild is already in progress')
if (status === 'full') return reply(503, 'queue is full, try later')
```

**Lane caps** (`JobMetadata.maxQueued` and `RedisJMOptions.laneCaps` — the smaller defined cap wins):

- Count the lane's **queue list only** (O(1) `LLEN`): delayed runs aren't counted until promoted, running runs never.
- Internal push-backs bypass the cap — promotion of a delayed run or retry, an unknown-job requeue, a start-failure requeue, or maintenance re-queueing an abandoned pop — so the list can exceed it.
- **Reject-only**: a full lane refuses new work (`'full'`, nothing written, no lock taken); nothing already accepted is dropped to make room. Silently discarding accepted work is exactly the failure mode the cap exists to prevent, so there is no "drop oldest" mode.

**`maxInFlight`** (per job): an enqueue while the job already has `>= maxInFlight` runs holding a lock — queued, delayed, or running, across all runIds and instances — returns `'busy'` and writes nothing. It applies to every producer (timers, `every()`, manual triggers). `every()`'s `skipIfInFlight` is the per-`runId` equivalent.

**Watching it:** `inFlightCount(jobName)` for the exact number `maxInFlight` is enforced against (one O(1) `SCARD` — fine on a hot path), `inFlight(jobName)` for the per-status split (O(runs of that job) — dashboards and occasional use), `listQueued()` for what is waiting and in which order, `health()` for lane lengths and memory.

## Cancellation

Every execution's `ctx.signal` is an `AbortSignal` that aborts when the run should stop wasting work:

- **Ownership loss** — the run was superseded by a re-enqueue of the same `runId`, reached a terminal state (including a `maxRunMs` stale), or was unqueued. Detected via the heartbeat hook's guarded write, so it fires within one `heartbeatInterval`. (A run merely staled by a lapsed heartbeat but still owned by this execution **self-heals** instead — see [Long-running handlers](#long-running-handlers--heartbeats).)
- **Timeout** — the attempt exceeded its execution timeout (reason `'timeout'`).
- **Shutdown** — `stop({ abort: true })` aborts every in-flight run (reason `'manager stopped'`).

Cancellation is **cooperative** — nothing forcibly kills a handler. Check `signal.aborted` (or listen for `'abort'`, or hand the signal to I/O that accepts one) at natural checkpoints; `signal.reason` carries a short string cause. A handler that never checks the signal runs to completion, but its record writes are fenced out — the danger is only its **external side effects**.

```typescript
manager.createJob({ jobName: 'export' }, async (inputs: { rows: Row[] }, ctx) => {
  for (const row of inputs.rows) {
    if (ctx.signal.aborted) return          // stop early — cooperative
    await writeRow(row)                      // external side effect worth avoiding after abort
  }
})
```

User hooks can also trigger this signal via `payload.abort(reason?)` as a custom kill-switch.

### Settling aborted runs

By default an abort only flips `ctx.signal`: `execute()` keeps waiting for the handler, so a handler that ignores its signal holds its concurrency slot — and `stop()`'s drain — until it returns (or until its timeout). Set `abortGraceMs` (manager default, per job, or on `execute()`) to bound that:

```typescript
const manager = new RedisJM(redis, 'my-app', { abortGraceMs: 5_000 })
manager.hook('error', ({ error }) => {
  if (error instanceof JobAbortedError) console.warn(`aborted (${error.reason}), gave up after ${error.graceMs}ms`)
})
```

- When the signal aborts for any reason **other than the run's own timeout** — ownership loss, `stop({ abort: true })`, `payload.abort()`, an external `signal` — the handler gets `abortGraceMs` ms to settle on its own. A handler that rejects inside the grace (e.g. a cooperative `AbortError`) has **its own error** as the attempt's error; one that resolves inside it finishes normally. Only a handler still pending at the end of the grace is **abandoned**: `execute()` rejects with a [`JobAbortedError`](#jobabortederror), the slot frees, `stop()` can resolve.
- `0` settles on the next timer tick. A timeout shorter than the grace wins (`JobTimeoutError`).
- Like a timed-out attempt, the abandoned handler **keeps running detached** — see [Timeouts](#timeouts); its heartbeat is stopped and nothing it does can overwrite the record. A retry (or successor) may overlap it: [Delivery guarantees](#delivery-guarantees).
- **An abandoned attempt consumes an attempt**, exactly like a handler that throws on abort or a timeout. With the default `attempts: 1`, a run abandoned by `stop({ abort: true })` ends `error` and is **not** re-run; set `attempts > 1` (with a `backoff`) if an aborted run must be re-run elsewhere.

What happens to the state of an abandoned run (the existing failure path, fenced on the run's `executionId`):

| Abort cause | Record | Lock | Manager events |
|---|---|---|---|
| `stop({ abort: true })`, record still ours | attempts left → `delayed` (`readyAt = now + backoff`, delayed-set entry); else `error` | retry: kept; final: released | `retry` or `error` |
| Superseded (re-enqueued and re-claimed elsewhere) | untouched | untouched (the successor's) | none |
| `maxRunMs` stale (still our run) | like a handler throw on a stale-but-owned run: retry → `delayed`, else `error` | as left | `retry` or `error` |
| Unqueued mid-run | nothing to write; the lock nothing backs is released | released | `error` |
| `payload.abort()` / direct `execute({ signal })` | as the first row when owned; a direct `execute()` writes nothing | — | job `error` hook |

## Failure handling

### Enqueue errors are not dedupes

An enqueue has two very different "not queued" outcomes, and they must never be collapsed into one:

- **`'deduped'` / `'busy'` / `'full'`** (or `false` from `queue()`) — Redis answered; the run is already in flight, or a limit refused it. Nothing to retry blindly.
- **`RedisJMEnqueueError`** — Redis refused or failed the write. The run is **not** in flight (or, for `connection`/`timeout`, possibly not). Reporting this as "already running" silently loses the trigger.

```typescript
async function trigger(runId: string, inputs: Inputs) {
  try {
    const { status } = await job.enqueue(runId, inputs)
    return status // 'queued' | 'deduped' | 'busy' | 'full'
  } catch (err) {
    if (!(err instanceof RedisJMEnqueueError)) throw err // validation bug
    switch (err.reason) {
      case 'oom':              // Redis is full: shed load and alert — don't retry in a tight loop
      case 'readonly':         // failover in progress: retry after a pause
        throw new ServiceUnavailable(err.message)
      case 'connection':
      case 'timeout':          // ambiguous: retrying with the SAME runId is safe (may return 'deduped')
        return (await job.enqueue(runId, inputs)).status
      case 'inputs-too-large': // store the payload elsewhere and enqueue a reference
        throw err
      default:
        throw err
    }
  }
}
```

Every `RedisJMEnqueueError` also fires the manager-level `enqueueFailed` hook (wire it to metrics), and OOM refusals are counted in `health().oomRefusals`.

### Start-failure recovery

A run can fail between the pop and a fully established execution — the claim write fails because Redis is down or out of memory, or a job-level `start` hook throws. Such a run is never left queued-or-running with its lock held and no queue entry. The manager fires `startFailed` with one of three actions:

| `action` | When | What happened |
|---|---|---|
| `'requeued'` | The claim never landed | The jobId was pushed back to the head of its record's own lane — only while its record is still `queued` and its `claiming` entry still parked, so an overlapping recovery (e.g. maintenance on another instance) never adds a duplicate entry. |
| `'deferred'` | The claim never landed **and** the push-back failed too (e.g. Redis out of memory) | The run is **not lost**: it stays in the `claiming` set (record `queued`, lock held) and maintenance puts it back on its lane once it has sat there past the stale threshold. |
| `'failed'` | The claim landed (or a job-level `start` hook threw) | Routed through the run's normal failure path: retry when attempts remain, else terminal `error`. |

When the claim write reported an error but the record is no longer `queued` (the claim actually landed despite the error, or the run moved on), there is nothing of this pop's to recover: nothing is pushed back, no `startFailed` fires, and the case is only logged.

After a start failure the instance pauses popping briefly (≤ 1s after a requeue; a whole stale threshold after an OOM or a deferral, since every further pop would hit the same wall). Maintenance keeps running meanwhile. Under OOM the pop script itself is refused, so a full Redis pops — and loses — nothing.

### Delivery guarantees

redisjm is **not exactly-once**. Write handlers to be **idempotent** and to observe `ctx.signal`. What you can rely on:

- One run per `runId` holds the lock at a time; a duplicate enqueue is `'deduped'`.
- A run that was popped but never started (pop/claim failure, the popping instance died before claiming) is re-queued — never silently dropped — and executed later.
- A failed or timed-out attempt is retried up to `attempts`.

When a run can execute **more than once**:

- A timed-out attempt — or one abandoned by [`abortGraceMs`](#settling-aborted-runs) — keeps running detached while its retry (or a successor) starts; detached handlers don't count against `concurrency`.
- A handler that pinned the event loop past the stale threshold was staled; a producer re-enqueued the same `runId` and a successor ran while the original was still going (its record writes are fenced, its side effects are not).

When a run can **end without completing**:

- Its instance died mid-execution (or it exceeded `maxRunMs`): maintenance marks it `stale` and releases the lock; it is **not** retried automatically. If the work must complete, re-enqueue it — e.g. let your cron re-trigger the same `runId` (safe: dedupe skips it while it's still in flight).

## Maintenance

Maintenance reclaims what crashes and failures leave behind: stale running records, orphaned locks, abandoned pops, expired history.

### Scheduling and the lock

`start()` runs maintenance on the manager's **own timer** — once immediately, then every `maintenanceInterval` ms (default: the stale threshold; `0` disables) — **not through the job queue**, so it keeps running when every concurrency slot is busy or the queue is backed up. Passes never overlap on one instance, and `stop()` awaits an in-flight pass.

Every instance ticks, but each tick goes through `runMaintenance()`, which first tries `SET redisjm:{tg}:maintenance-lock <token> NX PX <ttl>` (ttl ≈ `maintenanceInterval` minus a small margin). The lock is deliberately **never released** — its expiry spaces passes about one interval apart across the whole fleet, however many instances tick. A tick that loses returns `null`.

Workers that never call `start()` must schedule `runMaintenance()` themselves (see [`runMaintenance`](#runmaintenance-promisemaintenanceresult--null)). `performMaintenance()` runs a full pass without the lock.

### What a full pass does

All **deletions run before any write**, so a pass on a Redis at `maxmemory` frees what it can before the first write is refused. Every Redis operation is individually guarded: a failed write is counted and skipped instead of aborting the pass, and failures are logged once per pass with their classified reason.

1. **Expired history and garbage** — finished/error/stale records older than `keepFinishedInterval` and unparseable/foreign records (plus their locks) are deleted (`cleanedCount`).
2. **Stale running runs** — a `running` record whose heartbeat lapsed past the stale threshold, or (with `maxRunMs`) that has run too long, is marked `stale` and its lock released (`staleCount`).
3. **Abandoned pops** — runs in the `claiming` set longer than the stale threshold that were never claimed (the popping instance died, or its claim failed and couldn't be pushed back) are put back at the **head** of their lane (`requeuedCount`). The run never started, so nothing is lost or executed twice; a late claim by the original popper is fenced.
4. **Orphaned delayed records** — a `delayed` record missing from the delayed set is suspected on one pass (`suspectedAt`) and marked `stale` on a later pass past the stale threshold. Legacy `queued` records written by ≤ 0.1.x get the same check against their lane list (see [Upgrading](#upgrading-from-01x)).
5. **Orphaned locks** — a lock with no backing record is suspected on one pass and released on a later one past the stale threshold (counted in `staleCount`).
6. **Per-job bookkeeping** — prunes per-job lock-set members whose lock is gone, lanes whose list is empty, and jobs with nothing left.

**Compare-and-set writes.** Every write-back is a compare-and-set against the record as read: it lands only if the stored record is still exactly what maintenance scanned, and it never re-creates a deleted record. Deletions are compare-and-delete too — a record replaced by a fresh enqueue of the same `runId` between the scan and the delete is kept. Maintenance therefore never overwrites a concurrent claim, heartbeat, or finish, even at `concurrency > 1` across many instances.

### Bounded passes

Each stage examines at most `maxRecordsPerPass` items (default `1000`), and the log, lock, and job scans resume from **cursors persisted in Redis**, so consecutive passes — on any instance — rotate through a large log instead of each doing an O(n) sweep. Presence checks are pipelined per batch.

The trade-off: a record is examined once per full rotation, so on a large log, detection latency for stale runs (and, below Redis 7.4, history cleanup) is roughly `ceil(logSize / maxRecordsPerPass) × maintenanceInterval`. Size `maxRecordsPerPass` so a rotation stays well under your tolerance — or keep the log small (see `keepFinishedInterval` in [Memory & eviction](#memory--eviction)).

### Emergency mode (Redis out of memory)

If the lock `SET` is refused with an OOM error, `runMaintenance()` runs an **emergency pass** instead: lock-free and **delete-only** — expired terminal records and garbage records are deleted (Redis accepts deletions at `maxmemory`) and the garbage records' locks released; nothing is written. Deletions are idempotent, so emergency passes on several instances at once are safe. It is bounded like a full pass and resumes from a per-instance cursor (the shared cursor can't be written under OOM). Result: `mode: 'emergency'`, `staleCount: 0`, `requeuedCount: 0`.

Any other lock error (connection, timeout, read-only replica) is logged and the tick skipped.

### `MaintenanceResult`

| Field | Meaning |
|---|---|
| `staleCount` | Runs reclaimed as stale (lapsed heartbeat, `maxRunMs`, orphaned delayed/legacy-queued records) plus orphaned locks released |
| `cleanedCount` | Log records deleted (expired terminal records + garbage) |
| `requeuedCount` | Abandoned pops put back on their lane (always `0` in emergency mode) |
| `mode` | `'full'` or `'emergency'` |

After each timer pass the manager also checks memory pressure (see below).

### `maintenance` hook

`manager.hook('maintenance', …)` fires after every pass **this instance ran** — the timer, `runMaintenance()`, `performMaintenance()`, the legacy maintenance job — or when a pass **could not run because of an error**. It does **not** fire for the normal "another instance holds the lock" skip (that would fire on every instance every tick). On the timer it fires before `memoryPressure`. Like every manager hook it is an isolated observer.

| Payload field | Meaning |
|---|---|
| `result` | The `MaintenanceResult` (`mode: 'full' \| 'emergency'`); `null` when the lock read failed or the pass threw |
| `failedOps` | Redis operations that failed and were skipped inside the pass (the pass still completed) |
| `error`, `reason` | The first failure — the thrown error, the lock error, or the first failed operation — and its classified `RedisErrorReason` |
| `durationMs` | Wall-clock ms of the pass itself (lock acquisition excluded); for a lock failure, of the failed lock attempt |

```typescript
manager.hook('maintenance', ({ result, failedOps, reason, durationMs }) => {
  metrics.gauge('redisjm.maintenance.last_pass_ts', Date.now())     // alarm on staleness of this metric
  if (!result || failedOps > 0) metrics.increment('redisjm.maintenance.failures', 1, { reason })
  if (result) metrics.timing('redisjm.maintenance.duration_ms', durationMs)
})
```

## Memory & eviction

redisjm keeps all of its state in Redis with **no key TTLs**. If Redis fills up, the right outcome is that new work is **refused loudly** — not that existing work is silently evicted. Recommended setup:

1. **`maxmemory-policy noeviction`**, ideally on a **dedicated Redis instance** for the queue, so cache-like data elsewhere can't push the queue over the edge (and so eviction settings for caches don't apply to it). `start()` warns about `allkeys-*`/`volatile-*` policies (read from `INFO memory`, so it works on managed services that block `CONFIG`).
2. **An alarm** — wire the `memoryPressure` hook (fires when `used_memory / maxmemory` crosses `memoryWarnRatio`, default `0.8`, once per crossing; checked after each maintenance timer pass, so it needs `start()` with maintenance enabled) and/or poll `health()`.
3. **Bound the inputs** — `maxInputsBytes` rejects oversized payloads before they reach Redis; store large data elsewhere and enqueue a reference.
4. **Bound the queues** — `laneCaps` / `maxQueued` (see [Backpressure](#backpressure)).

**What happens at `maxmemory` under `noeviction`:** enqueues throw `RedisJMEnqueueError` with reason `'oom'` and write nothing; pops are refused up front, so nothing is popped or lost (logged once per episode); a claim that fails is requeued or deferred (see [Start-failure recovery](#start-failure-recovery)); maintenance switches to emergency delete-only passes to free expired history. Running handlers keep running, but their record writes (heartbeats, progress, the final `finish`/`error`) are refused too — a run that completes while Redis is full can't record its outcome and is reclaimed as `stale` once maintenance can write again.

**`keepFinishedInterval` is the main memory knob.** Every terminal record — inputs included — stays in the log that long, so a high-throughput group holds roughly `throughput × keepFinishedInterval` records. The default stays at `60000` because callers commonly poll `get()` for a run's result. Lower it (or set `0`) for high-volume jobs whose outcome you observe through hooks instead. Terminal records are reclaimed twice over: on Redis ≥ 7.4 each gets a hash-field TTL (`HPEXPIRE`) atomically with its terminal write, so it expires even when maintenance can't run; maintenance's sweep reclaims them too (the only mechanism below 7.4).

## Job Statuses

| Status | Description | Holds the lock? | In log? |
|---|---|---|---|
| `queued` | Waiting on a lane queue (or popped, not yet claimed) | Yes | Yes |
| `delayed` | Staged on the delayed set (scheduled run or pending retry) | Yes | Yes |
| `running` | Currently executing with active heartbeat | Yes | Yes |
| `stale` | Reclaimed by maintenance: heartbeat lapsed, `maxRunMs` exceeded, or orphaned. `staleReason` says which. A still-alive run with a lapsed heartbeat self-heals back to `running` on its next heartbeat/`setProgress`/`setAttrs` | No (released when marked) | Kept for `keepFinishedInterval` |
| `finished` | Completed successfully | No | Kept for `keepFinishedInterval` |
| `error` | Failed with an error (final failure) | No | Kept for `keepFinishedInterval` |

> **Terminal states are observable by default.** With the default `keepFinishedInterval: 60000`, `finished`/`error`/`stale` records linger ~60s, so `list()`/`get()` return them for a minute after the run settles. Set `keepFinishedInterval: 0` to opt into the legacy **write-only** behavior — finished/error records are deleted in the same atomic step that ends the run (`stale` records stay until maintenance's next pass), and terminal outcomes are observable only via the `finish`/`error` hooks.

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
const worker = new RedisJM(redis, 'my-app')
worker.createJob({ jobName: 'store-images', lane: 'images' }, storeImagesFn)
worker.start(1000) // polls `images` (+ `__maintenance`) — never the default lane
```

### Produce vs. consume

**Producing a job needs only a `Job` instance and the manager — no registration.** Registration is what auto-subscribes a consumer to a lane. So a pod that must **enqueue** a laned job but must **not run** it (e.g. a web pod that also `start()`s to consume its own default-lane jobs) must **not** register that job — registering it would subscribe the pod to the lane, and it would then pop the heavy work.

To produce without consuming, construct an **unregistered** `Job` and enqueue through it:

```typescript
import { RedisJM, Job } from '@prostojs/redisjm'

const manager = new RedisJM(redis, 'my-app')
manager.createJob({ jobName: 'render-page' }, renderFn) // default-lane job this web pod consumes
manager.start(1000)

// Enqueue image work WITHOUT registering it, so this pod never pops the heavy handler:
const imageJob = new Job<{ url: string }>(
  { jobName: 'store-images', lane: 'images' },
  async () => {}, // never runs on this pod
  manager,
)
await imageJob.enqueue('img-42', { url: 'https://example.com/img.png' })
```

Rule of thumb: **register to consume, construct-without-register to only produce.** The producer's `Job` metadata is what applies at enqueue time (`lane`, `maxQueued`, `maxInFlight`, `maxInputsBytes`), so keep it in sync with the consumer's.

### Lane ordering (`laneStrategy`)

Each poll checks lanes in order and takes the first available job. The reserved `__maintenance` lane is always polled first; the work lanes are ordered by `laneStrategy`; [old lanes](#lane-changes-across-deploys) come last:

- `'roundRobin'` (default) — rotates the work-lane order each poll, so no lane is starved by a busier one.
- `'priority'` — uses `lanePriority`, an explicit high→low list of lane names. Work lanes absent from the list trail in registration order.

```typescript
new RedisJM(redis, 'my-app', {
  laneStrategy: 'priority',
  lanePriority: ['images', 'thumbnails'],
})
```

Lanes at their `laneConcurrency` cap are skipped (see [Per-lane concurrency](#per-lane-concurrency)).

### One lane per handler set

Lanes isolate at **lane** granularity, not per-jobName. If two consumers share a lane but register disjoint job names, each keeps popping names it can't handle and requeues them — reintroducing wasted contention within that lane. The supported pattern is **one lane per disjoint handler set**: every subscriber of a lane should register every jobName on that lane.

### Lane changes across deploys

A queued run keeps the lane it was enqueued on. When a deploy moves a job to a different lane, its already-queued runs stay poppable: every push records the lane's list key in the job's lane set, and consumers of the job also drain the **old** lanes it still has entries on (refreshed every few seconds). Draining an old lane takes only entries of jobs the consumer registers — it never pops (and burns the requeue budget of) other jobs' entries. Maintenance stops tracking an old lane once it is empty.

Caveats:

- Old lanes are polled **after** the current lanes, so under a sustained backlog on the current lanes they can starve until it clears.
- Only the first 100 entries of an old lane are inspected per pop: more than 100 entries of jobs this consumer doesn't register at the head of an old lane block its entries behind them. An old lane still shared with other jobs is never pruned, so each idle poll keeps scanning it.
- Pre-0.2 enqueues don't record lanes — change a job's lane in a **later** deploy than the upgrade to 0.2 (see [Upgrading](#upgrading-from-01x)).

### Adopting lanes on an existing group

Roll a lane-aware version out to **every** instance in the group **before** any producer starts declaring non-default lanes. A pre-lane instance's maintenance loop mishandles laned records — it looks for them on the legacy queue and can wrongly mark them stale — so no laned work should exist until the whole group is lane-aware.

## Operations checklist

- [ ] Redis ≥ 7.0 (≥ 7.4 for history TTLs), standalone primary — not Cluster.
- [ ] `maxmemory` set, `maxmemory-policy noeviction`, ideally a dedicated instance; no startup warning from redisjm about the policy.
- [ ] Memory alarm wired: `memoryPressure` hook and/or `health()` in your metrics (`usedRatio`, `oomRefusals`, `claiming`, queue lengths).
- [ ] `maintenance` hook wired to metrics / run history: alarm when no pass has been seen for a few intervals or `failedOps > 0`.
- [ ] `jobTimeout` (or per-job `timeoutMs`) set for every job that does I/O; handlers pass `ctx.signal` to their I/O and are idempotent.
- [ ] `maxRunMs` set above your slowest healthy run, as a backstop.
- [ ] `abortGraceMs` set if shutdown must be bounded against handlers that ignore `ctx.signal` (and `attempts > 1` for runs that must survive an abort).
- [ ] Producers handle every enqueue outcome: `deduped`/`busy`/`full` vs `RedisJMEnqueueError` — never report an error as "already running". `enqueueFailed` wired to metrics.
- [ ] Queue growth bounded: `laneCaps`/`maxQueued` for lanes fed by external traffic; `maxInputsBytes` set.
- [ ] `keepFinishedInterval` sized for throughput (lower it for high-volume jobs observed via hooks).
- [ ] Maintenance running: every consumer calls `start()` with `maintenanceInterval > 0`, or schedules `runMaintenance()`; `maxRecordsPerPass` sized for the log (see [Bounded passes](#bounded-passes)).
- [ ] Graceful shutdown: `await manager.stop()` on `SIGTERM` before closing Redis.
- [ ] Clocks synced (NTP) across instances.
- [ ] Lane changes shipped in a separate deploy from library upgrades.

## Upgrading to 0.3

0.3 adds features (`abortGraceMs`, `inFlightCount()`, `fleet()`, `listQueued()` / `getMany()`, the `maintenance` hook) and **no Redis format change to existing keys**; Redis ≥ 7.0 and standalone remain the requirements. What to know:

| Change | What to do |
|---|---|
| **`presence` is on by default.** Every instance that calls `start()` now writes one small fleet entry per `heartbeatInterval` and adds two keys per group (`instances`, `instance-info`). | Nothing, if the cost is fine. `presence: false` opts out (and then `fleet()` doesn't see the instance). Set `instanceLabel` (e.g. the pod name) to make entries recognizable. |
| **A fenced `ctx` write no longer emits `update`** — a detached (timed-out / abandoned) handler's `setProgress` / `setAttrs` used to fire the manager `update` event even though the write was rejected; now only a write that landed does. | Nothing; don't rely on `update` events from detached handlers. |
| **`abortGraceMs` is opt-in** (an abort still waits for the handler unless you set it). An abandoned attempt consumes an attempt. | Set it where shutdown or slot-holding must be bounded; see [Settling aborted runs](#settling-aborted-runs). |
| **[`fleet()`](#fleet-promiseredisjmfleet) under-counts during a rolling deploy:** instances older than 0.3.0 never register. | Keep any fallback capacity estimate until every consumer runs ≥ 0.3.0. |
| **[`inFlightCount()`](#inflightcountjobname-promisenumber) drifts with 0.1.x instances** in the group (their enqueues/finishes don't maintain the per-job lock sets) — same as `maxInFlight` / `inFlight()`; exact in a pure ≥ 0.2 group. | Don't gate on it until no 0.1.x instance remains. |
| **[`listQueued()`](#listqueuedoptions-promisequeuedpage) is blind to pre-0.2 consumers' pops** (no `claiming` entry) and to lanes only a pre-0.2 producer wrote, unless the lane is passed explicitly. | Pass `lane` explicitly for such lanes during a mixed deploy. |
| **The `maintenance` hook** only fires on ≥ 0.3.0 instances (0.1.x maintenance, a queued job, never did). | Wire it on the 0.3 instances. |

## Upgrading from 0.1.x

### Breaking and behavioral changes

| Change | What to do |
|---|---|
| **Redis ≥ 7.0 required.** Enqueue, pop, and record transitions are shebang Lua scripts; the `LMPOP` pop and its sequential-`LPOP` fallback for older servers are gone. | Upgrade Redis first. Redis Cluster is not supported. |
| **Maintenance is no longer a queued job.** `start()` runs it on its own timer behind a group lock; `createMaintenanceJob()` is still exported (its handler now calls `runMaintenance()`) and auto-registered for compatibility. | **Remove any `maintenanceJob.queue('', null)` / cron enqueue of the maintenance job.** Every popped entry of it is treated as a sign a pre-0.2 instance is alive and keeps the costlier legacy orphan check on. Use `start()`, or `runMaintenance()` on your own timer. |
| **Maintenance passes are bounded** (`maxRecordsPerPass`) instead of scanning the whole log; `MaintenanceResult` gains `requeuedCount` and `mode`. | Size `maxRecordsPerPass` for large logs. |
| **Enqueue failures throw `RedisJMEnqueueError`** (original error in `cause`) instead of the raw ioredis error. The enqueue is atomic — no rollback, no half-written runs. | Catch `RedisJMEnqueueError` and branch on `reason`; don't map errors to "already queued". |
| **`queue()`/`queueFirst()` `false`** also means `busy`/`full` when you configure `maxInFlight`/caps. | Use `enqueue()` to tell outcomes apart. |
| **Manager-level hooks are isolated observers.** A throwing `manager.hook(...)` handler is logged and no longer affects the run. | Move logic that must be able to fail a run into a job-level hook. |
| **A throwing job-level `start` hook fails the run** through the retry-or-final path (it used to propagate and leave the run to go stale). | Expect `error`/`retry` for such runs. |
| **Start failures are recovered** and reported via the new `startFailed` hook (`requeued` / `deferred` / `failed`). | Wire `startFailed` to monitoring. |
| **`stop()`** also clears `every()` timers, awaits an in-flight maintenance pass and `popAndExecute()` calls, and `popAndExecute()` returns `false` after `stop()` until the next `start()`. | Don't call `popAndExecute()` after `stop()` expecting work. |
| The 0.0.3-era cutover helper that relocated a maintenance entry stranded on the legacy default queue was removed. | Upgrading from ≤ 0.0.3: go through 0.1.x first. |

New records carry `enqueuedAt` and, when staled, `staleReason`; 0.1.x-written records keep working.

### Mixed rolling deploy (0.1.x and 0.2 instances in one group)

Deploy **consumers first or all at once**, and keep the mixed window short. While both versions run:

- **A 0.1.x consumer that dies between its pop and its claim** leaves a 0.2-written run `queued` with its lock held and no queue entry. 0.2 maintenance catches this through a *legacy window* that opens whenever a 0.2 instance pops a maintenance entry enqueued by a 0.1.x instance (and stays open for at least 10 minutes after the last one); while it is open, maintenance also checks 0.2-written queued records against their lane list (`LPOS`) — costlier, but bounded per pass. **Exception:** 0.1.x instances running with `maintenanceInterval: 0` never enqueue maintenance entries, so the window never opens for them; a run they strand stays locked until you `unqueue(jobId)` it.
- **0.1.x maintenance writes are not compare-and-set** and can overwrite a concurrent 0.2 claim; that run's `ctx.signal` aborts (ownership loss) and it ends `stale`.
- **0.1.x enqueues and finishes don't maintain the per-job lock sets**, so `maxInFlight` and `inFlight()` drift (over- or under-counting) until those runs drain and maintenance prunes the sets. Don't rely on `maxInFlight` until the whole group is on 0.2.
- **0.1.x enqueues don't record lanes**, so a 0.2 consumer can't find their runs on an old lane. Change a job's lane only in a later deploy, once no 0.1.x instance is left.
- **`payload.attempt`** is read from the record's `attempt`, which 0.1.x claims increment the same way, so a 0.2 instance claiming a retry whose earlier attempt ran on 0.1.x reports the right number (0.1.x instances' own payloads have no `attempt`).
- **`popAndExecute()`-only workers** (no `start()`) still need `createMaintenanceJob(manager)` registered during the window: 0.1.x instances keep enqueueing the maintenance job on the `__maintenance` lane, which every instance polls.

## Full Example: Distributed Job Processing

```typescript
import Redis from 'ioredis'
import { RedisJM, RedisJMEnqueueError } from '@prostojs/redisjm'

const redis = new Redis(process.env.REDIS_URL)
const manager = new RedisJM(redis, 'my-service', {
  heartbeatInterval: 5000,
  roundsToStale: 2,
  keepFinishedInterval: 60_000,
  concurrency: 4,
  jobTimeout: 10 * 60_000,
  maxRunMs: 60 * 60_000,
  laneCaps: { default: 5000 },
  maxInputsBytes: 64 * 1024,
})

// Define jobs
const reportJob = manager.createJob(
  { jobName: 'daily-report', attempts: 3, backoff: (n) => n * 30_000, maxInFlight: 1 },
  async (inputs: { date: string }, ctx) => {
    await ctx.setAttrs({ step: 'fetching data' })
    await ctx.setProgress(0.3)
    // ... fetch data (pass ctx.signal to cancellable I/O)

    await ctx.setAttrs({ step: 'generating report' })
    await ctx.setProgress(0.7)
    // ... generate report

    await ctx.setProgress(1)
  }
)

// Observability (manager hooks are isolated observers)
manager.hook('enqueueFailed', ({ jobId, reason }) => console.error(`enqueue ${jobId} failed: ${reason}`))
manager.hook('startFailed', ({ jobId, action }) => console.warn(`start of ${jobId} failed: ${action}`))
manager.hook('memoryPressure', (h) => console.error(`Redis memory at ${h.usedRatio}`))

// Start processing (maintenance runs on its own timer — nothing else to schedule)
manager.start(1000)

// CRON handler (runs on every instance; the date runId dedupes across instances)
async function onCron() {
  const today = new Date().toISOString().slice(0, 10)
  try {
    const { status } = await reportJob.enqueue(today, { date: today })
    if (status !== 'queued') console.log(`report ${today}: ${status}`) // deduped / busy / full
  } catch (err) {
    if (err instanceof RedisJMEnqueueError) console.error(`report ${today} NOT queued: ${err.reason}`)
    else throw err
  }
}

// Graceful shutdown — await the drain so in-flight jobs finish and their locks release
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

// Enqueue on a specific manager
await job.enqueue('run-1', { source: 'api' }, managerB)
```

Each event payload carries the `manager` that drove the execution, so when two managers in one process share a Job (and even a target group), only the driving manager's hooks act on a given run's events — no double writes or double-dispatched events.

## Types

All types are exported from the package root. The short ones:

```typescript
type JobAttrValue = string | number | boolean | null | undefined
type JobAttrs = Record<string, JobAttrValue>
type JobStatus = 'queued' | 'running' | 'finished' | 'error' | 'stale' | 'delayed'
type LaneStrategy = 'roundRobin' | 'priority'
type RedisJMLogger = (message: string, error?: Error) => void
type RedisErrorReason = 'oom' | 'readonly' | 'connection' | 'timeout' | 'unknown'
type EnqueueErrorReason = RedisErrorReason | 'inputs-too-large'

interface EnqueueResult { status: 'queued' | 'deduped' | 'busy' | 'full'; jobId: string }
interface QueueOptions { delay?: number }
interface EnqueueOptions extends QueueOptions { first?: boolean }
interface StopOptions { abort?: boolean }
```

| Type | Describes |
|---|---|
| `RedisJMOptions` / `ResolvedRedisJMOptions` | Constructor options / the same with defaults applied (`getOptions()`, everything except `logger`) — see the [options table](#constructor) |
| `JobMetadata` | Job definition — see [Job metadata](#job-metadata) |
| `JobFunction`, `JobContext`, `JobExecuteOptions` | Handler signature, its `ctx`, and `execute()` options |
| `JobLogRecord` | A log record: `jobId`, `jobName`, `runId`, `inputs`, `targetGroup`, `lane?`, `status`, `progress`, `attrs?`, `error?`, timestamps (`startedAt`, `finishedAt`, `heartbeat`, `readyAt`, `enqueuedAt`), `attempt`, `executionId`, `requeueCount`, `suspectedAt`, `staleReason` (`'heartbeat' \| 'orphaned' \| 'maxRunMs'`) |
| `JobHooks`, `RedisJMHooks` | Job-level and manager-level hook maps — see [Events](#events) |
| `JobEventPayload`, `JobErrorEventPayload`, `JobRetryEventPayload`, `JobTimeoutEventPayload`, `JobUpdateEventPayload` | Run-event payloads |
| `EnqueueFailedEventPayload`, `StartFailedEventPayload` | `enqueueFailed` / `startFailed` payloads |
| `EveryOptions` | `every()` options |
| `InFlightCounts` | `inFlight()` result `{ total, queued, delayed, running }` |
| `ListPageOptions`, `ListPage` | `listPage()` options / result |
| `ListQueuedOptions`, `QueuedEntry`, `QueuedPage` | `listQueued()` options / entry `{ jobId, jobName, runId, lane?, status, poppedAt?, readyAt? }` / page `{ entries, nextOffset?, complete }` |
| `FleetInstance`, `RedisJMFleet` | `fleet()` result — see [`fleet()`](#fleet-promiseredisjmfleet) |
| `MaintenanceEventPayload` | `maintenance` hook payload `{ result, failedOps, error?, reason?, durationMs }` |
| `RedisJMHealth` | `health()` result |
| `RedisJMStats` | `stats()` result `{ queues, delayed, locks, statuses }` |
| `MaintenanceResult` | `{ staleCount, cleanedCount, requeuedCount, mode }` |

Exported classes, functions and values: `RedisJM`, `Job`, `RedisJMEnqueueError`, `JobTimeoutError`, `JobAbortedError`, `RunSupersededError`, `classifyRedisError`, `createMaintenanceJob`, `MAINTENANCE_JOB_NAME` (`'__redisjm_maintenance'`), and `MAINTENANCE_LANE` (`'__maintenance'`).

## License

MIT
