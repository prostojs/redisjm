# @prostojs/redisjm

Redis Job Manager for distributed job queues in Kubernetes-like multi-instance environments.

When running multiple instances of the same application, `@prostojs/redisjm` ensures that job runs are queued exactly once and picked up by only one instance. It uses Redis for queue management, distributed locking, heartbeat monitoring, and job lifecycle tracking.

## Features

- Redis-backed job queue with atomic distributed locking (Redis Set + `SADD`)
- Ensures only one job `runId` is scheduled — running and stale jobs also block the queue
- Priority queue support (`queueFirst` for urgent jobs)
- Automatic heartbeat monitoring to detect stale/abandoned jobs
- Progress tracking (0-1) and custom attributes per job
- Event system (start, finish, error, heartbeat, update) built on [hookable](https://github.com/unjs/hookable)
- Built-in maintenance job for stale detection and log cleanup
- Polling-based job execution with `start` / awaitable `stop` (graceful drain)
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
| `redisjm:{tg}:locks` | Set | Locked jobIds (queued + running + stale). Atomic via SADD |
| `redisjm:{tg}:log` | Hash | jobId -> JSON record with full job state |

> A job with **no lane** keeps the exact legacy key `redisjm:{tg}:queue`; only a named lane gets its own `:lane:{lane}:queue` list. The locks set and log hash stay **group-wide** across all lanes — one lock namespace and a single monitoring pane for every worker type.

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
| `keepFinishedInterval` | `0` | Milliseconds to keep finished/error/stale records in the log (0 = remove immediately — see the caveat under [Job Statuses](#job-statuses)) |
| `maintenanceInterval` | `heartbeatInterval * roundsToStale` | Milliseconds between automatic maintenance enqueues while `start()` is polling (0 = disable auto-maintenance) |
| `unknownJobRequeueLimit` | `5` | Times a job whose name isn't registered on the popping instance is re-queued (lock held) for a sibling instance before being dropped as an error. `0` restores the legacy drop-on-first-pop behavior |
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

Pass an optional `lane` in the metadata to route the job to a named sub-queue serviced only by consumers that register it — see [Lanes](#lanes).

##### `queue<TInputs>(job, runId, inputs): Promise<boolean>`

Adds a job run to the end of the queue. Returns `true` if successfully queued, `false` if the jobId is already locked (queued, running, or stale).

```typescript
const success = await manager.queue(job, 'order-123', { orderId: '123' })
```

> **Dedupe is on the lock, not the log record.** `queue()` dedupes on `jobName#runId` in the locks set, which releases on finish/error (or when maintenance reclaims an orphan). Two consequences for integrators:
> - **Always check the boolean return.** `false` means "already locked" — a caller that ignores it reports false success.
> - **A reused `runId` can be swallowed by a stale/orphaned lock after a hard kill** until maintenance reclaims it (bounded by `maintenanceInterval`, or indefinitely if auto-maintenance is off). For idempotent *manual* triggers where you want a fresh run regardless, prefer a unique `runId` per trigger (e.g. append a timestamp). This does **not** apply to the maintenance job, which deliberately uses a stable empty `runId` to self-dedupe.

##### `queueFirst<TInputs>(job, runId, inputs): Promise<boolean>`

Same as `queue` but adds to the front of the queue (priority insert).

```typescript
await manager.queueFirst(job, 'urgent-order', { orderId: '456' })
```

##### `isQueued(jobId): Promise<boolean>`

Checks if a jobId is currently locked (queued, running, or stale).

```typescript
const locked = await manager.isQueued('process-order#order-123')
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

> With the default `keepFinishedInterval: 0`, a successful run's record is deleted the instant it finishes, so `get()` returns `undefined` — `undefined` means "no record", **not** "never ran". Set `keepFinishedInterval > 0` to observe terminal states, or wire the `finish`/`error` hooks.

##### `unqueue(jobId): Promise<void>`

Removes a job from the queue, locks, and log entirely.

```typescript
await manager.unqueue('process-order#order-123')
```

##### `popAndExecute(): Promise<boolean>`

Pops the next job from the queue, matches it to a registered Job instance by name, and executes it. Returns `true` if a job was popped (even if execution failed), `false` if the queue was empty.

If the job name is not registered on this instance, it is re-queued (lock held) up to `unknownJobRequeueLimit` times so a sibling instance that *does* register the handler — e.g. a freshly deployed pod — can claim it. Once the budget is exhausted the record is set to status `"error"` with error `"Job name is unknown"` and the lock is released. Handler failures, unknown-name drops, and missing-record drops are reported through the configured `logger`.

##### `start(interval): void`

Starts a polling loop that calls `popAndExecute()`. When a job is executed, the next poll fires immediately. When the queue is empty, waits `interval` ms before the next poll. Throws `TypeError` if `interval` is not a positive number.

Unless `maintenanceInterval` is `0`, `start()` also auto-wires maintenance: it first proactively reclaims a maintenance lock orphaned by a hard-killed instance (which would otherwise deadlock — maintenance can't reclaim its own lock), then enqueues the built-in maintenance job once immediately and every `maintenanceInterval` ms thereafter. All instances enqueue concurrently — the lock ensures only one maintenance run executes at a time.

```typescript
manager.start(1000) // poll every 1 second when idle
```

##### `stop(): Promise<void>`

Stops the polling loop and the auto-maintenance timer, and resolves once the in-flight job (if any) has settled — so a shutdown handler can `await` a graceful drain before exiting. For locks to release cleanly, prefer graceful shutdown (`stop()` on `SIGTERM`); a hard `SIGKILL` leaves orphaned locks that maintenance reclaims after the stale threshold.

##### `performMaintenance(): Promise<MaintenanceResult>`

Scans the job log and:
1. Marks running jobs as `"stale"` if `now - lastHeartbeat > heartbeatInterval * roundsToStale`, removes their lock
2. Marks orphaned queued jobs as `"stale"` and removes their lock — a `queued` record that is no longer in the queue list was popped by an instance that died before the `start` event fired. Detection is two-pass to avoid racing the normal pop→start window: the first scan stamps `suspectedAt` on the record; a later scan reclaims it if it is still orphaned after the stale threshold.
3. Removes finished/error/stale log records older than `keepFinishedInterval`

Returns `{ staleCount, cleanedCount }`.

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
    setProgress: (progress: number) => Promise<void>  // 0-1
    setAttrs: (attrs: TAttrs) => Promise<void>
  }
) => void | Promise<void>
```

#### Methods

##### `execute(inputs, options?): Promise<void>`

Runs the job function with heartbeat timer and context callbacks. Options:

```typescript
interface JobExecuteOptions {
  targetGroup?: string
  heartbeatInterval?: number  // enables automatic heartbeat events
  runId?: string              // explicit runId (otherwise derived from inputs)
}
```

##### `queue(runId, inputs, manager?): Promise<boolean>`

Convenience method -- delegates to `RedisJM.queue`.

##### `getJobId(runId): string`

Returns `"jobName#runId"`.

##### `getMetadata(): JobMetadata` / `getName(): string`

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

Both `Job` and `RedisJM` emit events via [hookable](https://github.com/unjs/hookable):

| Event | Payload | Description |
|---|---|---|
| `start` | `{ job, targetGroup, runId, inputs }` | Before job function runs |
| `finish` | `{ job, targetGroup, runId, inputs }` | After successful completion |
| `error` | `{ job, targetGroup, runId, inputs, error }` | On error (also rethrows) |
| `heartbeat` | `{ job, targetGroup, runId, inputs }` | Periodic heartbeat tick |
| `update` | `{ job, targetGroup, runId, inputs, progress?, attrs? }` | On setProgress/setAttrs |

`RedisJM` re-dispatches events only when `targetGroup` matches and updates the Redis log accordingly.

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
- **Don't blanket-`DEL` the Redis keys.** The `:queue`, `:locks`, and `:log` keys are shared by **every** job in the target group — deleting one wipes all runs in the group, not just yours. Use `unqueue(jobId)` to remove a single run.

### Long-running handlers & heartbeats

Heartbeats are emitted from a timer on the single-threaded event loop. A long, tightly synchronous handler that never `await`s starves that timer, so heartbeats stop landing and maintenance may mark the run `stale` and release its lock — which can let another instance pick up the **same** `runId` and run it concurrently (duplicate execution). Chunk long work and `await` between chunks (e.g. `setProgress`/`setAttrs` per chunk) so heartbeats can fire:

```typescript
manager.createJob({ jobName: 'backfill' }, async (inputs: { ids: string[] }, ctx) => {
  for (let i = 0; i < inputs.ids.length; i += 100) {
    await processChunk(inputs.ids.slice(i, i + 100)) // yields to the loop → heartbeat lands
    await ctx.setProgress(Math.min(1, (i + 100) / inputs.ids.length))
  }
})
```

## Job Statuses

| Status | Description | Blocks queue? | In log? |
|---|---|---|---|
| `queued` | Waiting in queue | Yes | Yes |
| `running` | Currently executing with active heartbeat | Yes | Yes |
| `stale` | Heartbeat expired, detected by maintenance | Yes (until maintenance cleans it) | Yes |
| `finished` | Completed successfully | No | Kept for `keepFinishedInterval` |
| `error` | Failed with an error | No | Kept for `keepFinishedInterval` |

> **Default `keepFinishedInterval: 0` makes terminal states write-only.** With the default, `finished`/`error`/`stale` records are deleted the instant they're set, so `list()`/`get()` only ever return `queued`/`running` — terminal outcomes are observable only via the `finish`/`error` hooks. Set `keepFinishedInterval > 0` (e.g. `60000`) to keep them queryable.

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

## Types

```typescript
type JobAttrs = Record<string, string | number | boolean | null | undefined>
type JobStatus = 'queued' | 'running' | 'finished' | 'error' | 'stale'
type LaneStrategy = 'roundRobin' | 'priority'

interface JobMetadata {
  jobName: string
  description?: string
  lane?: string                    // named sub-queue; omitted → default (legacy) lane
}

type RedisJMLogger = (message: string, error?: Error) => void

interface RedisJMOptions {
  heartbeatInterval?: number       // default 5000
  roundsToStale?: number           // default 2
  keepFinishedInterval?: number    // default 0
  maintenanceInterval?: number     // default heartbeatInterval * roundsToStale; 0 disables
  unknownJobRequeueLimit?: number  // default 5; 0 = drop unknown names immediately
  laneStrategy?: LaneStrategy      // default 'roundRobin'
  lanePriority?: string[]          // default []; high→low lane order for 'priority'
  logger?: RedisJMLogger | false   // default console.error; false to silence
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
  suspectedAt?: number          // internal: orphaned-queued suspect timestamp (maintenance)
  requeueCount?: number         // internal: times re-queued for an unknown job name
}

interface JobContext<TAttrs extends JobAttrs = JobAttrs> {
  setProgress: (progress: number) => Promise<void>
  setAttrs: (attrs: TAttrs) => Promise<void>
}

type JobFunction<TInputs, TAttrs extends JobAttrs> = (
  inputs: TInputs,
  ctx: JobContext<TAttrs>,
) => void | Promise<void>

interface MaintenanceResult {
  staleCount: number
  cleanedCount: number
}
```

The `LaneStrategy` type and the reserved-lane constant `MAINTENANCE_LANE` (`'__maintenance'`) are also exported for use when configuring or inspecting lanes.

## License

MIT
