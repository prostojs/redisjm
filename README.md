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
- Polling-based job execution with `start` / `stop`
- Target group isolation — multiple app groups can share the same Redis instance
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

Three Redis structures per target group:

| Key pattern | Redis type | Purpose |
|---|---|---|
| `redisjm:{tg}:queue` | List | Ordered queue of jobId strings (RPUSH/LPUSH + LPOP) |
| `redisjm:{tg}:locks` | Set | Locked jobIds (queued + running + stale). Atomic via SADD |
| `redisjm:{tg}:log` | Hash | jobId -> JSON record with full job state |

## API Reference

### `RedisJM`

The main job manager class. Uses Redis to manage job queues, locks, and the job log.

#### Constructor

```typescript
new RedisJM(redis: Redis, targetGroup: string, options?: RedisJMOptions)
```

- `redis` -- An [ioredis](https://github.com/redis/ioredis) client instance
- `targetGroup` -- A string prefix for all Redis keys; only clients sharing the same target group share queues and locks
- `options` -- Optional configuration:

| Option | Default | Description |
|---|---|---|
| `heartbeatInterval` | `5000` | Milliseconds between heartbeat updates during job execution |
| `roundsToStale` | `2` | Number of missed heartbeat intervals before a job is considered stale |
| `keepFinishedInterval` | `0` | Milliseconds to keep finished/error/stale records in the log (0 = remove immediately) |
| `maintenanceInterval` | `heartbeatInterval * roundsToStale` | Milliseconds between automatic maintenance enqueues while `start()` is polling (0 = disable auto-maintenance) |

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

##### `queue<TInputs>(job, runId, inputs): Promise<boolean>`

Adds a job run to the end of the queue. Returns `true` if successfully queued, `false` if the jobId is already locked (queued, running, or stale).

```typescript
const success = await manager.queue(job, 'order-123', { orderId: '123' })
```

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

Returns all job log records (all statuses).

```typescript
const records = await manager.list()
```

##### `unqueue(jobId): Promise<void>`

Removes a job from the queue, locks, and log entirely.

```typescript
await manager.unqueue('process-order#order-123')
```

##### `popAndExecute(): Promise<boolean>`

Pops the next job from the queue, matches it to a registered Job instance by name, and executes it. Returns `true` if a job was popped (even if execution failed), `false` if the queue was empty.

If the job name is not registered, the log record is set to status `"error"` with error `"Job name is unknown"`.

##### `start(interval): void`

Starts a polling loop that calls `popAndExecute()`. When a job is executed, the next poll fires immediately. When the queue is empty, waits `interval` ms before the next poll.

Unless `maintenanceInterval` is `0`, `start()` also auto-wires maintenance: it enqueues the built-in maintenance job once immediately (so locks orphaned by a crashed instance are reclaimed soon after restart) and then every `maintenanceInterval` ms. All instances enqueue concurrently — the lock ensures only one maintenance run executes at a time.

```typescript
manager.start(1000) // poll every 1 second when idle
```

##### `stop(): void`

Stops the polling loop and the auto-maintenance timer. The currently executing job (if any) will finish.

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

### `createMaintenanceJob(manager): Job<null>`

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

```typescript
manager.hook('start', (payload) => {
  console.log(`Job ${payload.job.getName()} started`)
})

manager.hook('error', (payload) => {
  console.error(`Job failed:`, payload.error)
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

// Graceful shutdown
process.on('SIGTERM', () => {
  manager.stop()
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

interface JobMetadata {
  jobName: string
  description?: string
}

interface RedisJMOptions {
  heartbeatInterval?: number    // default 5000
  roundsToStale?: number        // default 2
  keepFinishedInterval?: number // default 0
  maintenanceInterval?: number  // default heartbeatInterval * roundsToStale; 0 disables
}

interface JobLogRecord<TInputs = unknown, TAttrs extends JobAttrs = JobAttrs> {
  jobId: string
  jobName: string
  runId: string
  inputs: TInputs
  targetGroup: string
  status: JobStatus
  startedAt?: number
  finishedAt?: number
  heartbeat?: number
  progress: number
  attrs?: TAttrs
  error?: string
  suspectedAt?: number          // internal: orphaned-queued suspect timestamp (maintenance)
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

## License

MIT
