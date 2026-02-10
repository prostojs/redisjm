# Redis Job Manager

package @prostojs/redisjm

## Purpose

Job Manager powered by redis for k8s-like installations that use multiple instances of same app.
It uses redis to push jobs into a queue.
Each job run can be picked up only by one instance.
App instances can have CRON-job to add job-runs into queue, so many apps can simultaniously attempt to add the same job-run to the queue. Redisjm takes care of locking runId to make sure that the job-run is added only once.

Each app instance processes one job at a time using `RedisJM.start()` to begin polling the queue. When an app instance is shut down or reloaded, an in-progress job may be abandoned. The library detects such stale jobs via heartbeats and provides a maintenance job to clean them up.

Glossary:
 - job - a function with metadata (jobName, inputs, status, ...)
 - job-run - a particular job execution that has it's runId (usually it is serialized inputs)
 - jobId - jobName + runId (separated by '#')
 - jobs-queue - an ordered list of jobId strings, supporting FIFO with priority insert
 - locks - a set of jobId strings currently blocked from re-queuing (includes queued, running, stale)
 - log - a hash of job records with full state (status, progress, attrs, heartbeat, timestamps)

Features:
 - uses redis to manage job-queues, locks, and job logs
 - ensures only one job runId is scheduled; subsequent jobs with same runId are rejected
 - if a jobId is running or stale, it still blocks the queue
 - dispatches events: start, finish, error, heartbeat, update
 - supports async job functions
 - heartbeat mechanism to detect stale/abandoned jobs
 - progress tracking and custom attributes per job
 - built-in maintenance job for stale detection and log cleanup

## Stack

 - Typescript
 - pnpm
 - husky for commit lint

## Redis Key Structure

Three separate Redis structures per target group:

| Key pattern | Redis type | Purpose |
|---|---|---|
| `redisjm:{tg}:queue` | List | Ordered queue of jobId strings. RPUSH for normal, LPUSH for priority, LPOP to pop. |
| `redisjm:{tg}:locks` | Set | Set of currently locked jobIds. SADD is atomic — replaces the old NX lock pattern. |
| `redisjm:{tg}:log` | Hash | jobId → JSON record with full job state (status, progress, attrs, timestamps). |

Queue flow:
1. `SADD locks jobId` → returns 0 if already locked → return false
2. `RPUSH queue jobId` (or `LPUSH` for queueFirst)
3. `HSET log jobId → {status: "queued", ...}`

## Implementation

### RedisJM Options

RedisJM constructor accepts an optional options object:
- `heartbeatInterval` — milliseconds interval for heartbeat updates (default 5000)
- `roundsToStale` — number of heartbeat intervals without update before a job is considered stale (default 2)
- `keepFinishedInterval` — milliseconds before finished/error/stale job records are removed from the log (default 0 — removed immediately)

### Class RedisJM

Accepts instance of redis client, a `targetGroup` string, and optional options in the constructor.
`targetGroup` is used to prefix all redis artifacts, so only the clients with same target group can share the queue and locks.

RedisJM extends hookable from unjs to dispatch events.

RedisJM tracks registered Job instances by jobName. jobName must be unique among registered jobs.

RedisJM Methods:
 - `isQueued(jobId)` — checks if jobId is in the locks set (blocked from re-queuing)
 - `queue(job, runId, inputs)` — SADD lock, RPUSH queue, HSET log with status "queued". Returns true/false.
 - `queueFirst(job, runId, inputs)` — same as queue but uses LPUSH (priority insert at front)
 - `list()` — returns all log records (JobLogRecord[])
 - `unqueue(jobId)` — removes from queue list (LREM), locks set (SREM), and log hash (HDEL)
 - `createJob(metadata, fn)` — creates Job instance and registers it
 - `registerJob(job)` — registers by jobName (must be unique), hooks on all events to handle Redis updates and re-dispatch
 - `unregisterJob(job)` — removes hooks and unregisters
 - `getTargetGroup()` — returns target group id
 - `popAndExecute()` — LPOP from queue, match jobId to registered Job by jobName, execute. If jobName is unknown: set log status to "error" with error "Job name is unknown", remove from locks. Returns true if popped something, false if queue empty.
 - `start(interval)` — starts poll loop that calls popAndExecute. Uses recursive setTimeout: delay=0 after successful pop (process next immediately), delay=interval when queue is empty.
 - `stop()` — stops the poll loop
 - `performMaintenance()` — scans log for stale and expired records (see Maintenance section)

### Class Job

Accepts job metadata, a job function, and an optional RedisJM instance (default manager).
Job has two generics: `Job<TInputs, TAttrs>` where TAttrs extends `Record<string, string | number | boolean | null | undefined>`.

Job extends hookable from unjs to dispatch events.

Job Methods:
- `execute(inputs, options?)` — options: `{ targetGroup?, heartbeatInterval?, runId? }`. Sets up heartbeat timer (if heartbeatInterval provided), creates context callbacks (setProgress, setAttrs), dispatches start event, calls job function with (inputs, ctx), dispatches finish/error. Clears heartbeat timer on completion.
- `queue(runId, inputs, manager?)` — delegates to RedisJM.queue
- `getJobId(runId)` — returns "jobName#runId"
- `getMetadata()` — returns job metadata
- `getName()` — returns jobName

Job function signature: `(inputs: TInputs, ctx: { setProgress, setAttrs }) => void | Promise<void>`
- `setProgress(n)` — accepts 0-1, dispatches 'update' event. Returns Promise<void>.
- `setAttrs(attrs)` — accepts TAttrs, dispatches 'update' event. Returns Promise<void>.

Job events:
- `start` — before calling job function. Payload: { job, targetGroup, runId, inputs }
- `finish` — after job function completes. Payload: { job, targetGroup, runId, inputs }
- `error` — on error during execution. Payload: { job, targetGroup, runId, inputs, error }
- `heartbeat` — on each heartbeat tick. Payload: { job, targetGroup, runId, inputs }
- `update` — on setProgress/setAttrs. Payload: { job, targetGroup, runId, inputs, progress?, attrs? }

### RedisJM Event Handling

When a registered Job dispatches events and targetGroup matches, RedisJM:
- `start` → updates log record: status="running", startedAt=now, heartbeat=now. Re-dispatches on self.
- `heartbeat` → updates log record: heartbeat=now. Re-dispatches on self.
- `update` → updates log record: progress and/or attrs. Re-dispatches on self.
- `finish` → updates log record: status="finished", finishedAt=now. SREM from locks. If keepFinishedInterval=0, HDEL from log. Re-dispatches on self.
- `error` → updates log record: status="error", error message, finishedAt=now. SREM from locks. If keepFinishedInterval=0, HDEL from log. Re-dispatches on self.

### Job Statuses

- `queued` — in queue, waiting to be picked up
- `running` — currently executing, heartbeat active
- `finished` — completed successfully
- `error` — failed with an error
- `stale` — detected as abandoned (heartbeat expired), set by maintenance job

### Job Log Record

Stored as JSON in the log hash:
```
{
  jobId, jobName, runId, inputs, targetGroup,
  status: "queued" | "running" | "finished" | "error" | "stale",
  startedAt?: number,
  finishedAt?: number,
  heartbeat?: number,
  progress: number (0-1),
  attrs?: TAttrs,
  error?: string
}
```

### Maintenance

`performMaintenance()` on RedisJM scans the log and:
1. For records with status "running": if `now - heartbeat > heartbeatInterval * roundsToStale`, mark as "stale", set finishedAt=now, SREM from locks.
2. For records with status "finished", "error", or "stale": if `now - finishedAt > keepFinishedInterval`, HDEL from log.

Returns `{ staleCount, cleanedCount }`.

A pre-defined `createMaintenanceJob(manager)` factory is exported. It returns a Job that calls `manager.performMaintenance()` and automatically registers it with the manager. Maintenance is idempotent, so use an empty `runId` to ensure at most one is queued/running at a time (the lock is released on completion, allowing re-queue):
```ts
const maintenanceJob = createMaintenanceJob(manager)
// Periodically try to queue maintenance (all instances, only one succeeds)
setInterval(() => maintenanceJob.queue('', null), 30000)
```

## Notes

Same Job instance can be attached to different RedisJM instances, but it keeps the default one forwarded via constructor.

Events re-dispatching is filtered by `targetGroup`.

Preferable way to instantiate Job is using RedisJM.createJob method.

Job Inputs must be a serializable JSON.

Need to pay attention on proper TS types, generics. Job has two generics: TInputs and TAttrs. When passed into RedisJM methods, types must be inferred from the Job instance.

## Testing

vitest unit-tests

## Build/Package

Use rolldown basic bundling with externalized dependencies, and rollup with `rollup-plugin-dts` plugin to build types. Destination: `dist`
Formats: mjs, cjs.

example of build command:
"build": "rolldown -c rolldown.config.ts && tsc && rollup -c rollup.config.js && rm -rf .types"

For reference use these files:
https://raw.githubusercontent.com/prostojs/urlql/refs/heads/main/rollup.config.js
https://raw.githubusercontent.com/prostojs/urlql/refs/heads/main/rolldown.config.ts
https://raw.githubusercontent.com/prostojs/urlql/refs/heads/main/package.json

and for husky
https://raw.githubusercontent.com/prostojs/urlql/refs/heads/main/.husky/commit-msg
https://raw.githubusercontent.com/prostojs/urlql/refs/heads/main/.commitlintrc.js

## Publish

use pnpm features
scripts in package.json:
- release - runs a sequence of patch version, build, test, add version tag, publish
- release:minor - same as release but increases minor version
- release:major - same as above but increases the major version.
- build
- test

package.json must include dist in 'files',
example
```
  "type": "module",
  "main": "dist/index.cjs",
  "module": "dist/index.mjs",
  "types": "dist/index.d.ts",
  "sideEffects": false,
  "exports": {
    "./package.json": "./package.json",
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.mjs",
      "require": "./dist/index.cjs"
    }
  },
```

# Documentation

Extensive documentation with examples in README.md
