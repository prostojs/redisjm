# Redis Job Manager

package @prostojs/redisjm

## Purpose

Job Manager powered by redis for k8s-like installations that use multiple instances of same app.
It uses redis to push jobs into a queue.
Each job run can be picked up only by one instance.
App instances can have CRON-job to add job-runs into queue, so many apps can simultaniously attempt to add the same job-run to the queue. Redisjm takes care of locking runId to make sure that the job-run is added only once.

Each app instance polls the queue via `RedisJM.start()` and executes up to `concurrency` job-runs at a time (default 1). When an app instance is shut down or reloaded, an in-progress job may be abandoned. The library detects such stale jobs via heartbeats (fencing re-enqueued successors against zombie writes) and provides a maintenance job to clean them up.

Glossary:
 - job - a function with metadata (jobName, inputs, status, ...)
 - job-run - a particular job execution that has it's runId (usually it is serialized inputs)
 - jobId - jobName + runId (separated by '#')
 - jobs-queue - an ordered list of jobId strings, supporting FIFO with priority insert (per lane)
 - locks - a set of jobId strings currently blocked from re-queuing (includes queued, delayed, running, stale)
 - log - a hash of job records with full state (status, progress, attrs, heartbeat, timestamps, executionId, attempt)
 - delayed set - a sorted set of jobId → ready-at ms (scheduled runs + pending retries)
 - executionId - per-run fencing token; the record's owner is the execution whose start hook stamped it
 - lane - a named sub-queue within a target group; workers poll only the lanes of their registered jobs

Features:
 - uses redis to manage job-queues, locks, delayed set, and job logs
 - ensures only one job runId is scheduled; subsequent jobs with same runId are rejected
 - if a jobId is delayed, running, or stale, it still blocks the queue
 - dispatches events: start, finish, error, retry, heartbeat, update
 - supports async job functions
 - heartbeat mechanism to detect stale/abandoned jobs
 - execution fencing (per-run executionId) so a zombie run can't clobber a re-enqueued successor
 - delayed runs and automatic retries with configurable backoff (attempts / backoff metadata)
 - per-instance concurrency (concurrency option) and cooperative cancellation (ctx.signal)
 - progress tracking and custom attributes per job
 - built-in maintenance job for stale detection, orphaned-lock reclaim, and log cleanup
 - lanes: heterogeneous workers share one group/log/maintenance loop, each polls only its lanes

## Stack

 - Typescript
 - pnpm
 - husky for commit lint

## Redis Key Structure

Redis structures per target group:

| Key pattern | Redis type | Purpose |
|---|---|---|
| `redisjm:{tg}:queue` | List | Ordered queue of jobId strings for the default lane. RPUSH for normal, LPUSH for priority; popped via LMPOP (Redis ≥ 7) with sequential LPOP fallback. |
| `redisjm:{tg}:lane:{lane}:queue` | List | Ordered queue for a named lane. Default lane keeps the legacy `:queue` key for back-compat. |
| `redisjm:{tg}:locks` | Set | Set of currently locked jobIds (queued + delayed + running + stale). SADD is atomic. |
| `redisjm:{tg}:log` | Hash | jobId → JSON record with full job state. |
| `redisjm:{tg}:delayed` | Sorted Set | jobId → ready-at ms. Scheduled (delay) runs and pending retries. Group-wide; record's `lane` routes on promotion. |
| `redisjm:{tg}:suspects` | Hash | jobId → first-seen ms. Backs the two-pass orphaned-lock reclaim (stage 4 of maintenance). |

Queue flow (immediate):
1. `SADD locks jobId` → returns 0 if already locked → return false
2. `HSET log jobId → {status: "queued", ...}` (record written BEFORE the queue entry, so a poller never sees an entry with no record)
3. `RPUSH lane-queue jobId` (or `LPUSH` for queueFirst)

Queue flow (delayed, `delay > 0`):
1. `SADD locks jobId` (dedupe still applies while delayed)
2. `HSET log jobId → {status: "delayed", readyAt, ...}`
3. `ZADD delayed readyAt jobId`

On a crash between the HSET and the queue/delayed write, the record is left `queued`/`delayed` but absent from its structure — reclaimed by maintenance (two-pass). A crash between SADD and HSET leaves a record-less lock — reclaimed by maintenance stage 4.

## Implementation

### RedisJM Options

RedisJM constructor accepts an optional options object:
- `heartbeatInterval` — milliseconds interval for heartbeat updates (default 5000)
- `roundsToStale` — number of heartbeat intervals without update before a job is considered stale (default 2)
- `keepFinishedInterval` — milliseconds terminal (finished/error/stale) records linger so get()/list() can observe them (default 60000; `0` = legacy write-only, dropped the instant the job leaves running)
- `maintenanceInterval` — ms between auto-maintenance enqueues while polling (default heartbeatInterval * roundsToStale; 0 disables)
- `unknownJobRequeueLimit` — times an unregistered job name is re-queued (lock held) for a sibling before being dropped as error (default 5; 0 = drop on first pop)
- `concurrency` — max simultaneous runs per instance (default 1; positive integer, floored; throws TypeError on <1 or non-finite)
- `laneStrategy` / `lanePriority` — lane poll ordering (`roundRobin` default, or `priority` with an explicit high→low list)
- `logger` — sink `(message, error?) => void` for operational errors (default console.error; `false` silences)

Job metadata additionally accepts:
- `lane` — named sub-queue (default lane when omitted)
- `attempts` — total tries incl. first before final failure (default 1 = no retries; floored, clamped ≥ 1 via Job.getAttempts())
- `backoff` — ms before the next retry, fixed number or fn(failedAttempt)→ms (default 0; negatives/non-finite clamped to 0 via Job.getBackoffMs())

Queue options (`queue`/`queueFirst`/`Job.queue`):
- `delay` — ms to stage the run on the delayed set instead of the live queue (finite ≥ 0; `delay > 0` with queueFirst throws)

Stop options:
- `abort` — when true, aborts every in-flight run's ctx.signal (reason 'manager stopped') before draining (default false)

### Class RedisJM

Accepts instance of redis client, a `targetGroup` string, and optional options in the constructor.
`targetGroup` is used to prefix all redis artifacts, so only the clients with same target group can share the queue and locks.

RedisJM extends hookable from unjs to dispatch events.

RedisJM tracks registered Job instances by jobName. jobName must be unique among registered jobs.

RedisJM Methods:
 - `isLocked(jobId)` — checks if jobId is in the locks set (blocked in any active state: queued/delayed/running/stale)
 - `isQueued(jobId)` — deprecated alias for isLocked (name misleading — returns true for any active lock, not only queued)
 - `queue(job, runId, inputs, options?)` — SADD lock, HSET log, then RPUSH lane-queue (or ZADD delayed when options.delay > 0). Returns true/false.
 - `queueFirst(job, runId, inputs, options?)` — same as queue but LPUSH (priority insert). delay > 0 throws.
 - `stats()` — best-effort snapshot: per-lane queue depths, delayed count, lock count, status histogram (see RedisJMStats). Non-transactional; lane visibility scoped to lanes this instance can name.
 - `queueSize(lane?)` — LLEN of one lane's queue list (default lane when omitted). Delayed runs not counted (not on a lane list until promoted).
 - `list()` — returns all log records (JobLogRecord[]). Incremental HSCAN; corrupt/foreign fields skipped.
 - `get(jobId)` — single record via HGET, or undefined.
 - `unqueue(jobId)` — removes from lane queue (LREM, lane resolved from record), delayed set (ZREM), locks (SREM), log (HDEL), and suspects (HDEL).
 - `createJob(metadata, fn)` — creates Job instance and registers it
 - `registerJob(job)` — registers by jobName (must be unique), hooks on all events to handle Redis updates and re-dispatch
 - `unregisterJob(job)` — removes hooks and unregisters
 - `getTargetGroup()` — returns target group id
 - `getOptions()` — returns resolved options (defaults applied)
 - `popAndExecute()` — promote due delayed runs, pop one jobId (LMPOP over subscribed lanes / LPOP fallback), match to registered Job, execute. Unknown jobName: re-queue up to unknownJobRequeueLimit (lock held) for a sibling, then set log "error" ("Job name is unknown") and release lock. A RunSupersededError out of the run is a benign skip (touch neither lock nor log). Returns true if popped something, false if queue empty.
 - `start(interval)` — starts poll loop. Recursive setTimeout: delay=0 after work found, delay=interval when idle. With concurrency>1 dispatches runs without awaiting, popping again until all slots full. Auto-wires maintenance unless maintenanceInterval=0.
 - `stop(options?)` — stops poll + maintenance timers, awaits ALL in-flight runs to settle (drain). options.abort aborts every ctx.signal first (cooperative fast shutdown). Awaits the in-flight poll before snapshotting inFlightRuns (a poll caught mid-pop must be part of the drain).
 - `performMaintenance()` — scans log + locks for stale/orphaned/expired records (see Maintenance section)

### Class Job

Accepts job metadata, a job function, and an optional RedisJM instance (default manager).
Job has two generics: `Job<TInputs, TAttrs>` where TAttrs extends `Record<string, string | number | boolean | null | undefined>`.

Job extends hookable from unjs to dispatch events.

Job Methods:
- `execute(inputs, options?)` — options: `{ targetGroup?, heartbeatInterval?, runId?, manager?, logger?, signal? }`. Mints a per-execution `executionId` (fencing token). Wires ctx.signal (aborts on external `signal` or via payload.abort). Dispatches start (the claim), sets up heartbeat timer AFTER start resolves (so a throwing start/finish hook can't leak a timer), calls fn(inputs, ctx), dispatches finish/error. Only a job-FUNCTION throw dispatches error; a throwing start/finish hook propagates without flipping status. Clears heartbeat timer + detaches external-signal listener in finally.
- `queue(runId, inputs, manager?, options?)` — delegates to RedisJM.queue
- `queueFirst(runId, inputs, manager?, options?)` — delegates to RedisJM.queueFirst
- `getJobId(runId)` — returns "jobName#runId"
- `getMetadata()` — returns job metadata
- `getName()` — returns jobName
- `getLane()` — returns lane or undefined
- `getAttempts()` — floored, clamped ≥ 1
- `getBackoffMs(attempt)` — resolves backoff (number or fn), negatives/non-finite → 0
- `setDefaultManager(manager)` — sets the default manager used by queue()

Job function signature: `(inputs: TInputs, ctx: { setProgress, setAttrs, signal }) => void | Promise<void>`
- `setProgress(n)` — throws TypeError on non-finite, clamps to [0,1], dispatches 'update'. Returns Promise<void>.
- `setAttrs(attrs)` — MERGES into existing attrs (successive calls accumulate), dispatches 'update'. Returns Promise<void>.
- `signal` — AbortSignal, aborted on ownership loss (heartbeat detects superseded/terminal/unqueued) or stop({abort:true}). A run merely staled but still owned (matching executionId) self-heals instead of aborting.

Job events (every payload also carries `executionId`, `manager?`, and `abort(reason?)`):
- `start` — before calling job function.
- `finish` — after job function completes.
- `error` — on job-function error. Payload adds `error`. Job-level hook fires on EVERY failed attempt.
- `heartbeat` — on each heartbeat tick.
- `update` — on setProgress/setAttrs. Payload adds `progress?` / `attrs?`.
(retry is manager-level only — see RedisJM Event Handling.)

### RedisJM Event Handling

Re-dispatch is filtered by `shouldHandle`: targetGroup must match, and when the payload carries a `manager` it must be this manager (so two managers sharing one Job don't double-handle). All lifecycle writes are FENCED by executionId (see Execution Fencing below).

- `start` → the CLAIM. updateLog only if status==="queued": set status="running", startedAt=now, heartbeat=now, executionId=payload.executionId, attempt++, delete suspectedAt. A 'rejected' claim (status no longer queued) means the entry was superseded → throw RunSupersededError. 'missing' → direct execute(), proceed. Then re-dispatch.
- `heartbeat` → updateLog if executionId matches AND status is "running" OR "stale" (a "stale" record still owned by this execution is RESURRECTED: status→"running", clear finishedAt, and after the write re-SADD the released lock — see Execution Fencing). heartbeat=now. If result !== 'written' (rejected = owner changed / terminal, missing = unqueued), call payload.abort(...) and SKIP the re-dispatch (the ownership-loss detector). Otherwise re-dispatch.
- `update` → same accept-and-resurrect rule as heartbeat (executionId matches AND status "running" OR "stale"; a "stale"-but-ours record self-heals to "running" + re-SADD lock): set progress and/or MERGE attrs. Re-dispatch.
- `finish` → updateLog only if executionId matches: status="finished", finishedAt=now. 'rejected' → touch nothing, skip event. Else release lock (+ HDEL if keepFinishedInterval=0) and re-dispatch.
- `error` → updateLog only if executionId matches. attempt = record.attempt (the 1-based attempt that just failed). record.error = message (both branches). If attempt < maxAttempts (RETRY): status="delayed", readyAt=now+backoff, delete finishedAt, KEEP lock; after write ZADD delayed and fire manager-level `retry` (error, attempt, nextAttemptAt=readyAt). If the prior status was "stale" (staled mid-run, lock released) the retry also re-SADDs the lock before the ZADD — same self-heal as heartbeat/update — so the delayed record holds its lock through backoff (the dedupe invariant). Else (FINAL): status="error", finishedAt=now; release lock and fire manager-level `error`. 'rejected' → touch/fire nothing.

Note: job-level `error` hook (on the Job) fires on every failed attempt inside execute(); manager-level `error` fires only on final failure; manager-level `retry` fires per scheduled retry.

Heartbeat write failures during execute() are reported via the execute() logger, not swallowed.

### Execution Fencing

Each execution mints a per-run `executionId` (randomUUID). The `start` hook stamps it onto the record when it claims a `queued` record. finish/error/heartbeat/update all guard on `record.executionId === payload.executionId` (heartbeat/update additionally accept a "running" OR "stale" record, finish/error accept any status). A record's owner is thus the execution whose start stamped it. Scenario fenced: a stalled handler is staled by maintenance (lock released), a producer re-enqueues the same runId (fresh `queued` record, no executionId), the original handler finally finishes — its executionId no longer matches, so all its writes are rejected. It can't overwrite the successor's record, srem the successor's lock, or (under keepFinishedInterval=0) delete it. RunSupersededError is thrown out of the superseded run; popAndExecute treats it as a benign skip. updateLog returns 'written' | 'rejected' (mutator returned false) | 'missing' (no record), letting hooks tell "owner changed" from "unqueued/cleaned".

Self-heal (stale recovery): staleness detection can't tell a dead handler from one that merely pinned its event loop past the heartbeat threshold (a blocked loop also blocks the client heartbeat timer). So the recovery is symmetric with detection: the moment a staled-but-still-owned execution writes again (a heartbeat, setProgress, or setAttrs), heartbeat/update RESURRECT the record — flip "stale"→"running", clear the maintenance-stamped finishedAt, and re-SADD the lock maintenance released — so the run keeps its progress/attrs stream and re-establishes dedupe (a producer can't double-enqueue the runId while it's demonstrably alive). The retry branch of `error` recovers the same way for a run that throws a retryable error straight off a stale record (no intervening update): it clears finishedAt and re-SADDs the lock so the resulting `delayed` record holds its lock through the backoff window, instead of sitting unlocked (which would let a producer re-enqueue and swallow the retry). This closes the old asymmetry where such a run's updates were rejected (status "stale") yet its finish (fenced on executionId only) still landed, freezing progress mid-run and dropping the final attrs. The zombie case stays fenced: once a successor re-enqueues (fresh record, no/other executionId), resurrection's executionId guard fails and the old execution's writes keep rejecting. Consumer effect: a "stale" badge becomes a transient display state, not a point of no return.

### Concurrency & Cancellation

- Concurrency (option, default 1): poll loop keeps up to N runs in flight — dispatches a popped run (a thunk) WITHOUT awaiting, then re-polls; waits (Promise.race) only when inFlightRuns.size >= N. Each run owns an AbortController tracked in abortControllers; stop() drains ALL in-flight runs and (with abort) aborts every controller. Per-instance only — the distributed lock still ensures one runId runs on one instance.
- Cancellation (ctx.signal): aborted on ownership loss (heartbeat hook's guarded write not 'written' → payload.abort — i.e. the record's owner changed, went terminal, or was unqueued; a stale-but-owned run self-heals instead) or stop({abort:true}). Cooperative — nothing forcibly kills a handler; it must observe signal.aborted / 'abort'. payload.abort(reason?) is also exposed to user hooks as a custom kill-switch. execute() follows an optional external `options.signal` and detaches the listener when the run settles.

### Delayed runs & retries

- Delayed set `redisjm:{tg}:delayed` (ZSET jobId → ready-at ms), group-wide; record's persisted `lane` routes it on promotion.
- Enqueue with delay: status="delayed" + readyAt, lock held, ZADD instead of RPUSH. Retry: onError stages status="delayed" back on the set, lock HELD through the backoff (dedupe persists).
- Promotion: `popAndExecute`/poll calls `promoteDueDelayed` before each pop. Rate-limited to ≤1/sec; sweeps ≤8 due ids/pass. Per id: ZREM (reply 1 = atomic claim, loser skips), flip status delayed→queued (delete readyAt), RPUSH onto the record's lane. A won ZREM with a missing/non-delayed record is garbage → release lock. ⇒ a delayed run becomes poppable within ~1s of readyAt; ~8 promotions/sec/instance.

### Job Statuses

- `queued` — on a lane queue, waiting to be picked up
- `delayed` — staged on the delayed set (scheduled run or pending retry), lock held
- `running` — currently executing, heartbeat active
- `finished` — completed successfully
- `error` — final failure (attempts budget exhausted)
- `stale` — detected as abandoned (heartbeat expired / orphaned queued|delayed / orphaned lock), set by maintenance

### Job Log Record

Stored as JSON in the log hash:
```
{
  jobId, jobName, runId, inputs, targetGroup,
  lane?: string,                // enqueued lane (persisted for maintenance/unqueue); omitted for default lane
  status: "queued" | "delayed" | "running" | "finished" | "error" | "stale",
  startedAt?: number,
  finishedAt?: number,
  heartbeat?: number,
  progress: number (0-1),
  attrs?: TAttrs,
  error?: string,
  executionId?: string,         // fencing token stamped by the claiming execution
  attempt?: number,             // 1-based count of executions that claimed this run
  readyAt?: number,             // epoch-ms a delayed run becomes poppable (present only while delayed)
  suspectedAt?: number,         // orphaned queued|delayed suspect timestamp (maintenance)
  requeueCount?: number         // times re-queued for an unknown job name
}
```

### Maintenance

`performMaintenance()` scans the log (incremental HSCAN) and locks (SSCAN), with `now = Date.now()` and staleThreshold = heartbeatInterval * roundsToStale:
1. status "running": if `now - lastHeartbeat > staleThreshold`, mark "stale", finishedAt=now, SREM lock. (lastHeartbeat = heartbeat ?? startedAt ?? 0)
2. status "queued"|"delayed": liveness = queued must be on its lane queue (LPOS), delayed must be on the delayed set (ZSCORE). If absent → two-pass: first scan stamps suspectedAt; a later scan past staleThreshold marks "stale" + SREM lock. Present again → clear suspectedAt. (an overdue-but-present delayed entry is healthy — promotion owns it.)
3. status "finished"|"error"|"stale": if `now - finishedAt > keepFinishedInterval`, HDEL. Unparseable/foreign records dropped unconditionally (garbage — retention doesn't apply), counted in cleanedCount.
4. Orphaned locks: a locks member with NO backing log record (crash between SADD and HSET). Two-pass via the `suspects` hash: first sighting stamps first-seen ms; still record-less past staleThreshold → SREM lock + clear suspect (counted stale). Suspects whose record now exists or whose lock is gone are exonerated.

Returns `{ staleCount, cleanedCount }` (orphaned queued/delayed/lock reclaims count toward staleCount; expired terminals + garbage toward cleanedCount).

`start()` also calls `reclaimStaleMaintenanceLock()` before the first enqueue: maintenance is the only thing that reclaims stale running locks, so a maintenance run killed mid-flight would hold its own lock forever and deadlock the group. It reclaims only demonstrably-stale maintenance locks (running past threshold, or a record-less lock), NOT a `queued`-but-absent record (indistinguishable from a live pop→start window). Cutover exception: a `queued` maintenance record still present on the LEGACY default queue (enqueued by a pre-lanes 0.0.3 instance) is relocated onto the `__maintenance` lane (lock held) so pure lane workers can run it.

A pre-defined `createMaintenanceJob(manager)` factory is exported. It returns a Job that calls `manager.performMaintenance()` and automatically registers it with the manager. Maintenance is idempotent, so use an empty `runId` to ensure at most one is queued/running at a time (the lock is released on completion, allowing re-queue):
```ts
const maintenanceJob = createMaintenanceJob(manager)
// Periodically try to queue maintenance (all instances, only one succeeds)
setInterval(() => maintenanceJob.queue('', null), 30000)
```

## Notes

Same Job instance can be attached to different RedisJM instances, but it keeps the default one forwarded via constructor.

Events re-dispatching is filtered by `targetGroup` and, when the payload carries a `manager`, by manager identity (only the driving manager acts).

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
