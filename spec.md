# Redis Job Manager

package @prostojs/redisjm — design spec as of 0.2.0

## Purpose

Job Manager powered by redis for k8s-like installations that use multiple instances of same app.
It uses redis to push jobs into a queue.
Each job run can be picked up only by one instance.
App instances can have CRON-job to add job-runs into queue, so many apps can simultaniously attempt to add the same job-run to the queue. Redisjm takes care of locking runId to make sure that the job-run is added only once.

Each app instance polls the queue via `RedisJM.start()` and executes up to `concurrency` job-runs at a time (default 1). When an app instance is shut down or reloaded, an in-progress job may be abandoned. The library detects such stale jobs via heartbeats (fencing re-enqueued successors against zombie writes) and runs maintenance on its own timer to reclaim them.

0.2.0 hardening goals (motivated by a production incident in which Redis hit `maxmemory` while holding TTL-less redisjm keys):
 - an enqueue failure must never look like a dedupe; a refused write must leave nothing half-written behind
 - a full Redis must neither lose popped work nor stop maintenance from freeing memory
 - a hung handler must not hold a slot (or its record) forever
 - maintenance cost must be bounded per pass, and its writes must never clobber a concurrent writer
 - producers must be able to apply backpressure (caps, single-flight)

Glossary:
 - job - a function with metadata (jobName, inputs, status, ...)
 - job-run - a particular job execution that has it's runId (usually it is serialized inputs)
 - jobId - jobName + runId (separated by '#')
 - jobs-queue - an ordered list of jobId strings, supporting FIFO with priority insert (per lane)
 - locks - a set of jobId strings currently blocked from re-queuing (queued, delayed, running; a popped-not-yet-claimed run is queued)
 - log - a hash of job records with full state (status, progress, attrs, heartbeat, timestamps, executionId, attempt)
 - delayed set - a sorted set of jobId → ready-at ms (scheduled runs + pending retries)
 - claiming set - a sorted set of jobId → pop ms: runs popped but not yet claimed by their executor
 - executionId - per-run fencing token; the record's owner is the execution whose start hook stamped it
 - lane - a named sub-queue within a target group; workers poll only the lanes of their registered jobs
 - stale threshold - `heartbeatInterval * roundsToStale`

Features:
 - uses redis to manage job-queues, locks, delayed set, claiming set, and job logs
 - every queue-state change is an atomic server-side Lua script (enqueue, pop, record transitions)
 - ensures only one job runId is in flight; subsequent enqueues with same runId are `deduped`
 - explicit enqueue outcomes (`queued` / `deduped` / `busy` / `full`); Redis failures throw a typed `RedisJMEnqueueError`
 - dispatches events: start, finish, error, retry, timeout, heartbeat, update, enqueueFailed, startFailed, memoryPressure
 - manager-level hooks are isolated observers; job-level hooks are the lifecycle
 - supports async job functions
 - heartbeat mechanism to detect stale/abandoned jobs; execution timeouts and a `maxRunMs` backstop
 - execution fencing (per-run executionId) so a zombie run can't clobber a re-enqueued successor
 - delayed runs and automatic retries with configurable backoff (attempts / backoff metadata)
 - per-instance and per-lane concurrency, cooperative cancellation (ctx.signal), `wake()`
 - backpressure: lane caps (`maxQueued` / `laneCaps`) and per-job `maxInFlight`
 - progress tracking and custom attributes per job
 - maintenance on its own timer: lock-guarded, bounded per pass, delete-first, compare-and-set, emergency delete-only mode under OOM
 - memory guardrails: eviction-policy check, `health()`, `memoryPressure`, hash-field TTL on terminal records (Redis ≥ 7.4), `maxInputsBytes`
 - lanes: heterogeneous workers share one group/log/maintenance loop, each polls only its lanes; queued runs survive a job's lane change

## Stack

 - Typescript
 - pnpm
 - husky for commit lint
 - Redis ≥ 7.0 (shebang Lua scripts); ≥ 7.4 for hash-field TTL (optional). Not Redis Cluster (unhashed keys, multi-key scripts, the prune script builds undeclared lane keys).

## Redis Key Structure

Redis structures per target group:

| Key pattern | Redis type | Purpose |
|---|---|---|
| `redisjm:{tg}:queue` | List | Ordered queue of jobId strings for the default lane. Pushed by the enqueue script (RPUSH, LPUSH for priority), popped by the pop script. |
| `redisjm:{tg}:lane:{lane}:queue` | List | Ordered queue for a named lane. Default lane keeps the legacy `:queue` key for back-compat. |
| `redisjm:{tg}:locks` | Set | Currently locked jobIds (queued + delayed + running). |
| `redisjm:{tg}:log` | Hash | jobId → JSON record with full job state. Terminal fields get `HPEXPIRE keepFinishedInterval` (Redis ≥ 7.4). |
| `redisjm:{tg}:delayed` | Sorted Set | jobId → ready-at ms. Scheduled (delay) runs and pending retries. Group-wide; record's `lane` routes on promotion. |
| `redisjm:{tg}:claiming` | Sorted Set | jobId → pop ms. Added atomically with the pop; removed atomically with the claim (or by requeue/drop paths). |
| `redisjm:{tg}:suspects` | Hash | jobId → first-seen ms. Backs the two-pass orphaned-lock reclaim. |
| `redisjm:{tg}:jobs` | Set | Job registry: job names with per-job sets. Maintenance's index for per-job upkeep. |
| `redisjm:{tg}:jobs:{jobName}:locks` | Set | This job's locked jobIds. `maxInFlight` compares its SCARD; `inFlight()` reads it. Maintained by the scripts; drift is pruned by maintenance. |
| `redisjm:{tg}:jobs:{jobName}:lanes` | Set | Lane LIST KEYS (`redisjm:{tg}:queue`, `redisjm:{tg}:lane:{lane}:queue`) this job has (had) entries on. Registered atomically with every push; consumers drain old lanes from it; maintenance prunes empty lanes. |
| `redisjm:{tg}:maintenance-lock` | String, PX TTL | Maintenance lock (`SET NX PX`). Never released — expiry spaces passes across the fleet. |
| `redisjm:{tg}:maintenance-cursor` / `-locks-cursor` / `-jobs-cursor` | String | Persisted HSCAN/SSCAN cursors of the bounded maintenance stages. |
| `redisjm:{tg}:legacy-seen` | String, PX TTL | Marks that a pre-0.2 instance was seen recently (opens the legacy orphan-check window). |

### Scripts

All scripts start with a `#!lua` shebang (Redis ≥ 7.0) and run via EVALSHA with an EVAL fallback on NOSCRIPT. Scripts that may write declare NO flags (shebang only): such a script is not `allow-oom`, so when Redis is over `maxmemory` the server refuses the whole script up front (nothing executed) — for the pop script this is what guarantees a full Redis pops (and loses) nothing; a legacy shebang-less script would LPOP and only fail at its first memory-growing write. Deletion-only scripts (purge, prune-job, delete-if-unchanged) are `allow-oom`: freeing memory while Redis is full is exactly when they matter.

| Script | Flags | Does |
|---|---|---|
| enqueue | — | Per entry, in entry order: locked → `deduped`; job lock set SCARD ≥ maxInFlight → `busy`; lane LLEN ≥ cap → `full`; else accepted. Then for accepted entries: SADD lock + job lock set, HSET record, RPUSH / LPUSH (reverse order, so a batch keeps its order at the head) / ZADD delayed; register the lane's list key in the job's lane set and the job in the registry. |
| pop | — | Over the lane keys in poll order: own lane (`'*'`) → LPOP; old lane (allow-list `#jobA#jobB#`) → first entry within the first 100 whose jobName is allow-listed, LREM'd. ZADD the popped id to `claiming` (score = now) in the same step. |
| transition | — | Compare-and-set of one record (expected value sent as its SHA-1) plus the structure side effects of the status change (see Record writes). Ops: claiming `take`/`remove`, delayed `take`/`add`, lock `take`/`retire`/`drop`, optional push. |
| purge | allow-oom | Removes one run's record and run lock (global + job set) together, plus optionally its lane-list entry (LREM), `claiming`, delayed and suspects entries. Mode `force` = unconditional (`unqueue`, a popped id without '#'); mode `orphan` = only while the stored value still matches the witness the caller sends (`''` = absent, else the SHA-1 of the raw garbage bytes the manager's own parser rejected), so a run re-enqueued meanwhile is never deleted or unlocked. Validity is always judged in JS (`parseRecord`), never by Lua `cjson`, so the two can never disagree. |
| prune-job | allow-oom | Per job: SREM a random sample (200) of job-lock-set members whose global lock is gone; SREM lane keys (read from the set, not declared — one reason Cluster is unsupported) whose list is empty; SREM the job from the registry when both sets are empty. |
| delete-if-unchanged | allow-oom | Kind `h`: HDEL each hash field only while its raw value still has the SHA-1 that was judged (expired records and JS-judged garbage alike). Kind `z`: ZREM each sorted-set member only while its score is still the one read (maintenance's claiming drops, promotion's stray delayed entries) — a fresh pop/retry that re-scored the member in between survives. |

Enqueue flow: validation (lane, delay, `first`+`delay`) and the inputs-size guard in JS (before any Redis traffic), then ONE enqueue-script call for the whole batch. There is no rollback path: a failed script wrote nothing. Statuses are per entry; a Redis failure throws one `RedisJMEnqueueError` for the call (reason classified; `jobId` = first entry). `queue()`/`queueFirst()` = `status === 'queued'`.

Invariant upheld by the scripts: a `queued` record is on its lane list or in `claiming`; a `delayed` record is on the delayed set; a locked jobId is in both the global and its job's lock set.

## Implementation

### RedisJM Options

RedisJM constructor accepts an optional options object:
- `heartbeatInterval` — milliseconds interval for heartbeat updates (default 5000)
- `roundsToStale` — number of heartbeat intervals without update before a job is considered stale (default 2)
- `keepFinishedInterval` — milliseconds terminal (finished/error/stale) records linger so get()/list() can observe them (default 60000 — kept because callers poll get() for results; memory cost ≈ throughput × interval; `0` = legacy write-only, dropped the instant the job leaves running)
- `maintenanceInterval` — ms between maintenance timer ticks while started (default heartbeatInterval * roundsToStale; 0 disables)
- `jobTimeout` — default per-attempt execution timeout in ms (default 0 = none); JobMetadata.timeoutMs wins
- `maxRunMs` — maintenance stales a running record older than this regardless of heartbeat (default 0 = off)
- `unknownJobRequeueLimit` — times an unregistered job name is re-queued (lock held) for a sibling before being dropped as error (default 5; 0 = drop on first pop)
- `concurrency` — max simultaneous poll-loop runs per instance (default 1; positive integer, floored; throws TypeError on <1 or non-finite)
- `laneConcurrency` — `Record<lane, number>` ('default' key = default lane): max simultaneous poll-loop runs of a lane on this instance, within `concurrency` (default {})
- `laneCaps` — `Record<lane, number>`: enqueue cap on a lane's list length (default {})
- `maxInputsBytes` — max JSON-serialized inputs size (default 0 = unlimited); JobMetadata.maxInputsBytes wins
- `maxRecordsPerPass` — per-stage bound of one maintenance pass (default 1000)
- `memoryWarnRatio` — used/max memory ratio that fires `memoryPressure` (default 0.8; 0 disables)
- `laneStrategy` / `lanePriority` — lane poll ordering (`roundRobin` default, or `priority` with an explicit high→low list)
- `logger` — sink `(message, error?) => void` for operational errors (default console.error; `false` silences)

Lane-keyed limits are normalized: non-finite / negative values dropped, the rest floored.

Job metadata additionally accepts:
- `lane` — named sub-queue (default lane when omitted)
- `attempts` — total tries incl. first before final failure (default 1 = no retries; floored, clamped ≥ 1 via Job.getAttempts())
- `backoff` — ms before the next retry, fixed number or fn(failedAttempt)→ms (default 0; negatives/non-finite clamped to 0 via Job.getBackoffMs())
- `timeoutMs` — per-attempt execution timeout (overrides jobTimeout; 0 opts out; resolved via Job.getTimeoutMs())
- `maxQueued` — lane cap for this job's enqueues (effective cap = min of maxQueued and laneCaps[lane])
- `maxInFlight` — max locked runs of this job (any runId) → `busy`
- `maxInputsBytes` — overrides the manager default (0 opts out)

Enqueue options (`enqueue`/`enqueueMany`/`queue`/`queueFirst`/Job variants):
- `delay` — ms to stage the run on the delayed set instead of the live queue (finite ≥ 0)
- `first` (enqueue/enqueueMany only; implied by queueFirst) — priority insert; `delay > 0` with a priority insert throws TypeError

Stop options:
- `abort` — when true, aborts every in-flight run's ctx.signal (reason 'manager stopped') before draining (default false)

### Class RedisJM

Accepts instance of redis client, a `targetGroup` string, and optional options in the constructor.
`targetGroup` is used to prefix all redis artifacts, so only the clients with same target group can share the queue and locks. It must not contain ':'.

RedisJM extends hookable from unjs to dispatch events. Manager-level hooks are dispatched through an isolating `emit`: each handler is awaited in turn, a throwing/rejecting one is logged and skipped (it can't change the run's outcome or Redis state, nor stop the other handlers).

RedisJM tracks registered Job instances by jobName. jobName must be unique among registered jobs and must not contain '#'.

RedisJM Methods:
 - `isLocked(jobId)` — checks if jobId is in the locks set
 - `isQueued(jobId)` — deprecated alias for isLocked
 - `enqueue(job, runId, inputs, options?)` — one enqueue-script call → `{ status: 'queued'|'deduped'|'busy'|'full', jobId }`. Redis failure → `RedisJMEnqueueError` + `enqueueFailed` hook; OOM counted for health().
 - `enqueueMany(job, entries, options?)` — same, many runs of one job in ONE script call → per-entry results. All-or-nothing on failure; an oversized entry rejects the batch before any write.
 - `queue(job, runId, inputs, options?)` / `queueFirst(...)` — boolean shorthands (`true` iff queued).
 - `every(job, intervalMs, { inputs, runId?, immediate?, skipIfInFlight? })` — timer enqueue; returns a cancel fn; cleared by stop(). skipIfInFlight (default) = fixed runId (default 'every') so the lock dedupes across instances; false = `<runId>-<tickMs>`. Failures logged (+ enqueueFailed), never thrown.
 - `stats()` — best-effort snapshot: per-lane queue depths, delayed count, lock count, status histogram (full log scan). Non-transactional; lane visibility scoped to lanes this instance can name.
 - `health({ scan? })` — INFO memory (used/max/ratio/policy), oomRefusals, per-lane LLEN (known lanes), delayed/claiming/locks cardinalities, running (estimate = locks − Σqueues − delayed − claiming, floored at 0; exact with scan), stale (null unless scan).
 - `inFlight(jobName)` — SMEMBERS job lock set + one HMGET → `{ total, queued, delayed, running }` (claiming counts as queued). O(runs of that job).
 - `queueSize(lane?)` — LLEN of one lane's queue list (default lane when omitted). Delayed runs not counted.
 - `list()` — returns all log records (JobLogRecord[]). Incremental HSCAN; corrupt/foreign fields skipped.
 - `listPage({ status?, lane?, jobName?, limit?, cursor? })` — HSCAN page (COUNT limit) with filters; keeps scanning until ~limit matches or the end → `{ records, cursor }` ('0' = done).
 - `get(jobId)` — single record via HGET, or undefined.
 - `unqueue(jobId)` — one atomic `force` purge: record, lock (global + job set), lane-queue entry (lane resolved from the record), delayed, claiming, and suspects entries.
 - `createJob(metadata, fn)` — creates Job instance and registers it
 - `registerJob(job)` — registers by jobName (must be unique), hooks on all events to handle Redis updates and re-dispatch
 - `unregisterJob(job)` — removes hooks and unregisters
 - `getTargetGroup()` — returns target group id
 - `getOptions()` — returns resolved options (defaults applied)
 - `popAndExecute()` — returns false once stop() was called (until start()). Otherwise: promote due delayed runs, build the poll plan, run the pop script, resolve the popped id, execute; resolves after the run settles. Independent of slots (not counted against concurrency/laneConcurrency) but tracked so stop() drains it. Rejects if the pop itself fails (nothing popped).
 - `start(interval)` — starts poll loop + maintenance timer (see below); checks the eviction policy once (INFO memory).
 - `wake()` — if the loop is idle-waiting and a slot is free, re-poll now; if a poll is in flight, make the next scheduling immediate; no-op when not polling or all slots busy.
 - `stop(options?)` — stops poll, maintenance and every() timers; marks stopped; awaits the in-flight maintenance pass, the in-flight poll (a poll caught mid-pop must be part of the drain), then ALL in-flight poll-loop runs and popAndExecute calls. options.abort aborts every ctx.signal (twice: before and after awaiting the in-flight poll).
 - `runMaintenance()` — lock-guarded pass (see Maintenance).
 - `performMaintenance()` — unguarded full pass (manual use).

### Poll loop

Recursive setTimeout: delay=0 after work found, delay=interval when idle. Per poll:
1. all `concurrency` slots busy → wait for one run to settle (Promise.race), then re-poll.
2. poll plan = `__maintenance` lane first, own work lanes (roundRobin rotation or lanePriority order), then old lanes (allow-listed); lanes at their `laneConcurrency` cap are filtered out. Empty plan → wait for a slot.
3. pops are paused while `popsPausedUntil` (start-failure back-off) is in the future.
4. promote due delayed runs (≤ 1/sec, ≤ 8 ids), run the pop script, resolve the popped id; dispatch a real run WITHOUT awaiting it, counted per lane; when a capped lane's run settles, `wake()`.

Old lanes: every OLD_LANE_REFRESH_MS (5s) the instance pipelines SMEMBERS of each registered job's lane set (lane list keys); keys other than its own lanes' become old lanes with an allow-list of its job names found there. A failed refresh keeps the previous plan.

Resolving a popped id (drop branches purge only while no valid record backs the id; a stray duplicate leaves everything to the record's owner):
 - no '#' → `force` purge (record, lock, claiming entry).
 - jobName is the maintenance job → mark `legacy-seen` (a pre-0.2 instance enqueued it), then continue normally.
 - unknown jobName → requeue (CAS: only while `queued`; requeueCount++, enqueuedAt=now, RPUSH to the record's lane, ZREM claiming) up to unknownJobRequeueLimit; record not `queued` → stray duplicate entry, skip without touching the record; budget exhausted → CAS to terminal `error` ("Job name is unknown"; lock released and claiming entry removed in the same step); record missing → `orphan` purge.
 - no record, or an unparseable one → `orphan` purge (record, lock, claiming entry) — a no-op if a valid record appeared meanwhile.
 - otherwise → execution thunk with the resolved timeout (`job.getTimeoutMs() ?? jobTimeout`).
 - a Redis error while resolving → start-failure recovery (requeue/defer).

### Start-failure recovery

A failure between the pop and an established run must never leave a record queued-or-running with its lock held and no queue entry.
 - Claim write failed (tagged by the manager's start hook) or resolution failed: one transition that only applies while the record is still `queued` AND its claiming entry is still parked (precondition `take`): LPUSH the id to the head of the RECORD's own lane, ZREM claiming, register the lane key → action `requeued`. If the record is no longer `queued` (the claim landed despite the error, or the run moved on), nothing is touched and no event fires — logged only. If the transition fails too (e.g. OOM) → action `deferred`: the id stays in `claiming` (record `queued`, lock held) and maintenance requeues it after the stale threshold. Pops pause for min(1s, staleThreshold) after a requeue, a whole stale threshold after OOM/deferral.
 - Claim landed (record `running` under this executionId) and a later start-phase step threw (a job-level start hook) → dispatch the job's `error` hook so the normal fenced retry-or-final logic runs → action `failed`.
 - A job-level start hook registered before the manager's claim threw (record still `queued`) → CAS to terminal `error`, release lock → action `failed`. Not requeued (a deterministic hook failure would loop).
 - Fires manager hook `startFailed` { jobId, jobName, runId, reason, action, error }.

### Class Job

Accepts job metadata, a job function, and an optional RedisJM instance (default manager).
Job has two generics: `Job<TInputs, TAttrs>` where TAttrs extends `Record<string, string | number | boolean | null | undefined>`.

Job extends hookable from unjs to dispatch events. Job-level hooks are the LIFECYCLE: awaited in registration order, throw semantics preserved.

Job Methods:
- `execute(inputs, options?)` — options: `{ targetGroup?, heartbeatInterval?, runId?, manager?, logger?, signal?, timeoutMs? }`. Mints a per-execution `executionId` (fencing token). Wires ctx.signal (aborts on external `signal` or via payload.abort). Dispatches start (the claim; an error thrown out of the start phase is tagged with the executionId so the manager can recover it), sets up heartbeat timer AFTER start resolves, runs the handler (raced against `timeoutMs` when > 0), dispatches finish/error. A handler throw or timeout dispatches error (then rethrows the original error; a throwing error hook is logged, not masking it). A throwing finish hook propagates without flipping status to error. The heartbeat timer is stopped AND an in-flight heartbeat awaited before any terminal hook (a late heartbeat write must not land after the terminal write). Clears timers + detaches external-signal listener in finally.
- Timeout: on expiry, reject with `JobTimeoutError(timeoutMs)` BEFORE aborting the signal with reason 'timeout' (the timeout wins even if an abort listener rejects the handler synchronously); the abandoned handler promise gets a handler that logs once if it later settles/rejects.
- `enqueue(runId, inputs, manager?, options?)` / `enqueueMany(entries, manager?, options?)` — delegate to the manager
- `queue(runId, inputs, manager?, options?)` / `queueFirst(...)` — delegate to the manager
- `getJobId(runId)` — returns "jobName#runId"
- `getMetadata()` — returns a copy of the job metadata
- `getName()` — returns jobName
- `getLane()` — returns lane or undefined
- `getAttempts()` — floored, clamped ≥ 1
- `getBackoffMs(attempt)` — resolves backoff (number or fn), negatives/non-finite → 0
- `getTimeoutMs()` — own timeout: undefined (defer to manager), 0 (explicitly none; also for negative/non-finite), or the value
- `setDefaultManager(manager)` — sets the default manager used by enqueue/queue

Job function signature: `(inputs: TInputs, ctx: { setProgress, setAttrs, signal }) => void | Promise<void>`
- `setProgress(n)` — throws TypeError on non-finite, clamps to [0,1], dispatches 'update'. Returns Promise<void>.
- `setAttrs(attrs)` — MERGES into existing attrs (successive calls accumulate), dispatches 'update'. Returns Promise<void>.
- `signal` — AbortSignal, aborted on ownership loss (heartbeat detects superseded/terminal/unqueued), timeout ('timeout'), or stop({abort:true}). A run merely staled by a lapsed heartbeat but still owned self-heals instead of aborting.

Job events (every payload also carries `executionId`, `manager?`, and `abort(reason?)`):
- `start` — the claim, before calling job function.
- `finish` — after job function completes.
- `error` — on job-function error or timeout (or a start hook that threw after the claim). Payload adds `error`. Job-level hook fires on EVERY failed attempt.
- `heartbeat` — on each heartbeat tick.
- `update` — on setProgress/setAttrs. Payload adds `progress?` / `attrs?`.
(retry, timeout, enqueueFailed, startFailed, memoryPressure are manager-level only.)

### Record writes: compare-and-set

Every record write (claim, heartbeat, update, finish, error/retry, promotion, unknown-job requeue/drop, maintenance) goes through `updateLog(jobId, mutate)`:
1. Read the record — or start from a seed: the JSON this instance last read or wrote for a running run (the claim starts from the pop's read; heartbeats, updates and the terminal write start from the previous write), so the hot path does no extra read. A stale seed just costs a CAS conflict and a re-read. Missing/unparseable → 'missing'.
2. `mutate(record)` returns `false` (→ 'rejected') or optional side effects.
3. The transition script writes the new JSON ONLY if the stored JSON still hashes (SHA-1) to what was read (0 = conflict → re-read and re-apply, up to 5 attempts, then throw; -1 = record deleted meanwhile → 'missing', never re-created; -2 = precondition failed → 'rejected').
4. Side effects are DERIVED from the status change (stated once, in `transition`) and applied atomically with the write:
   - leaving `queued` (or a queued record pushed back) → ZREM claiming (with `takeFromClaiming`: only if still there, else -2 — so overlapping requeues push once);
   - `delayed` → `queued` (promotion) → ZREM delayed must succeed (else -2); entering `delayed` → ZADD delayed at readyAt;
   - entering a terminal status → `retire`: write it, set `HPEXPIRE log keepFinishedInterval FIELDS 1 jobId` (when > 0; pcall → no-op below 7.4), SREM the lock (global + job set). With keepFinishedInterval 0, finished/error → `drop`: HDEL instead of writing. A `stale` record is always kept (a live run may still resurrect it) until maintenance deletes it. Doing any of HDEL/HPEXPIRE/SREM separately could hit a record re-enqueued (or resurrected) in between;
   - `stale` → `running`/`delayed` (resurrection, retry off a staled run) → re-take the lock;
   - an explicit push (requeue, promotion) → LPUSH/RPUSH onto the record's lane, registering the lane key in the job's lane set.
   A re-enqueue's HSET clears a field TTL.
Writes from one instance to the same record are serialized locally (a per-jobId promise chain), so concurrent setProgress/setAttrs/heartbeat/finish of one run never exhaust each other's CAS budget; the retry budget is left for writers on other instances.

Cost: a run normally takes about 4 + n Redis round trips — pop script, record read, claim, terminal write, plus one per heartbeat/update (n) — not counting promotion sweeps.

### RedisJM Event Handling

Re-dispatch is filtered by `shouldHandle`: targetGroup must match, and when the payload carries a `manager` it must be this manager (so two managers sharing one Job don't double-handle). All lifecycle writes are FENCED by executionId (see Execution Fencing below). Manager-level events are emitted after the write, via the isolating emit.

- `start` → the CLAIM. CAS only if status==="queued": set status="running", startedAt=now, heartbeat=now, executionId=payload.executionId, attempt++, delete suspectedAt; ZREM claiming atomically. A Redis error in the claim is tagged as a claim-write failure (→ start-failure recovery). 'rejected' → throw RunSupersededError. 'missing' → direct execute(), proceed. Then emit.
- `heartbeat` → CAS if executionId matches AND status is "running", or "stale" with staleReason ≠ 'maxRunMs' (RESURRECT: status→"running", clear finishedAt/staleReason, re-take the lock atomically). heartbeat=now. If result !== 'written' → payload.abort(...) and SKIP the emit (the ownership-loss detector).
- `update` → same accept-and-resurrect rule as heartbeat: set progress and/or MERGE attrs. Emit.
- `finish` → CAS only if executionId matches: status="finished", finishedAt=now — the lock release and history TTL (or the deletion when keepFinishedInterval=0) happen in the same step. 'rejected' → touch nothing, skip event. 'missing' (unqueued mid-run) → `orphan` purge of the leftover lock. Emit.
- `error` → CAS only if executionId matches. attempt = record.attempt. record.error = message. If attempt < maxAttempts (RETRY): status="delayed", readyAt=now+backoff, delete finishedAt/staleReason, ZADD delayed atomically, KEEP the lock (re-take it atomically if the record was "stale" — the lock was released by maintenance); emit `timeout` first if the error is a JobTimeoutError, then `retry` (error, attempt, nextAttemptAt=readyAt). Else (FINAL): status="error", finishedAt=now, retired in the same step (lock release + TTL, or deletion when keepFinishedInterval=0); emit `timeout` if applicable, then `error` ('missing' → `orphan` purge first). 'rejected' → touch/fire nothing.

Note: job-level `error` hook fires on every failed attempt inside execute(); manager-level `error` fires only on final failure; manager-level `retry` fires per scheduled retry; manager-level `timeout` fires per timed-out attempt before its retry/error.

Heartbeat write failures during execute() are reported via the execute() logger, not swallowed.

### Execution Fencing

Each execution mints a per-run `executionId` (randomUUID). The `start` hook stamps it onto the record when it claims a `queued` record. finish/error/heartbeat/update all guard on `record.executionId === payload.executionId` (heartbeat/update additionally accept "running", or "stale" that isn't a maxRunMs stale; finish/error accept any status). A record's owner is thus the execution whose start stamped it. Scenario fenced: a stalled handler is staled by maintenance (lock released), a producer re-enqueues the same runId (fresh `queued` record, no executionId), the original handler finally finishes — its executionId no longer matches, so all its writes are rejected. RunSupersededError is thrown out of a superseded claim; the poll loop treats it as a benign skip. Combined with CAS, a fenced decision is made against the record as it is at write time, not as it was read.

Self-heal (stale recovery): staleness detection can't tell a dead handler from one that merely pinned its event loop past the heartbeat threshold. So the recovery is symmetric with detection: the moment a staled-but-still-owned execution writes again (a heartbeat, setProgress, or setAttrs), heartbeat/update RESURRECT the record — "stale"→"running", clear finishedAt, re-take the lock atomically — so the run keeps its progress/attrs stream and re-establishes dedupe. The retry branch of `error` recovers the same way for a run that throws a retryable error straight off a stale record. A `maxRunMs` stale is NOT resurrected by heartbeat/update (a hung handler's heartbeat timer would otherwise undo the backstop forever); the rejected heartbeat aborts the run's signal. The execution's own outcome still lands on a maxRunMs stale, as on any stale-but-owned record: a later finish writes `finished`; a throw schedules a retry when attempts remain (else `error`). The zombie case stays fenced: once a successor re-enqueues (fresh record, no/other executionId), resurrection's executionId guard fails.

### Concurrency & Cancellation

- Concurrency (option, default 1): poll loop keeps up to N runs in flight — dispatches a popped run (a thunk) WITHOUT awaiting, then re-polls; waits (Promise.race) only when inFlightRuns.size >= N. Per-lane caps (`laneConcurrency`) filter the poll plan; a lane's counter is decremented when its run settles. Each run owns an AbortController tracked in abortControllers; stop() drains ALL in-flight runs and (with abort) aborts every controller. Per-instance only — the distributed lock still ensures one runId runs on one instance. Maintenance never occupies a slot.
- Cancellation (ctx.signal): aborted on ownership loss (heartbeat hook's guarded write not 'written'), timeout, or stop({abort:true}). Cooperative — nothing forcibly kills a handler. payload.abort(reason?) is also exposed to user hooks as a custom kill-switch. execute() follows an optional external `options.signal` and detaches the listener when the run settles.

### Timeouts

- Resolved timeout = `JobMetadata.timeoutMs` (0 = none) ?? `RedisJMOptions.jobTimeout` (0 = none). Passed to execute() as `timeoutMs`.
- On expiry: JobTimeoutError, signal aborted ('timeout'), job `error` hook → manager retry-or-final; the slot frees immediately. The abandoned handler keeps running; its record writes are fenced (record no longer running under its executionId); a retry may overlap it — handlers must observe the signal and be idempotent.
- `maxRunMs` (maintenance): a running record with `now - startedAt > maxRunMs` → stale (staleReason 'maxRunMs', error message set, lock released, field TTL), regardless of heartbeat.

### Delayed runs & retries

- Delayed set `redisjm:{tg}:delayed` (ZSET jobId → ready-at ms), group-wide; record's persisted `lane` routes it on promotion.
- Enqueue with delay: status="delayed" + readyAt, lock held, ZADD instead of push — inside the enqueue script. Retry: onError stages status="delayed" back on the set, lock HELD through the backoff (dedupe persists).
- Promotion: `promoteDueDelayed` before each pop, rate-limited to ≤1/sec, ≤8 due ids per sweep (concurrently). Per id ONE transition: precondition ZREM delayed (the atomic claim — a racing instance's promotion is a no-op), status delayed→queued, delete readyAt, enqueuedAt=now, RPUSH onto the record's lane. Missing or garbage record → `orphan` purge (delayed entry, lock, the garbage record) — a no-op if a valid record appeared meanwhile. Record not delayed → remove the stray entry only while its score is still the one read (a retry re-scheduled in between keeps its entry).

### Backpressure

- Lane caps: effective cap = min(JobMetadata.maxQueued, laneCaps[laneLabel]) when either is defined; checked in the enqueue script against LLEN of the lane list (delayed entries don't count; a batch counts its own accepted non-delayed entries). Full → `full`, nothing written. Reject-only by design (no drop-oldest: silently dropping accepted work is the failure mode being prevented). Internal pushes (promotion, unknown-job requeue, start-failure requeue, maintenance requeue) bypass the cap.
- maxInFlight: compared against SCARD of the job's lock set inside the enqueue script → `busy`. Drift (members whose lock is gone, e.g. released by a pre-0.2 instance) can over-count until pruned; pre-0.2 enqueues are not added, so it can under-count during a mixed deploy.
- maxInputsBytes: Buffer.byteLength(JSON.stringify(inputs)) checked in JS before any Redis traffic → RedisJMEnqueueError('inputs-too-large') (+ enqueueFailed).

### Lanes

- Default lane = legacy key `redisjm:{tg}:queue` (label 'default'); named lane = `redisjm:{tg}:lane:{lane}:queue`. Lane names: `^[A-Za-z0-9_-]+$`, no `__` prefix (reserved: `__maintenance`).
- Consumers poll `__maintenance` first, then the union of their registered jobs' lanes (roundRobin / priority), then old lanes.
- Lane changes across deploys: a queued record keeps its enqueue-time lane. Every push registers the lane's list key in `jobs:{jobName}:lanes` atomically, so consumers of a job discover lanes it still has entries on and drain them with an allow-list pop (only allow-listed job names, first 100 entries scanned, LREM) — never popping other jobs' entries (which would burn their unknownJobRequeueLimit). Maintenance prunes empty lanes from the set. Old lanes are polled last (can starve under sustained backlog); a shared old lane with >100 foreign entries at its head blocks allow-listed entries behind them; pre-0.2 enqueues don't register lanes.

### Job Statuses

- `queued` — on a lane queue (or in `claiming`), waiting to be claimed
- `delayed` — staged on the delayed set (scheduled run or pending retry), lock held
- `running` — currently executing, heartbeat active
- `finished` — completed successfully
- `error` — final failure (attempts budget exhausted)
- `stale` — reclaimed by maintenance (heartbeat lapsed / maxRunMs / orphaned delayed or legacy queued); lock released atomically with the mark

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
  enqueuedAt?: number,          // epoch-ms last put on a lane list; presence marks a 0.2+ record
  suspectedAt?: number,         // orphan suspect timestamp (maintenance two-pass check)
  staleReason?: "heartbeat" | "orphaned" | "maxRunMs",
  requeueCount?: number         // times re-queued for an unknown job name
}
```

### Maintenance

Scheduling: `start()` (unless maintenanceInterval = 0) registers the maintenance job (rolling-deploy compat only) and starts a timer: one tick immediately, then every maintenanceInterval. A tick is skipped while this instance's previous pass is in flight; after each pass the memory-pressure check runs. Maintenance never goes through the queue or a concurrency slot.

`runMaintenance()`:
- `SET maintenance-lock <uuid> NX PX ttl`, ttl = maintenanceInterval − min(1000, interval/10) (stale threshold when auto-maintenance is off). NEVER released: expiry spaces passes ~one interval apart across the fleet.
- acquired → `performMaintenance()` (mode 'full'); not acquired → `null`.
- SET refused with OOM → EMERGENCY pass: lock-free, delete-only — HSCAN a batch (maxRecordsPerPass) from a per-instance cursor, compare-and-delete expired terminal records and garbage, SREM garbage locks. Idempotent, safe to run concurrently. `{ staleCount: 0, requeuedCount: 0, mode: 'emergency' }`.
- other lock errors → log, `null`.

`performMaintenance()` — one full pass, unguarded. Every stage bounded by maxRecordsPerPass; cursors persisted (a failed cursor write restarts that scan at 0); presence checks pipelined per batch; every operation individually guarded (failures counted, logged once per pass with the classified reason of the first).
1. READ: log batch (HSCAN from the shared cursor) → garbage (unparseable / no string status), expired terminal (finishedAt older than keepFinishedInterval), live (running/queued/delayed). Presence: delayed → ZSCORE delayed; LEGACY queued (no enqueuedAt) → LPOS on its lane + ZSCORE claiming; while the legacy window is open (`legacy-seen` exists), also new-format queued records older than the stale threshold. Claiming: ZRANGEBYSCORE claiming -inf now-staleThreshold WITHSCORES LIMIT max + HGET each → still `queued` = requeue, else drop the entry only while its score is unchanged. Locks batch (SSCAN, own cursor) → pipelined HEXISTS; record-less locks: first sighting stamped in `suspects`, past the threshold = orphan. Suspects HSCAN → exonerate those with a record or no lock. Job registry batch (SSCAN, own cursor, ≤ 100).
2. DELETE (before any write — deletions succeed at maxmemory): compare-and-delete expired + garbage (batches of 500; garbage re-read as raw bytes and re-judged by the JS parser first); SREM garbage locks; drop stale claiming entries (score-conditional); release orphan locks (staleCount); HDEL exonerated suspects; prune-job script per job.
3. WRITE (each a CAS): running records overdue (maxRunMs from startedAt, else heartbeat ?? startedAt older than the stale threshold, re-checked on the CURRENT record and same executionId) → stale + unlock + field TTL. Presence-checked records: absent & unsuspected → stamp suspectedAt; absent & suspected past threshold → stale ('orphaned') + unlock; present & suspected → clear suspectedAt. Overdue claims → requeue at the HEAD of the record's lane with `takeFromClaiming` (requeuedCount). Stamp new lock suspects. Persist the three cursors.
Returns `{ staleCount, cleanedCount, requeuedCount, mode: 'full' }`.

The legacy window: popping an entry of the maintenance job (0.2+ never enqueues it) sets `legacy-seen` with TTL max(10 min, 10 × stale threshold). A pre-0.2 consumer pops with LMPOP (no claiming entry), so a crash between its pop and claim would orphan a new-format queued record; the window turns on the LPOS check for those. 0.1.x instances with maintenanceInterval 0 never enqueue maintenance, so they never open it.

`createMaintenanceJob(manager)` remains exported: registers `__redisjm_maintenance` on lane `__maintenance`; its handler calls `runMaintenance()` (lock-guarded). Needed only to consume entries enqueued by pre-0.2 instances; enqueueing it from 0.2 code is discouraged (occupies a slot, opens the legacy window).

### Memory

- Recommended: `noeviction`, a dedicated instance, an alarm. `start()` reads `maxmemory_policy` from INFO memory (never CONFIG GET — managed services block it) and warns on `allkeys-*` (may evict queue/lock/log keys) and `volatile-*` (can't free TTL-less redisjm keys).
- Under OOM: enqueue script refused (nothing written, `oom` error, oomRefusals++); pop script refused (nothing popped; logged once per episode); claim refused → requeue/defer; maintenance → emergency mode; deletion-only scripts (purge, prune-job, delete-if-unchanged) are allow-oom, so purges and pruning still work.
- History: terminal records get a field TTL (≥ 7.4) atomically with the terminal write; maintenance also sweeps them.
- `memoryPressure`: after each timer pass, if used/max ≥ memoryWarnRatio and armed → snapshot health(), disarm, log, emit; re-armed when the ratio drops below. A failed snapshot leaves it armed.

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
