# Proposal: Lanes — native support for heterogeneous worker types

**Status:** proposal / not yet implemented
**Author:** handed off from an rvmode design session (Claude + Mavrik)
**Target version:** next minor (`0.0.4` or `0.1.0` — see "Versioning" below)
**Scope:** additive change to core (`src/redisjm.ts`, `src/job.ts`, `src/types.ts`, `src/maintenance.ts`), plus tests. No breaking changes.

> **Revision note (post-review):** revised after an adversarial review pass. Material corrections since the first draft: (1) `requeueUnknownJob` must be lane-aware — it is **not** "unchanged" (§5.5); (2) rolling-deploy safety is **not** symmetric — old-version maintenance corrupts laned records, so the lane rollout must be staged (§6); (3) `laneStrategy: 'priority'` gains an explicit `lanePriority` ordering, and the reserved `__maintenance` lane is pinned ahead of the poll order (§4.4/§5.6); (4) `targetGroup` must be constrained to keep the `:lane:` infix collision-proof (§4.1); (5) shared lanes isolate at *lane* granularity, not *handler* granularity (§5.5); (6) the produce-vs-consume split is driven by registration, not by `start()` — a mixed producer+consumer pod must construct-without-register to produce a lane it must not consume (§4.3/§11).

---

## 1. Motivation

We want to run **different kinds of workers against the same job system**: e.g. web/SSR pods that handle light scheduled jobs, and a dedicated Fargate worker that handles a heavy image-processing job (fetch → sharp resize → S3). The heavy work must *not* run on the web pods (it OOMs a small pod), but it should still be the *same* job system — one place to enqueue, one place to monitor.

Today redisjm has no first-class way to express "this job is serviced by that worker type." The only tools are:

1. **One group per worker type** — works, but fragments observability (`list()` is per-group), duplicates maintenance/retention/scheduler wiring, and models the split as *two unrelated job systems* rather than *one system with specialized workers*.
2. **Share a group and lean on the unknown-job requeue** — actively harmful (see §3).

This proposal adds **lanes**: named parallel queues *within* a group, where a consumer only services the lanes its registered jobs belong to. One group, one log, one maintenance loop, one monitoring pane — but the poppable work is partitioned so a worker never pops work it can't handle.

---

## 2. Current architecture (as of `0.0.3`)

Grounding references are to the current source.

- **`targetGroup` is the only namespace.** All keys derive from it (`src/redisjm.ts:714-724`):
  ```
  redisjm:<group>:queue    # List  — the single work queue
  redisjm:<group>:locks    # Set   — jobId dedupe (SADD is the lock)
  redisjm:<group>:log      # Hash  — jobId -> JobLogRecord JSON
  ```
- **One queue per group; blind pop.** `popAndExecute()` (`src/redisjm.ts:353`) does `LPOP redisjm:<group>:queue`, *then* parses `jobName#runId` and looks up `jobsByName.get(jobName)` (`:367`). Routing capability is only checked **after** the job is already popped.
- **Unknown-job requeue** (`src/redisjm.ts:432` `requeueUnknownJob`): if this instance has no handler for the popped name, RPUSH it back (lock held) up to `unknownJobRequeueLimit` (default 5, `src/types.ts:42-50`), then drop it as `error: 'Job name is unknown'`. This was designed for **rolling deploys** (a pod transiently missing a just-added handler), per its own doc comment (`:368-374`).
- **Locks** are group-wide and keyed by `jobName#runId` (`enqueue`, `src/redisjm.ts:669`; `SADD` at `:678`). `jobName` must be unique per manager (`registerJob`, `:235`).
- **Log** is group-wide; `list()` (`src/redisjm.ts:151`) reads the whole hash → this is what admin dashboards read.
- **Maintenance** (`src/redisjm.ts:589` `performMaintenance`) scans the log hash and, for orphaned `queued` records, does `LPOS redisjm:<group>:queue jobId` (`:615`) to distinguish "popped by a dead instance" from "still waiting." It runs as a built-in job (`src/maintenance.ts`, `MAINTENANCE_JOB_NAME = '__redisjm_maintenance'`, queued with `runId=''`), auto-wired by `start()` (`src/redisjm.ts:475-487`) and lock-deduped so exactly one instance runs it per interval.

---

## 3. The problem with heterogeneous workers today

If pods and a worker **share a group** and each services a disjoint set of job names, the single blind queue forces every instance to pop jobs it can't handle:

1. **Wasted contention.** Web pods repeatedly `LPOP` the heavy job, requeue it, and spin their idle interval — Redis ops + latency before the one capable worker gets a turn.
2. **Latent correctness bug.** `requeueCount` increments on **every** non-handling pop, group-wide (`src/redisjm.ts:439-442`). With N busy web pods and 1 worker, a routable job can reach `unknownJobRequeueLimit` (5) and be **dropped as "Job name is unknown" before the worker ever pops it**. The busier the pods, the likelier a valid job is silently killed.

The unknown-job path is a *rolling-deploy transient* mechanism; using it as a routing mechanism is a misuse. Hence the separate-group workaround — which trades this bug for observability fragmentation and duplicated wiring.

---

## 4. Proposed design: lanes

A **lane** is a named sub-queue inside a group that a worker type services. The group keeps ONE shared log, ONE shared lock set, ONE maintenance loop — only the *poppable work list* splits by lane.

### 4.1 Key layout

```
redisjm:<group>:queue            # default lane — UNCHANGED legacy key (jobs with no lane)
redisjm:<group>:lane:<lane>:queue  # named lane queue, e.g. redisjm:portal:lane:images:queue
redisjm:<group>:locks            # group-wide — UNCHANGED
redisjm:<group>:log              # group-wide — UNCHANGED
```

> **Backward-compat rule (critical):** a job with **no lane** (`lane === undefined` or `'default'`) uses the **exact legacy queue key** `redisjm:<group>:queue`, not `redisjm:<group>:lane:default:queue`. This strands zero in-flight work on upgrade and lets lane-unaware (old) and lane-aware (new) instances coexist during a rolling deploy.
>
> **Lane-name validation:** lane names must match `^[A-Za-z0-9_-]+$` — reject `#`, `:`, and the `__` prefix (reserved, see §5.3); mirrors the existing `#`-in-jobName guard at `src/redisjm.ts:230-234`.
>
> **Group-name validation (new, required):** the `:lane:` infix is only collision-proof if `targetGroup` cannot itself contain the infix. Today `targetGroup` is unvalidated (`src/redisjm.ts:79-82`) and keys are built by raw interpolation (`src/redisjm.ts:714-724`), so with lanes two distinct `(group, lane)` pairs can alias onto one Redis key. Example: group `portal` + lane `images` → `redisjm:portal:lane:images:queue`, byte-identical to the **default** queue of a group literally named `portal:lane:images`. Without lanes this was harmless (only fixed `:queue`/`:locks`/`:log` suffixes existed); the `:lane:` infix introduces it. Fix: reject `:` in `targetGroup` (validate in the constructor), or length/percent-encode each key segment. Pick one and lock it in the validator.

### 4.2 API surface (additive)

One optional field on job metadata:

```ts
// src/types.ts — JobMetadata
export interface JobMetadata {
  jobName: string
  description?: string
  lane?: string   // NEW. Omitted → default lane (legacy queue key).
}
```

Declaration is all a consumer needs:

```ts
manager.createJob({ jobName: 'store-images', lane: 'images' }, storeFn)
manager.createJob({ jobName: 'retention-sweep' /* no lane */ }, sweepFn)
```

### 4.3 Auto-subscription (the ergonomic core)

A consumer, when it calls `start()`, **polls exactly the union of lanes across its registered jobs** — no separate subscription config:

- A dedicated image worker registers only `store-images` (lane `images`) → services only the `images` lane.
- Web pods register the scheduled jobs (default lane) → service only the default lane.

**Producers still enqueue anything from anywhere.** Enqueue needs only a `Job` instance and the manager — it performs **no registration check** (`enqueue`, `src/redisjm.ts:669-703`, uses only `job.getJobId`/`job.getName`/lane). So the produce/consume split is driven by **registration**, not by `start()`:

- A *pure* producer constructs the manager and calls `job.queue(...)` without `start()`; nothing is polled.
- A **mixed producer+consumer** pod (the rvmode web pod: consumes default-lane scheduled jobs via `start()`, *and* enqueues the heavy `images` job) must **not** register the laned job it only produces — registering it would add `images` to its poll set (§5.6), and it would then pop the heavy handler and OOM: the exact failure §1 exists to prevent. To enqueue a lane it does not consume, construct an unregistered `new Job({ jobName, lane }, noopFn, manager)` and call `manager.queue(...)` / `job.queue(runId, inputs, manager)`. See the §11 `defineJob` note for the produce-only wrapper this implies.

The split is purely on the *consuming* side; producing is unchanged — but **"register to consume, construct-without-register to only produce"** is the load-bearing rule and must be documented, not left implicit.

### 4.4 Polling with `LMPOP`

Replace the single `LPOP` in `popAndExecute()` with an atomic multi-list pop across subscribed lanes:

```
LMPOP <numkeys> <lane-key-1> <lane-key-2> ... LEFT
```

- **Requires Redis ≥ 7.0.** (rvmode runs ElastiCache 7.1 and `ioredis ^5.9.2`, both fine.) Provide a **sequential-`LPOP` fallback** for Redis < 7 so the package stays usable everywhere — detect once (capability probe or an option) and branch. The fallback must honour the same lane ordering/rotation so `roundRobin` doesn't silently degrade to fixed-order priority (asserted in §9).
- **`__maintenance` is always polled first.** The reserved maintenance lane (§5.3) is prepended to the key list unconditionally — ahead of both the `priority` order and the `roundRobin` rotation. It holds at most one lock-deduped entry per interval, so front-loading it can never starve work, and it guarantees a saturated work lane cannot starve group-wide crash recovery.
- **Fairness:** `LMPOP` checks keys in the given order and pops from the first non-empty list → fixed order = **priority**. To avoid starving a low-priority lane, default to **round-robin** by rotating the *work-lane* key order each poll. Expose knobs:
  ```ts
  // src/types.ts — RedisJMOptions
  laneStrategy?: 'roundRobin' | 'priority'   // default 'roundRobin'
  lanePriority?: string[]                     // explicit high→low lane order for 'priority' (see below)
  ```
  With `'roundRobin'`, the manager rotates the work-lane order each poll (per-manager cursor, advanced once per poll). With `'priority'`, order is significant — but the poll set (§5.6) is an unordered *union*, so there is **no implicit order to encode priority with**: relying on job-registration / `Set` insertion order is fragile and undocumented. Callers therefore supply `lanePriority`; work lanes absent from it follow in a deterministic tail order (registration order). `__maintenance` is never listed — it is always first (above).

### 4.5 What stays exactly the same (and why that's the win)

- **Locks — unchanged.** Keyed by `jobName#runId`, group-wide. `jobName` is unique **per manager** (`registerJob`, `src/redisjm.ts:235`; group-wide uniqueness is a deployment convention, not enforced — see §5.5), and the group-wide lock dedupes `jobName#runId` across *all* lanes at once, so there is no cross-lane lock collision even if a `jobName` were mis-declared onto two lanes. The dedupe guarantee is preserved *verbatim*. **Do not add lane to the lock key** — that would be the *unsafe* option, letting the same run enqueue once per lane.
- **Log — unchanged.** Group-wide `redisjm:<group>:log`; `list()`/`get()` read all lanes. **Single monitoring pane across all worker types with zero change** — this is the decisive advantage over separate groups.
- **`isQueued` — unchanged** (lock-set membership, lane-independent).

---

## 5. The details that need care

### 5.1 Persist `lane` on the log record

Add `lane?: string` to `JobLogRecord` (`src/types.ts:71`), written at enqueue (`src/redisjm.ts:682-690`). Reason: **maintenance and `unqueue` run on instances that may not register the job**, yet must operate on the correct lane queue. They read the lane off the record rather than a local registry lookup.

### 5.2 Maintenance must resolve the lane per record

`performMaintenance` orphaned-queued check (`src/redisjm.ts:615`) currently does `LPOS redisjm:<group>:queue jobId`. Change it to `LPOS <queueKey(record.lane)> jobId`. Stale-running reclaim (`:605-613`) touches only the log + lock set → no queue key → unchanged.

### 5.3 Maintenance job itself lives on a reserved lane

The maintenance job (`__redisjm_maintenance`) must run for the **whole group**, but a pure image worker doesn't subscribe to the default lane, so it can't be the instance that runs default-lane maintenance. Options, in preference order:

1. **Reserved `__maintenance` lane that every instance implicitly subscribes to** (alongside its work lanes). The maintenance job is enqueued there; it stays lock-deduped (one run per interval) and every instance helps reclaim — preserving today's crash-resilience without polluting work lanes. Recommended.
2. Always-subscribe every instance to the default lane too — rejected: reintroduces the pop-and-requeue contention (§3) for default-lane *work* jobs on pure workers.

Implementation: give the built-in maintenance job `lane: '__maintenance'` (`src/maintenance.ts:28`), and have `start()`'s subscribed-lane set always include `__maintenance`. Keep the reserved lane out of the public lane namespace (reject user lanes starting with `__`).

### 5.4 `unqueue` resolves the lane

`unqueue(jobId)` (`src/redisjm.ts:187`) does `LREM redisjm:<group>:queue`. Read the record first for its `lane`, then `LREM <queueKey(lane)>`. (Or, if the record is already gone, `LREM` across all known lane queues — but reading the record is cleaner and matches maintenance.)

### 5.5 Unknown-job requeue must become lane-aware

With lanes, a consumer only pops from lanes it subscribes to, so it will normally always have a handler. The requeue net still matters for two narrow cases: (a) rolling deploys where a lane gains a new jobName not yet on all lane-subscribers, and (b) a lane with multiple jobNames where a consumer registers only some.

**`requeueUnknownJob` is NOT unchanged (correction).** Current code RPUSHes to the default/legacy key: `this.redis.rpush(this.getQueueKey(), jobId)` (`src/redisjm.ts:447`). But in *both* cases above the job was popped from a **named** lane, and a same-lane sibling that can handle it polls only its own lanes (§5.6) — never the default queue. Leaving the requeue on the default key strands the job off its lane: it bounces among default-lane pods (which also lack the handler), `requeueCount` climbs group-wide, and after `unknownJobRequeueLimit` it is dropped as `error: 'Job name is unknown'` — the exact §3 cross-worker drop-bug this proposal exists to eliminate, re-created through the requeue path. It also desyncs maintenance, whose new `LPOS queueKey(record.lane)` (§5.2) looks on the *named* lane and finds it absent. The record is already loaded at `src/redisjm.ts:436` and carries `lane` (§5.1), so the fix is one line: **RPUSH to `queueKey(record.lane)`**, exactly like `unqueue` (§5.4) and maintenance (§5.2).

**Isolation is lane-level, not handler-level (limitation — state it plainly).** Even with the lane-aware requeue, case (b) — one lane whose jobNames are split across consumers with *disjoint* handler sets — still reintroduces §3-style behavior *within* that lane: worker A (handles `store-images`) keeps popping `thumbnail-images`, requeues it back onto `images`; worker B (handles `thumbnail-images`) does the mirror; and under contention a valid job's persisted `requeueCount` can still reach the limit and be dropped. Lanes eliminate the cross-worker drop-bug **only between worker types that use *different* lanes.** The supported pattern is therefore **one lane per disjoint handler set** — every subscriber of a lane should register every jobName on that lane. Document this; do not claim the drop-bug is gone for arbitrarily-split shared lanes. (True per-jobName routing within a single lane would need handler-level routing, out of scope — use separate lanes.)

### 5.6 Subscribed-lane computation

Compute the poll set as: `{ lane of each registered job } ∪ { '__maintenance' }`, then map each lane to its queue key (default lane / `undefined` / `'default'` → legacy key). **Deduplicate on the resolved queue key, not the lane name** — otherwise a manager that registers both a no-lane job and an explicit `lane: 'default'` job feeds `LMPOP` the legacy key twice, double-weighting the default lane under `roundRobin`. Build a `Set<queueKey>` and prepend `queueKey('__maintenance')` (always first, §4.4).

Two contract points the first draft left ambiguous:

- **Not gated on `start()`.** `popAndExecute()` is public and exercised directly (≈15 existing tests call it with no `start()`). Derive the poll set from `registeredJobs ∪ { '__maintenance' }` **per call** (or a lazily-recomputed field), not from state only `start()` populates — otherwise those call sites (and any direct user of `popAndExecute`) poll nothing. The set is always ≥ 1 (`__maintenance`), so `numkeys` is never 0.
- **Recompute on register/unregister** (recommended over "fixed at `start()`"). The current library resolves handlers at pop time (`jobsByName.get`, `src/redisjm.ts:367`), so it already supports registering jobs after `start()`; freezing the lane set at `start()` would silently strand any job later registered on a new lane. Recompute is a cheap in-memory `Set` update, and `__maintenance` is unconditional so it is never affected by registration timing.

---

## 6. Backward compatibility & rolling deploy

- **No lane declared → identical behavior to 0.0.3**, same keys, same semantics. The feature is opt-in.
- **Pop path is safe both directions.** Old (lane-unaware) pods `LPOP redisjm:<group>:queue`; new pods `LMPOP` their subscribed lanes. Non-laned jobs stay on the legacy key, visible to both. Laned jobs go to lane keys that only new, subscribing instances read — old pods never pop work they couldn't handle. (Note: a new pod's poll set includes the legacy default key **only if it registers a default-lane job**; a *pure* named-lane worker does not poll the legacy key — §5.6. This corrects the first draft's claim that new pods always LMPOP the legacy key.)
- **The *maintenance* path is NOT safe while any 0.0.3 instance is live — the lane rollout must be staged.** `performMaintenance` scans the entire shared log (`src/redisjm.ts:592`) regardless of registration, and a 0.0.3 binary `LPOS`es the **legacy** queue for every `queued` record (`src/redisjm.ts:615`) — it cannot be patched to §5.2's `queueKey(record.lane)`. So while old and new instances co-maintain (both enqueue + race the maintenance job), an old run sees a new **laned** record in the shared log, `LPOS`es the legacy queue, finds it absent, stamps `suspectedAt`, and past the stale threshold marks the still-valid job `stale` and **releases its lock** (`src/redisjm.ts:614-627`). With the default `keepFinishedInterval: 0` the next pass `hdel`s the record while the entry still sits in the lane queue; the worker later pops it, `hget` returns `null`, and it is dropped as "no log record" (`src/redisjm.ts:391-398`) — silent loss. (Without cleanup, the freed lock instead permits a duplicate enqueue → double execution.) The default stale threshold is ~10 s; deploy windows are minutes, so the conditions align.

  **Required rollout (two-phase):**
  1. Ship the lane-aware release to **every** instance in the group with **no producer emitting a non-default lane yet** — all work stays on the legacy key, so no laned records exist for old maintenance to mishandle.
  2. Only once the whole group is confirmed lane-aware (no 0.0.3 binary remains) do producers start declaring lanes.

  Rollback is the mirror image: stop emitting laned work and let lanes drain before rolling back to a lane-unaware binary. "Safe in both directions" holds for the *pop* path only, and for maintenance only once the group is uniformly lane-aware.
- **Maintenance job placement during cutover.** A 0.0.3 instance enqueues the maintenance job to the legacy queue and holds the group-wide lock; a *pure* new worker doesn't poll the legacy key, and `reclaimStaleMaintenanceLock` deliberately won't reclaim a `queued` record (`src/redisjm.ts:544-571`). For a group that moves **all** work to lanes with **no** surviving default-lane consumer, that last legacy maintenance entry can strand with its lock held, deadlocking group-wide maintenance. **Implemented mitigation:** `reclaimStaleMaintenanceLock` now *relocates* such an entry. At startup, if the maintenance record is `queued` and its jobId is present on the legacy queue (via `LPOS`), it stamps `record.lane = '__maintenance'`, then `LREM`s the legacy entry and `RPUSH`es it onto the `__maintenance` lane — lock held throughout, `LREM` before `RPUSH` (never on both queues at once), and skipping the `RPUSH` if a concurrent default-lane consumer already popped it (`LREM` returned 0). (The earlier "reclaim if absent from both queues" phrasing was imprecise — the stranded entry is *present* on the legacy queue, so relocation, not reclamation, is the fix.) The operational precondition — keep ≥ 1 default-lane consumer across the cutover — also avoids it, and rvmode's topology is immune regardless.
- **Locks/log wire format:** adding optional `lane` to `JobLogRecord` is forward/backward compatible (older code ignores the extra field on parse; `parseRecord` is permissive). The hazard above is not the wire format — it is old code *acting* on the shared log via the wrong queue key.

---

## 7. Alternatives considered (and rejected)

- **Federated groups + cross-group `list()`/supervisor.** Smaller core change, but only *packages* the fragmentation — still N managers plus a merge layer for monitoring. Lanes give the same isolation with native single-pane observability. Rejected.
- **Redis Streams + consumer groups.** The textbook multi-worker primitive: `XREADGROUP` per worker type, `XAUTOCLAIM` would replace the entire heartbeat/stale/maintenance machinery. But it's a ground-up rewrite of the List+Set+Hash model and its hard-won crash-recovery edge cases (`suspectedAt` two-pass, orphaned-queued reclaim, maintenance-lock self-deadlock handling). Worth considering for a future `1.0`, out of scope here.

---

## 8. Implementation checklist (file by file)

- [ ] **`src/types.ts`**
  - `JobMetadata.lane?: string`
  - `JobLogRecord.lane?: string`
  - `RedisJMOptions.laneStrategy?: 'roundRobin' | 'priority'` and `RedisJMOptions.lanePriority?: string[]` (+ resolved defaults in `ResolvedRedisJMOptions`: `laneStrategy: 'roundRobin'`, `lanePriority: []`). **Note:** the two `getOptions()` default-assertion tests (`redisjm.test.ts:25`, `:36`) must gain the new resolved keys — this is why §9's "existing tests pass unchanged" is scoped to routing/queue-key behavior, not literally every assertion.
- [ ] **`src/job.ts`**
  - Carry `lane` through `getMetadata()` (`:132`); add `getLane(): string | undefined`.
- [ ] **`src/redisjm.ts`**
  - `queueKey(lane?)` helper: `undefined`/`'default'` → `redisjm:<group>:queue`; named → `redisjm:<group>:lane:<lane>:queue`. Keep `getLocksKey`/`getLogKey` group-wide.
  - **Constructor (`:79`): validate `targetGroup`** (reject `:`, or encode key segments) so the `:lane:` infix stays collision-proof (§4.1).
  - `enqueue` (`:669`): stamp `record.lane`; push to `queueKey(job lane)`. **Validate the lane here too** — the producer path never touches `registerJob`.
  - `start` (`:468`): compute the subscribed lane-key set (§5.6); poll via `LMPOP` with `__maintenance` first, then round-robin rotation (or `lanePriority` order); sequential-`LPOP` fallback for Redis < 7 (same ordering).
  - `popAndExecute` (`:353`): derive the lane set **per call** (§5.6, not gated on `start()`); pop via `LMPOP`/fallback (unwrap the `[key,[elem]]` reply to the bare `jobId`); the rest (parse `jobName#runId`, handler lookup, execute) is unchanged.
  - **`requeueUnknownJob` (`:432`): RPUSH to `queueKey(record.lane)`, not `getQueueKey()`** (record already read at `:436`) — §5.5. This is the one-line fix that keeps laned requeues on their lane.
  - `unqueue` (`:187`): resolve lane from record, `LREM` the right queue.
  - `performMaintenance` (`:589`): `LPOS` against `queueKey(record.lane)`.
  - `reclaimStaleMaintenanceLock` (`:544`): optionally reclaim a `queued __redisjm_maintenance#` absent from both the `__maintenance` lane and the legacy queue (§6 cutover mitigation).
  - Validate lane names (`^[A-Za-z0-9_-]+$`, no `#`/`:`, no `__` prefix for user lanes); **exempt `jobName === MAINTENANCE_JOB_NAME`** so the internal job's `__maintenance` lane is allowed.
- [ ] **`src/maintenance.ts`**
  - Give the built-in job `lane: '__maintenance'` (`:28`).
- [ ] **`src/__tests__/mock-redis.ts`**
  - Implement `LMPOP` with faithful first-non-empty-in-key-order semantics (the §9 fairness tests depend on it); confirm `LPOS`/`LREM`/`LPUSH`/`RPUSH` already present.
- [ ] **README + CHANGELOG**
  - Document lanes, the register-to-consume / construct-without-register-to-produce rule (§4.3), `laneStrategy` + `lanePriority`, the Redis 7 requirement for `LMPOP` (+ fallback), the **staged rollout** (§6), and the one-lane-per-handler-set guidance (§5.5).

## 9. Test plan

- **Routing:** a manager registering only lane-A jobs never pops a lane-B job (no requeue, no drop) — the §3 regression, asserted directly.
- **Auto-subscription:** subscribed lane set = union of registered job lanes ∪ `__maintenance`; default-laned jobs still served on the legacy key; poll set is deduped on resolved queue key (a no-lane job + an explicit `lane: 'default'` job do not double-weight the legacy key).
- **Requeue stays on its lane (§5.5):** a lane-`images` subscriber lacking the handler requeues to the **`images`** lane key (not `redisjm:<group>:queue`), and a same-lane sibling that registers the handler then pops it. Assert maintenance `LPOS queueKey('images')` agrees (job not marked stale).
- **Handler-level split contention (§5.5 limitation):** two subscribers of one lane with disjoint handler sets — assert the documented behavior (a job reaches its handler within budget under the one-lane-per-handler-set pattern; and, as a negative, that arbitrarily splitting a lane can still exhaust `requeueCount`, justifying the guidance).
- **Backward compat:** a job with no lane uses `redisjm:<group>:queue` byte-for-byte. Existing *routing* tests pass unchanged; the direct-`popAndExecute` tests (no `start()`) still work because the lane set is computed per call; the handler-less unknown-job suite is **rewritten** (a handler-less manager subscribes to no work lane and legitimately won't pop the default queue); the two `getOptions()` default assertions gain the new resolved keys.
- **Fairness:** with two non-empty lanes and `roundRobin`, both drain (no starvation); with `priority` + `lanePriority`, the listed order wins; `__maintenance` is popped even when a higher work lane is saturated under `priority`.
- **Maintenance across lanes:** orphaned-queued reclaim `LPOS`es the correct lane queue; stale-running reclaim unchanged; maintenance runs on a pure-worker instance via `__maintenance` and reclaims a **default-lane** orphan it never polls.
- **Staged-rollout hazard (§6):** simulate an old-style maintenance (`LPOS` legacy queue only) against a laned `queued` record and assert it would false-`stale`/unlock it — encoding *why* the two-phase rollout is required (and that a uniformly lane-aware group does not exhibit it).
- **Maintenance cutover strand (§6):** a `queued __redisjm_maintenance#` stranded on the legacy queue with no default-lane consumer is relocated onto the `__maintenance` lane by `reclaimStaleMaintenanceLock` at startup, then popped and run.
- **`unqueue`** removes from the correct lane queue.
- **Group-name collision (§4.1):** a `targetGroup` containing `:` (or a group/lane pair that would alias) is rejected by the constructor validator.
- **Locks:** dedupe still keyed by `jobName#runId`, independent of lane.
- **Redis < 7 fallback:** sequential-`LPOP` path passes the same routing/fairness assertions (drive via mock or a version flag).

## 10. Open decisions for the implementer

1. **Lane key infix + group encoding** — `:lane:` proposed; collision-proofing now *also* requires constraining `targetGroup` (reject `:`) or encoding key segments (§4.1). Pick one and lock it in the validator.
2. **Recompute lanes on register/unregister** — recommended (§5.6); "fixed at `start()`" would strand jobs registered on a new lane after `start()`, regressing today's late-registration support. Downgraded from an open toss-up to a recommendation.
3. **`laneStrategy` default** — `roundRobin` (safe against starvation); `priority` opt-in and now requires `lanePriority` for a defined order (§4.4).
4. **Version bump** — additive & non-breaking argues `0.0.4`; the new required-for-lanes Redis 7 `LMPOP` (with fallback), the new public options, **and the staged-rollout requirement (§6)** push toward `0.1.0`. Recommend `0.1.0` so the rollout constraint sits on a visible minor-version boundary.
5. **Redis-version detection** for the `LMPOP`/`LPOP` branch — capability probe at `start()` vs. an explicit option. Probe is more ergonomic; option is more predictable.
6. **Maintenance cutover strand (§6)** — RESOLVED: implemented as a startup *relocation* in `reclaimStaleMaintenanceLock` (moves the stranded legacy entry to the `__maintenance` lane). The operational precondition (keep a default-lane consumer during cutover) remains an alternative; rvmode's topology is immune regardless.

---

## 11. Downstream context (rvmode) — why this is worth doing

The immediate consumer is the rvmode portal. This feature **simplifies** the planned dedicated-Fargate image worker instead of complicating it. Without lanes, the plan was a *separate group* (`portal-images`) with duplicated engine wiring (`RedisJmService` / `JobScheduler` / `JobRegistry`) and a `/jobs` admin grid that could only see one group. With lanes:

- `defineJob` gains an optional `lane`; the heavy store job is declared `lane: 'images'`.
- The Fargate image worker registers only that job and auto-services the `images` lane; nothing heavy runs on the web pods.
- The web pods stay producer-only for it — which, per §4.3, means they must **not** register the heavy job (registering it would auto-subscribe them to `images` and OOM). `defineJob` therefore needs a produce-only mode (register for typed enqueue without adding the lane to the poll set), or the web pods construct an unregistered `Job` to enqueue. This is the one place the "single `defineJob` everywhere" story needs a produce-vs-consume distinction.
- **One group, one `/jobs` grid across both worker types, one maintenance loop** — the isolation we want, minus the workaround tax.

So the payoff is concrete and near-term: the same feature that makes redisjm properly multi-worker also removes the main source of complexity from the rvmode worker design.
