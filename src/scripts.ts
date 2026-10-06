import { createHash } from 'node:crypto'
import type Redis from 'ioredis'

/**
 * A server-side Lua script, run via `EVALSHA` with an `EVAL` fallback on `NOSCRIPT` (the script cache
 * is per server and is emptied by a restart / failover / `SCRIPT FLUSH`).
 *
 * Every script starts with a `#!lua` shebang. The ones that may write declare NO flags. That is
 * deliberate: a flag-less shebang script is NOT `allow-oom`, so when Redis is over `maxmemory` the server
 * refuses the WHOLE script up front (OOM error, nothing executed). For the pop script this is what
 * guarantees that a full Redis pops nothing — a legacy (shebang-less) script would run its LPOP and only
 * fail at the first memory-growing write, losing the popped id. The deletion-only scripts (purge, job
 * prune, compare-and-delete) are flagged `allow-oom` instead: freeing memory while Redis is full is
 * exactly when they matter. Shebang scripts need Redis >= 7.0.
 */
export interface LuaScript {
  readonly source: string
  readonly sha: string
}

/**
 * SHA-1 hex of a string (hashed as UTF-8) or of raw bytes — what the scripts compare against
 * (`redis.sha1hex`, over the stored bytes).
 */
export function sha1Hex(value: string | Buffer): string {
  return createHash('sha1').update(value).digest('hex')
}

function defineScript(source: string): LuaScript {
  return { source, sha: sha1Hex(source) }
}

/**
 * Atomic enqueue of one or more runs of ONE job (`enqueue` / `enqueueMany`). Per entry, in order:
 * already locked → `deduped`; job already has `>= maxInFlight` locked runs → `busy`; lane list already
 * `>= cap` long → `full`; else take the run lock, add it to the job's lock set, write the record, push
 * it onto its lane (or ZADD it to the delayed set), and register the lane's list KEY in the job's lane
 * set and the job in the registry → `queued`. Being one flag-less shebang script, a Redis at maxmemory refuses it
 * up front: an OOM-refused enqueue writes NOTHING (no lock, no record) — nothing to roll back.
 *
 * KEYS: 1 locks, 2 log, 3 delayed, 4 lane list, 5 job lock set, 6 job lane set, 7 job registry.
 * ARGV: 1 job name, 2 mode ('R' tail | 'L' head | 'D' delayed), 3 cap (-1 = none), 4 maxInFlight
 *       (-1 = none), then per entry: jobId, record JSON, delayed score.
 * Returns the status per entry, in entry order. Statuses are decided in entry order (earlier entries win
 * a cap / maxInFlight slot); mode 'L' then pushes the accepted entries in reverse so a batch keeps its
 * order at the head of the lane.
 */
export const ENQUEUE_SCRIPT = defineScript(`#!lua
-- redisjm:enqueue v2
local cap = tonumber(ARGV[3])
local maxInFlight = tonumber(ARGV[4])
local mode = ARGV[2]
local n = (#ARGV - 4) / 3
local out = {}
-- Pass 1: decide every entry's status in ENTRY order (earlier entries win a cap / maxInFlight slot).
local accepted = {}
local acceptedIds = {}
local length = 0
if cap >= 0 then length = redis.call('LLEN', KEYS[4]) end
local inFlight = 0
if maxInFlight >= 0 then inFlight = redis.call('SCARD', KEYS[5]) end
for i = 0, n - 1 do
  local id = ARGV[5 + i * 3]
  if acceptedIds[id] or redis.call('SISMEMBER', KEYS[1], id) == 1 then
    out[i + 1] = 'deduped'
  elseif maxInFlight >= 0 and inFlight >= maxInFlight then
    out[i + 1] = 'busy'
  elseif cap >= 0 and length >= cap then
    out[i + 1] = 'full'
  else
    out[i + 1] = 'queued'
    acceptedIds[id] = true
    accepted[#accepted + 1] = i
    inFlight = inFlight + 1
    -- Caps count the lane LIST only: a delayed run doesn't occupy it until promoted.
    if mode ~= 'D' then length = length + 1 end
  end
end
-- Pass 2: write the accepted entries ('L' pushes in reverse so the batch keeps its order at the head).
local first, last, step = 1, #accepted, 1
if mode == 'L' then first, last, step = #accepted, 1, -1 end
for k = first, last, step do
  local i = accepted[k]
  local id = ARGV[5 + i * 3]
  redis.call('SADD', KEYS[1], id)
  redis.call('SADD', KEYS[5], id)
  redis.call('HSET', KEYS[2], id, ARGV[6 + i * 3])
  if mode == 'D' then
    redis.call('ZADD', KEYS[3], ARGV[7 + i * 3], id)
  elseif mode == 'L' then
    redis.call('LPUSH', KEYS[4], id)
  else
    redis.call('RPUSH', KEYS[4], id)
  end
end
if #accepted > 0 then
  redis.call('SADD', KEYS[6], KEYS[4])
  redis.call('SADD', KEYS[7], ARGV[1])
end
return out
`)

/**
 * Atomic pop: take the first eligible entry from the lane lists (in the given order) AND record it in
 * the `claiming` zset (score = pop time) in the same step, so a popped-but-not-yet-claimed run is
 * never invisible: it is either on its lane list or in `claiming` until its claim lands.
 *
 * Per lane, ARGV carries a spec: `'*'` = the lane is this instance's own — LPOP its head. Otherwise the
 * spec is an ALLOW-LIST `#jobA#jobB#` (job names can't contain '#'): the lane is an OLD lane some of
 * this instance's jobs still have entries on (their lane changed across a deploy), and only entries of
 * those jobs may be taken — the first match within the first `scanLimit` entries, removed with LREM —
 * so draining an old lane never pops (and requeues, burning their budget) other jobs' entries.
 *
 * KEYS[1] = claiming zset, KEYS[2..n] = lane lists in poll order.
 * ARGV[1] = now (epoch ms), ARGV[2] = scanLimit, ARGV[2 + i] = spec for KEYS[1 + i].
 * Returns `{ laneKey, jobId }` or nil.
 */
export const POP_SCRIPT = defineScript(`#!lua
-- redisjm:pop v2
local scanLimit = tonumber(ARGV[2])
for i = 2, #KEYS do
  local spec = ARGV[i + 1]
  local id = false
  if spec == '*' then
    id = redis.call('LPOP', KEYS[i])
  else
    for _, candidate in ipairs(redis.call('LRANGE', KEYS[i], 0, scanLimit - 1)) do
      local hash = string.find(candidate, '#', 1, true)
      if hash and string.find(spec, '#' .. string.sub(candidate, 1, hash - 1) .. '#', 1, true) then
        redis.call('LREM', KEYS[i], 1, candidate)
        id = candidate
        break
      end
    end
  end
  if id then
    redis.call('ZADD', KEYS[1], ARGV[1], id)
    return {KEYS[i], id}
  end
end
return false
`)

/**
 * Compare-and-set of one log record plus the queue-structure side effects of that transition, all in
 * one atomic step. The record is written only if its stored JSON still hashes to `expected` — the SHA-1
 * of the value the caller read (or last wrote) and mutated, sent instead of the value itself so a write
 * doesn't ship the record twice — so a concurrent writer (a claim, a heartbeat, a finish, another
 * instance's maintenance) can never be overwritten by a stale read-modify-write, and a record deleted
 * meanwhile is never re-created (`-1`); the caller re-reads and retries.
 *
 * KEYS: 1 log hash, 2 claiming zset, 3 locks set, 4 delayed zset, 5 target list (any key when unused),
 *       6 the job's lock set, 7 the job's lane set.
 * ARGV: 1 jobId, 2 SHA-1 hex of the expected JSON, 3 new JSON, 4 push side ('' | 'L' | 'R' onto KEYS[5]),
 *       5 claiming op, 6 delayed op, 7 delayed score (with delayed op 'add'), 8 lock op,
 *       9 field TTL in ms (with lock op 'retire'; 0 = none).
 * Ops ('' = none):
 * - claiming: 'take' = precondition: ZREM the claiming entry and abort unless it was there (a requeue of
 *   an unclaimed pop — two overlapping requeues must not BOTH push it back); 'remove' = ZREM it.
 * - delayed: 'take' = precondition: ZREM the delayed entry and abort unless it was there (the promotion
 *   claim); 'add' = ZADD it with ARGV[7] (retry scheduling).
 * - lock: 'take' = (re-)SADD the run lock (global + job set); 'retire' = the record turns terminal: write
 *   it, give it a hash-field TTL of ARGV[9] ms when > 0 (`HPEXPIRE`, Redis >= 7.4 — `pcall` makes it a
 *   no-op on older servers) and SREM the lock; 'drop' = like 'retire' but HDEL the record instead of
 *   writing it (no history kept). Retiring HERE, atomically with the terminal write, is what makes the
 *   order of record and lock moot: a separate HDEL / HPEXPIRE / SREM after the write could land on a
 *   record written in between — a re-enqueue of the runId (its lock is free) or a resurrected run — and
 *   delete, expire or unlock that live run.
 * A push also registers KEYS[5] in the job's lane set (old-lane draining depends on it).
 * Returns 1 written, 0 conflict (stored JSON changed), -1 record missing, -2 precondition failed.
 */
export const TRANSITION_SCRIPT = defineScript(`#!lua
-- redisjm:transition v4
local id = ARGV[1]
local cur = redis.call('HGET', KEYS[1], id)
if not cur then return -1 end
if redis.sha1hex(cur) ~= ARGV[2] then return 0 end
if ARGV[6] == 'take' and redis.call('ZREM', KEYS[4], id) == 0 then return -2 end
if ARGV[5] == 'take' and redis.call('ZREM', KEYS[2], id) == 0 then return -2 end
local lock = ARGV[8]
if lock == 'drop' then
  redis.call('HDEL', KEYS[1], id)
else
  redis.call('HSET', KEYS[1], id, ARGV[3])
  if lock == 'retire' and tonumber(ARGV[9]) > 0 then
    redis.pcall('HPEXPIRE', KEYS[1], ARGV[9], 'FIELDS', 1, id)
  end
end
if ARGV[4] == 'L' or ARGV[4] == 'R' then
  if ARGV[4] == 'L' then redis.call('LPUSH', KEYS[5], id) else redis.call('RPUSH', KEYS[5], id) end
  redis.call('SADD', KEYS[7], KEYS[5])
end
if ARGV[5] == 'remove' then redis.call('ZREM', KEYS[2], id) end
if lock == 'retire' or lock == 'drop' then
  redis.call('SREM', KEYS[3], id)
  redis.call('SREM', KEYS[6], id)
elseif lock == 'take' then
  redis.call('SADD', KEYS[3], id)
  redis.call('SADD', KEYS[6], id)
end
if ARGV[6] == 'add' then redis.call('ZADD', KEYS[4], ARGV[7], id) end
return 1
`)

/**
 * Removes one run's state — its record and run lock plus the requested queue-structure entries — in one
 * atomic, deletion-only step (`allow-oom`: it must work while Redis is full). Being one step, the record
 * and its lock go together: there is no window in which the lock is free while the record still exists,
 * which would let a producer re-enqueue the runId and then have its fresh record deleted.
 *
 * KEYS: 1 log, 2 locks, 3 the job's lock set, 4 claiming, 5 delayed, 6 suspects, 7 lane list (any key
 *       when unused).
 * ARGV: 1 jobId, 2 mode, 3 flags, 4 witness ('orphan' mode).
 * Modes: 'force' = purge unconditionally (`unqueue`, a malformed popped id); 'orphan' = purge only while
 *        the stored value is still what the CALLER judged not to be a record: absent (witness `''`), or
 *        the very garbage it read (witness = SHA-1 of its raw bytes). The script never judges validity
 *        itself — Lua's cjson disagrees with `JSON.parse` both ways (NaN / hex / `1.` / raw control chars
 *        accepted; nesting > 1000 rejected) — so the manager's parser stays the single source of truth.
 *        Any other value (the runId was re-enqueued meanwhile) belongs to that run → no-op. Garbage that
 *        vanished meanwhile still purges: nothing backs the jobId.
 * Flags: 'q' = LREM it from KEYS[7]; 'c' = ZREM from claiming; 'd' = ZREM from delayed; 's' = HDEL its
 *        orphaned-lock suspicion.
 * Returns 1 purged, 0 skipped ('orphan' mode, the value changed).
 */
export const PURGE_SCRIPT = defineScript(`#!lua flags=allow-oom
-- redisjm:purge v2
local id = ARGV[1]
local flags = ARGV[3]
if ARGV[2] == 'orphan' then
  local cur = redis.call('HGET', KEYS[1], id)
  if cur and redis.sha1hex(cur) ~= ARGV[4] then return 0 end
end
redis.call('HDEL', KEYS[1], id)
redis.call('SREM', KEYS[2], id)
redis.call('SREM', KEYS[3], id)
if string.find(flags, 'q', 1, true) then redis.call('LREM', KEYS[7], 1, id) end
if string.find(flags, 'c', 1, true) then redis.call('ZREM', KEYS[4], id) end
if string.find(flags, 'd', 1, true) then redis.call('ZREM', KEYS[5], id) end
if string.find(flags, 's', 1, true) then redis.call('HDEL', KEYS[6], id) end
return 1
`)

/**
 * Maintenance upkeep of one job's bookkeeping sets: drop job-lock-set members whose run lock is gone
 * (a random sample of up to `sample` members per call, so drift self-heals over passes without an O(k)
 * script), drop lane-list keys from the job's lane set whose list is empty (atomic with every push, which
 * re-registers its lane, so a lane can't be pruned while it holds an entry), and drop the job from the
 * registry once both sets are empty. It only ever deletes, so it is flagged `allow-oom` (it may run
 * while Redis is full — that is when freeing bookkeeping matters).
 *
 * KEYS: 1 locks, 2 job lock set, 3 job lane set, 4 job registry.
 * ARGV: 1 job name, 2 sample size.
 * Returns the number of pruned lock-set members + lanes.
 */
export const PRUNE_JOB_SCRIPT = defineScript(`#!lua flags=allow-oom
-- redisjm:prune-job v2
local pruned = 0
local sample = redis.call('SRANDMEMBER', KEYS[2], tonumber(ARGV[2]))
for _, id in ipairs(sample) do
  if redis.call('SISMEMBER', KEYS[1], id) == 0 then
    redis.call('SREM', KEYS[2], id)
    pruned = pruned + 1
  end
end
for _, key in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  if redis.call('LLEN', key) == 0 then
    redis.call('SREM', KEYS[3], key)
    pruned = pruned + 1
  end
end
if redis.call('SCARD', KEYS[2]) == 0 and redis.call('SCARD', KEYS[3]) == 0 then
  redis.call('SREM', KEYS[4], ARGV[1])
end
return pruned
`)

/**
 * Compare-and-delete: removes each hash field / sorted-set member only while it is still exactly what the
 * caller read. Deletion-only, so `allow-oom` (maintenance must free memory while Redis is full).
 * - `'h'` (log records): check = SHA-1 of the value read — of the scanned JSON for an expired terminal
 *   record, of the RAW bytes for garbage (a garbage value need not be valid UTF-8, so a decoded copy
 *   needn't hash like the stored bytes). Without it, a record that expired between the scan and the
 *   delete could have been REPLACED by a fresh enqueue of the same runId (its lock is free) — and a blind
 *   HDEL would delete that new run's record. Validity is never judged here (see `PURGE_SCRIPT`).
 * - `'z'` (`claiming` / `delayed` entries): check = the score read. A blind ZREM would erase an entry
 *   re-added since — a fresh pop's `claiming` mark, a retry's `delayed` entry.
 *
 * KEYS: 1 the hash / sorted set. ARGV: 1 kind ('h' | 'z'), then pairs of (field / member, check).
 * Returns the fields / members actually deleted.
 */
export const DELETE_IF_UNCHANGED_SCRIPT = defineScript(`#!lua flags=allow-oom
-- redisjm:delete-if-unchanged v3
local zset = ARGV[1] == 'z'
local deleted = {}
for i = 2, #ARGV, 2 do
  local member = ARGV[i]
  local doomed = false
  if zset then
    local score = redis.call('ZSCORE', KEYS[1], member)
    doomed = score and tonumber(score) == tonumber(ARGV[i + 1])
  else
    local cur = redis.call('HGET', KEYS[1], member)
    doomed = cur and redis.sha1hex(cur) == ARGV[i + 1]
  end
  if doomed then
    if zset then redis.call('ZREM', KEYS[1], member) else redis.call('HDEL', KEYS[1], member) end
    deleted[#deleted + 1] = member
  end
end
return deleted
`)

/** Lua snippet shared by the fleet scripts: `now` = Redis SERVER time in ms (client clock skew is irrelevant). */
const LUA_NOW_MS = `local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)`

/**
 * Fleet presence refresh: prunes instances whose lease lapsed (at most 100 per call, only when there are
 * some) and renews the calling instance's lease — its expiry (`now + ttl`, Redis SERVER time) in the
 * `instances` zset. The info JSON in the `instance-info` hash is written only when passed (ARGV 3
 * non-empty); without it, an instance whose info is gone from the hash (lapsed and pruned, flushed) is NOT
 * re-registered and the script returns `1` so the caller resends WITH its info. Flag-less shebang script:
 * under maxmemory the whole refresh is refused up front (the instance then lapses after its ttl, which is
 * truthful — a Redis at maxmemory refuses every pop anyway). Returns `0` on success.
 *
 * KEYS: 1 instances zset, 2 instance-info hash. ARGV: 1 instanceId, 2 ttlMs, 3 info JSON or '', 4 key ttlMs.
 *
 * Both keys also get a key-level `PEXPIRE` (ARGV 4), refreshed on every successful write, so a group whose
 * instances all died does not keep them forever. The caller passes a multiple of the lease ttl. `GT` (Redis >= 7)
 * keeps the TTL monotonic: instances with different heartbeat intervals never shorten each other's.
 */
export const PRESENCE_SCRIPT = defineScript(`#!lua
-- redisjm:presence v4
${LUA_NOW_MS}
local dead = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 100)
if #dead > 0 then
  for _, id in ipairs(dead) do
    redis.call('HDEL', KEYS[2], id)
    redis.call('ZREM', KEYS[1], id)
  end
end
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
elseif redis.call('HEXISTS', KEYS[2], ARGV[1]) == 0 then
  return 1
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
for k = 1, 2 do
  -- GT never sets a TTL on a key that has none (no TTL counts as infinite), so a fresh key needs a plain PEXPIRE.
  if redis.call('PTTL', KEYS[k]) < 0 then
    redis.call('PEXPIRE', KEYS[k], ARGV[4])
  else
    redis.call('PEXPIRE', KEYS[k], ARGV[4], 'GT')
  end
end
return 0
`)

/**
 * Fleet read: the live instances (lease not yet lapsed on Redis SERVER time) with their info JSON.
 * Read-only (`no-writes`: runs under maxmemory and on replicas); the lapsed entries are left for the next
 * presence refresh to prune.
 *
 * KEYS: 1 instances zset, 2 instance-info hash.
 * Returns `{ now, id1, expiresAt1, infoJson1, id2, … }` (`infoJson` is nil when the hash field is gone).
 */
export const FLEET_SCRIPT = defineScript(`#!lua flags=no-writes
-- redisjm:fleet v1
${LUA_NOW_MS}
local live = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. now, '+inf', 'WITHSCORES')
local out = { now }
for i = 1, #live, 2 do
  out[#out + 1] = live[i]
  out[#out + 1] = live[i + 1]
  out[#out + 1] = redis.call('HGET', KEYS[2], live[i])
end
return out
`)

/**
 * Queue listing in pop order — one atomic, read-only snapshot (`no-writes`: runs under maxmemory and on
 * replicas). The ordered sequence is: runs popped and not yet claimed (`claiming`, by pop time), then the
 * lane lists in the given order (head → tail), then the delayed set (by `readyAt`). Entries are filtered
 * by an optional job-name prefix and an optional lane label; `offset` matching entries are skipped and up
 * to `limit` returned (plus one match of lookahead to know whether more exist).
 *
 * The delayed and claiming sets hold bare jobIds, so THEIR lane lives only in the record: it is resolved
 * server-side with `cjson.decode` (never shipping records to the client; a string search for `"lane"`
 * would be unsafe — 0.1.x records serialize `inputs` BEFORE `lane`, so nested input keys could match) —
 * for every candidate under a lane filter, otherwise only for entries that land in the page. Records are
 * decoded at most `decodeCap` times and at most `scanCap` entries are walked per call; hitting either stops
 * the listing early (`complete = 0`). Without a job filter, list / zset prefixes are skipped
 * arithmetically (`LLEN` / `ZCARD`), so only the window is read.
 *
 * KEYS: 1 claiming zset, 2 delayed zset, 3 log hash, 4.. lane lists in output order.
 * ARGV: 1 lane filter label ('' = none), 2 job prefix (`'<name>#'` or ''), 3 offset, 4 limit, 5 decode cap,
 *       6 scan cap, then one lane label per lane list (ARGV[6 + n] labels KEYS[3 + n]).
 * Returns `{ complete, more, kind, id, score, lane, kind, … }` — kind `'c'` claiming / `'l'` list /
 * `'d'` delayed, score = pop time / '' / readyAt, lane = label ('' when unknown).
 */
export const LIST_QUEUED_SCRIPT = defineScript(`#!lua flags=no-writes
-- redisjm:list-queued v1
local LIST_CHUNK = 500
local ZSET_CHUNK = 100
local filterLane = ARGV[1]
local prefix = ARGV[2]
local offset = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local decodeCap = tonumber(ARGV[5])
local scanCap = tonumber(ARGV[6])
local plain = prefix == '' and filterLane == ''
local decodes = 0
local scanned = 0
local skipped = 0
local count = 0
local more = 0
local complete = 1
local out = {}

-- A run's lane label from its record ('' = no record / unreadable); nil when the decode cap is spent.
local function resolveLane(id)
  if decodes >= decodeCap then return nil end
  decodes = decodes + 1
  local raw = redis.call('HGET', KEYS[3], id)
  if not raw then return '' end
  local ok, rec = pcall(cjson.decode, raw)
  if not ok or type(rec) ~= 'table' then return '' end
  local lane = rec['lane']
  if type(lane) ~= 'string' or lane == '' or lane == 'default' then return 'default' end
  return lane
end

-- Considers one entry; returns true when the listing is finished.
local function visit(kind, id, score, label)
  if prefix ~= '' and string.sub(id, 1, #prefix) ~= prefix then return false end
  local lane = label
  -- A delayed / claiming entry's lane lives in its record: resolved when it decides the lane filter or
  -- when the entry lands in the page (never for the skipped prefix or past the page).
  if kind ~= 'l' and (filterLane ~= '' or (skipped >= offset and count < limit)) then
    lane = resolveLane(id)
    if lane == nil then
      if count >= limit then more = 1 else complete = 0 end
      return true
    end
  end
  if filterLane ~= '' and lane ~= filterLane then return false end
  if skipped < offset then
    skipped = skipped + 1
    return false
  end
  if count >= limit then
    more = 1
    return true
  end
  count = count + 1
  out[#out + 1] = kind
  out[#out + 1] = id
  out[#out + 1] = score
  out[#out + 1] = lane
  return false
end

-- Walks one source in chunks (kind 'l' = lane list, else a zset by score); returns true when the listing
-- is finished. Without filters the skipped prefix is skipped arithmetically (LLEN / ZCARD) and only the
-- page window (+1 lookahead) is read.
local function walk(kind, key, label)
  local isList = kind == 'l'
  if isList and filterLane ~= '' and label ~= filterLane then return false end
  local from = 0
  if plain then
    local size
    if isList then size = redis.call('LLEN', key) else size = redis.call('ZCARD', key) end
    if skipped + size <= offset then
      skipped = skipped + size
      return false
    end
    from = offset - skipped
    skipped = offset
  end
  while true do
    local n = isList and LIST_CHUNK or ZSET_CHUNK
    if plain then n = math.min(n, limit - count + 1) end
    local chunk, width
    if isList then
      chunk = redis.call('LRANGE', key, from, from + n - 1)
      width = 1
    else
      chunk = redis.call('ZRANGE', key, from, from + n - 1, 'WITHSCORES')
      width = 2
    end
    for j = 1, #chunk, width do
      scanned = scanned + 1
      if scanned > scanCap then
        complete = 0
        return true
      end
      if visit(kind, chunk[j], isList and '' or chunk[j + 1], label) then return true end
    end
    if #chunk < n * width then return false end
    from = from + n
  end
end

local function run()
  if walk('c', KEYS[1], '') then return end
  for i = 4, #KEYS do
    if walk('l', KEYS[i], ARGV[i + 3]) then return end
  end
  walk('d', KEYS[2], '')
end
run()

local res = { complete, more }
for i = 1, #out do res[#res + 1] = out[i] end
return res
`)

/**
 * Runs `script` via EVALSHA, falling back to EVAL (which also loads it into the server's cache) when
 * the server answers `NOSCRIPT`. Any other error propagates unchanged (e.g. an OOM refusal).
 */
export async function runScript(redis: Redis, script: LuaScript, keys: string[], args: Array<string | number>): Promise<unknown> {
  try {
    return await redis.evalsha(script.sha, keys.length, ...keys, ...args)
  } catch (err) {
    if (err instanceof Error && /^NOSCRIPT\b/.test(err.message)) {
      return redis.eval(script.source, keys.length, ...keys, ...args)
    }
    throw err
  }
}
