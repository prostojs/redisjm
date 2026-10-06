import { vi } from 'vitest'
import type Redis from 'ioredis'
import { sha1Hex } from '../scripts'

/** The exact error Redis replies with when a write is refused at maxmemory under `noeviction`. */
export function oomError(): Error {
  const err = new Error("OOM command not allowed when used memory > 'maxmemory'.")
  err.name = 'ReplyError'
  return err
}

/** What ioredis rejects a command with once its connection is gone. */
export function connectionError(): Error {
  return new Error('Connection is closed.')
}

/**
 * Commands Redis flags `denyoom` (refused when used_memory > maxmemory) among those redisjm uses.
 * Deletions (HDEL/SREM/DEL/LREM/ZREM/LPOP…), HPEXPIRE and reads are NOT in the list — Redis accepts
 * them under OOM. EVAL/EVALSHA of a flag-less `#!lua` script is refused under OOM too (see below).
 */
const DENYOOM = new Set(['set', 'sadd', 'hset', 'rpush', 'lpush', 'zadd'])

export interface MockRedisExtras {
  _dump: () => {
    store: Map<string, string>
    sets: Map<string, Set<string>>
    hashes: Map<string, Map<string, string>>
    lists: Map<string, string[]>
    zsets: Map<string, Map<string, number>>
    fieldTtls: Map<string, Map<string, number>>
    keyTtls: Map<string, number>
  }
  /** Simulates `maxmemory` + `noeviction`: while on, DENYOOM commands and shebang scripts reject with `oomError()`. */
  _setOom: (on: boolean) => void
  /** Sets what `INFO memory` reports. */
  _setInfo: (info: { used_memory?: number; maxmemory?: number; maxmemory_policy?: string }) => void
  /** Simulates a server without HPEXPIRE (Redis < 7.4). */
  _setHpexpireSupported: (on: boolean) => void
  /** Empties the script cache (like `SCRIPT FLUSH` / a restart): the next EVALSHA gets NOSCRIPT. */
  _flushScripts: () => void
}

/**
 * In-memory stand-in for the ioredis client covering the commands redisjm uses. Lua scripts are
 * EMULATED BY IDENTITY: the mock recognizes redisjm's scripts by their `-- redisjm:<name>` marker and
 * runs an equivalent implementation built from the mock's own (spy-able, failure-injectable) commands.
 * MULTI/EXEC and pipelines run their queued commands through the same methods.
 */
export function createMockRedis(): Redis & MockRedisExtras {
  const store = new Map<string, string>()
  // String-key expiries (epoch ms, compared against the possibly-faked Date.now()) for `SET … PX`.
  const expiries = new Map<string, number>()
  const sets = new Map<string, Set<string>>()
  const hashes = new Map<string, Map<string, string>>()
  const fieldTtls = new Map<string, Map<string, number>>()
  /** Key-level PEXPIRE deadlines recorded by the presence script (not enforced). */
  const keyTtls = new Map<string, number>()
  const lists = new Map<string, string[]>()
  // Sorted sets: key → Map<member, score>. Ordering is derived on read (zrangebyscore) by score asc.
  const zsets = new Map<string, Map<string, number>>()
  const scripts = new Map<string, string>()
  let oom = false
  let hpexpireSupported = true
  let info = { used_memory: 1_000_000, maxmemory: 0, maxmemory_policy: 'noeviction' }

  const checkOom = (cmd: string) => {
    if (oom && DENYOOM.has(cmd)) throw oomError()
  }
  const expireIfDue = (key: string) => {
    const at = expiries.get(key)
    if (at !== undefined && Date.now() >= at) {
      store.delete(key)
      expiries.delete(key)
    }
  }
  /** Applies due hash-field TTLs of `key` (HPEXPIRE emulation). */
  const purgeFields = (key: string) => {
    const ttls = fieldTtls.get(key)
    const hash = hashes.get(key)
    if (!ttls || !hash) return
    for (const [field, at] of ttls) {
      if (Date.now() >= at) {
        hash.delete(field)
        ttls.delete(field)
      }
    }
  }
  const clearFieldTtl = (key: string, field: string) => fieldTtls.get(key)?.delete(field)
  /**
   * Cursor-aware SCAN emulation with Redis' guarantee: an element present for the whole scan is
   * returned, even if others are deleted between calls. Items are visited in sorted key order; a
   * cursor id remembers the last key returned (cursor '0' = start / done).
   */
  const scanCursors = new Map<string, string>()
  let nextCursorId = 1
  const scanSlice = <T>(items: T[], cursor: string, args: any[], keyOf: (item: T) => string): [string, T[]] => {
    const countIdx = args.findIndex((a) => typeof a === 'string' && a.toUpperCase() === 'COUNT')
    const count = countIdx === -1 ? 10 : Number(args[countIdx + 1])
    const after = cursor === '0' ? undefined : scanCursors.get(cursor)
    const sorted = [...items].sort((a, b) => (keyOf(a) < keyOf(b) ? -1 : keyOf(a) > keyOf(b) ? 1 : 0))
    const remaining = after === undefined ? sorted : sorted.filter((item) => keyOf(item) > after)
    const slice = remaining.slice(0, count)
    if (slice.length === remaining.length) return ['0', slice]
    const id = String(nextCursorId++)
    scanCursors.set(id, keyOf(slice[slice.length - 1]))
    return [id, slice]
  }

  const m: any = {
    // String commands
    // Supports the `SET key value [PX ms] [NX]` shapes redisjm uses (PX/NX in any order).
    set: vi.fn(async (key: string, value: string, ...args: any[]) => {
      checkOom('set')
      expireIfDue(key)
      const hasNx = args.includes('NX')
      if (hasNx && store.has(key)) return null
      store.set(key, value)
      const pxIdx = args.findIndex((a) => typeof a === 'string' && a.toUpperCase() === 'PX')
      if (pxIdx !== -1) expiries.set(key, Date.now() + Number(args[pxIdx + 1]))
      else expiries.delete(key)
      return 'OK'
    }),
    get: vi.fn(async (key: string) => {
      expireIfDue(key)
      return store.get(key) ?? null
    }),
    exists: vi.fn(async (key: string) => {
      expireIfDue(key)
      return store.has(key) ? 1 : 0
    }),
    del: vi.fn(async (key: string) => {
      const existed = store.delete(key)
      expiries.delete(key)
      return existed ? 1 : 0
    }),

    // Set commands
    sadd: vi.fn(async (key: string, ...members: string[]) => {
      checkOom('sadd')
      if (!sets.has(key)) sets.set(key, new Set())
      const s = sets.get(key)!
      let added = 0
      for (const member of members) {
        if (!s.has(member)) {
          s.add(member)
          added++
        }
      }
      return added
    }),
    // Variadic like the real command: SREM key m1 [m2 …] → number actually removed.
    srem: vi.fn(async (key: string, ...members: string[]) => {
      const s = sets.get(key)
      if (!s) return 0
      let removed = 0
      for (const member of members) if (s.delete(member)) removed++
      return removed
    }),
    srandmember: vi.fn(async (key: string, count: number) => [...(sets.get(key) ?? [])].slice(0, Number(count))),
    sismember: vi.fn(async (key: string, member: string) => {
      const s = sets.get(key)
      return s?.has(member) ? 1 : 0
    }),
    smembers: vi.fn(async (key: string) => {
      const s = sets.get(key)
      return s ? [...s] : []
    }),
    scard: vi.fn(async (key: string) => {
      const s = sets.get(key)
      return s ? s.size : 0
    }),
    // ioredis SSCAN reply shape: [cursor, members[]], honoring the cursor and COUNT.
    sscan: vi.fn(async (key: string, cursor: string, ...args: any[]) => {
      return scanSlice([...(sets.get(key) ?? [])], cursor, args, (m) => m)
    }),

    // List commands
    rpush: vi.fn(async (key: string, value: string) => {
      checkOom('rpush')
      if (!lists.has(key)) lists.set(key, [])
      const list = lists.get(key)!
      list.push(value)
      return list.length
    }),
    lpush: vi.fn(async (key: string, value: string) => {
      checkOom('lpush')
      if (!lists.has(key)) lists.set(key, [])
      const list = lists.get(key)!
      list.unshift(value)
      return list.length
    }),
    lpop: vi.fn(async (key: string) => {
      const list = lists.get(key)
      if (!list || list.length === 0) return null
      return list.shift()!
    }),
    lrange: vi.fn(async (key: string, start: number, stop: number) => {
      const list = lists.get(key) ?? []
      return list.slice(Number(start), Number(stop) === -1 ? undefined : Number(stop) + 1)
    }),
    lrem: vi.fn(async (key: string, _count: number, value: string) => {
      const list = lists.get(key)
      if (!list) return 0
      const idx = list.indexOf(value)
      if (idx === -1) return 0
      list.splice(idx, 1)
      return 1
    }),
    lpos: vi.fn(async (key: string, value: string) => {
      const list = lists.get(key)
      if (!list) return null
      const idx = list.indexOf(value)
      return idx === -1 ? null : idx
    }),
    llen: vi.fn(async (key: string) => {
      const list = lists.get(key)
      return list ? list.length : 0
    }),

    // Hash commands (with HPEXPIRE field-TTL emulation: HSET of a field clears its TTL, like Redis).
    hget: vi.fn(async (key: string, field: string) => {
      purgeFields(key)
      return hashes.get(key)?.get(field) ?? null
    }),
    // HSET key field value [field value …] or HSET key { field: value, … } → number of NEW fields.
    hset: vi.fn(async (key: string, ...args: any[]) => {
      checkOom('hset')
      const pairs: Array<[string, string]> = typeof args[0] === 'object' && args[0] !== null
        ? Object.entries(args[0])
        : Array.from({ length: args.length / 2 }, (_, i) => [args[i * 2], args[i * 2 + 1]])
      if (!hashes.has(key)) hashes.set(key, new Map())
      const hash = hashes.get(key)!
      let added = 0
      for (const [field, value] of pairs) {
        if (!hash.has(field)) added++
        hash.set(field, String(value))
        clearFieldTtl(key, field)
      }
      return added
    }),
    hgetall: vi.fn(async (key: string) => {
      purgeFields(key)
      const hash = hashes.get(key)
      if (!hash) return {}
      return Object.fromEntries(hash)
    }),
    // Variadic like the real command: HDEL key f1 [f2 …] → number actually removed.
    hdel: vi.fn(async (key: string, ...fields: string[]) => {
      purgeFields(key)
      const hash = hashes.get(key)
      if (!hash) return 0
      let removed = 0
      for (const f of fields) {
        if (hash.delete(f)) removed++
        clearFieldTtl(key, f)
      }
      return removed
    }),
    hmget: vi.fn(async (key: string, ...fields: string[]) => {
      purgeFields(key)
      return fields.map((f) => hashes.get(key)?.get(f) ?? null)
    }),
    // Raw-bytes variants (ioredis `…Buffer`): the stored string as UTF-8 bytes.
    hgetBuffer: vi.fn(async (key: string, field: string) => {
      const value = await m.hget(key, field)
      return value === null ? null : Buffer.from(value)
    }),
    hmgetBuffer: vi.fn(async (key: string, ...fields: string[]) => {
      const values: Array<string | null> = await m.hmget(key, ...fields)
      return values.map((value) => (value === null ? null : Buffer.from(value)))
    }),
    hexists: vi.fn(async (key: string, field: string) => {
      purgeFields(key)
      return hashes.get(key)?.has(field) ? 1 : 0
    }),
    hlen: vi.fn(async (key: string) => {
      purgeFields(key)
      return hashes.get(key)?.size ?? 0
    }),
    // ioredis HSCAN reply shape: [cursor, flatArray] alternating field, value; honors cursor + COUNT.
    hscan: vi.fn(async (key: string, cursor: string, ...args: any[]) => {
      purgeFields(key)
      const [next, slice] = scanSlice([...(hashes.get(key) ?? new Map<string, string>())], cursor, args, ([f]) => f)
      return [next, slice.flat()]
    }),

    // Sorted-set commands. Scores are stored as numbers; zscore returns a string|null like ioredis.
    zadd: vi.fn(async (key: string, score: number, member: string) => {
      checkOom('zadd')
      if (!zsets.has(key)) zsets.set(key, new Map())
      const z = zsets.get(key)!
      const isNew = !z.has(member)
      z.set(member, Number(score))
      return isNew ? 1 : 0
    }),
    zrem: vi.fn(async (key: string, ...members: string[]) => {
      const z = zsets.get(key)
      if (!z) return 0
      let removed = 0
      for (const member of members) if (z.delete(member)) removed++
      return removed
    }),
    zscore: vi.fn(async (key: string, member: string) => {
      const score = zsets.get(key)?.get(member)
      return score === undefined ? null : String(score)
    }),
    zcard: vi.fn(async (key: string) => zsets.get(key)?.size ?? 0),
    // ioredis call shape: zrangebyscore(key, min, max, ['WITHSCORES'], ['LIMIT', offset, count]).
    zrangebyscore: vi.fn(async (key: string, min: any, max: any, ...args: any[]) => {
      const z = zsets.get(key)
      if (!z) return []
      const minScore = min === '-inf' ? Number.NEGATIVE_INFINITY : Number(min)
      const maxScore = max === '+inf' ? Number.POSITIVE_INFINITY : Number(max)
      let members = [...z.entries()]
        .filter(([, score]) => score >= minScore && score <= maxScore)
        .sort((a, b) => a[1] - b[1])
        .map(([member]) => member)
      const limitIdx = args.findIndex((a) => typeof a === 'string' && a.toUpperCase() === 'LIMIT')
      if (limitIdx !== -1) {
        const offset = Number(args[limitIdx + 1])
        const count = Number(args[limitIdx + 2])
        members = members.slice(offset, offset + count)
      }
      if (args.some((a) => typeof a === 'string' && a.toUpperCase() === 'WITHSCORES')) {
        return members.flatMap((member) => [member, String(z.get(member))])
      }
      return members
    }),

    // Server commands
    info: vi.fn(async (_section?: string) => [
      '# Memory',
      `used_memory:${info.used_memory}`,
      `maxmemory:${info.maxmemory}`,
      `maxmemory_policy:${info.maxmemory_policy}`,
      '',
    ].join('\r\n')),
    // Generic command entry point; only what redisjm sends through it (HPEXPIRE/HPTTL) is emulated.
    call: vi.fn(async (command: string, ...args: any[]) => {
      const cmd = command.toUpperCase()
      if (cmd === 'HPEXPIRE' || cmd === 'HPTTL') {
        if (!hpexpireSupported) throw new Error(`ERR unknown command '${command}', with args beginning with: `)
        const key = String(args[0])
        purgeFields(key)
        const fieldsIdx = args.findIndex((a) => String(a).toUpperCase() === 'FIELDS')
        const fields = args.slice(fieldsIdx + 2).map(String)
        if (cmd === 'HPTTL') {
          return fields.map((f) => {
            if (!hashes.get(key)?.has(f)) return -2
            const at = fieldTtls.get(key)?.get(f)
            return at === undefined ? -1 : Math.max(0, at - Date.now())
          })
        }
        const ms = Number(args[1])
        return fields.map((f) => {
          if (!hashes.get(key)?.has(f)) return -2
          if (!fieldTtls.has(key)) fieldTtls.set(key, new Map())
          fieldTtls.get(key)!.set(f, Date.now() + ms)
          return 1
        })
      }
      throw new Error(`ERR unknown command '${command}'`)
    }),

    // Scripting: EVALSHA needs the script cached (by a previous EVAL), else NOSCRIPT.
    eval: vi.fn(async (source: string, numKeys: number, ...rest: any[]) => {
      scripts.set(sha1Hex(source), source)
      return runScript(source, rest.slice(0, numKeys).map(String), rest.slice(numKeys).map(String))
    }),
    evalsha: vi.fn(async (sha: string, numKeys: number, ...rest: any[]) => {
      const source = scripts.get(sha)
      if (!source) throw new Error('NOSCRIPT No matching script. Please use EVAL.')
      return runScript(source, rest.slice(0, numKeys).map(String), rest.slice(numKeys).map(String))
    }),

    // MULTI/EXEC: queued commands; under OOM a queued DENYOOM command aborts the whole transaction
    // (EXECABORT with `previousErrors`, like ioredis); otherwise per-command errors are reported in
    // place and the other commands still apply (Redis runtime-error semantics).
    multi: vi.fn(() => queueChain(true)),
    pipeline: vi.fn(() => queueChain(false)),

    _dump: () => ({ store, sets, hashes, lists, zsets, fieldTtls, keyTtls }),
    _setOom: (on: boolean) => {
      oom = on
    },
    _setInfo: (next: Partial<typeof info>) => {
      info = { ...info, ...next }
    },
    _setHpexpireSupported: (on: boolean) => {
      hpexpireSupported = on
    },
    _flushScripts: () => scripts.clear(),
  }

  function queueChain(transaction: boolean) {
    const queued: Array<[string, any[]]> = []
    const chain: any = new Proxy({}, {
      get(_target, prop: string) {
        if (prop === 'length') return queued.length
        if (prop === 'exec') {
          return async () => {
            if (transaction && oom && queued.some(([name]) => DENYOOM.has(name))) {
              const err = new Error('EXECABORT Transaction discarded because of previous errors.') as Error & { previousErrors: Error[] }
              err.name = 'ReplyError'
              err.previousErrors = queued.filter(([name]) => DENYOOM.has(name)).map(() => oomError())
              throw err
            }
            const results: Array<[Error | null, unknown]> = []
            for (const [name, args] of queued) {
              try {
                results.push([null, await m[name](...args)])
              } catch (err) {
                results.push([err as Error, null])
              }
            }
            return results
          }
        }
        return (...args: any[]) => {
          queued.push([prop, args])
          return chain
        }
      },
    })
    return chain
  }

  /** Emulates redisjm's Lua scripts (identified by marker) with the mock's own commands. */
  async function runScript(source: string, keys: string[], argv: string[]): Promise<unknown> {
    // A flag-less `#!lua` script is refused up front under OOM — nothing in it runs. `allow-oom` (deletion
    // only) and `no-writes` (read-only) scripts are not.
    if (oom && source.startsWith('#!lua') && !/flags=[^\n]*(allow-oom|no-writes)/.test(source)) throw oomError()
    if (source.includes('-- redisjm:enqueue')) {
      const [locks, log, delayed, lane, jobLocks, jobLanes, registry] = keys
      const [jobName, mode, capArg, maxArg] = argv
      const cap = Number(capArg)
      const maxInFlight = Number(maxArg)
      const n = (argv.length - 4) / 3
      const out: string[] = []
      const accepted: number[] = []
      const acceptedIds = new Set<string>()
      let length = cap >= 0 ? await m.llen(lane) : 0
      let inFlight = maxInFlight >= 0 ? await m.scard(jobLocks) : 0
      for (let i = 0; i < n; i++) {
        const id = argv[4 + i * 3]
        if (acceptedIds.has(id) || (await m.sismember(locks, id)) === 1) out.push('deduped')
        else if (maxInFlight >= 0 && inFlight >= maxInFlight) out.push('busy')
        else if (cap >= 0 && length >= cap) out.push('full')
        else {
          out.push('queued')
          acceptedIds.add(id)
          accepted.push(i)
          inFlight++
          if (mode !== 'D') length++
        }
      }
      if (mode === 'L') accepted.reverse()
      for (const i of accepted) {
        const [id, json, score] = argv.slice(4 + i * 3, 7 + i * 3)
        await m.sadd(locks, id)
        await m.sadd(jobLocks, id)
        await m.hset(log, id, json)
        if (mode === 'D') await m.zadd(delayed, Number(score), id)
        else if (mode === 'L') await m.lpush(lane, id)
        else await m.rpush(lane, id)
      }
      if (accepted.length) {
        await m.sadd(jobLanes, lane)
        await m.sadd(registry, jobName)
      }
      return out
    }
    if (source.includes('-- redisjm:pop')) {
      const [claiming, ...lanes] = keys
      const scanLimit = Number(argv[1])
      for (let i = 0; i < lanes.length; i++) {
        const lane = lanes[i]
        const spec = argv[2 + i]
        let id: string | null = null
        if (spec === '*') {
          id = await m.lpop(lane)
        } else {
          for (const candidate of await m.lrange(lane, 0, scanLimit - 1)) {
            const hash = candidate.indexOf('#')
            if (hash !== -1 && spec.includes(`#${candidate.slice(0, hash)}#`)) {
              await m.lrem(lane, 1, candidate)
              id = candidate
              break
            }
          }
        }
        if (id) {
          await m.zadd(claiming, Number(argv[0]), id)
          return [lane, id]
        }
      }
      return null
    }
    if (source.includes('-- redisjm:transition')) {
      const [logKey, claimingKey, locksKey, delayedKey, targetKey, jobLocks, jobLanes] = keys
      const [id, expectedSha, next, side, claiming, delayed, delayedScore, lock, fieldTtl] = argv
      const cur = await m.hget(logKey, id)
      if (cur === null) return -1
      if (sha1Hex(cur) !== expectedSha) return 0
      if (delayed === 'take' && (await m.zrem(delayedKey, id)) === 0) return -2
      if (claiming === 'take' && (await m.zrem(claimingKey, id)) === 0) return -2
      if (lock === 'drop') {
        await m.hdel(logKey, id)
      } else {
        await m.hset(logKey, id, next)
        if (lock === 'retire' && Number(fieldTtl) > 0) {
          // `redis.pcall`: an unsupported HPEXPIRE (Redis < 7.4) is ignored, not a script error.
          try {
            await m.call('HPEXPIRE', logKey, Number(fieldTtl), 'FIELDS', 1, id)
          } catch {}
        }
      }
      if (side === 'L' || side === 'R') {
        if (side === 'L') await m.lpush(targetKey, id)
        else await m.rpush(targetKey, id)
        await m.sadd(jobLanes, targetKey)
      }
      if (claiming === 'remove') await m.zrem(claimingKey, id)
      if (lock === 'retire' || lock === 'drop') {
        await m.srem(locksKey, id)
        await m.srem(jobLocks, id)
      } else if (lock === 'take') {
        await m.sadd(locksKey, id)
        await m.sadd(jobLocks, id)
      }
      if (delayed === 'add') await m.zadd(delayedKey, Number(delayedScore), id)
      return 1
    }
    if (source.includes('-- redisjm:purge')) {
      const [logKey, locksKey, jobLocks, claimingKey, delayedKey, suspectsKey, laneKey] = keys
      const [id, mode, flags, witness] = argv
      if (mode === 'orphan') {
        const cur = await m.hget(logKey, id)
        if (cur !== null && sha1Hex(cur) !== witness) return 0
      }
      await m.hdel(logKey, id)
      await m.srem(locksKey, id)
      await m.srem(jobLocks, id)
      if (flags.includes('q')) await m.lrem(laneKey, 1, id)
      if (flags.includes('c')) await m.zrem(claimingKey, id)
      if (flags.includes('d')) await m.zrem(delayedKey, id)
      if (flags.includes('s')) await m.hdel(suspectsKey, id)
      return 1
    }
    if (source.includes('-- redisjm:delete-if-unchanged')) {
      const [key] = keys
      const zset = argv[0] === 'z'
      const deleted: string[] = []
      for (let i = 1; i < argv.length; i += 2) {
        const member = argv[i]
        let doomed = false
        if (zset) {
          const score = await m.zscore(key, member)
          doomed = score !== null && Number(score) === Number(argv[i + 1])
        } else {
          const cur = await m.hget(key, member)
          doomed = cur !== null && sha1Hex(cur) === argv[i + 1]
        }
        if (doomed) {
          if (zset) await m.zrem(key, member)
          else await m.hdel(key, member)
          deleted.push(member)
        }
      }
      return deleted
    }
    if (source.includes('-- redisjm:prune-job')) {
      const [locks, jobLocks, jobLanes, registry] = keys
      const [jobName, sample] = argv
      let pruned = 0
      for (const id of await m.srandmember(jobLocks, Number(sample))) {
        if ((await m.sismember(locks, id)) === 0) {
          await m.srem(jobLocks, id)
          pruned++
        }
      }
      for (const key of await m.smembers(jobLanes)) {
        if ((await m.llen(key)) === 0) {
          await m.srem(jobLanes, key)
          pruned++
        }
      }
      if ((await m.scard(jobLocks)) === 0 && (await m.scard(jobLanes)) === 0) await m.srem(registry, jobName)
      return pruned
    }
    if (source.includes('-- redisjm:presence')) {
      const [instances, info] = keys
      const [id, ttl, json, keyTtl] = argv
      // Server time = the (possibly faked) client clock in the mock.
      const now = Date.now()
      for (const dead of await m.zrangebyscore(instances, '-inf', now, 'LIMIT', 0, 100)) {
        await m.hdel(info, dead)
        await m.zrem(instances, dead)
      }
      if (json !== '') await m.hset(info, id, json)
      else if ((await m.hexists(info, id)) === 0) return 1
      await m.zadd(instances, now + Number(ttl), id)
      // PEXPIRE … GT: the deadline only ever moves later.
      for (const k of [instances, info]) keyTtls.set(k, Math.max(keyTtls.get(k) ?? 0, now + Number(keyTtl)))
      return 0
    }
    if (source.includes('-- redisjm:fleet')) {
      const [instances, info] = keys
      const now = Date.now()
      const out: unknown[] = [now]
      const live = [...(zsets.get(instances) ?? [])].filter(([, score]) => score > now).sort((x, y) => x[1] - y[1])
      for (const [id, score] of live) out.push(id, String(score), await m.hget(info, id))
      return out
    }
    if (source.includes('-- redisjm:list-queued')) {
      // A simplified model of LIST_QUEUED_SCRIPT (the decode / scan caps and the chunked reads are
      // covered by the integration tests): the ordered sequence, the filters, then the page slice.
      const [claimingKey, delayedKey, logKey, ...laneKeys] = keys
      const [filterLane, prefix] = argv
      const offset = Number(argv[2])
      const limit = Number(argv[3])
      const ranked = (key: string) => [...(zsets.get(key) ?? [])]
        .sort((x, y) => x[1] - y[1] || (x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0))
        .map(([id, score]) => [id, String(score)] as const)
      // A run's lane label from its record ('' = no record / unreadable).
      const recordLane = async (id: string): Promise<string> => {
        const raw = await m.hget(logKey, id)
        let rec: any
        try {
          rec = raw === null ? null : JSON.parse(raw)
        } catch {
          return ''
        }
        if (typeof rec !== 'object' || rec === null) return ''
        return typeof rec.lane !== 'string' || rec.lane === '' || rec.lane === 'default' ? 'default' : rec.lane
      }
      const sequence: Array<[kind: string, id: string, score: string, label: string | null]> = [
        ...ranked(claimingKey).map(([id, score]) => ['c', id, score, null] as [string, string, string, null]),
        ...laneKeys.flatMap((key, i) => (lists.get(key) ?? []).map((id) => ['l', id, '', argv[6 + i]] as [string, string, string, string])),
        ...ranked(delayedKey).map(([id, score]) => ['d', id, score, null] as [string, string, string, null]),
      ]
      const matches: string[][] = []
      for (const [kind, id, score, label] of sequence) {
        if (prefix !== '' && !id.startsWith(prefix)) continue
        const lane = label ?? (await recordLane(id))
        if (filterLane !== '' && lane !== filterLane) continue
        matches.push([kind, id, score, lane])
      }
      const page = matches.slice(offset, offset + limit)
      return [1, matches.length > offset + limit ? 1 : 0, ...page.flat()]
    }
    throw new Error('mock-redis: unknown script')
  }

  return m as Redis & MockRedisExtras
}
