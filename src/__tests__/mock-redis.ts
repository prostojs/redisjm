import { vi } from 'vitest'
import type Redis from 'ioredis'

export function createMockRedis(): Redis & { _dump: () => { store: Map<string, string>; sets: Map<string, Set<string>>; hashes: Map<string, Map<string, string>>; lists: Map<string, string[]> } } {
  const store = new Map<string, string>()
  const sets = new Map<string, Set<string>>()
  const hashes = new Map<string, Map<string, string>>()
  const lists = new Map<string, string[]>()

  return {
    // String commands
    set: vi.fn(async (key: string, value: string, ...args: any[]) => {
      const hasNx = args.includes('NX')
      if (hasNx && store.has(key)) return null
      store.set(key, value)
      return 'OK'
    }),
    get: vi.fn(async (key: string) => {
      return store.get(key) ?? null
    }),
    del: vi.fn(async (key: string) => {
      store.delete(key)
      return 1
    }),

    // Set commands
    sadd: vi.fn(async (key: string, member: string) => {
      if (!sets.has(key)) sets.set(key, new Set())
      const s = sets.get(key)!
      if (s.has(member)) return 0
      s.add(member)
      return 1
    }),
    srem: vi.fn(async (key: string, member: string) => {
      const s = sets.get(key)
      if (!s || !s.has(member)) return 0
      s.delete(member)
      return 1
    }),
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
    // ioredis SSCAN reply shape: [cursor, members[]]. Emit everything in one iteration (cursor '0');
    // the cursor-loop in scanSet still exercises its loop-exit path.
    sscan: vi.fn(async (key: string, ..._args: any[]) => {
      const s = sets.get(key)
      return ['0', s ? [...s] : []]
    }),

    // List commands
    rpush: vi.fn(async (key: string, value: string) => {
      if (!lists.has(key)) lists.set(key, [])
      const list = lists.get(key)!
      list.push(value)
      return list.length
    }),
    lpush: vi.fn(async (key: string, value: string) => {
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
    lmpop: vi.fn(async (...args: any[]) => {
      // ioredis call shape: lmpop(numkeys, key1, key2, ..., direction[, 'COUNT', count]).
      // Scan the given keys IN ORDER, pop from the first non-empty list, and return
      // [poppedKey, [element]] (COUNT is unused by redisjm); null when every key is empty.
      const numkeys = args[0] as number
      const keys = args.slice(1, 1 + numkeys) as string[]
      const direction = args[1 + numkeys] as string
      for (const key of keys) {
        const list = lists.get(key)
        if (list && list.length > 0) {
          const element = direction === 'RIGHT' ? list.pop()! : list.shift()!
          return [key, [element]]
        }
      }
      return null
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

    // Hash commands
    hget: vi.fn(async (key: string, field: string) => {
      const hash = hashes.get(key)
      return hash?.get(field) ?? null
    }),
    hset: vi.fn(async (key: string, field: string, value: string) => {
      if (!hashes.has(key)) hashes.set(key, new Map())
      hashes.get(key)!.set(field, value)
      return 1
    }),
    hgetall: vi.fn(async (key: string) => {
      const hash = hashes.get(key)
      if (!hash) return {}
      return Object.fromEntries(hash)
    }),
    hdel: vi.fn(async (key: string, field: string) => {
      const hash = hashes.get(key)
      if (!hash) return 0
      const existed = hash.has(field)
      hash.delete(field)
      return existed ? 1 : 0
    }),
    hexists: vi.fn(async (key: string, field: string) => {
      const hash = hashes.get(key)
      return hash?.has(field) ? 1 : 0
    }),
    // ioredis HSCAN reply shape: [cursor, flatArray] where flatArray alternates field, value.
    // Emit everything in one iteration (cursor '0'); scanHash's cursor loop still exercises its exit.
    hscan: vi.fn(async (key: string, ..._args: any[]) => {
      const hash = hashes.get(key)
      const flat: string[] = []
      if (hash) {
        for (const [f, v] of hash) flat.push(f, v)
      }
      return ['0', flat]
    }),

    _dump: () => ({ store, sets, hashes, lists }),
  } as unknown as Redis & { _dump: () => any }
}
