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
    lrem: vi.fn(async (key: string, _count: number, value: string) => {
      const list = lists.get(key)
      if (!list) return 0
      const idx = list.indexOf(value)
      if (idx === -1) return 0
      list.splice(idx, 1)
      return 1
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

    _dump: () => ({ store, sets, hashes, lists }),
  } as unknown as Redis & { _dump: () => any }
}
