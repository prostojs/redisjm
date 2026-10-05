/**
 * Shared harness of the opt-in real-Redis suites (`integration*.test.ts`).
 *
 * - `useSharedRedis(prefix)` — the shared server at `REDIS_URL`. It is shared with other apps: only the
 *   suite's own `redisjm:<prefix>-*` keys are ever touched (SCAN + UNLINK — never KEYS, FLUSHALL /
 *   FLUSHDB / CONFIG SET / SCRIPT FLUSH).
 * - `useDedicatedRedis(prefix)` — a throwaway `redis-server` spawned for the suite (`--maxmemory 2mb
 *   --maxmemory-policy noeviction`, no persistence) and killed in `afterAll`, for what must not run on
 *   the shared server (out-of-memory scenarios, script-cache flushes, eviction-policy changes).
 *
 * Suites are skipped when `REDIS_URL` is unset (`describe.skipIf(!REDIS_URL)`); dedicated blocks also
 * when no `redis-server` binary is found (`!REDIS_SERVER_BIN`). Every manager created through a
 * harness's `newManager` is stopped after each test, so no timer leaks.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync } from 'node:fs'
import { createServer } from 'node:net'
import Redis from 'ioredis'
import { afterAll, afterEach, beforeAll } from 'vitest'
import { RedisJM } from '../redisjm'
import type { JobMetadata, RedisJMOptions } from '../types'

export const REDIS_URL = process.env.REDIS_URL

/** A local `redis-server` binary for the dedicated-server suites, if any. */
export const REDIS_SERVER_BIN = ['/opt/homebrew/bin/redis-server', '/usr/local/bin/redis-server', '/usr/bin/redis-server']
  .find((p) => existsSync(p))

export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `fn` every `intervalMs` until it resolves truthy, or throws once `timeoutMs` elapses. Used for
 * every "eventually" assertion so the suites tolerate real-timer jitter without brittle exact sleeps.
 */
export async function until(fn: () => boolean | Promise<boolean>, timeoutMs = 3000, intervalMs = 20): Promise<void> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    if (await fn()) return
    if (Date.now() > deadline) throw new Error(`until(): condition not met within ${timeoutMs}ms`)
    await sleep(intervalMs)
  }
}

let groupCounter = 0
/** A fresh, unique target group (`<prefix>-<ts>-<n>`, no ':' — the constructor rejects that). */
export const newGroup = (prefix: string): string => `${prefix}-${Date.now()}-${++groupCounter}`

/** Deletes keys matching `pattern` via SCAN + UNLINK (never KEYS on big keyspaces, never FLUSH). */
export async function cleanup(redis: Redis, pattern: string): Promise<void> {
  let cursor = '0'
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000)
    if (keys.length) await redis.unlink(...keys)
    cursor = next
  } while (cursor !== '0')
}

/** Asks the OS for a free TCP port. */
export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer()
    srv.once('error', reject)
    srv.listen(0, '127.0.0.1', () => {
      const address = srv.address()
      const port = typeof address === 'object' && address ? address.port : 0
      srv.close(() => resolve(port))
    })
  })
}

/** What a harness hands its suite. `redis` is connected in `beforeAll` — read it inside hooks/tests. */
export interface RedisHarness {
  readonly redis: Redis
  /** A unique target group with the suite's prefix. */
  newGroup: () => string
  /** A manager (suite defaults < `options`) on `group`, stopped after the test. */
  newManager: (options: RedisJMOptions, group?: string, client?: Redis) => RedisJM
}

/**
 * Tracks the managers a test creates; `stopAll` stops them (call it first in the harness' `afterEach`,
 * before any key cleanup). Some tests leave a deliberately hung handler in flight, so the drain is
 * aborted and bounded instead of awaited to the end.
 */
function trackManagers(prefix: string, defaults: RedisJMOptions, client: () => Redis): Omit<RedisHarness, 'redis'> & {
  stopAll: () => Promise<void>
} {
  const managers: RedisJM[] = []
  return {
    stopAll: async () => {
      await Promise.all(managers.splice(0).map((m) => Promise.race([m.stop({ abort: true }), sleep(300)])))
    },
    newGroup: () => newGroup(prefix),
    newManager: (options, group = newGroup(prefix), redis = client()) => {
      const manager = new RedisJM(redis, group, { ...defaults, ...options })
      managers.push(manager)
      return manager
    },
  }
}

/**
 * Harness for a suite on the SHARED server at `REDIS_URL`: one connection for the suite, extra
 * connections via `newClient()` (quit after each test), and after each test every `redisjm:<prefix>-*`
 * key of the suite is deleted.
 */
export function useSharedRedis(prefix: string, defaults: RedisJMOptions = {}): RedisHarness & { newClient: () => Redis } {
  let redis: Redis | undefined
  const clients: Redis[] = []
  const { stopAll, ...managers } = trackManagers(prefix, defaults, () => redis!)
  beforeAll(async () => {
    redis = new Redis(REDIS_URL!, { maxRetriesPerRequest: null })
    // Wait for a live connection before any test runs.
    await redis.ping()
  })
  afterAll(async () => {
    await redis?.quit()
  })
  afterEach(async () => {
    await stopAll()
    await Promise.all(clients.splice(0).map((c) => c.quit().catch(() => {})))
    await cleanup(redis!, `redisjm:${prefix}-*`)
  })
  return {
    ...managers,
    get redis() {
      return redis!
    },
    newClient: () => {
      const client = new Redis(REDIS_URL!, { maxRetriesPerRequest: null })
      clients.push(client)
      return client
    },
  }
}

/**
 * Harness for a suite on a DEDICATED throwaway `redis-server` (2mb maxmemory, `noeviction`, no
 * persistence) spawned in `beforeAll` and killed in `afterAll`. Managers default to `logger: false`.
 * After each test the filler keys of `fillToOom()` and the suite's `redisjm:<prefix>-*` keys are
 * deleted, so every test starts with free memory.
 */
export function useDedicatedRedis(prefix: string): RedisHarness & {
  /** An extra connection to the dedicated server (quit after the test). */
  newClient: () => Redis
  /** Fills Redis until even a tiny write is refused with OOM. */
  fillToOom: () => Promise<void>
  /** Deletes the filler keys (deletions are accepted under OOM). */
  freeMemory: () => Promise<void>
} {
  let server: ChildProcess | undefined
  let redis: Redis | undefined
  let port = 0
  let fillerCount = 0
  const clients: Redis[] = []
  const { stopAll, ...managers } = trackManagers(prefix, { logger: false }, () => redis!)
  const freeMemory = () => cleanup(redis!, 'filler:*')
  beforeAll(async () => {
    port = await freePort()
    server = spawn(REDIS_SERVER_BIN!, [
      '--port', String(port), '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no',
      '--maxmemory', '2mb', '--maxmemory-policy', 'noeviction',
    ], { stdio: 'ignore' })
    redis = new Redis({ port, host: '127.0.0.1', maxRetriesPerRequest: 1, retryStrategy: (n) => (n > 20 ? null : 50) })
    // The server may still be starting: connection-refused retries are expected, don't log them as unhandled.
    redis.on('error', () => {})
    await until(async () => (await redis!.ping().catch(() => '')) === 'PONG', 5000, 50)
  })
  afterAll(async () => {
    await redis?.quit().catch(() => {})
    server?.kill('SIGKILL')
  })
  afterEach(async () => {
    await stopAll()
    await Promise.all(clients.splice(0).map((c) => c.quit().catch(() => {})))
    await freeMemory()
    await cleanup(redis!, `redisjm:${prefix}-*`)
  })
  return {
    ...managers,
    get redis() {
      return redis!
    },
    freeMemory,
    newClient: () => {
      const client = new Redis({ port, host: '127.0.0.1', maxRetriesPerRequest: 1 })
      client.on('error', () => {})
      clients.push(client)
      return client
    },
    /**
     * Big chunks first, then progressively smaller ones: a refused 64KB SET is refused partly because
     * of its own query buffer (which counts toward used_memory and is freed right after), so stopping at
     * the first big refusal would leave room for the small writes the tests expect to be refused.
     */
    fillToOom: async () => {
      for (const size of [64 * 1024, 4096, 256, 16]) {
        const chunk = 'x'.repeat(size)
        for (let i = 0; ; i++) {
          if (i > 5000) throw new Error('fillToOom: Redis never reported OOM')
          try {
            await redis!.set(`filler:${fillerCount++}`, chunk)
          } catch (err) {
            if (/^OOM/.test((err as Error).message)) break
            throw err
          }
        }
      }
    },
  }
}

/** A handler that never settles. */
export const hung = (): Promise<void> => new Promise<void>(() => {})

/** Pops the head of the default lane of `group` the way a consumer does, leaving it in `claiming`. */
export const popOnly = (m: RedisJM, group: string): Promise<{ key: string; jobId: string } | null> =>
  (m as any).popFromLanes([{ key: `redisjm:${group}:queue`, spec: '*' }])

/** Registers job `hang` (a handler that never settles), queues run `r1`, starts the loop and waits for `running`. */
export async function startHung(manager: RedisJM, metadata: Partial<JobMetadata> = {}): Promise<void> {
  const job = manager.createJob({ jobName: 'hang', ...metadata }, hung)
  await job.queue('r1', null)
  manager.start(10)
  await until(async () => (await manager.get('hang#r1'))?.status === 'running')
}
