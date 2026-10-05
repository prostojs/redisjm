/**
 * Shared fixtures of the mock-Redis unit suites that run on target group `'g'`: key names, and helpers
 * bound to the suite's current mock (`mockHelpers(() => redis)` — the mock is recreated per test).
 */
import { expect, vi } from 'vitest'
import type { Job } from '../job'
import { RedisJM } from '../redisjm'
import type { JobLogRecord, JobMetadata, RedisJMOptions } from '../types'
import type { createMockRedis } from './mock-redis'

type MockRedis = ReturnType<typeof createMockRedis>

export const LOG = 'redisjm:g:log'
export const LOCKS = 'redisjm:g:locks'
export const QUEUE = 'redisjm:g:queue'
export const CLAIMING = 'redisjm:g:claiming'
export const laneKey = (lane: string): string => `redisjm:g:lane:${lane}:queue`
export const jobLocks = (job: string): string => `redisjm:g:jobs:${job}:locks`
export const jobLanes = (job: string): string => `redisjm:g:jobs:${job}:lanes`

/** A handler that never settles. */
export const hung = (): Promise<void> => new Promise<void>(() => {})

/** Pops the head of the default lane the way a consumer does, leaving it in `claiming` (not claimed). */
export const popOnly = (m: RedisJM, key = QUEUE): Promise<{ key: string; jobId: string } | null> =>
  (m as any).popFromLanes([{ key, spec: '*' }])

export function mockHelpers(redis: () => MockRedis) {
  return {
    /** Members of a set, sorted. */
    setOf: (key: string): string[] => [...(redis()._dump().sets.get(key) ?? [])].sort(),
    /** Members of a sorted set, by score. */
    zsetOf: (key: string): string[] => [...(redis()._dump().zsets.get(key) ?? new Map<string, number>())]
      .sort((a, b) => a[1] - b[1]).map(([member]) => member),
    /** Elements of a list, head first. */
    listOf: (key: string): string[] => redis()._dump().lists.get(key) ?? [],
    /**
     * Registers a job `h#r1` (a handler that never settles, `metadata` merged over `{ jobName: 'h' }`), queues
     * a run and starts the poll loop; resolves once the run is `running`.
     */
    startHung: async (m: RedisJM, metadata: Partial<JobMetadata> = {}): Promise<Job<null>> => {
      const job = m.createJob<null>({ jobName: 'h', ...metadata }, hung)
      await m.queue(job, 'r1', null)
      m.start(5)
      await vi.waitFor(async () => {
        const json = await redis().hget(LOG, 'h#r1')
        expect(json ? (JSON.parse(json) as JobLogRecord).status : undefined).toBe('running')
      })
      return job
    },
    /** The stored record of `jobId`, parsed (`null` when absent). */
    readRecord: async (jobId: string): Promise<JobLogRecord | null> => {
      const json = await redis().hget(LOG, jobId)
      return json ? (JSON.parse(json) as JobLogRecord) : null
    },
    /**
     * Writes a record straight into the log — `jobName`/`runId` derived from `jobId`, the other required
     * fields defaulted — and takes its lock unless `locked` is `false`.
     */
    seed: async (record: Partial<JobLogRecord> & { jobId: string }, locked = true): Promise<void> => {
      const [jobName, runId] = record.jobId.split('#')
      await redis().hset(LOG, record.jobId, JSON.stringify({
        jobName, runId, inputs: null, targetGroup: 'g', progress: 0, ...record,
      }))
      if (locked) await redis().sadd(LOCKS, record.jobId)
    },
    /** A manager on group `'g'`: 60s history, no auto-maintenance, silent — unless overridden. */
    newManager: (options: RedisJMOptions = {}): RedisJM =>
      new RedisJM(redis(), 'g', { keepFinishedInterval: 60000, maintenanceInterval: 0, logger: false, ...options }),
  }
}
