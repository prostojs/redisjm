/**
 * Shared fixtures of the mock-Redis unit suites that run on target group `'g'`: key names, and helpers
 * bound to the suite's current mock (`mockHelpers(() => redis)` — the mock is recreated per test).
 */
import { RedisJM } from '../redisjm'
import type { JobLogRecord, RedisJMOptions } from '../types'
import type { createMockRedis } from './mock-redis'

type MockRedis = ReturnType<typeof createMockRedis>

export const LOG = 'redisjm:g:log'
export const LOCKS = 'redisjm:g:locks'
export const QUEUE = 'redisjm:g:queue'
export const CLAIMING = 'redisjm:g:claiming'
export const laneKey = (lane: string): string => `redisjm:g:lane:${lane}:queue`
export const jobLocks = (job: string): string => `redisjm:g:jobs:${job}:locks`
export const jobLanes = (job: string): string => `redisjm:g:jobs:${job}:lanes`

export function mockHelpers(redis: () => MockRedis) {
  return {
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
