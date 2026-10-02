import { Job } from './job'
import type { RedisJM } from './redisjm'

/** Reserved name of the built-in maintenance job. */
export const MAINTENANCE_JOB_NAME = '__redisjm_maintenance'

/** Reserved lane the built-in maintenance job runs on; every instance implicitly polls it first. */
export const MAINTENANCE_LANE = '__maintenance'

/**
 * Creates and registers the built-in maintenance job, whose handler runs one lock-guarded
 * maintenance pass (`manager.runMaintenance()`).
 *
 * You normally don't need it: `manager.start()` runs maintenance on the manager's own timer (see the
 * `maintenanceInterval` option), outside the job queue and the concurrency slots, and registers this
 * job only so maintenance entries enqueued by OLDER instances during a rolling deploy are still
 * consumed from the reserved `__maintenance` lane. For manual scheduling without `start()`, prefer
 * calling `manager.runMaintenance()` from your own timer over enqueueing this job — a queued pass
 * waits behind (and occupies) a concurrency slot, and a popped entry of this job is taken as a sign that
 * a pre-0.2 instance is still running, which switches on maintenance's (costlier) per-record orphan
 * check for queued runs for a while.
 *
 * @param manager - The RedisJM instance to perform maintenance on (also used for registration)
 * @returns A registered Job that calls `manager.runMaintenance()` when executed
 *
 * @example
 * ```ts
 * // Preferred: no job at all
 * setInterval(() => manager.runMaintenance(), 30000)
 * ```
 */
export function createMaintenanceJob(manager: RedisJM): Job<null, never> {
  const job = new Job<null, never>(
    {
      jobName: MAINTENANCE_JOB_NAME,
      description: 'Scans for stale jobs and cleans up expired log records',
      // Reserved lane every instance implicitly subscribes to, so even a pure worker (no default-lane
      // subscription) still consumes maintenance entries enqueued by older instances.
      lane: MAINTENANCE_LANE,
    },
    async (): Promise<void> => {
      // Lock-guarded like the timer's pass, so a queued legacy entry never runs a second concurrent
      // full pass alongside the timer-driven ones.
      await manager.runMaintenance()
    },
    manager,
  )
  manager.registerJob(job)
  return job
}
