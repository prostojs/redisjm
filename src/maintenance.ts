import { Job } from './job'
import type { RedisJM } from './redisjm'
import type { MaintenanceResult } from './types'

/** Reserved name of the built-in maintenance job. */
export const MAINTENANCE_JOB_NAME = '__redisjm_maintenance'

/**
 * Creates and registers a maintenance job that detects stale jobs and cleans up expired log records.
 * Maintenance is idempotent — use an empty `runId` (`''`) to ensure at most one is queued at a time.
 *
 * Note: `manager.start()` wires this automatically (see the `maintenanceInterval` option).
 * Manual wiring is only needed when auto-maintenance is disabled (`maintenanceInterval: 0`)
 * or when maintenance should run outside the polling loop.
 *
 * @param manager - The RedisJM instance to perform maintenance on (also used for registration)
 * @returns A registered Job that calls `manager.performMaintenance()` when executed
 *
 * @example
 * ```ts
 * const maintenanceJob = createMaintenanceJob(manager)
 * // Periodically try to queue (all instances, only one succeeds)
 * setInterval(() => maintenanceJob.queue('', null), 30000)
 * ```
 */
export function createMaintenanceJob(manager: RedisJM): Job<null, never> {
  const job = new Job<null, never>(
    { jobName: MAINTENANCE_JOB_NAME, description: 'Scans for stale jobs and cleans up expired log records' },
    async (): Promise<void> => {
      await manager.performMaintenance()
    },
    manager,
  )
  manager.registerJob(job)
  return job
}
