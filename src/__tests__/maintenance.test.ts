import { describe, it, expect, vi } from 'vitest'
import { RedisJM } from '../redisjm'
import { Job } from '../job'
import { createMaintenanceJob } from '../maintenance'
import { createMockRedis } from './mock-redis'

describe('createMaintenanceJob', () => {
  it('should create a job with name __redisjm_maintenance', () => {
    const redis = createMockRedis()
    const manager = new RedisJM(redis, 'test-group', {
      heartbeatInterval: 1000,
      roundsToStale: 2,
      keepFinishedInterval: 1000,
    })
    const job = createMaintenanceJob(manager)
    expect(job).toBeInstanceOf(Job)
    expect(job.getName()).toBe('__redisjm_maintenance')
  })

  it('should call performMaintenance when executed', async () => {
    const redis = createMockRedis()
    const manager = new RedisJM(redis, 'test-group', {
      heartbeatInterval: 1000,
      roundsToStale: 2,
      keepFinishedInterval: 1000,
    })

    // Add a stale job to verify maintenance runs
    const jobId = 'staleJob#run1'
    await redis.sadd('redisjm:test-group:locks', jobId)
    await redis.hset('redisjm:test-group:log', jobId, JSON.stringify({
      jobId, jobName: 'staleJob', runId: 'run1', inputs: null,
      targetGroup: 'test-group', status: 'running', progress: 0,
      startedAt: Date.now() - 10000, heartbeat: Date.now() - 5000,
    }))

    const maintenanceJob = createMaintenanceJob(manager)
    await maintenanceJob.execute(null, { targetGroup: 'test-group' })

    // Verify stale job was handled
    const logJson = await redis.hget('redisjm:test-group:log', jobId)
    const record = JSON.parse(logJson!)
    expect(record.status).toBe('stale')
  })

  it('should work when registered and queued through the manager', async () => {
    const redis = createMockRedis()
    const manager = new RedisJM(redis, 'test-group', {
      heartbeatInterval: 1000,
      roundsToStale: 2,
      keepFinishedInterval: 1000,
    })

    const maintenanceJob = createMaintenanceJob(manager)

    // Queue and execute via manager (already registered by createMaintenanceJob)
    const queued = await maintenanceJob.queue('maint-1', null)
    expect(queued).toBe(true)

    const executed = await manager.popAndExecute()
    expect(executed).toBe(true)
  })
})
