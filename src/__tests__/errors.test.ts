import { describe, it, expect } from 'vitest'
import { classifyRedisError, JobTimeoutError, RedisJMEnqueueError } from '../errors'
import { connectionError, oomError } from './mock-redis'

describe('classifyRedisError', () => {
  it('classifies OOM reply errors (also when quoted inside another reply)', () => {
    expect(classifyRedisError(oomError())).toBe('oom')
    expect(classifyRedisError(new Error("EXECABORT Transaction discarded because of: OOM command not allowed when used memory > 'maxmemory'."))).toBe('oom')
  })

  it('classifies READONLY replica errors', () => {
    expect(classifyRedisError(new Error("READONLY You can't write against a read only replica."))).toBe('readonly')
  })

  it('classifies connection failures by message and by socket code', () => {
    expect(classifyRedisError(connectionError())).toBe('connection')
    expect(classifyRedisError(new Error("Stream isn't writeable and enableOfflineQueue options is false"))).toBe('connection')
    expect(classifyRedisError(Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:1'), { code: 'ECONNREFUSED' }))).toBe('connection')
    expect(classifyRedisError(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }))).toBe('connection')
    expect(classifyRedisError(Object.assign(new Error('connect ETIMEDOUT'), { code: 'ETIMEDOUT' }))).toBe('connection')
  })

  it('classifies client-side timeouts', () => {
    const maxRetries = new Error('Reached the max retries per request limit (which is 20).')
    maxRetries.name = 'MaxRetriesPerRequestError'
    expect(classifyRedisError(maxRetries)).toBe('timeout')
    expect(classifyRedisError(new Error('Command timed out'))).toBe('timeout')
  })

  it('falls back to unknown for anything else, including non-errors', () => {
    expect(classifyRedisError(new Error('WRONGTYPE Operation against a key holding the wrong kind of value'))).toBe('unknown')
    expect(classifyRedisError('boom')).toBe('unknown')
    expect(classifyRedisError(undefined)).toBe('unknown')
    // "OOM" must be the error code, not just any substring.
    expect(classifyRedisError(new Error('BLOOM filter missing'))).toBe('unknown')
  })
})

describe('error classes', () => {
  it('RedisJMEnqueueError carries reason, jobId and the original cause', () => {
    const cause = oomError()
    const err = new RedisJMEnqueueError('oom', 'job#1', cause)
    expect(err).toBeInstanceOf(Error)
    expect(err.name).toBe('RedisJMEnqueueError')
    expect(err.reason).toBe('oom')
    expect(err.jobId).toBe('job#1')
    expect(err.cause).toBe(cause)
    expect(err.message).toContain('job#1')
    expect(err.message).toContain('oom')
  })

  it('JobTimeoutError carries timeoutMs', () => {
    const err = new JobTimeoutError(250, 'job#1')
    expect(err.name).toBe('JobTimeoutError')
    expect(err.timeoutMs).toBe(250)
    expect(err.message).toContain('250ms')
  })
})
