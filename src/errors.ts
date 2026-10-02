import { toError } from './utils'

/**
 * Coarse classification of a Redis command failure, shared by every path that has to decide what a
 * failed write MEANS (enqueue errors, maintenance's lock/emergency decision, start-phase recovery):
 *
 * - `'oom'`        — the server refused a write because `used_memory > maxmemory` under a
 *                    `noeviction` policy (`OOM command not allowed …`). Deletions still work.
 * - `'readonly'`   — the client is talking to a read-only replica (`READONLY …`), e.g. mid-failover.
 * - `'connection'` — the connection is gone or never came up (ioredis "Connection is closed.",
 *                    `ECONNREFUSED` / `ECONNRESET` / `ETIMEDOUT` / `EPIPE` …).
 * - `'timeout'`    — the client gave up waiting (ioredis `MaxRetriesPerRequestError`, or a
 *                    `commandTimeout` "Command timed out").
 * - `'unknown'`    — anything else (including non-Redis errors).
 */
export type RedisErrorReason = 'oom' | 'readonly' | 'connection' | 'timeout' | 'unknown'

/**
 * Reason carried by {@link RedisJMEnqueueError}: a classified Redis failure, or `'inputs-too-large'`
 * when the serialized inputs exceed a configured size limit (rejected before any Redis write).
 */
export type EnqueueErrorReason = RedisErrorReason | 'inputs-too-large'

/** Socket-level error codes (Node `err.code`) that mean the connection itself is unusable. */
const CONNECTION_ERROR_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ENOTFOUND',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ECONNABORTED',
])

/**
 * Classifies a thrown value from a Redis command into a {@link RedisErrorReason}. Pure and
 * synchronous; never throws. Matching is on the server's error PREFIX for reply errors (Redis
 * prefixes every error reply with an upper-case code: `OOM`, `READONLY`, …) and on ioredis' own
 * error names / messages / socket codes for client-side failures.
 *
 * @example
 * ```ts
 * try { await redis.set('k', 'v') } catch (err) {
 *   if (classifyRedisError(err) === 'oom') { ... }
 * }
 * ```
 */
export function classifyRedisError(err: unknown): RedisErrorReason {
  if (err === null || err === undefined) return 'unknown'
  const error = toError(err)
  const message = error.message ?? ''
  // A MULTI whose queued commands were refused fails EXEC with `EXECABORT …`; ioredis attaches the
  // refusals as `previousErrors` — classify by the first one (under maxmemory that is the OOM).
  const previous = (error as Error & { previousErrors?: unknown }).previousErrors
  if (/^EXECABORT\b/.test(message) && Array.isArray(previous) && previous.length > 0) {
    return classifyRedisError(previous[0])
  }
  // Reply errors: Redis puts the error code first. Also accept the canonical OOM phrase anywhere, in
  // case a wrapper quotes it.
  if (/^OOM\b/.test(message) || message.includes('OOM command not allowed')) return 'oom'
  if (/^READONLY\b/.test(message)) return 'readonly'
  // Client-side give-ups. Checked before the generic connection codes: a MaxRetriesPerRequestError
  // is raised BECAUSE the connection kept failing, but what the caller observes is a timeout.
  if (error.name === 'MaxRetriesPerRequestError' || /Command timed out/i.test(message)) return 'timeout'
  const code = (error as Error & { code?: unknown }).code
  if (typeof code === 'string' && CONNECTION_ERROR_CODES.has(code)) return 'connection'
  if (
    /Connection is closed/i.test(message)
    || /Stream isn't writeable/i.test(message)
    || /\bECONN(REFUSED|RESET|ABORTED)\b|\bETIMEDOUT\b|\bEPIPE\b/.test(message)
  ) {
    return 'connection'
  }
  return 'unknown'
}

/**
 * Thrown by `queue()` / `queueFirst()` / `enqueue()` / `enqueueMany()` (and their `Job` counterparts)
 * when Redis refuses or fails an enqueue, or (reason `'inputs-too-large'`) before any write when the
 * serialized inputs exceed the size limit. The enqueue is one atomic script, so a refused one (e.g. OOM —
 * Redis refuses the script up front) wrote nothing and retrying the same runId is safe. A `connection` /
 * `timeout` failure is ambiguous: the script may still have run on the server, so a retry can come back
 * `'deduped'`. Validation errors (bad lane, bad delay) are NOT wrapped — they stay plain `TypeError`/`Error`.
 *
 * @example
 * ```ts
 * try {
 *   await manager.queue(job, 'run-1', inputs)
 * } catch (err) {
 *   if (err instanceof RedisJMEnqueueError && err.reason === 'oom') {
 *     // Redis is full: shed load / alert instead of retrying in a tight loop
 *   }
 * }
 * ```
 */
export class RedisJMEnqueueError extends Error {
  /** Classified cause of the failure (see {@link RedisErrorReason}, plus `'inputs-too-large'`). */
  readonly reason: EnqueueErrorReason
  /** The `"jobName#runId"` whose enqueue failed. */
  readonly jobId: string
  /** The underlying error (the Redis reply / client error). */
  override readonly cause: Error

  constructor(reason: EnqueueErrorReason, jobId: string, cause: unknown) {
    const causeError = toError(cause)
    super(`enqueue of "${jobId}" failed (${reason}): ${causeError.message}`)
    this.name = 'RedisJMEnqueueError'
    this.reason = reason
    this.jobId = jobId
    // Assigned explicitly (not via `super(msg, { cause })`): with ES2022 class fields a declared
    // `cause` field would otherwise be re-initialized to `undefined` after `super()` returns.
    this.cause = causeError
  }
}

/**
 * Thrown out of `Job.execute()` (and passed to the `error` hooks) when a run exceeds its execution
 * timeout (`JobMetadata.timeoutMs` / `RedisJMOptions.jobTimeout`). The run's `ctx.signal` is aborted
 * with reason `'timeout'` at the same moment. It is an ordinary run failure: `attempts`/`backoff`
 * apply, so a timed-out attempt is retried when the budget allows.
 */
export class JobTimeoutError extends Error {
  /** The timeout that elapsed, in ms. */
  readonly timeoutMs: number

  constructor(timeoutMs: number, jobId?: string) {
    super(`${jobId ? `job "${jobId}" ` : 'job '}timed out after ${timeoutMs}ms`)
    this.name = 'JobTimeoutError'
    this.timeoutMs = timeoutMs
  }
}
