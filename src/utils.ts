/**
 * Normalizes an unknown thrown value into an `Error`. JS can throw anything (strings, plain
 * objects, `undefined`); this guarantees a real `Error` with a usable `.message`/`.stack` for
 * logging and re-throwing, without masking a genuine `Error` that was thrown.
 */
export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err))
}

/**
 * A thrown value that can key a `WeakMap`/`WeakSet` tag: an object as-is (preserving its identity, e.g.
 * `instanceof RunSupersededError`), a thrown primitive normalized to an `Error` first.
 */
export function toTaggable(err: unknown): object {
  return typeof err === 'object' && err !== null ? err : toError(err)
}

/** Normalizes an optional limit (ms, bytes, count): finite and `> 0` → itself, anything else → `0` (off). */
export function positiveOrZero(value: number | undefined): number {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : 0
}

/** Normalizes an optional cap: finite and `>= 0` → floored, anything else → `undefined` (no cap). */
export function nonNegativeInt(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) && value >= 0 ? Math.floor(value) : undefined
}

/** Node's timer limit (2^31 - 1 ms); a longer delay fires after 1 ms. */
const MAX_TIMER_MS = 2_147_483_647

/**
 * Validates an `abortGraceMs` value (`false` / `undefined` = none, else a finite number in `0..2^31-1`) and returns
 * it; anything else throws a `TypeError` naming `label`.
 */
export function checkAbortGraceMs(value: unknown, label: string): number | false | undefined {
  if (value === undefined || value === false) return value
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= MAX_TIMER_MS) return value
  throw new TypeError(`${label}: abortGraceMs must be false or a finite number in 0..${MAX_TIMER_MS}, got ${String(value)}`)
}
