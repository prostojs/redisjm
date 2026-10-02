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
