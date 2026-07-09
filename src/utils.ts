/**
 * Normalizes an unknown thrown value into an `Error`. JS can throw anything (strings, plain
 * objects, `undefined`); this guarantees a real `Error` with a usable `.message`/`.stack` for
 * logging and re-throwing, without masking a genuine `Error` that was thrown.
 */
export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err))
}
