import { Code, ConnectError } from "@connectrpc/connect";

export { Code };

export type ErrorReason =
  | "invalid"
  | "not_found"
  | "stale"
  | "queue_full"
  | "group_full"
  | "no_space"
  | "dependency"
  | "conflict"
  | "final"
  | "not_final";

export class RunnelError extends Error {
  override readonly name = "RunnelError";
  readonly code: Code;
  readonly reason: ErrorReason | undefined;
  readonly retryAfterMs: number | undefined;

  constructor(message: string, code: Code, reason?: ErrorReason, retryAfterMs?: number, options?: ErrorOptions) {
    super(message, options);
    this.code = code;
    this.reason = reason;
    this.retryAfterMs = retryAfterMs;
  }

  get busy(): boolean {
    return this.code === Code.ResourceExhausted;
  }
}

export function toRunnelError(err: unknown): RunnelError {
  if (err instanceof RunnelError) {
    return err;
  }
  const ce = ConnectError.from(err);
  const reason = ce.metadata.get("runnel-error") ?? undefined;
  const after = ce.metadata.get("retry-after");
  const seconds = after === null ? Number.NaN : Number(after);
  return new RunnelError(ce.rawMessage, ce.code, reason as ErrorReason | undefined, Number.isFinite(seconds) ? seconds * 1000 : undefined, {
    cause: err,
  });
}

export async function call<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    throw toRunnelError(err);
  }
}

export class ShutdownError extends Error {
  override readonly name = "ShutdownError";
  constructor() {
    super("runnel: worker shutting down");
  }
}

export class TimeoutError extends Error {
  override readonly name = "TimeoutError";
  constructor(readonly timeoutMs: number) {
    super(`runnel: attempt timed out after ${timeoutMs / 1000}s`);
  }
}

export class DeadlineError extends Error {
  override readonly name = "DeadlineError";
  constructor() {
    super("runnel: deadline passed");
  }
}

export class LeaseLostError extends Error {
  override readonly name = "LeaseLostError";
  constructor() {
    super("runnel: lease lost");
  }
}

export class SignatureError extends Error {
  override readonly name = "SignatureError";
}
