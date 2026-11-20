import { create } from "@bufbuild/protobuf";
import { durationFromMs } from "@bufbuild/protobuf/wkt";

import { OutcomeKind, OutcomeSchema, type Outcome } from "./gen/runnel/v1/worker_pb.js";
import { encodeJSON } from "./job.js";

export class Control extends Error {
  override readonly name = "RunnelControl";

  constructor(
    readonly outcome: Outcome,
    message: string,
  ) {
    super(message);
  }
}

function message(err: unknown): string {
  if (err === undefined) {
    return "";
  }
  if (err instanceof Error) {
    return err.message;
  }
  if (typeof err === "string") {
    return err;
  }
  if (typeof err === "bigint" || typeof err === "number" || typeof err === "boolean" || typeof err === "symbol") {
    return String(err);
  }
  try {
    const json = JSON.stringify(err) as string | undefined;
    return json ?? Object.prototype.toString.call(err);
  } catch {
    return Object.prototype.toString.call(err);
  }
}

export function fail(err: unknown): Control {
  const m = message(err);
  return new Control(create(OutcomeSchema, { kind: OutcomeKind.FAIL, error: m }), m);
}

export function retryIn(delayMs: number, err?: unknown): Control {
  const m = message(err);
  return new Control(create(OutcomeSchema, { kind: OutcomeKind.RETRY, error: m, delay: durationFromMs(delayMs) }), m);
}

export function busy(delayMs: number): Control {
  return new Control(create(OutcomeSchema, { kind: OutcomeKind.BUSY, delay: durationFromMs(delayMs) }), "busy");
}

export function pending(handle: string, delayMs: number): Control {
  return new Control(create(OutcomeSchema, { kind: OutcomeKind.PENDING, handle, delay: durationFromMs(delayMs) }), "pending");
}

export function done(result: unknown): Outcome {
  return create(OutcomeSchema, { kind: OutcomeKind.DONE, result: encodeJSON(result) });
}

export function retry(err: unknown): Outcome {
  return create(OutcomeSchema, { kind: OutcomeKind.RETRY, error: message(err) || "error" });
}

export function release(): Outcome {
  return create(OutcomeSchema, { kind: OutcomeKind.RELEASE });
}
