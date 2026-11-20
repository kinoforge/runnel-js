import { create } from "@bufbuild/protobuf";
import { durationFromMs, durationMs, timestampDate, timestampFromDate, type Duration, type Timestamp } from "@bufbuild/protobuf/wkt";

import { BackoffSchema, NewJobSchema, State, type Job as PbJob, type NewJob } from "./gen/runnel/v1/types_pb.js";

export type JobState = "blocked" | "waiting" | "running" | "polling" | "completed" | "dead" | "cancelled" | "expired";

const finalStates: ReadonlySet<JobState> = new Set(["completed", "dead", "cancelled", "expired"]);

export function jobState(s: State): JobState {
  const name = State[s];
  return (name ? name.toLowerCase() : "unknown") as JobState;
}

export function pbState(s: JobState): State {
  return State[s.toUpperCase() as keyof typeof State];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeJSON(value: unknown): Uint8Array {
  if (value === undefined) {
    return new Uint8Array();
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  return encoder.encode(JSON.stringify(value));
}

function decodeJSON(raw: Uint8Array): unknown {
  return raw.length === 0 ? undefined : JSON.parse(decoder.decode(raw));
}

function date(t: Timestamp | undefined): Date | undefined {
  return t ? timestampDate(t) : undefined;
}

function ms(d: Duration | undefined): number | undefined {
  return d ? durationMs(d) : undefined;
}

export interface AttemptError {
  attempt: number;
  queue: string;
  kind: string;
  at: Date | undefined;
  error: string;
}

export class Job {
  readonly id: string;
  readonly namespace: string;
  readonly queue: string;
  readonly kind: string;
  readonly state: JobState;
  readonly priority: number;
  readonly group: string;
  readonly pool: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly version: bigint;
  readonly worker: string;
  readonly handle: string;
  readonly uniqueKey: string;
  readonly dependsOn: readonly string[];
  readonly callbackUrl: string;
  readonly meta: Readonly<Record<string, string>>;
  readonly rawPayload: Uint8Array;
  readonly rawResult: Uint8Array;
  readonly errors: readonly AttemptError[];
  readonly runAt: Date | undefined;
  readonly startBy: Date | undefined;
  readonly deadline: Date | undefined;
  readonly timeoutMs: number | undefined;
  readonly leaseUntil: Date | undefined;
  readonly createdAt: Date | undefined;
  readonly startedAt: Date | undefined;
  readonly finishedAt: Date | undefined;
  #payload: { v: unknown } | undefined;
  #result: { v: unknown } | undefined;

  constructor(j: PbJob) {
    this.id = j.id;
    this.namespace = j.namespace;
    this.queue = j.queue;
    this.kind = j.kind;
    this.state = jobState(j.state);
    this.priority = j.priority;
    this.group = j.group;
    this.pool = j.pool;
    this.attempt = j.attempt;
    this.maxAttempts = j.maxAttempts;
    this.version = j.version;
    this.worker = j.worker;
    this.handle = j.handle;
    this.uniqueKey = j.uniqueKey;
    this.dependsOn = j.dependsOn;
    this.callbackUrl = j.callbackUrl;
    this.meta = j.meta;
    this.rawPayload = j.payload;
    this.rawResult = j.result;
    this.errors = j.errors.map((e) => ({ attempt: e.attempt, queue: e.queue, kind: e.kind, at: date(e.at), error: e.error }));
    this.runAt = date(j.runAt);
    this.startBy = date(j.startBy);
    this.deadline = date(j.deadline);
    this.timeoutMs = ms(j.timeout);
    this.leaseUntil = date(j.leaseUntil);
    this.createdAt = date(j.createdAt);
    this.startedAt = date(j.startedAt);
    this.finishedAt = date(j.finishedAt);
  }

  get payload(): unknown {
    this.#payload ??= { v: decodeJSON(this.rawPayload) };
    return this.#payload.v;
  }

  get result(): unknown {
    this.#result ??= { v: decodeJSON(this.rawResult) };
    return this.#result.v;
  }

  get final(): boolean {
    return finalStates.has(this.state);
  }

  get polling(): boolean {
    return this.handle !== "";
  }
}

export interface Backoff {
  baseMs?: number;
  maxMs?: number;
  factor?: number;
  jitter?: number;
}

export interface JobInput {
  queue: string;
  kind: string;
  payload?: unknown;
  priority?: number;
  group?: string;
  pool?: string;
  runAt?: Date;
  delayMs?: number;
  maxAttempts?: number;
  uniqueKey?: string;
  uniqueForMs?: number;
  startTimeoutMs?: number;
  deadline?: Date;
  ttlMs?: number;
  timeoutMs?: number;
  backoff?: Backoff;
  dependsOn?: string[];
  callbackUrl?: string;
  meta?: Record<string, string>;
}

function optDuration(v: number | undefined): Duration | undefined {
  return v === undefined ? undefined : durationFromMs(v);
}

function optTimestamp(v: Date | undefined): Timestamp | undefined {
  return v === undefined ? undefined : timestampFromDate(v);
}

export function newJob(j: JobInput): NewJob {
  const b = j.backoff;
  return create(NewJobSchema, {
    queue: j.queue,
    kind: j.kind,
    payload: encodeJSON(j.payload),
    priority: j.priority ?? 0,
    group: j.group ?? "",
    pool: j.pool ?? "",
    runAt: optTimestamp(j.runAt),
    delay: optDuration(j.delayMs),
    maxAttempts: j.maxAttempts ?? 0,
    uniqueKey: j.uniqueKey ?? "",
    uniqueFor: optDuration(j.uniqueForMs),
    startTimeout: optDuration(j.startTimeoutMs),
    deadline: optTimestamp(j.deadline),
    ttl: optDuration(j.ttlMs),
    timeout: optDuration(j.timeoutMs),
    backoff: b
      ? create(BackoffSchema, { base: optDuration(b.baseMs), max: optDuration(b.maxMs), factor: b.factor ?? 0, jitter: b.jitter ?? 0 })
      : undefined,
    dependsOn: j.dependsOn ?? [],
    callbackUrl: j.callbackUrl ?? "",
    meta: j.meta ?? {},
  });
}
