import { randomBytes } from "node:crypto";
import { hostname } from "node:os";

import { durationFromMs, timestampMs } from "@bufbuild/protobuf/wkt";

import type { Client } from "./client.js";
import { Code, DeadlineError, LeaseLostError, ShutdownError, TimeoutError, toRunnelError } from "./errors.js";
import { PollFeed, StreamFeed, type Feed, type Logger } from "./feed.js";
import type { Job as PbJob } from "./gen/runnel/v1/types_pb.js";
import type { Outcome } from "./gen/runnel/v1/worker_pb.js";
import { Job } from "./job.js";
import { Control, done, fail, release, retry } from "./outcome.js";
import { sleep } from "./pipe.js";

export interface JobContext {
  readonly signal: AbortSignal;
  readonly worker: Worker;
  dependencies(): Promise<Job[]>;
}

export type Handler = (job: Job, ctx: JobContext) => unknown;

export interface Handlers {
  run: Handler;
  poll?: Handler;
}

export interface WorkerOptions {
  queues: string[];
  id?: string;
  concurrency?: number;
  leaseMs?: number;
  shutdownTimeoutMs?: number;
  mode?: "stream" | "poll";
  claimWaitMs?: number;
  finishTries?: number;
  retryMinMs?: number;
  retryMaxMs?: number;
  logger?: Logger;
}

interface Run {
  pb: PbJob;
  job: Job;
  abort: AbortController;
  lost: boolean;
  done: Promise<void>;
  leaseMs: number;
  leaseUntil: number;
}

const retryable = new Set([Code.Unavailable, Code.Unknown, Code.Internal, Code.DeadlineExceeded]);

function defaultID(): string {
  const host = hostname().replace(/[^A-Za-z0-9._-]/g, "-").slice(0, 64) || "worker";
  return `${host}-${process.pid}-${randomBytes(3).toString("hex")}`;
}

export class Worker {
  readonly id: string;
  readonly #client: Client;
  readonly #o: Required<Omit<WorkerOptions, "id" | "logger">>;
  readonly #log: Logger;
  readonly #handlers = new Map<string, Handlers>();
  readonly #running = new Map<string, Run>();
  readonly #stop = new AbortController();
  #feed: Feed | undefined;
  #stopping = false;
  #beating = false;
  #beatDue = false;

  constructor(client: Client, o: WorkerOptions) {
    this.#client = client;
    this.id = o.id ?? defaultID();
    this.#log = o.logger ?? console;
    this.#o = {
      queues: o.queues,
      concurrency: o.concurrency ?? 10,
      leaseMs: o.leaseMs ?? 30_000,
      shutdownTimeoutMs: o.shutdownTimeoutMs ?? 30_000,
      mode: o.mode ?? "stream",
      claimWaitMs: o.claimWaitMs ?? 20_000,
      finishTries: o.finishTries ?? 6,
      retryMinMs: o.retryMinMs ?? 200,
      retryMaxMs: o.retryMaxMs ?? 5_000,
    };
  }

  handle(kind: string, h: Handler | Handlers): this {
    this.#handlers.set(kind, typeof h === "function" ? { run: h } : h);
    return this;
  }

  get running(): number {
    return this.#running.size;
  }

  stop(): void {
    this.#stop.abort();
  }

  async run(signal?: AbortSignal): Promise<void> {
    if (this.#feed) {
      throw new Error("runnel: worker is already running");
    }
    if (this.#o.queues.length === 0 || this.#handlers.size === 0) {
      throw new Error("runnel: a worker needs queues and handlers");
    }
    const o = this.#o;
    const config = {
      client: this.#client,
      worker: this.id,
      queues: o.queues,
      leaseMs: o.leaseMs,
      claimWaitMs: o.claimWaitMs,
      retryMinMs: o.retryMinMs,
      retryMaxMs: o.retryMaxMs,
      log: this.#log,
      free: () => (this.#stopping ? 0 : o.concurrency - this.#running.size),
      deliver: (j: PbJob) => {
        this.#deliver(j);
      },
    };
    const feed: Feed = o.mode === "poll" ? new PollFeed(config) : new StreamFeed(config);
    this.#feed = feed;
    const beat = setInterval(() => void this.#heartbeat(), o.leaseMs / 3);
    const fed = feed.run();
    await this.#untilStopped(signal);
    this.#stopping = true;
    feed.stop();
    const drained = (): Promise<unknown> => Promise.all([...this.#running.values()].map((r) => r.done));
    if (!(await this.#within(drained(), o.shutdownTimeoutMs))) {
      for (const r of this.#running.values()) {
        r.abort.abort(new ShutdownError());
      }
      await drained();
    }
    if (!(await this.#within(fed, o.shutdownTimeoutMs))) {
      feed.abort();
      await fed;
    }
    await drained();
    clearInterval(beat);
  }

  async #within(p: Promise<unknown>, ms: number): Promise<boolean> {
    const timer = new AbortController();
    const ok = await Promise.race([p.then(() => true), sleep(ms, timer.signal).then(() => false)]);
    timer.abort();
    return ok;
  }

  #untilStopped(signal: AbortSignal | undefined): Promise<void> {
    const all = signal ? AbortSignal.any([signal, this.#stop.signal]) : this.#stop.signal;
    return new Promise((resolve) => {
      if (all.aborted) {
        resolve();
        return;
      }
      all.addEventListener(
        "abort",
        () => {
          resolve();
        },
        { once: true },
      );
    });
  }

  #deliver(pb: PbJob): void {
    if (this.#stopping) {
      void this.#finish(pb, release());
      return;
    }
    const now = this.#client.clock.now();
    const until = pb.leaseUntil ? timestampMs(pb.leaseUntil) : now + this.#o.leaseMs;
    const r: Run = { pb, job: new Job(pb), abort: new AbortController(), lost: false, done: Promise.resolve(), leaseMs: until - now, leaseUntil: until };
    this.#running.set(pb.id, r);
    r.done = this.#execute(r).finally(() => {
      if (this.#running.get(pb.id) === r) {
        this.#running.delete(pb.id);
      }
      this.#feed?.grant(1);
    });
  }

  #timers(r: Run): () => void {
    const timers: NodeJS.Timeout[] = [];
    const { job } = r;
    if (job.timeoutMs !== undefined && job.timeoutMs > 0) {
      const ms = job.timeoutMs;
      timers.push(
        setTimeout(() => {
          r.abort.abort(new TimeoutError(ms));
        }, ms),
      );
    }
    if (job.deadline) {
      const clock = this.#client.clock;
      const left = clock.toLocal(job.deadline.getTime()) - clock.local();
      timers.push(
        setTimeout(
          () => {
            r.abort.abort(new DeadlineError());
          },
          Math.max(0, left),
        ),
      );
    }
    return () => {
      timers.forEach(clearTimeout);
    };
  }

  async #attempt(r: Run): Promise<Outcome> {
    const { job } = r;
    const h = this.#handlers.get(job.kind);
    const ctx: JobContext = {
      signal: r.abort.signal,
      worker: this,
      dependencies: () => this.#dependencies(job),
    };
    try {
      if (!h) {
        throw fail(`runnel: no handler for kind "${job.kind}"`);
      }
      const fn = job.polling ? h.poll : h.run;
      if (!fn) {
        throw fail(`kind "${job.kind}" has a handle but no poller`);
      }
      const value: unknown = await fn(job, ctx);
      return value instanceof Control ? value.outcome : done(value);
    } catch (err) {
      return this.#classify(err, r.abort.signal.reason);
    }
  }

  #classify(err: unknown, reason: unknown): Outcome {
    if (reason instanceof ShutdownError) {
      return release();
    }
    if (reason instanceof TimeoutError) {
      return retry(`timed out after ${reason.timeoutMs / 1000}s`);
    }
    if (reason instanceof DeadlineError) {
      return fail("deadline passed").outcome;
    }
    return err instanceof Control ? err.outcome : retry(err);
  }

  async #execute(r: Run): Promise<void> {
    const clear = this.#timers(r);
    let outcome: Outcome;
    try {
      outcome = await this.#attempt(r);
    } finally {
      clear();
    }
    if (r.lost) {
      this.#log.warn("runnel: lease lost, result dropped", r.pb.id);
      return;
    }
    await this.#finish(r.pb, outcome);
  }

  async #finish(pb: PbJob, outcome: Outcome): Promise<void> {
    let pause = this.#o.retryMinMs;
    for (let i = 1; i <= this.#o.finishTries; i++) {
      try {
        await this.#client.rpc.workers.finish({ namespace: this.#client.namespace, worker: this.id, id: pb.id, version: pb.version, outcome });
        return;
      } catch (e) {
        const err = toRunnelError(e);
        if (!retryable.has(err.code)) {
          this.#log.warn("runnel: finish refused, result dropped", pb.id, err.message);
          return;
        }
        this.#log.error("runnel: finishing job failed", pb.id, i, err.message);
        await sleep(pause);
        pause = Math.min(pause * 2, this.#o.retryMaxMs);
      }
    }
  }

  async #dependencies(job: Job): Promise<Job[]> {
    if (job.dependsOn.length === 0) {
      return [];
    }
    try {
      const res = await this.#client.rpc.workers.dependencies({ namespace: this.#client.namespace, ids: [...job.dependsOn] });
      return res.jobs.map((j) => new Job(j));
    } catch (err) {
      throw toRunnelError(err);
    }
  }

  async #heartbeat(): Promise<void> {
    if (this.#beating) {
      this.#beatDue = true;
      return;
    }
    const sent = new Map(this.#running);
    if (sent.size === 0) {
      return;
    }
    this.#beating = true;
    try {
      const res = await this.#client.rpc.workers.extend(
        { namespace: this.#client.namespace, worker: this.id, ids: [...sent.keys()], lease: durationFromMs(this.#o.leaseMs) },
        { timeoutMs: this.#o.leaseMs / 3 },
      );
      this.#client.clock.observe(res.now);
      const now = this.#client.clock.now();
      const lost = new Set(res.lost);
      for (const [id, r] of sent) {
        if (lost.has(id)) {
          this.#lose(id, r);
        } else {
          r.leaseUntil = now + r.leaseMs;
        }
      }
    } catch (err) {
      this.#log.error("runnel: heartbeat failed", toRunnelError(err).message);
    } finally {
      this.#beating = false;
    }
    const now = this.#client.clock.now();
    for (const [id, r] of sent) {
      if (now >= r.leaseUntil) {
        this.#lose(id, r);
      }
    }
    if (this.#beatDue) {
      this.#beatDue = false;
      void this.#heartbeat();
    }
  }

  #lose(id: string, r: Run): void {
    if (this.#running.get(id) === r && !r.lost) {
      r.lost = true;
      r.abort.abort(new LeaseLostError());
    }
  }
}
