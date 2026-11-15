import { create } from "@bufbuild/protobuf";
import { durationFromMs } from "@bufbuild/protobuf/wkt";

import type { Client } from "./client.js";
import { toRunnelError } from "./errors.js";
import type { Job as PbJob } from "./gen/runnel/v1/types_pb.js";
import { WorkRequestSchema, type WorkRequest } from "./gen/runnel/v1/worker_pb.js";
import { Pipe, sleep } from "./pipe.js";

export interface Logger {
  error(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
}

export interface FeedConfig {
  client: Client;
  worker: string;
  queues: string[];
  leaseMs: number;
  claimWaitMs: number;
  retryMinMs: number;
  retryMaxMs: number;
  log: Logger;
  free(): number;
  deliver(job: PbJob): void;
}

export interface Feed {
  run(): Promise<void>;
  grant(n: number): void;
  stop(): void;
  abort(): void;
}

class Backoff {
  #pause: number;

  constructor(private readonly c: FeedConfig) {
    this.#pause = c.retryMinMs;
  }

  reset(): void {
    this.#pause = this.c.retryMinMs;
  }

  async wait(signal: AbortSignal): Promise<void> {
    await sleep(this.#pause, signal);
    this.#pause = Math.min(this.#pause * 2, this.c.retryMaxMs);
  }
}

function credit(n: number): WorkRequest {
  return create(WorkRequestSchema, { msg: { case: "credit", value: n } });
}

export class StreamFeed implements Feed {
  #pipe: Pipe<WorkRequest> | undefined;
  #stopped = false;
  readonly #abort = new AbortController();

  constructor(private readonly c: FeedConfig) {}

  async run(): Promise<void> {
    const c = this.c;
    const backoff = new Backoff(c);
    while (!this.stopped()) {
      const pipe = new Pipe<WorkRequest>();
      pipe.push(
        create(WorkRequestSchema, {
          msg: {
            case: "start",
            value: { namespace: c.client.namespace, worker: c.worker, queues: c.queues, lease: durationFromMs(c.leaseMs) },
          },
        }),
      );
      this.#pipe = pipe;
      this.grant(c.free());
      try {
        for await (const r of c.client.rpc.workers.work(pipe, { signal: this.#abort.signal })) {
          backoff.reset();
          c.client.clock.observe(r.now);
          if (r.msg.case === "job") {
            c.deliver(r.msg.value);
          }
        }
      } catch (err) {
        if (!this.stopped()) {
          c.log.error("runnel: work stream failed", toRunnelError(err));
        }
      } finally {
        this.#pipe = undefined;
        pipe.close();
      }
      if (!this.stopped()) {
        await backoff.wait(this.#abort.signal);
      }
    }
  }

  stopped(): boolean {
    return this.#stopped;
  }

  grant(n: number): void {
    if (n > 0) {
      this.#pipe?.push(credit(n));
    }
  }

  stop(): void {
    this.#stopped = true;
    this.#pipe?.close();
  }

  abort(): void {
    this.stop();
    this.#abort.abort();
  }
}

export class PollFeed implements Feed {
  #stopped = false;
  #wake: (() => void) | undefined;
  readonly #abort = new AbortController();

  constructor(private readonly c: FeedConfig) {}

  async run(): Promise<void> {
    const c = this.c;
    const backoff = new Backoff(c);
    while (!this.stopped()) {
      const n = c.free();
      if (n <= 0) {
        await new Promise<void>((resolve) => {
          this.#wake = resolve;
        });
        continue;
      }
      try {
        const res = await c.client.rpc.workers.claim(
          {
            namespace: c.client.namespace,
            worker: c.worker,
            queues: c.queues,
            limit: n,
            lease: durationFromMs(c.leaseMs),
            wait: durationFromMs(c.claimWaitMs),
          },
          { signal: this.#abort.signal },
        );
        backoff.reset();
        c.client.clock.observe(res.now);
        for (const j of res.jobs) {
          c.deliver(j);
        }
      } catch (err) {
        if (this.stopped()) {
          break;
        }
        c.log.error("runnel: claim failed", toRunnelError(err));
        await backoff.wait(this.#abort.signal);
      }
    }
  }

  stopped(): boolean {
    return this.#stopped;
  }

  grant(): void {
    const wake = this.#wake;
    this.#wake = undefined;
    wake?.();
  }

  stop(): void {
    this.#stopped = true;
    this.grant();
  }

  abort(): void {
    this.stop();
    this.#abort.abort();
  }
}
