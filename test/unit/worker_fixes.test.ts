import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";

import type { Client } from "../../src/client.js";
import { ServerClock } from "../../src/clock.js";
import { LeaseLostError } from "../../src/errors.js";
import type { Job as PbJob } from "../../src/gen/runnel/v1/types_pb.js";
import { JobSchema } from "../../src/gen/runnel/v1/types_pb.js";
import { OutcomeKind } from "../../src/gen/runnel/v1/worker_pb.js";
import { retry } from "../../src/outcome.js";
import { Worker } from "../../src/worker.js";

interface Finished {
  id: string;
  kind: OutcomeKind;
}

function untilAborted(signal: AbortSignal | undefined): Promise<never> {
  return new Promise((_, reject) => {
    signal?.addEventListener("abort", () => {
      reject(new Error("aborted"));
    });
  });
}

function fake(
  jobs: PbJob[],
  extend: (n: number) => Promise<string[]>,
  again?: { jobs: PbJob[]; after: Promise<void> },
): { client: Client; finished: Finished[] } {
  const finished: Finished[] = [];
  let claims = 0;
  let beats = 0;
  const workers = {
    claim: async (_: unknown, o: { signal?: AbortSignal }) => {
      claims++;
      if (claims === 1) {
        return { jobs };
      }
      if (claims === 2 && again) {
        await again.after;
        return { jobs: again.jobs };
      }
      return await untilAborted(o.signal);
    },
    extend: async () => ({ lost: await extend(++beats) }),
    finish: (req: { id: string; outcome: { kind: OutcomeKind } }) => {
      finished.push({ id: req.id, kind: req.outcome.kind });
      return Promise.resolve({});
    },
  };
  const client = { namespace: "ns", clock: new ServerClock(), rpc: { workers } } as unknown as Client;
  return { client, finished };
}

const quiet = { info: () => undefined, warn: () => undefined, error: () => undefined };

async function until(what: string, ms: number, ok: () => boolean): Promise<void> {
  const end = Date.now() + ms;
  while (!ok()) {
    if (Date.now() > end) {
      throw new Error(`timed out waiting for ${what}`);
    }
    await new Promise((r) => setTimeout(r, 10));
  }
}

describe("worker fixes", () => {
  it("renews a heartbeat by the lease the server granted, not the one asked for", async () => {
    const start = Date.now();
    const job = create(JobSchema, { id: "j1", kind: "k", queue: "q", leaseUntil: timestampFromMs(start + 1000) });
    const { client } = fake([job], (n) => (n === 1 ? Promise.resolve([]) : Promise.reject(new Error("unavailable"))));
    const w = new Worker(client, { queues: ["q"], mode: "poll", leaseMs: 3000, shutdownTimeoutMs: 100, logger: quiet });
    let lostAt = 0;
    w.handle("k", async (_, ctx) => {
      await untilAborted(ctx.signal).catch(() => {
        if (ctx.signal.reason instanceof LeaseLostError) {
          lostAt = Date.now();
        }
      });
    });
    const ran = w.run();
    await until("the lease to end", 6000, () => lostAt > 0);
    w.stop();
    await ran;
    expect(lostAt - start).toBeLessThan(3600);
  });

  it("reports a timeout when the handler returns after its context ended", async () => {
    const job = create(JobSchema, { id: "j1", kind: "k", queue: "q", timeout: { seconds: 0n, nanos: 100_000_000 } });
    const { client, finished } = fake([job], () => Promise.resolve([]));
    const w = new Worker(client, { queues: ["q"], mode: "poll", shutdownTimeoutMs: 100, logger: quiet });
    w.handle("k", async (_, ctx) => {
      await untilAborted(ctx.signal).catch(() => undefined);
      return "ignored";
    });
    const ran = w.run();
    await until("the job to finish", 3000, () => finished.length > 0);
    w.stop();
    await ran;
    expect(finished[0]?.kind).toBe(OutcomeKind.RETRY);
  });

  it("ends the run that still held a job when the job is handed out again", async () => {
    const first = create(JobSchema, { id: "j1", kind: "k", queue: "q", version: 1n });
    const again = create(JobSchema, { id: "j1", kind: "k", queue: "q", version: 2n });
    let started = (): void => undefined;
    const after = new Promise<void>((r) => {
      started = r;
    });
    const { client, finished } = fake([first], () => Promise.resolve([]), { jobs: [again], after });
    const w = new Worker(client, { queues: ["q"], mode: "poll", shutdownTimeoutMs: 100, logger: quiet });
    let calls = 0;
    let reason: unknown;
    w.handle("k", async (_, ctx) => {
      calls++;
      if (calls > 1) {
        return "second";
      }
      started();
      await untilAborted(ctx.signal).catch(() => {
        reason = ctx.signal.reason;
      });
      return "first";
    });
    const ran = w.run();
    await until("the second run to finish", 3000, () => finished.length > 0);
    await until("the first run to end", 3000, () => reason !== undefined);
    w.stop();
    await ran;
    expect(reason).toBeInstanceOf(LeaseLostError);
    expect(finished.map((f) => f.kind)).toEqual([OutcomeKind.DONE]);
  });

  it("stops an idle poll worker without waiting for its long poll", async () => {
    const { client } = fake([], () => Promise.resolve([]));
    const w = new Worker(client, { queues: ["q"], mode: "poll", shutdownTimeoutMs: 5000, logger: quiet });
    w.handle("k", () => undefined);
    const ran = w.run();
    await new Promise((r) => setTimeout(r, 50));
    const stopped = Date.now();
    w.stop();
    await ran;
    expect(Date.now() - stopped).toBeLessThan(1000);
  });

  it("refuses a concurrency or lease that is not positive", async () => {
    const { client } = fake([], () => Promise.resolve([]));
    for (const o of [{ concurrency: 0 }, { leaseMs: 0 }, { leaseMs: -1 }]) {
      const w = new Worker(client, { queues: ["q"], mode: "poll", logger: quiet, ...o });
      w.handle("k", () => undefined);
      await expect(w.run()).rejects.toThrow(/positive/);
    }
  });

  it("keeps a thrown value that cannot be turned into JSON", () => {
    const loop: Record<string, unknown> = {};
    loop.self = loop;
    expect(retry(loop).error).toBe("[object Object]");
    expect(retry(10n).error).toBe("10");
  });
});
