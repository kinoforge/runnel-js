import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import type { Client } from "../../src/client.js";
import { ServerClock } from "../../src/clock.js";
import { JobSchema } from "../../src/gen/runnel/v1/types_pb.js";
import { Worker } from "../../src/worker.js";

interface Extend {
  at: number;
  ids: string[];
}

function untilAborted(signal: AbortSignal | undefined): Promise<never> {
  return new Promise((_, reject) => {
    signal?.addEventListener("abort", () => {
      reject(new Error("aborted"));
    });
  });
}

function fake(claims: (() => Promise<string[]>)[], extend: (call: Extend, n: number, timeoutMs: number) => Promise<string[]>): { client: Client; extends: Extend[] } {
  const calls: Extend[] = [];
  let claimed = 0;
  const workers = {
    claim: async (_: unknown, o: { signal?: AbortSignal }) => {
      const next = claims[claimed++];
      const ids = next ? await next() : await untilAborted(o.signal);
      return { jobs: ids.map((id) => create(JobSchema, { id, kind: "k", queue: "q" })) };
    },
    extend: async (req: { ids: string[] }, o: { timeoutMs: number }) => {
      const call = { at: Date.now(), ids: [...req.ids] };
      calls.push(call);
      return { lost: await extend(call, calls.length, o.timeoutMs) };
    },
    finish: () => Promise.resolve({}),
  };
  const client = { namespace: "ns", clock: new ServerClock(), rpc: { workers } } as unknown as Client;
  return { client, extends: calls };
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

describe("worker heartbeats", () => {
  it("follows a timed-out heartbeat at once", async () => {
    const { client, extends: calls } = fake([() => Promise.resolve(["j1"])], async (_, n, timeoutMs) => {
      if (n === 1) {
        await new Promise((r) => setTimeout(r, timeoutMs + 50));
        throw new Error("deadline exceeded");
      }
      return [];
    });
    const w = new Worker(client, { queues: ["q"], mode: "poll", leaseMs: 1200, shutdownTimeoutMs: 100, logger: quiet });
    w.handle("k", (_, ctx) => untilAborted(ctx.signal));
    const ran = w.run();
    await until("two heartbeats", 3000, () => calls.length >= 2);
    w.stop();
    await ran;
    const [first, second] = calls;
    expect(second && first && second.at - first.at).toBeLessThan(600);
  });

  it("keeps the run that replaced an ending one", async () => {
    let lostSent: () => void = () => undefined;
    const lost = new Promise<void>((r) => (lostSent = r));
    let firstEnded = 0;
    const { client, extends: calls } = fake(
      [
        () => Promise.resolve(["j1"]),
        async () => {
          await lost;
          return ["j1"];
        },
      ],
      (_, n) => {
        if (n === 1) {
          lostSent();
          return Promise.resolve(["j1"]);
        }
        return Promise.resolve([]);
      },
    );
    const w = new Worker(client, { queues: ["q"], mode: "poll", concurrency: 2, leaseMs: 600, shutdownTimeoutMs: 100, logger: quiet });
    let runs = 0;
    w.handle("k", async (_, ctx) => {
      if (++runs === 1) {
        await new Promise((r) => setTimeout(r, 300));
        firstEnded = Date.now();
        return;
      }
      await untilAborted(ctx.signal);
    });
    const ran = w.run();
    await until("a heartbeat for the second run after the first ended", 3000, () =>
      calls.some((c) => firstEnded > 0 && c.at > firstEnded && c.ids.includes("j1")),
    );
    w.stop();
    await ran;
  });
});
