import { describe, expect, it } from "vitest";

import { busy, Code, fail, pending, retryIn, RunnelError } from "../../src/index.js";
import { sleep } from "../../src/pipe.js";
import { env, work } from "./env.js";

const fast = { baseMs: 100, factor: 1, jitter: 0 };

describe("contract: jobs", () => {
  it("C01 a worker completes a job with a result", async () => {
    const c = await (await env()).client();
    work(c, {}, { welcome: (j) => ({ sent: true, to: (j.payload as { to: string }).to }) });
    const job = await c.enqueueAndWait({ queue: "q", kind: "welcome", payload: { to: "ana" } }, { timeoutMs: 15_000 });
    expect(job.state).toBe("completed");
    expect(job.result).toEqual({ sent: true, to: "ana" });
    expect((await c.get(job.id)).result).toEqual({ sent: true, to: "ana" });
  });

  it("C02 a job that throws once completes on attempt 2", async () => {
    const c = await (await env()).client();
    let calls = 0;
    work(c, {}, {
      flaky: () => {
        calls++;
        if (calls === 1) {
          throw new Error("boom");
        }
        return "ok";
      },
    });
    const job = await c.enqueueAndWait({ queue: "q", kind: "flaky", backoff: fast }, { timeoutMs: 15_000 });
    expect(job.state).toBe("completed");
    expect(job.attempt).toBe(2);
    expect(job.errors.map((e) => e.error)).toEqual(["boom"]);
  });

  it("C03 a job that always throws is dead after its attempts", async () => {
    const c = await (await env()).client();
    work(c, {}, {
      broken: () => {
        throw new Error("still broken");
      },
    });
    const job = await c.enqueueAndWait({ queue: "q", kind: "broken", maxAttempts: 2, backoff: fast }, { timeoutMs: 15_000 });
    expect(job.state).toBe("dead");
    expect(job.attempt).toBe(2);
    expect(job.errors).toHaveLength(2);
  });

  it("C04 fail ends a job at once", async () => {
    const c = await (await env()).client();
    work(c, {}, {
      strict: () => {
        throw fail(new Error("bad input"));
      },
    });
    const job = await c.enqueueAndWait({ queue: "q", kind: "strict" }, { timeoutMs: 15_000 });
    expect(job.state).toBe("dead");
    expect(job.attempt).toBe(1);
    expect(job.errors[0]?.error).toBe("bad input");
  });

  it("C05 retryIn waits the delay before the next attempt", async () => {
    const c = await (await env()).client();
    const starts: number[] = [];
    let thrown = 0;
    work(c, {}, {
      later: () => {
        starts.push(Date.now());
        if (starts.length === 1) {
          thrown = Date.now();
          throw retryIn(2000, "not yet");
        }
        return null;
      },
    });
    const job = await c.enqueueAndWait({ queue: "q", kind: "later" }, { timeoutMs: 15_000 });
    expect(job.state).toBe("completed");
    expect(starts).toHaveLength(2);
    expect((starts[1] ?? 0) - thrown).toBeGreaterThanOrEqual(2000);
  });

  it("C06 busy tries again without using an attempt", async () => {
    const c = await (await env()).client();
    let calls = 0;
    work(c, {}, {
      crowded: () => {
        calls++;
        return calls === 1 ? busy(300) : "ran";
      },
    });
    const job = await c.enqueueAndWait({ queue: "q", kind: "crowded" }, { timeoutMs: 15_000 });
    expect(job.state).toBe("completed");
    expect(calls).toBe(2);
    expect(job.attempt).toBe(1);
    expect(job.errors).toHaveLength(0);
  });

  it("C07 pending parks the job and the poller finishes it", async () => {
    const c = await (await env()).client();
    const handles: string[] = [];
    work(c, {}, {
      render: {
        run: () => pending("task-81", 300),
        poll: (j) => {
          handles.push(j.handle);
          return handles.length < 3 ? pending("", 300) : { url: "https://cdn.example/v.mp4" };
        },
      },
    });
    const job = await c.enqueueAndWait({ queue: "q", kind: "render" }, { timeoutMs: 15_000 });
    expect(job.state).toBe("completed");
    expect(job.result).toEqual({ url: "https://cdn.example/v.mp4" });
    expect(handles).toEqual(["task-81", "task-81", "task-81"]);
    expect(job.attempt).toBe(1);
  });

  it("C08 a cancelled job never reaches a worker", async () => {
    const c = await (await env()).client();
    const { id } = await c.enqueue({ queue: "q", kind: "noop" });
    expect((await c.cancel(id)).state).toBe("cancelled");
    let calls = 0;
    const w = work(c, {}, {
      noop: () => {
        calls++;
      },
    });
    await sleep(1500);
    await w.stop();
    expect(calls).toBe(0);
    expect((await c.get(id)).state).toBe("cancelled");
  });

  it("C13 a full queue is refused with the server's retry delay", async () => {
    const e = await env();
    const c = await e.client();
    await c.rpc.config.putQueue({ namespace: e.ns, queue: { name: "full", maxQueued: 1 } });
    await c.enqueue({ queue: "full", kind: "k" });
    const err = await c.enqueue({ queue: "full", kind: "k" }).catch((x: unknown) => x);
    expect(err).toBeInstanceOf(RunnelError);
    const re = err as RunnelError;
    expect(re.code).toBe(Code.ResourceExhausted);
    expect(re.reason).toBe("queue_full");
    expect(re.retryAfterMs).toBeGreaterThan(0);
  });

  it("C14 a unique key makes one job", async () => {
    const c = await (await env()).client();
    const res = [];
    for (let i = 0; i < 5; i++) {
      res.push(await c.enqueue({ queue: "q", kind: "k", uniqueKey: "invoice-7" }));
    }
    expect(new Set(res.map((r) => r.id)).size).toBe(1);
    expect(res.filter((r) => r.duplicate)).toHaveLength(4);
  });

  it("C15 a flow runs in order and passes results on", async () => {
    const c = await (await env()).client();
    const order: string[] = [];
    let seen: unknown[] = [];
    work(c, { concurrency: 4 }, {
      step: async (j, ctx) => {
        const name = (j.payload as { name: string }).name;
        order.push(name);
        if (name === "d") {
          seen = (await ctx.dependencies()).map((x) => x.result).sort();
        }
        return name.toUpperCase();
      },
    });
    const step = (name: string): { queue: string; kind: string; payload: { name: string } } => ({ queue: "q", kind: "step", payload: { name } });
    const ids = await c.enqueueFlow({
      a: { job: step("a") },
      b: { job: step("b"), after: ["a"] },
      c: { job: step("c"), after: ["a"] },
      d: { job: step("d"), after: ["b", "c"] },
    });
    const d = await c.wait(ids.d?.id ?? "", { timeoutMs: 15_000 });
    expect(d.state).toBe("completed");
    expect(order[0]).toBe("a");
    expect(order[3]).toBe("d");
    expect(seen).toEqual(["B", "C"]);
  });

  it("C18 a key without the producer role cannot enqueue", async () => {
    const c = await (await env()).client(["reader"]);
    const err = await c.enqueue({ queue: "q", kind: "k" }).catch((x: unknown) => x);
    expect(err).toBeInstanceOf(RunnelError);
    expect((err as RunnelError).code).toBe(Code.PermissionDenied);
  });
});
