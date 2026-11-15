import { createServer } from "node:http";
import type { AddressInfo } from "node:net";

import { describe, expect, it } from "vitest";

import { type CallbackBody, Client, DeadlineError, LeaseLostError, parseCallback, ShutdownError } from "../../src/index.js";
import { sleep } from "../../src/pipe.js";
import { blockUntilAborted, callbackHost, dropping, env, modes, until, url, work } from "./env.js";

const fast = { baseMs: 100, factor: 1, jitter: 0 };

async function deadlineCheck(c: Client): Promise<void> {
  let reason: unknown;
  let at = 0;
  work(c, {}, {
    slow: (_, ctx) => {
      ctx.signal.addEventListener("abort", () => {
        reason = ctx.signal.reason;
        at = Date.now();
      });
      return blockUntilAborted(ctx.signal);
    },
  });
  const deadline = new Date(Date.now() + 2000);
  const { id } = await c.enqueue({ queue: "q", kind: "slow", deadline });
  const job = await c.wait(id, { timeoutMs: 15_000 });
  expect(job.state).toBe("expired");
  expect(reason).toBeInstanceOf(DeadlineError);
  expect(Math.abs(at - deadline.getTime())).toBeLessThan(500);
}

describe("contract: worker", () => {
  it("C09 the handler is cancelled at the deadline and the job expires", async () => {
    await deadlineCheck(await (await env()).client());
  });

  it("C19 deadlines use the server's clock when the worker's clock is off", async () => {
    await deadlineCheck(await (await env()).client(undefined, { now: () => Date.now() + 30_000 }));
  });

  it("C17 a signed callback arrives once and verifies", async () => {
    const secret = "whsec_contract";
    const e = await env({ callbackSecret: secret });
    const c = await e.client();
    const got: CallbackBody[] = [];
    const srv = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (b: Buffer) => chunks.push(b));
      req.on("end", () => {
        got.push(parseCallback(req.headers["runnel-signature"] as string, Buffer.concat(chunks), { secrets: [secret] }));
        res.end();
      });
    });
    await new Promise<void>((resolve) => srv.listen(0, "0.0.0.0", resolve));
    try {
      const port = (srv.address() as AddressInfo).port;
      work(c, {}, { notify: () => ({ ok: true }) });
      const { id } = await c.enqueue({ queue: "q", kind: "notify", callbackUrl: `http://${callbackHost}:${port}/hook` });
      await until(
        () => Promise.resolve(got.length),
        (n) => n > 0,
      );
      await sleep(1500);
      expect(got).toHaveLength(1);
      expect(got[0]?.job.id).toBe(id);
      expect(got[0]?.job.state).toBe("completed");
    } finally {
      srv.close();
    }
  });

  describe.each(modes)("%s", (mode) => {
    it("C10 heartbeats keep a long job", async () => {
      const e = await env();
      const c = await e.client();
      let runs = 0;
      const long = (): Promise<string> => {
        runs++;
        return sleep(4000).then(() => "done");
      };
      work(c, { mode, leaseMs: 1500 }, { long });
      work(await e.client(), { mode, leaseMs: 1500 }, { long });
      const job = await c.enqueueAndWait({ queue: "q", kind: "long" }, { timeoutMs: 15_000 });
      expect(job.state).toBe("completed");
      expect(job.attempt).toBe(1);
      expect(job.errors).toHaveLength(0);
      expect(runs).toBe(1);
    });

    it("C11 a lease that runs out is reported lost and the job runs again", async () => {
      const e = await env();
      const c = new Client({ url, namespace: e.ns, transport: dropping(await e.token(["producer", "reader", "worker"]), "Extend") });
      let reason: unknown;
      let calls = 0;
      work(c, { mode, leaseMs: 1500 }, {
        held: (_, ctx) => {
          calls++;
          if (calls > 1) {
            return "second";
          }
          return blockUntilAborted(ctx.signal).catch((err: unknown) => {
            reason = err;
            throw err;
          });
        },
      });
      const job = await c.enqueueAndWait({ queue: "q", kind: "held", backoff: fast }, { timeoutMs: 15_000 });
      expect(reason).toBeInstanceOf(LeaseLostError);
      expect(job.state).toBe("completed");
      expect(job.attempt).toBe(2);
      expect(job.errors).toHaveLength(1);
    });

    it("C12 shutdown gives back held jobs untouched", async () => {
      const c = await (await env()).client();
      let reason: unknown;
      const w = work(c, { mode, concurrency: 1, shutdownTimeoutMs: 300 }, {
        block: (_, ctx) =>
          blockUntilAborted(ctx.signal).catch((err: unknown) => {
            reason = err;
            throw err;
          }),
      });
      const ids = (await c.enqueueMany([1, 2, 3].map(() => ({ queue: "q", kind: "block" })))).map((r) => r.id);
      await until(
        () => Promise.resolve(w.worker.running),
        (n) => n === 1,
      );
      await w.stop();
      expect(reason).toBeInstanceOf(ShutdownError);
      for (const id of ids) {
        const j = await c.get(id);
        expect(j.state).toBe("waiting");
        expect(j.attempt).toBe(0);
      }
    });

    it("C16 concurrency caps jobs running at once", async () => {
      const c = await (await env()).client();
      let now = 0;
      let most = 0;
      work(c, { mode, concurrency: 2 }, {
        task: async () => {
          now++;
          most = Math.max(most, now);
          await sleep(300);
          now--;
        },
      });
      const ids = (await c.enqueueMany([1, 2, 3, 4, 5, 6].map(() => ({ queue: "q", kind: "task" })))).map((r) => r.id);
      for (const id of ids) {
        expect((await c.wait(id, { timeoutMs: 15_000 })).state).toBe("completed");
      }
      expect(most).toBe(2);
    });
  });
});
