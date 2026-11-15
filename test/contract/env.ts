import { randomBytes } from "node:crypto";

import type { Interceptor, Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { onTestFinished } from "vitest";

import { Client, type ClientOptions, type Handler, type Handlers, Worker, type WorkerOptions } from "../../src/index.js";
import { sleep } from "../../src/pipe.js";

export const url = process.env.RUNNEL_URL ?? "http://127.0.0.1:8700";
export const callbackHost = process.env.RUNNEL_CALLBACK_HOST ?? "127.0.0.1";
const adminToken = process.env.RUNNEL_ADMIN_TOKEN ?? "contract-admin-token-0123456789";

export const quiet = process.env.RUNNEL_DEBUG ? console : { error: (): void => undefined, warn: (): void => undefined };

export interface Env {
  ns: string;
  client(roles?: string[], extra?: Partial<ClientOptions>): Promise<Client>;
  token(roles: string[]): Promise<string>;
}

export async function env(o: { callbackSecret?: string } = {}): Promise<Env> {
  const ns = `c${randomBytes(5).toString("hex")}`;
  const admin = new Client({ url, namespace: ns, token: adminToken });
  const clients: Client[] = [admin];
  onTestFinished(() => {
    for (const c of clients) {
      c.close();
    }
  });
  await admin.rpc.admin.putNamespace({ name: ns, ...(o.callbackSecret ? { callbackSecret: o.callbackSecret } : {}) });
  const token = async (roles: string[]): Promise<string> =>
    (await admin.rpc.admin.createKey({ namespace: ns, name: "contract", roles })).token;
  return {
    ns,
    token,
    async client(roles = ["producer", "reader", "worker", "admin"], extra = {}) {
      const c = new Client({ url, namespace: ns, token: await token(roles), ...extra });
      clients.push(c);
      return c;
    },
  };
}

export function dropping(token: string, method: string): Transport {
  const auth: Interceptor = (next) => (req) => {
    req.header.set("Authorization", `Bearer ${token}`);
    return next(req);
  };
  const drop: Interceptor = (next) => (req) => (req.method.name === method ? Promise.reject(new Error(`${method} dropped`)) : next(req));
  return createGrpcTransport({ baseUrl: url, interceptors: [auth, drop] });
}

export interface Running {
  worker: Worker;
  stop(): Promise<void>;
}

export function work(c: Client, o: Partial<WorkerOptions>, handlers: Record<string, Handler | Handlers>): Running {
  const worker = new Worker(c, { queues: ["q"], leaseMs: 3000, shutdownTimeoutMs: 5000, claimWaitMs: 1000, retryMinMs: 50, logger: quiet, ...o });
  for (const [kind, h] of Object.entries(handlers)) {
    worker.handle(kind, h);
  }
  const ctl = new AbortController();
  const done = worker.run(ctl.signal);
  let stopped = false;
  const stop = async (): Promise<void> => {
    if (!stopped) {
      stopped = true;
      ctl.abort();
      await done;
    }
  };
  onTestFinished(stop);
  return { worker, stop };
}

export async function until<T>(fn: () => Promise<T>, ok: (v: T) => boolean, ms = 20_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (ok(v) || Date.now() > end) {
      return v;
    }
    await sleep(100);
  }
}

export function blockUntilAborted(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    signal.addEventListener("abort", () => {
      reject(signal.reason as Error);
    });
  });
}

export const modes = ["stream", "poll"] as const;
