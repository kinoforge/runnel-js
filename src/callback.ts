import { createHmac, timingSafeEqual } from "node:crypto";

import { SignatureError } from "./errors.js";
import type { JobState } from "./job.js";

export const defaultSignatureAgeMs = 5 * 60_000;

export interface VerifyOptions {
  secrets: string[];
  maxAgeMs?: number;
  now?: number;
}

export interface CallbackBody {
  event: string;
  delivery: number;
  job: {
    id: string;
    namespace: string;
    queue: string;
    kind: string;
    state: JobState;
    attempt: number;
    result?: unknown;
    error?: string;
    [field: string]: unknown;
  };
}

export function sign(secret: string, t: number, body: string | Uint8Array): string {
  const mac = createHmac("sha256", secret).update(`${t}.`).update(body).digest("hex");
  return `t=${t},v1=${mac}`;
}

function parse(header: string): { t: number; sigs: string[] } {
  let t = 0;
  const sigs: string[] = [];
  for (const part of header.split(",")) {
    const [k, ...rest] = part.trim().split("=");
    const v = rest.join("=");
    if (k === "t") {
      if (!/^[+-]?\d+$/.test(v)) {
        throw new SignatureError("runnel: bad callback signature: bad time");
      }
      t = Number(v);
    } else if (k === "v1") {
      sigs.push(v);
    }
  }
  if (t === 0 || sigs.length === 0) {
    throw new SignatureError("runnel: bad callback signature: missing t or v1");
  }
  return { t, sigs };
}

function same(a: string, b: string): boolean {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}

export function verifyCallback(header: string, body: string | Uint8Array, o: VerifyOptions): void {
  const maxAge = o.maxAgeMs && o.maxAgeMs > 0 ? o.maxAgeMs : defaultSignatureAgeMs;
  const { t, sigs } = parse(header);
  const age = (o.now ?? Date.now()) - t * 1000;
  if (Math.abs(age) > maxAge) {
    throw new SignatureError(`runnel: bad callback signature: signed ${Math.round(age / 1000)}s from now`);
  }
  for (const secret of o.secrets) {
    if (secret === "") {
      continue;
    }
    const want = sign(secret, t, body);
    if (sigs.some((sig) => same(`t=${t},v1=${sig}`, want))) {
      return;
    }
  }
  throw new SignatureError("runnel: bad callback signature: no secret matches");
}

export function parseCallback(header: string, body: string | Uint8Array, o: VerifyOptions): CallbackBody {
  verifyCallback(header, body, o);
  const text = typeof body === "string" ? body : new TextDecoder().decode(body);
  return JSON.parse(text) as CallbackBody;
}
