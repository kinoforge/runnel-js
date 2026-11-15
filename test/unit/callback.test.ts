import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { parseCallback, sign, SignatureError, verifyCallback } from "../../src/index.js";

interface Vector {
  name: string;
  secrets: string[];
  body: string;
  now: number;
  max_age_seconds: number;
  header: string;
  valid: boolean;
}

const vectors = (JSON.parse(readFileSync(new URL("../fixtures/callback-signatures.json", import.meta.url), "utf8")) as { vectors: Vector[] })
  .vectors;

describe("callback signatures", () => {
  it("has the shared vectors", () => {
    expect(vectors.length).toBeGreaterThanOrEqual(10);
  });

  it.each(vectors.map((v) => [v.name, v] as const))("%s", (_, v) => {
    const check = (): void => {
      verifyCallback(v.header, v.body, { secrets: v.secrets, maxAgeMs: v.max_age_seconds * 1000, now: v.now * 1000 });
    };
    if (v.valid) {
      expect(check).not.toThrow();
    } else {
      expect(check).toThrow(SignatureError);
    }
  });

  it("verifies bytes the same as text", () => {
    const body = '{"event":"job.completed","delivery":7,"job":{"id":"j1","state":"completed"}}';
    const header = sign("s", 1_800_000_000, body);
    const got = parseCallback(header, new TextEncoder().encode(body), { secrets: ["s"], now: 1_800_000_000_000 });
    expect(got.delivery).toBe(7);
    expect(got.job.state).toBe("completed");
  });
});
