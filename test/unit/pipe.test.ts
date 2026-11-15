import { describe, expect, it } from "vitest";

import { Pipe } from "../../src/pipe.js";

describe("pipe", () => {
  it("hands out pushed values in order and ends when closed", async () => {
    const p = new Pipe<number>();
    p.push(1);
    const later = (async () => {
      const got: number[] = [];
      for await (const v of p) {
        got.push(v);
      }
      return got;
    })();
    p.push(2);
    await Promise.resolve();
    p.push(3);
    p.close();
    p.push(4);
    expect(await later).toEqual([1, 2, 3]);
  });
});
