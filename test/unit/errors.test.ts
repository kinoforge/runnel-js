import { Code, ConnectError } from "@connectrpc/connect";
import { describe, expect, it } from "vitest";

import { toRunnelError } from "../../src/errors.js";

describe("errors", () => {
  it("names the Runnel error and the retry delay", () => {
    const ce = new ConnectError("queue full", Code.ResourceExhausted, new Headers({ "runnel-error": "queue_full", "retry-after": "3" }));
    const err = toRunnelError(ce);
    expect(err.code).toBe(Code.ResourceExhausted);
    expect(err.reason).toBe("queue_full");
    expect(err.retryAfterMs).toBe(3000);
    expect(err.busy).toBe(true);
    expect(err.cause).toBe(ce);
  });

  it("leaves reason and delay empty when the server sent none", () => {
    const err = toRunnelError(new ConnectError("no", Code.PermissionDenied));
    expect(err.reason).toBeUndefined();
    expect(err.retryAfterMs).toBeUndefined();
    expect(err.busy).toBe(false);
  });
});
