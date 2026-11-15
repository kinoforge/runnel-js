import { create } from "@bufbuild/protobuf";
import { durationMs, timestampFromDate } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";

import { JobSchema, State } from "../../src/gen/runnel/v1/types_pb.js";
import { Job, newJob, pbState } from "../../src/job.js";

describe("jobs", () => {
  it("sends payloads as JSON bytes and raw bytes untouched", () => {
    expect(new TextDecoder().decode(newJob({ queue: "q", kind: "k", payload: { a: 1 } }).payload)).toBe('{"a":1}');
    const raw = new TextEncoder().encode('{"n":12345678901234567}');
    expect(newJob({ queue: "q", kind: "k", payload: raw }).payload).toBe(raw);
    expect(newJob({ queue: "q", kind: "k" }).payload.length).toBe(0);
  });

  it("turns milliseconds into durations", () => {
    const j = newJob({ queue: "q", kind: "k", delayMs: 1500, backoff: { baseMs: 100, factor: 3 } });
    expect(j.delay && durationMs(j.delay)).toBe(1500);
    expect(j.backoff?.base && durationMs(j.backoff.base)).toBe(100);
    expect(j.backoff?.factor).toBe(3);
    expect(j.timeout).toBeUndefined();
  });

  it("reads a job", () => {
    const at = new Date("2026-11-15T10:00:00Z");
    const j = new Job(
      create(JobSchema, { id: "j1", state: State.POLLING, handle: "task-1", payload: new TextEncoder().encode('{"x":true}'), deadline: timestampFromDate(at) }),
    );
    expect(j.state).toBe("polling");
    expect(j.polling).toBe(true);
    expect(j.final).toBe(false);
    expect(j.payload).toEqual({ x: true });
    expect(j.result).toBeUndefined();
    expect(j.deadline?.getTime()).toBe(at.getTime());
  });

  it("maps every state both ways", () => {
    for (const s of ["blocked", "waiting", "running", "polling", "completed", "dead", "cancelled", "expired"] as const) {
      expect(new Job(create(JobSchema, { state: pbState(s) })).state).toBe(s);
    }
  });
});
