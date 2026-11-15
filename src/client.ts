import { durationFromMs } from "@bufbuild/protobuf/wkt";
import { createClient, type Client as RpcClient, type Interceptor, type Transport } from "@connectrpc/connect";
import { createConnectTransport, createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";

import { ServerClock, type Now } from "./clock.js";
import { call } from "./errors.js";
import { AdminService } from "./gen/runnel/v1/admin_pb.js";
import { ConfigService } from "./gen/runnel/v1/config_pb.js";
import { JobService } from "./gen/runnel/v1/job_pb.js";
import { State, type Job as PbJob } from "./gen/runnel/v1/types_pb.js";
import { WorkerService } from "./gen/runnel/v1/worker_pb.js";
import { Job, newJob, pbState, type JobInput, type JobState } from "./job.js";

export interface ClientOptions {
  url: string;
  namespace: string;
  token?: string;
  protocol?: "grpc" | "connect";
  transport?: Transport;
  now?: Now;
  maxWaitMs?: number;
}

export interface Inserted {
  id: string;
  duplicate: boolean;
  error?: string;
}

export interface FlowStep {
  job: JobInput;
  after?: string[];
}

export interface ListFilter {
  queue?: string;
  kind?: string;
  group?: string;
  state?: JobState;
  cursor?: string;
  limit?: number;
}

export interface WaitOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface Rpc {
  jobs: RpcClient<typeof JobService>;
  workers: RpcClient<typeof WorkerService>;
  config: RpcClient<typeof ConfigService>;
  admin: RpcClient<typeof AdminService>;
}

const defaultMaxWaitMs = 60_000;

function bearer(token: string | undefined): Interceptor[] {
  if (!token) {
    return [];
  }
  return [
    (next) => (req) => {
      req.header.set("Authorization", `Bearer ${token}`);
      return next(req);
    },
  ];
}

function inserted(r: { id: string; duplicate: boolean; error: string }): Inserted {
  return r.error ? { id: r.id, duplicate: r.duplicate, error: r.error } : { id: r.id, duplicate: r.duplicate };
}

export class Client {
  readonly namespace: string;
  readonly rpc: Rpc;
  readonly clock: ServerClock;
  readonly #session: Http2SessionManager | undefined;
  readonly #maxWaitMs: number;

  constructor(o: ClientOptions) {
    this.namespace = o.namespace;
    this.clock = new ServerClock(o.now);
    this.#maxWaitMs = o.maxWaitMs ?? defaultMaxWaitMs;
    let transport = o.transport;
    if (!transport) {
      const interceptors = bearer(o.token);
      this.#session = new Http2SessionManager(o.url);
      transport =
        o.protocol === "connect"
          ? createConnectTransport({ baseUrl: o.url, httpVersion: "2", sessionManager: this.#session, interceptors })
          : createGrpcTransport({ baseUrl: o.url, sessionManager: this.#session, interceptors });
    }
    this.rpc = {
      jobs: createClient(JobService, transport),
      workers: createClient(WorkerService, transport),
      config: createClient(ConfigService, transport),
      admin: createClient(AdminService, transport),
    };
  }

  async enqueue(job: JobInput): Promise<Inserted> {
    const [first] = await this.enqueueMany([job]);
    if (!first) {
      throw new Error("runnel: enqueue answered no result");
    }
    return first;
  }

  async enqueueMany(jobs: JobInput[]): Promise<Inserted[]> {
    const res = await call(() => this.rpc.jobs.enqueue({ namespace: this.namespace, jobs: jobs.map(newJob) }));
    return res.results.map(inserted);
  }

  async enqueueFlow(steps: Record<string, FlowStep>): Promise<Record<string, Inserted>> {
    const res = await call(() =>
      this.rpc.jobs.enqueueFlow({
        namespace: this.namespace,
        steps: Object.entries(steps).map(([name, s]) => ({ name, job: newJob(s.job), after: s.after ?? [] })),
      }),
    );
    return Object.fromEntries(Object.entries(res.steps).map(([name, r]) => [name, inserted(r)]));
  }

  async enqueueAndWait(job: JobInput, opts: WaitOptions = {}): Promise<Job> {
    const { id } = await this.enqueue(job);
    return this.wait(id, opts);
  }

  async get(id: string): Promise<Job> {
    const res = await call(() => this.rpc.jobs.getJob({ namespace: this.namespace, id }));
    return this.#job(res.job);
  }

  async wait(id: string, opts: WaitOptions = {}): Promise<Job> {
    const until = opts.timeoutMs === undefined ? Infinity : Date.now() + opts.timeoutMs;
    for (;;) {
      const left = Math.max(0, Math.min(until - Date.now(), this.#maxWaitMs));
      const res = await call(() =>
        this.rpc.jobs.waitJob({ namespace: this.namespace, id, wait: durationFromMs(left) }, opts.signal ? { signal: opts.signal } : {}),
      );
      const job = this.#job(res.job);
      if (res.finished || job.final || Date.now() >= until) {
        return job;
      }
    }
  }

  async list(filter: ListFilter = {}): Promise<{ jobs: Job[]; nextCursor: string }> {
    const res = await call(() =>
      this.rpc.jobs.listJobs({
        namespace: this.namespace,
        queue: filter.queue ?? "",
        kind: filter.kind ?? "",
        group: filter.group ?? "",
        state: filter.state ? pbState(filter.state) : State.UNSPECIFIED,
        cursor: filter.cursor ?? "",
        limit: filter.limit ?? 0,
      }),
    );
    return { jobs: res.jobs.map((j) => new Job(j)), nextCursor: res.nextCursor };
  }

  async cancel(id: string): Promise<Job> {
    const res = await call(() => this.rpc.jobs.cancelJob({ namespace: this.namespace, id }));
    return this.#job(res.job);
  }

  async retry(id: string): Promise<Job> {
    const res = await call(() => this.rpc.jobs.retryJob({ namespace: this.namespace, id }));
    return this.#job(res.job);
  }

  async stats(): Promise<Record<string, Partial<Record<JobState, number>>>> {
    const res = await call(() => this.rpc.jobs.getStats({ namespace: this.namespace }));
    return Object.fromEntries(
      res.queues.map((q) => [q.queue, Object.fromEntries(Object.entries(q.counts).map(([s, n]) => [s, Number(n)]))]),
    );
  }

  close(): void {
    this.#session?.abort();
  }

  #job(j: PbJob | undefined): Job {
    if (!j) {
      throw new Error("runnel: answer without a job");
    }
    return new Job(j);
  }
}
