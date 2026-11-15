# Runnel for TypeScript and Node

The TypeScript and Node client and worker for [Runnel](https://github.com/kinoforge/runnel), the job queue. It talks to `runnel serve` over gRPC, so workers get jobs pushed to them as soon as they have room.

Needs Node 22.12 or later and Runnel 0.0.4 or later. The package is ESM; `require()` works too on Node 22.12 and later.

```sh
npm install @kinoforge/runnel
```

Until the first release is on npm, install it from GitHub: `npm install github:kinoforge/runnel-js`. Newer npm asks you to allow its `prepare` script, which builds it.

## Enqueue a job

```ts
import { Client } from "@kinoforge/runnel";

const runnel = new Client({ url: "http://runnel.internal:8700", namespace: "shop", token: process.env.RUNNEL_KEY });

const { id } = await runnel.enqueue({ queue: "emails", kind: "welcome", payload: { to: "ana@example.com" } });

const job = await runnel.enqueueAndWait({ queue: "emails", kind: "welcome", payload: { to: "ana@example.com" } }, { timeoutMs: 30_000 });
console.log(job.state, job.result);
```

The key needs the `producer` role to enqueue and `reader` to read jobs.

## Run a worker

```ts
import { Client, Worker } from "@kinoforge/runnel";

const runnel = new Client({ url: "http://runnel.internal:8700", namespace: "shop", token: process.env.RUNNEL_KEY });
const worker = new Worker(runnel, { queues: ["emails"], concurrency: 10 });

worker.handle("welcome", async (job, { signal }) => {
  const { to } = job.payload as { to: string };
  await mailer.send(to, { signal });
  return { sent: true };
});

const stop = new AbortController();
process.once("SIGTERM", () => stop.abort());
await worker.run(stop.signal);
```

The key needs the `worker` role. What the handler returns becomes the job's result. If it throws, the job is retried after the backoff, and is `dead` once its attempts run out.

The worker renews the leases of its running jobs every third of `leaseMs`. On shutdown it stops taking jobs, waits up to `shutdownTimeoutMs` for running ones, then aborts them; an aborted job is given back untouched, without using an attempt.

## Outcomes

Return or throw one of these to choose what happens next:

| Call | What happens |
|---|---|
| `fail(err)` | the job is `dead` now, with no more retries |
| `retryIn(ms, err)` | the job runs again after exactly `ms` |
| `busy(ms)` | the job runs again after `ms`, without using an attempt |
| `pending(handle, ms)` | the job waits for outside work; its poller runs after `ms` |

```ts
import { busy, fail, pending, retryIn } from "@kinoforge/runnel";

worker.handle("charge", async (job) => {
  const res = await payments.charge(job.payload);
  if (res.status === 429) throw busy(res.retryAfterMs);
  if (res.status === 400) throw fail(res.error);
  return res.receipt;
});
```

## Outside work that takes a while

Give the kind a `run` that starts the work and a `poll` that checks on it. `job.handle` holds what `run` passed to `pending`:

```ts
worker.handle("video.render", {
  run: async (job) => pending((await renderer.start(job.payload)).id, 10_000),
  poll: async (job) => {
    const task = await renderer.status(job.handle);
    if (task.running) return pending("", 10_000);
    if (task.lost) throw fail("renderer lost the task");
    return { url: task.url };
  },
});
```

## Cancellation

Every handler gets a `signal`. It aborts with a reason you can check:

| Reason | When |
|---|---|
| `TimeoutError` | the attempt ran longer than the job's `timeoutMs`; the job is retried |
| `DeadlineError` | the job's `deadline` passed; the job is `expired` |
| `LeaseLostError` | the job was cancelled, or its lease ran out; the result is dropped |
| `ShutdownError` | the worker is stopping; the job is given back |

Deadlines and leases are measured on the server's clock. The worker learns it from every answer, so a worker whose own clock is off still stops on time.

## Errors

Calls throw `RunnelError` with the gRPC `code`, the Runnel `reason` and, when the server is full, `retryAfterMs`:

```ts
import { Code, RunnelError } from "@kinoforge/runnel";

try {
  await runnel.enqueue({ queue: "emails", kind: "welcome" });
} catch (err) {
  if (err instanceof RunnelError && err.reason === "queue_full") {
    await new Promise((wake) => setTimeout(wake, err.retryAfterMs ?? 1000));
  }
}
```

`reason` is one of `invalid`, `not_found`, `stale`, `queue_full`, `group_full`, `no_space`, `dependency`, `conflict`, `final` and `not_final`.

## Callbacks

When a namespace has a callback secret, Runnel signs every callback. Check it before trusting the body:

```ts
import { parseCallback } from "@kinoforge/runnel";

app.post("/hooks/runnel", express.raw({ type: "*/*" }), (req, res) => {
  const body = parseCallback(req.header("runnel-signature") ?? "", req.body, { secrets: [process.env.RUNNEL_CALLBACK_SECRET ?? ""] });
  console.log(body.event, body.job.id, body.job.state);
  res.sendStatus(204);
});
```

Pass the raw body, not parsed JSON. Give two secrets while you rotate them. A signature older than 5 minutes is refused; set `maxAgeMs` to change that. A delivery can arrive twice: dedupe by `body.delivery`.

## Options

`Client`:

| Option | Default | Meaning |
|---|---|---|
| `url` | | the server, `http://` or `https://` |
| `namespace` | | the namespace every call works in |
| `token` | | a namespace key `rnk_…`, or the admin token |
| `protocol` | `grpc` | `connect` to use the Connect protocol instead |
| `transport` | | your own Connect transport, for TLS options or interceptors |
| `maxWaitMs` | `60000` | the longest single wait `wait` asks the server for |

`Worker`:

| Option | Default | Meaning |
|---|---|---|
| `queues` | | the queues to take jobs from |
| `concurrency` | `10` | jobs run at once |
| `leaseMs` | `30000` | how long a job is held between heartbeats |
| `shutdownTimeoutMs` | `30000` | how long `run` waits for running jobs when stopping |
| `mode` | `stream` | `stream` has jobs pushed over one gRPC stream; `poll` claims them with long-polling calls |
| `claimWaitMs` | `20000` | in `poll` mode, how long one claim waits for jobs |
| `id` | host, pid and a random suffix | the worker's name in leases |
| `finishTries` | `6` | tries to report an outcome when the server is unreachable |
| `retryMinMs`, `retryMaxMs` | `200`, `5000` | the backoff between those tries and between reconnects |
| `logger` | `console` | where errors and warnings go |

## Lower level

`client.rpc` holds the generated clients for every call of the [gRPC API](https://github.com/kinoforge/runnel/blob/main/docs/grpc-api.md), including settings and admin. The messages are exported from `@kinoforge/runnel/rpc`.

## Tests

`npm test` runs the unit tests. `npm run test:contract` runs the [shared SDK scenarios](https://github.com/kinoforge/runnel/tree/main/sdk-contract) against a real server; CI starts one in Docker.

## License

Apache 2.0.
