# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- When the server hands a job out again while this worker still runs an earlier attempt of it, for example after a missed heartbeat, the earlier run is ended as lease lost. Before, it was forgotten and kept running, and `stop()` waited for it forever.
- After a heartbeat, a job's lease is renewed by the lease the server granted at claim time, at most the worker's `lease`. A server whose `max_lease` is shorter than the worker's `lease` no longer leaves the worker thinking it holds a job the server has taken back.
- A handler that returns normally after its timeout, deadline or the worker's shutdown ended its context is reported as that timeout, deadline or release, not as done.
- Stopping an idle polling worker no longer waits for its long-poll claim (up to 20s) or a reconnect backoff to end.
- A worker refuses a `concurrency` or `lease` that is not positive.
- Jobs handed back while the worker shuts down are awaited before `run()` returns, and the heartbeat timer stops even when shutdown throws.
- A thrown value that cannot be turned into JSON, such as a cycle or a `BigInt`, no longer crashes the worker.
- A job state this version does not know reads as unknown instead of failing.
- A heartbeat that is still running when the next one is due is followed by another at once, as in the Go worker. Before, the next beat was skipped, so one slow `Extend` could lose every running job's lease.
- When a job comes back to the same worker while its earlier run is still ending, the earlier run no longer removes the new one, which kept its heartbeats.
- A zero server time is ignored instead of moving the server clock to 1970.

## [0.1.0-beta.1] - 2026-11-15

### Added

- `Client`: enqueue one job, a batch or a flow, enqueue and wait, get, wait, list, cancel, retry and stats, over gRPC or the Connect protocol.
- `Worker`: handlers per kind with pollers for outside work, concurrency, jobs pushed over the `Work` stream or claimed by long polling, heartbeats, a lease watchdog, timeouts and deadlines on the server's clock, and graceful shutdown that gives unfinished jobs back.
- Outcome helpers `fail`, `retryIn`, `busy` and `pending`.
- `RunnelError` with the gRPC code, the Runnel reason and the retry delay.
- `verifyCallback` and `parseCallback` for signed callbacks, checked against the shared test vectors.
- The generated clients and messages for the whole Runnel API, in `client.rpc` and `@kinoforge/runnel/rpc`.
- The shared SDK contract scenarios C01 to C19, run in CI against the Runnel image.

[Unreleased]: https://github.com/kinoforge/runnel-js/compare/v0.1.0-beta.1...HEAD
[0.1.0-beta.1]: https://github.com/kinoforge/runnel-js/releases/tag/v0.1.0-beta.1
