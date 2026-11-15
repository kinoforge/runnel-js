# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
