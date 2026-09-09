# Server connection and lifecycle parity ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory the complete Go server connection/lifecycle package surfaces, restore
Go-master behavior and focused regressions, validate with the Ready profile,
and publish one meaningful batch to `hparser-integration`.

## Progress

- [x] (2026-09-02) Pulled the latest `origin/hparser-integration` tip and read
  all production, test, fixture, generated/platform, and build artifacts in
  the server, advertised-status, HTTP, common-test, TLS, variable, and
  real-TiKV package surfaces before editing.
- [x] (2026-09-02) Restored connection liveness, prefetch interruption,
  ChangeUser rollback, prepared long-data quota handling, profiling logs,
  advertised-status lifecycle, and connection-event logging.
- [x] (2026-09-02) Added focused unit, HTTP, common-server, and real-TiKV
  disconnect regressions; recorded fail-before evidence for the explicit
  transaction disconnect case.
- [x] (2026-09-02) Ran the Ready validation profile and the required Bazel
  preparation attempt; Bazel is unavailable locally.
- [x] (2026-09-02) Staged only this server batch, committed it, pushed to
  `origin/hparser-integration`, verified the remote SHA, and fast-forward pulled.
- [ ] Continue the rolling audit with the next unrecorded Go package.

## Constraints

Connection cancellation, transaction cleanup, and prepared-statement memory
accounting are correctness-sensitive. Keep the Go server as the owner until a
dependency-closed Rust SQL server exists; do not claim a partial Rust port as a
complete package.

## Outcome

The evidence is recorded in
`rust/testport/receipts/server_connection.md`; this ExecPlan remains open while
the repository-wide rolling audit continues.
