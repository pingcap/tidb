# `pkg/util/deadlockhistory` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains `BUILD.bazel`, production
`deadlock_history.go`, source test `deadlock_history_test.go`, and package
harness `main_test.go`. It has four top-level source tests and no `doc.go`,
fixture, generated source, benchmark, fuzz target, example, platform variant,
or build-tagged production variant. The checkout package is byte-identical to
the pin.

`main_test.go` supplies Go's common test setup and goroutine leak checker;
Rust's aggregate unit test does not start Go runtime workers and needs no
package setup analogue. `BUILD.bazel` maps to the existing `tidb-executor`
production owner and the generated aggregate test target.

## Rust ownership and integration

`tidb-executor::deadlock_history` owns only the package behavior: column
constants, wait-chain and record values, column-to-datum conversion, the
thread-safe bounded history, the process-global history, and conversion from
TiKV's deadlock error. History IDs start at one, remain monotonic across clear
and resize, do not advance at zero capacity, preserve the newest entries when
shrunk, and wrap through native unsigned arithmetic. Snapshots retain shared
ownership of the stored record, the Rust equivalent of Go returning the same
record pointer.

The Rust-only package-level information-schema row renderer, key decoder,
retryable collection policy, server configuration, and live recording entry
point were removed. As in Go, executor code now owns retryable admission and
calls `ErrDeadlockToDeadlockRecord` before pushing the global history. The
ordinary session information-schema reader owns `KEY_INFO` decoding and
`CURRENT_SQL_DIGEST_TEXT` lookup. The latter now reads the cumulative
statement-summary map, replacing Rust's former unconditional SQL NULL.

The package test surface is the same four source-shaped identities:
collection, datum conversion, deadlock-error conversion, and resize. The
supplemental Rust concurrency, snapshot, and process-policy test identities
were removed or folded into their Go owner. The unregistered duplicate
`tests_deadlocks_table_source.rs` file was deleted; the ordinary session SQL
test remains the consumer regression and now verifies digest-text retrieval.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/deadlockhistory` — passed.
- `GOTOOLCHAIN=go1.25.10 go test ./pkg/util/deadlockhistory -count=1` — passed; four tests.
- `cargo check -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server` — passed.
- `cargo test -p tidb-executor --lib deadlock_history::tests` — passed; four tests.
- `cargo test -p tidb-session --lib deadlocks_table_exposes_package_rows_and_requires_process` — passed.
- `cargo test -p tidb-exec --lib a_live_deadlock_failure_is_recorded_before_it_reaches_sql` — passed.
- `cargo test -p tidb-exec --lib retryable_deadlock_history_obeys_the_process_policy` — passed.
- `cargo fmt -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server` — passed.
- `git diff --check` — passed.

No Go source, Go test, Bazel metadata, or Go module file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: improved; package API/behavior matches its Go owner and the
  ordinary information-schema path now resolves digest text and key metadata.
- Compatibility: deadlock IDs, retention, datum nullability, digest encoding,
  retryable admission, PROCESS access, and DEADLOCKS row values follow the
  pinned Go boundaries.
- Performance: history operations remain mutex-protected and bounded; the
  information-schema reader snapshots once and borrows the catalog once per
  query.
- Not verified locally: a live TiKV deadlock followed by a distributed
  `CLUSTER_DEADLOCKS` read. Package tests and local executor/session consumer
  regressions cover the pinned package and ordinary local reader boundary.
