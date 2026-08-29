# `pkg/util/ppcpuusage` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `cpuusages.go` and
`BUILD.bazel`. They define the two-duration value, mutex-protected per-SQL
state, SQL-ID-gated TiDB accumulation, unconditional TiKV accumulation,
wrapping ID allocation, and CPU-time reset. There is no Go test, test-support
file, package doc, README, fixture, benchmark, generated or platform variant,
or ownership file. The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/ppcpuusage.rs` is the production owner. Its
`CpuUsages` and `SqlCpuUsages` preserve the source state and method behavior;
Rust's standard `Duration` is the native representation used by all existing
CPU-time consumers. SQL IDs use explicit wrapping addition, matching Go's
`uint64` overflow.

The audit changed every mutex acquisition to recover the protected state after
a poisoned owner, removing a Rust-only failure mode that Go's `sync.Mutex`
does not have. The four Rust-only unit tests were removed because this Go
package has no test artifact.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/ppcpuusage -count=1` — passed (`[no test files]`).
- `cargo check -p tidb-util -p tidb-stmtsummary --locked` — passed.
- `cargo test -p tidb-stmtsummary --locked` — passed (44 unit tests and doc
  tests).
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: mutex panic recovery now matches Go's non-poisoning lock.
- Compatibility: only Rust-only tests were removed; production APIs remain.
- Performance: unchanged.
