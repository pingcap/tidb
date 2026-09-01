# `pkg/util/ppcpuusage` — complete Go-master package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package has exactly two artifacts, both read in full: `cpuusages.go` and
`BUILD.bazel`. They define the two-duration value, mutex-protected per-SQL
state, SQL-ID-gated TiDB accumulation, unconditional TiKV accumulation,
wrapping ID allocation, and CPU-time reset. There is no Go test, test-support
file, package doc, README, fixture, benchmark, generated or platform variant,
or ownership file. The checkout is byte-identical to the pin.
The inventory is exactly 94 lines: 8 build lines and 86 production lines.

## Rust ownership and audit result

`rust/crates/tidb-util/src/ppcpuusage.rs` is the production owner. Its
`CpuUsages` and `SqlCpuUsages` preserve the source state and method behavior;
CPU times remain signed `i64` nanosecond counts, matching Go's
`time.Duration`, including negative values and wrapping addition. SQL IDs use
explicit wrapping addition, matching Go's `uint64` overflow. The two
statement-summary implementations retain that signed representation through
aggregation and SQL/JSON output instead of converting it to Rust's unsigned
`std::time::Duration`.

The audit changed every mutex acquisition to recover the protected state after
a poisoned owner, removing a Rust-only failure mode that Go's `sync.Mutex`
does not have. All Rust-only unit tests and the `must_use` diagnostic were
removed because this Go package has no test artifact or equivalent diagnostic.
The current living ExecPlan is
`rust/docs/operations/util-ppcpuusage-audit-execplan.md`.

## Validation

Profile: **Ready** for this docs-only authority refresh. No Go, Rust, Bazel,
or module source changed, so `make bazel_prepare` is not required.

- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/ppcpuusage` — passed.
- Pinned-Go `go test ./pkg/util/ppcpuusage -count=1` — passed in the current and exact detached Go-master worktrees (`[no test files]`).
- With the pinned OpenSSL environment, `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util ppcpuusage --lib -- --test-threads=1` — passed with zero tests, matching Go.
- With the same environment, `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --lib` — passed for the production consumer.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` and the batch diff checks — passed.

The focused Rust commands emitted only existing workspace warnings. Full
workspace tests, all-target consumer tests, and Bazel execution remain outside
this leaf receipt.

## Risk

- Correctness: mutex panic recovery matches Go's non-poisoning lock, and CPU
  durations no longer lose Go's signed/wrapping behavior.
- Compatibility: CPU-time fields now use their source nanosecond representation
  rather than Rust's semantically different unsigned duration type.
- Performance: integer accumulation remains constant-time and allocation-free.
