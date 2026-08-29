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
CPU times remain signed `i64` nanosecond counts, matching Go's
`time.Duration`, including negative values and wrapping addition. SQL IDs use
explicit wrapping addition, matching Go's `uint64` overflow. The two
statement-summary implementations retain that signed representation through
aggregation and SQL/JSON output instead of converting it to Rust's unsigned
`std::time::Duration`.

The audit changed every mutex acquisition to recover the protected state after
a poisoned owner, removing a Rust-only failure mode that Go's `sync.Mutex`
does not have. The four broad Rust-only unit tests were removed because this
Go package has no test artifact. One focused regression protects the signed
and wrapping behavior after the previous unsigned representation was found to
diverge from Go.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/ppcpuusage -count=1` — passed (`[no test files]`).
- `cargo test --offline --locked -p tidb-util ppcpuusage` — passed.
- `cargo test --offline --locked -p tidb-stmtsummary` — passed (44 unit tests;
  doc tests contain no cases).
- `cargo check --offline --locked -p tidb-util -p tidb-stmtsummary
  --all-targets` — passed.
- Strict owner-only clippy passed for `tidb-util --lib` and
  `tidb-stmtsummary --all-targets`; warnings printed while compiling dependency
  crates are pre-existing and outside these owner-only lint scopes.
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: mutex panic recovery matches Go's non-poisoning lock, and CPU
  durations no longer lose Go's signed/wrapping behavior.
- Compatibility: CPU-time fields now use their source nanosecond representation
  rather than Rust's semantically different unsigned duration type.
- Performance: integer accumulation remains constant-time and allocation-free.
