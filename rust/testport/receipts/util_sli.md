# `pkg/util/sli` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full:

- `sli.go` — the transaction accumulator, validity and small-transaction
  rules, metric selection, reset, and string rendering;
- `BUILD.bazel` — one production library target and no package-local test.

There is no `doc.go`, README, test, fixture, benchmark, generated/platform
variant, ownership file, or additional harness. The checkout is byte-identical
to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sli.rs` is the sole package owner. The audit moved
the implementation out of the unrelated `tidb-exec` compatibility crate and
removed its Rust-only observation return type, public field accessors, public
constructor, per-instance failpoint switch, clone/equality surface, synthetic
KV-size fixtures, and supplementary tests that did not execute Go's session,
executor, or storage integration.

The retained implementation has Go's zero-valued accumulator and exported
operations. `FinishExecuteStmt` now reports directly to the process Prometheus
histograms and returns nothing. The histograms use the exact Go metric names,
help text, exponential buckets, namespace, and subsystem.

Go's production integration is outside this package: `LazyTxn` owns the value,
executor completion supplies commit/scan details, insert-select invalidates it,
and the server supplies elapsed time and transaction state. Rust does not yet
have a production path that exposes exact commit write bytes/keys from its
storage transaction, so this package deliberately does not invent estimates or
wire a partial metric that could report false throughput. That dependency is
an explicit integration prerequisite, not behavior added to `pkg/util/sli`.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/sli` — blocked before this package compiled by the
  workspace's existing `google.golang.org/grpc/internal/transport` /
  `http2.TrailerPrefix` dependency mismatch.
- `cargo test -p tidb-util --locked` — passed.
- `cargo check -p tidb-exec --lib --locked` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; the package no longer exposes a synthetic substitute
  for metric reporting and keeps Go's accumulation, validity, threshold,
  reset, and formatting behavior.
- Compatibility: the unused `tidb-exec` module and Rust-only APIs are removed.
- Performance: metric initialization occurs once; steady-state reporting is a
  direct Prometheus histogram observation as in Go.
