# `pkg/util/promutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `factory.go`,
`registry.go`, `registry_test.go`, and `BUILD.bazel`. There is no package doc,
README, fixture, benchmark, generated or platform variant, test main, or
ownership file. The local Go package is byte-identical to the pin.

The production package defines six direct-return metric factory methods, a
default factory, the Prometheus registerer surface, a registry that discards
all operations, and a fresh default registry. Its only test is
`TestNoopRegistry`.

## Rust ownership and audit result

`rust/crates/tidb-util/src/promutil/mod.rs` and `tests.rs` are the production
and test owners. Native Prometheus metric and collector types provide the
external package boundary. Where the Rust client rejects an invalid metric
descriptor during construction, the adapter panics instead of adding a
result-returning API that does not exist in Go.

The audit removed that result-returning divergence from all six `Factory`
methods and removed unused metric/error re-exports. It also removed the two
Rust-only tests for the default factory and default registry, plus the
`MustRegister` branch added to the noop-registry test. The remaining test maps
only Go `TestNoopRegistry`, including duplicate registration and unconditional
unregistration success.

## Validation

Profile: WIP; this is one completed package in the continuing package-by-
package audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/promutil` — passed.
- `go test ./pkg/util/promutil -run '^TestNoopRegistry$' -count=1` — passed.
- `cargo test -p tidb-util promutil::tests --lib --locked` — passed (1 test).
- `cargo check -p tidb-util --locked` — passed.
- `cargo test -p tidb-util --locked` — passed (640 unit tests, 3 ignored helpers, all integration and doc tests).
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: normal metric creation now has Go's direct-return contract;
  malformed native descriptors panic at the Rust client boundary.
- Compatibility: this intentionally removes Rust-only `Result` return values
  and unused re-exports; repository-wide search found no external callers.
- Performance: unchanged.
