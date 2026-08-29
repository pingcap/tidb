# `pkg/util/disttask` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full: `idservice.go`,
`idservice_test.go`, and `BUILD.bazel`. There is no package doc, benchmark,
fixture, generated or platform variant, README, or ownership file. The local
Go package is byte-identical to the pin.

Production behavior is the `IP:port` executor ID, first-match server lookup,
membership test, live infosync lookup, and test-only mock lookup. Discovery
errors, empty server maps, and missing IDs return the empty string.

## Rust ownership and audit result

`rust/crates/tidb-domain/src/disttask.rs` owns the complete package. It uses
the existing Rust `ServerInfo` and `serverinfo_syncer::Syncer`, matching the
Go package's domain dependencies. Rust's existing synchronous syncer boundary
has no Go-style context parameter; its server-map result and error behavior
are preserved here. The test-only function receives the mock server map
explicitly because Rust has no Go package-global mock infosync manager.

The audit removed the former `tidb-util` dependency projection and its public
discovery trait. It also removed two supplemental Rust tests absent from the
Go package. `FindServerInfo` again exposes the source's index-or-`-1` result
instead of a Rust-only `Option` contract. The remaining test is a direct port
of every `TestGenServerID` row, including the out-of-range port and IPv6 case.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/disttask` —
  passed.
- `go test ./pkg/util/disttask -count=1` — blocked before package execution by
  existing build failures in `pkg/util/hack` (`checkMapABI` is undefined) and
  gRPC transport (`http2.TrailerPrefix` is undefined).
- `cargo test --offline --locked -p tidb-domain disttask` — passed the one
  source test; 137 unrelated tests were filtered out.
- `cargo check --offline --locked -p tidb-domain -p tidb-util --all-targets` —
  passed; existing warnings remain outside this change.
- `cargo clippy --offline --locked -p tidb-domain --lib --no-deps -- -A clippy::unnecessary-map-or -A clippy::unnecessary-sort-by -D warnings` —
  passed. The allowances cover three existing `topn_slow_query` findings;
  the new package has no lint warning.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the source test, all-target compilation, and owner-crate lint
  pass. The Go test remains unverified because of the unrelated build
  failures above.
- Compatibility: the unused Rust-only `tidb_util::disttask` API is removed;
  the source-shaped owner is now `tidb_domain::disttask`.
- Performance: executor-ID formatting and linear lookup retain the source
  complexity.
