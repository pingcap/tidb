# `pkg/util/domainutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `repair_vars.go` and
`BUILD.bazel`. There is no package doc, test, test harness, benchmark, fixture,
generated input/output, platform variant, README, or ownership file. The local
Go package is byte-identical to the pin.

Production behavior is one process-global, mutex-protected repair registry:
repair mode, a lowercased table-name list, and a hash map of shallow-copied
database metadata containing quarantined table pointers. It exposes the seven
registry operations plus the two integer session-context keys and their string
rendering.

## Rust ownership and audit result

`rust/crates/tidb-domain/src/domainutil.rs` is the sole owner. The audit deleted
a second independent implementation and its six Rust-only tests from
`tidb-exec`, removed two more Rust-only owner tests, and removed the extra
`as_str` convenience in favor of the source-shaped string formatting trait.

The retained owner now uses hash maps/sets rather than a Rust-only sorted-map
policy, and reuses `tidb-mysql`'s Go `strings.ToLower` implementation rather
than Rust full-case expansion. Server startup publishes `repair-mode` and
`repair-table-list` into this one global after installing the effective config,
matching `cmd/tidb-server/main.go`.

Go's infoschema, planner, and DDL packages consume this registry. Their Rust
integration remains owned by those separate package audits; this receipt does
not claim those packages complete.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/domainutil` — passed.
- `go test ./pkg/util/domainutil -count=1` — blocked before package execution by existing build failures: missing `checkMapABI` in `pkg/util/hack` and missing `http2.TrailerPrefix` in gRPC transport.
- `cargo check --offline --locked -p tidb-domain -p tidb-exec -p tidb-server` — passed with existing warnings.
- `cargo test --offline --locked -p tidb-domain --no-run` — passed with an existing warning.
- `rustfmt --edition 2021 --check crates/tidb-domain/src/domainutil.rs crates/tidb-server/src/lib.rs crates/tidb-exec/src/lib.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: removes split-brain repair state and restores Go case folding
  and unordered map behavior. The safe Rust getter returns owned collection
  handles rather than exposing storage after releasing a lock.
- Compatibility: deletes the unused `tidb-exec::domainutil` API and Rust-only
  tests; the canonical owner remains public from `tidb-domain`.
- Performance: removes one unused global registry; active operations retain
  the same lock and expected constant-time map/set behavior as Go.
