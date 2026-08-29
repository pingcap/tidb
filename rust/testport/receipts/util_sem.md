# `pkg/util/sem` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The root package has exactly four artifacts, all read in full: `sem.go`,
`sem_test.go`, `main_test.go`, and `BUILD.bazel`. They define the SEM enable
state, sysvar-default changes, schema/table/status/sysvar visibility rules,
restricted privilege recognition, five unit tests, and the common test
harness. There is no package doc, README, fixture, benchmark,
generated/platform variant, or ownership file. `pkg/util/sem/compat` and
`pkg/util/sem/v2` are separate Go packages and are not part of this claim. The
checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sem.rs` is the production owner. Its policy tables,
atomic enable state, Go-compatible schema folding, privilege rule, hostname
restore, enable log, and the two process-default sysvar effects were already
wired into the session and server crates. Every inlined policy value was
checked against the pinned `metadef`, `mysql`, and `vardef` sources.

The audit retained exactly the five Go source tests and removed Rust-only
assertions for an extra Unicode fold input, lowercase panic behavior, three
sysvars omitted by the source test, and the supplementary Enable/Disable
state-transition test.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `cargo test -p tidb-util --locked sem::tests::` — passed (5 tests).
- `cargo check -p tidb-session --lib --locked` — passed.
- `cargo check -p tidb-server --lib --locked` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `go test ./pkg/util/sem` — blocked before this package compiled by the
  workspace's existing missing `pkg/util/hack.checkMapABI` build selection and
  gRPC `http2.TrailerPrefix` dependency mismatch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; production behavior was already aligned.
- Compatibility: only supplementary Rust test cases are removed.
- Performance: unchanged.
