# `pkg/util/trxevents` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `trx_events.go` and
`BUILD.bazel`. There is no package doc, test, test harness, benchmark, fixture,
generated input/output, platform variant, README, or ownership file. The local
Go package is byte-identical to the pin.

Production behavior is one native-width event tag, the zero-valued
coprocessor-lock event kind, a `CopMeetLock` containing an optional kvproto lock
pointer, a private tagged/erased `TransactionEvent`, its wrapper and extractor,
and the callback function alias. A default event panics when its zero tag makes
the extractor assert a nil erased payload; a wrapped nil event returns nil.

## Rust ownership and audit result

`rust/crates/tidb-txnkv/src/trxevents.rs` owns the package, and the ordinary
`tidb-distsql` locked-response path constructs and consumes it. Optional
`Arc` pointers are the native shared-ownership mapping of Go's two pointer
fields, so cloning an event retains pointer identity rather than deep-copying
the protobuf payload. Rust-only value equality was removed from the public
event types, as Go compares their pointer/interface payloads by identity rather
than recursively by value. The five-test package-only Rust suite was deleted
because pinned Go has no tests; source-derived downstream callback tests remain.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/trxevents` — passed.
- `go test ./pkg/util/trxevents -count=1` — blocked before package execution by the existing `google.golang.org/grpc/internal/transport` reference to missing `http2.TrailerPrefix`.
- `cargo check --offline --locked -p tidb-txnkv -p tidb-distsql` — passed with existing warnings.
- `cargo test --offline --locked -p tidb-distsql --test all direct_unary_dispatch_contract::locked_response_publishes_the_exact_transaction_event_before_recovery -- --exact` — passed.
- `cargo test --offline --locked -p tidb-distsql --test all query_runtime_source::select_client_options_deliver_typed_transaction_events -- --exact` — passed.
- `rustfmt --edition 2021 --check crates/tidb-txnkv/src/trxevents.rs crates/tidb-distsql/src/cop_paging/direct_unary_query_transport.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: pointer sharing now matches Go; wrapped-nil and default-event
  behavior are preserved.
- Compatibility: removes Rust-only value equality and package-only tests;
  the wrapper now accepts the native equivalent of Go's pointer argument.
- Performance: replaces deep event/protobuf clones with atomic reference-count
  increments on the infrequent locked-response callback path.
