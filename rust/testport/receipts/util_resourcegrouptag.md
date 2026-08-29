# `pkg/util/resourcegrouptag` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains `BUILD.bazel`, production
`resource_group_tag.go`, source test `resource_group_tag_test.go`, and package
harness `main_test.go`. It has three top-level source tests and no `doc.go`,
fixture, generated source, benchmark, fuzz target, example, platform variant,
or build-tagged production variant. The checkout package is byte-identical to
the pin.

`main_test.go` supplies Go's common test setup and goroutine leak checker;
Rust's synchronous package test starts no runtime workers and needs no harness
analogue. Go's package-local `go_test` maps to the standalone
`resource_group_tag_source` Cargo target.

## Rust ownership and integration

`tidb-txnkv::resource_group_tag` now exclusively owns the package behavior:
protobuf tag decoding with the source hexadecimal error, row/index/unknown key
classification, and first-key extraction for Get, BatchGet, Scan, Prewrite,
Commit, BatchRollback, coprocessor, batch-coprocessor, and pessimistic-lock
requests. Rust's request trait is the language-native substitute for Go's
`tikvrpc.Request` type switch; empty vectors represent its nil/empty nested
request values.

The previously mixed `pkg/kv.ResourceGroupTagBuilder`, table-ID decode hook,
and request mutation trait remain in `resource_group.rs`, their Go owner. Three
supplemental builder unit tests and the builder test formerly attached to the
resourcegrouptag suite were removed; the existing `pkg/kv`-shaped
`TestResourceGroupTagEncoding` retains that source behavior. The package test
now has exactly the three Go identities and cases. Its explicit Cargo target
is excluded from the oversized aggregate harness, matching the independent Go
package test artifact and preventing unrelated crate tests from gating it.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/resourcegrouptag` — passed.
- `GOTOOLCHAIN=go1.25.10 go test ./pkg/util/resourcegrouptag -count=1` — passed; three tests.
- `cargo check -p tidb-txnkv` — passed.
- `cargo test -p tidb-txnkv --test resource_group_tag_source` — passed; three tests.
- `cargo test -p tidb-executor --lib deadlock_history::tests::test_err_deadlock_to_deadlock_record` — passed; production decoder consumer.
- `cargo fmt -p tidb-txnkv` — passed.
- `git diff --check` — passed.

No Go source, Go test, Bazel metadata, or Go module file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: unchanged production behavior; ownership and source test scope
  now follow Go package boundaries.
- Compatibility: protobuf bytes, absent digest handling, malformed-tag error
  spelling, key labels, and first-key selection follow the pinned package.
- Performance: the split is compile-time organization only; decoding and key
  extraction retain the same allocations and constant-time first-element
  access.
- Not verified locally: construction of every concrete TiKV client request
  wrapper, because Rust uses typed protobuf requests rather than Go's nullable
  interface wrapper. All representable pinned request cases and the live
  deadlock decoder consumer are covered.
