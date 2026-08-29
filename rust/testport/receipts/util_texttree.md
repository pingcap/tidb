# `pkg/util/texttree` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full:

- `texttree.go`: five exported tree runes plus `Indent4Child` and
  `PrettyIdentifier`.
- `texttree_test.go`: `TestPrettyIdentifier` and `TestIndent4Child`.
- `main_test.go`: the common Go test setup and Go-runtime goroutine-leak
  harness.
- `BUILD.bazel`: one library and one short, race-enabled test target.

There is no package doc, benchmark, fixture, generated source, platform
variant, README, or ownership file. The local Go package is byte-identical to
the pin. `main_test.go` controls only the Go test process; Cargo owns the Rust
test process, so it has no production or test-behavior port.

## Rust ownership and audit result

`rust/crates/tidb-util/src/texttree.rs` owns the complete package. It uses
`tidb_datatype::GoString` because Go strings may contain arbitrary bytes. As
in Go, indentation is iterated as Unicode code points, consuming each invalid
UTF-8 byte as one replacement character; `PrettyIdentifier` then appends the
identifier's original bytes without decoding them.

The audit removed the previous valid-UTF-8-only `&str`/`String` narrowing,
duplicate supplemental test groups, Rust-only `must_use` diagnostics, and the
remaining arbitrary-byte supplemental regression. Exactly the two Go test
identities remain. The ordinary consumers in `tidb-util::plancodec` and
`tidb-planner::explain` convert the result to `String` only at their
source-guaranteed valid-UTF-8 plan metadata boundary.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

Passed:

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/texttree`
- `go test ./pkg/util/texttree -count=1`
- `cargo test --offline --locked -p tidb-util texttree`
- `cargo check --offline --locked -p tidb-planner --lib`
- `cargo test --offline --locked -p tidb-util binary_plan_preserves_tree_order_access_and_runtime_fields`
- `cargo test --offline --locked -p tidb-util connection_formats_select_source_columns`
- `cargo test --offline --locked -p tidb-planner --lib explain`
- `cargo clippy --offline --locked -p tidb-util --lib --no-deps -- -A
  clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A
  clippy::wrong-self-convention -A clippy::new-without-default -D warnings`
- `cargo clippy --offline --locked -p tidb-planner --lib --no-deps`
- `rustfmt --edition 2021 --check crates/tidb-util/src/texttree.rs
  crates/tidb-util/src/plancodec.rs crates/tidb-planner/src/explain/mod.rs`
- `git diff --check`

The broader `cargo check --offline --locked -p tidb-util -p tidb-planner
--all-targets` reached two existing unrelated `tidb-planner` integration-test
compile errors: a missing `RuleContext.column_allocator` initializer and a
`SortItem`/`ByItems` mismatch. The affected production libraries and scoped
tests pass as recorded above.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the full source surface and both consumers are covered by the
  pinned implementation and source tests.
- Compatibility: the public Rust return type is now byte-preserving
  `GoString`; both existing valid-UTF-8 production consumers are adapted.
- Performance: valid input still performs one rune pass and one output
  allocation, matching the source algorithm's shape.
