# `br/pkg/streamhelper/spans` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly seven artifacts, all read in full: `sorted.go`,
`sorted_test.go`, `utils.go`, `utils_test.go`, `value_sorted.go`,
`value_sorted_test.go`, and `BUILD.bazel`. There is no package doc, README,
fixture, benchmark, generated or platform variant, or ownership file. The
local Go package is byte-identical to the pin.

Production behavior includes start-key and value/start-key ordered indexes,
upper-bound value joins, overlap discovery, split/merge/refusion of adjacent
equal-valued spans, collapse, traversal, minimum-value lookup, and valued-set
comparison. The source contains exactly four tests: `TestBasic`,
`TestSubRange`, `TestValuedEquals`, and `TestSortedBasic`.

## Rust ownership and audit result

The complete package maps to `rust/crates/tidb-br/src/spans.rs` and its
`spans/{sorted,utils,value_sorted}.rs` modules. `BTreeMap` is the native
ordered-index boundary for Go's `google/btree` indexes. The four Rust tests
map exactly to the four Go tests.

Go aliases `Span` to the two-field `kv.KeyRange`; Rust now owns that
field-identical carrier directly in the package rather than importing it from
an incomplete `br/pkg/utils` port. The package's sole needed
`utils.CompareBytesExt` dependency is private and preserves the exact four
empty/infinity comparison branches. No other `br/pkg/utils` behavior is
exposed or claimed.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `cargo check -p tidb-util -p tidb-br --locked` — passed; existing model
  warnings remain outside this change.
- `cargo test -q -p tidb-br --locked -- --test-threads=1` — passed: 31 tests
  passed and 2 unrelated BR tests were ignored.
- `cargo test -q -p tidb-util --locked -- --test-threads=1` — passed: 597 unit
  tests passed, 3 were ignored, and every integration and doc test passed.
- `cargo fmt --all` and `git diff --check` — passed.
- `rg -n "br_key_utils" rust/crates rust/testport --glob '!target/**'` —
  returned no references before these receipts were added.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the package's complete test surface and both affected owner
  crates pass.
- Compatibility: `Span` remains the same public two-vector shape; its
  Rust-only convenience constructor was removed in favor of Go-shaped field
  construction.
- Performance: the ordered indexes and comparisons are unchanged; removing
  an unused utility module has no runtime cost.
