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

## Rust return-contract alignment (2026-09-07)

This follow-up stays entirely on the Rust side, reusing the complete pinned Go
inventory above. Before editing, the four owning modules were byte-identical to
commit `408cd9bceb031289c6c93da16ff4f78b9d35d521` and contained 1,021 lines:
`src/spans.rs` (79), `src/spans/sorted.rs` (450), `src/spans/utils.rs` (255),
and `src/spans/value_sorted.rs` (237). The shared `Cargo.toml`, crate root,
workspace member, lockfile entry, every function, the four source-derived
tests, and every caller were also inspected. `tidb-br` has no reverse Cargo
dependency and the span APIs have no caller outside these four modules. There
is no fixture, generated source/input, platform variant, feature, custom build,
example, benchmark target, fuzz target, or separate test artifact for this
package slice.

Go permits callers to discard the direct results of `join`, `Valued.Less`,
`Valued.Equals`, `NewFullWith`, `Overlaps`, `ValuedSetEquals`, `Sorted`,
`ValueSortedFull.Min`, and `ValueSortedFull.MinValue`. Rust had added
`#[must_use]` to all nine counterparts. A focused module regression invokes
the nine calls under `#[deny(unused_must_use)]`; it failed before the source
edit with exactly nine diagnostics and passes after removal of those nine
annotations. No span value, ordering, merge, traversal, or rendering behavior
changed.

Four annotations remain intentionally. `stringify_range` returns `String`,
while `collapse` and `full` return `Vec`; those standard-library return types
are inherently must-use, so deleting only the explicit attribute would not
make the Go-shaped call discardable. `Valued::new` is a Rust construction
helper with no declaration in the source package. The post-edit owner slice is
1,036 lines, including the new regression; the 19-line manifest and 72-line
crate root bring the inspected Rust package integration surface to 1,127
lines.

Ready validation:

- Focused regression: `cargo +nightly-2026-08-22 test --manifest-path
  rust/Cargo.toml --offline --locked -p tidb-br --lib
  spans::return_contract_tests::direct_source_returns_may_be_ignored_like_go
  -- --exact --test-threads=1` — passed, 1 test.
- Complete shared-crate gate: `cargo +nightly-2026-08-22 nextest run
  --manifest-path rust/Cargo.toml --offline --locked -p tidb-br
  --no-fail-fast` — passed, 32 active tests; two unrelated benchmark-shaped
  tests skipped.
- Owner/build surface: `cargo +nightly-2026-08-22 check --manifest-path
  rust/Cargo.toml --offline --locked -p tidb-br --all-targets` — passed; only
  pre-existing warnings in dependencies were emitted.
- Scoped nightly `rustfmt --check`, repository `make lint`, and
  `git diff --check` — passed.

Only Rust owner source, this receipt, and the ExecPlans changed. No Go, Bazel,
Cargo metadata, module, import, generated, or build-target input changed, so
`make bazel_prepare` is not required. Correctness and performance risk are
minimal because this is a compile-time caller-contract change; compatibility
improves for direct source-shaped calls. Live BR integration was not run
because the owner has no service or fixture dependency and the complete
deterministic crate suite covers the affected slice.
