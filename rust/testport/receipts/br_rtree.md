# `br/pkg/rtree` — Rust return-contract alignment

Pinned Go inventory evidence is retained in
`rust/testport/receipts/go-kvproto-client-compatibility.md`: exactly seven
artifacts and 1,298 lines, comprising `rtree.go`, `logging.go`, four source
test/benchmark/fuzz artifacts, and `BUILD.bazel`. That inventory records no
fixture, generated source/input, platform variant, or other build artifact.
This 2026-09-07 follow-up deliberately does not reopen Go code, following the
user's Rust-only direction.

## Complete Rust inventory

The complete implementation owner is `rust/crates/tidb-br/src/rtree.rs` plus
`src/rtree/logging.rs` and `src/rtree/rtree.rs`. Before this edit those three
modules contained 68, 93, and 1,517 lines respectively (1,678 total) and were
byte-identical to their package landing at
`e40dbe9f6a7a41f910450b9874fdfddeaacc484c`. They map the two production Go
files, all public/private functions, the three range-tree forms, checksum and
metadata seams, and the complete test corpus.

The Rust test surface had ten active inline tests plus one ignored
benchmark-shaped test: log rendering, range update/incomplete ranges,
force-put, intersection, range merging in keyspace/non-keyspace modes, the
source fuzz seed, progress trees, both callback/checksum paths, and table-key
framing. The 19-line shared manifest, 72-line crate root, workspace member, and
lockfile entry were inspected. The crate has no feature, build script,
generated output/input, platform variant, fixture, example, separate test
artifact, or reverse Cargo dependency. Internal production callers are
`restore_utils/merge.rs`, `restore_utils/proto.rs`, and
`restore_utils/rewrite_rule.rs`; source search found no caller outside
`tidb-br`.

## Alignment decision

Go permits callers to discard twelve direct scalar/struct results:
`KeyRange.Contains`, `KeyRange.ContainsRange`, `Range.BytesAndKeys`,
`NewRangeStatsTree`, promoted `RangeStatsTree.Len`, `NeedsMerge`,
`NewRangeTree`, `NewRangeTreeWithFreeListG`, promoted `RangeTree.Len`,
`NewProgressRangeTree`, promoted `ProgressRangeTree.Len`, and
`ProgressRangeTree.GetChecksumMap`. Rust had added `#[must_use]` to every
counterpart. The focused regression calls all twelve under
`#[deny(unused_must_use)]`; after adding explicit generic payload types, its
valid pre-fix run failed with exactly twelve diagnostics. Removing only those
twelve annotations makes the same test pass.

Seventeen annotations remain intentionally. `intersect`, `get`, and `find`
return inherent `Option` values; `merged_ranges` and `get_incomplete_range`
return inherent `Vec` values; `zap_ranges` returns inherent `String`.
`KeyRange::{new}`, `Range::{new,with_files}`, the promoted-field accessors on
`Range`/`RangeStats`, and the three `is_empty` conveniences are native Rust
representation helpers rather than direct declarations in the source
package. Deleting those explicit attributes either cannot restore Go
discardability or would weaken a native-only API without aligning a source
call.

No range, merge, checksum, tree, error, logging, or metadata behavior changed.
The post-edit owner is 1,701 lines, including the focused regression; the
shared manifest and crate root bring the inspected integration surface to
1,792 lines.

## Validation

Profile: Ready, because this package batch is committed and published as an
independent checkpoint.

- Focused regression: `cargo +nightly-2026-08-22 test --manifest-path
  rust/Cargo.toml --offline --locked -p tidb-br --lib
  rtree::rtree::tests::direct_source_returns_may_be_ignored_like_go -- --exact
  --test-threads=1` — passed, 1 test. The corresponding pre-fix run failed with
  exactly twelve `unused_must_use` diagnostics.
- Complete shared crate: `cargo +nightly-2026-08-22 nextest run
  --manifest-path rust/Cargo.toml --offline --locked -p tidb-br
  --no-fail-fast` — passed, 33 active tests; two unrelated benchmark-shaped
  tests skipped.
- Owner/build surface: `cargo +nightly-2026-08-22 check --manifest-path
  rust/Cargo.toml --offline --locked -p tidb-br --all-targets` — passed; only
  pre-existing dependency warnings were emitted.
- Scoped nightly `rustfmt --check`, repository `make lint`, and
  `git diff --check` — passed.

Only Rust owner source and parity documentation changed. No Go, Bazel, Cargo
metadata, module, import, generated, or build-target input changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: minimal; the edit changes compiler diagnostics only, and the
  complete deterministic shared-crate suite passes.
- Compatibility: improved for direct source-shaped calls; native and inherent
  return contracts remain explicit.
- Performance: none; generated code and runtime instructions are unchanged.
- Not verified locally: no live BR/object-store integration was run because
  the affected owner has no live-service or fixture dependency; its metadata
  boundary is covered by the existing deterministic recording sink.
