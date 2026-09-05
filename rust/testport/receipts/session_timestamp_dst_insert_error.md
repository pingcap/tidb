# Rust session TIMESTAMP DST insert-error receipt

Status: bounded Rust-only alignment batch; this receipt covers the completed
error class for a `TIMESTAMP` written during a daylight-saving gap.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go session package was
enumerated and read from the fetched tree: 92 artifacts under `pkg/session`
(25 production Go files, 45 tests, and fixture, generated, platform, build,
and metadata files). The related Go expression package was rechecked in full:
208 artifacts (117 production Go files, 78 tests, generated/build inputs, and
package metadata). The planner package was also rechecked in full: 568
artifacts (196 production Go files, 166 tests, plus generated sources,
fixtures, platform variants, `BUILD.bazel`, and ownership/build metadata).
No Go, generated, fixture, platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-executor` has 291
tracked files and `tidb-session` has 222, including every production source,
inline and standalone test, generated test harness input, fixture, platform
variant, Cargo/build artifact, and package metadata. The changed Rust files
are `tidb-executor/src/driver/write_cast.rs` and
`tidb-session/src/tests_timestamp_range.rs`.

## Alignment

Go's TIMESTAMP conversion preserves the adjusted value when a wall-clock
literal falls in a DST gap, returning `ErrTimestampInDSTTransition` (8179).
The INSERT caller then handles that error specially in
`pkg/executor/insert_common.go:handleErr` (around lines 356-369): it retitles
the value as `ErrTruncateWrongInsertValue` (1292), including the target column
and one-based row, returning an error in strict mode or a warning in
non-strict mode. UPDATE/raw `table.CastValue` paths retain the internal 8179
diagnostic.

Rust's `write_cast` event arm exposed 8179 directly for every shape. A strict
INSERT therefore returned 8179 instead of Go's 1292, and a non-strict INSERT
stored the adjusted timestamp with the wrong warning code/message.

The event arm now selects `IncorrectTemporalValue` only for `CastShape::InsertRow`
and keeps `TimestampInDSTTransition` for raw and UPDATE shapes. Focused unit
and session regressions cover strict rejection, non-strict warning/storage,
the exact 1292 text, and all existing timestamp-range/DST cases.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::write_cast::source_tests::timestamp_dst_gap_keeps_adjusted_value_and_insert_1292_diagnostic -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_timestamp_range:: -- --nocapture --test-threads=1`

All focused tests passed (one write-cast regression and five session timestamp
range/DST tests).

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only the INSERT-row naming of a DST-gap conversion changes. The adjusted
timestamp, strict/non-strict storage decision, raw conversion diagnostic, and
UPDATE behavior remain unchanged. No Go source, generated output, fixture,
platform variant, or build artifact was modified.
