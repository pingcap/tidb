# `pkg/statistics/handle/cache/internal/testutil` → `tidb-stats-handle-cache-internal-testutil`

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 14 | `bca31c934cc04e651524816942cf7877f9621d9b` | workspace member and crate manifest |
| `testutil.go` | 95 | `ca2c179152567e3feaa22a7632d06d8d8ee17ce2` | `src/lib.rs` |

The support package has no generated, platform-specific, test, fixture-file,
or benchmark artifacts.

## Behavior mapping

- `new_mock_statistics_table` returns a shared actual `tidb_stats::Table`
  with the source zero-valued `HistColl` and one-based column/index IDs.
- Negative column/index counts produce no entries, matching the Go `int` loop.
- Optional CMS sketches use depth/width 1, optional TopN values have capacity
  one and contain the empty byte value with count one, and optional histograms
  retain the source metadata and one-bucket allocation hint.
- Initial columns and indexes have full-load status; append helpers deliberately
  omit it, matching their source struct literals.
- Memory accounting comes from the real Rust sketches and the native
  histogram allocation, so cache tests exercise the same production table
  memory path instead of caller-provided fixture costs.
- Append helpers derive the next ID from the current table map length and add
  a CMS-only item.

The former `MockStatisticsTableShape` and its two source-absent tests were
removed. They produced no table, sketches, histogram, load status, memory
accounting, or append behavior.

## Validation

WIP profile: this is a source-test-free support package, so the package gate is
strict compilation/linting plus the affected statistics owner gate.

- `cargo check --locked -p tidb-stats-handle-cache-internal-testutil`
- `cargo clippy --locked -p tidb-stats-handle-cache-internal-testutil --no-deps -- -D warnings`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats-handle-cache-internal-testutil/src/lib.rs crates/tidb-stats/src/lib.rs`
- `git diff --check`
