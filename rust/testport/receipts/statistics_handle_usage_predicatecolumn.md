# `pkg/statistics/handle/usage/predicatecolumn` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 18 | `c7049ed525fe51065f6f0d4ff223edf1aa101db3` | `0cf79e6c78690d2438d69c0b260fa482c0565d634d25a60a4aa143d6fe350fe1` |
| `predicate_column.go` | 157 | `a25cc23bc074bec6a48c8f60af9b56552603c8a7` | `b435e31c25f2d91ab59bcb27732c6e4a30a1560bd1b150fb8bc438e07e8516c1` |

There is no package test, `doc.go`, support file, fixture, generated input or
output, example, benchmark, fuzz target, build-tag variant, or platform
variant at the pin.

## Rust ownership and behavior

The package maps to
`rust/crates/tidb-exec/src/cluster_predicate_column.rs` and the corresponding
write planners in `cluster_stats_write.rs` because native metadata snapshots,
catalog rows, cleanup mutations, and replacement writes share that crate's
transaction boundary.

| Pinned Go behavior | Rust owner |
| --- | --- |
| private `loadColumnStatsUsage`, `LoadColumnStatsUsage`, and `LoadColumnStatsUsageForTable` | `cluster_predicate_column::{load,load_column_stats_usage,load_column_stats_usage_for_table}` |
| `GetPredicateColumns` and `cleanupDroppedColumnStatsUsage` | `cluster_predicate_column::predicate_columns` plus `cluster_stats_write::plan_get_predicate_columns` |
| `SaveColumnStatsUsageForTable` | `cluster_stats_write::plan_column_stats_usage_write` |

- complete and per-table loads skip NULL identity fields, preserve NULL usage
  timestamps, decode stored TIMESTAMP values as UTC instants, and project them
  into the requested session location at Go's `types.DefaultFsp` (zero); like pinned
  `CONVERT_TZ`, incomplete dates become NULL rather than surviving as zero
  timestamps;
- predicate-column lookup removes rows for dropped columns and returns only
  current rows whose `last_used_at` is non-NULL and valid; if the latest
  infoschema no longer contains the table, cleanup is skipped but the stored
  predicate IDs are still selected, exactly as Go's two-step function does;
- replacement writes preserve every supplied table/column identity, including
  the Go behavior of ignoring `TableItemID.IsIndex`, and explicitly replace
  either timestamp with NULL when absent or invalid-zero;
- the ordinary cluster ANALYZE path consumes cleanup and selected IDs through
  the metadata transaction rather than through an in-memory substitute.

`analyze_commit_size_source::{loaded_stats_usage_replaces_timestamps_in_one_plan,
column_stats_usage_write_does_not_filter_the_table_item_kind,
predicate_column_load_and_cleanup_match_the_pinned_storage_contract,
predicate_column_missing_table_skips_cleanup_but_still_reads_usage,
predicate_column_invalid_zero_timestamp_matches_convert_tz_null}` executes the
complete read, timezone, NULL, replacement, cleanup, missing-table, invalid
time, and selection contract. The package has no original Go tests to dispose
separately.

## Integration and validation

The importing root usage package exposes complete-load behavior through the
session provider and uses predicate lookup in ordinary ANALYZE. MySQL bootstrap
owns the storage schema; no duplicate SQL-string-only or leaf cache path was
added.

Ready validation for the importing atomic work includes the owner and all
ordinary consumers: focused `tidb-exec` storage tests, session ANALYZE tests,
server usage tests, multi-crate check/clippy, formatting, `make lint`, and
`git diff --check`. No Go or Bazel source changed, so `make bazel_prepare` is
not required.

The 2026-08-30 re-audit recorded both new regressions failing before their
fixes. The missing-table test returned `[]` instead of `[9]`; the invalid-zero
test returned two `Some(ZeroTime)` values instead of `None`. Both exact tests
then passed with:

    cargo test --locked -p tidb-exec --test all analyze_commit_size_source::predicate_column_missing_table_skips_cleanup_but_still_reads_usage -- --exact --nocapture
    cargo test --locked -p tidb-exec --test all analyze_commit_size_source::predicate_column_invalid_zero_timestamp_matches_convert_tz_null -- --exact --nocapture

The WIP package re-audit also passed:

    cargo test --locked -p tidb-exec --test all analyze_commit_size_source::predicate_column_ -- --nocapture
    cargo test --locked -p tidb-exec --test all analyze_commit_size_source::loaded_stats_usage_replaces_timestamps_in_one_plan -- --exact --nocapture
    cargo test --locked -p tidb-exec --test all analyze_commit_size_source::column_stats_usage_write_does_not_filter_the_table_item_kind -- --exact --nocapture
    cargo check --locked -p tidb-exec
    cargo fmt --all -- --check
    git diff --check
