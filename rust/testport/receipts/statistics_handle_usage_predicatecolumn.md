# `pkg/statistics/handle/usage/predicatecolumn` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 18 | `c7049ed525fe51065f6f0d4ff223edf1aa101db3` |
| `predicate_column.go` | 157 | `a25cc23bc074bec6a48c8f60af9b56552603c8a7` |

There is no package test, `doc.go`, support file, fixture, generated input or
output, example, fuzz target, or platform variant at the pin.

## Rust ownership and behavior

The package maps to
`rust/crates/tidb-exec/src/cluster_predicate_column.rs` and the corresponding
write planners in `cluster_stats_write.rs` because native metadata snapshots,
catalog rows, cleanup mutations, and replacement writes share that crate's
transaction boundary.

- complete and per-table loads skip NULL identity fields, preserve NULL usage
  timestamps, decode stored TIMESTAMP values as UTC instants, and project them
  into the requested session location at TIMESTAMP(6);
- predicate-column lookup removes rows for dropped columns and returns only
  current rows whose `last_used_at` is non-NULL and valid;
- replacement writes preserve every supplied table/column identity, including
  the Go behavior of ignoring `TableItemID.IsIndex`, and explicitly replace
  either timestamp with NULL when absent;
- the ordinary cluster ANALYZE path consumes cleanup and selected IDs through
  the metadata transaction rather than through an in-memory substitute.

`analyze_commit_size_source::{loaded_stats_usage_replaces_timestamps_in_one_plan,
column_stats_usage_write_does_not_filter_the_table_item_kind,
predicate_column_load_and_cleanup_match_the_pinned_storage_contract}` executes
the complete read, timezone, NULL, replacement, cleanup, and selection
contract. The package has no original Go tests to dispose separately.

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
