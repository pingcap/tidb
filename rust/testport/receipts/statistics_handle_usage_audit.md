# `pkg/statistics/handle/usage` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 55 | `48fc84076eff3b8d0241487fbb06b36f33bfab29` |
| `export_test.go` | 27 | `01469f608577f92c0e540c2c1e47c78c022881b6` |
| `index_usage.go` | 62 | `3016755169bfcddebbd6b5f9b955a524dc18235c` |
| `index_usage_integration_test.go` | 113 | `22adec8f549a8b7f10e58be26db20e33c86c9b4d` |
| `predicate_column.go` | 63 | `16238f0c707ae3f7372cd34c162f0ac7664826b1` |
| `predicate_column_test.go` | 263 | `58c1857a9ae4aaa28557262302499e126f006963` |
| `session_stats_collect.go` | 692 | `855d4269d65417e4fb561d180cbb93e8541ffc0c` |
| `session_stats_collect_test.go` | 403 | `d88e62a2129e28768ceea55cd63bdc93c2ce8a73` |

There is no `doc.go`, fixture, generated source/input, example, fuzz target, or
build/platform variant. Subpackages remain separate atomic claims; collector,
indexusage, and predicatecolumn have complete receipts.

## Native ownership and source behavior

The complete Go package maps to three native boundaries:

- `tidb-stats-handle-usage` owns table-delta and predicate-usage maps, session
  collectors and sweeping, dump guards, eligibility, batch constants, lock and
  partition expansion, and indexusage delegation;
- `tidb-exec::{cluster_predicate_column,cluster_stats_write}` owns metadata
  reads and transactional write plans;
- `tidb-server::cluster_session_node` owns restricted-session-equivalent
  snapshots, per-batch commits, workers, schema exclusions, historical-meta
  dispatch, and shutdown flushing.

The combined path preserves Go's latest predicate timestamp and earliest
nonzero delta initialization time, deleted-session sweep, sorted deadlock-safe
ordering, 2,048-row column batches, 100,000-table delta batches, one-hour and
ratio eligibility, twelve-hour column timestamp throttling, partition/global
lock rules, and nonfatal per-table historical-meta recording.

Two audited error/encoding details are explicit:

- if a later column batch fails, every entry is merged back for retry, including
  rows from earlier batches that already committed, exactly like Go's one
  deferred `colMap` merge;
- collected `SystemTime` values are truncated to whole seconds before storing
  TIMESTAMP(6), matching `t.UTC().Format(types.TimeFormat)` rather than adding
  Rust-only microsecond precision.

`SessionStatsItem::update_col_stats_usage` accepts the caller's one timestamp,
matching `session.UpdateColStatsUsage`; the unused source-absent
`StatsUsageHandle: Default` surface was removed.

## Original test and support disposition

- `export_test.go`'s duration accessors are represented by private Rust test
  setters and the dump-eligibility source test; they are not production API.
- `TestGCIndexUsage` runs as
  `source_index_usage_integration_test_gc_index_usage` over real shared
  `TableInfo` index metadata.
- predicate cleanup, collected-column growth, persisted-option enabled and
  disabled behavior, and the no-predicate index/no-index/primary-key cases run
  in `analyze_commit_size_source` and `tests_analyze`, including mandatory
  index/primary columns and the exact warning.
- first touch, no bump inside twelve hours, and bump after an old value run in
  `analyze_commit_size_source` through actual system-table plans.
- `TestDumpColStatsUsageWriter_ConcurrentMultiTables` is unconditionally
  skipped by the pinned Go test before setup. Rust preserves its batching and
  throttle behavior but does not invent an executable concurrency claim.
- initialization-time persistence and earliest-time merge run in the usage
  crate; the latter exercises the blocked/overlapping dump invariant directly.
- a server regression proves later-batch failure requeues all column entries,
  and a second proves Go's whole-second formatting into TIMESTAMP(6).

The earlier isolated `tidb-stats` usage fact modules, SQL-string-only
predicatecolumn module, empty gap-marker tests, duplicate aggregate tests, and
source-absent convenience constructor remain removed. No alternate cache,
leaf persistence, or test-only execution path was introduced.

## Validation

Profile: Ready, because this receipt makes the package-level completion claim.
The exact commands and their outcomes are recorded in the living ExecPlan at
`rust/docs/statistics/usage-package-parity-execplan.md`. No Go source, Go
imports, Bazel metadata, module dependency, or top-level Go test changed, so
`make bazel_prepare` is not required.
