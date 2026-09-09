# `pkg/statistics/handle/autoanalyze` package parity receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete upstream inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 71 | `bb541aeb13beb1caaddcb01c3e6924ba48f09123` |
| `autoanalyze.go` | 920 | `a590ca2f59b563b7d23c7508111f4219ff6c0490` |
| `autoanalyze_test.go` | 615 | `f082ca3fa12b372e94d5de27d0405c710106e724` |

All 1,606 lines were read. There are 14 tests, no benchmark, generated input,
fixture, platform variant, or package-local support file. The Bazel target's
dependencies are integrated through the Rust workspace crates named below;
there is no separate Rust build target for this Go facade package.

## Integration decision

The Go package is a composition owner, not a second planner or executor. Rust
therefore maps it across the existing production owners:

- `tidb-stats-handle-autoanalyze-{exec,priorityqueue,refresher}` own parameter
  loading, queue selection/validation, DDL/DML refresh, windows, concurrency,
  process tracking, retries, and worker lifecycle.
- `tidb-server::cluster_session_node` owns statistics ownership, live catalog,
  locks, system sessions, cleanup workers, and the ordinary ANALYZE entrypoint.
- `tidb-exec::real_tikv_analyze` owns physical analyze tasks, selected columns,
  GLOBAL skipped types, vector exclusion, job rows, cache publication, and
  partition/global effects.

There is one execution path. Queue SQL is parsed into `AnalyzeStatement`, marked
as automatic only for Go's observable job identity, and sent through the same
physical ANALYZE builder/executor used by client statements. No cache-only or
auto-analyze-only executor pipeline remains. Pinned Go permanently enables the
priority queue, so Rust does not expose the source-unreachable legacy random
scheduler as an alternate runtime.

The package pass found and fixed two shared-path defects rather than adding
workarounds: TopN lacked Go's output-only inline projection and corrupted the
queue's unchanged duration SQL; and ANALYZE did not apply Go's skipped-type,
mandatory-index-column, generated-dependency, and unconditional vector-column
filter. Automatic jobs now persist `auto analyze ...` rather than manual
`analyze ...` identity.

## Original test disposition

| Go test | Executable Rust owner |
| --- | --- |
| `TestEnableAutoAnalyzePriorityQueue` | `tidb-session::sysvar::auto_analyze_priority_queue_is_always_enabled`; server queue integration |
| `TestAutoAnalyzeLockedTable` | server `auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path`; priorityqueue `source_queue_static_lock_and_system_view_filters` |
| `TestAutoAnalyzeWithPredicateColumns` | same server integration; `tidb-session::analyze_default_predicate_columns_follow_usage_and_mandatory_index_columns` |
| `TestDisableAutoAnalyze` | priorityqueue `source_factory_matches_ratio_index_version_and_partition_rules`; refresher/domain worker gate tests |
| `TestDisableAutoAnalyzeWithAnalyzeAllColumnsOptions` | same factory and worker owners; server integration exercises live `ALL` after queue rebuild |
| `TestTableAnalyzed` | server ordinary/automatic analyze integrations and stats reload assertions |
| `TestNeedAnalyzeTable` | priorityqueue `source_factory_matches_ratio_index_version_and_partition_rules` over unanalyzed, ratio-zero, below-ratio, and modified cases |
| `TestAutoAnalyzeSkipColumnTypes` | server `auto_analyze_skips_configured_column_types_like_go`; `tidb-exec::skipped_analyze_types_keep_index_columns_and_always_drop_vectors` |
| `TestAutoAnalyzeOnEmptyTable` | refresher invalid/outside-window tests plus server ordinary/automatic publication paths |
| `TestAutoAnalyzeOutOfSpecifiedTime` | refresher window tests and priorityqueue inclusive/wrapping-window test |
| `TestCleanupCorruptedAnalyzeJobsOnCurrentInstance` | server `corrupted_analyze_job_cleanup_matches_go` |
| `TestCleanupCorruptedAnalyzeJobsOnDeadInstances` | server `corrupted_analyze_job_cleanup_matches_go` |
| `TestSkipAutoAnalyzeOutsideTheAvailableTime` | priorityqueue `source_auto_analysis_time_window_is_utc_minute_inclusive`; refresher rejection tests |
| `TestAutoAnalyzeWithVectorIndex` | priorityqueue factory test plus `tidb-exec::skipped_analyze_types_keep_index_columns_and_always_drop_vectors` |

The external planner `TestAutoAnalyzeForMissingPartition` is also executable as
server `auto_analyze_fills_missing_partition_statistics_like_go`; its obsolete
ignored empty planner test was removed.

## Validation

Targeted package checks and server tests are recorded in the package commit.
Server owner-election cases are run as isolated test processes because the
in-memory election service deliberately outlives sequential factories briefly;
running all owner tests in one binary creates a harness race that production
nodes do not have. No Go or Bazel source changed, so `make bazel_prepare` was
not required.
