# `pkg/statistics/asyncload` package audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `c1bb9dc8c8ee2a2282387d0bc0eca93a1db61d9d` | `ef2c216b18c3077813f1b894b95e0fb57da24323489bdf588482ea6c77a4c967` | build metadata inventoried |
| `async_load.go` | 120 | `2a35e37483fa1ffe1d27b22a1a3af8fb24bcf87b` | `422c0649b2e9d10e42b0542a7c2cd44c5346b6beca5d11035770f0aeeff0bbe2` | `tidb-stats::async_load`; complete |
| `async_load_test.go` | 267 | `ac4331b0cca610b7256f5088445defdff178e52d` | `649715b90eb3e2e96dfcfa6074168cb91ede3672d9be0d5955f39973f57bd362` | all five integration behaviors mapped below |

The package has no generated, platform-specific, benchmark, fixture, or other
support artifacts.

## Production mapping

The package owns one process-global, 128-shard map keyed by
`model.TableItemID`. `tidb-stats::async_load` preserves the absolute item-ID
shard key, one read/write lock per shard, unordered snapshots, insert upgrade
from metadata-only to full load, no downgrade, deletion, and total length.
The singleton is public while its constructor remains private. Rust must name
the singleton's concrete type in its public signature; unlike Go, it cannot
export a value whose type is private, so `NeededStatsMap` is public only as
that language-boundary carrier and is not re-exported from the crate root.

Planner validity/rule paths insert into the singleton. `Catalog::
load_needed_histograms` supplies the in-process statistics-handle boundary,
and `real_tikv_stats::load_needed_histograms_from_cluster` supplies the real
storage/shared-cache boundary. For a positive statistics lease, an independent
ticker invokes the latter after Go's `InitStatsDone` boundary, matching
Domain's separate `asyncLoadHistogram` loop rather than coupling it to the
ordinary statistics refresher. A negative lease starts neither loader; a zero
lease starts only Go's ordinary three-second fallback loader and no asynchronous
histogram ticker. Every attempted item is deleted whether loading
succeeds, is skipped as stale, or returns an error; the first real error stops
that snapshot's remaining iteration and is logged by the worker.

Go tests full-load eligibility for every asynchronous column even when the
queued request is metadata-only, but still passes the queued flag to storage.
Indexes are always loaded fully. Both boundaries preserve that distinction.
They resolve physical partitions through the current target set, discard
internal/nonexistent columns, missing tables and dropped indexes, create the
same empty column for analyzed=false demand, and
publish successful items into the shared statistics cache.
The real index path preserves Go's observable ordering: cache eligibility and
histogram metadata are read before current index metadata is revalidated;
bucket, TopN, and CMS payloads are read only after that schema check succeeds.

## Original test mapping

| Go test | Rust evidence |
| --- | --- |
| `TestLoadColumnStatisticsAfterTableDrop` | `statistics_request_tests::load_column_statistics_after_table_drop` queues the real column, executes `DROP TABLE`, drains, and verifies item removal |
| `TestLoadStatisticsAfterColumnDrop` | `statistics_request_tests::load_statistics_after_column_drop` queues column `b`, executes `ALTER TABLE ... DROP COLUMN`, drains, and verifies item removal |
| `TestLoadIndexStatisticsAfterTableDrop` | `statistics_request_tests::load_index_statistics_after_table_drop` queues the real index, executes `DROP TABLE`, drains, and verifies item removal |
| `TestLoadStatisticsAfterIndexDrop` | `statistics_request_tests::load_statistics_after_index_drop` queues index `ia`, executes `ALTER TABLE ... DROP INDEX`, drains, and verifies item removal |
| `TestLoadCorruptedStatistics` | `cluster_stats_load::tests::a_corrupted_integer_bound_uses_go_conversion_flags` uses the exact corrupt `upper_bound` bytes and proves Go's conversion yields integer zero rather than an error; `histograms_in_flight_cleans_completed_statistics_items` proves successful load deletion |

Additional branch evidence mirrors production code exercised by those tests:
`asynchronous_column_metadata_request_uses_full_load_eligibility` is the
fail-before/pass-after regression for Go's hard-coded full-load eligibility,
`asynchronous_index_request_is_always_full_load` covers Go's index rule, and
`failed_async_load_removes_the_item_and_returns_the_storage_error` covers the
deferred-delete/error branch in `storage.LoadNeededHistograms`.

## Removed non-parity surface

The public constructor, `Default`, `is_empty`, root-level `NeededStatsMap`
re-export, and two caller-owned/concurrency unit tests had no original Go API
or test counterpart and were removed. The global map remains the only
constructible production instance.

## Validation

WIP evidence gathered while closing the package:

- fail-before: `cargo test --manifest-path rust/Cargo.toml -p tidb-executor statistics_request_tests::asynchronous_column_metadata_request_uses_full_load_eligibility -- --nocapture` (loader requests were `[]`, expected one metadata-only request)
- `cargo test --manifest-path rust/Cargo.toml -p tidb-executor statistics_request_tests::asynchronous_ -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-executor statistics_request_tests::load_ -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec cluster_stats_load::tests::a_corrupted_integer_bound_uses_go_conversion_flags -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec stats_watch::tests::async_histogram_loading_has_an_independent_ticker_and_shutdown -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec stats_watch::tests::async_histogram_ticker_retains_one_tick_while_init_is_running -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-server real_tikv_node::schema_following::tests::asynchronous_statistics_loading_requires_a_positive_lease -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-server real_tikv_node::schema_following::tests::statistics_reload_targets_follow_the_current_catalog -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats --no-run`
- `cargo check --manifest-path rust/Cargo.toml -p tidb-stats -p tidb-exec -p tidb-executor -p tidb-server`

Ready profile passed:

- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo check --manifest-path rust/Cargo.toml -p tidb-stats -p tidb-exec -p tidb-executor -p tidb-server`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-executor statistics_request_tests::` (16 passed)
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec cluster_stats_load::tests::` (11 passed)
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec stats_watch::tests::async_histogram_loading_has_an_independent_ticker_and_shutdown -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec stats_watch::tests::async_histogram_ticker_retains_one_tick_while_init_is_running -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-server real_tikv_node::schema_following::tests::asynchronous_statistics_loading_requires_a_positive_lease -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-server real_tikv_node::schema_following::tests::statistics_reload_targets_follow_the_current_catalog -- --exact`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec --test all one_item_load_uses_only_that_items_key_ranges`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats --no-run`
- `make lint`

A non-gating broad `cargo test --manifest-path rust/Cargo.toml -p tidb-stats`
run exposed the existing process-global analyze-default race in
`pkg_statistics_go_tests_source::build_sample_full_ndv` (three TopN entries
instead of two during the parallel suite). The test and builder are outside
this diff; the exact test passed three consecutive isolated runs. No expected
output or unrelated global-state code was changed.
