# `pkg/statistics/handle/cache` parity receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

This receipt covers the complete root Go package. The `internal`, `metrics`,
and `internal/*` directories are separate Go packages with their own atomic
receipts.

## Atomic inventory

| Artifact | Lines | Git blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 60 | `78a789a6e5f8577700aefd04a914ad402776b225` | `tidb-stats-handle-cache/Cargo.toml` owns the library, dependency, feature, test, and benchmark graph. |
| `stats_table_row_cache.go` | 212 | `682b9ba91df93b62c49e4e5c379758aafd0d2e23` | The process-wide row/column-length cache and estimators live in `tidb-stats-handle-cache`; fresh restricted storage reads and information-schema consumers are wired through `tidb-exec`, `tidb-server`, `tidb-session`, and `tidb-executor`. |
| `statscache.go` | 410 | `fd98ba1eb884e0b68b5daaa01ae8358a8d0d1c05` | `StatsCacheImpl`, ordered refresh, batch publication, cancellation, table reuse/reload/delete decisions, lifecycle, failpoint, metrics, and health buckets live in `tidb-stats-handle-cache`. |
| `statscacheinner.go` | 197 | `68b6a08e8e21ce683d55b9ddd49777661f9ffd93` | `StatsCache` selects the completed LFU or map backend, preserves their update modes, and owns the lifecycle maximum version. |
| `statscache_test.go` | 202 | `602fe311ace06d4ecbfb62bd64ded6191a2ef2cf` | The two original tests map to `source_batch_update_flushes_each_side_at_its_limit` and `source_healthy_metrics_use_exact_buckets`. |
| `bench_test.go` | 186 | `aaf4e64fd433a408d7393ead26cb78d27acce80e` | `benches/statscache.rs` retains all six LFU/map update, put/get, and get-only workload shapes in one compiling benchmark target. |

There are no generated files, platform variants, external fixtures, or other
test/support artifacts in this Go package.

## Production behavior

`rust/crates/tidb-stats-handle-cache/src/lib.rs` owns both Go caches:

- `StatsTableRowCache::update_by_id` performs the row-count read first and the
  non-index column-length read second, publishes neither map if either read
  fails, merges successful rows without clearing unrelated IDs, and clamps
  negative `tot_col_size` values to zero.
- `estimate_data_length` and `get_data_and_index_length` preserve fixed- and
  variable-width accounting, public-column/index filtering, partition-local
  versus global-index placement, aggregate partition row/data length,
  sequence row count, unsigned arithmetic, and zero-row average length.
- `StatsCacheImpl` preserves LFU in-place versus map copy-on-write selection,
  atomic replacement/close, offset scan version, ordered and deduplicated
  targeted refresh, ten-item independent update/delete batches, cancellation,
  metadata-only table reuse, storage reload, DDL disappearance, per-table
  read-error continuation, and targeted watermark suppression.
- `StatsCache` preserves hit/miss/update/delete accounting, retrying `Put`,
  copy/update/delete behavior, lifecycle max version, dynamic capacity,
  eviction/wait/close, and Go's single compare-and-swap attempt for each
  quota-mode update.

The information-schema boundary follows pinned Go rather than the former
Rust snapshot shortcut:

- `tidb-exec::cluster_stats_load` reads `mysql.stats_meta` and the
  `is_index=0` range of `mysql.stats_histograms` directly. Non-empty ID lists
  use deduplicated clustered-key prefixes, matching Go's restricted `IN`
  reads without scanning or decoding unrelated rows.
- `tidb-server::cluster_session_node` opens the two fresh storage snapshots,
  updates the one process-global cache only after both reads succeed, passes
  the statement's active resource group, and computes logical plus physical
  partition estimates from the current schema image.
- `tidb-session` plans against schema-only virtual tables first and makes the
  refresh decision from the resolved `PhysicalMemTable.Columns`, exactly where
  Go's retriever reads it. `TABLES` therefore refreshes only when its pruned
  scan retains one of the four size columns, while `PARTITIONS` refreshes on
  every scan because pinned Go deliberately does not prune that memory table.
  Read failures are warning-only and retain prior values.
- `tidb-executor` retains the logical aggregate tuple and each physical
  partition tuple independently, so both information-schema tables expose
  the same row, average, data, and index lengths as Go. Catalog epochs do not
  move when these independent cached estimates change.

The old catalog-build snapshot adapter and hard-coded partition zeros were
removed. The private Go SQL-string helper has no Rust production counterpart
because the native storage adapter performs keyed reads rather than building
SQL text; no public helper or standalone test surface remains.

## Test and benchmark mapping

- The two original `statscache_test.go` cases execute the same batch-boundary
  sequence and exact ten health gauge values.
- Cache-focused tests additionally exercise observable production contracts
  that Go tests in dependent packages: backend selection/update, refresh
  reuse/deletion/error handling, cancellation, targeted watermark behavior,
  atomic row-cache publication, negative-size clamping, partition/global
  index estimates, and the feature-gated failpoint.
- `table_row_cache_source_reads_requested_storage_rows` proves the real
  storage boundary, ID deduplication, unsigned counts, and index exclusion.
- `information_schema_partitions_reports_gos_rows` proves physical partition
  and unpartitioned row/length output.
- `mem_table_prunes_only_the_listed_tables_and_keeps_one_column` proves SQL's
  lowercase spelling resolves to Go's canonical, prunable `TABLES` identity.
- `loaded_column_ndv_reaches_grouped_cluster_plans` proves a pruned `TABLES`
  projection performs no refresh, a size projection refreshes and consumes
  its values, a retained scan inside a CTE is discovered, and an unpruned
  `PARTITIONS` projection refreshes even without a size column.
- The benchmark target retains all six names/workload pairs from
  `bench_test.go`; its executable entrypoint is the Rust equivalent of the
  Go daily benchmark aggregator.

## Validation

WIP profile commands used while iterating:

- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec --test all table_row_cache_source_reads_requested_storage_rows -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-session information_schema_partitions_reports_gos_rows -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-server loaded_column_ndv_reaches_grouped_cluster_plans -- --nocapture`

Ready profile, all passed (the first sandboxed `make lint` attempt could not
resolve `proxy.golang.org`; the identical approved-network rerun passed):

- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo check --manifest-path rust/Cargo.toml -p tidb-planner -p tidb-stats-handle-cache -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server`
- `cargo bench --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache --bench statscache --no-run`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache --features failpoints`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-planner mem_table_prunes_only_the_listed_tables_and_keeps_one_column -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-exec --test all table_row_cache_source_reads_requested_storage_rows -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-session information_schema_partitions_reports_gos_rows -- --nocapture`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-server loaded_column_ndv_reaches_grouped_cluster_plans -- --nocapture`
- `make lint`
