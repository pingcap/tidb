# `pkg/statistics/handle/handletest/statstest` package receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 28 | `87d3d57d5c38d97ac8e763964f1dd7c403782061` |
| `main_test.go` | 34 | `b67b9ea6815e74523833175bae8590006dfefc25` |
| `stats_test.go` | 872 | `f8c532b84dea4253643482cdc1e1eac2994fd69e` |

All 934 lines were read. The package contains 17 tests, one shared helper
suite, `TestMain`, and no benchmark, fixture, generated input, or platform
variant. Its Bazel target is flaky, race-enabled, and has `shard_count = 17`;
those runner settings are validation metadata rather than TiDB behavior.

## Rust ownership

This is an external behavior package. Its production mapping spans the native
Rust owners already used by the server:

- `tidb-stats-handle-cache` and its map/LFU implementations own lifecycle
  versions, copy/update, memory cost, rejection/eviction, and cache misses.
- `tidb-exec::{cluster_stats_write,cluster_stats_load,real_tikv_stats}` own the
  persisted statistics image and startup stages.
- `SharedStats` is the live cache authority, and the cluster ANALYZE, DDL,
  delta-dump, and schema-following paths publish through it.
- `StatsTarget::for_table` expands one logical table into the global ID and
  every current physical partition while excluding stale dropped IDs.

## Original-test mapping

| Pinned Go tests | Rust evidence |
| --- | --- |
| `TestStatsCacheProcess` | `analyze_publication_does_not_advance_the_cache_lifecycle_version` proves the newly corrected post-ANALYZE `SkipMoveForward` path; ordinary refresh still advances the lifecycle version through `StatsCacheImpl::update_from_source` |
| `TestStatsCache`, `TestStatsCacheMemTracker` | `source_refresh_reuses_payload_deletes_unknown_and_skips_load_errors`, `cache_update_preserves_and_refreshes_resident_histogram_payload`, `source_put_replace_delete_and_copy`, and LFU replacement/eviction tests cover schema drift, usable older stats, exact table-memory cost, and resident-payload accounting |
| `TestStatsStoreAndLoad` | `plan_stats_write` plus `ClusterStatsLoader::load_table` round trips meta, histogram, bucket, TopN, CMS, and existence state; `tidb-stats-handle-internal::assert_table_equal` owns the complete equality contract |
| `TestInitStatsMemTraceWithLite`, `TestInitStatsMemTraceWithoutLite`, `TestInitStatsMemTraceWithConcurrentLite`, `TestInitStatsMemTraceWithoutConcurrentLite` | map/LFU cost tests plus `initial_stats_matches_go_table_scope_and_payload_shapes`; the four Go tests call the same helper twice per lite setting and do not select a separate concurrent behavior branch |
| `TestInitStats`, `TestInitStatsForPartitionedTable` | `initial_stats_matches_go_table_scope_and_payload_shapes` covers analyzed, lite, non-lite, targeted/repeated/all-current physical IDs, highest ID, and stale dropped rows; production `StatsTarget::for_table` supplies logical/global and partition IDs |
| `TestInitStatsWithoutHandlingDDLEvent` | `initial_stats_handles_missing_histograms_and_topn_without_buckets` proves a `stats_meta` row without histogram metadata remains non-pseudo with no fabricated column or index object |
| `TestInitStatsVer2`, `TestInitStats51358`, `TestInitStatsIssue41938` | the same staged load retains metadata-only columns, fully loads indexes, leaves PK-column TopN absent, accepts synthesized/added-column metadata, and never decodes evicted timestamp-column payload during startup; cache failpoint coverage proves a forced miss independently |
| `TestDumpStatsDeltaInBatch` | production delta dump uses one transaction start timestamp for every statement in a 100,000-table batch; `stats_delta_batch_assigns_one_transaction_version_to_every_table` proves all persisted rows receive that one version |
| `TestInitStatsForTableWithTopNButNoBuckets` | `initial_stats_handles_missing_histograms_and_topn_without_buckets` proves TopN is resident and the index is full-load when no stored index bucket exists |
| `TestInitStatsMemoryFullBlocksBucketsButKeepsTopN` | `load_initial_stats_snapshot_with_memory_limits` now performs Go's histogram → TopN → bucket stages and rechecks the one-quarter-system-memory/live-quota policy between targets; `initial_stats_matches_go_table_scope_and_payload_shapes` proves a threshold crossed after TopN retains TopN, omits buckets, and leaves the index non-full |

`TestMain` only installs common Go test setup and leak-detector exclusions. Rust
does not port those as product APIs.

## Root fixes and parity decision

The old audit removed 17 ignored `unreachable!` functions from another Go
snapshot, but became stale after the cache/storage/server paths were wired.
The complete pinned package comparison found and fixed two remaining behavior
gaps:

1. Non-lite Rust startup loaded index TopN and buckets in one operation. It now
   uses Go's separate histogram/CMS, TopN, and bucket stages, checks both cache
   limits between work units, retains metadata after the limit is reached, and
   marks TopN-only indexes full only when no bucket rows exist.
2. Post-ANALYZE Rust publication advanced the cache lifecycle version. It now
   uses Go's targeted `SkipMoveForward` option in the default quota mode while
   preserving Go's legacy copy-on-write behavior when quota mode is disabled.

No ignored carrier, Rust-only eager payload path, separate cached statistics
runner, or test-only production feature remains. The complete external package
is claimed.

## Validation

Focused commands and the final Ready gate are recorded in the statistics
parity ExecPlan and batch commit.
