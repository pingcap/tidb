# `pkg/statistics/handle/storage` package audit

Reference: TiDB Go commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

The package has 12 artifacts and 5,051 lines. Every artifact was read before
the Rust decision.

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 85 | `10ec62a5ebc6f674654b2e50718d7bd73ba5386c` | `496fbf4798e97f4bc76be95fc0edcd28f373bc70eb30b44acac3db9b4771e023` |
| `dump_test.go` | 707 | `cbfb3f574b752ad124ea1c55d323b0b80856f06d` | `c01109ba99aba2f490dc01b3ae45a2258737d93190d7287fc4fcdfaf7778cceb` |
| `gc.go` | 362 | `5409d0517ee04b153b4b2e51f533f6bd2b045cea` | `e9a7c86922f22a5ce92efa375e69812fa1c1b585a95d6db2fcdf4c7a65d18b2f` |
| `gc_test.go` | 159 | `5c09443c9e385bbab586975242fd63afc4768980` | `931b7e88a43ec6cfe3f84a09f1f3b97ade646e3de7efe874235cd98b0645ba71` |
| `json.go` | 341 | `2e13988ad8198164e629a911205961da908cfc5a` | `c495fa76a7efb5d37875a409d7ac3acdcfb2f0bae726f430ffe2be3e8fa6798d` |
| `read.go` | 1,080 | `eca8eaeffd90d958c304719b5a4ea0c67a74a118` | `e677e16c951eb55190d863d6d7535ab50caa39ed50d0e190421ce14b8a0260fe` |
| `read_test.go` | 402 | `50bde01f6ca0602dad7ba1c030b0df131d92253a` | `d8b66ef6e8636c51f21928b6eb747f8515baaa1ec93376143b10d0323eb1c875` |
| `save.go` | 579 | `f671304687b40541f5423394c33cc7c903097ba9` | `ac475156d5654e1e867817bd9f73c828380d55f5396dfaf76aa98ddaeb524adf` |
| `stats_read_writer.go` | 689 | `bd9bc956407975c29d440614580e4dc95f946ada` | `c077f52d1597446ed68e959f825d87c07d45fe4d9e76a84b9f198c3f6850a4ce` |
| `stats_read_writer_test.go` | 226 | `fa755c995519b1ad2a4f7f468308b00d85c5e18e` | `07ef9f1cb6d9d0eb783a98d9a3c8de4a1e9499817e302e9450dd02f5a21e0347` |
| `table_size_stats.go` | 224 | `96bf4a86a0f3d33a766a6cdd484a710478a03980` | `4e3dc7536df23b3ccf633308c2bd8b7384f59fdceb7b0a775c8e202143c4dddd` |
| `update.go` | 197 | `0db14574eda367605d4d3b4df9c6d649007833b5` | `86a99d9ba2002fba85de5c50327b9d0801abf193660fe0d9d53cb2d155d97553` |

The four test files contain 29 tests and no benchmarks. They validate the
integrated package through a mock store/domain: transactional save and delta
update, storage GC, typed histogram/sketch reconstruction, lazy cache loads,
JSON dump/load and legacy compatibility, partition/global statistics,
predicate usage, historical metadata, concurrent workers, failpoints, and
slow-save recovery.

## Package behavior

Go's minimum behavioral unit is `statsReadWriter` plus its package functions.
It obtains pooled session contexts, owns transaction boundaries and start
timestamps, reads and writes all statistics system tables, converts typed
histogram bounds and sketches, updates the ordinary statistics cache, records
history, observes infoschema and partition state, performs memory and SQL-kill
accounting, and coordinates concurrent JSON loading. The small arithmetic and
SQL-formatting expressions are private implementation details of those paths.

## Rust ownership map and current decision

The former disconnected arithmetic/SQL-string leaves and ignored test carrier
were removed. Production ownership now spans native boundaries rather than a
single crate:

- `tidb-executor::load_stats` owns JSON/protobuf conversion and gzip blocks.
- `tidb-exec::cluster_stats_load`, `cluster_stats_dump`,
  `real_tikv_stats_dump`, and `cluster_stats_write` own canonical storage
  reads, snapshot transaction boundaries, dumps, and mutation plans.
- `tidb-exec::real_tikv_stats`, `real_tikv_load_stats`, and
  `real_tikv_analyze` own real transaction boundaries and cache refresh.
- `tidb-server` owns the MySQL client-local transfer and cluster session
  integration.

The current package pass closed five source-proven orchestration gaps:

- Partition LOAD STATS now converts and persists inside the same capped worker
  boundary as Go, returns the first error, recovers worker panics as errors,
  and retains Go's direct nonpartitioned path.
- `PersistStatsBySnapshot` now invokes the callback for absent
  nonpartitioned statistics, skips absent partition/global statistics, keeps
  schema order, visits global last, and stops on the first error.
- `UpdateStatsMetaVersionForGC` now refreshes both metadata versions in one
  real transaction and performs gated historical-meta recording afterward in
  a separate best-effort transaction.
- `UpdateStatsVersion` now updates every `stats_meta.version` and
  `stats_histograms.version` from one transaction start TS without changing
  `last_stats_histograms_version`.
- `ReadColumnDistributionStats` now takes one PD timestamp and one read-only
  transaction, reads histogram metadata first, then TopN and buckets for
  Analyze v2, rejects negative null counts before payload reads, returns
  metadata-only full columns for older stats versions, and publishes no
  shared-cache mutation on failure. Its DDL auto-presplit consumer is a
  separate `pkg/ddl` package boundary.
- `ChangeGlobalStatsID` now moves exactly Go's six tables, in Go's order, in
  one real transaction. Both clustered and `_tidb_rowid` table layouts retain
  valid record/index keys, and a target-key collision aborts the whole plan as
  the corresponding SQL UPDATE would.

The obsolete standalone `tidb-session` LOAD STATS implementation and its tests
were removed: it read a server-local path and published only an in-memory
planner cache, while pinned Go receives bytes through client-local transfer,
persists `mysql.stats_*`, and refreshes through the ordinary cache path.

## Original-test behavior matrix

Every one of the pinned package's 29 tests is accounted for below. “Direct”
means the named Rust regression drives the same production boundary. “Split”
means the Go integration case crosses Rust crate boundaries and its assertions
are divided among the named production regressions; it is not treated as a
weaker package-completion claim.

| Pinned Go test | Rust production evidence | Status |
| --- | --- | --- |
| `TestConversion` | `load_stats::canonical_table_dumps_back_to_go_json_shape`, `json_builds_full_table_including_hidden_columns_and_fm_sketch`, and `cluster_stats_load::storage_image_builds_the_canonical_full_statistics_table` | Split |
| `TestDumpGlobalStats` | `cluster_stats_dump::dump_stats_to_json` applies the static/dynamic partition gate and always attempts `global`; `cluster_load_stats::present_partitions_load_only_named_entries_and_global` proves the matching physical IDs | Split |
| `TestLoadGlobalStats` | `cluster_load_stats::present_partitions_load_only_named_entries_and_global` | Direct |
| `TestLastStatsHistUpdateVersionAfterLoadStats` | `analyze_commit_size_source::loaded_stats_final_meta_update_preserves_unnamed_columns` | Direct |
| `TestLoadPartitionStats` | `cluster_load_stats::present_partitions_load_only_named_entries_and_global` plus `real_tikv_load_stats::partition_load_workers_sum_successful_item_counts` | Split |
| `TestLoadPredicateColumns` | `analyze_commit_size_source::loaded_stats_usage_replaces_timestamps_in_one_plan` | Direct |
| `TestLoadPartitionStatsErrPanic` | `real_tikv_load_stats::partition_load_workers_return_first_error_and_stop_claiming_tasks` and `partition_load_workers_recover_panics_as_errors` | Direct |
| `TestDumpPartitions` | `cluster_stats_dump::dump_stats_to_json` and `cluster_load_stats::present_partitions_load_only_named_entries_and_global` | Split |
| `TestDumpAlteredTable` | `cluster_stats_load::storage_image_builds_the_canonical_full_statistics_table` filters stored items through current `TableInfo`; `load_stats::json_builds_full_table_including_hidden_columns_and_fm_sketch` performs schema-shaped conversion | Split |
| `TestDumpPseudoColumns` | `load_stats::uninitialized_placeholder_keeps_realtime_count_and_is_pseudo` | Direct |
| `TestDumpVer2Stats` | canonical dump/load regressions in `tidb-executor::load_stats` plus durable bucket/TopN/CMS/FM round trips in `analyze_commit_size_source` | Split |
| `TestLoadStatsForNewCollation` | `cluster_stats_load::a_string_column_bound_stays_raw_bytes_because_it_may_be_a_collation_key` | Direct |
| `TestJSONTableToBlocks` | `load_stats::json_table_blocks_round_trip` | Direct |
| `TestLoadStatsFromOldVersion` | `load_stats::missing_stats_ver_infers_version_one_from_ndv` and serde's accepted legacy `ext_stats` field | Split |
| `TestPersistStats` | all three `cluster_stats_dump::persist_*` regressions | Direct |
| `TestGCStats` | `cluster_session_node::stats_gc_matches_go_item_and_dropped_table_phases` | Direct |
| `TestGCPartition` | `cluster_session_node::stats_gc_matches_go_partition_phases` | Direct |
| `TestGCColumnStatsUsage` | `cluster_session_node::stats_gc_matches_go_column_usage_cleanup` | Direct |
| `TestDeleteAnalyzeJobs` | `analyze_commit_size_source::analyze_job_lifecycle_and_timestamp_cleanup_match_go` and the cluster-session delete path | Direct |
| `TestExtremCaseOfGC` | `cluster_session_node::stats_gc_keeps_meta_for_existing_table_without_histograms` | Direct |
| `TestLoadStats` | `analyze_commit_size_source::lite_load_keeps_metadata_and_evicts_the_histogram_payload`, `async_global_stats_loads_each_payload_by_item`, and catalog async-load regressions | Split |
| `TestReadColumnDistributionStatsUsesOneSnapshot` | `analyze_commit_size_source::column_distribution_read_is_atomic_ordered_and_validated` plus `real_tikv_stats::read_column_distribution_stats_from_cluster` | Direct |
| `TestLoadNonExistentIndexStats` | catalog regressions `load_statistics_after_index_drop` and `failed_async_load_removes_the_item_and_returns_the_storage_error`; cluster loading treats absent metadata as a successful skip | Split |
| `TestColumnStatsIsInvalidSkipsInternalColumnID` | planner collection rejects nonpositive column IDs; `real_tikv_stats::load_needed_histograms_from_cluster` repeats the storage-boundary guard | Split |
| `TestLoadNeededHistogramsSkipsInternalColumnID` | `real_tikv_stats::needed_histogram_loading_skips_only_internal_columns`; `load_needed_histograms_from_cluster` removes every queued item after its attempt | Direct |
| `TestUpdateStatsMetaVersionForGC` | `update_stats_meta_version_for_gc`, `slow_save_version_refresh_changes_only_the_two_go_columns`, and historical-meta round-trip regressions | Split |
| `TestSlowStatsSaving` | `real_tikv_analyze::slow_global_stats_save_uses_go_five_lease_boundary` and `slow_save_version_refresh_changes_only_the_two_go_columns` | Split |
| `TestSlowStatsSavingForPartitionedTable` | ordinary and global writers both call `refresh_slow_stats_save_version`; `ordinary_and_global_slow_saves_share_go_refresh_and_error_policy` proves the common boundary | Direct shared policy |
| `TestFailedToHandleSlowStatsSaving` | `ordinary_and_global_slow_saves_share_go_refresh_and_error_policy` injects refresh failure and asserts Go's exact statement-visible error | Direct |

This atomic package is complete at its Go package boundary. The related
`ActionFlashbackCluster`, `ActionAlterTablePartitioning`, and
`ActionRemovePartitioning` storage calls are now reached through the ordinary
durable DDL subscriber. Their two- and six-statement sequences retain Go's
individual statement boundaries inside one pessimistic event transaction.

## Current WIP validation

- `cargo test --locked -p tidb-exec cluster_load_stats::tests:: -- --nocapture`
  passed: 5 tests.
- `cargo test --locked -p tidb-exec real_tikv_load_stats::tests:: -- --nocapture`
  passed: 4 tests.
- `cargo test --locked -p tidb-exec cluster_stats_dump::tests:: -- --nocapture`
  passed: 3 tests.
- `cargo test --locked -p tidb-exec --test all flashback_stats_version -- --nocapture`
  passed: 1 test.
- `cargo test --locked -p tidb-exec --test all global_stats_id -- --nocapture`
  passed: 2 tests.
- `cargo test --locked -p tidb-exec --lib needed_histogram_loading_skips_only_internal_columns -- --nocapture`
  passed: 1 test.
- `cargo test --locked -p tidb-exec --lib ordinary_and_global_slow_saves_share_go_refresh_and_error_policy -- --nocapture`
  passed: 1 test.
- `cargo test --locked -p tidb-exec --test all analyze_commit_size_source:: -- --nocapture`
  passed: 54 tests.
- `cargo test --locked -p tidb-exec --test all column_distribution_read_is_atomic_ordered_and_validated -- --nocapture`
  passed: 1 test.
- `cargo test --locked -p tidb-server --test all load_stats -- --nocapture`
  passed outside the filesystem/network sandbox: 2 tests.
- `cargo check --locked -p tidb-exec -p tidb-session -p tidb-server` passed.
- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `make lint` passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required. This is
an atomic package-completion receipt, not a repository-wide parity claim.

## 2026-09-02 Go-master source restoration

Against fetched Go master `78cac443a4f46c13bfe27eb247b5c80657952547`, the
working branch lacked the storage read implementation and table-size helper.
The complete 12-artifact source shape is restored, including one-snapshot
`ReadColumnDistributionStats` ordering/error behavior and statement-local
`TableSizeStats` reads that skip `mysql.stats_histograms` for TABLE_ROWS-only
requests. Focused failpoint-wrapped regressions cover the atomic snapshot and
the histogram-read skip; the full Go-master storage package suite passes in a
detached test worktree. The existing branch's broader statistics-handle
integration remains a separate dependency boundary.
