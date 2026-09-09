# `pkg/statistics/handle/ddl` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 48 | `34f701e7488bb6e0843b6609856a46e4c393d443` | build/test metadata inventoried |
| `ddl.go` | 171 | `520486c48817d5327b7df52f5c02e93c20dd7e30` | live behavior mapped into the cluster DDL/session owner |
| `ddl_test.go` | 1621 | `dd3a930a2807b630d73be0e9490ad80ff9812863` | all 24 tests inventoried below |
| `subscriber.go` | 682 | `d2d3406ff3358b45d2863950e99bfffd71f86523` | all admitted event actions mapped below |
The package has no generated, platform-specific, benchmark, support, or
fixture artifacts beyond this inventory. `ddl/testutil` is a distinct Go
package with its own atomic receipt and is not part of this claim.

## Production behavior matrix

| Go behavior | Rust owner | Status |
| --- | --- | --- |
| capacity-1000 DDL event channel and durable notifier subscription | DDL jobs publish `SchemaChangeEvent` rows; the statistics handler runs from the notifier owner | equivalent delivery and transaction boundary implemented without retaining a second compatibility channel |
| one pessimistic transaction per subscriber event | `ClusterNotifierSession::{begin_pessimistic,commit,rollback}` delegates to the ordinary session transaction controller | implemented; the production-notifier regression proves the internal session has an active transaction |
| `HandleDDLEvent` logs and ignores subscriber errors | `finish_stats_handler` | implemented; the event is marked processed instead of retried after a statistics failure |
| create/truncate/drop table physical-ID behavior | `stage_stats_notifier_event` and table-event producers | implemented |
| add/modify column initialization | `stage_stats_notifier_event` and column-event producers | implemented, including `INSERT IGNORE` preservation of later statistics |
| add/truncate/drop partition behavior | `stage_stats_notifier_event` and partition-event producers | implemented |
| exchange-partition global count/modify deltas | `SchemaChangeEvent::exchange_partition` subscriber branch and SQL DDL producer | implemented |
| reorganize-partition initialize/retire without global delta | `SchemaChangeEvent::reorganize_partitions` subscriber branch | implemented at this package's event boundary |
| alter table partitioning initialize and change global statistics ID | `SchemaChangeEvent::add_partitioning` subscriber branch | implemented at this package's event boundary |
| remove partitioning change global ID and retire partitions | `SchemaChangeEvent::remove_partitioning` subscriber branch | implemented at this package's event boundary |
| flashback cluster table-wide statistics version update | `SchemaChangeEvent::flashback_cluster` subscriber branch | implemented at this package's event boundary |
| `UpdateStatsVersion` statement boundaries | two ordered `stage_pessimistic_statement` calls for meta then histograms | implemented; both use the event transaction start TS and preserve `last_stats_histograms_version` |
| `ChangeGlobalStatsID` statement boundaries | six ordered `stage_pessimistic_statement` calls | implemented in Go's meta, TopN, FM sketch, buckets, histograms, column-usage order |
| add index no-op | subscriber dispatch performs no statistics write | implemented |
| drop schema best-effort delayed deletion | drop-schema subscriber branch | implemented |
| global/static physical-ID selection | subscriber target expansion | implemented from the global prune-mode value |
| conditional historical metadata | `record_schema_change_history` | implemented |
| locked/unlocked delta writes | shared statistics write statements used by partition events | implemented |
| post-event cache visibility | affected physical IDs reload through the shared cache | implemented |

## Complete Go test matrix

| Go test | Covered Rust behavior | Status |
| --- | --- | --- |
| `TestDDLAfterLoad` | DDL after initialized 1,000-row cache | covered |
| `TestDDLTable` | create/drop table and add-column stats | covered by integrated lifecycle/column tests |
| `TestSystemTableDDLHasNoEvent` | mem/system schema suppression | covered |
| `TestTruncateTable` | nonpartitioned table ID replacement | covered |
| `TestTruncateAPartitionedTable` | fresh physical IDs and retired versions for whole-table truncate | covered |
| `TestDDLHistogram` | add-column histogram default/null shapes | covered |
| `TestDDLPartition` | create/add/drop partition stats in static and dynamic prune modes | covered |
| `TestReorgPartitions` | reorganize partition initializes additions, retires removals, and preserves the global row | direct production subscriber regression |
| `TestIncreasePartitionCountOfHashPartitionTable` | same reorganize-event contract with multiple added IDs | covered by the definition-order production subscriber path |
| `TestDecreasePartitionCountOfHashPartitionTable` | same reorganize-event contract with multiple retired IDs | covered by the definition-order production subscriber path |
| `TestTruncateAPartition` | one partition replacement and global count | covered |
| `TestTruncateAPartitionAndDropTableImmediately` | truncate/drop ordering and delayed retirement | covered by independent committed notifier events and idempotent version refresh |
| `TestTruncateAHashPartition` | hash partition count/modify delta, new ID, retired version | covered |
| `TestTruncatePartitions` | multi-partition replacement and global count | covered |
| `TestDropAPartition` | one partition global count/removal | covered |
| `TestDropPartitions` | multi-partition global count/removal | covered |
| `TestExchangeAPartition` | exchange global count/modify and lock behavior | direct production subscriber regression plus SQL exchange producer coverage |
| `TestExchangeAPartitionAndDropTableImmediately` | exchange/drop ordering | covered by committed exchange then idempotent drop retirement |
| `TestRemovePartitioning` | six-table global ID move and partition retirement | direct production subscriber regression |
| `TestAddPartitioning` | partition initialization then six-table global ID move | direct production subscriber regression |
| `TestDropSchema` | best-effort physical/global retirement | covered |
| `TestExchangePartition` | concurrent exchange delta updates | exchange subscriber uses the shared locking delta statements in one pessimistic event transaction |
| `TestDumpStatsDeltaBeforeHandleDDLEvent` | a delayed create event preserves a previously flushed metadata row | direct production subscriber regression |
| `TestDumpStatsDeltaBeforeHandleAddColumnEvent` | delayed add-column events preserve later analyzed histograms | direct production subscriber regression |

## Removed non-parity carriers

The disconnected `ddl_subscriber`, `ddl_physical_ids`, `ddl_stats_delta`, and
`ddl_queue_gate` compatibility modules and their mock-effect tests were removed
in earlier batches. The live implementation now sits on the real catalog,
transaction, `mysql.stats_*`, lock, historical-meta, and shared-cache owners.
The live DDL path retains its affected-ID cache reload because removing it
leaves Rust's immediately observable global statistics stale after partition
DDL, unlike the pinned handle behavior.

## Claim state

This atomic package is **complete at its Go package boundary**. All four
artifacts and all 24 original tests are inventoried, every admitted event is
wired to the ordinary durable notifier and shared statistics storage/cache
owners, the subscriber uses one real pessimistic transaction, and Go's error
suppression and individual SQL-statement boundaries are preserved. SQL syntax
and DDL job producers owned by other Go packages remain claims of those
packages; their absence cannot be reassigned to this subscriber package.

## Validation

WIP profile for this atomic package pass:

- fail-before and pass-after:
  `cargo test --locked -p tidb-server --lib ddl_success_is_not_replaced_by_a_statistics_subscriber_error -- --nocapture`
- `cargo test --locked -p tidb-server --lib table_lifecycle_ddl_updates_statistics_like_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib add_column_ddl_initializes_statistics_like_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib drop_schema_ddl_retires_all_statistics_like_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib system_table_ddl_does_not_publish_statistics_events_like_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib modify_column_ddl_recreates_missing_default_statistics_like_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib truncate_partitions_refreshes_global_stats_meta_like_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib ddl_after_loaded_statistics_matches_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib truncate_partitioned_table_statistics_match_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib truncate_hash_partition_statistics_match_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib add_partition_statistics_follow_global_prune_mode_like_go -- --nocapture`
- `cargo test --locked -p tidb-server --lib drop_partitions_statistics_match_go -- --nocapture`
- `cargo test --offline -p tidb-server --lib reorganize_partition_event_updates_statistics_like_go -- --nocapture`
- `cargo test --offline -p tidb-server --lib add_and_remove_partitioning_events_move_statistics_like_go -- --nocapture`
- `cargo test --offline -p tidb-server --lib flashback_cluster_event_refreshes_statistics_versions_like_go -- --nocapture`
- `cargo test --offline -p tidb-server --lib exchange_partition_event_updates_global_statistics_like_go -- --nocapture`
- `cargo test --offline -p tidb-server --lib delayed_create_and_add_column_events_preserve_newer_statistics_like_go -- --nocapture`
- `cargo test --offline -p tidb-server --lib stats_notifier_uses_a_real_internal_transaction_like_go -- --nocapture`
- `cargo test --offline -p tidb-server --lib stats_handler_marks_a_failed_event_processed_like_go -- --nocapture`
- `cargo test --offline -p tidb-exec --test all flashback_stats_version_updates_meta_and_histograms_only -- --nocapture`
- `cargo test --offline -p tidb-exec --test all global_stats_id_moves_all_six_go_tables -- --nocapture`
- `cargo check --offline -p tidb-exec -p tidb-server`
- `cargo fmt --all -- --check`
- `git diff --check`
