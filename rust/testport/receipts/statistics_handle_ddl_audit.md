# `pkg/statistics/handle/ddl` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 48 | `34f701e7488bb6e0843b6609856a46e4c393d443` | build/test metadata inventoried |
| `ddl.go` | 171 | `520486c48817d5327b7df52f5c02e93c20dd7e30` | live behavior mapped into the cluster DDL/session owner |
| `ddl_test.go` | 1621 | `dd3a930a2807b630d73be0e9490ad80ff9812863` | all 24 tests inventoried below |
| `subscriber.go` | 682 | `d2d3406ff3358b45d2863950e99bfffd71f86523` | implemented actions and missing action owners inventoried below |
| `testutil/BUILD.bazel` | 16 | `cc82e4b4088bd98a4e9f786a5719414233b26629` | build metadata inventoried |
| `testutil/util.go` | 70 | `de5ee871e48ff017af80264fa3b12484663aeb6a` | transactional test-event support inventoried |

The package has no generated, platform-specific, benchmark, or fixture
artifacts beyond this inventory.

## Production behavior matrix

| Go behavior | Rust owner | Status |
| --- | --- | --- |
| capacity-1000 DDL event channel and notifier subscriber | cluster DDL completion path | architecture differs; no equivalent event channel exists |
| `HandleDDLEvent` logs and ignores subscriber errors | `cluster_session_node::run_ddl` + `handle_stats_ddl_result` | implemented; a post-commit stats error cannot replace DDL success |
| create-table physical-ID initialization | `update_table_ddl_stats` | implemented |
| truncate-table initialize-new then delayed-delete-old | `update_table_ddl_stats` | implemented |
| drop-table delayed deletion | `update_table_ddl_stats` | implemented |
| add-column initialization | `update_column_ddl_stats` | implemented |
| modify-column initialization unless DDL already analyzed | `update_column_ddl_stats` | initialization implemented; analyzed DDL branch has no Rust DDL producer |
| add partition | `update_partition_ddl_stats` | implemented |
| truncate partition: initialize, global count delta, retire | `update_partition_ddl_stats` | implemented |
| drop partition: global count delta, retire | `update_partition_ddl_stats` | implemented |
| exchange partition global count/modify deltas | none | missing with cluster exchange-partition DDL |
| reorganize partition initialize/retire without global delta | none | missing with cluster reorganize-partition DDL |
| alter table partitioning initialize and change global stats ID | none | missing with cluster add-partitioning DDL |
| remove partitioning change global ID and retire partitions | none | missing with cluster remove-partitioning DDL |
| flashback cluster table-wide stats-version update | storage helper exists; no cluster flashback DDL producer | missing event integration |
| add index no-op | cluster DDL path performs no stats write | implemented |
| drop schema best-effort delayed deletion | `update_drop_schema_stats` | implemented |
| global/static physical-ID selection | `stats_physical_ids` | implemented from the global prune-mode value |
| conditional historical metadata | `record_schema_change_history` | implemented |
| locked/unlocked delta writes | `cluster_stats_write` plans used by partition updates | implemented |
| post-event cache visibility | DDL path reloads affected physical IDs into the shared cache | required to reproduce Go handle-visible results after synchronous test-event handling |

## Complete Go test matrix

| Go test | Covered Rust behavior | Status |
| --- | --- | --- |
| `TestDDLAfterLoad` | DDL after initialized 1,000-row cache | covered |
| `TestDDLTable` | create/drop table and add-column stats | covered by integrated lifecycle/column tests |
| `TestSystemTableDDLHasNoEvent` | mem/system schema suppression | covered |
| `TestTruncateTable` | nonpartitioned table ID replacement | covered |
| `TestTruncateAPartitionedTable` | fresh physical IDs and retired versions for whole-table truncate | covered |
| `TestDDLHistogram` | add-column histogram default/null shapes | covered |
| `TestDDLPartition` | create/add/drop partition stats | covered for supported actions |
| `TestReorgPartitions` | reorganize partition | missing DDL owner |
| `TestIncreasePartitionCountOfHashPartitionTable` | hash partition reorganization | missing DDL owner |
| `TestDecreasePartitionCountOfHashPartitionTable` | hash partition reorganization | missing DDL owner |
| `TestTruncateAPartition` | one partition replacement and global count | covered |
| `TestTruncateAPartitionAndDropTableImmediately` | truncate/drop ordering | partial |
| `TestTruncateAHashPartition` | hash partition count/modify delta, new ID, retired version | covered |
| `TestTruncatePartitions` | multi-partition replacement and global count | covered |
| `TestDropAPartition` | one partition global count/removal | covered |
| `TestDropPartitions` | multi-partition global count/removal | covered |
| `TestExchangeAPartition` | exchange global count/modify and lock behavior | missing DDL owner |
| `TestExchangeAPartitionAndDropTableImmediately` | exchange/drop ordering | missing DDL owner |
| `TestRemovePartitioning` | global stats ID move and partition retirement | missing DDL owner |
| `TestAddPartitioning` | partition initialization and global stats ID move | missing DDL owner |
| `TestDropSchema` | best-effort physical/global retirement | covered |
| `TestExchangePartition` | concurrent exchange delta updates | storage delta primitive covered; exchange integration missing |
| `TestDumpStatsDeltaBeforeHandleDDLEvent` | pre-event stats-delta flush ordering | missing notifier/handle ordering |
| `TestDumpStatsDeltaBeforeHandleAddColumnEvent` | add-column pre-event delta flush ordering | missing notifier/handle ordering |

## Removed non-parity carriers

The disconnected `ddl_subscriber`, `ddl_physical_ids`, `ddl_stats_delta`, and
`ddl_queue_gate` compatibility modules and their mock-effect tests were removed
in earlier batches. The live implementation now sits on the real catalog,
transaction, `mysql.stats_*`, lock, historical-meta, and shared-cache owners.
The live DDL path retains its affected-ID cache reload because removing it
leaves Rust's immediately observable global statistics stale after partition
DDL, unlike the pinned handle behavior.

## Claim state

This package remains **in progress and unclaimed**. The inventory is complete,
but exact package parity still requires the cluster DDL owners for exchange,
reorganize, add/remove partitioning, and flashback, plus the notifier ordering
covered by the final two Go tests. No subset above is a package-completion
claim.

## Validation

WIP profile for the current live-path correction:

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
- `cargo check --locked -p tidb-server`
- `cargo fmt --all -- --check`
- `git diff --check`
