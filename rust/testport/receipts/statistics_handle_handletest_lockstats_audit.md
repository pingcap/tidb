# `pkg/statistics/handle/handletest/lockstats` package receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 26 | `3844e6021902716601fc527a3e9f7c7155e95512` |
| `lock_partition_stats_test.go` | 543 | `bcc25057cc9102762d299053740aaf5d5235bce5` |
| `lock_table_stats_test.go` | 394 | `c4966a46c506e977cc29c445bbf8aa56b15cd294` |
| `main_test.go` | 34 | `52f60bbcecf9bbbcd9331f927f52a4683672d173` |

All 997 lines were read. The package contains 21 tests, `TestMain`, and no
benchmark, fixture, generated input, or platform variant. The Bazel target is
flaky and has `shard_count = 21`; Rust uses deterministic focused tests instead
of carrying those Go test-runner settings into production behavior.

## Rust ownership

The package is an external behavior suite rather than a production owner. Its
one complete Rust mapping spans the native boundaries that implement the same
behavior:

- `tidb-stats::lock_stats` owns the shared table/partition policy, sorted skip
  warnings, whole-table gates, delta merge, count clamping, and global updates.
- `tidb-session` and `tidb-exec::{cluster_stats_lock,real_tikv_stats_lock}` own
  SQL routing, statement warnings, `SHOW STATS_LOCKED`, independent lock
  transactions, and persisted `mysql.stats_table_locked` rows.
- `tidb-stats-handle-usage::prepare_delta_updates` makes a logical-table lock
  apply to new partition deltas, matching Go's add-partition inheritance path.
- `cluster_stats_write::plan_delete_table_stats` removes lock rows in the same
  hard statistics-GC path used after drop, truncate, and reorganize DDL. The
  partition DDL adapter derives inserted and retired physical IDs by name and
  ID; an exchange with unchanged IDs therefore changes no lock row.

## Original-test mapping

| Pinned Go tests | Rust evidence |
| --- | --- |
| `TestLockAndUnlockPartitionStats`, `TestLockAndUnlockPartitionsStats`, `TestLockAndUnlockPartitionStatsRepeatedly` | `partition_operations_match_go_whole_table_gate_and_stable_messages`; persisted target expansion in `table_lock_round_trips_through_the_persisted_cluster_rows` |
| `TestSkipLockPartition`, `TestUnlockOnePartitionOfLockedTableWouldFail`, `TestUnlockTheUnlockedTableWouldGenerateWarning`, `TestSkipLockALotOfPartitions` | `partition_operations_match_go_whole_table_gate_and_stable_messages`, including stable sorted names; `stats_partition_unlock_obeys_the_whole_table_gate` |
| `TestReorganizePartitionShouldCleanUpLockInfo`, `TestDropPartitionShouldCleanUpLockInfo`, `TestTruncatePartitionShouldCleanUpLockInfo` | `table_stats_delete_preserves_go_soft_and_hard_phases` proves the common hard-GC delete retracts `stats_table_locked`; the production DDL adapter sends every retired physical ID through that GC owner |
| `TestExchangePartitionShouldChangeNothing` | production partition-ID comparison emits no inserted or retired ID when the physical IDs are unchanged, and `apply_stats_ddl_change` is a no-op for two empty sets |
| `TestNewPartitionShouldBeLockedIfWholeTableLocked` | `partition_updates_follow_go_lock_and_global_rules` proves a delta for a new physical partition is persisted as locked when only its logical parent ID is locked |
| `TestUnlockSomePartitionsWouldUpdateGlobalCountCorrectly`, `TestUnlockPartitionedTableWouldUpdateGlobalCountCorrectly`, `TestDeltaInLockInfoCanBeNegative` | `table_unlock_merges_negative_partition_delta_into_partition_and_global_meta`, `table_lock_and_unlock_match_go_skip_and_delta_rules`, and `stats_delta_updates_match_go_positive_negative_and_locked_rows` |
| `TestLockAndUnlockTableStats`, `TestLockAndUnlockPartitionedTableStats`, `TestLockTableAndUnlockTableStatsRepeatedly`, `TestLockAndUnlockTablesStats` | `stats_lock_statements_and_show_locked_share_the_persisted_store`, `table_lock_round_trips_through_the_persisted_cluster_rows`, and shared multi-target policy tests |
| `TestDropTableShouldCleanUpLockInfo`, `TestTruncateTableShouldCleanUpLockInfo` | the same explicit hard-GC lock-row regression plus the table DDL adapter's complete retired-ID set |

The historical-stats failpoint in `TestLockAndUnlockTableStats` proves that a
best-effort history failure is not part of LOCK STATS success. Rust's lock
transaction has no history dependency, so it has the same observable result
without a Rust-only failure switch. `TestMain` contributes only shared test
bootstrap and goroutine-leak exclusions; neither is product behavior to port.

## Parity decision

The earlier receipt correctly removed 17 ignored empty functions from a
different Go snapshot, but became stale after the production lock, session,
delta-dump, and DDL-GC paths were wired. The complete pinned external package
is now claimed. No cache-only lock runner, eager DDL lock rewrite, extra
warning, or other Rust-only behavior was added.

## Validation

Focused commands are recorded in the statistics parity ExecPlan and the batch
commit. The package gate covers shared policy, logical-parent lock inheritance,
persisted negative-delta unlock, hard-GC lock cleanup, session integration,
formatting, diff hygiene, and the repository Ready lint gate.
