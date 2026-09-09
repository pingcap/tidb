# `pkg/statistics/handle/lockstats` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 54 | `81f80a75142d23e4dbd7488f5cba936685a6d26b` |
| `lock_stats.go` | 305 | `b6465219b46882fa79fd90e5e96ddada68ab4801` |
| `lock_stats_test.go` | 336 | `7f6219fd8fe9f8520d373d8d59be931f02f116a4` |
| `main_test.go` | 34 | `52f60bbcecf9bbbcd9331f927f52a4683672d173` |
| `query_lock.go` | 55 | `cdf877d409985781d79fc89004b7585e0214ad8b` |
| `query_lock_test.go` | 153 | `93bef1a47a53fffb8bc9f2550a19297121357e5c` |
| `unlock_stats.go` | 218 | `8ccc848466eef012017d07418dbd4a4f46d0a6b2` |
| `unlock_stats_test.go` | 365 | `152d2073bf2de09b60ab5706661239fc54ea54fc` |

All 1,520 lines were read. The package contains three production files,
thirteen tests, one common `TestMain`, and one build target. It has no
generated or platform variant, fixture, benchmark, fuzz target, or example.

## Production mapping

`tidb-stats::lock_stats` owns Go's shared table/partition policy: lock-set
intersection, sorted skip diagnostics, whole-table gates, delta reads, count
clamping, physical-to-logical delta propagation, and error ordering.
`tidb-executor::stats_lock` and `tidb-session::stats_lock_arm` adapt that policy
to the staged in-process catalog. `tidb-exec::cluster_stats_lock`,
`tidb-exec::real_tikv_stats_lock`, and the server seam adapt the same policy to
the cluster system tables and ordinary session route.

The cluster adapter now mirrors `CallWithSCtx(..., FlagWrapTxn)` rather than
flattening the package into one optimistic mutation plan. It opens one
independent pessimistic transaction, resolves targets once, runs the initial
lock query at the transaction snapshot, and executes each Go restricted SQL
statement separately: lock insert, meta-version update, lock-delta select,
meta-delta update, and lock delete. Later statements read earlier staged
writes; only the conflicting statement is rebuilt at the advanced
`for_update_ts`; every failure rolls the transaction back. The duplicate
`ON DUPLICATE KEY UPDATE table_id = table_id` arm remains a lock-carrying
write. Transaction start TS supplies every version update.

The old public `plan_cluster_stats_lock` flattened planner and its Rust-only
`cluster_stats_lock_source.rs` tests had no production caller and encoded
different conflict behavior. They were removed rather than retained as a
second execution model.

## Original-test disposition

| Pinned test | Rust evidence |
| --- | --- |
| `TestGenerateSkippedTablesMessage`, `TestGenerateSkippedPartitionsMessage` | `tidb-stats::lock_stats` policy tests cover empty, singular, partial, all-skipped, sorted, lock, and unlock forms |
| `TestInsertIntoStatsTableLocked` | `lock_insert_and_meta_version_are_separate_go_statements` plus the catalog adapter test cover statement order, start-TS version, duplicate arm, and persistence |
| `TestAddLockedTables`, `TestAddLockedPartitions`, `TestAddLockedPartitionsFailed` | shared policy tests and ordinary session table/partition tests |
| `TestGetTablesLockedStatuses`, `TestQueryLockedTables` | shared set intersection plus catalog/live `SHOW STATS_LOCKED` reads |
| `TestGetStatsDeltaFromTableLocked`, `TestUpdateStatsAndUnlockTable` | missing-row zero behavior, count clamp, start-TS update, and catalog delta merge tests |
| `TestRemoveLockedTables`, `TestRemoveLockedPartitions`, `TestRemoveLockedPartitionsFailedIfTheWholeTableIsLocked` | shared policy tests, session gate test, and `stats_lock_live_pessimistic_transaction_merges_partition_delta_like_go` |

Go's `TestMain` performs common Go test setup and goroutine leak checking. Rust
has no package-level Go harness or goroutines, so it has no executable semantic
counterpart. Bazel scheduling metadata maps to the aggregate Cargo test target.

## WIP validation

- `cargo test --offline -p tidb-stats lock_stats -- --nocapture` passed: four
  focused policy tests.
- `cargo test --offline -p tidb-exec --lib lock_insert_and_meta_version_are_separate_go_statements -- --nocapture`
  passed.
- `cargo test --offline -p tidb-server --lib stats_lock_live_pessimistic_transaction_merges_partition_delta_like_go -- --nocapture`
  passed against embedded Unistore.
- `cargo test --offline -p tidb-session --lib stats_lock_statements_and_show_locked_share_the_persisted_store -- --nocapture`
  passed.
- `cargo test --offline -p tidb-session --lib stats_partition_unlock_obeys_the_whole_table_gate -- --nocapture`
  passed.
- `cargo check --offline -p tidb-exec -p tidb-server` passed with pre-existing
  warnings.
- `cargo fmt --all -- --check` and `git diff --check` passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required. This is
a WIP package-completion receipt, not a repository-wide Ready parity claim.
