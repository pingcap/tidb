// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `#[ignore]` gap ports of all twenty-eight `pkg/ddl` part-16 tests in
//! `pkg/ddl/tests/tiflash/ddl_tiflash_test.go`
//! (`TestTiFlashNoRedundantPDRules` :192 through
//! `TestTiFlashAvailableAfterResetReplica` :1325).
//!
//! Every Go test in that file builds its world through
//! `createTiFlashContext` (:81): a unistore cluster inspected to add two
//! `engine=tiflash` stores, `infosync.NewMockTiFlash` (a mock TiFlash
//! status/status-server pair that records placement rules and answers sync
//! requests), and a background replica poller paced by
//! `ddl.PollTiFlashInterval`. The tier under test here has no TiFlash
//! replica poller, no PD placement-rule store, no mock TiFlash server and no
//! `ALTER TABLE/DATABASE ... SET TIFLASH REPLICA` carrier (the only
//! TiFlash-adjacent code is the BR restore recorder,
//! `crate::tiflash_recorder`), so each contract is recorded as a gap with
//! its Go source. Nothing is approximated.

/// Go `ddl_tiflash_test.go:192-278::TestTiFlashNoRedundantPDRules`: running
/// every DDL kind (create/drop/truncate/recover table, add/drop/reorganize
/// partition, schema drop/flashback, ...) against TiFlash-replicated tables
/// leaves the mock TiFlash's placement-rule set with NO redundant rules —
/// each physical id holds at most one rule, dropped tables' rules are
/// removed.
// go-parity-gap: no mock TiFlash/PD placement-rule store, no replica poller.
#[test]
#[ignore]
fn tiflash_ddls_leave_no_redundant_pd_placement_rules() {
}

/// Go `:280-319::TestTiFlashReplicaPartitionTableNormal`: setting
/// `tiflash replica 1` on a range-partitioned table marks every partition
/// available once the mock TiFlash reports sync, without deadlock.
// go-parity-gap: no SET TIFLASH REPLICA carrier, no replica poller.
#[test]
#[ignore]
fn tiflash_replica_on_partition_table_becomes_available() {
}

/// Go `:321-359::TestTiFlashReplicaPartitionTableBlock`: with the mock
/// TiFlash blocking sync, `admin alter table t set tiflash replica 0` still
/// completes while a partition is unavailable.
// go-parity-gap: no SET TIFLASH REPLICA carrier, no sync-blocking mock.
#[test]
#[ignore]
fn tiflash_replica_reset_completes_while_sync_is_blocked() {
}

/// Go `:361-395::TestTiFlashReplicaAvailable`: after
/// `alter table ddltiflash set tiflash replica 1` the poller flips
/// `TiFlashReplica.Available` within a bounded wait (`CheckTableAvailable`).
// go-parity-gap: no SET TIFLASH REPLICA carrier, no replica poller.
#[test]
#[ignore]
fn tiflash_replica_becomes_available_within_the_poll_interval() {
}

/// Go `:397-412::TestTiFlashTruncatePartition`: TRUNCATE PARTITION keeps the
/// replica count, replaces the partition's placement rule for the NEW
/// physical id, and the truncated partitions re-sync to available.
// go-parity-gap: no per-partition placement-rule store and no replica
// poller.
#[test]
#[ignore]
fn tiflash_truncate_partition_replaces_the_partition_rule() {
}

/// Go `:414-436::TestTiFlashFailTruncatePartition`: TRUNCATE PARTITION that
/// keeps failing drives `tidb_ddl_error_count_limit = 3` into job rollback
/// without leaking TiFlash rules.
// go-parity-gap: no job retry loop, no rule store.
#[test]
#[ignore]
fn tiflash_failing_truncate_partition_rolls_back_without_leaking_rules() {
}

/// Go `:438-452::TestTiFlashDropPartition`: DROP PARTITION removes the
/// dropped partition's placement rule (after GC) and keeps the remaining
/// partitions available.
// go-parity-gap: no per-partition placement-rule store and no GC hook.
#[test]
#[ignore]
fn tiflash_drop_partition_removes_the_dropped_rule() {
}

/// Go `:454-548::TestTiFlashFlashbackCluster`: FLASHBACK CLUSTER restores
/// dropped tables and re-establishes their TiFlash replica settings (via the
/// recorder semantics), with placement rules for every physical id
/// re-created and tables re-available.
// go-parity-gap: no flashback-cluster carrier, no rule store.
#[test]
#[ignore]
fn tiflash_flashback_cluster_restores_replicas_and_rules() {
}

/// Go `:550-575::TestTiFlashTruncateTable`: TRUNCATE TABLE on a
/// TiFlash-replicated partitioned table keeps the schema readable at once
/// (`ShouldCheckTiFlashReplica` timing) and re-establishes availability for
/// the new physical ids.
// go-parity-gap: no rule store, no replica poller.
#[test]
#[ignore]
fn tiflash_truncate_table_rebuilds_rules_for_new_ids() {
}

/// Go `:577-594::TestTiFlashMassiveReplicaAvailable`: 50 replicated tables
/// all reach Available — the poller's batching keeps up.
// go-parity-gap: no replica poller.
#[test]
#[ignore]
fn tiflash_massive_replicas_all_become_available() {
}

/// Go `:596-624::TestSetPlacementRuleNormal`:
/// `alter table ddltiflash set tiflash replica 1 location labels 'a','b'`
/// stores a placement rule carrying the location labels for the table id.
// go-parity-gap: no SET TIFLASH REPLICA carrier (no LOCATION LABELS parse
// surface), no rule store.
#[test]
#[ignore]
fn set_tiflash_replica_with_location_labels_stores_the_rule() {
}

/// Go `:626-676::TestSetPlacementRuleWithGCWorker`: the same set-replica
/// flow driven through a real GC worker pass keeps rules consistent across
/// delete-range processing.
// go-parity-gap: no GC worker and no rule store in this tier.
#[test]
#[ignore]
fn set_tiflash_replica_survives_a_gc_worker_pass() {
}

/// Go `:678-698::TestSetPlacementRuleFail`: when the mock TiFlash rejects
/// placement-rule writes, the poller retries instead of surfacing errors.
// go-parity-gap: no rule-write path to fail.
#[test]
#[ignore]
fn tiflash_placement_rule_failures_are_retried() {
}

/// Go `:700-778::TestTiFlashBackoffer`: the poller's backoffer caps ticks
/// (`maxTick 10`, rate 1.5) as configured.
// go-parity-gap: no poller/backoffer in this tier.
#[test]
#[ignore]
fn tiflash_poller_backoffer_respects_tick_limits() {
}

/// Go `:780-826::TestTiFlashBackoff`: with sync held unavailable and polling
/// paused, the backoff path schedules retries without busy-spinning.
// go-parity-gap: no poller/backoff path in this tier.
#[test]
#[ignore]
fn tiflash_sync_unavailability_backs_off() {
}

/// Go `:828-901::TestAlterDatabaseBasic`: `ALTER DATABASE ... SET TIFLASH
/// REPLICA` overrides per-table settings for every table in the schema
/// (including tables created afterwards and partitioned tables), and `SET
/// TIFLASH REPLICA 0` at the database level clears them.
// go-parity-gap: no ALTER DATABASE carrier, no replica setting store.
#[test]
#[ignore]
fn alter_database_set_tiflash_replica_overrides_tables() {
}

/// Go `:903-981::TestTiFlashBatchRateLimiter`: with
/// `tidb_batch_pending_tiflash_count` at a threshold, creating more tables
/// than the threshold with a pending replica blocks the session until the
/// poller drains the queue.
// go-parity-gap: no batch-create pending queue or rate limiter.
#[test]
#[ignore]
fn tiflash_batch_create_is_rate_limited_by_pending_count() {
}

/// Go `:983-1007::TestTiFlashBatchKill`: a rate-limited batchCreate blocked
/// on the pending count is interruptible via the SQL killer.
// go-parity-gap: no batch-create pending queue, no killer wiring.
#[test]
#[ignore]
fn tiflash_batch_create_is_killable_while_blocked() {
}

/// Go `:1009-1021::TestTiFlashBatchUnsupported`: `ALTER DATABASE ... SET
/// TIFLASH REPLICA` over a schema containing a VIEW answers
/// ErrViewAtDDLPosition (the unsupported-object guard), not the rate-limit
/// block.
// go-parity-gap: no ALTER DATABASE carrier to reach the object guard.
#[test]
#[ignore]
fn alter_database_tiflash_over_a_view_reports_the_unsupported_object() {
}

/// Go `:1023-1064::TestTiFlashProgress`: `information_schema.tiflash_tables`
/// / segment-style progress rows (`tiflash_progress` through
/// `CheckTableAvailable` helpers) report per-table replica progress between
/// 0 and 1 until available.
// go-parity-gap: no progress bookkeeping or information_schema carrier.
#[test]
#[ignore]
fn tiflash_progress_reads_between_zero_and_one_until_available() {
}

/// Go `:1066-1107::TestTiFlashProgressForPartitionTable`: progress for a
/// partitioned table aggregates over its partitions.
// go-parity-gap: no progress bookkeeping or information_schema carrier.
#[test]
#[ignore]
fn tiflash_progress_aggregates_over_partitions() {
}

/// Go `:1109-1126::TestTiFlashGroupIndexWhenStartup`: at startup the poller
/// reads the TiFlash group index so already-synced tables are not re-polled
/// from scratch.
// go-parity-gap: no poller, no group-index persistence.
#[test]
#[ignore]
fn tiflash_poller_honors_the_group_index_at_startup() {
}

/// Go `:1128-1177::TestTiFlashFailureProgressAfterAvailable`: after a table
/// becomes available, injected progress failures do not flip it back to
/// unavailable.
// go-parity-gap: no poller state machine.
#[test]
#[ignore]
fn tiflash_progress_failures_after_available_do_not_reset_the_flag() {
}

/// Go `:1179-1207::TestTiFlashProgressAfterAvailable`: progress reads keep
/// reporting 1 for available tables (no regression below the available
/// marker).
// go-parity-gap: no progress bookkeeping.
#[test]
#[ignore]
fn tiflash_progress_stays_at_one_after_available() {
}

/// Go `:1209-1236::TestTiFlashProgressAfterAvailableForPartitionTable`: the
/// same available-stability contract for partitioned tables.
// go-parity-gap: no progress bookkeeping.
#[test]
#[ignore]
fn tiflash_progress_stays_at_one_after_available_for_partitions() {
}

/// Go `:1238-1262::TestTiFlashProgressCache`: progress reads are cached per
/// poll tick, so repeated reads inside a tick see one snapshot.
// go-parity-gap: no progress cache.
#[test]
#[ignore]
fn tiflash_progress_reads_are_cached_per_tick() {
}

/// Go `:1264-1323::TestTiFlashProgressAvailableList`: the available-list
/// fast path marks tables available in bulk without per-table sync checks.
// go-parity-gap: no available-list fast path.
#[test]
#[ignore]
fn tiflash_available_list_marks_tables_in_bulk() {
}

/// Go `:1325-1350::TestTiFlashAvailableAfterResetReplica`: after Available
/// at replica 1, `set tiflash replica 2` re-runs the sync cycle to Available
/// at 2 (through `mockTiFlashStoreCount`), and `set tiflash replica 0`
/// clears `TiFlashReplica` to nil in the table meta.
// go-parity-gap: no SET TIFLASH REPLICA carrier, no replica poller, no
// store-count mock.
#[test]
#[ignore]
fn tiflash_replica_resyncs_after_raising_and_clears_after_reset() {
}
