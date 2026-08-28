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

//! `#[ignore]` gap ports of the last four `pkg/ddl` part-17 tests in
//! `pkg/ddl/tests/tiflash/ddl_tiflash_test.go` —
//! `TestTiFlashPartitionNotAvailable` :1352,
//! `TestTiFlashAvailableAfterAddPartition` :1382,
//! `TestTiFlashAvailableAfterDownOneStore` :1417 and
//! `TestTiFlashReorgPartition` :1437 (the file's remaining part-16 tests are
//! covered by `tiflash_replica_ddl_source.rs`).
//!
//! Every one of these Go tests builds its world through
//! `createTiFlashContext` (`ddl_tiflash_test.go:75`): a unistore cluster with
//! added `engine=tiflash` stores, `infosync.NewMockTiFlash`, and a background
//! replica poller paced by `ddl.PollTiFlashInterval`
//! (`RoundToBeAvailable = 2`, `ddl_tiflash_test.go:71`). This tier has no
//! TiFlash replica poller, no PD placement-rule store, no mock TiFlash
//! sync-status server, no failpoints, and no `ALTER TABLE ... SET TIFLASH
//! REPLICA` carrier (the only TiFlash-adjacent code is the BR restore
//! recorder, `crate::tiflash_recorder`; the measured ALTER answer is the
//! generic `1105 this ALTER TABLE action is not supported yet` from
//! `ddl/alter_table.rs`'s catch-all arm). Each contract is recorded as a gap
//! with its Go source; nothing is approximated.

/// Go `ddl_tiflash_test.go:1352-1380::TestTiFlashPartitionNotAvailable`. On a
/// range-partitioned table with `tiflash replica 1`:
/// `MockTiFlash.ResetSyncStatus(partitionID, false)`
/// (`pkg/domain/infosync/mock_infosync.go`) makes the poller flip the TABLE's
/// `TiFlashReplica.Available` to false within
/// `PollTiFlashInterval * RoundToBeAvailable * 6`
/// (`waitTableReplicaStateWithTableName`, :531); resetting it true flips the
/// table back available; and a SECOND reset-to-false must NEVER flip an
/// available table back to unavailable (`require.Never` over
/// `PollTiFlashInterval * RoundToBeAvailable * 3`, :1369-1375) —
/// `CheckTableAvailable` (:519) closes by requiring the replica available at
/// count 1.
// go-parity-gap: no mock TiFlash sync-status store, no replica poller, no SET
// TIFLASH REPLICA carrier.
#[test]
#[ignore]
fn tiflash_partition_reset_sync_status_flips_and_then_cannot_downgrade_the_replica() {
}

/// Go `ddl_tiflash_test.go:1382-1415::TestTiFlashAvailableAfterAddPartition`.
/// A range-partitioned table's replica becomes available; then, with
/// failpoints `github.com/pingcap/tidb/pkg/ddl/sleepBeforeReplicaOnly` =
/// `return(2)`, `waitForAddPartition` = `return(3)` and
/// `PollTiFlashReplicaStatusReplaceCurAvailableValue` = `return(false)`
/// slowing and spoofing the poller, `ALTER TABLE ... ADD PARTITION` keeps the
/// table AVAILABLE throughout and finishes with two partition definitions.
// go-parity-gap: no replica poller, no failpoints, no ADD PARTITION-with-
// replica carrier.
#[test]
#[ignore]
fn tiflash_replica_stays_available_across_an_add_partition() {
}

/// Go `ddl_tiflash_test.go:1417-1435::TestTiFlashAvailableAfterDownOneStore`.
/// With failpoints `github.com/pingcap/tidb/pkg/ddl/OneTiFlashStoreDown` and
/// `github.com/pingcap/tidb/pkg/domain/infosync/OneTiFlashStoreDown` both
/// `return` (a store reported down), `alter table ddltiflash set tiflash
/// replica 1` still reaches `CheckTableAvailable` count 1: one down store
/// does not block a replica from becoming available.
// go-parity-gap: no store-topology mock, no replica poller, no SET TIFLASH
// REPLICA carrier.
#[test]
#[ignore]
fn tiflash_replica_becomes_available_even_with_one_store_down() {
}

/// Go `ddl_tiflash_test.go:1437-1504::TestTiFlashReorgPartition` (under
/// `TempDisableEmulatorGC`, :171). On a two-partition range table with
/// `tiflash replica 1` available, the partition `table-<pid>-r` placement
/// rule exists in the mock TiFlash (`GetPlacementRule`). A
/// `REORGANIZE PARTITION p0 INTO (...)` whose DeleteOnly state keeps failing
/// (failpoint `beforeRunOneJobStep`, first pass bumps `job.ErrorCount` to
/// 1000) is answered `[ddl] add partition wait for tiflash replica to
/// complete` (`pkg/ddl/partition.go:3470`, the reorganize wait raised when
/// `checkPartitionReplica` (`partition.go:469`) still reports the new
/// partitions' regions without TiFlash peers). A second
/// attempt, with the failpoint mocking TiFlash-store peers onto every new
/// partition's regions via the PD region cache, succeeds; `admin check table`
/// passes; the placement rule survives until a mocked GC worker
/// (`gcworker.NewMockGCWorker` + `DeleteRanges(MaxInt64)`) deletes the dropped
/// partition's ranges, after which the rule is gone.
// go-parity-gap: no job queue with ErrorCount retries, no failpoints, no PD
// placement-rule/region mocks, no GC worker, no SET TIFLASH REPLICA carrier.
#[test]
#[ignore]
fn reorganize_partition_waits_for_tiflash_replicas_and_gc_clears_the_rule() {
}
