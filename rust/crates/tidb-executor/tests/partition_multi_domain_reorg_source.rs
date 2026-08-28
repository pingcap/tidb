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

//! Port of `pkg/ddl/tests/partition/multi_domain_test.go` — the 36 tests
//! from `:44::TestMultiSchemaReorganizePartitionIssue56819` through
//! `:3128::TestNonClusteredUpdateReorgUpdate` (batch window items 865-900).
//!
//! Every test here is driven by `runMultiSchemaTest`
//! (`multi_domain_test.go:897`) or `runCoveringTest` (`:1812`): a TWO-domain
//! dist context (DDL owner + non-owner, `:906-:909`), an online ALTER whose
//! progress is observed PER SCHEMA STATE by polling
//! `information_schema.DDL_JOBS.schema_state` from the owner session, with
//! concurrent DML from a second session that does not yet see the new
//! schema — plus per-state insert/update/delete/delete-reorg coverage of
//! the global and local index entry sets, and post-state id/rowid audits.
//! This tier applies DDL synchronously to metadata with no job queue, no
// schema states, no second domain and no failpoints (the `crate::ddl`
//! module doc), so none of that harness is modelable; every test below is
//! an `#[ignore]` gap port whose contract is re-derived from its Go body.
//! Nothing here is approximated by a synchronous assertion: the POINT of
//! each Go test is the intermediate-state behavior.

use tidb_executor::Catalog;

/// Shared placeholder so the gap bodies compile without unused-crate
/// warnings; each gap test names the catalog its Go original would build.
fn catalog() -> Catalog {
    Catalog::default()
}

/// Go `multi_domain_test.go:44::TestMultiSchemaReorganizePartitionIssue56819`:
/// `alter table t reorganize partition p1 into (partition p0 values less
/// than (100), partition p1 values less than (200))` over a global-unique
/// indexed range table; in `delete only` state a concurrent insert `(4,4)`
/// must be visible through BOTH sessions via the global index (`:53-:57`).
// go-parity-gap: REORGANIZE PARTITION is 1105 here (measured); the
// delete-only global-index double-visibility window needs schema states.
#[test]
#[ignore]
fn multi_schema_reorganize_partition_issue56819() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:63::TestMultiSchemaDropRangePartition`: drop p0
/// of a 3-global-index range table; in `write only` the owner sees 1526
/// "Table has no partition for value matching a partition being dropped"
/// while the non-owner sees 1062 duplicates (`:81-:88`); in `delete only`
/// the roles flip and `select ... partition (pNonExisting)` answers 1735
/// (`:89-:120`).
// go-parity-gap: the write-only/delete-only partition-dropping windows,
// 1526/1735 refusals and dual-session reads need the online state machine.
#[test]
#[ignore]
fn multi_schema_drop_range_partition_state_ladder() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:141::TestMultiSchemaDropListDefaultPartition`:
/// the same drop-state ladder over `partition by list (a)` with a DEFAULT
/// partition bound.
// go-parity-gap: same missing online-DDL carriers as the range variant.
#[test]
#[ignore]
fn multi_schema_drop_list_default_partition_state_ladder() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:220::TestMultiSchemaDropListColumnsDefaultPartition`:
/// the same ladder over `list columns (a,b)` with DEFAULT, plus per-state
/// reads of `c` through the global unique index (`:246`, `:270`).
// go-parity-gap: same missing online-DDL carriers.
#[test]
#[ignore]
fn multi_schema_drop_list_columns_default_partition_state_ladder() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:311::TestMultiSchemaReorganizePartition`: split
/// p1 into (p0, p1) while inserts/updates stream in from both sessions in
/// every state; the delete-reorg `show create table` must show the NEW
/// definition set on the owner side (`:409-:424`) and the post state
/// audits the full 30-row result (`:432-:435`).
// go-parity-gap: REORGANIZE PARTITION + schema states + dual domains.
#[test]
#[ignore]
fn multi_schema_reorganize_partition_streams_dml_across_states() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:449::TestMultiSchemaPartitionByGlobalIndex`:
/// `alter table t partition by key (b,a) partitions 5 update indexes
/// (idx_ba global, idx_ab local)` — unique indexes convert global↔local
/// mid-DDL with per-state duplicate-entry visibility (`:466-:475`) and the
/// four conversion combinations checked in `postFn` (`:560-:578`).
// go-parity-gap: ALTER ... PARTITION BY + UPDATE INDEXES is 1105 here
// (measured), and GLOBAL unique indexes are refused at create time.
#[test]
#[ignore]
fn multi_schema_partition_by_global_index_conversions() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:580::TestMultiSchemaModifyColumn`:
/// `alter table t modify column b int unsigned not null` observed through
/// the write-reorg window where the owner writes BOTH column versions while
/// the non-owner reads the old one (`:586-:678`).
// go-parity-gap: the changing-column dual-write window needs the online
// reorg; this tier's modify is synchronous.
#[test]
#[ignore]
fn multi_schema_modify_column_dual_version_window() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:680::TestMultiSchemaModifyColumnConcurrentDMLAcrossPartitions`:
/// the same window on a table partitioned across the modified column, so
/// backfill and concurrent DML interleave per partition (`:686-:734`).
// go-parity-gap: same missing online reorg carriers.
#[test]
#[ignore]
fn multi_schema_modify_column_concurrent_dml_across_partitions() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:735::TestMultiSchemaDropUniqueIndex`: dropping
/// `uk_b` while DML streams; the index must stay constraint-enforcing until
/// delete-only, then stop being maintained (`:741-:796`).
// go-parity-gap: DROP INDEX mid-state visibility needs schema states.
#[test]
#[ignore]
fn multi_schema_drop_unique_index_state_ladder() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1155::TestMultiSchemaReorganizePK`: reorganizing
/// a range table whose PRIMARY KEY is the handle, with per-state DML and a
/// post-state admin check (`:1180-:1190`).
// go-parity-gap: REORGANIZE PARTITION + schema states.
#[test]
#[ignore]
fn multi_schema_reorganize_pk() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1194::TestMultiSchemaReorganizePKBackfillDML`:
/// the PK-handle variant with backfill-time DML injected per state
/// (`backfillDML`, `:1203-:1210`).
// go-parity-gap: same missing carriers, plus the backfill-DML injection
// hook (`runMultiSchemaTestWithBackfillDML`, :900).
#[test]
#[ignore]
fn multi_schema_reorganize_pk_backfill_dml() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1232::TestMultiSchemaReorganizeNoPK`: the
/// no-primary-key variant (nonclustered rowids), same ladder.
// go-parity-gap: REORGANIZE PARTITION + schema states.
#[test]
#[ignore]
fn multi_schema_reorganize_no_pk() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1273::TestMultiSchemaReorganizeNoPKBackfillDML`:
/// the no-PK variant with backfill-time DML.
// go-parity-gap: same missing carriers.
#[test]
#[ignore]
fn multi_schema_reorganize_no_pk_backfill_dml() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1314::TestMultiSchemaTruncatePartitionWithGlobalIndex`:
/// truncating a hash partition covered by a GLOBAL unique index; every
/// state must keep the global constraint enforced across the surviving
/// partition while the truncated one drains (`:1321-:1400`).
// go-parity-gap: the global unique index is refused at create time (1105,
// measured) and the state ladder is absent.
#[test]
#[ignore]
fn multi_schema_truncate_partition_with_global_index() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1503::TestMultiSchemaTruncatePartitionWithPKGlobal`:
/// the same ladder with the PRIMARY key as the global index
/// (`primary key nonclustered global`, `:1504`).
// go-parity-gap: the GLOBAL primary key itself is refused at create time on
// this tier (8264, measured); the state ladder is absent.
#[test]
#[ignore]
fn multi_schema_truncate_partition_with_pk_global() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1695::TestRemovePartitioningNoPKCovering` (via
/// `runCoveringTest`, `:1812`): `alter table t remove partitioning` while a
/// 4-dimension insert/update/delete/ODKU matrix (execute-state × from-state
/// × op × row-origin, `:1836-:1856`) replays against the 7 schema states.
// go-parity-gap: REMOVE PARTITIONING is 1105 here (measured) and the
// covering matrix is indexed by schema states.
#[test]
#[ignore]
fn remove_partitioning_no_pk_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1705::TestReorganizePartitionNoPKCovering`: the
/// covering matrix over a 10-way pMax split.
// go-parity-gap: REORGANIZE PARTITION + schema states.
#[test]
#[ignore]
fn reorganize_partition_no_pk_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1715::TestRePartitionByKeyNoPKCovering`: the
/// covering matrix over `alter table t partition by key(a) partitions 3`.
// go-parity-gap: ALTER ... PARTITION BY is 1105 (measured); schema states.
#[test]
#[ignore]
fn re_partition_by_key_no_pk_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1725::TestPartitionByKeyNoPKCovering`: the
/// covering matrix partitioning an UNPARTITIONED table by key.
// go-parity-gap: ALTER ... PARTITION BY is 1105 (measured); schema states.
#[test]
#[ignore]
fn partition_by_key_no_pk_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1731::TestAddKeyPartitionNoPKCovering`: the
/// covering matrix over `alter table t add partition partitions 1` (KEY).
// go-parity-gap: ADD PARTITION on a KEY table answers 1512 here (measured,
// range/list-only) and the matrix needs schema states.
#[test]
#[ignore]
fn add_key_partition_no_pk_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1737::TestCoalesceKeyPartitionNoPKCovering`: the
/// covering matrix over `alter table t coalesce partition 1`.
// go-parity-gap: COALESCE PARTITION is 1105 (measured); schema states.
#[test]
#[ignore]
fn coalesce_key_partition_no_pk_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1743::TestRemovePartitioningCovering`: the
/// covering matrix with a NONCLUSTERED primary key table.
// go-parity-gap: REMOVE PARTITIONING is 1105 (measured); schema states.
#[test]
#[ignore]
fn remove_partitioning_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1753::TestReorganizePartitionCovering`: the
/// covering matrix over the 10-way split, nonclustered-PK variant.
// go-parity-gap: REORGANIZE PARTITION is 1105 (measured); schema states.
#[test]
#[ignore]
fn reorganize_partition_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1763::TestRePartitionByKeyCovering`: the
/// covering matrix over the key re-partition, nonclustered-PK variant.
// go-parity-gap: ALTER ... PARTITION BY is 1105 (measured); schema states.
#[test]
#[ignore]
fn re_partition_by_key_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1773::TestPartitionByKeyCovering`: the covering
/// matrix partitioning a nonclustered-PK table by key.
// go-parity-gap: ALTER ... PARTITION BY is 1105 (measured); schema states.
#[test]
#[ignore]
fn partition_by_key_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1779::TestAddKeyPartitionCovering`: the covering
/// matrix over KEY-table ADD PARTITION, nonclustered-PK variant.
// go-parity-gap: ADD PARTITION on KEY answers 1512 (measured); schema states.
#[test]
#[ignore]
fn add_key_partition_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:1785::TestCoalesceKeyPartitionCovering`: the
/// covering matrix over COALESCE PARTITION, nonclustered-PK variant.
// go-parity-gap: COALESCE PARTITION is 1105 (measured); schema states.
#[test]
#[ignore]
fn coalesce_key_partition_covering_matrix() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2143::TestIssue58692`: during `alter table t
/// remove partitioning`, the `afterWaitSchemaSynced` failpoint streams
/// inserts/updates (`:2156-:2166`); afterwards the index-scan and table-scan
/// readings of `*, _tidb_rowid` must agree exactly and admin check stay
/// clean (`:2169-:2174`).
// go-parity-gap: REMOVE PARTITIONING is 1105 (measured), the failpoint and
// the `_tidb_rowid` projection are unported.
#[test]
#[ignore]
fn issue58692_remove_partitioning_backfill_matches_index_and_table_reads() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2176::TestDuplicateRowsNoPK`: an update fired in
/// the delete-reorganization state (`afterRunOneJobStep`, `:2184-:2191`)
/// during remove-partitioning must not duplicate rows; the final table holds
/// exactly `1 2` with rowid 1 (`:2193-:2195`).
// go-parity-gap: no failpoint/job-state carrier; `_tidb_rowid` unselectable.
#[test]
#[ignore]
fn duplicate_rows_no_pk_after_delete_reorg_update() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2200::TestDuplicateRowsPK59680` (issue 59680):
/// the nonclustered-primary-key variant of the same delete-reorg update.
// go-parity-gap: same missing carriers.
#[test]
#[ignore]
fn duplicate_rows_pk59680_after_delete_reorg_update() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2224::TestIssue58864`: DML fired in the
/// `job.State == done` window (`:2235-:2244`) during remove-partitioning
/// must not fail the DDL.
// go-parity-gap: no job queue / failpoint carrier.
#[test]
#[ignore]
fn issue58864_dml_in_job_done_window_does_not_fail_ddl() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2249::TestMultiSchemaNewTiDBRowID`: after
/// EXCHANGE PARTITION gives every partition rows whose `_tidb_rowid`s
/// collide (`:2267-:2292`), `alter table t coalesce partition 3` must
/// reallocate non-colliding rowids through the reorg; per-state admin checks
/// and the final rowid-exact read (`:2320-:2490`) pin the allocator.
// go-parity-gap: COALESCE PARTITION is 1105 (measured), EXCHANGE PARTITION
// is 1105, and `_tidb_rowid` is not selectable on this tier.
#[test]
#[ignore]
fn multi_schema_new_tidb_rowid_after_coalesce() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2491::TestBackfillConcurrentDML`: during
/// nonclustered backfill into new partitions, the
/// `PartitionBackfillNonClustered` failpoint forces a batch failure+retry
/// while duplicate `_tidb_rowid`s exist across exchanged partitions
/// (`:2521-:2560`); the retry must not duplicate rows in the new partitions.
// go-parity-gap: no failpoints, no backfill retry machine, no EXCHANGE
// PARTITION, no `_tidb_rowid`.
#[test]
#[ignore]
fn backfill_concurrent_dml_retry_does_not_duplicate() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2696::TestBackfillConcurrentDMLRange`: the same
/// retry-duplication probe over an INTERVAL range table
/// (`partition by range (a) interval (100) first partition less than (100)
/// last partition less than (900)`, `:2709`) with per-partition exchanges
/// (`:2711-:2734`).
// go-parity-gap: INTERVAL partitioning syntax, exchanges, failpoints and
// the reorg-info retry machinery are all absent on this tier.
#[test]
#[ignore]
fn backfill_concurrent_dml_range_retry_does_not_duplicate() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:2890::TestMultiSchemaReorgDeleteNonClusteredRange`:
/// merging three partitions into one (`reorganize partition p1,p2,p3 into
/// (partition newP1 values less than (300))`, `:2906`) while deletes and
/// updates move rows BETWEEN partitions in every state, with rowid-exact
/// per-state reads pinning which physical row the reorg keeps
/// (`:2907-:3127`).
// go-parity-gap: REORGANIZE PARTITION + schema states + `_tidb_rowid`.
#[test]
#[ignore]
fn multi_schema_reorg_delete_nonclustered_range_merges_three_partitions() {
    let _catalog = catalog();
}

/// Go `multi_domain_test.go:3128::TestNonClusteredUpdateReorgUpdate`: an
/// update during the write-reorganization window (`:3138-:3146`) must
/// remove the correct physical row when newFromMap entries collide with
/// newFromKey-only matches — final state `1 11 1`, `2 12 30001` (`:3148`).
// go-parity-gap: REMOVE PARTITIONING is 1105 (measured), the failpoint and
// rowid projection are unported.
#[test]
#[ignore]
fn nonclustered_update_reorg_update_removes_the_correct_row() {
    let _catalog = catalog();
}
