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

//! Port of Go `pkg/ddl/db_change_test.go` (part2 slice: `TestShowCreateTable`
//! through `TestParallelAddGeneratedColumnAndAlterModifyColumn`, 30 tests).
//!
//! Almost every test in this file drives Go's ONLINE schema-change state
//! machine: a failpoint hook (`afterWaitSchemaSynced` /
//! `beforeRunOneJobStep`) pauses a DDL job in a named intermediate state
//! (`StateDeleteOnly`, `StateWriteOnly`, `StateWriteReorganization`,
//! `StateDeleteReorganization`), runs DML against that intermediate schema,
//! and pins which columns the DML may name and which values land in the
//! underlying indexes (`runTestInSchemaState`, `db_change_test.go:792`). This
//! crate applies DDL synchronously — there is no job queue, no intermediate
//! schema state, and no failpoint layer (documented in `crate::ddl`'s module
//! header as the deferred "schema-version/DDL-job machinery") — so those
//! tests are recorded as ignored parity gaps, each keeping its Go recipe in
//! the doc comment for whoever closes the gap.
//!
//! `TestAlterIndexVisibility` is the exception: its observable is per-index
//! metadata, which this crate owns in full, so it is ported for real.

use crate::{Catalog, StmtContext, DEFAULT_DATABASE};

fn alter(sql: &str, catalog: &mut Catalog) -> Result<(), crate::DriverError> {
    crate::ddl::run_alter_table_in(sql, catalog, DEFAULT_DATABASE, &StmtContext::for_query())
}

/// Port of `pkg/ddl/db_change_test.go::TestAlterIndexVisibility` (line 946,
/// regression for issue #70049): ordinary indexes whose names carry an
/// underscore-delimited suffix (`idx_k_1`, `idx_k_copy`) must not be mistaken
/// for the temporary `_idx_k_<n>` indexes a modify column leaves behind, so
/// toggling ONE index's visibility flips exactly that index and leaves its
/// same-prefix siblings alone — in both directions.
///
/// Go reads the result from `information_schema.tidb_indexes.is_visible`;
/// that view is the session layer's projection of the index metadata this
/// crate owns (`KvIndex.visible`, `alter_metadata::alter_index_visibility_action`),
/// so the same contract is asserted on the flags themselves.
#[test]
fn alter_index_visibility_toggles_one_index_among_same_prefix_siblings() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE t_invisible (k INT, KEY idx_k (k), KEY idx_k_1 (k), KEY idx_k_copy (k))",
        &mut catalog,
    )
    .unwrap();

    alter(
        "ALTER TABLE t_invisible ALTER INDEX idx_k INVISIBLE",
        &mut catalog,
    )
    .expect("alter index idx_k invisible");
    let visibility = |catalog: &Catalog, name: &str| {
        let table = match catalog.table_in(DEFAULT_DATABASE, name) {
            Some(crate::TableEntry::Kv(table)) => table,
            _ => panic!("{name} is not a KV table"),
        };
        table
            .indexes()
            .iter()
            .map(|index| (index.name.clone(), index.visible))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        visibility(&catalog, "t_invisible"),
        vec![
            ("idx_k".to_owned(), false),
            ("idx_k_1".to_owned(), true),
            ("idx_k_copy".to_owned(), true),
        ]
    );

    // The reverse: three INVISIBLE keys declared at CREATE time, one made
    // visible again. Only the named index flips.
    crate::run_create_table_on(
        "CREATE TABLE t_visible (k INT, KEY idx_k (k) INVISIBLE, KEY idx_k_1 (k) INVISIBLE, \
         KEY idx_k_copy (k) INVISIBLE)",
        &mut catalog,
    )
    .unwrap();
    assert_eq!(
        visibility(&catalog, "t_visible"),
        vec![
            ("idx_k".to_owned(), false),
            ("idx_k_1".to_owned(), false),
            ("idx_k_copy".to_owned(), false),
        ]
    );
    alter(
        "ALTER TABLE t_visible ALTER INDEX idx_k VISIBLE",
        &mut catalog,
    )
    .expect("alter index idx_k visible");
    assert_eq!(
        visibility(&catalog, "t_visible"),
        vec![
            ("idx_k".to_owned(), true),
            ("idx_k_1".to_owned(), false),
            ("idx_k_copy".to_owned(), false),
        ]
    );
}

/// `pkg/ddl/db_change_test.go::TestShowCreateTable` (line 50): while `ALTER
/// TABLE ADD INDEX`/`ADD COLUMN` walks its intermediate states, `SHOW CREATE
/// TABLE` observed in the failpoint hook shows the job's own progress —
/// `idx` appears only once its job is done, the added column only after —
/// and the final text carries every completed change.
// go-parity-gap: the pinned behavior is SHOW CREATE TABLE output observed
// from a failpoint hook DURING the DDL's intermediate states; this crate has
// no intermediate states, no failpoints, and SHOW is session-layer.
#[test]
#[ignore = "go-parity-gap: intermediate-state SHOW CREATE TABLE needs the online DDL state machine"]
fn show_create_table_tracks_mid_ddl_index_and_column_states() {}

/// `pkg/ddl/db_change_test.go::TestDropNotNullColumn` (line 122, issue
/// #8654): dropping a NOT NULL column with a default (int, varchar, time,
/// json, and an expression default) must let a WRITE-ONLY-state insert set
/// only the remaining columns and succeed, because the dropped column's
/// default fills it.
// go-parity-gap: inserts are executed from a failpoint hook at
// StateWriteOnly; no intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state inserts need the online DDL state machine"]
fn drop_not_null_column_allows_write_only_inserts() {}

/// `pkg/ddl/db_change_test.go::TestTwoStates` (line 180): the four tested
/// statements (insert, insert naming the added column, update, replace) are
/// compiled and executed against `t` in each of the delete-only, write-only,
/// and write-reorganization states of `ALTER TABLE t ADD COLUMN d3 enum ...`,
/// pinning the compile-time "unknown column" errors per state and the
/// surviving rows.
// go-parity-gap: per-state compile/exec matrix driven from a failpoint hook;
// no intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: the two-states per-state compile/exec matrix needs the online DDL state machine"]
fn two_states_add_column_state_matrix() {}

/// `pkg/ddl/db_change_test.go::TestWriteOnlyWriteNULL` (line 447, PR #6249):
/// in the write-only state of `ADD COLUMN c5 int not null default 1 after
/// c4`, `INSERT ... ON DUPLICATE KEY UPDATE` writes NULL for the invisible
/// new column, and a later read reports the DEFAULT 1 (`select c4, c5` →
/// `8 1`).
// go-parity-gap: the DML runs at StateWriteOnly via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state ON DUPLICATE KEY inserts need the online DDL state machine"]
fn write_only_writes_null_for_new_column_on_dup_update() {}

/// `pkg/ddl/db_change_test.go::TestWriteOnlyOnDupUpdate` (line 458, PR
/// #6249): same write-only contract as above, after a DELETE that empties the
/// table; the duplicate-key path then inserts, and the read reports `2 1`.
// go-parity-gap: the DML runs at StateWriteOnly via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state ON DUPLICATE KEY inserts need the online DDL state machine"]
fn write_only_on_dup_update_sees_the_column_default() {}

/// `pkg/ddl/db_change_test.go::TestWriteOnlyOnDupUpdateForAddColumns` (line
/// 471): the two-column form (`add column c5 ..., add column c44 ...`) gives
/// the same write-only contract for BOTH added columns (`select c4, c5, c44`
/// → `2 1 1`).
// go-parity-gap: the DML runs at StateWriteOnly via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state multi-add-column DML needs the online DDL state machine"]
fn write_only_on_dup_update_for_add_columns() {}

/// `pkg/ddl/db_change_test.go::TestWriteReorgForModifyColumnTimestampToInt`
/// (line 484): in the write-reorganization state of `MODIFY COLUMN c1
/// bigint`, an insert lands with the timestamp value, and both rows read back
/// as `20200710010508` once the change finishes.
// go-parity-gap: DML runs at StateWriteReorganization via a failpoint hook;
// no intermediate states or backfill in this crate.
#[test]
#[ignore = "go-parity-gap: write-reorg-state modify-column DML needs the online DDL state machine"]
fn write_reorg_modify_column_timestamp_to_int() {}

/// `pkg/ddl/db_change_test.go::TestWriteReorgForModifyColumn` (line 509):
/// during `CHANGE COLUMN c cc tinyint not null default 1 first` in
/// write-reorganization state, DML must overflow (`[types:1690]constant 555
/// overflows tinyint`) through the index that still holds the old column
/// shape — pinning which columns PhysicalIndexScan's ToPB uses.
// go-parity-gap: state-machine DML plus index-scan column mapping during
// backfill; no intermediate states or backfill in this crate.
#[test]
#[ignore = "go-parity-gap: write-reorg-state change-column DML needs the online DDL state machine"]
fn write_reorg_modify_column() {}

/// `pkg/ddl/db_change_test.go::TestWriteReorgForModifyColumnWithUniqIdx`
/// (line 515): the same contract with a UNIQUE index on the changing column.
// go-parity-gap: state-machine DML plus index-scan column mapping during
// backfill; no intermediate states or backfill in this crate.
#[test]
#[ignore = "go-parity-gap: write-reorg-state change-column DML needs the online DDL state machine"]
fn write_reorg_modify_column_with_uniq_idx() {}

/// `pkg/ddl/db_change_test.go::TestWriteReorgForModifyColumnWithPKIsHandle`
/// (line 521): the same contract on a PKIsHandle table with a unique index
/// and a composite secondary index.
// go-parity-gap: state-machine DML plus index-scan column mapping during
// backfill; no intermediate states or backfill in this crate.
#[test]
#[ignore = "go-parity-gap: write-reorg-state change-column DML needs the online DDL state machine"]
fn write_reorg_modify_column_with_pk_is_handle() {}

/// `pkg/ddl/db_change_test.go::TestWriteReorgForModifyColumnWithPrimaryIdx`
/// (line 552): the same contract with a `primary index` (nonclustered PK).
// go-parity-gap: state-machine DML plus index-scan column mapping during
// backfill; no intermediate states or backfill in this crate.
#[test]
#[ignore = "go-parity-gap: write-reorg-state change-column DML needs the online DDL state machine"]
fn write_reorg_modify_column_with_primary_idx() {}

/// `pkg/ddl/db_change_test.go::TestWriteReorgForModifyColumnWithoutFirst`
/// (line 558): the change-column contract without a `FIRST` placement.
// go-parity-gap: state-machine DML plus index-scan column mapping during
// backfill; no intermediate states or backfill in this crate.
#[test]
#[ignore = "go-parity-gap: write-reorg-state change-column DML needs the online DDL state machine"]
fn write_reorg_modify_column_without_first() {}

/// `pkg/ddl/db_change_test.go::TestWriteReorgForModifyColumnWithoutDefaultVal`
/// (line 564): the change-column contract when the new column declares no
/// DEFAULT.
// go-parity-gap: state-machine DML plus index-scan column mapping during
// backfill; no intermediate states or backfill in this crate.
#[test]
#[ignore = "go-parity-gap: write-reorg-state change-column DML needs the online DDL state machine"]
fn write_reorg_modify_column_without_default_val() {}

/// `pkg/ddl/db_change_test.go::TestDeleteOnlyForModifyColumnWithoutDefaultVal`
/// (line 570): the change-column contract observed from the DELETE-ONLY state
/// instead.
// go-parity-gap: state-machine DML via a failpoint hook; no intermediate
// states in this crate.
#[test]
#[ignore = "go-parity-gap: delete-only-state change-column DML needs the online DDL state machine"]
fn delete_only_modify_column_without_default_val() {}

/// `pkg/ddl/db_change_test.go::TestWriteOnly` (line 622): in the write-only
/// state of `ADD COLUMN c5 int not null default 1 first`, delete/update/insert
/// that do NOT name the new column all succeed.
// go-parity-gap: DML runs at StateWriteOnly via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state DML needs the online DDL state machine"]
fn write_only_dml_without_naming_the_new_column() {}

/// `pkg/ddl/db_change_test.go::TestWriteOnlyForAddColumns` (line 635): the
/// two-column add form gives the same write-only contract.
// go-parity-gap: DML runs at StateWriteOnly via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state DML needs the online DDL state machine"]
fn write_only_for_add_columns() {}

/// `pkg/ddl/db_change_test.go::TestDeleteOnly` (line 648): in the delete-only
/// state of `DROP COLUMN c1`, statements naming `c1` fail with
/// `[planner:1054]Unknown column` (field list, where clause, multi-table
/// DELETE where/on clauses), reads still see the column, and the surviving
/// row reads `N 2017-07-01 00:00:00 8`.
// go-parity-gap: per-state planner errors via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: delete-only-state unknown-column contracts need the online DDL state machine"]
fn delete_only_drop_column_planner_errors() {}

/// `pkg/ddl/db_change_test.go::TestSchemaChangeForDropColumnWithIndexes`
/// (line 673): dropping an indexed column observed in the write-only,
/// delete-only, AND delete-reorganization states; DML that names the column
/// fails, DML that does not succeeds, and the table ends empty and
/// consistent.
// go-parity-gap: three-state drop-column matrix via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: the drop-column state matrix needs the online DDL state machine"]
fn schema_change_drop_column_with_indexes() {}

/// `pkg/ddl/db_change_test.go::TestSchemaChangeForDropColumnsWithIndexes`
/// (line 700): the same contract dropping TWO indexed columns at once.
// go-parity-gap: three-state drop-columns matrix via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: the drop-columns state matrix needs the online DDL state machine"]
fn schema_change_drop_columns_with_indexes() {}

/// `pkg/ddl/db_change_test.go::TestDeleteOnlyForDropExpressionIndex` (line
/// 727): while `DROP INDEX expr_idx` (an index on `(a+1)`) sits in
/// delete-only, deletes by other columns succeed and the hidden generated
/// column's data stays consistent (`admin check table` passes).
// go-parity-gap: delete-only-state drops of an expression index via a
// failpoint hook; no intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: delete-only-state expression-index drop needs the online DDL state machine"]
fn delete_only_drop_expression_index() {}

/// `pkg/ddl/db_change_test.go::TestDeleteOnlyForDropColumns` (line 744): in
/// the delete-only state of `DROP COLUMN c1, DROP COLUMN c3`, an insert
/// naming them fails with `[planner:1054]Unknown column 'c1'`.
// go-parity-gap: per-state planner errors via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: delete-only-state unknown-column contracts need the online DDL state machine"]
fn delete_only_for_drop_columns() {}

/// `pkg/ddl/db_change_test.go::TestWriteOnlyForDropColumn` (line 755): in the
/// write-only state of `DROP COLUMN c3`, updates naming `c3` fail with
/// `[planner:1054]Unknown column` while single- and multi-table updates that
/// avoid it succeed, and the surviving row reads `a N 8`.
// go-parity-gap: per-state planner errors via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state drop-column contracts need the online DDL state machine"]
fn write_only_for_drop_column() {}

/// `pkg/ddl/db_change_test.go::TestWriteOnlyForDropColumns` (line 774): the
/// two-column drop form, with the same write-only planner contracts.
// go-parity-gap: per-state planner errors via a failpoint hook; no
// intermediate states in this crate.
#[test]
#[ignore = "go-parity-gap: write-only-state drop-columns contracts need the online DDL state machine"]
fn write_only_for_drop_columns() {}

/// `pkg/ddl/db_change_test.go::TestShowIndex` (line 867): `SHOW INDEX` rows
/// observed during the intermediate add-index states show only `PRIMARY`
/// (nonclustered PK renders as a real index row), then both rows once the
/// job finishes; plus the clustered/nonclustered `SHOW INDEX` +
/// `information_schema.tidb_indexes` shapes for clustered and nonclustered
/// primary keys (int and char) on a range-partitioned and plain table.
// go-parity-gap: mid-state SHOW INDEX via a failpoint hook and the
// SHOW/information_schema projection live in the session layer; the executor
// owns only the index metadata.
#[test]
#[ignore = "go-parity-gap: mid-state SHOW INDEX needs the state machine plus the session-layer SHOW projection"]
fn show_index_intermediate_states_and_clustered_shapes() {}

/// `pkg/ddl/db_change_test.go::TestParallelAlterIndex` (line 969): two
/// concurrent `ALTER TABLE t ALTER INDEX idx1 INVISIBLE` jobs both succeed
/// and a later select works.
// go-parity-gap: concurrent DDL job scheduling is not modeled; the executor
// applies one ALTER at a time synchronously.
#[test]
#[ignore = "go-parity-gap: concurrent DDL job scheduling is not modeled in this crate"]
fn parallel_alter_index_invisible_twice() {}

/// `pkg/ddl/db_change_test.go::TestParallelAlterModifyColumn` (line 982):
/// two concurrent `MODIFY COLUMN b int FIRST` jobs both succeed.
// go-parity-gap: concurrent DDL job scheduling is not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: concurrent DDL job scheduling is not modeled in this crate"]
fn parallel_alter_modify_column_first() {}

/// `pkg/ddl/db_change_test.go::TestParallelAlterModifyColumnWithData`
/// (line 995): concurrent modify-columns on real rows — the loser reports
/// `[ddl:8245]column c id 3 does not exist, this column may have been updated
/// by other DDL ran in parallel`, the winner's double→int reorg lands `3`/`33`,
/// and the modify+rename and modify+change pairings read `2`/`22`.
// go-parity-gap: concurrent DDL job scheduling and the 8245 lost-race
// detection are not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: concurrent DDL scheduling and lost-race error 8245 are not modeled"]
fn parallel_alter_modify_column_with_data() {}

/// `pkg/ddl/db_change_test.go::TestParallelAlterModifyColumnToNotNullWithData`
/// (line 1068): concurrent null→not-null modify columns; the loser gets 8245,
/// NULL inserts are rejected, and the paired double/int not-null changes land
/// the documented rows including a `<nil>`.
// go-parity-gap: concurrent DDL scheduling and the 8245 lost-race detection
// are not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: concurrent DDL scheduling and lost-race error 8245 are not modeled"]
fn parallel_alter_modify_column_to_not_null_with_data() {}

/// `pkg/ddl/db_change_test.go::TestParallelAddGeneratedColumnAndAlterModifyColumn`
/// (line 1124): `ADD COLUMN f INT GENERATED ALWAYS AS(a+1)` racing
/// `MODIFY COLUMN a char(16)` makes the modify lose with
/// `[ddl:8200]Unsupported modify column: oldCol is a dependent column 'a' for
/// generated column`.
// go-parity-gap: concurrent DDL job scheduling is not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: concurrent DDL job scheduling is not modeled in this crate"]
fn parallel_add_generated_column_and_alter_modify_column() {}
