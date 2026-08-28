// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Ports of the `pkg/ddl/db_change_test.go` tests assigned to this batch
//! (origin/master, functions :50-:1063).
//!
//! The Go file's harness is the ONLINE schema-change state machine:
//! `runTestInSchemaState` (:800) freezes a job at one
//! `model.SchemaState` (delete-only / write-only / write-reorganization) via
//! the `afterWaitSchemaSynced`/`beforeRunOneJobStep` failpoints and runs DML
//! against the mid-state table; `testControlParallelExecSQL` races two DDL
//! statements. This tier applies DDL synchronously to metadata and has no
//! job queue, so the state-machine tests are `#[ignore]`d gaps carrying their
//! re-derived contracts. The two tests whose observable contracts are plain
//! final-state metadata — [`alter_index_visibility_matches_the_exact_index_not_its_suffix_siblings`]
//! and [`show_index_entries_follow_the_primary_key_storage`] — RUN here.

use crate::driver::{DEFAULT_DATABASE, TableEntry};
use crate::{Catalog, StmtContext, run_alter_table_in, run_create_index_in, run_create_table_on};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog).unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn index_names(catalog: &Catalog, table: &str) -> Vec<(String, bool)> {
    match catalog.get_table_for_test(table) {
        Some(TableEntry::Kv(kv)) => kv
            .indexes()
            .iter()
            .map(|index| (index.name.clone(), index.unique))
            .collect(),
        _ => panic!("{table} is not a KV table"),
    }
}

/// `db_change_test.go:946::TestAlterIndexVisibility` — regression test for
/// issue 70049.
///
/// Go asserts, through `information_schema.tidb_indexes`
/// (key_name, is_visible), that `ALTER TABLE .. ALTER INDEX idx_k INVISIBLE`
/// flips ONLY the exact name, leaving the underscore-suffixed siblings
/// `idx_k_1`/`idx_k_copy` alone — Go's `setIndexVisibility`
/// (`pkg/ddl/index.go:740-747`) matches `idx.Name.L == name.L` (plus a
/// changing/temp-index leg keyed on `GetChangingOriginName`, which is what
/// the suffix names must not be mistaken for). The second fixture flips
/// `idx_k` back VISIBLE among three create-time-invisible indexes.
///
/// Assertion surface: `tidb_indexes` is rendered by `tidb-session`; this tier
/// pins the field that table renders, `KvIndex.visible`
/// (`kv_table/table_meta.rs:541`), exactly as the in-crate test
/// `an_index_declared_invisible_is_maintained_but_never_planned` does.
#[test]
fn alter_index_visibility_matches_the_exact_index_not_its_suffix_siblings() {
    let mut catalog = Catalog::default();

    create(
        &mut catalog,
        "create table t_invisible (k int, key idx_k(k), key idx_k_1(k), key idx_k_copy(k))",
    );
    run_alter_table_in(
        "alter table t_invisible alter index idx_k invisible",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    let visible: Vec<bool> = match catalog.get_table_for_test("t_invisible") {
        Some(TableEntry::Kv(kv)) => {
            let mut names: Vec<(String, bool)> = kv
                .indexes()
                .iter()
                .map(|index| (index.name.to_ascii_lowercase(), index.visible))
                .collect();
            names.sort_by(|a, b| a.0.cmp(&b.0));
            names.into_iter().map(|(_, visible)| visible).collect()
        }
        _ => panic!("t_invisible is not a KV table"),
    };
    // Go's rows, ordered by key_name: idx_k NO, idx_k_1 YES, idx_k_copy YES.
    assert_eq!(
        visible,
        vec![false, true, true],
        "idx_k_1/idx_k_copy must keep Go's YES"
    );

    create(
        &mut catalog,
        "create table t_visible (k int, key idx_k(k) invisible, key idx_k_1(k) invisible, \
         key idx_k_copy(k) invisible)",
    );
    run_alter_table_in(
        "alter table t_visible alter index idx_k visible",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    let visible: Vec<bool> = match catalog.get_table_for_test("t_visible") {
        Some(TableEntry::Kv(kv)) => {
            let mut names: Vec<(String, bool)> = kv
                .indexes()
                .iter()
                .map(|index| (index.name.to_ascii_lowercase(), index.visible))
                .collect();
            names.sort_by(|a, b| a.0.cmp(&b.0));
            names.into_iter().map(|(_, visible)| visible).collect()
        }
        _ => panic!("t_visible is not a KV table"),
    };
    // Go's rows: idx_k YES, idx_k_1 NO, idx_k_copy NO.
    assert_eq!(visible, vec![true, false, false]);
}

/// `db_change_test.go:867::TestShowIndex`, final-state halves.
///
/// Go's SHOW INDEX / information_schema.tidb_indexes rows across four
/// primary-key storage variants and one partitioned table. The renderer is
/// `tidb-session`'s (and Go's `infoschema_reader.go:1520-1561`), but the
/// metadata it reads is built here, so the storage-shape facts are pinned at
/// their source:
///
/// - nonclustered int PK: PRIMARY is a REAL index entry (Go's tidb_indexes
///   row `PRIMARY ... Clustered NO` comes from `tb.Indices`).
/// - clustered int PK (`PKIsHandle`): NO PRIMARY entry — Go's row is
///   SYNTHESIZED by the reader (`infoschema_reader.go:1527-1553`), not stored
///   (this mirrors Go's `TableInfo`, whose `Indices` holds no PRIMARY for a
///   PKIsHandle table).
/// - clustered char(100) PK: the table is a COMMON handle
///   (`common_handle_offsets`), which is this tier's carrier for the fact Go
///   stores as a `Primary: true` IndexInfo (Go's copr path finds it via
///   `tables.FindPrimaryIndex`, `pkg/table/tables/tables.go:667`); this tier's
///   KvIndex list keeps no PRIMARY entry for it.
/// - nonclustered char(100) PK: a real PRIMARY entry again, rowid handle.
/// - the range-partitioned `tr`: `create index idx1` adds exactly one entry.
///
/// The FAILPOINT half of the Go test — SHOW INDEX observed DURING the
/// `alter table t add index c2(c2)` job's delete-only/write-only/reorg
/// states — is the `#[ignore]`d sibling below.
#[test]
fn show_index_entries_follow_the_primary_key_storage() {
    let mut catalog = Catalog::default();

    // :872-923 — t (c1 int primary key nonclustered, c2 int); after
    // `alter table t add index c2(c2)` both entries exist, PRIMARY first.
    create(
        &mut catalog,
        "create table t (c1 int primary key nonclustered, c2 int)",
    );
    run_create_index_in(
        "create index c2 on t (c2)",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        index_names(&catalog, "t"),
        vec![("PRIMARY".to_owned(), true), ("c2".to_owned(), false)],
        "Go's final SHOW INDEX: PRIMARY row then c2 row"
    );

    // :908-922 — the range-partitioned tr with `create index idx1 on tr
    // (purchased)`: exactly one index entry, no primary.
    create(
        &mut catalog,
        "create table tr (id int, name varchar(50), purchased date) \
         partition by range (year(purchased)) (partition p0 values less than (1990), \
         partition p1 values less than (1995), partition p2 values less than (2000), \
         partition p3 values less than (2005), partition p4 values less than (2010), \
         partition p5 values less than (2015))",
    );
    run_create_index_in(
        "create index idx1 on tr (purchased)",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        index_names(&catalog, "tr"),
        vec![("idx1".to_owned(), false)],
        "Go's SHOW INDEX from tr: one idx1 row"
    );

    // :924-927 — clustered int PK: handle IS the key, no PRIMARY entry.
    create(
        &mut catalog,
        "create table tr1 (id int primary key clustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr1").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(kv.pk_handle_offset().is_some());
        assert_eq!(
            index_names(&catalog, "tr1"),
            vec![("vv".to_owned(), false)],
            "Go's reader SYNTHESIZES this table's PRIMARY row (Clustered YES); nothing is stored"
        );
    }

    // :929-932 — nonclustered int PK: PRIMARY is a real entry, Clustered NO.
    // Go's own expected ROW order puts vv first (the table constraint becomes
    // IndexInfo[0], the inline PK is appended after it), which is exactly this
    // tier's `indexes()` order.
    create(
        &mut catalog,
        "create table tr2 (id int primary key nonclustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr2").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(kv.pk_handle_offset().is_none());
        assert_eq!(
            index_names(&catalog, "tr2"),
            vec![("vv".to_owned(), false), ("PRIMARY".to_owned(), true)]
        );
    }

    // :934-937 — clustered char(100) PK: a COMMON handle table. Go stores the
    // primary as a `Primary: true` IndexInfo (found by
    // tables.FindPrimaryIndex, tables.go:667) and renders Clustered YES from
    // it; this tier carries the handle itself and keeps no PRIMARY KvIndex.
    create(
        &mut catalog,
        "create table tr3 (id char(100) primary key clustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr3").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(
            !kv.common_handle_offsets().is_empty(),
            "clustered char PK is a common handle"
        );
        assert_eq!(index_names(&catalog, "tr3"), vec![("vv".to_owned(), false)]);
    }

    // :939-942 — nonclustered char(100) PK: PRIMARY entry, Clustered NO; row
    // order vv-then-PERIMARY as Go pins it for this shape too.
    create(
        &mut catalog,
        "create table tr4 (id char(100) primary key nonclustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr4").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(kv.pk_handle_offset().is_none());
        assert!(
            kv.common_handle_offsets().is_empty(),
            "a nonclustered char PK is NOT the handle"
        );
        assert_eq!(
            index_names(&catalog, "tr4"),
            vec![("vv".to_owned(), false), ("PRIMARY".to_owned(), true)]
        );
    }
}

/// `db_change_test.go:867::TestShowIndex`, failpoint half.
#[test]
#[ignore = "go-parity-gap: SHOW INDEX observed DURING the add-index job's delete-only/write-only/write-reorg states via afterWaitSchemaSynced; no job state machine here (Go db_change_test.go:79-100)"]
fn show_index_during_intermediate_states_hides_the_new_index() {
    // Derivation: on t (c1 int primary key nonclustered, c2 int), a fresh
    // session's `show index from t` at each of the three intermediate states
    // must still show ONLY the PRIMARY row — the new c2 row appears with the
    // job's public state.
}

/// `db_change_test.go:50::TestShowCreateTable`.
#[test]
#[ignore = "go-parity-gap: SHOW CREATE TABLE rendered from inside the afterWaitSchemaSynced hook at non-public states; needs the job machine and the SHOW CREATE renderer (Go db_change_test.go:50-119)"]
fn show_create_table_during_intermediate_states_shows_the_previous_shape() {
    // Derivation: five sequential ALTERs (add index idx, add index idx1, add
    // column c on t; add columns c, d on t2); at every non-public state a
    // `show create table` from the hook must render the shape BEFORE the
    // current DDL (e.g. after `alter table t add column c int` the rendered
    // text still has both KEYs but no `c`), with exact golden strings pinned
    // per step in the Go file.
}

/// `db_change_test.go:122::TestDropNotNullColumn` (issue #8654).
#[test]
#[ignore = "go-parity-gap: inserts issued inside the hook exactly at the DROP COLUMN job's StateWriteOnly across five column types (int, varchar, time, json, expression default); state machine out of this tier (Go db_change_test.go:122-178)"]
fn drop_not_null_column_writes_defaults_in_write_only_state() {
    // Derivation: t..t4 each hold one NOT NULL column (a int not null default
    // 11; b varchar(255) not null; c time not null; d json not null;
    // e varchar(256) default (REPLACE(UPPER(UUID()), '-', '')) not null).
    // During each `alter table .. drop column <col>`'s write-only state an
    // insert WITHOUT that column must succeed — the write-only column
    // supplies its default/origin value rather than erroring.
}

/// `db_change_test.go:180::TestTwoStates`.
#[test]
#[ignore = "go-parity-gap: executes the four-sql battery once per intermediate add-column state (delete-only / write-only / write-reorganization) through testExecInfo's compiled sessions; requires failpoint-rewritten binaries (Go db_change_test.go:180-344)"]
fn two_states_runs_the_dml_battery_against_each_add_column_state() {
    // Derivation: t (c1 int, c2 varchar(64), c3 enum('N','Y') not null
    // default 'N', c4 timestamp on update current_timestamp, key(c1,c2)).
    // `alter table t add column d3 enum('a','b') not null default 'a' after
    // c3`; at delete-only case 0 runs (insert with 4 columns), at write-only
    // case 1 compiles (unknown column d3), at write-reorganization case 2
    // executes plus case 1 executes against the write-only mock and case 3
    // compiles; after the DDL, case 4 executes and case 3 executes against
    // the old schema. Every expected error/rows pairing is pinned in the Go
    // file.
}

/// `db_change_test.go:447::TestWriteOnlyWriteNULL` (pull/6249).
#[test]
#[ignore = "go-parity-gap: runTestInSchemaState battery at the add-column job's write-only state (Go db_change_test.go:447-456)"]
fn write_only_write_null_fills_the_write_only_column() {
    // Derivation: insert t set c1='c1_new', c3='2019-02-12', c4=8 on
    // duplicate key update c1=values(c1) runs at StateWriteOnly of
    // `alter table t add column c5 int not null default 1 after c4`; then
    // `select c4, c5 from t` returns "8 1" — the write-only c5 takes its
    // default, not NULL.
}

/// `db_change_test.go:458::TestWriteOnlyOnDupUpdate`.
#[test]
#[ignore = "go-parity-gap: runTestInSchemaState battery at the write-only state (Go db_change_test.go:458-469)"]
fn write_only_on_dup_update_fills_the_write_only_column() {
    // Derivation: delete, then insert-on-duplicate (c1='c1_dup' hits the
    // existing row), then insert-on-duplicate with a new key; at the end
    // `select c4, c5 from t` is "2 1" — the duplicated row also receives
    // c5's default.
}

/// `db_change_test.go:471::TestWriteOnlyOnDupUpdateForAddColumns`.
#[test]
#[ignore = "go-parity-gap: runTestInSchemaState battery for a two-column ADD COLUMNS at the write-only state (Go db_change_test.go:471-482)"]
fn write_only_on_dup_update_for_add_columns_fills_both() {
    // Derivation: as TestWriteOnlyOnDupUpdate but the ALTER adds c5 AND c44
    // (`add column c5 int not null default 1 after c4, add column c44 int
    // not null default 1`); `select c4, c5, c44 from t` is "2 1 1".
}

/// `db_change_test.go:484::TestWriteReorgForModifyColumnTimestampToInt`.
#[test]
#[ignore = "go-parity-gap: timestamp->bigint CTC observed at write-reorganization with auto-increment inserts; state machine out of this tier (Go db_change_test.go:484-507)"]
fn write_reorg_for_modify_column_timestamp_to_int_preserves_values() {
    // Derivation: tt (id int primary key auto_increment, c1 timestamp
    // default '2020-07-10 01:05:08'); during `alter table tt modify column
    // c1 bigint`'s reorg an insert lands; `select c1 from tt` then shows
    // "20200710010508" twice — the timestamp rendered as the new bigint.
}

/// `db_change_test.go:509::TestWriteReorgForModifyColumn`.
#[test]
#[ignore = "go-parity-gap: testModifyColumn battery at write-reorganization (noneIdx shape) (Go db_change_test.go:508-513)"]
fn write_reorg_for_modify_column_runs_the_dml_battery() {
    // Derivation: tt (a varchar(64), b int default 1, c int not null default
    // 0, index idx(c), index idx1(a), index idx2(a, c)); `change column c cc
    // tinyint not null default 1 first`; 13 DML statements run at the reorg
    // state, with the tinyint overflow errors [types:1690] on 555/333, then
    // `admin check table tt` must pass.
}

/// `db_change_test.go:515::TestWriteReorgForModifyColumnWithUniqIdx`.
#[test]
#[ignore = "go-parity-gap: testModifyColumn battery with unique indexes at write-reorganization (Go db_change_test.go:514-519)"]
fn write_reorg_for_modify_column_with_uniq_idx_runs_the_dml_battery() {
    // Derivation: same battery over `.. unique index idx(c), unique index
    // idx1(a), index idx2(a, c)` — unique-index maintenance against the
    // changing column mid-reorg; `admin check table tt` must pass.
}

/// `db_change_test.go:521::TestWriteReorgForModifyColumnWithPKIsHandle`.
#[test]
#[ignore = "go-parity-gap: 12-statement battery against a PKIsHandle table during a CTC reorg, incl. use-index updates and replace; state machine out of this tier (Go db_change_test.go:521-550)"]
fn write_reorg_for_modify_column_with_pk_is_handle_runs_the_battery() {
    // Derivation: tt (a int not null, b int default 1, c int not null
    // default 0, unique index idx(c), primary key idx1(a) clustered, index
    // idx2(a, c)) rows (-1,-11),(1,11); `change column c cc tinyint not null
    // default 1 first` at reorg; the battery pins [types:1690] overflow
    // errors and every successful write through idx2; `admin check table tt`.
}

/// `db_change_test.go:552::TestWriteReorgForModifyColumnWithPrimaryIdx`.
#[test]
#[ignore = "go-parity-gap: testModifyColumn battery with a primary index at write-reorganization (Go db_change_test.go:552-556)"]
fn write_reorg_for_modify_column_with_primary_idx_runs_the_dml_battery() {
    // Derivation: `.. index idx(c), primary index idx1(a), index idx2(a, c)`
    // shape; same 13-statement battery at the reorg state.
}

/// `db_change_test.go:558::TestWriteReorgForModifyColumnWithoutFirst`.
#[test]
#[ignore = "go-parity-gap: testModifyColumn battery without FIRST at write-reorganization (Go db_change_test.go:558-562)"]
fn write_reorg_for_modify_column_without_first_runs_the_dml_battery() {
    // Derivation: same battery with `change column c cc tinyint not null
    // default 1` (no FIRST clause) at the reorg state.
}

/// `db_change_test.go:564::TestWriteReorgForModifyColumnWithoutDefaultVal`.
#[test]
#[ignore = "go-parity-gap: testModifyColumn battery without a default at write-reorganization (Go db_change_test.go:564-568)"]
fn write_reorg_for_modify_column_without_default_val_runs_the_dml_battery() {
    // Derivation: same battery with `change column c cc tinyint first` (no
    // default) at the reorg state.
}

/// `db_change_test.go:570::TestDeleteOnlyForModifyColumnWithoutDefaultVal`.
#[test]
#[ignore = "go-parity-gap: testModifyColumn battery at the DELETE-ONLY state; delete-only visibility of the changing column is out of this tier (Go db_change_test.go:570-620)"]
fn delete_only_for_modify_column_without_default_val_runs_the_dml_battery() {
    // Derivation: same battery as the reorg variant but at
    // StateDeleteOnly: the overflow-update arms become plain updates (the
    // delete-only changing column rejects nothing), `admin check table tt`.
}

/// `db_change_test.go:622::TestWriteOnly`.
#[test]
#[ignore = "go-parity-gap: runTestInSchemaState battery at the add-column job's write-only state (Go db_change_test.go:622-633)"]
fn write_only_hides_the_added_column_but_fills_it() {
    // Derivation: the standard t fixture; insert/update referencing c5-style
    // new columns at the write-only state error [planner:1054] Unknown
    // column, while existing-column DML keeps working and the final query
    // pins the defaults written behind the scenes.
}

/// `db_change_test.go:635::TestWriteOnlyForAddColumns`.
#[test]
#[ignore = "go-parity-gap: runTestInSchemaState battery for two added columns at the write-only state (Go db_change_test.go:635-646)"]
fn write_only_for_add_columns_hides_both_but_fills_them() {
    // Derivation: as TestWriteOnly with `add column c5 .., add column c44
    // ..` — both columns are invisible to mid-state DML yet receive their
    // defaults.
}

/// `db_change_test.go:648::TestDeleteOnly`.
#[test]
#[ignore = "go-parity-gap: runTestInSchemaState battery at the drop-column job's delete-only state (Go db_change_test.go:648-671)"]
fn delete_only_hides_the_dropped_column_from_every_dml() {
    // Derivation: `alter table t drop column c1` at StateDeleteOnly: five
    // statements referencing c1 (insert/update/delete values, two
    // multi-table deletes) all fail [planner:1054] Unknown column 'c1', and
    // `select * from t` still returns "N 2017-07-01 00:00:00 8".
}

/// `db_change_test.go:673::TestSchemaChangeForDropColumnWithIndexes`.
#[test]
#[ignore = "go-parity-gap: DML against a table whose dropped column is indexed, observed at three states (write-only, delete-only, delete-reorganization) (Go db_change_test.go:673-698)"]
fn schema_change_for_drop_column_with_indexes_survives_three_states() {
    // Derivation: t1 (a bigint unsigned not null primary key, b int, c int,
    // index idx(b)); the delete/insert/update battery runs at each of
    // StateWriteOnly, StateDeleteOnly, StateDeleteReorganization of
    // `alter table t1 drop column b`; final `select * from t1` is empty.
}

/// `db_change_test.go:700::TestSchemaChangeForDropColumnsWithIndexes`.
#[test]
#[ignore = "go-parity-gap: same battery with TWO indexed dropped columns across three states (Go db_change_test.go:700-725)"]
fn schema_change_for_drop_columns_with_indexes_survives_three_states() {
    // Derivation: t1 (.., b int, c int, d int, index idx(b), index idx2(d));
    // `alter table t1 drop column b, drop column d` at StateWriteOnly,
    // StateDeleteOnly, StateDeleteReorganization with the same battery.
}

/// `db_change_test.go:727::TestDeleteOnlyForDropExpressionIndex`.
#[test]
#[ignore = "go-parity-gap: delete against the hidden expression-index column at the drop-index job's delete-only state, then admin check table; state machine out of this tier (Go db_change_test.go:727-742)"]
fn delete_only_for_drop_expression_index_keeps_the_table_consistent() {
    // Derivation: tt (a int, b int) with index expr_idx((a+1)); during its
    // DROP INDEX's delete-only state `delete from tt where b=8` succeeds and
    // `admin check table tt` passes afterwards.
}

/// `db_change_test.go:744::TestDeleteOnlyForDropColumns`.
#[test]
#[ignore = "go-parity-gap: insert at the delete-only state of a two-column drop (Go db_change_test.go:744-753)"]
fn delete_only_for_drop_columns_hides_both_from_inserts() {
    // Derivation: `alter table t drop column c1, drop column c3` at
    // StateDeleteOnly; `insert t set c1=..` fails [planner:1054] Unknown
    // column 'c1' in 'field list'.
}

/// `db_change_test.go:755::TestWriteOnlyForDropColumn`.
#[test]
#[ignore = "go-parity-gap: update battery at the drop-column job's write-only state (Go db_change_test.go:755-772)"]
fn write_only_for_drop_column_still_reads_the_column() {
    // Derivation: at StateWriteOnly of `alter table t drop column c3`,
    // updates naming c3 fail [planner:1054] Unknown column (field list and
    // where clause arms, single- and multi-table), while the dropped
    // column's value survives reads: `select * from t` keeps "a N 8".
}

/// `db_change_test.go:774::TestWriteOnlyForDropColumns`.
#[test]
#[ignore = "go-parity-gap: update battery at the two-column drop job's write-only state (Go db_change_test.go:774-798)"]
fn write_only_for_drop_columns_still_reads_the_columns() {
    // Derivation: `alter table t drop column c3, drop column c1` at
    // StateWriteOnly; updates naming c1/c3 fail [planner:1054]; `select *
    // from t` keeps "N 8".
}

/// `db_change_test.go:969::TestParallelAlterIndex`.
#[test]
#[ignore = "go-parity-gap: races two `alter table t alter index idx1 invisible` via testControlParallelExecSQL; parallel DDL job control is out of this tier (Go db_change_test.go:969-980)"]
fn parallel_alter_index_is_idempotent_under_a_race() {
    // Derivation: both sessions' ALTERs succeed (the second is a no-op via
    // validateAlterIndexVisibility's early return), and `select * from t`
    // works afterwards.
}

/// `db_change_test.go:982::TestParallelAlterModifyColumn`.
#[test]
#[ignore = "go-parity-gap: races two identical MODIFY COLUMN jobs; needs the parallel-DDL conflict machinery (Go db_change_test.go:982-993)"]
fn parallel_alter_modify_column_converges() {
    // Derivation: two `ALTER TABLE t MODIFY COLUMN b int FIRST` race; both
    // succeed and `select * from t` works afterwards.
}

/// `db_change_test.go:995::TestParallelAlterModifyColumnWithData`.
#[test]
#[ignore = "go-parity-gap: three racing modify/rename pairs with [ddl:8245] conflict errors and post-race data expectations; parallel DDL out of this tier (Go db_change_test.go:995-1062)"]
fn parallel_alter_modify_column_with_data_reports_the_conflict() {
    // Derivation: two identical `MODIFY COLUMN c int` race — one wins, the
    // other fails "[ddl:8245]column c id 3 does not exist, this column may
    // have been updated by other DDL ran in parallel", and the surviving
    // table carries the double->int conversion ("3", then "33" after a
    // fresh insert). Pairs two and three (modify b double vs rename b->bb,
    // and modify b double vs change b bb int) both succeed with "2"/"22"
    // expectations.
}
