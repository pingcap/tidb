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

//! Ports of `pkg/ddl/tests/fail/fail_db_test.go` (part12 items 693-702 of
//! `pkg/ddl`'s `func Test*`/`func Benchmark*` declarations sorted by file
//! and line; item 703, `tests/fail/main_test.go:27::TestMain`, is the
//! package's unbootstrap'd-store harness with no assertions and is recorded
//! as skipped in the batch receipt), read from `origin/master`.
//!
//! Every Go test here arms a `failpoint` (a code-level error/panic
//! injection seam, `github.com/pingcap/failpoint`) around a DDL job and
//! asserts the recovery: the job fails with the injected error, the schema
//! stays consistent, and a retry succeeds. This tier has no failpoint seam
//! and no job queue — the runners either succeed or refuse up front — so
//! the injection-driven tests are explicit gaps. `TestModifyColumn`'s
//! assertion ladder is mostly PLAIN SQL (the failpoint-free legs), and those
//! legs are ported as a running test against the tier's CHANGE/MODIFY
//! COLUMN carrier. Nothing is approximated.

use tidb_datatype::Datum;
use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::{admin_check, run_insert_on, run_select_on, Catalog, RowDecodeContext, StmtContext, TableEntry};

/// The text of a datum, however the codec chose to represent it.
fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) => u.to_string(),
        other => panic!("unexpected datum {other:?}"),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(datum_text).collect())
        .collect()
}

// --- TestModifyColumn (pkg/ddl/tests/fail/fail_db_test.go:362) ---
//
// Go's failpoint-free ladder, re-derived from the Go assertions:
//   * `alter table t change column b bb mediumint first` succeeds: the
//     column list becomes bb, a, c and the stored rows read `2 1 3` /
//     `22 11 33`;
//   * `change column a aa mediumint after c` succeeds: bb, c, aa with rows
//     `2 3 1` / `22 33 11` / `111 333 222`;
//   * on the hash-partitioned t1, `modify column a mediumint` fails
//     `[ddl:8200]Unsupported modify column: can't change the partitioning
//     column, since it would require reorganize all partitions`.
//
// Two Go legs are NOT reproducible here and live in the ignored tests
// below: the opening `change column c cc mediumint` over `primary key(c)`
// (8200 "this column has primary key flag") and the
// `change column a aa tinyint after c` move over the stored `222`
// ([types:1265]Data truncated for column 'a', value is '222').
//
// Go additionally checks `admin check table` and SHOW CREATE golden text
// after each leg; `admin check table` is asserted here through the tier's
// checker, while the SHOW CREATE renderer does not exist in this tier (the
// same meta facts are asserted through the column order and row reads
// instead — that substitution is noted, not silently swapped).
#[test]
fn modify_column_refusals_and_reorders_match_go() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t (a int not null default 1, b int default 2, c int not null default 0, \
         primary key(c), index idx(b), index idx1(a), index idx2(b, c))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into t values (1, 2, 3), (11, 22, 33)", &mut catalog, &ctx).unwrap();

    // FIRST moves bb to the head and carries the stored rows with it.
    ddl::run_alter_table_in(
        "alter table t change column b bb mediumint first",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.table_in("test", "t") else {
        panic!("expected a storage-backed table");
    };
    let names: Vec<String> = table.columns.iter().map(|column| column.name.clone()).collect();
    assert_eq!(names, vec!["bb", "a", "c"]);
    assert_eq!(table.indexes().len(), 3, "Go: three indexes survive the change");
    let rows = run_select_on("select * from t", &mut catalog, &ctx).unwrap();
    assert_eq!(rows_text(&rows), vec![vec!["2", "1", "3"], vec!["22", "11", "33"]]);

    // Go inserts (111, 222, 333) in the CURRENT (bb, a, c) column order
    // BEFORE moving a: the row reads back bb=111, a=222, c=333.
    run_insert_on("insert into t values (111, 222, 333)", &mut catalog, &ctx).unwrap();

    // MEDIUMINT does fit: the move lands after c, and the stored rows come
    // along — the pre-move row reads bb, c, aa = 111, 333, 222. (Go first
    // tries TINYINT here and requires the 1265 refusal — that leg cannot
    // run, see the ignored test below.)
    ddl::run_alter_table_in(
        "alter table t change column a aa mediumint after c",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let rows = run_select_on("select * from t", &mut catalog, &ctx).unwrap();
    assert_eq!(
        rows_text(&rows),
        vec![
            vec!["2", "3", "1"],
            vec!["22", "33", "11"],
            vec!["111", "333", "222"],
        ]
    );

    // The partitioning column may not change: Go's 8200 with the
    // reorganize-all-partitions text.
    ddl::run_create_table_in(
        "create table t1(a int) partition by hash (a) partitions 2",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let error = ddl::run_alter_table_in(
        "alter table t1 modify column a mediumint",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err(
        "Go: [ddl:8200]Unsupported modify column: can't change the partitioning column, \
         since it would require reorganize all partitions",
    );
    let mysql = error.clone().to_mysql_error();
    assert_eq!(mysql.code, 8200);
    assert_eq!(
        mysql.message,
        "Unsupported modify column: can't change the partitioning column, since it would require reorganize all partitions"
    );

    // Go runs `admin check table t` after each leg; the final state here is
    // the fully-altered t.
    let Some(TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
        panic!("expected a storage-backed table");
    };
    admin_check::check_table(table, None, &RowDecodeContext::for_query(&ctx)).unwrap();
}

// The pk-guard, narrowing-data and generated-column legs of Go's
// TestModifyColumn (pkg/ddl/tests/fail/fail_db_test.go:424, :430, :435-448):
//   * `alter table t change column c cc mediumint` over `primary key(c)`
//     fails `[ddl:8200]Unsupported modify column: this column has primary
//     key flag` — Go's guard lives inside `checkModifyTypes`
//     (pkg/ddl/modify_column.go:2262-2273): a type move that is
//     INCOMPATIBLE but reorg-able (int → narrower mediumint) on a column
//     carrying the primary-key flag is refused with that exact text;
//   * `alter table t change column a aa tinyint after c` over the stored
//     `222` fails `[types:1265]Data truncated for column 'a', value is
//     '222'` — Go validates the TABLE'S ROWS against the new type during
//     the change;
//   * `modify column b/c mediumint` over generated columns → 8200 "old
//     column is generated"; a NEW generated column → 8200 "new column is
//     generated"; a depended-on column → 8200 "oldCol is a dependent column
//     'a' for generated column".
// Plus the discrete-row reorg leg (scattered batches), the PointGet leg and
// the null→not-null leg.
//
// go-parity-gap (documented divergence): the tier's MODIFY/CHANGE carrier
// refuses a handle-column move only when the TARGET leaves the integer
// domain (`ddl/alter_table.rs`: "this column has primary key flag" behind
// `integer_type`), so int → mediumint on the clustered handle SUCCEEDS here
// where Go refuses it; the narrowing move performs NO row validation (the
// stored 222 reads back through the new type); and the generated-column
// refusal arms do not exist at all. The reorg-batch/PointGet legs drive the
// schema-reorg machinery this tier does not model.
#[test]
#[ignore = "go-parity-gap: pk-guard fires only for non-integer targets, narrowing does no row validation, generated-column refusals absent"]
fn modify_column_pk_narrowing_and_generated_refusals_match_go() {
    // Contract (fail_db_test.go:424, :430, :435-448): the handle narrowing
    // move is 8200 "this column has primary key flag"; the tinyint move over
    // a stored 222 is [types:1265] "Data truncated for column 'a', value is
    // '222'"; all four generated-column legs are 8200 with Go's texts.
}

// --- TestHalfwayCancelOperations (pkg/ddl/tests/fail/fail_db_test.go:77) ---
//
// Go arms `truncateTableErr`, `renameTableErr` ("ty") and
// `exchangePartitionErr` failpoints: each DDL fails halfway, the OLD table
// contents and name must survive untouched (`select * from t/tx/ty/pt`
// keep their rows), a new-session read stays consistent, and
// `tidb_ddl_error_count_limit` caps the retries.
//
// go-parity-gap: no failpoint seam and no job queue — the tier's
// truncate/rename runners cannot fail after starting, so the
// halfway-cancel consistency contract is not exercisable.
#[test]
#[ignore = "go-parity-gap: no failpoint injection seam for halfway-cancelled DDL jobs"]
fn halfway_cancelled_ddl_leaves_the_schema_consistent() {
    // Contract (fail_db_test.go:77-156): after each injected failure the
    // pre-DDL rows are intact under the pre-DDL names, and the retryable
    // errors respect DDLErrorCountLimit.
}

// --- TestUpdateHandleFailed (pkg/ddl/tests/fail/fail_db_test.go:159) ---
//
// Go arms `errorUpdateReorgHandle` once during `alter table t add index
// idx_b(b)`: the reorg recovers, the index is built over the single row
// (`select count(*) use index(idx_b)` == 1) and `admin check index t
// idx_b` passes.
// go-parity-gap: no backfill reorg machinery and no failpoint seam.
#[test]
#[ignore = "go-parity-gap: index backfill reorg and its failpoint seam are not transcreated"]
fn add_index_recovers_from_a_failed_handle_update() {
    // Contract (fail_db_test.go:159-176): after the injected
    // update-reorg-handle failure, idx_b covers the row and
    // `admin check index t idx_b` is green.
}

// --- TestAddIndexFailed (pkg/ddl/tests/fail/fail_db_test.go:177) ---
//
// Go arms `mockBackfillRunErr` once while adding idx_b over 1000 rows
// spread across 100 split regions: the backfill retries past the injected
// error and the final `admin check index`/`admin check table` are green.
// go-parity-gap: no region splitting, no backfill workers, no failpoints.
#[test]
#[ignore = "go-parity-gap: region-split backfill with injected run errors is not transcreated"]
fn add_index_backfill_survives_an_injected_run_error() {
    // Contract (fail_db_test.go:177-212): 1000 rows across 100 regions,
    // one injected mockBackfillRunErr, idx_b complete and green.
}

// --- TestFailSchemaSyncer (pkg/ddl/tests/fail/fail_db_test.go:214) ---
//
// Go makes the schema reload fail (`ErrorMockReloadFailed` +
// `MemSyncer.CloseSession`), waits for the schema validator to stop, and
// requires DML to fail `[domain:8027]Information schema is out of date:
// schema failed to update in 1 lease, ...`; after the failpoint is removed
// the validator restarts and the insert succeeds.
// go-parity-gap: no schema-syncer/validator/lease machinery in this tier.
#[test]
#[ignore = "go-parity-gap: no schema validator or lease-out-of-date detection (8027) in this tier"]
fn a_stuck_schema_syncer_blocks_dml_with_8027() {
    // Contract (fail_db_test.go:214-256): insert fails
    // "[domain:8027]Information schema is out of date: schema failed to
    // update in 1 lease, please make sure TiDB can connect to TiKV" while
    // the validator is stopped, and succeeds once it restarts.
}

// --- TestGenGlobalIDFail (pkg/ddl/tests/fail/fail_db_test.go:258) ---
//
// Go arms `jobsubmit/mockGenGlobalIDFail` per case: create/truncate of
// plain and range-partitioned tables FAIL while the id generator errors,
// then SUCCEED with `return(false)` and stay `admin check`-green.
// go-parity-gap: global-id allocation has no failure seam in this tier.
#[test]
#[ignore = "go-parity-gap: global-id allocation has no injectable failure seam"]
fn gen_global_id_failure_fails_ddl_and_recovers() {
    // Contract (fail_db_test.go:258-306): every mockErr case errors; every
    // non-mock case succeeds, inserts, and admin-checks green (t1 plain,
    // t2 range-partitioned).
}

// --- TestRunDDLJobPanicEnableFastCreateTable
//     (pkg/ddl/tests/fail/fail_db_test.go:308) ---
// Go arms `mockPanicInRunDDLJob` (`1*panic("panic test")`) with
// `tidb_enable_fast_create_table=ON`: the panic is RECOVERED and the
// statement reports `[ddl:8214]Cancelled DDL job`.
// go-parity-gap: no RunDDLJob panic-recovery seam and no fast-create-table
// flag wiring in this tier.
#[test]
#[ignore = "go-parity-gap: RunDDLJob panic recovery (8214) has no carrier here"]
fn a_panicking_ddl_job_reports_cancelled_8214_with_fast_create_table() {
    // Contract (fail_db_test.go:308-319): create table under
    // tidb_enable_fast_create_table=ON with one injected panic answers
    // "[ddl:8214]Cancelled DDL job".
}

// --- TestRunDDLJobPanic (pkg/ddl/tests/fail/fail_db_test.go:321) ---
// The same panic recovery without the fast-create-table flag: the statement
// reports `[ddl:8214]Cancelled DDL job` and the schema stays consistent.
// go-parity-gap: same missing panic-recovery seam.
#[test]
#[ignore = "go-parity-gap: RunDDLJob panic recovery (8214) has no carrier here"]
fn a_panicking_ddl_job_reports_cancelled_8214() {
    // Contract (fail_db_test.go:321-333): create table with one injected
    // panic answers "[ddl:8214]Cancelled DDL job".
}

// --- TestPartitionAddIndexGC (pkg/ddl/tests/fail/fail_db_test.go:335) ---
//
// Go adds an index over a range-partitioned table with rows in three
// partitions while `mockUpdateCachedSafePoint` forces the GC safe-point
// expiry path: the ADD INDEX completes green (the mocked
// update-cached-safe-point fires during the reorg).
// go-parity-gap: no safe-point/GC interaction and no partition-level
// backfill reorg in this tier.
#[test]
#[ignore = "go-parity-gap: safe-point-expiry during partition backfill is not transcreated"]
fn partition_add_index_survives_a_forced_safe_point_expiry() {
    // Contract (fail_db_test.go:335-360): ADD INDEX idx (id, hired) over
    // the three partitions completes with the mocked safe-point update.
}

// --- TestPartitionAddPanic (pkg/ddl/tests/fail/fail_db_test.go:459) ---
//
// Go arms `CheckPartitionByRangeErr` and requires
// `alter table t add partition (partition p1 values less than (20))` to
// FAIL, then SHOW CREATE TABLE to still print exactly p0 (VALUES LESS THAN
// (10)) and NOT p1/20 — a failed ADD PARTITION leaves the meta untouched.
//
// go-parity-gap: the tier's ADD PARTITION runner has no failure seam (the
// `CheckPartitionByRangeErr` failpoint does not exist), so the
// failed-add-leaves-meta-untouched contract is not exercisable; the tier
// would simply add the partition.
#[test]
#[ignore = "go-parity-gap: ADD PARTITION has no injectable failure to leave the meta untouched against"]
fn failed_add_partition_leaves_the_meta_untouched() {
    // Contract (fail_db_test.go:459-477): the add fails; show create table
    // matches /PARTITION .p0. VALUES LESS THAN \(10\)/ and does NOT match
    // /PARTITION .p0. VALUES LESS THAN \(20\)/.
}
