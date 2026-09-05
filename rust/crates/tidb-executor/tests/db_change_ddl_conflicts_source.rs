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

//! Ports of the `pkg/ddl/db_change_test.go` family (part3 items 121–152 of
//! the package's `func Test*`/`func Benchmark*` declarations, sorted by file
//! and line), read from `origin/master`.
//!
//! The Go tests drive CONCURRENT sessions against the online-DDL job queue
//! (`testControlParallelExecSQL` parks the first job so a second collides
//! with it, and failpoint hooks run SQL inside schema states). This tier has
//! no job queue and no failpoints, so each port pins the observable
//! single-threaded contract the parallel test ultimately depends on — the
//! error or success the loser of the race must see — and every approximation
//! is named in the test's comment. Tests whose whole body is the concurrency
//! or the state machine itself are `#[ignore]`d documentaries with the
//! re-derived contract; they assert nothing until that machinery lands.

use tidb_datatype::Datum;
use tidb_executor::driver::Catalog;
use tidb_executor::{
    admin_check, ddl, run_create_table_on, run_delete_on, run_insert_on, run_select_on, KvTable,
    RowDecodeContext, StmtContext, TableEntry, WarnLevel,
};

/// The text of a string datum, however the codec chose to represent it
/// (crate `driver::tests::datum_text_for_test` is not visible here).
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

/// The storage-backed table a test just built, cloned for `admin_check`.
fn kv_table(catalog: &Catalog, database: &str, name: &str) -> KvTable {
    match catalog.table_in(database, name) {
        Some(TableEntry::Kv(table)) => table.clone(),
        _ => panic!("expected a storage-backed table {database}.{name}"),
    }
}

/// Go's `testControlParallelExecSQL` fixture table (`db_change_test.go:1452`):
/// `t(a int, b int, c double default null, d int auto_increment, e int,
/// index idx1(d), index idx2(d,e))` plus the range-partitioned `t_part`
/// every parallel test pre-creates beside it.
fn create_fixture_tables(catalog: &mut Catalog) {
    run_create_table_on(
        "create table t(a int, b int, c double default null, d int auto_increment, \
         e int, index idx1(d), index idx2(d,e))",
        catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values(1, 2, 3.1234, 4, 5)",
        catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    run_create_table_on(
        "create table t_part (a int key) partition by range(a) (\
         partition p0 values less than (10), partition p1 values less than (20))",
        catalog,
    )
    .unwrap();
}

// --- TestParallelDropColumn (pkg/ddl/db_change_test.go:1329) ---
//
// Go drops `c` from two racing sessions and requires exactly one loser with
// `[ddl:1091]column c doesn't exist`. The serialized form below pins the
// loser's contract: after the column is gone, naming it again is 1091.
#[test]
fn parallel_drop_column_twice_second_1091() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in("ALTER TABLE t drop COLUMN c", &mut catalog, "test", &ctx).unwrap();
    assert!(matches!(
        ddl::run_alter_table_in("ALTER TABLE t drop COLUMN c", &mut catalog, "test", &ctx),
        Err(tidb_executor::DriverError::UnknownColumnInAlter(column)) if column == "c",
    ));
}

// --- TestParallelDropColumns (pkg/ddl/db_change_test.go:1341) ---
//
// Same race with a two-column drop; the loser reports the FIRST missing
// name, `b`, still 1091.
#[test]
fn parallel_drop_columns_twice_second_1091_names_first() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "ALTER TABLE t drop COLUMN b, drop COLUMN c",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        ddl::run_alter_table_in(
            "ALTER TABLE t drop COLUMN b, drop COLUMN c",
            &mut catalog,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::UnknownColumnInAlter(column)) if column == "b",
    ));
}

// --- TestParallelDropIfExistsColumns (pkg/ddl/db_change_test.go:1353) ---
//
// Go runs `drop COLUMN if exists b, drop COLUMN if exists c` in two racing
// sessions and requires BOTH to succeed — the loser files notes, not errors.
// Running the same statement twice reproduces both outcomes on one session;
// the second run leaves one Note 1091 per guarded column, Go's
// `AppendNote`-swallowed `ErrCantDropFieldOrKey` text (captured in
// `drop_column_action`).
#[test]
fn parallel_drop_if_exists_columns_succeed_with_1091_notes() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    for _ in 0..2 {
        ddl::run_alter_table_in(
            "ALTER TABLE t drop COLUMN if exists b, drop COLUMN if exists c",
            &mut catalog,
            "test",
            &ctx,
        )
        .unwrap();
    }
    let warnings = ctx.take_warnings();
    assert_eq!(
        warnings
            .iter()
            .filter(|(_, code, _)| *code == 1091)
            .count(),
        2,
        "each guarded column files exactly one note: {warnings:?}"
    );
}

// --- TestParallelDropIndex (pkg/ddl/db_change_test.go:1365) ---
//
// Two racing sessions drop two DIFFERENT indexes; both must succeed.
#[test]
fn parallel_drop_index_both_succeed() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in("alter table t drop index idx1", &mut catalog, "test", &ctx).unwrap();
    ddl::run_alter_table_in("alter table t drop index idx2", &mut catalog, "test", &ctx).unwrap();
}

// --- TestParallelAlterAddIndex (pkg/ddl/db_change_test.go:1215) ---
//
// `add index index_b(b)` races `CREATE INDEX index_b ON t (c)`; Go requires
// the loser to fail with `[ddl:1061]index already exist index_b`. The
// serialized form pins the loser's error for whichever spelling runs second.
#[test]
fn parallel_alter_add_index_duplicate_1061() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "ALTER TABLE t add index index_b(b)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        ddl::run_create_index_in("CREATE INDEX index_b ON t (c)", &mut catalog, "test", &ctx),
        Err(tidb_executor::DriverError::DuplicateKeyName(name)) if name == "index_b",
    ));
}

// --- TestParallelAlterAddExpressionIndex (pkg/ddl/db_change_test.go:1285) ---
//
// The same duplicate-name race over EXPRESSION indexes: `add index
// expr_index_b((b+1))` races `CREATE INDEX expr_index_b ON t ((c+1))`, and
// the loser must report 1061.
#[test]
fn parallel_alter_add_expression_index_duplicate_1061() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "ALTER TABLE t add index expr_index_b((b+1))",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        ddl::run_create_index_in(
            "CREATE INDEX expr_index_b ON t ((c+1))",
            &mut catalog,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::DuplicateKeyName(name)) if name == "expr_index_b",
    ));
}

// --- TestParallelAddColumAndSetDefaultValue (pkg/ddl/db_change_test.go:1172) ---
//
// Go proves `add column cx int after c1` and `alter table tx alter c2 set
// default 'N'` can both commit while racing. The serialized form pins the
// compatibility they jointly need: both statements succeed on the same
// table, and the row the test deletes afterwards is still addressable.
#[test]
fn parallel_add_column_and_set_default_value_both_commit() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table tx (c1 varchar(64), c2 enum('N','Y') not null default 'N', \
         primary key idx2 (c2, c1))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into tx values('a', 'N')", &mut catalog, &StmtContext::for_query())
        .unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "alter table tx add column cx int after c1",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table tx alter c2 set default 'N'",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    run_delete_on("delete from tx where c1='a'", &mut catalog, &ctx)
        .expect("the row stays addressable through both ALTERs");
}

// --- TestParallelChangeColumnName (pkg/ddl/db_change_test.go:1194) ---
//
// `CHANGE a aa int` races `CHANGE b aa int`; Go requires EXACTLY ONE loser
// with `[schema:1060]Duplicate column name 'aa'`. Serialized, the second
// rename must lose with 1060.
#[test]
fn parallel_change_column_name_duplicate_1060_exactly_once() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in("ALTER TABLE t CHANGE a aa int", &mut catalog, "test", &ctx).unwrap();
    assert!(matches!(
        ddl::run_alter_table_in("ALTER TABLE t CHANGE b aa int", &mut catalog, "test", &ctx),
        Err(tidb_executor::DriverError::DuplicateColumnName(name)) if name == "aa",
    ));
}

// --- TestParallelAlterAddPartition (pkg/ddl/db_change_test.go:1312) ---
//
// `add partition p2 less than (30)` races `add partition p3 less than (30)`
// on the fixture `t_part` (bounds 10, 20); Go requires the loser to fail
// with 1493 VALUES LESS THAN must be strictly increasing.
#[test]
fn parallel_alter_add_partition_less_than_strictly_increasing_1493() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "alter table t_part add partition (partition p2 values less than (30))",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        ddl::run_alter_table_in(
            "alter table t_part add partition (partition p3 values less than (30))",
            &mut catalog,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::PartitionRangeNotIncreasing),
    ));
}

// --- TestParallelCreateAndRename (pkg/ddl/db_change_test.go:1391) ---
//
// `create table t_exists(c int)` races `rename t to t_exists`; the rename
// loses with `[schema:1050]Table 't_exists' already exists`.
#[test]
fn parallel_create_and_rename_collision_1050() {
    let mut catalog = Catalog::default();
    create_fixture_tables(&mut catalog);
    run_create_table_on("create table t_exists(c int)", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
    assert!(matches!(
        ddl::run_alter_table_in("alter table t rename to t_exists", &mut catalog, "test", &ctx),
        Err(tidb_executor::DriverError::Schema(
            tidb_executor::SchemaErrorKind::TableExists(qualified)
        )) if qualified == "test.t_exists",
    ));
}

// --- TestCreateTableIfNotExists (pkg/ddl/db_change_test.go:1540) ---
//
// Go runs `create table if not exists test_not_exists(a int)` from two
// sessions and expects NO error from either. Serialized, the second run is
// the loser: `run_create_table_in` returns `Ok(false)` — suppressed, never
// an error (Go files the note the parallel loser would carry).
#[test]
fn create_table_if_not_exists_parallel_no_error() {
    let mut catalog = Catalog::default();
    assert!(run_create_table_on("create table if not exists test_not_exists(a int)", &mut catalog)
        .unwrap());
    assert!(
        !run_create_table_on("create table if not exists test_not_exists(a int)", &mut catalog)
            .unwrap()
    );
}

// --- TestDDLIfExists (pkg/ddl/db_change_test.go:1573) ---
//
// Go runs every `IF EXISTS` DDL from two racing sessions and expects no
// error from either. Serialized: the same statement must succeed TWICE —
// the second run finds nothing and files notes instead of errors. This is
// exactly the contract the parallel loser depends on.
#[test]
fn ddl_if_exists_each_statement_survives_a_second_run() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table test_exists (a int key, b int)",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on(
        "create table test_exists_2 (a int key) partition by range(a) (\
         partition p0 values less than (10), partition p1 values less than (20), \
         partition p2 values less than (30))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    for sql in [
        "alter table test_exists drop column if exists c, drop column if exists d",
        "alter table test_exists drop column if exists b",
        "alter table test_exists change column if exists a c int",
        "alter table test_exists modify column if exists a bigint",
    ] {
        for _ in 0..2 {
            ddl::run_alter_table_in(sql, &mut catalog, "test", &ctx)
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        }
    }
    // Go's DROP INDEX sub-case first adds `idx_c (c)` (the column the CHANGE
    // above produced), then races two guarded drops.
    ddl::run_alter_table_in(
        "alter table test_exists add index idx_c (c)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    for _ in 0..2 {
        ddl::run_alter_table_in(
            "alter table test_exists drop index if exists idx_c",
            &mut catalog,
            "test",
            &ctx,
        )
        .unwrap();
    }
    // Go's final sub-case drops a partition twice under IF EXISTS. The
    // preceding `add index idx_c (c)` from the Go flow is skipped here
    // because this statement list already consumed `c`'s table above.
    for _ in 0..2 {
        ddl::run_alter_table_in(
            "alter table test_exists_2 drop partition if exists p1",
            &mut catalog,
            "test",
            &ctx,
        )
        .unwrap();
    }
    let warnings = ctx.take_warnings();
    assert!(
        warnings.iter().all(|(level, _, _)| *level == WarnLevel::Note),
        "every IF EXISTS suppression is a note: {warnings:?}"
    );
}

// --- TestCreateExpressionIndex (pkg/ddl/db_change_test.go:1715) ---
//
// The Go test hooks `afterWaitSchemaSynced` to run a DML matrix inside each
// schema state of `alter table t add index idx((b+1))` and then admin-checks
// the table; the issue-39784 tail adds `create index idx on
// test.t((lower(test.t.name)))` over mixed-case names. The state matrix
// needs the failpoint machinery (documented below); what this tier CAN pin
// is the index mechanics both halves share: an expression index over a
// populated table is built, is consistent, and the collation-aware
// `lower()` expression index of issue 39784 builds over mixed-case rows.
#[test]
fn create_expression_index_builds_and_admin_checks_clean() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(a int default 0, b int default 0)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t values (1, 1), (2, 2), (3, 3), (4, 4)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "alter table t add index idx((b+1))",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let mut table = kv_table(&catalog, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("expression index consistent with its rows");

    // Issue 39784: an expression index over `lower(name)`. Go drops and
    // recreates `t` here; a fresh catalog models the recreated table (this
    // tier has no DROP TABLE runner).
    let mut catalog2 = Catalog::default();
    run_create_table_on("create table t(name varchar(20))", &mut catalog2).unwrap();
    run_insert_on(
        "insert into t values ('Abc'), ('Bcd'), ('abc')",
        &mut catalog2,
        &ctx,
    )
    .unwrap();
    ddl::run_create_index_in(
        "create index idx on test.t((lower(test.t.name)))",
        &mut catalog2,
        "test",
        &ctx,
    )
    .unwrap();
    let mut table = kv_table(&catalog2, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("lower() expression index consistent with its rows");
}

// --- TestCreateUniqueExpressionIndex (pkg/ddl/db_change_test.go:1784) ---
//
// Same shape as above with `add unique index idx((a*b+1))`. The serialized
// port pins the index build over the populated fixture; the ODKU/update
// state matrix stays a gap (it needs the schema-state hooks).
#[test]
fn create_unique_expression_index_builds_and_admin_checks_clean() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(a int default 0, b int default 0)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t values (1, 1), (2, 2), (3, 3), (4, 4)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "alter table t add unique index idx((a*b+1))",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let mut table = kv_table(&catalog, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("unique expression index consistent with its rows");
    let rows = rows_text(&run_select_on("select * from t order by a, b", &catalog, &ctx).unwrap());
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], "1");
}

// --- TestDropExpressionIndex (pkg/ddl/db_change_test.go:1887) ---
//
// The Go test drops `idx((b+1))` under schema-state hooks and checks the
// surviving rows. Serialized port: the declared expression index drops
// cleanly and the table stays consistent and readable.
#[test]
fn drop_expression_index_roundtrip_keeps_rows_readable() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t(a int default 0, b int default 0, key idx((b+1)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 1), (2, 2), (3, 3), (4, 4)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in("alter table t drop index idx", &mut catalog, "test", &ctx).unwrap();
    let mut table = kv_table(&catalog, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("table consistent after dropping its expression index");
    let rows = rows_text(&run_select_on("select * from t order by a", &catalog, &ctx).unwrap());
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], "1");
}

// --- TestConcurrentSetDefaultValue (pkg/ddl/db_change_test.go:2048) ---
//
// Go interleaves a concurrent `ALTER COLUMN SET DEFAULT` into a
// `MODIFY COLUMN` job via `beforeRunOneJobStep`, then checks the surviving
// column type and that fresh rows take the settled default. The default
// half is portable: after `modify column a MEDIUMINT NULL DEFAULT
// '-8145111'` a row written with no columns reads -8145111, and after the
// TIMESTAMP spelling an empty insert succeeds (Go then checks SHOW CREATE
// TABLE, unported here — see the gap note below).
#[test]
fn concurrent_set_default_value_new_rows_take_modified_default() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(a YEAR NULL DEFAULT '2029')", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "alter table t modify column a MEDIUMINT NULL DEFAULT '-8145111'",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into t value()", &mut catalog, &ctx).unwrap();
    assert_eq!(
        rows_text(&run_select_on("select a from t", &catalog, &ctx).unwrap()),
        [["-8145111"]],
    );

    // Go rebuilds the table as `t(a int default 2)` and reruns the
    // modification with a TIMESTAMP default; the empty insert must succeed.
    let mut catalog2 = Catalog::default();
    run_create_table_on("create table t(a int default 2)", &mut catalog2).unwrap();
    ddl::run_alter_table_in(
        "alter table t modify column a TIMESTAMP NULL DEFAULT '2017-08-06 10:47:11'",
        &mut catalog2,
        "test",
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into t value()", &mut catalog2, &ctx)
        .expect("empty insert takes the settled TIMESTAMP default");
}

// --- go-parity-gap documentaries -------------------------------------------------
//
// Each `#[ignore]` below pins one Go test whose contract CANNOT be asserted
// in this tier without inventing behavior. Bodies stay empty on purpose:
// the doc comment is the restoration note.

// go-parity-gap: needs the DDL job queue's beforeRunOneJobStep/parallel-run
// conflict to produce Go's 8245 "column c id 3 does not exist ... ran in
// parallel" and the NOT NULL backfill reorg over live rows
// (pkg/ddl/db_change_test.go:1068::TestParallelAlterModifyColumnToNotNullWithData).
#[test]
#[ignore]
fn parallel_alter_modify_column_to_not_null_with_data() {
    // Contract to restore: double-`MODIFY c int not null` race ends with
    // err1 = nil, err2 = [ddl:8245]; surviving rows keep `c` NOT NULL
    // (NULL insert refused, 33.3 → "33" cast on read).
}

// go-parity-gap: the Go error ("[ddl:8200]Unsupported modify column: oldCol
// is a dependent column 'a' for generated column") is the PARALLEL job
// conflict's message; this tier's serialized check answers the different
// (also Go-native) 3108 generated-dependency refusal, so the parallel
// contract cannot be pinned without the job queue
// (pkg/ddl/db_change_test.go:1124::TestParallelAddGeneratedColumnAndAlterModifyColumn).
#[test]
#[ignore]
fn parallel_add_generated_column_and_alter_modify_column() {
    // Contract to restore: racing ADD COLUMN f AS (a+1) and MODIFY a
    // char(16) end with exactly one 8200 dependent-column failure.
}

// go-parity-gap: ADD PRIMARY KEY is refused outright by this tier ("this
// index kind is not supported yet"), so neither the race nor the 8200
// "this column has primary key flag" loser error can be pinned
// (pkg/ddl/db_change_test.go:1141::TestParallelAlterModifyColumnAndAddPK).
#[test]
#[ignore]
fn parallel_alter_modify_column_and_add_pk() {
    // Contract to restore: ADD PRIMARY KEY (b) NONCLUSTERED races MODIFY b
    // tinyint; the modify loses with 8200 primary-key-flag unsupported.
}

// go-parity-gap: vector indexes need the TiFlash replica mock
// (WithMockTiFlash), the MockCheckColumnarIndexProcess failpoint and the
// columnar-index DDL machinery, none of which this tier models
// (pkg/ddl/db_change_test.go:1228::TestParallelAlterAddVectorIndex).
#[test]
#[ignore]
fn parallel_alter_add_vector_index() {
    // Contract to restore: two racing `add vector index ((vec_cosine_distance(c)))
    // USING HNSW` jobs; the loser rolls back with
    // "[ddl:1061]DDL job rollback, error msg: vector index vecIdx function
    // vec_cosine_distance already exist on column c".
}

// go-parity-gap: columnar (inverted) indexes need the same TiFlash mock +
// failpoint machinery as the vector-index race above
// (pkg/ddl/db_change_test.go:1257::TestParallelAlterAddColumnarIndex).
#[test]
#[ignore]
fn parallel_alter_add_columnar_index() {
    // Contract to restore: two racing `add columnar index (b) USING
    // INVERTED` jobs; the loser reports "inverted columnar index colIdx
    // already exist on column b" through the job-rollback 1061 wrapper.
}

// go-parity-gap: ADD/DROP PRIMARY KEY statements are refused by this tier,
// so the "exactly one 1068 Multiple primary key defined" race contract and
// the drop-PK "index PRIMARY doesn't exist" loser cannot be pinned
// (pkg/ddl/db_change_test.go:1299::TestParallelAddPrimaryKey,
// pkg/ddl/db_change_test.go:1378::TestParallelDropPrimaryKey).
#[test]
#[ignore]
fn parallel_add_and_drop_primary_key_races() {
    // Contract to restore: racing ADD PRIMARY KEY index_b(b)/index_b(c)
    // ends with one 1068; racing DROP PRIMARY KEY twice ends with
    // "[ddl:1091]index PRIMARY doesn't exist" for the loser.
}

// go-parity-gap: DROP/ALTER SCHEMA have no runner in this tier — only the
// DDLExec operator's classification gates exist — so the 1008
// "Can't drop database" loser contract cannot be exercised
// (pkg/ddl/db_change_test.go:1404::TestParallelAlterAndDropSchema).
#[test]
#[ignore]
fn parallel_alter_and_drop_schema() {
    // Contract to restore: DROP SCHEMA db_drop_db races ALTER SCHEMA ...
    // CHARSET utf8mb4; the alter loses with
    // "[schema:1008]Can't drop database ''; database doesn't exist".
}

// go-parity-gap: CREATE DATABASE has no runner in this tier, so the
// if-not-exists double-create contract cannot be exercised
// (pkg/ddl/db_change_test.go:1548::TestCreateDBIfNotExists).
#[test]
#[ignore]
fn create_db_if_not_exists_parallel_no_error() {
    // Contract to restore: two racing `create database if not exists
    // test_not_exists` — neither session errors.
}

// go-parity-gap: serial ADD COLUMN, grouped ADD COLUMNS, ADD INDEX, and
// CREATE INDEX guards are modeled by
// `db_integration_ddl_types_source::add_column_if_not_exists_skips_duplicates_and_continues_grouped_adds`
// and `::index_if_not_exists_skips_duplicate_create_and_alter`. This
// concurrency documentary remains ignored because the Go matrix's job race
// and Note delivery are outside this serial catalog tier
// (pkg/ddl/db_change_test.go:1556::TestDDLIfNotExists).
#[test]
#[ignore]
fn ddl_if_not_exists_parallel_no_error() {
    // Contract to restore: racing `add column if not exists b int`,
    // `add column if not exists (c11 int, d11 int)`,
    // `add index if not exists idx_b (b)` and
    // `create index if not exists idx_b on test_not_exists (b)` — no
    // session errors, the loser of each race files a note.
}

// go-parity-gap: needs the afterGetSchemaAndTableByIdent failpoint to hold
// two sessions on one information schema, producing Go's
// "Information schema is changed" loser error — pure schema-version
// machinery this tier does not model
// (pkg/ddl/db_change_test.go:1599::TestParallelDDLBeforeRunDDLJob).
#[test]
#[ignore]
fn parallel_ddl_before_run_ddl_job_outdated_infoschema() {
    // Contract to restore: session 1 drops c2 with a fresh schema; session
    // 2's ADD COLUMN, prepared on the stale schema, fails with
    // ".*Information schema is changed.*".
}

// go-parity-gap: ALTER SCHEMA ... CHARSET has no runner; only
// resolve_database_charset (CREATE DATABASE options) exists, and the
// information_schema.schemata check has no retriever
// (pkg/ddl/db_change_test.go:1649::TestParallelAlterSchemaCharsetAndCollate).
#[test]
#[ignore]
fn parallel_alter_schema_charset_and_collate() {
    // Contract to restore: two racing ALTER SCHEMA ... CHARSET utf8mb4
    // COLLATE utf8mb4_general_ci both succeed and schemata then reports
    // "utf8mb4 utf8mb4_general_ci".
}

// go-parity-gap: needs the truncate-vs-add-column job race whose loser sees
// Go's [domain:8028] "Information schema is changed during the execution of
// the statement" — domain-level schema validation is unported
// (pkg/ddl/db_change_test.go:1667::TestParallelTruncateTableAndAddColumn,
// pkg/ddl/db_change_test.go:1681::TestParallelTruncateTableAndAddColumns).
#[test]
#[ignore]
fn parallel_truncate_table_and_add_column_8028() {
    // Contract to restore: TRUNCATE t races ADD COLUMN c3 int (and the
    // two-column spelling); the add loses with the 8028 [try again later]
    // message.
}

// go-parity-gap: needs runTestInSchemaState's failpoint hooks to run the
// INSERT/DELETE matrix inside StateWriteReorganization and then
// ADMIN CHECK the reorg'd table
// (pkg/ddl/db_change_test.go:1694::TestWriteReorgForColumnTypeChange).
#[test]
#[ignore]
fn write_reorg_for_column_type_change() {
    // Contract to restore: CHANGE a ddd TIME NULL DEFAULT '18:21:32' AFTER
    // c on a latin1 t_ctc table while DML runs in WriteReorganization;
    // `admin check table t_ctc` ends clean.
}

// go-parity-gap: the six rename-conflict scenarios need the
// beforeRunOneJobStep failpoint to inject a concurrent DDL between a
// rename's schema states, plus a second database for the cross-db forms
// (pkg/ddl/db_change_test.go:1939::TestParallelRenameTable). The serialized
// same-db collision IS pinned above (parallel_create_and_rename_collision_1050).
#[test]
#[ignore]
fn parallel_rename_table_concurrent_infoschema_conflicts() {
    // Contract to restore: rename-then-add-column, rename-then-rename
    // (same db and cross-db) and multi-table rename + add index all end
    // with the concurrent DDL failing "Information schema is changed".
}
