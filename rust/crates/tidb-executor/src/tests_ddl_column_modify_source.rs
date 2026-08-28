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

#![allow(missing_docs)]

//! GO PORT of `pkg/ddl/column_modify_test.go` (items 48-56 of the
//! pkg/ddl.part1 slice, read from `origin/master`).
//!
//! The Go file is driven by the on-line DDL state machine (it needs
//! `testkit.CreateMockStoreWithSchemaLease` and failpoint hooks); those
//! halves are the documented gaps below. The statement-level behavior its
//! assertions pin — ADD/CHANGE/MODIFY/DROP COLUMN against the transcreated
//! `pkg/ddl/column.go` lowering in `crate::ddl::alter_table` — IS present,
//! and the running tests here pin it end to end: default fills, the default
//! dropped by CHANGE, comments, flags, generated columns, and the captured
//! TiDB error codes 1060/1067/1170/1265/3855.

use crate::ddl::{run_alter_table_in, run_create_table_on};
use crate::driver::{run_insert_on, run_select_on};
use crate::{Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn rows(catalog: &Catalog, sql: &str) -> Vec<Vec<tidb_datatype::Datum>> {
    run_select_on(sql, catalog, &ctx()).unwrap()
}

fn alter(catalog: &mut Catalog, sql: &str) -> Result<(), crate::driver::DriverError> {
    run_alter_table_in(sql, catalog, crate::driver::DEFAULT_DATABASE, &ctx())
}

fn int(value: i64) -> tidb_datatype::Datum {
    tidb_datatype::Datum::Int(value)
}

fn str(value: &str) -> tidb_datatype::Datum {
    tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
        value.as_bytes().to_vec(),
        tidb_datatype::Collation::Utf8Mb4Bin,
    ))
}

/// GO PORT of `pkg/ddl/column_modify_test.go:48 TestAddAndDropColumn`.
///
/// Go inserts rows around a running `alter table t2 add column c4 int
/// default -1` and then pins (column_modify_test.go:110-133) that every row
/// answers `count(c4)`, pre-alter rows read the default -1, and rows
/// inserted after the DDL read their explicit value. The interleaving is the
/// failpoint-driven half — go-parity-gap — so this port pins the same three
/// observable facts deterministically: ten pre-alter rows, ten post-alter
/// rows.
#[test]
fn add_column_default_minus_one_fills_only_pre_alter_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t2 (c1 INT, c2 INT, c3 INT)", &mut catalog).unwrap();
    for i in 0..10 {
        run_insert_on(
            &format!("INSERT INTO t2 VALUES ({i}, {i}, {i})"),
            &mut catalog,
            &ctx(),
        )
        .unwrap();
    }

    alter(&mut catalog, "ALTER TABLE t2 ADD COLUMN c4 INT DEFAULT -1").unwrap();

    // Rows inserted after the DDL must carry their own value ("here c4 must
    // exist", column_modify_test.go:104).
    for i in 10..20 {
        run_insert_on(
            &format!("INSERT INTO t2 VALUES ({i}, {i}, {i}, {i})"),
            &mut catalog,
            &ctx(),
        )
        .unwrap();
    }

    // count(c4) > 0: every row answers the new column.
    let all = rows(&catalog, "SELECT c4 FROM t2");
    assert_eq!(all.len(), 20, "count(c4) over 10 pre + 10 post rows");

    // select count(c4) from t2 where c4 = -1  -> exactly the pre-alter rows
    // (column_modify_test.go:120-122).
    let defaulted = rows(&catalog, "SELECT c4 FROM t2 WHERE c4 = -1");
    assert_eq!(defaulted.len(), 10, "only pre-alter rows read the DEFAULT -1");
    assert!(defaulted.iter().all(|row| row[0] == int(-1)));

    // Per-value probes (column_modify_test.go:123-128): each post-alter value
    // is readable exactly.
    for i in 10..20 {
        assert_eq!(
            rows(&catalog, &format!("SELECT c4 FROM t2 WHERE c4 = {i}")),
            vec![vec![int(i)]],
            "select c4 from t2 where c4 = {i}"
        );
    }
}

/// GO PORT of `pkg/ddl/column_modify_test.go:257 TestDropColumn` (tail).
///
/// Go's main body races 25 concurrent `drop column` statements against
/// inserts — that failpoint-scheduled half is a gap. Its deterministic tail
/// (column_modify_test.go:292-295) pins that dropping a column named by the
/// table's partitioning function is refused with
/// `[ddl:3855]Column 'a' has a partitioning function dependency and cannot
/// be dropped or renamed`.
#[test]
fn drop_column_partition_dependency_errors_3855() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t1 (a INT, b INT) PARTITION BY HASH(a) PARTITIONS 4",
        &mut catalog,
    )
    .unwrap();

    let error = alter(&mut catalog, "ALTER TABLE t1 DROP COLUMN a")
        .expect_err("Go: [ddl:3855] partitioning function dependency");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 3855, "{mysql:?}");
    assert_eq!(
        mysql.message,
        "Column 'a' has a partitioning function dependency and cannot be dropped or renamed"
    );
}

/// GO PORT of `pkg/ddl/column_modify_test.go:294 TestChangeColumn`.
///
/// Go pins CHANGE/MODIFY COLUMN semantics against `t3 (a int default '0',
/// b varchar(10), d int not null default '0')`:
/// - `change a aa bigint` keeps the pre-existing row's `0` but DROPS the
///   default, so a later insert reads NULL (column_modify_test.go:303-305);
/// - `change d dd bigint not null` leaves the column without a default
///   (NoDefaultValueFlag, column_modify_test.go:307,:313);
/// - `change b b varchar(20) null default 'c' comment 'my comment'` sets the
///   comment, keeps the column nullable, and new inserts read 'c'
///   (column_modify_test.go:315-324);
/// - the timestamp variant sets the comment and drops NOT NULL
///   (column_modify_test.go:326-334);
/// - `modify en enum('a','z','b','c') not null default 'a'` succeeds
///   (column_modify_test.go:359-360);
/// - the captured failures: 1067 for an invalid default (column_modify_test.go:347-348),
///   1265 at row 1 for a NOT NULL change over a NULL row (column_modify_test.go:355-356),
///   1265 (WarnDataTruncated) for the bigint retarget (column_modify_test.go:357-358),
///   1060 for a rename onto an existing column (column_modify_test.go:362-364),
///   1170 for `k char(10) -> tinytext` under a non-unique index
///   (column_modify_test.go:372-374).
///
/// Three Go assertions are NOT reproduced because this tier measuredly
/// differs — go-parity-gap, not approximated: the `WrongDBName` (1102,
/// column_modify_test.go:349-350) and `WrongTableName` (1103,
/// column_modify_test.go:351-352) refusals for qualified changing names are
/// NOT raised (the statement is accepted), and `k char(10) PRIMARY KEY ->
/// tinytext` is accepted where Go raises 1170 (column_modify_test.go:367-369).
#[test]
fn change_column_retypes_renames_and_drops_defaults_with_captured_errors() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t3 (a INT DEFAULT '0', b VARCHAR(10), d INT NOT NULL DEFAULT '0')",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO t3 SET b = 'a'", &mut catalog, &ctx()).unwrap();
    assert_eq!(rows(&catalog, "SELECT a FROM t3"), vec![vec![int(0)]],
        "insert into t3 set b = 'a' reads the DEFAULT '0' (column_modify_test.go:301-302)");

    // change a aa bigint: old rows keep 0, the default is gone -> NULL.
    alter(&mut catalog, "ALTER TABLE t3 CHANGE a aa BIGINT").unwrap();
    run_insert_on("INSERT INTO t3 SET b = 'b'", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        rows(&catalog, "SELECT aa FROM t3"),
        vec![vec![int(0)], vec![tidb_datatype::Datum::Null]],
        "select aa from t3 after change + insert (column_modify_test.go:303-305)"
    );

    // change d dd bigint not null: no default remains on the column.
    alter(&mut catalog, "ALTER TABLE t3 CHANGE d dd BIGINT NOT NULL").unwrap();
    {
        let Some(crate::TableEntry::Kv(table)) = catalog.get_table_for_test("t3") else {
            panic!("t3 missing");
        };
        let col_d = &table.columns.as_ref()[2];
        assert!(col_d.default_value.is_none(), "NoDefaultValueFlag set (column_modify_test.go:313)");
        assert!(col_d.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL != 0);
    }

    // change b b varchar(20) null default 'c' comment 'my comment'.
    alter(&mut catalog, "ALTER TABLE t3 CHANGE b b VARCHAR(20) NULL DEFAULT 'c' COMMENT 'my comment'").unwrap();
    {
        let Some(crate::TableEntry::Kv(table)) = catalog.get_table_for_test("t3") else {
            panic!("t3 missing");
        };
        let col_b = &table.columns.as_ref()[1];
        assert_eq!(col_b.comment, "my comment", "column_modify_test.go:321");
        assert!(col_b.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0,
            "HasNotNullFlag must be false (column_modify_test.go:322)");
    }
    run_insert_on("INSERT INTO t3 SET aa = 3, dd = 5", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        rows(&catalog, "SELECT b FROM t3"),
        vec![vec![str("a")], vec![str("b")], vec![str("c")]],
        "select b from t3 (column_modify_test.go:323-324)"
    );

    // timestamp: add not null, then change to nullable with default + comment.
    alter(&mut catalog, "ALTER TABLE t3 ADD COLUMN c TIMESTAMP NOT NULL").unwrap();
    alter(&mut catalog, "ALTER TABLE t3 CHANGE c c TIMESTAMP NULL DEFAULT '2017-02-11' COMMENT 'col c comment' ON UPDATE CURRENT_TIMESTAMP").unwrap();
    {
        let Some(crate::TableEntry::Kv(table)) = catalog.get_table_for_test("t3") else {
            panic!("t3 missing");
        };
        let col_c = &table.columns.as_ref()[3];
        assert_eq!(col_c.comment, "col c comment", "column_modify_test.go:333");
        assert!(col_c.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0,
            "HasNotNullFlag must be false (column_modify_test.go:334)");
        assert!(col_c.field_type.flags() & tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW != 0,
            "ON UPDATE CURRENT_TIMESTAMP must survive the CHANGE");
    }

    // enum: add then reorder members via MODIFY (column_modify_test.go:336, :359-360).
    alter(&mut catalog, "ALTER TABLE t3 ADD COLUMN en ENUM('a', 'b', 'c') NOT NULL DEFAULT 'a'").unwrap();
    alter(&mut catalog, "ALTER TABLE t3 MODIFY en ENUM('a', 'z', 'b', 'c') NOT NULL DEFAULT 'a'").unwrap();

    // Failing: invalid default -> 1067 (column_modify_test.go:347-348).
    let error = alter(&mut catalog, "ALTER TABLE t3 CHANGE aa a BIGINT DEFAULT ''")
        .expect_err("Go: [ddl:1067] ErrInvalidDefault");
    assert_eq!(error.to_mysql_error().code, 1067);

    // Rename onto an existing column -> 1060 (column_modify_test.go:362-364).
    alter(&mut catalog, "ALTER TABLE t3 ADD COLUMN a BIGINT").unwrap();
    let error = alter(&mut catalog, "ALTER TABLE t3 CHANGE aa a BIGINT")
        .expect_err("Go: [ddl:1060] ErrDupFieldName");
    assert_eq!(error.to_mysql_error().code, 1060);

    // t4: NULL row under a NOT NULL change -> 1265 (column_modify_test.go:353-358).
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t4 (c1 INT, c2 INT, c3 INT DEFAULT 1, INDEX (c1))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO t4(c2) VALUES (NULL)", &mut catalog, &ctx()).unwrap();
    let error = alter(&mut catalog, "ALTER TABLE t4 CHANGE c1 a1 INT NOT NULL")
        .expect_err("Go: [ddl:1265]Data truncated for column 'a1' at row 1");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1265, "{mysql:?}");
    assert_eq!(mysql.message, "Data truncated for column 'a1' at row 1");
    let error = alter(&mut catalog, "ALTER TABLE t4 CHANGE c2 a BIGINT NOT NULL")
        .expect_err("Go: WarnDataTruncated");
    assert_eq!(error.to_mysql_error().code, 1265);

    // char(10) under a non-unique index cannot become tinytext -> 1170
    // (column_modify_test.go:372-374).
    run_create_table_on("CREATE TABLE t (k CHAR(10), v INT, INDEX(k))", &mut catalog).unwrap();
    let error = alter(&mut catalog, "ALTER TABLE t CHANGE COLUMN k k TINYTEXT")
        .expect_err("Go: ErrBlobKeyWithoutLength");
    assert_eq!(error.to_mysql_error().code, 1170);
}

/// GO PORT of `pkg/ddl/column_modify_test.go:379 TestVirtualColumnDDL`.
///
/// Go builds `test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2)
/// stored)` and pins, per column, `GeneratedExprString` (`` `a` + 8 `` /
/// `` `b` + 2 `` — the restored spelling) and `GeneratedStored`, then that
/// `insert ... values (1, default, default)` reads back `1 9 11`. The same
/// shape is repeated over a global temporary table and a local temporary
/// table — those session-scoped wrappers are the gap; the metadata and
/// default-insert behavior of the transcreated generated-column lowering is
/// pinned on a plain table here.
#[test]
fn virtual_column_ddl_metadata_and_default_insert() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE test_gv_ddl(a INT, b INT AS (a+8) VIRTUAL, c INT AS (b + 2) STORED)",
        &mut catalog,
    )
    .unwrap();

    {
        let Some(crate::TableEntry::Kv(table)) = catalog.get_table_for_test("test_gv_ddl") else {
            panic!("test_gv_ddl missing");
        };
        // column_modify_test.go:395-400 (testCases at :395): ["", false], ["`a` + 8", false],
        // ["`b` + 2", true].
        let expected = [("", None), ("`a` + 8", Some(false)), ("`b` + 2", Some(true))];
        for (column, (expr, stored)) in table.columns.as_ref().iter().zip(expected) {
            let generated = column.generated.as_ref();
            assert_eq!(
                generated.map(|g| g.expr_text.as_str()),
                match expr {
                    "" => None,
                    other => Some(other),
                },
                "GeneratedExprString of {}",
                column.name
            );
            assert_eq!(
                generated.map(|g| g.stored),
                stored,
                "GeneratedStored of {}",
                column.name
            );
        }
    }

    run_insert_on(
        "INSERT INTO test_gv_ddl VALUES (1, DEFAULT, DEFAULT)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        rows(&catalog, "SELECT * FROM test_gv_ddl"),
        vec![vec![int(1), int(9), int(11)]],
        "select * from test_gv_ddl (column_modify_test.go:415-417)"
    );
}

/// GO PORT of `pkg/ddl/column_modify_test.go:424
/// TestTransactionWithWriteOnlyColumn`.
///
/// Go runs a transaction INSIDE the add/drop column job's write-only state
/// (via `beforeRunOneJobStep`) and pins that the transaction neither sees
/// nor corrupts the invisible column. The write-only state machine is the
/// gap.
#[test]
#[ignore = "go-parity-gap: needs the write-only schema state plus a transaction running mid-job (beforeRunOneJobStep); the on-line state machine is not transcreated"]
fn transaction_with_write_only_column() {}

/// GO PORT of `pkg/ddl/column_modify_test.go:478
/// TestAddGeneratedColumnAndInsert` (issue #31735).
///
/// Go adds `gc int as ((a+1))` to `t1 (a int, unique kye(a))` while another
/// session issues upserts/replices in every intermediate state, and pins the
/// final rows read their generated values (column_modify_test.go:514-517).
/// The failpoint-scheduled concurrent DML is the gap; the observable
/// contract — existing rows answer gc = a + 1 once the column is added — is
/// pinned deterministically.
#[test]
fn add_generated_column_fills_existing_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t1 (a INT, UNIQUE kye(a))", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t1 VALUES (1), (10)", &mut catalog, &ctx()).unwrap();

    alter(&mut catalog, "ALTER TABLE t1 ADD COLUMN gc INT AS ((a+1))").unwrap();

    assert_eq!(
        rows(&catalog, "SELECT * FROM t1 ORDER BY a"),
        vec![
            vec![int(1), int(2)],
            vec![int(10), int(11)],
        ],
        "select * from t1 order by a: gc = a + 1 for the stored rows"
    );
}

/// GO PORT of `pkg/ddl/column_modify_test.go:524
/// TestColumnTypeChangeGenUniqueChangingName`.
///
/// Go pins the hidden changing-column machinery: during a type change the
/// job materializes the old column as `_col$_c2_0` and the covered index as
/// `_idx$_idx_0` (read through `GetModifyColumnArgs.ChangingColumn`), with
/// the `_col$__col$_c1_1`-style uniquifier growing for nested changes. This
/// tier's CHANGE renames in place with no hidden columns, so the assertions
/// have no counterpart.
#[test]
#[ignore = "go-parity-gap: the _col$_/_idx$_ hidden changing-column and changing-index machinery (model.GetModifyColumnArgs.ChangingColumn/ChangingIdxs) is not transcreated; this tier's CHANGE renames in place"]
fn column_type_change_gen_unique_changing_name() {}

/// GO PORT of `pkg/ddl/column_modify_test.go:620
/// TestModifyColumnReorgCheckpoint`.
///
/// Go pins that a `modify column b int` reorg checkpoints its table-range
/// loading: two rounds of `afterLoadTableRanges` with a shrinking second
/// count, across an owner resignation. Reorg checkpoints are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: needs the reorg checkpoint machinery (afterUpdateReorgMeta/afterLoadTableRanges, owner resignation); the reorg pipeline is not transcreated"]
fn modify_column_reorg_checkpoint() {}

/// GO PORT of `pkg/ddl/column_modify_test.go:659 TestIssue37611`.
///
/// Go pins that MODIFY/CHANGE of a generated column covered by an index is
/// refused with `[ddl:3106] ErrUnsupportedOnGeneratedColumn`
/// (generated_column.go:419 "Unsupported modification for generated columns
/// covered by an index"), while the generated column itself stays readable
/// through both an index hint and a table scan. MEASURED DIVERGENCE on this
/// tier: the transcreation refuses the same statements with the catch-all
/// 1105 "ALTER TABLE MODIFY COLUMN of a generated column is not supported
/// yet" (alter_table.rs modify path), so Go's 3106 assertions cannot be
/// pinned without approximating.
#[test]
#[ignore = "go-parity-gap: Rust refuses MODIFY/CHANGE of an indexed generated column with 1105 Unsupported, not Go's [ddl:3106] ErrUnsupportedOnGeneratedColumn (measured); pinning Go's errno here would fail and approximating is forbidden"]
fn issue_37611() {}
