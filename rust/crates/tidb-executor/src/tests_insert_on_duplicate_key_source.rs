// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Ports of the portable slices of `pkg/executor/insert_test.go:35
//! ::TestInsertOnDuplicateKeyWithBinlog`.
//!
//! Go drives the whole flow through a testkit session; here the same
//! statements run through the in-memory SQL gateway (`run_insert_reporting` /
//! `run_select_on`), whose `INSERT ... ON DUPLICATE KEY UPDATE` affected-row
//! contract is the port of Go's `InsertExec`/`fillRow` dup-key accounting
//! (`pkg/executor/insert_common.go:1027 insertRowWithUpdate` -> OK-packet
//! affected rows: 1 per insert, 2 per CHANGED duplicate update, 0 per
//! unchanged one).
//!
//! Binlog and OK-packet info strings are outside this tier: Go enables the
//! `pkg/table/tblsession/forceWriteBinlog` failpoint (`insert_test.go:38`)
//! and checks `Records: n  Duplicates: m  Warnings: 0` via
//! `tk.CheckLastMessage`; the Rust gateway answers with the affected-row
//! count only, so those two checks have no surface here and the second test
//! records them as a gap.

use crate::{run_create_table_on, run_insert_reporting, run_select_on, Catalog, StmtContext};
use tidb_datatype::Datum;

fn dml_ctx() -> StmtContext {
    // Go's default sql_mode: strict, not IGNORE.
    StmtContext::for_dml(false, true, false)
}

fn select_text(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &StmtContext::for_query())
        .expect("select succeeds")
        .into_iter()
        .map(|row| row.iter().map(datum_text).collect())
        .collect()
}

fn datum_text(datum: &Datum) -> String {
    match datum {
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Null => "<nil>".to_owned(),
        other => format!("{other:?}"),
    }
}

/// Go `insert_test.go:35::TestInsertOnDuplicateKeyWithBinlog`, the arms whose
/// assignments are constants or `VALUES(col)` over a VALUES source:
///
/// * `insert into t1 values(1, 1) on duplicate key update b1 = 400` reports
///   2 affected rows and leaves `(1, 400)` (`insert_test.go:100-103`);
/// * a constant assignment over a `SELECT` source that changes nothing
///   reports 0 (`insert_test.go:104-108`);
/// * a VALUES list mixing duplicates and fresh keys reports
///   inserted + 2xchanged = 7 for Go's five-row statement
///   (`insert_test.go:218-223`);
/// * a duplicate-key assignment naming an unknown column fails with
///   `Unknown column 'c' in 'field list'` (`insert_test.go:118-121`), the
///   planner 1054 shape;
/// * the decimal-PK regression (`insert_test.go:253-258`): `insert into t1
///   set c1 = 0.1 on duplicate key update c1 = 1` must UPDATE the clustered
///   row (2 affected) and read back `1.0000`, not insert a second row.
#[test]
fn on_duplicate_key_values_form_and_constant_assignments_match_mysql_affected_rows() {
    let mut catalog = Catalog::default();
    let ctx = dml_ctx();
    let insert =
        |sql: &str, catalog: &mut Catalog| run_insert_reporting(sql, catalog, "test", &ctx);

    run_create_table_on(
        "create table t1(a1 bigint primary key, b1 bigint)",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on(
        "create table t2(a2 bigint primary key, b2 bigint)",
        &mut catalog,
    )
    .unwrap();
    assert_eq!(
        insert("insert into t1 values(1, 100)", &mut catalog)
            .unwrap()
            .0,
        1
    );
    assert_eq!(
        insert("insert into t2 values(1, 200)", &mut catalog)
            .unwrap()
            .0,
        1
    );

    // insert_test.go:100-103: changed duplicate -> 2 affected, row updated.
    assert_eq!(
        insert(
            "insert into t1 values(1, 1) on duplicate key update b1 = 400",
            &mut catalog,
        )
        .unwrap()
        .0,
        2
    );
    assert_eq!(select_text(&catalog, "select * from t1"), [["1", "400"]]);

    // insert_test.go:104-108: constant assignment over a SELECT source that
    // resolves to the same row value -> 0 affected, row untouched.
    assert_eq!(
        insert(
            "insert into t1 select 1, 400 from t2 on duplicate key update b1 = 400",
            &mut catalog,
        )
        .unwrap()
        .0,
        0
    );
    assert_eq!(select_text(&catalog, "select * from t1"), [["1", "400"]]);

    // insert_test.go:218-223: five values, two of them duplicates that
    // CHANGE -> 3 inserts + 2*2 = 7 affected rows.
    run_create_table_on("create table t3(a int primary key, b int)", &mut catalog).unwrap();
    insert(
        "insert into t3 values(1,1),(2,2),(3,3),(4,4),(5,5)",
        &mut catalog,
    )
    .unwrap();
    assert_eq!(
        insert(
            "insert into t3 values(4,14),(5,15),(6,16),(7,17),(8,18) on duplicate key update b = b + 10",
            &mut catalog,
        )
        .unwrap()
        .0,
        7
    );
    assert_eq!(
        select_text(&catalog, "select * from t3 order by a"),
        [
            ["1", "1"],
            ["2", "2"],
            ["3", "3"],
            ["4", "14"],
            ["5", "15"],
            ["6", "16"],
            ["7", "17"],
            ["8", "18"]
        ],
    );

    // insert_test.go:118-121: the assignment's LHS must exist in the target
    // table; Go fails with `[planner:1054]Unknown column 'c' in 'field list'`.
    let error = insert(
        "insert into t1 select * from t2 on duplicate key update c = t2.b",
        &mut catalog,
    )
    .unwrap_err();
    assert!(
        matches!(
            &error,
            crate::DriverError::UnknownColumnInClause { column, clause }
                if column == "c" && clause == "field list"
        ),
        "unexpected error: {error:?}"
    );

    // insert_test.go:253-258: decimal primary key under the `SET` syntax;
    // the duplicate routes into the UPDATE and reads back as `1.0000`.
    run_create_table_on(
        "create table td(c1 decimal(6,4), primary key(c1))",
        &mut catalog,
    )
    .unwrap();
    assert_eq!(
        insert("insert into td set c1 = 0.1", &mut catalog)
            .unwrap()
            .0,
        1
    );
    assert_eq!(
        insert(
            "insert into td set c1 = 0.1 on duplicate key update c1 = 1",
            &mut catalog,
        )
        .unwrap()
        .0,
        2
    );
    assert_eq!(
        select_text(&catalog, "select * from td use index(primary)"),
        [["1.0000"]]
    );
}
