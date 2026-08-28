// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Ports of the portable slice of `pkg/executor/insert_test.go:683
//! ::TestInsertNullInNonStrictMode`, plus its `ON DUPLICATE KEY UPDATE` gap.
//!
//! Go moves the session to `sql_mode = ''` (non-strict) and pins where a
//! `NULL` written to a `NOT NULL` column is an ERROR versus a WARNING plus
//! the column default. Go's rule is `ResetContextOfStmt`'s `*ast.InsertStmt`
//! arm: `ErrGroupBadNull` errors when `!IgnoreErr && (strict ||
//! len(stmt.Lists) == 1)` -- a single-row VALUES insert errors in EVERY
//! mode, while multi-row and `INSERT ... SELECT` statements downgrade to a
//! 1048 warning and store `''`. The Rust gate is
//! `driver/dml.rs`'s `bad_null_level` (`!ctx.ignore_err() && (ctx.strict()
//! || insert.rows.len() == 1)`), driven by
//! `StmtContext::for_dml(_, strict, ignore_err)`.

use crate::{run_create_table_on, run_insert_reporting, run_select_on, run_update_on, Catalog, StmtContext};
use tidb_datatype::Datum;

fn select_col1(catalog: &Catalog, id: i64) -> String {
    run_select_on(
        &format!("select col1 from tn where id = {id}"),
        catalog,
        &StmtContext::for_query(),
    )
    .expect("select succeeds")
    .remove(0)
    .remove(0)
    .as_string()
    .map(|value| String::from_utf8_lossy(value.bytes()).into_owned())
    .unwrap_or_else(|| "<non-text>".to_owned())
}

fn all_rows(catalog: &Catalog) -> Vec<Vec<String>> {
    run_select_on("select id, col1 from tn order by id", catalog, &StmtContext::for_query())
        .expect("select succeeds")
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|datum| match datum {
                    Datum::Int(value) => value.to_string(),
                    Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

/// Go `insert_test.go:683::TestInsertNullInNonStrictMode`, the VALUES-path
/// arms:
///
/// * strict + IGNORE stores `''` with a 1048 warning (:691);
/// * non-strict SINGLE-row insert still errors `Column 'col1' cannot be
///   null` (:695-698) -- the mode-invariant single-row promotion;
/// * non-strict `INSERT ... SELECT` and multi-row inserts warn and store
///   `''` (:702-703);
/// * a non-strict UPDATE of a NOT NULL column warns and stores `''` (:704).
///
/// `select *` ends with every `col1` as `''` in Go (:705-706); the
/// `ON DUPLICATE KEY UPDATE` arms are the gap test below.
#[test]
fn non_strict_insert_null_semantics_follow_the_values_path() {
    let mut catalog = Catalog::default();
    let strict = StmtContext::for_dml(false, true, false);
    let strict_ignore = StmtContext::for_dml(false, true, true);
    let non_strict = StmtContext::for_dml(false, false, false);

    run_create_table_on(
        "create table tn (id int primary key, col1 varchar(10) not null default '')",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on("create table ts (id int primary key, col1 varchar(10))", &mut catalog).unwrap();

    // insert_test.go:690: feeds the later INSERT ... SELECT.
    run_insert_reporting("insert into ts values (1, null)", &mut catalog, "test", &strict).unwrap();

    // insert_test.go:691: strict + IGNORE -> warning 1048, '' stored.
    run_insert_reporting("insert ignore into tn values(5, null)", &mut catalog, "test", &strict_ignore)
        .unwrap();
    let warnings = strict_ignore.take_warnings();
    assert!(
        warnings.iter().any(|(_, code, message)| *code == 1048 && message.contains("Column 'col1' cannot be null")),
        "expected a 1048 warning, got {warnings:?}"
    );
    assert_eq!(select_col1(&catalog, 5), "");

    // insert_test.go:695-698: non-strict single-row inserts still fail with
    // Go `table.ErrColumnCantNull` (1048), for both the VALUES and SET forms.
    let error = run_insert_reporting("insert into tn values(1, null)", &mut catalog, "test", &non_strict)
        .unwrap_err();
    assert!(
        matches!(&error, crate::DriverError::ColumnCannotBeNull(name) if name == "col1"),
        "unexpected error: {error:?}"
    );
    let error = run_insert_reporting("insert into tn set id = 1, col1 = null", &mut catalog, "test", &non_strict)
        .unwrap_err();
    assert!(
        matches!(&error, crate::DriverError::ColumnCannotBeNull(name) if name == "col1"),
        "unexpected error: {error:?}"
    );

    // insert_test.go:702: non-strict INSERT ... SELECT downgrades to ''.
    run_insert_reporting("insert into tn select * from ts", &mut catalog, "test", &non_strict).unwrap();
    assert_eq!(select_col1(&catalog, 1), "");

    // insert_test.go:703: multi-row non-strict VALUES downgrades to ''.
    run_insert_reporting(
        "insert into tn values(2, null), (3, 3), (4, 4)",
        &mut catalog,
        "test",
        &non_strict,
    )
    .unwrap();
    assert_eq!(select_col1(&catalog, 2), "");

    // insert_test.go:704: non-strict UPDATE also downgrades to ''.
    run_update_on("update tn set col1 = null where id = 3", &mut catalog, &non_strict).unwrap();
    assert_eq!(select_col1(&catalog, 3), "");

    assert_eq!(
        all_rows(&catalog),
        [
            ["1", ""],
            ["2", ""],
            ["3", ""],
            ["4", "4"],
            ["5", ""],
        ]
    );
}

/// Go `insert_test.go:683::TestInsertNullInNonStrictMode`, the two
/// `ON DUPLICATE KEY UPDATE col1 = null` arms:
///
/// * `insert t1 VALUES (5, 5) ON DUPLICATE KEY UPDATE col1 = null` (:699)
///   must fail `Column 'col1' cannot be null` (strict, non-IGNORE,
///   single-row);
/// * `insert ignore t1 VALUES (4, 4) ON DUPLICATE KEY UPDATE col1 = null`
///   (:705) must warn 1048 and leave the stored `''`.
///
/// Measured on this tier the ODKU assignment path has no bad-null gate:
/// the first arm returns affected=2 and STORES NULL, the second stores NULL
/// with no warning -- both diverge from Go.
#[test]
#[ignore = "go-parity-gap: the ODKU assignment path lacks the bad-null gate (measured: col1 = null via ON DUPLICATE KEY UPDATE stores NULL and reports affected=2 instead of erroring, and stores NULL without a 1048 warning under IGNORE)"]
fn non_strict_odku_null_assignment_lacks_the_bad_null_gate() {}
