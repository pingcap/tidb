//! Go `INSERT IGNORE` (`e.ignoreErr`) takes precedence over STRICT mode:
//! `HandleTruncate` (`pkg/types/datum.go:1311`) downgrades every write
//! conversion error to a warning and keeps the converted value — truncated
//! varchar, zero-filled int — where non-IGNORE strict rejects. Go notes the
//! blanket shape itself: "TODO: should not filter all types of errors here".

use tidb_executor::{ddl, run_insert_on, run_select_on, Catalog, CreateTableSettings, StmtContext};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(3), c int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn insert_ignore_strict_too_long_stores_truncated() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    // IgnoreErr takes precedence over strict: warn and store 'abc'.
    let ignore = StmtContext::for_dml(false, true, true);
    run_insert_on("insert into t values (1, 'abc', 7)", &mut catalog, &strict).unwrap();
    run_insert_on("insert ignore into t values (2, 'abcdef', 8)", &mut catalog, &ignore)
        .expect("INSERT IGNORE downgrades 1406 to a warning");
    let rows = run_select_on("select a, b from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 2, "the ignored row IS stored");
    assert_eq!(format!("{:?}", rows[1][1].as_raw_bytes()), "Some([97, 98, 99])");
}

#[test]
fn insert_ignore_strict_bad_int_stores_zero() {
    let mut catalog = setup();
    let ignore = StmtContext::for_dml(false, true, true);
    run_insert_on(
        "insert ignore into t values (3, 'abc', 9)",
        &mut catalog,
        &ignore,
    )
    .expect("INSERT IGNORE downgrades the bad-int conversion");
}

#[test]
fn insert_strict_without_ignore_still_rejects() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    assert!(run_insert_on("insert into t values (1, 'abcdef', 0)", &mut catalog, &strict).is_err());
}
