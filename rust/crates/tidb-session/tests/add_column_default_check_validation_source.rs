//! ADD COLUMN with an inline CHECK whose DEFAULT backfills violating values:
//! `add column c int default (-1) check (c >= 0)` on a non-empty table
//! refuses with 3819 — the whole DDL rolls back (the column stays unknown).
//! Go's build -> validate flow refuses the ALTER the same way.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn self_violating_default_refused_and_rolled_back() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session.run("create table t (id int primary key)").unwrap();
    session.run("insert into t values (1), (2)").unwrap();

    // The DEFAULT (-1) backfills the existing rows, violating the new CHECK.
    let error = session
        .run("alter table t add column c int default (-1) check (c >= 0)")
        .expect_err("3819")
        .to_string();
    assert_eq!(error, "Check constraint 't_chk_1' is violated.");

    // The rollback is complete: the column does not exist.
    let error = session.run("select c from t").expect_err("rolled back").to_string();
    assert!(error.contains("Unknown column"), "{error}");
}
