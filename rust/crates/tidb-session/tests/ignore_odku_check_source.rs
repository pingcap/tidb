//! INSERT IGNORE + ODKU + CHECK: a duplicate row whose UPDATE branch would
//! violate the CHECK is skipped by the IGNORE downgrade — the stored row is
//! untouched and the affected count does not include it (Go's
//! doDupRowUpdate returns the CHECK error, which the IGNORE path downgrades
//! per row).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
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

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int check (b > 0))")
        .unwrap();
    session.run("insert into t values (1, 5)").unwrap();
}

#[test]
fn ignore_odku_skips_the_violating_update() {
    let mut session = Session::new();
    setup(&mut session);

    let affected = match session
        .run("insert ignore into t values (1, 99) on duplicate key update b = -1")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 0, "the ODKU update does not apply");

    assert_eq!(rows(&mut session, "select a, b from t"), "1|5", "the stored row is untouched");
}
