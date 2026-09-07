//! Multi-row `INSERT IGNORE` with a CHECK constraint: violating rows are
//! skipped with a warning and NOT counted; conforming rows of the same
//! statement are stored and survive later violations (Go's per-row skip —
//! `insert ignore` never rolls back rows it already stored).

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
        .run("create table t (a int primary key, b int, check (b > 0))")
        .unwrap();
    session.run("insert into t values (1, 1)").unwrap();
}

#[test]
fn violating_rows_skip_conforming_rows_persist() {
    let mut session = Session::new();
    setup(&mut session);

    // Rows (2,-2) and (4,-4) violate; row (3,3) conforms.
    let affected = match session
        .run("insert ignore into t values (2, -2), (3, 3), (4, -4)")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 1, "only the conforming row counts");

    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|1;3|3",
        "the violating rows are skipped, the conforming row is stored"
    );
}
