//! Self-referencing `INSERT INTO t SELECT ... FROM t`: the source rows are
//! materialized before the write, so reads see the pre-statement snapshot —
//! `a + 10` over {1,2} yields exactly {11,12} (no feedback loop), and a
//! single-row aggregate copy (`max(a) + 100`) appends one row.

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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn self_select_writes_the_snapshot() {
    let mut session = Session::new();
    session.run("create table t (a int primary key)").unwrap();
    session.run("insert into t values (1), (2)").unwrap();

    let inserted = match session.run("insert into t select a + 10 from t").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(inserted, 2);
    assert_eq!(rows(&mut session, "select a from t order by a"), "1;2;11;12");

    // The aggregate copy sees the CURRENT snapshot ({1,2,11,12} -> max 112).
    let inserted = match session.run("insert into t select max(a) + 100 from t").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(inserted, 1);
    assert_eq!(rows(&mut session, "select a from t order by a"), "1;2;11;12;112");
}
