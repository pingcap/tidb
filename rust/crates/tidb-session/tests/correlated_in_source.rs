//! Correlated IN: even with duplicate inner matches per outer row the
//! outer row emits at most once — `a in (select grp from s)` with s holding
//! (1,10) twice answers 1 and 2, and NOT IN keeps only the outer rows whose
//! group is absent. (Contrast: correlated EXISTS fans out — see
//! docs/correlated-exists-fanout-divergence.md.)

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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
    session.run("create table u (a int primary key)").unwrap();
    session.run("create table s (grp int, x int)").unwrap();
    session.run("insert into u values (1), (2)").unwrap();
    session.run("insert into s values (1, 10), (1, 10), (2, 20)").unwrap();
}

#[test]
fn correlated_in_and_not_in() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(
        rows(&mut session, "select a from u where a in (select grp from s) order by a"),
        "i:1;i:2"
    );
    assert_eq!(
        rows(&mut session, "select a from u where a not in (select grp from s) order by a"),
        ""
    );
}
