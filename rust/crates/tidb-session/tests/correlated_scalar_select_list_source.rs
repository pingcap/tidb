//! Correlated scalar subquery in the SELECT list: each row's projection
//! evaluates the subquery against that row — max(v) per matching k, NULL
//! when nothing matches (row a=9).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
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

fn seed(session: &mut Session) {
    session.run("create table t (a int primary key)").unwrap();
    session.run("insert into t values (1), (2), (9)").unwrap();
    session.run("create table s (k int, v int)").unwrap();
    session.run("insert into s values (1, 111), (1, 222), (2, 50)").unwrap();
}

#[test]
fn correlated_scalar_in_the_select_list() {
    let mut session = Session::new();
    seed(&mut session);

    assert_eq!(
        rows(
            &mut session,
            "select a, (select max(v) from s where s.k = t.a) from t order by a"
        ),
        "1|222;2|50;9|NULL",
        "per-row max, NULL for the unmatched row"
    );
}
