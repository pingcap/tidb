//! ANY/ALL quantified comparisons and DISTINCT NULL folding: `> ANY`
//! passes beyond the subquery minimum, `> ALL` beyond its maximum, and
//! SELECT DISTINCT folds duplicate NULLs into one row.

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
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (5), (20), (30)").unwrap();
    session.run("create table s (v int)").unwrap();
    session.run("insert into s values (10), (25)").unwrap();
    session.run("create table n (a int)").unwrap();
    session
        .run("insert into n values (NULL), (1), (NULL), (1), (2)")
        .unwrap();
}

#[test]
fn any_all_and_distinct_nulls() {
    let mut session = Session::new();
    seed(&mut session);

    // `> ANY (10, 25)` = greater than the MIN (10).
    assert_eq!(
        rows(&mut session, "select a from t where a > any (select v from s) order by a"),
        "20;30"
    );
    // `> ALL (10, 25)` = greater than the MAX (25).
    assert_eq!(
        rows(&mut session, "select a from t where a > all (select v from s) order by a"),
        "30"
    );

    // DISTINCT treats the two NULLs as equal: one NULL row survives.
    assert_eq!(
        rows(&mut session, "select distinct a from n order by a"),
        "NULL;1;2"
    );
}
