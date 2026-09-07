//! NULL ordering and grouping: NULLs sort FIRST ascending and LAST
//! descending (MySQL), and NULLs group together under GROUP BY.

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

#[test]
fn nulls_sort_first_then_last_and_group_together() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (NULL), (2), (1), (NULL)").unwrap();

    assert_eq!(
        rows(&mut session, "select a from t order by a"),
        "NULL;NULL;1;2",
        "NULLs lead the ascending order"
    );
    assert_eq!(
        rows(&mut session, "select a from t order by a desc"),
        "2;1;NULL;NULL",
        "NULLs trail the descending order"
    );
    assert_eq!(
        rows(&mut session, "select a, count(*) from t group by a order by a"),
        "NULL|2;1|1;2|1",
        "the two NULLs group into one bucket of 2"
    );
}
