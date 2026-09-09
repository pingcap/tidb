//! Multi-row ODKU accounting: two duplicates plus one fresh insert report
//! Go's 5 affected rows (2 per updated row + 1 for the insert), the dup
//! rows all take the assignment value, and the fresh row lands.

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
fn two_updates_one_insert_report_five() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, v int)").unwrap();
    session.run("insert into t values (1, 10), (2, 20), (3, 30)").unwrap();

    let affected = match session
        .run("insert into t values (1, 99), (3, 99), (4, 40) on duplicate key update v = 77")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 5, "Go: 2 per updated row (2 dups) + 1 insert");

    assert_eq!(
        rows(&mut session, "select a, v from t order by a"),
        "1|77;2|20;3|77;4|40"
    );
}
